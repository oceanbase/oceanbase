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

#include "lib/allocator/ob_qsync.h"
#include "lib/allocator/ob_retire_station.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace keybtree
{
using namespace oceanbase::common;

template<typename BtreeKey, typename BtreeVal>
void BtreeNode<BtreeKey, BtreeVal>::reset()
{
  index_.reset();
  magic_num_ = MAGIC_NUM;
  level_ = 0;
  new(&lock_) RWLock();
  max_del_version_ = 0;
  host_ = nullptr;
  ObLink::reset();
}

template<typename BtreeKey, typename BtreeVal>
int BtreeNode<BtreeKey, BtreeVal>::make_new_root(BtreeKey key1, BtreeNode *node_1, BtreeKey key2, BtreeNode *node_2, int16_t level, int64_t version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node_1) || OB_ISNULL(node_2)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    level_ = level;
    max_del_version_ = version;
    set_key_value(0, key1, (BtreeVal)node_1);
    set_key_value(1, key2, (BtreeVal)node_2);
    if (is_leaf()) {
      index_.unsafe_insert(0, 0);
      index_.unsafe_insert(1, 1);
    } else {
      index_.inc_count(2);
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
void BtreeNode<BtreeKey, BtreeVal>::print(FILE *file, const int depth) const
{
  if (OB_ISNULL(file)) {
    // do nothing
  } else if (is_leaf()) {
    MultibitSet index;
    index.load(index_);
    int count = index.size();
    fprintf(file, " index=%lx %dV:%ld ", index.get(), count, max_del_version_);
    for (int i = 0; i < count; i++) {
      fprintf(file, " %lx->%lx", (uint64_t)kvs_[i].key_.get_ptr(), (uint64_t)kvs_[i].val_);
    }
    for (int i = 0; i < count; i++) {
      fprintf(file, "\n%*s %s%s->%lx", depth * 2 + 2, "|-", get_tag(i, &index) ? "#": "+", get_key(i, &index).repr(), (uint64_t)get_val(i, &index));
    }
  } else {
    int count = size();
    fprintf(file, " index=%lx %dC:%ld", index_.get(), count, max_del_version_);
    for (int i = 0; i < count; i++) {
      fprintf(file, " %lx->%lx", (uint64_t)kvs_[i].key_.get_ptr(), (uint64_t)kvs_[i].val_);
    }
    for (int i = 0; i < count; i++) {
      fprintf(file, "\n%*s %s%s->%lx", depth * 2 + 2, "|-", get_tag(i)? "#": "+", get_key(i).repr(), (uint64_t)get_val(i));
      BtreeNode *child = (BtreeNode *)get_val(i);
      child->print(file, depth + 1);
    }
  }
}

template<typename BtreeKey, typename BtreeVal>
int BtreeNode<BtreeKey, BtreeVal>::get_next_active_child(int pos, int64_t version, int64_t* cnt, MultibitSet *index)
{
  if (version < max_del_version_) {
    ++pos;
  } else {
    while(++pos < size(index)) {
      if (!get_tag(pos, index)) {
        break;
      }
      if (OB_NOT_NULL(cnt)) {
        (*cnt) += estimate_level_weight(level_);
      }
    }
  }
  return pos;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeNode<BtreeKey, BtreeVal>::get_prev_active_child(int pos, int64_t version, int64_t* cnt, MultibitSet *index)
{
  if (version < max_del_version_) {
    --pos;
  } else {
    while(--pos >= 0) {
      if (!get_tag(pos, index)) {
        break;
      }
      if (OB_NOT_NULL(cnt)) {
        (*cnt) += estimate_level_weight(level_);
      }
    }
  }
  return pos;
}

template<typename BtreeKey, typename BtreeVal>
void BtreeNode<BtreeKey, BtreeVal>::copy(BtreeNode &dest, const int dest_start, const int start, const int end)
{
  if (OB_LIKELY(start < end)) {
    for (int i = 0; i < end - start; ++i) {
      dest.set_key_value(dest_start + i, get_key(start + i), get_val_with_tag(start + i));
      if (dest.is_leaf()) {
        dest.index_.unsafe_insert(dest_start + i, dest_start + i);
      }
    }
    if (!dest.is_leaf()) {
      dest.index_.inc_count(end - start);
    }
  }
}

template<typename BtreeKey, typename BtreeVal>
void BtreeNode<BtreeKey, BtreeVal>::copy_and_insert(BtreeNode &dest_node, const int start, const int end, int pos,
                                BtreeKey key_1, BtreeVal val_1, BtreeKey key_2, BtreeVal val_2)
{
  if (pos - start >= 0) {
    copy(dest_node, 0, start, pos);
    dest_node.insert_into_node(pos - start, key_1, val_1);
  }
  dest_node.insert_into_node(pos + 1 - start, key_2, val_2);
  copy(dest_node, (pos + 2) - start, pos + 1, end);
}

template<typename BtreeKey, typename BtreeVal>
uint64_t BtreeNode<BtreeKey, BtreeVal>::check_tag(MultibitSet *index) const
{
  uint64_t tag = 1;
  MultibitSet temp;
  if (OB_ISNULL(index)) {
    temp.load(this->index_);
    index = &temp;
  }
  for(int i = 0; i < size(index); i++) {
    if (0 == get_tag(i, index)) {
      tag = 0;
      break;
    }
  }
  return tag;
}

template<typename BtreeKey, typename BtreeVal>
void BtreeNode<BtreeKey, BtreeVal>::replace_child(BtreeNode *new_node, const int pos, BtreeNode *child, int64_t del_version)
{
  new_node->level_ = level_;
  new_node->max_del_version_ = std::max(max_del_version_, del_version);
  copy(*new_node, 0, 0, size());
  new_node->set_val(pos, (BtreeVal)child);
}

template<typename BtreeKey, typename BtreeVal>
void BtreeNode<BtreeKey, BtreeVal>::replace_child_and_key(BtreeNode *new_node, const int pos, BtreeKey key, BtreeNode *child, int64_t del_version)
{
  new_node->level_ = level_;
  new_node->max_del_version_ = std::max(max_del_version_, del_version);
  copy(*new_node, 0, 0, size());
  new_node->insert_into_node(pos, key, (BtreeVal)child);
}

template<typename BtreeKey, typename BtreeVal>
void BtreeNode<BtreeKey, BtreeVal>::split_child_no_overflow(BtreeNode *new_node, const int pos, BtreeKey key_1, BtreeVal val_1,
                                        BtreeKey key_2, BtreeVal val_2, int64_t del_version)
{
  new_node->level_ = level_;
  new_node->max_del_version_ = std::max(max_del_version_, del_version);
  copy_and_insert(*new_node, 0, size(), pos, key_1, val_1, key_2, val_2);
}

template<typename BtreeKey, typename BtreeVal>
void BtreeNode<BtreeKey, BtreeVal>::split_child_cause_recursive_split(BtreeNode *new_node_1, BtreeNode *new_node_2,
                                                  const int pos,
                                                  BtreeKey key_1,
                                                  BtreeVal val_1, BtreeKey key_2, BtreeVal val_2, int64_t del_version)
{
  const int32_t half_limit = is_leaf() ?
                             ((ObKeyBtree *)this->get_host())->update_split_info(pos) :
                             NODE_KEY_COUNT / 2;
  new_node_1->level_ = level_;
  new_node_2->level_ = level_;
  new_node_1->max_del_version_ = std::max(max_del_version_, del_version);
  new_node_2->max_del_version_ = std::max(max_del_version_, del_version);
  if (pos < half_limit) {
    copy_and_insert(*new_node_1, 0, half_limit, pos, key_1, val_1, key_2, val_2);
    copy(*new_node_2, 0, half_limit, size());
  } else {
    copy(*new_node_1, 0, 0, half_limit);
    copy_and_insert(*new_node_2, half_limit, size(), pos, key_1, val_1, key_2, val_2);
  }
}

template<typename BtreeKey, typename BtreeVal>
void Path<BtreeKey, BtreeVal>::reset()
{
  depth_ = 0;
  is_found_ = false;
}

template<typename BtreeKey, typename BtreeVal>
int Path<BtreeKey, BtreeVal>::push(BtreeNode *node, const int pos)
{
  int ret = OB_SUCCESS;
  if (depth_ >= MAX_DEPTH) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    path_[depth_].node_ = node;
    path_[depth_].pos_ = pos;
    depth_++;
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int Path<BtreeKey, BtreeVal>::top(BtreeNode *&node, int &pos)
{
  int ret = OB_SUCCESS;
  if (depth_ <= 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    node = nullptr;
    pos = -1;
  } else {
    node = path_[depth_ - 1].node_;
    pos = path_[depth_ - 1].pos_;
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int Path<BtreeKey, BtreeVal>::top_k(int k, BtreeNode*& node, int& pos)
{
  int ret = OB_SUCCESS;
  if (depth_ < k) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    node = nullptr;
    pos = -1;
  } else {
    node = path_[depth_ - k].node_;
    pos = path_[depth_ - k].pos_;
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int BaseHandle<BtreeKey, BtreeVal>::acquire_ref()
{
  int ret = OB_SUCCESS;
  qc_slot_ = qclock_.enter_critical();
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int GetHandle<BtreeKey, BtreeVal>::get(BtreeNode *root, BtreeKey key, BtreeVal &val)
{
  int ret = OB_SUCCESS;
  BtreeNode *leaf = nullptr;
  int pos = -1;
  bool is_found = false;
  MultibitSet *index = &this->index_;
  index->reset();
  if (OB_ISNULL(root)) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  while (OB_SUCCESS == ret && OB_ISNULL(leaf)) {
    if (is_found) {
      pos = 0;
    } else if (OB_FAIL(root->find_pos(this->get_comp(), key, is_found, pos, index))) {
      break;
    }
    if (pos < 0) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (root->is_leaf()) {
      leaf = root;
    } else {
      root = reinterpret_cast<BtreeNode *>(root->get_val(pos));
    }
  }
  if (OB_FAIL(ret) || OB_ISNULL(leaf)) {
    // do nothing
  } else if (is_found) {
    if (0 == index->size()) {
      index->load(leaf->get_index());
    }
    val = leaf->get_val(pos, index);
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ScanHandle<BtreeKey, BtreeVal>::get(BtreeKey &key, BtreeVal &val)
{
  int ret = OB_SUCCESS;
  BtreeNode *leaf = nullptr;
  int pos = 0;
  if (OB_FAIL(path_.top(leaf, pos))) {
    ret = OB_ITER_END;
  } else {
    key = leaf->get_key(pos, &this->index_);
    val = leaf->get_val_with_tag(pos, version_, &this->index_);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ScanHandle<BtreeKey, BtreeVal>::get(BtreeKey &key, BtreeVal &val, bool is_backward, BtreeKey*& last_key)
{
  int ret = OB_SUCCESS;
  BtreeNode *leaf = nullptr;
  int pos = 0;
  if (OB_FAIL(path_.top(leaf, pos))) {
    ret = OB_ITER_END;
  } else {
    MultibitSet *index = &this->index_;
    key = leaf->get_key(pos, index);
    val = leaf->get_val_with_tag(pos, version_, index);
    last_key = is_backward? &leaf->get_key(0, index): &leaf->get_key(leaf->size(index) - 1, index);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ScanHandle<BtreeKey, BtreeVal>::pop_level_node(const bool is_backward, const int64_t level, const double ratio,
    const int64_t gap_limit, int64_t &element_count, int64_t &phy_element_count,
    BtreeKey*& last_key, int64_t &gap_size)
{
  int ret = OB_SUCCESS;
  BtreeNode *node = nullptr;
  int pos = 0;
  int64_t cur_node_cnt = 0;
  do {
    if (OB_FAIL(path_.pop(node, pos))) {
      ret = OB_ITER_END;
    }
  } while (OB_SUCCESS == ret && node->level_ < level);
  if (OB_SUCCESS == ret) {
    MultibitSet *index = &this->index_;
    int count = node->size(index);
    cur_node_cnt = is_backward ? pos : count - pos;
    element_count += cur_node_cnt;
    phy_element_count += cur_node_cnt;
    last_key = is_backward? &node->get_key(0, index): &node->get_key(count - 1, index);
    if (node->check_tag(index)) {
      gap_size += cur_node_cnt;
    } else if (gap_size > 0) {
      if (!is_backward) {
        for (int64_t i = 0; i < count; ++i) {
          if (node->get_tag(i, index)) {
            ++gap_size;
          } else {
            if (gap_size >= gap_limit) {
              element_count -= static_cast<int64_t>(static_cast<double>(gap_size) * ratio);
              STORAGE_LOG(TRACE, "found a gap", K(element_count), K(gap_size), K(*last_key));
            }
            gap_size = 0;
            break;
          }
        }
      } else {
        for (int64_t i = count - 1; i >= 0; --i) {
          if (node->get_tag(i, index)) {
            ++gap_size;
          } else {
            if (gap_size >= gap_limit) {
              element_count -= static_cast<int64_t>(static_cast<double>(gap_size) * ratio);
              STORAGE_LOG(TRACE, "found a gap", K(element_count), K(gap_size), K(*last_key));
            }
            gap_size = 0;
            break;
          }
        }
      }
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ScanHandle<BtreeKey, BtreeVal>::pop_level_node(const int64_t level)
{
  int ret = OB_SUCCESS;
  while(true) {
    BtreeNode *node = nullptr;
    int pos = 0;
    if (OB_FAIL(path_.top(node, pos))) {
      ret = OB_ITER_END;
    } else if (node->level_ >= level) {
      break;
    } else {
      UNUSED(path_.pop(node, pos));
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ScanHandle<BtreeKey, BtreeVal>::find_path(BtreeNode *root, BtreeKey key, int64_t version)
{
  int ret = OB_SUCCESS;
  int pos = -1;
  bool may_exist = true;
  bool is_found = false;
  MultibitSet *index = &this->index_;
  index->reset();
  version_ = version;
  while (OB_NOT_NULL(root) && OB_SUCCESS == ret) {
    if (!may_exist || is_found) {
      pos = 0;
    } else if (OB_FAIL(root->find_pos(this->get_comp(), key, is_found, pos, index))) {
      break;
    }
    if (pos < 0) {
      may_exist = false;
      pos = 0;
    }
    if (OB_FAIL(path_.push(root, pos))) {
      // do nothing
    } else if (root->is_leaf()) {
      if (0 == index->size()) {
        index->load(root->get_index());
      }
      path_.set_is_found(is_found);
      root = nullptr;
    } else {
      root = (BtreeNode *)root->get_val(pos);
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ScanHandle<BtreeKey, BtreeVal>::scan_forward(const int64_t level) {
  int ret = OB_SUCCESS;
  BtreeNode *node = nullptr;
  int pos = 0;
  int64_t version = -1;
  int64_t *skip_cnt = nullptr;
  MultibitSet *index = &this->index_;
  while (OB_SUCCESS == ret) {
    if (OB_FAIL(path_.pop(node, pos))) {
      ret = OB_ITER_END;
    } else if ((pos = node->get_next_active_child(pos, version, skip_cnt, index)) >= node->size(index)) {
      // do nothing
    } else {
      path_.push(node, pos);
      if (node->level_ == level) {
        break;
      } else {
        BtreeNode* child = (BtreeNode*)node->get_val(pos);
        if (child->is_leaf()) {
          index->load(child->get_index());
        }
        path_.push(child, -1);
      }
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ScanHandle<BtreeKey, BtreeVal>::scan_backward(const int64_t level)
{
  int ret = OB_SUCCESS;
  BtreeNode *node = nullptr;
  int pos = 0;
  int64_t version = -1;
  int64_t *skip_cnt = nullptr;
  MultibitSet *index = &this->index_;
  while(OB_SUCCESS == ret) {
    if (OB_FAIL(path_.pop(node, pos))) {
      ret = OB_ITER_END;
    } else if ((pos = node->get_prev_active_child(pos, version, skip_cnt, index)) < 0) {
      // do nothing
    } else {
      path_.push(node, pos);
      if (node->level_ == level) {
        break;
      } else {
        BtreeNode* child = (BtreeNode*)node->get_val(pos);
        if (child->is_leaf()) {
          index->load(child->get_index());
        }
        path_.push(child, child->size(index));
      }
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ScanHandle<BtreeKey, BtreeVal>::scan_forward(bool skip_inactive, int64_t* skip_cnt) {
  int ret = OB_SUCCESS;
  BtreeNode* node = nullptr;
  int pos = 0;
  int64_t version = skip_inactive? version_: 0;
  MultibitSet *index = &this->index_;
  while(OB_SUCCESS == ret) {
    if (OB_FAIL(path_.pop(node, pos))) {
      ret = OB_ITER_END;
    } else if ((pos = node->get_next_active_child(pos, version, skip_cnt, index)) >= node->size(index)) {
      // do nothing
    } else {
      path_.push(node, pos);
      if (node->is_leaf()) {
        break;
      } else {
        BtreeNode* child = (BtreeNode*)node->get_val(pos);
        if (child->is_leaf()) {
          index->load(child->get_index());
        }
        path_.push(child, -1);
      }
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ScanHandle<BtreeKey, BtreeVal>::scan_backward(bool skip_inactive, int64_t* skip_cnt)
{
  int ret = OB_SUCCESS;
  BtreeNode *node = nullptr;
  int pos = 0;
  int64_t version = skip_inactive? version_: 0;
  MultibitSet *index = &this->index_;
  while(OB_SUCCESS == ret) {
    if (OB_FAIL(path_.pop(node, pos))) {
      ret = OB_ITER_END;
    } else if ((pos = node->get_prev_active_child(pos, version, skip_cnt, index)) < 0) {
      // do nothing
    } else {
      path_.push(node, pos);
      if (node->is_leaf()) {
        break;
      } else {
        BtreeNode* child = (BtreeNode*)node->get_val(pos);
        if (child->is_leaf()) {
          index->load(child->get_index());
        }
        path_.push(child, child->size(index));
      }
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
BtreeNode<BtreeKey, BtreeVal> *WriteHandle<BtreeKey, BtreeVal>::alloc_node()
{
  BtreeNode *p = nullptr;
  if (OB_NOT_NULL(p = (BtreeNode *)base_.alloc_node(get_is_in_delete()))) {
    alloc_list_.push(p);
  }
  return p;
}

template<typename BtreeKey, typename BtreeVal>
void WriteHandle<BtreeKey, BtreeVal>::free_node(BtreeNode *p)
{
  if (OB_NOT_NULL(p)) {
    base_.free_node(p);
    p = nullptr;
  }
}

template<typename BtreeKey, typename BtreeVal>
void WriteHandle<BtreeKey, BtreeVal>::free_list()
{
  BtreeNode *p = nullptr;
  while (OB_NOT_NULL(p = (BtreeNode *)retire_list_.pop())) {
    p->wrunlock();
  }
  while (OB_NOT_NULL(p = (BtreeNode *)alloc_list_.pop())) {
    free_node(p);
  }
}

template<typename BtreeKey, typename BtreeVal>
void WriteHandle<BtreeKey, BtreeVal>::retire(const int btree_err)
{
  if (OB_SUCCESS != btree_err) {
    free_list();
  } else {
    base_.retire(retire_list_);
  }
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::insert_and_split_upward(BtreeKey key, BtreeVal &val, BtreeNode *&new_root)
{
  int ret = OB_SUCCESS;
  int pos = -1;
  BtreeNode *old_node = nullptr;
  BtreeNode *new_node_1 = nullptr;
  BtreeNode *new_node_2 = nullptr;
  MultibitSet *index = &this->index_;
  UNUSED(this->path_.pop(old_node, pos)); // pop may failed, old_node is allowd to be NULL
  if (OB_ISNULL(old_node)) {
    if (OB_ISNULL(new_node_1 = alloc_node())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      new_node_1->insert_into_node(0, key, val);
    }
  } else if (this->path_.get_is_found()) {
    ret = OB_ENTRY_EXIST;
    BtreeVal old_val = val;
    val = old_node->get_val(pos, index);
    OB_LOG(WARN, "duplicate key", K(old_node->get_key(pos, index)), K(key), K(old_node->get_val(pos, index)), K(val), K(old_val));
  } else {
    ret = insert_into_node(old_node, pos, key, val, new_node_1, new_node_2);
  }
  while (OB_SUCCESS == ret && OB_NOT_NULL(new_node_1)) {
    uint64_t tag1 = check_tag(new_node_1);
    uint64_t tag2 = check_tag(new_node_2);
    if (OB_ISNULL(new_node_2)) {
      BTREE_ASSERT(0 == tag1);
      if (OB_SUCCESS != this->path_.pop(old_node, pos)) {
        new_root = new_node_1;
        new_node_1 = nullptr;
      } else if (pos < 0) {
        ret = replace_child_and_key(old_node, 0, new_node_1->get_key(0, index), add_tag(new_node_1, tag1), new_node_1, 0);
      } else {
        if (old_node->get_tag(pos, index) == tag1) {
          ret = replace_child(old_node, pos, (BtreeVal)add_tag(new_node_1, tag1));
          new_node_1 = nullptr;
        } else {
          ret = replace_child_and_key(old_node, pos, new_node_1->get_key(0, index), add_tag(new_node_1, tag1), new_node_1, 0);
        }
      }
    } else {
      uint64_t v1 = tag1? new_node_1->get_max_del_version(): 0;
      uint64_t v2 = tag2? new_node_2->get_max_del_version(): 0;
      if (OB_SUCCESS != this->path_.pop(old_node, pos)) {
        ret = this->make_new_root(new_root, new_node_1->get_key(0, index), add_tag(new_node_1, tag1), new_node_2->get_key(0), add_tag(new_node_2, tag2), (int16_t)(new_node_1->get_level() + 1), std::max(v1, v2));
        new_node_1 = nullptr;
        new_node_2 = nullptr;
      } else {
        ret = split_child(old_node, std::max(0, pos), new_node_1->get_key(0, index),
                          (BtreeVal)add_tag(new_node_1, tag1), new_node_2->get_key(0, index),
                          (BtreeVal)add_tag(new_node_2, tag2), new_node_1, new_node_2, std::max(v1, v2));
      }
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::tag_delete(BtreeKey key, BtreeVal& val, int64_t version, BtreeNode *&new_root)
{
  int ret = OB_SUCCESS;
  BtreeNode *old_node = nullptr;
  int pos = -1;
  BtreeNode* new_node = nullptr;
  int iret = this->path_.pop(old_node, pos);
  UNUSED(iret);
  if (OB_SUCC(tag_delete_on_leaf(old_node, pos, key, val, version, new_node))) {
    ret = tag_upward(new_node, new_root);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::tag_insert(BtreeKey key, BtreeNode *&new_root)
{
  int ret = OB_SUCCESS;
  BtreeNode *old_node = nullptr;
  int pos = -1;
  BtreeNode *new_node = nullptr;
  int iret = this->path_.pop(old_node, pos); // pop may failed, old_node is allowd to be NULL
  UNUSED(iret);
  if (OB_SUCC(tag_insert_on_leaf(old_node, pos, key, new_node))) {
    ret = tag_upward(new_node, new_root);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::insert_into_node(BtreeNode *old_node, int pos, BtreeKey key, BtreeVal val, BtreeNode *&new_node_1, BtreeNode *&new_node_2)
{
  int ret = OB_SUCCESS;
  int tmp_pos = -1;
  int count = 0;
  BtreeNode * tmp_node = nullptr;
  new_node_2 = nullptr;
  if (OB_ISNULL(old_node)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(try_wrlock(old_node))) {
    // do nothing
  } else if ((count = old_node->size()) != this->index_.size()) {
    // another thread has finished a insert, we should retry.
    ret = OB_EAGAIN;
  } else if (pos < 0
             || old_node->is_overflow(1, &this->index_)
             || (OB_SUCCESS == (path_.get(path_.get_root_level() - 1, tmp_node, tmp_pos))
                 && tmp_node->get_tag(tmp_pos, &this->index_) == 1)) {
    // if father node needs to be updated, we can't append directly because of the possibility of failure of updating father node.
    BtreeKey dummy_key;
    ret = split_child(old_node, pos, pos < 0 ? dummy_key : old_node->get_key(pos, &this->index_),
                      pos < 0 ? nullptr : old_node->get_val_with_tag(pos, &this->index_),
                      key, val, new_node_1, new_node_2, 0);
  } else {
    old_node->set_spin();
    old_node->set_key_value(count, key, val);
    old_node->get_index().free_insert(pos + 1, count);
    // it can not be retired when inserted successfully.
    UNUSED(retire_list_.pop());
    old_node->wrunlock();
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::try_wrlock(BtreeNode *node)
{
  int ret = OB_SUCCESS;
  uint16_t uid = (uint16_t)(get_itid() + 1);
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (node->is_hold_wrlock(uid)) {
    // do nothing
  } else if (OB_FAIL(node->try_wrlock(uid))) {
    // do nothing
  } else {
    retire_list_.push(node);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::tag_delete_on_leaf(BtreeNode* old_node, int pos, BtreeKey key, BtreeVal& val, int64_t version, BtreeNode*& new_node)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  if (OB_ISNULL(old_node) || !this->path_.get_is_found()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    val = old_node->get_val(pos, &this->index_);
    ret = tag_leaf(old_node, pos, new_node, 1, version);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::tag_insert_on_leaf(BtreeNode* old_node, int pos, BtreeKey key, BtreeNode*& new_node)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  if (OB_ISNULL(old_node) || !this->path_.get_is_found()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ret = tag_leaf(old_node, pos, new_node, 0, 0);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::tag_upward(BtreeNode* new_node, BtreeNode*& new_root)
{
  int ret = OB_SUCCESS;
  BtreeNode* old_node = nullptr;
  int pos = -1;
  MultibitSet *index = &this->index_;
  while (OB_SUCCESS == ret && OB_NOT_NULL(new_node)) {
    uint64_t tag = new_node->check_tag(index);
    if (OB_SUCCESS != this->path_.pop(old_node, pos)) {
      new_root = new_node;
      new_node = nullptr;
    } else {
      if (old_node->get_tag(pos, index) == tag) {
        ret = replace_child(old_node, pos, (BtreeVal)add_tag(new_node, tag));
        new_node = nullptr;
      } else {
        ret = replace_child_by_copy(old_node, pos, (BtreeVal)add_tag(new_node, tag), new_node->get_max_del_version(), new_node);
      }
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::tag_leaf(BtreeNode *old_node, const int pos, BtreeNode*& new_node, uint64_t tag, int64_t version)
{
  int ret = OB_SUCCESS;
  uint64_t old_tag = 1;
  int tmp_pos = -1;
  BtreeNode * tmp_node = nullptr;
  if (OB_FAIL(try_wrlock(old_node))) {
    // do nothing
  } else if (old_node->is_leaf() && old_node->size() != this->index_.size()) {
    ret = OB_EAGAIN;
  } else {
    // Wrlocked and checked before. No need to use index anymore below.
    for (int i = 0; i < old_node->size(); ++i) {
      if (i != pos) {
        old_tag &= old_node->get_tag(i);
      }
    }
    if (OB_SUCCESS == (path_.get(path_.get_root_level() - 1, tmp_node, tmp_pos))
        && tmp_node->get_tag(tmp_pos) == (old_tag & tag)) {
      old_node->set_spin();
      // if no need to tag_upward, then unlocked.
      old_node->set_max_del_version(std::max(old_node->get_max_del_version(), version));
      // Update max_del_version firstly to keep not reading extra tag.
      old_node->set_val(pos, (BtreeVal)add_tag((BtreeNode*)old_node->get_val(pos), tag));
      // When it's fast unlocking, DO NOT retire.
      UNUSED(retire_list_.pop());
      old_node->wrunlock();
    } else if (OB_ISNULL(new_node = alloc_node())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      old_node->replace_child(new_node, pos, add_tag((BtreeNode*)old_node->get_val(pos, &this->index_), tag), version);
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::replace_child_by_copy(BtreeNode *old_node, const int pos, BtreeVal val, int64_t version, BtreeNode*& new_node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_wrlock(old_node))) {
    // do nothing
  } else if (old_node->is_leaf() && old_node->size() != this->index_.size()) {
    ret = OB_EAGAIN;
  } else if (OB_ISNULL(new_node = alloc_node())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    old_node->replace_child(new_node, pos, (BtreeNode*)val, version);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::replace_child(BtreeNode *old_node, const int pos, BtreeVal val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(old_node)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(old_node->try_rdlock())) {
    ret = OB_EAGAIN;
  } else {
    old_node->set_val(pos, val, &this->index_);
    old_node->rdunlock();
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::replace_child_and_key(BtreeNode *old_node, const int pos, BtreeKey key, BtreeNode *child, BtreeNode *&new_node, int64_t version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(old_node) || OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(try_wrlock(old_node))) {
    // do nothing
  } else if (old_node->is_leaf() && old_node->size() != this->index_.size()) {
    ret = OB_EAGAIN;
  } else if (OB_ISNULL(new_node = alloc_node())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    old_node->replace_child_and_key(new_node, pos, key, child, version);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::make_new_root(BtreeNode *&root, BtreeKey key1, BtreeNode *node_1, BtreeKey key2, BtreeNode *node_2, int16_t level, int64_t version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node_1) || OB_ISNULL(node_2)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(root = alloc_node())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ret = root->make_new_root(key1, node_1, key2, node_2, level, version);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int WriteHandle<BtreeKey, BtreeVal>::split_child(BtreeNode *old_node, const int pos, BtreeKey key_1, BtreeVal val_1, BtreeKey key_2,
                BtreeVal val_2, BtreeNode *&new_node_1, BtreeNode *&new_node_2, int64_t version)
{
  int ret = OB_SUCCESS;
  new_node_2 = nullptr;
  if (OB_ISNULL(old_node)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(try_wrlock(old_node))) {
    // do nothing
  } else if (old_node->is_overflow(1)) {
    if (OB_ISNULL(new_node_1 = alloc_node()) || OB_ISNULL(new_node_2 = alloc_node())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      old_node->split_child_cause_recursive_split(new_node_1, new_node_2, pos, key_1, val_1, key_2, val_2, version);
    }
  } else if (OB_ISNULL(new_node_1 = alloc_node())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    old_node->split_child_no_overflow(new_node_1, pos, key_1, val_1, key_2, val_2, version);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
void Iterator<BtreeKey, BtreeVal>::reset()
{
  scan_handle_.reset();
  jump_key_ = nullptr;
  cmp_result_ = 0;
  new(&start_key_)BtreeKey();
  new(&end_key_)BtreeKey();
  start_exclude_ = false;
  end_exclude_ = false;
  scan_backward_ = false;
  is_iter_end_ = false;
  iter_count_ = 0;
}

template<typename BtreeKey, typename BtreeVal>
int Iterator<BtreeKey, BtreeVal>::set_key_range(const BtreeKey min_key, const bool start_exclude,
                            const BtreeKey max_key, const bool end_exclude, int64_t version)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_FAIL(scan_handle_.acquire_ref())) {
    OB_LOG(ERROR, "acquire_ref fail", K(ret));
  } else if (OB_FAIL(scan_handle_.find_path(ATOMIC_LOAD(&btree_.root_), min_key, version))) {
    // do nothing
  } else {
    ret = comp_.compare(max_key, min_key, cmp);
    scan_backward_ = (cmp < 0);
    start_key_ = min_key;
    end_key_ = max_key;
    start_exclude_ = start_exclude;
    end_exclude_ = end_exclude;
    jump_key_ = nullptr;
    cmp_result_ = 0;
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int Iterator<BtreeKey, BtreeVal>::get_next(BtreeKey &key, BtreeVal &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter_next(key, value))){
    // do nothing
  } else if (0 == iter_count_) {
    int cmp = 0;
    ret = scan_backward_ ? comp_.compare(start_key_, key, cmp) : comp_.compare(key, start_key_, cmp);
    if (OB_SUCC(ret) && (cmp < 0 || (start_exclude_ && 0 == cmp))) {
      ret = iter_next(key, value);
    }
  }
  if (OB_SUCCESS == ret) {
    iter_count_++;
  } else {
    scan_handle_.release_ref();
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int Iterator<BtreeKey, BtreeVal>::next_on_level(const int64_t level, BtreeKey& key, BtreeVal& value)
{
  int ret = OB_SUCCESS;
  BtreeKey* jump_key = nullptr;
  if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(scan_handle_.pop_level_node(level))) {
    // do nothing
  } else if (OB_FAIL(scan_handle_.get(key, value, scan_backward_, jump_key))) {
    // do nothing
  } else {
    int cmp = 0;
    if (OB_FAIL(comp(key, jump_key, cmp))) {
      // do nothing
    } else if (cmp < 0) {
      ret = OB_ITER_END;
    } else if (cmp > 0) {
      if (OB_SUCCESS != (scan_backward_ ? scan_handle_.scan_backward(level) :
                            scan_handle_.scan_forward(level))) {
        is_iter_end_ = true;
      }
    } else if (cmp == 0) {
      is_iter_end_ = true;
    }
  }
  if (OB_SUCCESS != ret) {
    is_iter_end_ = true;
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int Iterator<BtreeKey, BtreeVal>::estimate_one_level(const int64_t level, const int64_t start_batch_count, const int64_t end_batch_count,
                       const int64_t max_node_count, const int64_t skip_range_limit, const double gap_ratio,
                       int64_t &level_physical_row_count, int64_t &level_element_count, int64_t &node_count)
{
  int ret = OB_SUCCESS;
  node_count = 0;
  level_physical_row_count = 0;
  level_element_count = 0;
  int64_t batch_physical_row_count= 0;
  int64_t batch_element_count = 0;
  int64_t batch_count = start_batch_count;
  int64_t gap_size = 0;
  while (node_count < max_node_count
      && OB_SUCC(iter_next_batch_level_node(batch_physical_row_count,
          batch_count, level, gap_size, skip_range_limit, batch_element_count, gap_ratio))) {
    STORAGE_LOG(TRACE, "iter one batch", K(batch_physical_row_count), K(batch_count), K(level),
        K(gap_size), K(batch_element_count), K(level_physical_row_count), K(level_element_count),
        K(node_count));
    level_physical_row_count += batch_physical_row_count;
    level_element_count += batch_element_count;
    node_count += batch_count;
    batch_count = batch_count * 2 >= end_batch_count ? end_batch_count : batch_count * 2;
  }
  if (gap_size >= skip_range_limit) {
    level_physical_row_count -= static_cast<int64_t>(static_cast<double>(gap_size) * gap_ratio);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int Iterator<BtreeKey, BtreeVal>::estimate_element_count(int64_t &physical_row_count, int64_t &element_count, const double ratio)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_SAMPLE_LEAF_COUNT = 500;
  int64_t node_count = 0;
  int64_t avg_element_count_per_leaf = 0;
  int64_t level_element_count = 0;
  int64_t level_physical_row_count = 0;
  int64_t limit = OB_SKIP_RANGE_LIMIT;
  physical_row_count = 0;
  element_count = 0;
  if (OB_FAIL(estimate_one_level(0, 1, 1024, MAX_SAMPLE_LEAF_COUNT, limit, ratio,
        level_physical_row_count, level_element_count, node_count))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to estimate level 0", K(ret));
    }
  }
  physical_row_count += level_physical_row_count;
  element_count += level_element_count;
  STORAGE_LOG(TRACE, "finish sample leaf level", K(physical_row_count), K(element_count), K(node_count));
  if (OB_SUCCESS == ret && node_count >= MAX_SAMPLE_LEAF_COUNT) {
    avg_element_count_per_leaf = MAX(std::lround(static_cast<double>(level_element_count) / static_cast<double>(node_count)), 1);
    limit = limit / avg_element_count_per_leaf + 1;
    if (OB_FAIL(estimate_one_level(1, 64, 1024, INT64_MAX,
            limit, ratio, level_physical_row_count, level_element_count, node_count))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to estimate level 1", K(ret));
      }
    }
    physical_row_count += level_physical_row_count * avg_element_count_per_leaf;
    element_count += level_element_count * avg_element_count_per_leaf;
  }
  STORAGE_LOG(TRACE, "finish sample second level", K(physical_row_count), K(element_count), K(node_count));
  if (OB_SUCCESS != ret) {
    scan_handle_.release_ref();
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int Iterator<BtreeKey, BtreeVal>::iter_next(BtreeKey &key, BtreeVal &value)
{
  int ret = OB_SUCCESS;
  bool skip_inactive = false;
  BtreeKey* jump_key = nullptr;
  if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(scan_handle_.get(key, value, scan_backward_, jump_key))) {
    // do nothing
  } else {
    int cmp = 0;
    if (OB_FAIL(comp(key, jump_key, cmp))) {
      // do nothing
    } else if (cmp < 0) {
      ret = OB_ITER_END;
    } else if (cmp > 0) {
      if (OB_SUCCESS != (scan_backward_ ? scan_handle_.scan_backward(skip_inactive) :
                            scan_handle_.scan_forward(skip_inactive))) {
        is_iter_end_ = true;
      }
    } else if (cmp == 0) {
      is_iter_end_ = true;
      if (0 != end_exclude_) {
        ret = OB_ITER_END;
      }
    }
  }
  if (OB_SUCCESS != ret) {
    is_iter_end_ = true;
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int Iterator<BtreeKey, BtreeVal>::iter_next_batch_level_node(int64_t &element_count, const int64_t batch_count,
                                         const int64_t level, int64_t &gap_size,
                                         const int64_t gap_limit, int64_t &phy_element_count,
                                         const double ratio)
{
  int ret = OB_SUCCESS;
  BtreeKey* jump_key = nullptr;
  int64_t cur_level_count = 0;
  int cmp = 0;
  element_count = 0;
  phy_element_count = 0;
  if (is_iter_end_) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCCESS == ret && cur_level_count < batch_count && !is_iter_end_) {
      if (OB_FAIL(scan_handle_.pop_level_node(
                  scan_backward_, level, ratio, gap_limit, element_count, phy_element_count,
                  jump_key, gap_size))) {
      } else {
        // rowkey comparation is very expensive, so we compare rowkey for batch leafs
        if (++cur_level_count == batch_count) {
          ret = comp(*jump_key, jump_key, cmp);
        } else {
          cmp = 1; // in the middle of the batch level node, skip key comparation,
                   // assume all values in this level node is satisfied
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (cmp < 0) {
          is_iter_end_ = true;
          if (gap_size >= gap_limit) {
            element_count -= static_cast<int64_t>(static_cast<double>(gap_size) * ratio);
            STORAGE_LOG(DEBUG, "found a gap", K(element_count), K(gap_size), K(*jump_key));
          }
          gap_size = 0;
          element_count /= 2; // FIXME: when the last rowkey in the batch leaf is not in the range,
                              // we only return the half of element count.
          phy_element_count /= 2;
        } else if (cmp > 0) {
          // middle leaf
          if (OB_SUCCESS != (scan_backward_ ? scan_handle_.scan_backward(level) :
                                scan_handle_.scan_forward(level))) {
            is_iter_end_ = true;
          }
        } else if (cmp == 0) {
          // the last leaf, all the values in this leaf are satified
          is_iter_end_ = true;
        }
      }
    }
  }
  if (OB_SUCCESS != ret) {
    is_iter_end_ = true;
  }
  return ret;
}

// cmp < 0: iter end
template<typename BtreeKey, typename BtreeVal>
int Iterator<BtreeKey, BtreeVal>::comp(BtreeKey& cur_key, BtreeKey* jump_key, int &cmp)
{
  int ret = OB_SUCCESS;
  cmp = 0;
  if (OB_ISNULL(jump_key)) {
    // do nothing
  } else if (jump_key_ == jump_key) {
    cmp = cmp_result_;
  } else {
    ret = scan_backward_? comp_.compare(*jump_key, end_key_, cmp_result_) : comp_.compare(end_key_, *jump_key, cmp_result_);
    jump_key_ = jump_key;
    cmp = cmp_result_;
  }
  if (OB_SUCC(ret) && cmp <= 0) {
    ret = scan_backward_? comp_.compare(cur_key, end_key_, cmp) : comp_.compare(end_key_, cur_key, cmp);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
void BtreeIterator<BtreeKey, BtreeVal>::KVQueue::reset()
{
  push_ = 0;
  pop_ = 0;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeIterator<BtreeKey, BtreeVal>::KVQueue::push(const BtreeKV &data)
{
  int ret = 0;
  if (push_ >= pop_ + capacity) {
    ret = OB_EAGAIN;
  } else {
    items_[idx(push_++)] = data;
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeIterator<BtreeKey, BtreeVal>::KVQueue::pop(BtreeKV &data)
{
  int ret = 0;
  if (pop_ >= push_) {
    ret = OB_EAGAIN;
  } else {
    data = items_[idx(pop_++)];
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeIterator<BtreeKey, BtreeVal>::init(ObKeyBtree &btree)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(iter_)) {
    ret = OB_INIT_TWICE;
  } else {
    iter_ = new(buf_) Iterator(btree);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
void BtreeIterator<BtreeKey, BtreeVal>::reset()
{
  kv_queue_.reset();
  is_iter_end_ = false;
  version_ = INT64_MAX;
  end_exclude_ = false;
  start_exclude_ = false;
  new(&end_key_)BtreeKey();
  new(&start_key_)BtreeKey();
  scan_backward_ = false;
  if (OB_NOT_NULL(iter_)) {
    iter_->reset();
    iter_ = nullptr;
  }
}

template<typename BtreeKey, typename BtreeVal>
int BtreeIterator<BtreeKey, BtreeVal>::set_key_range(const BtreeKey min_key, const bool start_exclude,
                                 const BtreeKey max_key, const bool end_exclude, int64_t version)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  ret = iter_->get_comp().compare(max_key, min_key, cmp);
  scan_backward_ = (cmp < 0);
  start_key_ = min_key;
  end_key_ = max_key;
  start_exclude_ = start_exclude;
  end_exclude_ = end_exclude;
  version_ = version;
  is_iter_end_ = false;
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeIterator<BtreeKey, BtreeVal>::get_next(BtreeKey &key, BtreeVal &value)
{
  int ret = OB_SUCCESS;
  BtreeKV item;
  if (OB_ISNULL(iter_)) {
    ret = OB_ITER_END;
  } else if (OB_SUCC(kv_queue_.pop(item))) {
    // do nothing
  } else if (OB_FAIL(scan_batch())) {
    // do nothing
  } else if (OB_FAIL(kv_queue_.pop(item))) {
    // do nothing
  }
  if (OB_SUCCESS == ret) {
    key = item.key_;
    value = item.val_;
  } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
    TRANS_LOG(ERROR, "get_next failed", KR(ret));
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeIterator<BtreeKey, BtreeVal>::scan_batch()
{
  int ret = OB_SUCCESS;
  if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(iter_->set_key_range(start_key_, start_exclude_, end_key_,
                                          end_exclude_, version_))) {
    // do nothing
  } else {
    BtreeKV item;
    while (OB_SUCCESS == ret) {
      if (OB_FAIL(iter_->get_next(item.key_, item.val_))) {
        is_iter_end_ = true;
        if (kv_queue_.size() > 0) {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_SUCCESS != kv_queue_.push(item)) {
        break;
      } else {
        start_key_ = item.key_;
        start_exclude_ = true;
      }
    }
    iter_->reset();
  }
  if (OB_SUCCESS != ret) {
    is_iter_end_ = true;
  }
  return ret;
}
// ob_keybtree_deps.h end

// ob_keybtree.h begin

template<typename BtreeKey, typename BtreeVal>
void BtreeNodeList<BtreeKey, BtreeVal>::bulk_push(BtreeNode* first, BtreeNode* last) {
  BtreeNode* tail = load_lock();
  last->next_ = tail;
  ATOMIC_STORE(&tail_, first);
}

template<typename BtreeKey, typename BtreeVal>
BtreeNode<BtreeKey, BtreeVal>* BtreeNodeList<BtreeKey, BtreeVal>::pop() {
  BtreeNode* tail = nullptr;
  if (OB_NOT_NULL(ATOMIC_LOAD(&tail_))) {
    tail = load_lock();
    ATOMIC_STORE(&tail_, OB_NOT_NULL(tail)? (BtreeNode*)tail->next_: nullptr);
  }
  return tail;
}

template<typename BtreeKey, typename BtreeVal>
BtreeNode<BtreeKey, BtreeVal>* BtreeNodeList<BtreeKey, BtreeVal>::load_lock() {
  BtreeNode* tail = nullptr;
  BtreeNode* LOCK = (BtreeNode*)~0UL;
  while(LOCK == (tail = ATOMIC_TAS(&tail_, LOCK)))
    sched_yield();
  return tail;
}

template<typename BtreeKey, typename BtreeVal>
int64_t BtreeNodeAllocator<BtreeKey, BtreeVal>::push_idx()
{
  RLOCAL(int64_t, push_idx);
  if (0 == push_idx) {
    push_idx = icpu_id();
  }
  return (push_idx++) % MAX_LIST_COUNT; 
}

template<typename BtreeKey, typename BtreeVal>
int64_t BtreeNodeAllocator<BtreeKey, BtreeVal>::pop_idx()
{
  RLOCAL(int64_t, pop_idx);
  if (0 == pop_idx) {
    pop_idx = icpu_id();
  }
  return (pop_idx++) % MAX_LIST_COUNT; 
}

template<typename BtreeKey, typename BtreeVal>
BtreeNode<BtreeKey, BtreeVal> *BtreeNodeAllocator<BtreeKey, BtreeVal>::alloc_node(const bool is_emergency)
{
  BtreeNode *p = nullptr;
  int ret = OB_SUCCESS;
  UNUSED(is_emergency);
  if (OB_FAIL(pop(p))) {
    OB_LOG(WARN, "alloc_block fail", K(get_allocated()));
  }
  return p;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeNodeAllocator<BtreeKey, BtreeVal>::pop(BtreeNode*& p)
{
  int64_t pop_list_idx = pop_idx();
  const int64_t NODE_SIZE = sizeof(BtreeNode);
  if (OB_ISNULL(p = free_list_array_[pop_list_idx].pop())) {
    // queue is empty, fill nodes.
    char *block = nullptr;
    if (OB_NOT_NULL(block = (char *)allocator_.alloc(NODE_SIZE * NODE_COUNT_PER_ALLOC))) {
      int64_t pushed_node_cnt = 0;
      // init all nodes
      for (int64_t idx = 0; (idx + 1) <= NODE_COUNT_PER_ALLOC; ++idx) {
        (new(block + idx * NODE_SIZE) BtreeNode())->next_ =
          reinterpret_cast<BtreeNode *>(block + (idx + 1) * NODE_SIZE);
      }
      // return first node
      p = reinterpret_cast<BtreeNode *>(block);
      p->next_ = nullptr;
      // first list jumped.
      pushed_node_cnt += (NODE_COUNT_PER_ALLOC / MAX_LIST_COUNT);
      free_list_array_[pop_list_idx].bulk_push(
        reinterpret_cast<BtreeNode *>(block + 1 * NODE_SIZE),//jump the first node
        reinterpret_cast<BtreeNode *>(block + (pushed_node_cnt - 1) * NODE_SIZE)
      );
      // every queue pushed (remaining nodes/remaining queues) nodes to keep every node being used.
      for (int64_t i = 1; i < MAX_LIST_COUNT; ++i) {
        int64_t list_idx = (pop_list_idx + i) % MAX_LIST_COUNT;
        BtreeNode * first_node_ptr = reinterpret_cast<BtreeNode *>(block + pushed_node_cnt * NODE_SIZE);
        pushed_node_cnt += (NODE_COUNT_PER_ALLOC - pushed_node_cnt) / (MAX_LIST_COUNT - i);
        BtreeNode * last_node_ptr = reinterpret_cast<BtreeNode *>(block + (pushed_node_cnt - 1) * NODE_SIZE);
        free_list_array_[list_idx].bulk_push(first_node_ptr, last_node_ptr);
      }
    }
  }
  return OB_NOT_NULL(p) ? OB_SUCCESS : OB_ALLOCATE_MEMORY_FAILED;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeRawIterator<BtreeKey, BtreeVal>::init(ObKeyBtree &btree)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(iter_)) {
    ret = OB_INIT_TWICE;
  } else {
    iter_ = new (buf_)Iterator(btree);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
void BtreeRawIterator<BtreeKey, BtreeVal>::reset()
{
  if (OB_NOT_NULL(iter_)) {
    iter_->~Iterator();
    iter_ = nullptr;
  }
}

template<typename BtreeKey, typename BtreeVal>
int BtreeRawIterator<BtreeKey, BtreeVal>::set_key_range(const BtreeKey min_key, const bool start_exclude,
                                    const BtreeKey max_key, const bool end_exclude, int64_t version)
{
  return iter_->set_key_range(min_key, start_exclude, max_key, end_exclude, version);
}

template<typename BtreeKey, typename BtreeVal>
int BtreeRawIterator<BtreeKey, BtreeVal>::get_next(BtreeKey &key, BtreeVal &val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(iter_->get_next(key, val))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeRawIterator<BtreeKey, BtreeVal>::estimate_key_count(int64_t top_level, int64_t& child_count, int64_t& key_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_ENTRY_NOT_EXIST;
    child_count = 0;
    key_count = 0;
  } else if (top_level > iter_->get_root_level()) {
    ret = OB_ENTRY_NOT_EXIST;
    child_count = 0;
    key_count = 0;
  } else {
    BtreeKey key;
    BtreeVal val = nullptr;
    int64_t level = iter_->get_root_level() - top_level;
    child_count = 0;
    while(OB_SUCC(iter_->next_on_level(level, key, val))) {
      child_count++;
    }
    key_count = child_count * estimate_level_weight(level);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

template <typename BtreeKey, typename BtreeVal>
int BtreeRawIterator<BtreeKey, BtreeVal>::split_range(int64_t top_level,
                                                      int64_t btree_node_count,
                                                      int64_t range_count,
                                                      common::ObIArray<BtreeKey> &key_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_NOT_INIT;
  } else if (range_count < 1 || btree_node_count < range_count) {
    ret = OB_INVALID_ARGUMENT;
  } else if (top_level > iter_->get_root_level()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    BtreeKey key;
    BtreeVal val = nullptr;
    int64_t level = iter_->get_root_level() - top_level;

    int64_t remaining_btree_node_count = btree_node_count;
    // each loop fill in a key for spliting range
    int64_t range_idx = 0;
    for (range_idx = 0; OB_SUCC(ret) && range_idx < range_count; range_idx++)
    {
      // (range_count - range_idx) means the last range count which need to be splitted
      int64_t btree_node_cnt_in_this_range = remaining_btree_node_count / (range_count - range_idx);

      // loop btree_node_cnt_in_this_range times to get the next key for splitting
      for (int64_t iter_node_count = 0; OB_SUCC(ret) && iter_node_count < btree_node_cnt_in_this_range;
           iter_node_count++) {
        if (OB_FAIL(iter_->next_on_level(level, key, val))) {
          OB_LOG(WARN,
                 "iterate btree node on level failed",
                 KR(ret),
                 K(level),
                 K(iter_node_count),
                 K(btree_node_cnt_in_this_range),
                 K(remaining_btree_node_count),
                 K(range_count),
                 K(range_idx));
        }
      }

      // update remaining btree node count
      remaining_btree_node_count = remaining_btree_node_count - btree_node_cnt_in_this_range;

      if (OB_SUCC(ret) && OB_FAIL(key_array.push_back(key))) {
        OB_LOG(WARN,
               "push back rowkey into key array failed",
               KR(ret),
               K(range_idx),
               K(range_count),
               K(top_level),
               K(btree_node_count));
      }
    }

    if (range_idx < range_count) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN,
             "btree split range: can not get enough sub range",
             K(btree_node_count),
             K(range_idx),
             K(range_count));
    }
  }

  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int BtreeRawIterator<BtreeKey, BtreeVal>::estimate_element_count(int64_t &physical_row_count, int64_t &element_count, const double ratio)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    physical_row_count = 0;
    element_count = 0;
    ret = OB_ITER_END;
  } else if (OB_FAIL(iter_->estimate_element_count(physical_row_count, element_count, ratio))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
bool BtreeRawIterator<BtreeKey, BtreeVal>::is_reverse_scan() const { return iter_->is_reverse_scan(); }

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::init()
{
  UNUSED(update_split_info(NODE_KEY_COUNT / 2));
  return OB_SUCCESS;
}

template<typename BtreeKey, typename BtreeVal>
void ObKeyBtree<BtreeKey, BtreeVal>::print(FILE *file) const
{
  if (OB_NOT_NULL(file)) {
    fprintf(file, "\n|root=%p node_size=%lu node_key_count=%d total_size=%ld\n", root_, sizeof(BtreeNode),
            NODE_KEY_COUNT, size());
    if (OB_NOT_NULL(root_)) {
      root_->print(file, 0);
    }
  }
}

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::pre_batch_destroy()
{
  ObTimeGuard tg("keybtree pre_batch_destroy", 50L * 1000L); // 50ms
  destroy(ATOMIC_SET(&root_, nullptr));
  return OB_SUCCESS;
}

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::batch_destroy()
{
  ObTimeGuard tg("keybtree batch_destroy", 50L * 1000L); // 50ms
  {
    HazardList reclaim_list;
    BtreeNode *p = nullptr;
    CriticalGuard(get_qsync());
    get_retire_station().purge(reclaim_list);
    tg.click();
    while (OB_NOT_NULL(p = reinterpret_cast<BtreeNode *>(reclaim_list.pop()))) {
      free_node(p);
      p = nullptr;
    }
    tg.click();
  }
  WaitQuiescent(get_qsync());
  tg.click();
  return OB_SUCCESS;
}

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::destroy(const bool is_batch_destroy)
{
  if (!is_batch_destroy) {
    ObTimeGuard tg("keybtree destroy", 50L * 1000L); // 50ms
    destroy(ATOMIC_SET(&root_, nullptr));
    tg.click();
    {
      HazardList reclaim_list;
      BtreeNode *p = nullptr;
      CriticalGuard(get_qsync());
      get_retire_station().purge(reclaim_list);
      tg.click();
      while (OB_NOT_NULL(p = reinterpret_cast<BtreeNode *>(reclaim_list.pop()))) {
        free_node(p);
        p = nullptr;
      }
      tg.click();
    }
    WaitQuiescent(get_qsync());
    tg.click();
  }
  return OB_SUCCESS;
}

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::del(const BtreeKey key, BtreeVal &value, int64_t version)
{
  int ret = OB_EAGAIN;
  while (OB_EAGAIN == ret) {
    BtreeNode *old_root = nullptr;
    BtreeNode *new_root = nullptr;
    WriteHandle handle(*this);
    handle.get_is_in_delete() = true;
    if (OB_FAIL(handle.acquire_ref())) {
      // do nothing
    } else if (OB_FAIL(handle.find_path(old_root = ATOMIC_LOAD(&root_), key))) {
      // do nothing
    } else if (OB_FAIL(handle.tag_delete(key, value, version, new_root = old_root))) {
      // do nothing
    } else if (old_root != new_root) {
      if (!ATOMIC_BCAS(&root_, old_root, new_root)) {
        ret = OB_EAGAIN;
      }
    }
    handle.release_ref();
    handle.retire(ret);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::re_insert(const BtreeKey key, BtreeVal value)
{
  int ret = OB_EAGAIN;
  UNUSED(value);
  while (OB_EAGAIN == ret) {
    BtreeNode *old_root = nullptr;
    BtreeNode *new_root = nullptr;
    WriteHandle handle(*this);
    handle.get_is_in_delete() = true;
    if (OB_FAIL(handle.acquire_ref())) {
      OB_LOG(ERROR, "acquire_ref fail", K(ret));
    } else if (OB_FAIL(handle.find_path(old_root = ATOMIC_LOAD(&root_), key))) {
      OB_LOG(ERROR, "re_insert fail", K(ret));
    } else if (OB_FAIL(handle.tag_insert(key, new_root = old_root))) {
      // do nothing
    } else if (old_root != new_root) {
      if (!ATOMIC_BCAS(&root_, old_root, new_root)) {
        ret = OB_EAGAIN;
      }
    }
    handle.release_ref();
    handle.retire(ret);
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::insert(const BtreeKey key, BtreeVal &value)
{
  int ret = OB_SUCCESS;
  BtreeNode *old_root = nullptr;
  BtreeNode *new_root = nullptr;
  WriteHandle handle(*this);
  BTREE_ASSERT(((uint64_t)value & 7ULL) == 0);
  handle.get_is_in_delete() = false;
  if (OB_FAIL(handle.acquire_ref())) {
    OB_LOG(ERROR, "acquire_ref fail", K(ret));
  } else {
    ret = OB_EAGAIN;
  }
  while (OB_EAGAIN == ret) {
    if (OB_FAIL(handle.find_path(old_root = ATOMIC_LOAD(&root_), key))) {
      OB_LOG(ERROR, "path.search error", K(root_), K(ret));
    } else if (OB_FAIL(handle.insert_and_split_upward(key, value, new_root = old_root))) {
      // do nothing
    } else if (old_root != new_root) {
      if (!ATOMIC_BCAS(&root_, old_root, new_root)) {
        ret = OB_EAGAIN;
      }
    }
    if (OB_EAGAIN == ret) {
      handle.free_list();
      //sched_yield();
    }
  }
  handle.release_ref();
  handle.retire(ret);
  if (OB_SUCC(ret)) {
    size_.inc(1);
  } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
    OB_LOG(WARN, "btree.set(key) error", KR(ret), K(key), K(value));
  } else {
    OB_LOG(ERROR, "btree.set(key) error", KR(ret), K(key), K(value));
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::get(const BtreeKey key, BtreeVal &value)
{
  int ret = OB_SUCCESS;
  GetHandle handle(*this);
  if (OB_FAIL(handle.acquire_ref())) {
    OB_LOG(ERROR, "acquire_ref fail", K(ret));
  } else if (OB_FAIL(handle.get(ATOMIC_LOAD(&root_), key, value))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      OB_LOG(ERROR, "btree.get(key) fail", KR(ret), K(key), K(value));
    }
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::set_key_range(BtreeIterator &iter, const BtreeKey min_key, const bool start_exclude,
                              const BtreeKey max_key, const bool end_exclude, int64_t version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter.init(*this))) {
    // do nothing
  } else if (OB_FAIL(iter.set_key_range(min_key, start_exclude, max_key, end_exclude, version))) {
    // do nothing
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
int ObKeyBtree<BtreeKey, BtreeVal>::set_key_range(BtreeRawIterator &iter, const BtreeKey min_key, const bool start_exclude,
                              const BtreeKey max_key, const bool end_exclude, int64_t version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter.init(*this))) {
    // do nothing
  } else if (OB_FAIL(iter.set_key_range(min_key, start_exclude, max_key, end_exclude, version))) {
    // do nothing
  }
  return ret;
}

template<typename BtreeKey, typename BtreeVal>
BtreeNode<BtreeKey, BtreeVal> *ObKeyBtree<BtreeKey, BtreeVal>::alloc_node(const bool is_emergency)
{
  BtreeNode *p = nullptr;
  if (OB_NOT_NULL(p = (BtreeNode *)node_allocator_.alloc_node(is_emergency))) {
    p->reset();
    p->set_host((void *)this);
  }
  return p;
}

template<typename BtreeKey, typename BtreeVal>
void ObKeyBtree<BtreeKey, BtreeVal>::free_node(BtreeNode *p)
{
  if (OB_NOT_NULL(p)) {
    ObKeyBtree *host = (ObKeyBtree *)p->get_host();
    if (OB_NOT_NULL(host)) {
      host->node_allocator_.free_node(p);
      p = nullptr;
    }
  }
}

template<typename BtreeKey, typename BtreeVal>
void ObKeyBtree<BtreeKey, BtreeVal>::retire(HazardList &retire_list)
{
  HazardList reclaim_list;
  BtreeNode *p = nullptr;
  CriticalGuard(get_qsync());
  get_retire_station().retire(reclaim_list, retire_list);
  while (OB_NOT_NULL(p = (BtreeNode *)reclaim_list.pop())) {
    free_node(p);
    p = nullptr;
  }
}

template<typename BtreeKey, typename BtreeVal>
int32_t ObKeyBtree<BtreeKey, BtreeVal>::update_split_info(int32_t split_pos)
{
  if (split_pos < 0) {
    split_pos = 0;
  }
  UNUSED(ATOMIC_FAA(&split_info_, 0x100000000ULL + split_pos));
  const int32_t ret = split_pos_sum_ / split_count_;
  return (ret < 1) ? 1 : ret;
}

template<typename BtreeKey, typename BtreeVal>
RetireStation &ObKeyBtree<BtreeKey, BtreeVal>::get_retire_station()
{
  static RetireStation retire_station_(get_qclock(), RETIRE_LIMIT);
  return retire_station_;
}

template<typename BtreeKey, typename BtreeVal>
QClock& ObKeyBtree<BtreeKey, BtreeVal>::get_qclock()
{
  static QClock qclock_;
  return qclock_;
}

template<typename BtreeKey, typename BtreeVal>
ObQSync& ObKeyBtree<BtreeKey, BtreeVal>::get_qsync()
{
  static ObQSync qsync;
  return qsync;
}

template<typename BtreeKey, typename BtreeVal>
void ObKeyBtree<BtreeKey, BtreeVal>::destroy(BtreeNode *root)
{
  ObKeyBtree *host = nullptr;
  if (OB_NOT_NULL(root) && OB_NOT_NULL(host = (ObKeyBtree *)(root->get_host()))) {
    for (int i = 0; i < root->size(); i++) {
      if (!root->is_leaf()) {
        destroy((BtreeNode *)(root->get_val(i)));
      } else {
        size_.inc(-1);
      }
    }
    host->node_allocator_.free_node(root);
  }
}
// ob_keybtree.h end

} // end common
} // end oceanbase
