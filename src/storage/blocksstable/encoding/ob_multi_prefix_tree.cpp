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

#define USING_LOG_PREFIX STORAGE

#include "ob_multi_prefix_tree.h"
#include "lib/container/ob_array_iterator.h"
#include "storage/blocksstable/ob_data_buffer.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

/*
 *  ObMultiPrefixTree::HashTable
 */
int ObMultiPrefixTree::HashTable::create(const int64_t bucket_cnt_limit,
    const int64_t hnode_cnt_limit, ObArenaAllocator *alloc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(created_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already created", K(ret));
  } else if (OB_UNLIKELY(bucket_cnt_limit <= 0)
      || OB_UNLIKELY(hnode_cnt_limit <= 0)
      || OB_UNLIKELY(bucket_cnt_limit < hnode_cnt_limit)
      || OB_ISNULL(alloc)
      || OB_UNLIKELY(0 != (bucket_cnt_limit & (bucket_cnt_limit - 1)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bucket_cnt_limit), K(hnode_cnt_limit));
  } else {
    bucket_cnt_limit_ = bucket_cnt_limit;
    hnode_cnt_limit_ = hnode_cnt_limit;
    alloc_ = alloc;
    const int64_t buckets_size = bucket_cnt_limit_ * static_cast<int64_t>(sizeof(HashNode *));
    const int64_t hnodes_size = hnode_cnt_limit_ * static_cast<int64_t>(sizeof(HashNode));
    if (OB_ISNULL(buckets_ = reinterpret_cast<HashNode **>(alloc_->alloc(buckets_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for bucket", K(ret), K(bucket_cnt_limit_));
    } else if (OB_ISNULL(hash_nodes_ = reinterpret_cast<HashNode *>(alloc_->alloc(hnodes_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for nodes", K(ret), K(hnode_cnt_limit_));
    } else {
      MEMSET(buckets_, 0, buckets_size);
      created_ = true;
    }
  }
  return ret;
}

int ObMultiPrefixTree::HashTable::build(TreeNode &tnode, const int64_t move_step)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not created", K(ret));
  } else {
    const char *str = NULL;
    const int64_t common_length = tnode.length_;
    CellList &list = tnode.cells_;
    const uint64_t mask = (bucket_cnt_limit_ - 1);
    const int64_t list_size = list.get_size();
    CellNode *cnode = list.get_first();

    for (int64_t cid = 0; OB_SUCC(ret) && cid < list_size; ++cid) {
      // make sure string has enough length
      if ((common_length + move_step) > cnode->datum_->len_ ) {
        // do nothing on this cell
        cnode = cnode->get_next();
      } else {
        str = cnode->datum_->ptr_ + common_length;
        int64_t pos = hash(str, move_step) & mask;
        HashNode *hnode = buckets_[pos];

        while(NULL != hnode) {
          if (0 == MEMCMP(hnode->step_str_, str, move_step)) {
            CellNode *cur = cnode;
            cnode = cnode->get_next();
            list.remove(cur);
            hnode->cells_.add_first(cur);
            break;
          } else if (++pos >= bucket_cnt_limit_) {
            pos = 0;
          }
          hnode = buckets_[pos];
        }
        if (NULL == hnode) {
          if (size_ == hnode_cnt_limit_) {
            // abort
            ret = OB_DATA_OUT_OF_RANGE;
          } else {
            hnode = &hash_nodes_[size_];
            hnode->reset();
            hnode->step_str_ = str;

            CellNode *cur = cnode;
            cnode = cnode->get_next();
            list.remove(cur);
            hnode->cells_.add_first(cur);
            ++size_;
            buckets_[pos] = hnode;
          }
        }
      }
    }
  }
  return ret;
}

/*
 *  ObMultiPrefixTree
 */
ObMultiPrefixTree::ObMultiPrefixTree() : is_inited_(false),
    alloc_(blocksstable::OB_ENCODING_LABEL_MULTI_PREFIX_TREE, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    tree_nodes_(NULL), cell_nodes_(NULL),
    tnode_cnt_(0), cnode_cnt_(0), cnode_cnt_limit_(0), ht_(),
    null_cnt_(0), nope_cnt_(0)
{
}

ObMultiPrefixTree::~ObMultiPrefixTree()
{
}

void ObMultiPrefixTree::reuse()
{
  // only need to reset last used part
  MEMSET(cell_nodes_, 0, cnode_cnt_ * sizeof(CellNode));
  // tree node will be reset when use it
  cnode_cnt_ = 0;
  tnode_cnt_ = 0;
  null_cnt_ = 0;
  nope_cnt_ = 0;
  //ht_.reuse();
}

int ObMultiPrefixTree::init(const int64_t cell_cnt, const int64_t bucket_cnt_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    const int64_t cnode_size = cell_cnt * static_cast<int64_t>(sizeof(CellNode));
    const int64_t tnode_size = MAX_TNODE_CNT * static_cast<int64_t>(sizeof(TreeNode));
    const int64_t cnode_cnt_limit = cell_cnt / CNODE_LIMIT_RATIO + 1;

    if (OB_FAIL(ht_.create(bucket_cnt_limit, cnode_cnt_limit, &alloc_))) {
      LOG_WARN("failed to create hashtable", K(ret), K(cell_cnt));
    } else if (OB_ISNULL(cell_nodes_ = reinterpret_cast<CellNode *>(alloc_.alloc(cnode_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc cell nodes", K(ret), K(cnode_size));
    } else if (OB_ISNULL(tree_nodes_ = reinterpret_cast<TreeNode *>(alloc_.alloc(tnode_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tree nodes", K(ret), K(tnode_size));
    } else {
      MEMSET(cell_nodes_, 0, cnode_size);
      cnode_cnt_ = cell_cnt;
      cnode_cnt_limit_ = cell_cnt;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMultiPrefixTree::init_root_node(const ObColDatums *col_datums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else { // col_values is checked before
    tree_nodes_[0].reset();
    for (int64_t row_id = 0; OB_SUCC(ret) && row_id < col_datums->count(); ++row_id) {
      const ObDatum &datum = col_datums->at(row_id);
      cell_nodes_[row_id].datum_ = &datum;
      if (datum.is_null()) {
        ++null_cnt_;
        cell_nodes_[row_id].len_ = -1;
      } else if (datum.is_nop()) {
        ++nope_cnt_;
        cell_nodes_[row_id].len_ = -2;
      } else if (datum.is_ext()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported extend datum type", K(ret), K(datum));
      } else {
        tree_nodes_[0].cells_.add_first(&cell_nodes_[row_id]);
      }
    }
    tnode_cnt_ = 1;
  }
  return ret;
}

int ObMultiPrefixTree::try_previous_length(const int64_t last_prefix_length, bool &stop)
{
  int ret = OB_SUCCESS;
  stop = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == last_prefix_length) {
    stop = true;
    // do nothing
  } else if (OB_FAIL(traverse_by_level(/* level = */ 1, stop, last_prefix_length))) {
    LOG_WARN("failed to traverse by level using previous length", K(ret));
  } else {
    // for try previous length (root node was split),
    // the cells in root node should be moved to a new tree node,
    // otherwise, root node migth be share the prefix with its
    // children, which will casue a bug
    TreeNode &root = tree_nodes_[0];
    if (!stop && 0 < root.cells_.get_size()) {
      if (OB_FAIL(add_node(root, root.cells_, 0))) {
        if (OB_DATA_OUT_OF_RANGE != ret) {
          LOG_WARN("failed to add new tree node", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMultiPrefixTree::build_tree(
    const ObColDatums *col_datums,
    int64_t &last_prefix_length,
    bool &suitable,
    int64_t &prefix_count,
    int64_t &prefix_length)
{
  int ret = OB_SUCCESS;
  bool stop = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(cnode_cnt_ != col_datums->count())
      || OB_UNLIKELY(0 > last_prefix_length)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(cnode_cnt),
        K(col_datums->count()), K(last_prefix_length));
  } else {

    if (OB_FAIL(init_root_node(col_datums))) {
      LOG_WARN("failed to init root node", K(ret), KP(col_datums));
    } else if (OB_FAIL(try_previous_length(last_prefix_length, stop))) {
      LOG_WARN("failed to try previous length", K(ret), KP(last_prefix_length));
    } else {
      int64_t level = stop ? 0 : 1;
      stop = false;
      // build tree level by level
      while (OB_SUCC(ret) && !stop && suitable) {
        if (OB_FAIL(traverse_by_level(level, stop))) {
          if (OB_LIKELY(OB_DATA_OUT_OF_RANGE == ret)) {
            ret = OB_SUCCESS;
            suitable = false;
          } else {
            LOG_WARN("failed to traverse level", K(ret), K(level), K(stop));
          }
        } else if (!stop) {
          ++level;
        } else if (0 == level && stop) {
          suitable = false;
        }
      }
      LOG_DEBUG("debug, multi-prefix tree", K(level), K(stop), K(suitable));
    }

    if (OB_FAIL(ret) || !suitable) {
      // do nothing
    } else if (OB_FAIL(complete_build(prefix_count, prefix_length, last_prefix_length))) {
      LOG_WARN("failed to complete build prefix tree", K(ret));
    }
  }
  return ret;
}

int ObMultiPrefixTree::traverse_by_level(const int64_t level, bool &stop,
    const int64_t move_step /* = MOVE_STEP */)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(0 > level)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(level));
  } else {
    stop = true;
    const int64_t cnt = tnode_cnt_; // tree nodes count might be changed
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      TreeNode &tnode = tree_nodes_[i];
      if (tnode.is_leaf() && tnode.is_active()) {
        tnode.set_inactive();
        ht_.reuse();
        if (OB_SUCCESS != (ret = ht_.build(tnode, move_step))) {
          if (OB_LIKELY(OB_DATA_OUT_OF_RANGE == ret)) {
            if (0 < level) {
              ret = OB_SUCCESS;
            } else {
              // not suitable
            }
          } else {
            LOG_WARN("failed to build hashtable", K(ret), K(level));
          }
        }

        if (OB_SUCC(ret)) {
          if (0 == ht_.size()) {
            LOG_DEBUG("size of hashtable is 0", K(ret), K(level));
          } else if (1 == ht_.size() && 0 == tnode.cells_.get_size()) {
            // contains the same step_str, update current tree node
            tnode.length_ += move_step;
            tnode.cells_.merge(ht_.get_nodes()[0].cells_);
            stop = false;
            tnode.set_active();
          } else {
            for (int64_t idx = 0; OB_SUCC(ret) && idx < ht_.size(); ++idx) {
              HashNode &hnode = ht_.get_nodes()[idx];
              if (hnode.cells_.get_size() >= cnode_cnt_ / SPLIT_PCT_THRESHOLD) {
                // split tree node
                if (OB_FAIL(add_node(tnode, hnode.cells_, move_step))) {
                  if (OB_DATA_OUT_OF_RANGE != ret) {
                    LOG_WARN("failed to add new tree node", K(ret));
                  }
                } else {
                  stop = false;
                }
              } else {
                // merge cells into current node
                tnode.cells_.merge(hnode.cells_);
              }
            }
          }
        }
      } else {
        // do nothing for inner tree node
      }
    }
  }
  return ret;
}

int ObMultiPrefixTree::add_node(TreeNode &par_tnode, CellList &cells, const int64_t move_step)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(MAX_TNODE_CNT == tnode_cnt_)) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_DEBUG("tnode out of range", K(ret), K_(tnode_cnt));
  } else {
    // add a new tnode
    TreeNode &new_tnode = tree_nodes_[tnode_cnt_];
    new_tnode.reset();
    ++tnode_cnt_;
    new_tnode.length_ = par_tnode.length_ + move_step;

    // add into parent node's children list
    par_tnode.children_ = &new_tnode;
    ++par_tnode.children_cnt_;

    // merge cells from hash node into new tree node
    new_tnode.cells_.merge(cells);
  }
  return ret;
}

int ObMultiPrefixTree::complete_build(int64_t &prefix_count,
    int64_t &total_prefix_length, int64_t &last_prefix_length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    prefix_count = 0;
    total_prefix_length = 0;
    last_prefix_length = INT64_MAX;
    // generate prefix for each tree node
    // traverse tree nodes from last
    for (int64_t tid = tnode_cnt_ -  1; OB_SUCC(ret) && 0 <= tid; --tid) {
      TreeNode &tnode = tree_nodes_[tid];
      if (tnode.is_leaf()) {
        if (OB_UNLIKELY(0 >= tnode.cells_.get_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tnode's cells is empty", K(tnode));
        } else {
          // directly choose the first cell as the prefix
          tnode.prefix_ = tnode.cells_.get_first()->datum_;
          tnode.ref_ = prefix_count;
          ++prefix_count;
          total_prefix_length += tnode.prefix_->len_;
          // record last prefix length, for reuse by next micro block
          if (tnode.length_ < last_prefix_length) {
            last_prefix_length = tnode.length_;
          }
          LOG_DEBUG("leaf tnode", K(tnode));
        }
      } else if (NULL != tnode.children_->prefix_) {
        // parent node choose the prefix of its first child as the prefix
        tnode.prefix_ = tnode.children_->prefix_;
        tnode.ref_ = tnode.children_->ref_;
        LOG_DEBUG("inner tnode", K(tnode));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child's prefix is NULL, unexpected", K(ret), K(tnode),
            "child", *tnode.children_);
      }
    }

    // generate final common length with prefix for each cell node
    for (int64_t tid = 0; OB_SUCC(ret) && tid < tnode_cnt_; ++tid) {
      TreeNode &tnode = tree_nodes_[tid];
      // only process the useful tree node
      if (0 < tnode.cells_.get_size() && 0 != tnode.length_) {
        const ObDatum *prefix = tnode.prefix_;
        CellNode *cnode = tnode.cells_.get_first();
        int64_t addition_len = 0;
        while (OB_SUCC(ret) && tnode.cells_.get_header() != cnode) {
          cnode->ref_ = tnode.ref_;
          addition_len = (prefix->ptr_ == cnode->datum_->ptr_) ?
              prefix->len_ - tnode.length_ :
              find_prefix(prefix->ptr_ + tnode.length_, cnode->datum_->ptr_ + tnode.length_,
              MIN(prefix->len_, cnode->len_) - tnode.length_);
          cnode->len_ = tnode.length_ + addition_len;
          LOG_DEBUG("cnode", K(*cnode));
          cnode = cnode->get_next();
        }
      }
    }
  }
  return ret;
}

OB_INLINE int64_t ObMultiPrefixTree::find_prefix(const char *left,
    const char *right, const int64_t len)
{
  int64_t pos = 0;
  for (; pos < len && *left == *right; ++pos, ++left, ++right) {
  }
  return pos;
}

ObMultiPrefixTreeFactory::ObMultiPrefixTreeFactory()
    : allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(blocksstable::OB_ENCODING_LABEL_PREFIX_TREE_FACTORY)),
      prefix_trees_()
{
  lib::ObMemAttr attr(MTL_ID(), blocksstable::OB_ENCODING_LABEL_PREFIX_TREE_FACTORY);
  allocator_.set_attr(attr);
  prefix_trees_.set_attr(attr);
}

ObMultiPrefixTreeFactory::~ObMultiPrefixTreeFactory()
{
  clear();
}

int ObMultiPrefixTreeFactory::create(const int64_t cell_cnt,
    const int64_t bucket_cnt_limit, ObMultiPrefixTree *&prefix_tree)
{
  int ret = OB_SUCCESS;
  prefix_tree = NULL;
  if (OB_UNLIKELY(0 >= cell_cnt || 0 >= bucket_cnt_limit
      || cell_cnt > bucket_cnt_limit)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(cell_cnt),
        K(bucket_cnt_limit));
  } else if (0 < prefix_trees_.count()) {
    ObMultiPrefixTree *cached_prefix_tree = prefix_trees_[prefix_trees_.count() - 1];
    if (OB_ISNULL(cached_prefix_tree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cached_prefix_tree is null", K(ret));
    } else if (OB_UNLIKELY(!cached_prefix_tree->is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cached_prefix_tree is not inited", K(ret));
    } else if (cached_prefix_tree->get_cnode_cnt_limit() >= cell_cnt
        && cached_prefix_tree->get_bucket_cnt_limit() >= bucket_cnt_limit) {
      // reuse prefix tree when needed
      prefix_tree = cached_prefix_tree;
      prefix_trees_.pop_back();
    } else {
      clear();
    }
  }
  if (OB_SUCC(ret) && NULL == prefix_tree) {
    if (OB_ISNULL(prefix_tree = allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc multi-prefix prefix_tree", K(ret));
    } else if (OB_FAIL(prefix_tree->init(cell_cnt, bucket_cnt_limit))) {
      LOG_WARN("failed to init multi-prefix prefix_tree", K(ret),
          K(cell_cnt), K(bucket_cnt_limit));
      allocator_.free(prefix_tree);
    }
  }
  return ret;
}

int ObMultiPrefixTreeFactory::recycle(ObMultiPrefixTree *prefix_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(prefix_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(prefix_tree));
  } else if (OB_UNLIKELY(!prefix_tree->is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prefix_tree is not inited", K(ret));
  } else if (OB_FAIL(prefix_trees_.push_back(prefix_tree))) {
    LOG_WARN("failed to push back prefix_tree", K(ret));
    allocator_.free(prefix_tree);
  }
  return ret;
}

void ObMultiPrefixTreeFactory::clear()
{
  FOREACH(prefix_tree, prefix_trees_) {
    allocator_.free(*prefix_tree);
  }
  prefix_trees_.reuse();
}

} // end namespace blocksstable
} // end namespace oceanbase
