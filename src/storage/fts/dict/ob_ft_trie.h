/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_TRIE_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_TRIE_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/list/ob_list.h"
#include "lib/ob_errno.h"
#include "storage/fts/dict/ob_ft_dict_def.h"

namespace oceanbase
{
namespace storage
{
template <typename DATA_TYPE>
struct ObFTTrieNodeData
{
  DATA_TYPE data_;
};

template <>
struct ObFTTrieNodeData<void>
{
};

template <typename DATA_TYPE = void>
struct ObFTTrieNode
{
public:
  ObFTTrieNode(ObIAllocator &alloc) : alloc_(alloc), children_(nullptr) {}

  ObFTSingleWord word_;
  bool is_leaf_ = false;

  struct DATBuildInfo
  {
    int32_t level_ = 0;
    uint32_t state_index_ = 0;
    ObFTWordCode code_ = 0;
    ObFTWordCode min_child_ = 0;
    ObFTWordCode max_child_ = 0;
  } dat_build_info_;

  ObFTTrieNodeData<DATA_TYPE> data_;

  struct NodeIndex
  {
    ObFTSingleWord word_;
    ObFTTrieNode<DATA_TYPE> *child_;
  };

  bool is_empty() { return (OB_ISNULL(children_) || children_->size() == 0); }

  int add_children(const NodeIndex &node)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(children_)) {
      if (OB_ISNULL(children_ = static_cast<ListType *>(
                        alloc_.alloc(sizeof(ObList<NodeIndex, ObIAllocator>))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_FTS_LOG(WARN, "Failed to alloc children vector", K(ret));
      } else {
        new (children_) ObList<NodeIndex, ObIAllocator>(alloc_);
      }
    } else {
    }

    if (OB_SUCC(ret) && OB_FAIL(children_->push_back(node))) {
      STORAGE_FTS_LOG(WARN, "Failed to push back child", K(ret));
    }
    return ret;
  }

  typedef ObList<NodeIndex, ObIAllocator> ListType;

  ObIAllocator &alloc_;
  ObList<NodeIndex, ObIAllocator> *children_;
};

/**  ObFTrie: Tmp memory structure to build the dictionary into kv_cache.
 */
template <typename DATA_TYPE = void>
class ObFTTrie final
{
public:
  ObFTTrie(ObIAllocator &allocator, ObCollationType collation_type)
      : allocator_(allocator), collation_type_(collation_type), root_(allocator), node_num_(0),
        level_statistics_()
  {
  }

  int insert(const ObString &words, const ObFTTrieNodeData<DATA_TYPE> &data);

  ObFTTrieNode<DATA_TYPE> *root() { return &root_; }

  size_t node_num() const { return node_num_; }

  int get_start_word(ObFTSingleWord &start_word) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(root_.children_) || root_.children_->empty()) {
      return OB_NOT_INIT;
    } else {
      start_word = root_.children_->get_first().word_;
    }
    return ret;
  }

  int get_end_word(ObFTSingleWord &end_word) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(root_.children_) || root_.children_->empty()) {
      return OB_NOT_INIT;
    } else {
      end_word = root_.children_->get_last().word_;
    }
    return ret;
  }

private:
  ObIAllocator &allocator_;
  ObCollationType collation_type_; // for trie type
  ObFTTrieNode<DATA_TYPE> root_;

  size_t node_num_;
  uint32_t level_statistics_[64];
};

extern template class ObFTTrie<void>;

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_TRIE_H_
