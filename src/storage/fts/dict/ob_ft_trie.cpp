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

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_trie.h"

#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{
template <typename DATA_TYPE>
int ObFTTrie<DATA_TYPE>::insert(const ObString &words, const ObFTTrieNodeData<DATA_TYPE> &data)
{
  int ret = OB_SUCCESS;
  ObString::obstr_size_t offset = 0;
  ObFTTrieNode<DATA_TYPE> *node_ptr = &root_;

  int level = 0;
  while (OB_SUCC(ret) && offset < words.length()) {
    int64_t char_len = 0;
    level++; // count from level 1
    if (OB_FAIL(ObCharset::first_valid_char(collation_type_,
                                            words.ptr() + offset,
                                            words.length() - offset,
                                            char_len))) {
      LOG_WARN("fail to get first valid char", K(ret));
    } else {
      ObString current_char(char_len, words.ptr() + offset);
      bool isNewNode = false;
      // once new, every child new, and children is ordered.
      if (isNewNode || node_ptr->is_empty()
          || node_ptr->children_->last()->word_.get_word() != current_char) {
        isNewNode = true; // anyway new node
        ObFTTrieNode<DATA_TYPE> *new_child = static_cast<ObFTTrieNode<DATA_TYPE> *>(
            allocator_.alloc(sizeof(ObFTTrieNode<DATA_TYPE>)));
        if (OB_ISNULL(new_child)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          new (new_child) ObFTTrieNode<DATA_TYPE>(allocator_);
          level_statistics_[level]++;
          new_child->dat_build_info_.level_ = level;
          new_child->is_leaf_ = (offset + char_len == words.length());
          if (OB_FAIL(new_child->word_.set_word(current_char.ptr(), current_char.length()))) {
            LOG_WARN("Failed to set new child", K(ret));
          } else {
            typename ObFTTrieNode<DATA_TYPE>::NodeIndex child_index;
            ObString word = new_child->word_.get_word();
            child_index.word_.set_word(word.ptr(), word.length());
            child_index.child_ = new_child;
            if (OB_FAIL(node_ptr->add_children(child_index))) {
              LOG_WARN("Failed to add children.", K(ret));
            } else {
              node_ptr = new_child;
              node_num_++;
            }
          }
        }

      } else {
        // no new node, just go to next level, always from behind
        node_ptr = node_ptr->children_->last()->child_;
      }
      offset += char_len;
    }
  }
  if (OB_SUCC(ret)) {
    node_ptr->is_leaf_ = true;
  }
  return ret;
}

template class ObFTTrie<void>;

} //  namespace storage
} //  namespace oceanbase
