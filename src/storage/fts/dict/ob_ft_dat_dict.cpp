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

#include "storage/fts/dict/ob_ft_dat_dict.h"

#include "lib/alloc/alloc_assist.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/list/ob_list.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/fts/dict/ob_ft_trie.h"
namespace oceanbase
{
namespace storage
{
template <typename DATA_TYPE>
int ObFTDATBuilder<DATA_TYPE>::init(ObFTTrie<DATA_TYPE> &trie)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Builder has already inited", K(ret));
  } else {
    size_t map_size = ObArrayHashMap::calc_memory_size(trie.node_num());
    size_t array_size = trie.node_num() * 6; // by experience.
    size_t base_size = array_size * sizeof(int32_t);
    size_t check_size = base_size;
    size_t total_size = sizeof(ObFTDAT) + map_size + base_size + check_size;

    if (OB_ISNULL(dat_ = static_cast<ObFTDAT *>(alloc_.alloc(total_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc dat mem block", K(ret));
    } else {
      memset(dat_, 0, total_size);
      dat_->mem_block_size_ = total_size;
      dat_->array_size_ = array_size;
      dat_->base_offset_ = map_size;
      dat_->check_offset_ = dat_->base_offset_ + base_size;
      map_ = dat_->get_map();
      if (OB_FAIL(map_->init(trie.node_num()))) {
        LOG_WARN("fail to init map", K(ret));
      }
    }
  }
  return ret;
}

template <typename DATA_TYPE>
int ObFTDATBuilder<DATA_TYPE>::build_from_trie(ObFTTrie<DATA_TYPE> &trie)
{
  typedef typename ObFTTrieNode<DATA_TYPE>::NodeIndex NodeIndex;

  int ret = OB_SUCCESS;
  if (OB_ISNULL(dat_) || OB_ISNULL(dat_->buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dat_ is null", K(ret));
  } else {
    int32_t *base = reinterpret_cast<int32_t *>(dat_->buff + dat_->base_offset_);
    int32_t *check = reinterpret_cast<int32_t *>(dat_->buff + dat_->check_offset_);
    base[ObFTDAT::FIRST_INDEX] = 1;
    ObArenaAllocator alloc(lib::ObMemAttr(MTL_ID(), "Tmp dfs"));
    ObList<ObFTTrieNode<DATA_TYPE> *, ObIAllocator> dfs_queue(alloc);

    // Start to build the DAT.
    trie.root()->dat_build_info_.state_index_ = ObFTDAT::FIRST_BASE;
    dfs_queue.push_back(trie.root());

    int next_base_offset = 0;
    int level = 1;

    while (OB_SUCC(ret) && !dfs_queue.empty()) {
      ObFTTrieNode<DATA_TYPE> *node = dfs_queue.get_first();
      dfs_queue.pop_front();

      int rootIdx = node->dat_build_info_.state_index_;

      // mark as leaf
      if (node->is_leaf_) {
        base[rootIdx] = -1;
      }

      if (level != node->dat_build_info_.level_) {
        level = node->dat_build_info_.level_;
        next_base_offset = 0;
      }

      if (node->is_empty()) {
        // nothing
      } else {
        int try_base = 1;
        try_base += next_base_offset;
        int start = try_base;
        bool need_code = true;
        int try_step = 1;

        for (; try_base < dat_->array_size_; try_base += try_step) {
          bool conflict = false;

          if (node->is_empty()) {
          } else {
            for (typename ObList<NodeIndex, ObIAllocator>::iterator iter = node->children_->begin();
                 iter != node->children_->end();
                 ++iter) {
              ObFTTokenCode child_code;
              NodeIndex &child_index = *iter;
              if (need_code) {
                if (OB_FAIL(encode(child_index.token_.get_token(), child_code))) {
                  LOG_WARN("Failed to encode");
                  break;
                }
                child_index.child_->dat_build_info_.code_ = child_code;
              }
              child_code = child_index.child_->dat_build_info_.code_;
              if (check[try_base + child_code] != 0) {
                conflict = true;
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else {
            need_code = false;

            if (!conflict) {
              if (node->is_leaf_) {
                base[rootIdx] = -try_base;
              } else {
                base[rootIdx] = try_base;
              }
              break;
            }
            try_step = MAX(1, (try_base - start) / 20);
          }
        }

        if (OB_FAIL(ret) || try_base >= dat_->array_size_) {
          break;
        } else {
          // try_average_.add(try_base + 1 - start);
          if (try_base - start > 100) {
            next_base_offset += try_base - start;
          }

          if (node->is_empty()) {
          } else {
            for (typename ObList<NodeIndex, ObIAllocator>::iterator iter = node->children_->begin();
                 iter != node->children_->end();
                 iter++) {
              typename ObFTTrieNode<DATA_TYPE>::NodeIndex &child_index = *iter;
              int my_index = abs(try_base) + child_index.child_->dat_build_info_.code_;
              // max_used = MAX(max_used, my_index);

              if (my_index >= dat_->array_size_) {
                expand();
                base = reinterpret_cast<int32_t *>(dat_->buff + dat_->base_offset_);
                check = reinterpret_cast<int32_t *>(dat_->buff + dat_->check_offset_);
              }
              child_index.child_->dat_build_info_.state_index_ = my_index;
              check[my_index] = rootIdx;
              dfs_queue.push_back(child_index.child_);
            }
          }
        }
      }

    } // while

    if (OB_FAIL(ret)) {
      // already logged
    } else if (OB_FAIL(trie.get_start_token(dat_->start_token_))) {
      LOG_WARN("fail to get start token", K(ret));
    } else if (OB_FAIL(trie.get_end_token(dat_->end_token_))) {
      LOG_WARN("fail to get end token", K(ret));
    }
    LOG_INFO("finish build ft dat", K(dat_->start_token_.get_token()), K(dat_->end_token_.get_token()));

    dfs_queue.reset();
    alloc.reset();
  }

  return ret;
}

template <typename DATA_TYPE>
int ObFTDATBuilder<DATA_TYPE>::expand()
{
  int ret = OB_SUCCESS;
  size_t array_size = dat_->array_size_;
  size_t base_inc = array_size * sizeof(int32_t);
  size_t check_inc = array_size * sizeof(int32_t);
  size_t buffer_size = dat_->mem_block_size_ + base_inc + check_inc;

  ObFTDAT *new_dat = nullptr;
  if (OB_ISNULL(new_dat = static_cast<ObFTDAT *>(alloc_.alloc(buffer_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    memset(new_dat, 0, buffer_size);
    new_dat->mem_block_size_ = buffer_size;
    new_dat->array_size_ = array_size * 2;
    new_dat->base_offset_ = dat_->base_offset_;
    new_dat->check_offset_ = dat_->check_offset_ + base_inc;
    // map and base
    MEMCPY(new_dat->buff, dat_->buff, dat_->check_offset_ - 0);
    // check
    size_t check_size = array_size * sizeof(int32_t);
    MEMCPY(new_dat->buff + new_dat->check_offset_, dat_->buff + dat_->check_offset_, check_size);
    alloc_.free(dat_);
    dat_ = new_dat;
    map_ = dat_->get_map();
  }
  return ret;
}

template <typename DATA_TYPE>
int ObFTDATBuilder<DATA_TYPE>::encode(const ObString &single_token, ObFTTokenCode &code)
{
  int ret = OB_SUCCESS;
  ret = map_->find(single_token, code);
  if (OB_ENTRY_NOT_EXIST == ret) {
    code = next_code_++;
    ret = map_->insert(single_token, code);
    if (OB_SUCCESS != ret) {
      LOG_WARN("fail to insert token code", K(ret));
    }
  }
  return ret;
}

template <typename DATA_TYPE>
int ObFTDATReader<DATA_TYPE>::match_with_hit(const ObString &ft_char,
                                             const ObDATrieHit &last_hit,
                                             ObDATrieHit &hit) const
{
  int ret = OB_SUCCESS;
  ObFTTokenCode code;
  hit.set_unmatch();
  int32_t *base = reinterpret_cast<int32_t *>(dat_->buff + dat_->base_offset_);
  int32_t *check = reinterpret_cast<int32_t *>(dat_->buff + dat_->check_offset_);

  if (OB_FAIL(map_->find(ft_char, code)) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("fail to find token code", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    hit.set_unmatch();
  } else {
    if (&last_hit == &hit) {
      // sameone
    } else {
      hit = last_hit;
    }
    if (0 == hit.base_idx_) {
      hit.base_idx_ = 1; // from current to do check;
    }
    // from current place to do check
    int32_t idx = abs(base[hit.base_idx_]) + code;

    if (check[idx] != hit.base_idx_) {
      hit.set_unmatch();
    } else if (base[idx] < 0) {
      hit.base_idx_ = idx;
      hit.end_pos_ = hit.end_pos_ + ft_char.length();
      hit.char_cnt_++;
      hit.set_match();
      hit.set_prefix(); // to be checked next time
    } else {
      hit.base_idx_ = idx;
      hit.end_pos_ = hit.end_pos_ + ft_char.length();
      hit.char_cnt_++;
      hit.set_prefix();
    }
  }
  return ret;
}

template class ObFTDATBuilder<void>;
template class ObFTDATReader<void>;

ObArrayHashMap *ObFTDAT::get_map() { return reinterpret_cast<ObArrayHashMap *>(buff); }

int ObArrayHashMap::find(const ObString &token, ObFTTokenCode &code) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  uint64_t hash = token.hash();
  uint64_t idx = hash & header_.locator_;
  while (header_.data[idx].used) {
    if (header_.data[idx].token.get_token() == token) {
      code = header_.data[idx].code;
      ret = OB_SUCCESS;
      break;
    } else {
      idx = (idx + 3) & header_.locator_;
    }
  }
  return ret;
}

int ObArrayHashMap::insert(const ObString &token, ObFTTokenCode code)
{
  int ret = OB_SUCCESS;
  uint64_t hash = token.hash();
  uint64_t index = hash & header_.locator_;
  while (header_.data[index].used) {
    index = (index + 3) & header_.locator_;
  }
  header_.data[index].code = code;
  header_.data[index].token.set_token(token.ptr(), token.length());
  header_.data[index].used = true;
  header_.count_++;
  return ret;
}

int ObArrayHashMap::init(size_t token_cnt)
{
  int ret = OB_SUCCESS;
  size_t capacity = calc_capacity(token_cnt);
  size_t size = calc_memory_size(token_cnt);
  memset(this, 0, size);
  header_.buffer_size_ = size;
  header_.capacity_ = capacity;
  header_.count_ = 0;
  header_.locator_ = capacity - 1;
  return OB_SUCCESS;
}

size_t ObArrayHashMap::calc_memory_size(size_t token_cnt)
{
  size_t capacity = calc_capacity(token_cnt);
  size_t size = sizeof(Header) + capacity * sizeof(Entry);
  return size;
}

size_t ObArrayHashMap::calc_capacity(size_t token_cnt)
{
  const int64_t capacity_power = 64 - __builtin_clzll(static_cast<size_t>(token_cnt + 1));
  return static_cast<size_t>(1) << (capacity_power + 1);
}

} //  namespace storage
} //  namespace oceanbase
