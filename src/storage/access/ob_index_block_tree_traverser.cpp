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

#include "storage/access/ob_index_block_tree_traverser.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

int ObIndexBlockTreeTraverser::init(blocksstable::ObSSTable &sstable, const ObITableReadInfo &index_read_info, ObTablet *tablet, const ObLabel &label)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice for index block tree traverser", KR(ret));
  } else if (OB_FAIL(allocator_.init(nullptr, OB_MALLOC_NORMAL_BLOCK_SIZE, ObMemAttr(MTL_ID(), label)))) {
    LOG_WARN("Fail to init fifo allocator for index block tree traverser", KR(ret), K(label));
  } else if (OB_FAIL(path_caches_.init())) {
    LOG_WARN("Fail to init path caches", KR(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid() || !index_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for index block tree traverser",
             KR(ret),
             K(sstable),
             K(index_read_info));
  } else {
    sstable_ = &sstable;
    read_info_ = &index_read_info;
    context_ = nullptr;
    visited_node_count_ = 0;
    visited_macro_node_count_ = 0;
    tablet_ = tablet;
    is_inited_ = true;
  }

  return ret;
}

void ObIndexBlockTreeTraverser::reuse()
{
  // Don't call allocator_->reuse()
  // This structure is reused multiple times to access Index Block Tree of the same sstable.
  // Not calling reuse ensures that micro blocks accesses from last traverse remain in memory, speeding up subsequent accesses.
  visited_node_count_ = 0;
  visited_macro_node_count_ = 0;
}

int ObIndexBlockTreeTraverser::handle_overflow_ranges()
{
  int ret = OB_SUCCESS;

  // overflow ranges are out of index block tree, just call next range
  while (OB_SUCC(ret) && !context_->is_ended()) {
    if (OB_FAIL(context_->next_range())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Fail to go to next range", KR(ret), KPC(context_));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::traverse(ObIIndexTreeTraverserContext &context)
{
  int ret = OB_SUCCESS;

  blocksstable::ObMicroBlockData root_index_block;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init traverser", KR(ret));
  } else if (OB_UNLIKELY(!context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid context", KR(ret), K(context));
  } else if (FALSE_IT(context_ = &context)) {
  } else if (sstable_->is_empty()) {
    // skip
  } else if (OB_FAIL(sstable_->get_index_tree_root(root_index_block))) {
    LOG_WARN("Fail to get index tree root", KR(ret), KPC(context_));
  } else if (OB_FAIL(path_caches_.load_root(root_index_block, *this))) {
    LOG_WARN("Fail to load root node to path caches", KR(ret));
  } else if (OB_FAIL(inner_node_traverse(nullptr, PathInfo(), 0))) {
    LOG_WARN("Fail to traverse index block tree", KR(ret), K(root_index_block), KPC(context_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(handle_overflow_ranges())) {
    LOG_WARN("Fail to handle overflow ranges", KR(ret), KPC(context_));
  }

  LOG_TRACE("Traverse index block tree, stats:", K(visited_node_count_), K(visited_macro_node_count_));

  return ret;
}

int ObIndexBlockTreeTraverser::is_range_cover_node_end_key(const blocksstable::ObDatumRange &range,
                                                           const blocksstable::ObMicroIndexInfo &micro_index_info,
                                                           bool &is_over)
{
  int ret = OB_SUCCESS;

  int endkey_cmp_ret = 0;

  // Don't compare multi version column
  // cmp_ret < 0         cmp_ret == 0 && range.is_right_closed()
  // node:  (.., 50]     (..., 100]
  // range: (0, 100)     (0  , 100]
  if (OB_FAIL(micro_index_info.endkey_.compare(range.get_end_key(),
                                               read_info_->get_datum_utils(),
                                               endkey_cmp_ret,
                                               /* compare_datum_cnt */ false))) {
    LOG_WARN("Fail to compare endkey", KR(ret), K(range), KPC(read_info_));
  } else {
    is_over = endkey_cmp_ret < 0 || (endkey_cmp_ret == 0 && range.is_right_closed());
  }

  return ret;
}

int ObIndexBlockTreeTraverser::goto_next_level_node(const blocksstable::ObMicroIndexInfo &micro_index_info,
                                                    const bool is_coverd_by_range,
                                                    const PathInfo &path_info,
                                                    const int64_t level)
{
  int ret = OB_SUCCESS;

  bool should_estimate = false;

  if (context_->is_never_estimate()) {
    // should_estimate = false;
  } else if (micro_index_info.is_data_block() && micro_index_info.get_row_count() <= OPEN_DATA_MICRO_BLOCK_ROW_LIMIT) {
    should_estimate = true;
  } else if (level >= PathNodeCaches::DEFAULT_BUFFER_CACHE_LEVEL) {
    should_estimate = true;
  }

  if (OB_FAIL(ret)) {
  } else if (should_estimate) {
    if (OB_FAIL(context_->on_node_estimate(micro_index_info, is_coverd_by_range))) {
      LOG_WARN("Fail to do context on leaf node estimate", KR(ret), K(micro_index_info));
    }
  } else {
    if (micro_index_info.is_leaf_block()) {
      visited_macro_node_count_++;
    }
    visited_node_count_++;

    if (OB_FAIL(micro_index_info.is_data_block()
                    ? leaf_node_traverse(micro_index_info, path_info, level)
                    : inner_node_traverse(&micro_index_info, path_info, level))) {
      LOG_WARN("Fail to traverse next level node", KR(ret));
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::leaf_node_traverse(const blocksstable::ObMicroIndexInfo &micro_index_info,
                                                  const PathInfo &path_info,
                                                  const int64_t level)
{
  int ret = OB_SUCCESS;

  PathNodeCaches::CacheNode *cache_node = nullptr;
  bool is_in_cache = false;
  ObMicroBlockRowScanner scanner(allocator_);
  const blocksstable::ObDatumRange &range = context_->get_curr_range();
  ObPartitionEst est;

  if (OB_FAIL(path_caches_.get(level, &micro_index_info, *this, cache_node, is_in_cache))) {
    LOG_WARN("Fail to get node in path caches", KR(ret), K(micro_index_info));
  } else if (OB_FAIL(scanner.estimate_row_count(*read_info_, cache_node->micro_data_, range, context_->consider_multi_version(), est))) {
    LOG_WARN("Fail to estimate row count", KR(ret), K(cache_node->micro_data_), KPC(context_));
  } else if (OB_FAIL(context_->on_leaf_node(micro_index_info, est))) {
    LOG_WARN("Fail to do context on leaf node", KR(ret), K(micro_index_info), KPC(context_));
  }

  return ret;
}

int ObIndexBlockTreeTraverser::inner_node_traverse(const blocksstable::ObMicroIndexInfo *micro_index_info,
                                                   const PathInfo &path_info,
                                                   const int64_t level)
{
  int ret = OB_SUCCESS;

  PathNodeCaches::CacheNode *cache_node = nullptr;
  TreeNodeContext *node = nullptr;
  blocksstable::ObMicroIndexInfo index_row;
  bool is_in_cache = false;

  if (OB_FAIL(path_caches_.get(level, micro_index_info, *this, cache_node, is_in_cache))) {
    LOG_WARN("Fail to get node in path caches", KR(ret), KPC(micro_index_info));
  } else {
    node = &cache_node->context_;
  }

  while (OB_SUCC(ret) && !context_->is_ended()) {
    const blocksstable::ObDatumRange &range = context_->get_curr_range();
    int64_t index_row_count = 0;

    // step 1. locate range in multi index row
    if (OB_FAIL(node->locate_range(range, index_row_count))) {
      if (ret != OB_BEYOND_THE_RANGE) {
        LOG_WARN("Fail to locate range", KR(ret));
      } else {
        // beyond the range, the remain ranges are all larger than node
        ret = OB_SUCCESS;
        break;
      }
    }

    // The path_info indicates which child of the parent node the current node is in (within the
    // interval being traverse). It could be the leftmost child, a middle child, or the rightmost
    // child. See the figure below.
    //                              start_key end_key
    //  C: coverd                       │       │
    //  N: partial coverage             ▼       ▼
    //                                  ─────────
    //                                 ┌─┬─┬─┬─┬─┐
    //                                 │N│C│C│C│N│
    //                                 │ │ │ │ │ │
    //                                 └┬┴─┴┬┴─┴┬┘
    //             ┌────────────────────┘   │   └──────────────────┐
    // left_most◄──┤                        ├──►in_middle          ├──►right_most
    //          ┌─┬┴┬─┐                  ┌─┬┴┬─┐                ┌─┬┴┬─┐
    //          │ │N│C│                  │C│C│C│                │C│C│N│
    //          │ │ │ ├──┐               │ │ │ │                │ │ │ │
    //          └─┴─┴─┘  │               └─┴─┴─┘                └─┴─┴─┘
    //             ───   │                                    i: 0 1 2
    //                   ▼
    //                still_estimating

    // step 2. iterate the index row to sum row count
    for (int64_t i = 0; OB_SUCC(ret) && i < index_row_count; i++) {
      if (OB_FAIL(node->get_next_index_row(index_row))) {
        LOG_WARN("Fail to get next index row", KR(ret), K(i), K(index_row_count));
      }

      // we can combine path_info information and compare endkey to check whether current range is
      // coverd the index row.
      bool is_coverd_by_range = false;
      PathInfo next_level_node_path_info(index_row_count, i, path_info);
      if (OB_FAIL(ret)) {
      } else if (next_level_node_path_info.in_middle_) {
        is_coverd_by_range = true;
      } else if (next_level_node_path_info.left_most_ && !next_level_node_path_info.right_most_) {
        is_coverd_by_range = range.get_start_key().is_min_rowkey();
      } else if (next_level_node_path_info.right_most_ && !next_level_node_path_info.left_most_) {
        if (range.get_end_key().is_max_rowkey()) {
          is_coverd_by_range = true;
        } else if (OB_FAIL(is_range_cover_node_end_key(range, index_row, is_coverd_by_range))) {
          LOG_WARN("Fail to compare range endkey and index row", KR(ret));
        }
      } else if (next_level_node_path_info.right_most_ && next_level_node_path_info.left_most_) {
        // locate range only hit one node, only when range is the whole range, this index-row is coverd by range
        is_coverd_by_range = range.get_start_key().is_min_rowkey() && range.get_end_key().is_max_rowkey();
      }

      ObIIndexTreeTraverserContext::ObTraverserOperationType operation;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(context_->on_inner_node(index_row, is_coverd_by_range, operation))) {
        LOG_WARN("Fail to do on inner node", KR(ret), K(is_coverd_by_range), K(index_row), KPC(context_));
      } else {
        switch (operation) {
        case ObIIndexTreeTraverserContext::GOTO_NEXT_LEVEL:
          if (OB_FAIL(goto_next_level_node(index_row, is_coverd_by_range, PathInfo(index_row_count, i, path_info), level + 1))) {
            LOG_WARN("Fail to goto next level", KR(ret), K(index_row));
          }
          break;
        case ObIIndexTreeTraverserContext::NOTHING:
          break;
        default:
          // do nothing
          break;
        };
      }
    }

    // step 3. continue to traverse next range
    if (OB_FAIL(ret)) {
    } else if (micro_index_info != nullptr) {
      // only root node can call next range
      break;
    } else if (OB_FAIL(context_->next_range())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Fail to advance context", KR(ret), KPC(context_));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("PathNodeCaches init twice", KR(ret), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < DEFAULT_BUFFER_CACHE_LEVEL; i++) {
      if (OB_FAIL(caches_.push_back(&buffer_caches_[i]))) {
        LOG_WARN("Fail to push back cache node", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::load_root(const blocksstable::ObMicroBlockData &block_data,
                                                         ObIndexBlockTreeTraverser &traverser)
{
  int ret = OB_SUCCESS;

  static_assert(DEFAULT_BUFFER_CACHE_LEVEL >= 1, "DEFAULT_BUFFER_CACHE_LEVEL must be larger or equal to 1");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("PathNodeCaches not init", KR(ret), KPC(this));
  } else if (caches_[0]->is_inited_) {
    // is inited, skip
  } else if (FALSE_IT(caches_[0]->micro_data_ = block_data)) {
  } else if (OB_FAIL(caches_[0]->context_.init(block_data, nullptr, traverser))) {
    LOG_WARN("Fail to init root node context", KR(ret), K(block_data));
  } else {
    caches_[0]->is_inited_ = true;
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::build_and_push_new_cache(const int64_t expect_access_idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(expect_access_idx != caches_.count())) {
    // we are iterating level one by one, so the caches count should be equal to the expect access index
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected caches count", KR(ret), K(expect_access_idx), K(caches_.count()));
  } else {
    CacheNode *new_cache_node = nullptr;
    if (OB_ISNULL(new_cache_node = static_cast<CacheNode *>(allocator_.alloc(sizeof(CacheNode))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for new cache node", KR(ret));
    } else {
      new (new_cache_node) CacheNode();
      if (OB_FAIL(caches_.push_back(new_cache_node))) {
        LOG_WARN("Fail to push back new cache node", KR(ret));
      }
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::find(const int64_t level,
                                                    const blocksstable::ObMicroIndexInfo *micro_index_info,
                                                    bool &is_in_cache)
{
  int ret = OB_SUCCESS;

  is_in_cache = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("PathNodeCaches not init", KR(ret), KPC(this));
  } else if (OB_FAIL(ensure_safe_access(level))) {
    LOG_WARN("Fail to ensure safe access", KR(ret), K(level));
  } else if (!caches_[level]->is_inited_) {
    is_in_cache = false;
  } else if (micro_index_info == nullptr) {
    // root node
    is_in_cache = caches_[0]->is_inited_;
  } else {
    blocksstable::ObMicroBlockCacheKey key(MTL_ID(), *micro_index_info);
    is_in_cache = (key == caches_[level]->key_);
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::get(const int64_t level,
                                                   const blocksstable::ObMicroIndexInfo *micro_index_info,
                                                   ObIndexBlockTreeTraverser &traverser,
                                                   CacheNode *&cache_node,
                                                   bool &is_in_cache)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(find(level, micro_index_info, is_in_cache))) {
    LOG_WARN("Fail to find node in cache", KR(ret), KPC(micro_index_info));
  } else if (is_in_cache) {
    // find function below will ensure safe access
    cache_node = caches_[level];
  } else {
    // not in cache, should load it
    cache_node = caches_[level];

    if (micro_index_info == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Must load root node at first", KR(ret), K(level));
    } else if (cache_node->is_inited_) {
      cache_node->reuse();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prefetch(*micro_index_info, cache_node->micro_handle_))) {
      LOG_WARN("Fail to prefetch next level micro block data", KR(ret), K(micro_index_info));
    } else if (OB_FAIL(cache_node->micro_handle_.get_micro_block_data(
                  &cache_node->macro_block_reader_, cache_node->micro_data_, micro_index_info->is_data_block()))) {
      LOG_WARN("Fail to get index block data", KR(ret));
    } else if (!micro_index_info->is_data_block()
               && OB_FAIL(
                   cache_node->is_inited_
                       ? cache_node->context_.open(cache_node->micro_data_, micro_index_info, traverser)
                       : cache_node->context_.init(cache_node->micro_data_, micro_index_info, traverser))) {
      LOG_WARN("Fail to init root node context", KR(ret), K(cache_node->micro_data_));
    } else if (OB_FAIL(
                   cache_node->key_.assign(blocksstable::ObMicroBlockCacheKey(MTL_ID(), *micro_index_info)))) {
      LOG_WARN("Fail to assign cache key", KR(ret), KPC(micro_index_info));
    } else {
      cache_node->is_inited_ = true;
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::prefetch(const blocksstable::ObMicroIndexInfo &micro_index_info,
                                                        ObMicroBlockDataHandle &micro_handle)
{
  int ret = OB_SUCCESS;

  bool found = false;
  micro_handle.reset();
  micro_handle.allocator_ = &allocator_;

  ObMicroBlockCacheKey key(MTL_ID(), micro_index_info);
  ObIMicroBlockCache *cache
      = micro_index_info.is_data_block()
            ? &ObStorageCacheSuite::get_instance().get_block_cache()
            : &ObStorageCacheSuite::get_instance().get_index_block_cache();

  if (OB_ISNULL(cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to get block cache because of null", KR(ret), KP(cache));
  } else if (OB_FAIL(cache->get_cache_block(key, micro_handle.cache_handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("Fail to get cache block", KR(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    found = true;
    micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
  }

  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(micro_index_info.row_header_->fill_micro_des_meta(true /* deep_copy_key */,
                                                                  micro_handle.des_meta_))) {
      LOG_WARN("Fail to fill micro block deserialize meta", KR(ret));
    } else if (OB_FAIL(cache->prefetch(MTL_ID(),
                                       micro_index_info.get_macro_id(),
                                       micro_index_info,
                                       /* use_cache */ true,
                                       common::ObTabletID(common::ObTabletID::INVALID_TABLET_ID),
                                       micro_handle.io_handle_,
                                       &allocator_))) {
      LOG_WARN("Fail to prefetch data micro block", KR(ret), K(micro_index_info));
    } else if (ObSSTableMicroBlockState::UNKNOWN_STATE == micro_handle.block_state_) {
      micro_handle.tenant_id_ = MTL_ID();
      micro_handle.macro_block_id_ = micro_index_info.get_macro_id();
      micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
      micro_handle.micro_info_.set(micro_index_info.get_block_offset(),
                                   micro_index_info.get_block_size(),
                                   micro_index_info.get_logic_micro_id(),
                                   micro_index_info.get_data_checksum());
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::TreeNodeContext::init(const ObMicroBlockData &block_data,
                                                     const ObMicroIndexInfo *micro_index_info,
                                                     ObIndexBlockTreeTraverser &traverser)
{
  int ret = OB_SUCCESS;

  common::ObQueryFlag query_flag(common::ObQueryFlag::Forward,
                         false,  /* is daily merge scan*/
                         false,  /* is read multiple macro block*/
                         false,  /* sys task scan, read one macro block in single io*/
                         false,  /* is full row scan*/
                         false,  /* index back */
                         false); /* query stat */

  if (OB_UNLIKELY(!traverser.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Invalid traverser", KR(ret));
  } else if (OB_FAIL(scanner_.init(traverser.get_read_info()->get_datum_utils(),
                                   traverser.get_allocator(),
                                   query_flag,
                                   traverser.get_sstable()->get_macro_offset()))) {
    LOG_WARN("Fail to init scanner", KR(ret), K(traverser));
  } else if (traverser.get_sstable() != nullptr
             && traverser.get_sstable()->is_ddl_merge_sstable()
             && traverser.get_tablet() != nullptr
             && OB_FALSE_IT(scanner_.set_iter_param(traverser.get_sstable(), traverser.get_tablet()))) {
  } else if (OB_FAIL(open(block_data, micro_index_info, traverser))) {
    LOG_WARN("Fail to open index block", KR(ret), K(scanner_));
  }

  return ret;
}

int ObIndexBlockTreeTraverser::TreeNodeContext::open(const blocksstable::ObMicroBlockData &block_data,
                                                     const blocksstable::ObMicroIndexInfo *micro_index_info,
                                                     const ObIndexBlockTreeTraverser &traverser)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(scanner_.open(micro_index_info == nullptr
                                ? blocksstable::ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID
                                : micro_index_info->get_macro_id(),
                            block_data))) {
    LOG_WARN("Fail to open index block", KR(ret), K(scanner_));
  }

  return ret;
}

void ObIndexBlockTreeTraverser::TreeNodeContext::reuse()
{
  scanner_.reuse();
}

int ObIndexBlockTreeTraverser::TreeNodeContext::locate_range(const blocksstable::ObDatumRange &range,
                                                             int64_t &index_row_count)
{
  int ret = OB_SUCCESS;

  index_row_count = 0;

  if (OB_FAIL(scanner_.locate_range(range, true, true))) {
    if (ret != OB_BEYOND_THE_RANGE) {
      LOG_WARN("Fail to locate range", KR(ret), K(range));
    }
  } else if (OB_FAIL(scanner_.get_index_row_count(index_row_count))) {
    LOG_WARN("Fail to get index row count", KR(ret), K(scanner_));
  }

  return ret;
}

int ObIndexBlockTreeTraverser::TreeNodeContext::get_next_index_row(blocksstable::ObMicroIndexInfo &idx_block_row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(scanner_.get_next(idx_block_row))) {
    LOG_WARN("Fail to get next index row", KR(ret), K(scanner_));
  }

  return ret;
}

}
}
