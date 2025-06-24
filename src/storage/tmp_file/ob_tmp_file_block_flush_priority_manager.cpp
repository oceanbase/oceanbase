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

#include "storage/tmp_file/ob_tmp_file_block_flush_priority_manager.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace tmp_file
{
int ObTmpFileBlockFlushPriorityManager::init()
{
  int ret = OB_SUCCESS;
  STATIC_ASSERT(ARRAYSIZEOF(flush_lists_) == (int64_t)BlockFlushLevel::MAX,
              "flush_lists_ size mismatch enum BlockFlushLevel count");
  STATIC_ASSERT(ARRAYSIZEOF(locks_) == (int64_t)BlockFlushLevel::MAX,
                "locks_ size mismatch enum BlockFlushLevel count");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTmpFileBlockFlushPriorityManager inited twice", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("ObTmpFileBlockFlushPriorityManager init succ", K(is_inited_));
  }
  return ret;
}

void ObTmpFileBlockFlushPriorityManager::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  ObTmpFileBlock* block = nullptr;
  for (int64_t i = 0; i < BlockFlushLevel::MAX; i++) {
    ObSpinLockGuard guard(locks_[i]);
    while (!flush_lists_[i].is_empty()) {
      if (OB_ISNULL(block = &flush_lists_[i].remove_first()->block_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block is null", KR(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("there is a block still exists when destroying", KR(ret), KP(block), KPC(block));
        // block->dec_ref_cnt();
      }
    }
    flush_lists_[i].clear();
  }
  LOG_INFO("ObTmpFileBlockFlushPriorityManager destroy succ");
}

int ObTmpFileBlockFlushPriorityManager::insert_block_into_flush_priority_list(const int64_t flushing_page_num,
                                                                              ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;
  BlockFlushLevel level = get_block_list_level_(flushing_page_num, block.is_exclusive_block());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockFlushPriorityManager is not inited", KR(ret));
  } else if (OB_UNLIKELY(level < BlockFlushLevel::L1 || level >= BlockFlushLevel::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(level), K(flushing_page_num), K(block));
  } else if (OB_UNLIKELY(!block.is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is not valid", KR(ret), K(block));
  } else {
    ObSpinLockGuard guard(locks_[level]);
    if (OB_UNLIKELY(!flush_lists_[level].add_last(&block.get_flush_blk_node()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to add block into flush_lists", KR(ret), K(block));
    } else {
      block.inc_ref_cnt();
      LOG_DEBUG("insert block into flush priority list succ",
          K(flushing_page_num), K(level), K(flush_lists_[level].get_size()), K(block));
    }
  }
  LOG_DEBUG("insert block into flush priority list", KR(ret), K(flushing_page_num), K(level), K(block));
  return ret;
}

int ObTmpFileBlockFlushPriorityManager::remove_block_from_flush_priority_list(const int64_t flushing_page_num,
                                                                              ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;
  BlockFlushLevel level = get_block_list_level_(flushing_page_num, block.is_exclusive_block());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockFlushPriorityManager is not inited", KR(ret), K(block));
  } else if (OB_UNLIKELY(level < BlockFlushLevel::L1 || level >= BlockFlushLevel::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(flushing_page_num), K(block));
  } else if (OB_UNLIKELY(!block.is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is not valid", KR(ret), K(block));
  } else {
    ObSpinLockGuard guard(locks_[level]);
    ObTmpFileBlkNode &flush_node = block.get_flush_blk_node();
    if (OB_ISNULL(flush_node.get_prev()) && OB_ISNULL(flush_node.get_next())) {
      // do nothing
    } else if (OB_ISNULL(flush_node.get_prev()) || OB_ISNULL(flush_node.get_next())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("flush node contains unexpected ptr", KR(ret), K(block));
    } else if (OB_ISNULL(flush_lists_[level].remove(&flush_node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block is not in flush_lists", KR(ret), K(block));
    } else {
      int64_t ref = block.dec_ref_cnt();
      if (0 == ref) {
        LOG_ERROR("unexpected ref cnt", K(ref), K(block));
      }
    }
  }
  LOG_DEBUG("remove block from flush priority list", KR(ret), K(flushing_page_num), K(level), K(block));
  return ret;
}

// currently, this function will only be called when inserting flushing pages into block's flushing list.
// when the block is choosen for flushing, whole pages in the list will be choosen.
// thus, in this case, the block will be removed from priority list rather than be adjusted priority.
// so, we require the old_flushing_page_num to be less than flushing_page_num.
int ObTmpFileBlockFlushPriorityManager::adjust_block_flush_priority(const int64_t old_flushing_page_num,
                                                                    const int64_t flushing_page_num,
                                                                    ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;
  BlockFlushLevel old_level = get_block_list_level_(old_flushing_page_num, block.is_exclusive_block());
  BlockFlushLevel new_level = get_block_list_level_(flushing_page_num, block.is_exclusive_block());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockFlushPriorityManager is not inited", KR(ret));
  } else if (OB_UNLIKELY(old_level < BlockFlushLevel::L1 || old_level >= BlockFlushLevel::MAX ||
                         new_level < BlockFlushLevel::L1 || new_level >= BlockFlushLevel::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(old_level), K(new_level),
             K(old_flushing_page_num), K(flushing_page_num), K(block));
  } else if (OB_UNLIKELY(!block.is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is not valid", KR(ret), K(block));
  } else if (old_level == new_level) {
    // do nothing
    LOG_DEBUG("block flush priority not changed", K(old_level), K(new_level),
        K(old_flushing_page_num), K(flushing_page_num), K(block));
  } else {
    bool need_insert = true;
    {
      ObSpinLockGuard guard(locks_[old_level]);
      ObTmpFileBlkNode &flush_node = block.get_flush_blk_node();
      if (OB_ISNULL(flush_node.get_prev()) && OB_ISNULL(flush_node.get_next())) {
        // do nothing, the block will be reinserted by flush thread
        need_insert = false;
        LOG_DEBUG("the block is not in flush_lists, maybe it is popped by flush thread",
                  K(old_level), K(new_level), K(block));
      } else if (OB_ISNULL(flush_node.get_prev()) || OB_ISNULL(flush_node.get_next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("flush node contains unexpected ptr", KR(ret), K(block));
      } else if (OB_ISNULL(flush_lists_[old_level].remove(&flush_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to remove block from flush_lists", KR(ret), K(block));
      }
    }
    if (OB_SUCC(ret) && need_insert) {
      ObSpinLockGuard guard(locks_[new_level]);
      if (OB_UNLIKELY(!flush_lists_[new_level].add_last(&block.get_flush_blk_node()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to add block into flush_lists", KR(ret), K(block));
      }
      LOG_DEBUG("adjust block from flush priority list succ", K(flush_lists_[new_level].get_size()),
                K(old_level), K(new_level), K(block));
    }
  }
  LOG_DEBUG("adjust block from flush priority list", KR(ret), K(old_flushing_page_num), K(flushing_page_num),
           K(old_level), K(new_level), K(block));
  return ret;
}

int ObTmpFileBlockFlushPriorityManager::popN_from_block_list_(const BlockFlushLevel flush_level,
                                                              const int64_t expected_count, int64_t &actual_count,
                                                              ObIArray<ObTmpFileBlockHandle> &block_handles)
{
  int ret = OB_SUCCESS;
  actual_count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockFlushPriorityManager is not inited", KR(ret));
  } else if (OB_UNLIKELY(flush_level < BlockFlushLevel::L1 || flush_level >= BlockFlushLevel::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(flush_level));
  } else {
    ObSpinLockGuard guard(locks_[flush_level]);
    ObTmpFileBlkNode *header = flush_lists_[flush_level].get_header();
    ObTmpFileBlkNode *node = flush_lists_[flush_level].get_header()->get_next();
    while (OB_SUCC(ret) && node != header && actual_count < expected_count) {
      ObTmpFileBlkNode *next_node = nullptr;
      ObTmpFileBlock *block = nullptr;
      bool set_flushing_succ = false;
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", KR(ret), KPC(node));
      } else if (OB_ISNULL(block = &node->block_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block is null", KR(ret));
      } else if (FALSE_IT(next_node = node->get_next())) {
      } else if (OB_FAIL(block->set_flushing_status(set_flushing_succ))) {
        LOG_WARN("fail to set flushing status", KR(ret), KPC(block));
      } else if (set_flushing_succ) {
        if (OB_ISNULL(node->get_prev()) || OB_ISNULL(node->get_next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("block is not in flush_lists", KR(ret), K(block));
        } else if (OB_ISNULL(flush_lists_[flush_level].remove(node))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to remove node", KR(ret), KPC(block), KPC(node));
        } else if (OB_FAIL(block_handles.push_back(block))) {
          LOG_ERROR("fail to push back", KR(ret), KPC(block));
        } else {
          LOG_DEBUG("pop block from flush priority list", K(flush_level), KPC(block));
          actual_count++;
          int64_t ref = block->dec_ref_cnt();
          if (0 == ref) {
            LOG_ERROR("unexpected ref cnt", K(ref), K(block));
          }
        }
      } else {
        LOG_DEBUG("add block into lock failed list", KR(ret), KPC(block));
      }
      if (OB_SUCC(ret)) {
        node = next_node;
      }
    } // end while
    LOG_DEBUG("popN_from_block_list_ finished", KR(ret), K(flush_level),
        K(expected_count), K(actual_count), K(flush_lists_[flush_level].get_size()), K(block_handles.count()));
  }
  return ret;
}

ObTmpFileBlockFlushPriorityManager::BlockFlushLevel ObTmpFileBlockFlushPriorityManager::get_block_list_level_(
  const int64_t flushing_page_num, const bool is_exclusive_block) const
{
  BlockFlushLevel level = BlockFlushLevel::INVALID;
  if (flushing_page_num <= 0 || flushing_page_num > ObTmpFileGlobal::BLOCK_PAGE_NUMS) {
    level = BlockFlushLevel::INVALID;
  } else if (is_exclusive_block) {
    if (flushing_page_num > 64 && flushing_page_num <= 256) {
      level = BlockFlushLevel::L1;
    } else{
      level = BlockFlushLevel::L2;
    }
  } else {
    if (flushing_page_num > 64 && flushing_page_num <= 256) {
      level = BlockFlushLevel::L3;
    } else{
      level = BlockFlushLevel::L4;
    }
  }
  return level;
}

ObTmpFileBlockFlushPriorityManager::BlockFlushLevel ObTmpFileBlockFlushPriorityManager::get_next_level_(const BlockFlushLevel level) const
{
  BlockFlushLevel ret = BlockFlushLevel::INVALID;
  if (level != BlockFlushLevel::INVALID && level != BlockFlushLevel::MAX) {
    if (level == BlockFlushLevel::L1) {
      ret = BlockFlushLevel::L2;
    } else if (level == BlockFlushLevel::L2) {
      ret = BlockFlushLevel::L3;
    } else if (level == BlockFlushLevel::L3) {
      ret = BlockFlushLevel::L4;
    } else if (level == BlockFlushLevel::L4) {
      ret = BlockFlushLevel::MAX;
    } else {
      ret = BlockFlushLevel::INVALID;
    }
  }
  return ret;
}

int64_t ObTmpFileBlockFlushPriorityManager::get_block_count()
{
  int64_t size = 0;

  for (int64_t i = 0; i < BlockFlushLevel::MAX; i++) {
    ObSpinLockGuard guard(locks_[i]);
    size += flush_lists_[i].get_size();
  }

  return size;
}

void ObTmpFileBlockFlushPriorityManager::print_blocks()
{
  int ret = OB_SUCCESS;
  bool cache_over = false;
  BlockFlushLevel flush_level = BlockFlushLevel::L1;

  while (OB_SUCC(ret) && !cache_over) {
    ObSpinLockGuard guard(locks_[flush_level]);
    LOG_INFO("printing blocks of flush priority manager begin", K(flush_lists_[flush_level].get_size()), K(flush_level));
    ObTmpFileBlkNode *header = flush_lists_[flush_level].get_header();
    ObTmpFileBlkNode *node = flush_lists_[flush_level].get_header()->get_next();
    while (OB_SUCC(ret) && node != header) {
      ObTmpFileBlock *block = nullptr;
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", KR(ret), KPC(node));
      } else if (OB_ISNULL(block = &node->block_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block is null", KR(ret));
      } else {
        LOG_INFO("print block of flush priority manage", KPC(block));
        node = node->get_next();
      }
    } // end while
    flush_level = get_next_level_(flush_level);
    if (BlockFlushLevel::INVALID == flush_level) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", KR(ret), K(flush_level));
    } else if (BlockFlushLevel::MAX == flush_level) {
      cache_over = true;
    }
    LOG_INFO("printing blocks of flush priority manager end", KR(ret), K(flush_lists_[flush_level].get_size()), K(flush_level));
  }
}

ObTmpFileBlockFlushIterator::ObTmpFileBlockFlushIterator() :
  is_inited_(false), prio_mgr_(nullptr), blocks_(),
  cur_level_(BlockFlushLevel::L1)
{}

ObTmpFileBlockFlushIterator::~ObTmpFileBlockFlushIterator()
{
  destroy();
}

int ObTmpFileBlockFlushIterator::init(ObTmpFileBlockFlushPriorityManager *prio_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(prio_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(prio_mgr));
  } else {
    blocks_.set_attr(ObMemAttr(MTL_ID(), "TFBlkFlushIter"));
    prio_mgr_ = prio_mgr;
    is_inited_ = true;
  }
  return ret;
}

void ObTmpFileBlockFlushIterator::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    if (cur_level_ < BlockFlushLevel::L1 || cur_level_ > BlockFlushLevel::MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected flush level", KR(ret), K(cur_level_));
    } else if (blocks_.count() > 0 &&
              OB_FAIL(reinsert_block_into_flush_priority_list_())){
      LOG_WARN("fail to reinsert block into flush priority list", KR(ret), K(blocks_.count()), KP(&blocks_));
    } else {
      blocks_.reset();
      cur_level_ = BlockFlushLevel::L1;
      prio_mgr_ = nullptr;
      is_inited_ = false;
    }
  }
}

int ObTmpFileBlockFlushIterator::reinsert_block_into_flush_priority_list_()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("reinsert block into flush priority list begin", K(blocks_.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < blocks_.count(); i++) {
    ObTmpFileBlock *block = blocks_[i].get();
    LOG_DEBUG("reinsert block into flush list", K(i), K(blocks_.count()), KPC(block));
    if (OB_ISNULL(block)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KR(ret));
    } else if (OB_FAIL(block->reinsert_into_flush_prio_mgr())) {
      LOG_ERROR("fail to reinsert block into flush priority list", KR(ret), KPC(block));
    }
  }
  return ret;
}

int ObTmpFileBlockFlushIterator::next(ObTmpFileBlockHandle &block_handle)
{
  int ret = OB_SUCCESS;
  block_handle.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(blocks_.count() > ObTmpFileBlockFlushIterator::MAX_CACHE_NUM)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cache num", KR(ret), K(blocks_.count()));
  } else if (OB_UNLIKELY(cur_level_ < BlockFlushLevel::L1 || cur_level_ >= BlockFlushLevel::MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_level_));
  } else if (blocks_.count() == 0 && OB_FAIL(cache_blocks_())) {
    if (OB_ITER_END == ret) {
      LOG_DEBUG("fail to cache blocks", KR(ret));
    } else {
      LOG_WARN("fail to cache blocks", KR(ret));
    }
  } else if (OB_FAIL(blocks_.pop_back(block_handle))) {
    LOG_WARN("fail to pop back block", KR(ret), K(blocks_.count()), KP(&blocks_));
  }
  LOG_DEBUG("try to get next block", KR(ret), K(cur_level_), K(blocks_.count()),
            K(block_handle), KP(prio_mgr_));
  return ret;
}

// Attention!!!
// must keep the blocks_ have enough memory to store block handles
int ObTmpFileBlockFlushIterator::cache_blocks_()
{
  int ret = OB_SUCCESS;
  bool cache_over = false;
  if (OB_UNLIKELY(!blocks_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(blocks_.count()), K(cur_level_));
  } else {
    while (OB_SUCC(ret) && !cache_over) {
      int64_t actual_count = 0;
      if (OB_FAIL(prio_mgr_->popN_from_block_list_(cur_level_, ObTmpFileBlockFlushIterator::MAX_CACHE_NUM, actual_count, blocks_))) {
        LOG_WARN("fail to get block from flush priority list", KR(ret), K(cur_level_), KP(&blocks_));
      } else if (actual_count == 0) {
        cur_level_ = prio_mgr_->get_next_level_(cur_level_);
        if (BlockFlushLevel::INVALID == cur_level_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status", KR(ret), K(cur_level_));
        } else if (BlockFlushLevel::MAX == cur_level_) {
          ret = OB_ITER_END;
        } else {
          // do nothing, pop blocks in the next level
        }
      } else {
        cache_over = true;
      }
    }
  }
  // DEBUG
  // if (OB_ITER_END == ret) {
  //   ObTmpFileBlockManager &blk_mgr = MTL(ObTenantTmpFileManager*)->get_sn_file_manager().get_tmp_file_block_manager();
  //   blk_mgr.print_blocks();
  // }
  LOG_DEBUG("cache blocks", KR(ret), K(cur_level_), K(blocks_.count()));
  return ret;
}

} // end namespace tmp_file
} // end namespace oceanbase
