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

#include "storage/tmp_file/ob_tmp_file_block_allocating_priority_manager.h"
#include "share/rc/ob_tenant_base.h"
namespace oceanbase
{
namespace tmp_file
{
//-----------------------ObTmpFileBlockRange-----------------------
int ObTmpFileBlockRange::init(const int64_t block_index,
                              const int16_t page_index,
                              const int16_t page_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(block_index == ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX ||
                  page_index < 0 || page_cnt <= 0 ||
                  page_index + page_cnt > ObTmpFileGlobal::BLOCK_PAGE_NUMS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_index), K(page_index), K(page_cnt));
  } else {
    block_index_ = block_index;
    page_index_ = page_index;
    page_cnt_ = page_cnt;
  }
  return ret;
}

void ObTmpFileBlockRange::reset()
{
  block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  page_index_ = 0;
  page_cnt_ = 0;
}

bool ObTmpFileBlockRange::is_valid() const
{
  return block_index_ != ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX &&
         page_index_ >= 0 && page_index_ < ObTmpFileGlobal::BLOCK_PAGE_NUMS &&
         page_cnt_ > 0 &&
         (page_index_ + page_cnt_ <= ObTmpFileGlobal::BLOCK_PAGE_NUMS);
}

//-----------------------ObTmpFileBlockAllocatingPriorityManager-----------------------

int ObTmpFileBlockAllocatingPriorityManager::init()
{
  int ret = OB_SUCCESS;
  STATIC_ASSERT(ARRAYSIZEOF(alloc_lists_) == (int64_t)BlockPreAllocLevel::MAX,
              "alloc_lists_ size mismatch enum BlockPreAllocLevel count");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTmpFileBlockAllocatingPriorityManager inited twice", KR(ret));
  } else {
    for (int32_t i = 0; i < BlockPreAllocLevel::MAX; ++i) {
      alloc_lists_[i].init(ObTmpFileBlockHandleList::PREALLOC_NODE);
    }
    is_inited_ = true;
    LOG_INFO("ObTmpFileBlockAllocatingPriorityManager init succ", K(is_inited_));
  }
  return ret;
}

void ObTmpFileBlockAllocatingPriorityManager::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  for (int64_t i = 0; i < BlockPreAllocLevel::MAX; i++) {
    int64_t cur_size = alloc_lists_[i].size();
    if (cur_size != 0) {
      LOG_ERROR("there are blocks in alloc_list_ when destroying", K(i), K(cur_size));
      PrintOperator print_op("destroy_alloc_prio");
      alloc_lists_[i].for_each(print_op);
    }
  }
  LOG_INFO("ObTmpFileBlockAllocatingPriorityManager destroy succ");
}

GetAllocatableBlockOp::GetAllocatableBlockOp(
    const int64_t necessary_page_num,
    int64_t &candidate_page_num,
    ObTmpFileBlockHandleList &list,
    ObIArray<ObTmpFileBlockHandle> &candidate_blocks) :
    necessary_page_num_(necessary_page_num),
    candidate_page_num_(candidate_page_num),
    list_(list),
    candidate_blocks_(candidate_blocks)
{}

bool GetAllocatableBlockOp::operator()(ObTmpFileBlkNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("block node is nullptr", KR(ret));
  } else if (candidate_page_num_ >= necessary_page_num_ ||
             candidate_blocks_.count() >= ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_BLOCK_NUM) {
    ret = OB_ITER_END;
  } else {
    // TODO: explain why we can access block free_page_num without lock
    ObTmpFileBlockHandle handle(&node->block_);
    if (handle->get_free_page_num_without_lock() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("block has no free page", KR(ret), K(handle));
    } else if (OB_FAIL(candidate_blocks_.push_back(handle))) {
      LOG_ERROR("fail to push back block handle", KR(ret), K(handle));
    } else if (OB_FAIL(list_.remove_without_lock_(handle))) {
      LOG_ERROR("fail to remove block handle", KR(ret), K(handle));
    } else {
      candidate_page_num_ += handle->get_free_page_num_without_lock();
    }
  }
  return OB_SUCCESS == ret;
}

// simply iterate the first [1, TMP_FILE_MAX_SHARED_PRE_ALLOC_BLOCK_NUM] blocks
// rather than iterate all blocks in the priority list.
// if the blocks in [1, TMP_FILE_MAX_SHARED_PRE_ALLOC_BLOCK_NUM] could meet the needs
// of pages in [necessary_page_num, expected_page_num], the choosen block will be filled in page_ranges;
// otherwise, the page_ranges is empty.
int ObTmpFileBlockAllocatingPriorityManager::alloc_page_range(const int64_t necessary_page_num,
                                                              const int64_t expected_page_num,
                                                              int64_t &remain_page_num,
                                                              ObIArray<ObTmpFileBlockRange>& page_ranges)
{
  int ret = OB_SUCCESS;
  page_ranges.reset();
  remain_page_num = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockAllocatingPriorityManager is not inited", KR(ret));
  } else if (OB_UNLIKELY(necessary_page_num <= 0 ||
                          expected_page_num > ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM ||
                          necessary_page_num > expected_page_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(necessary_page_num), K(expected_page_num));
  } else {
    int64_t candidate_alloc_page_num = 0;
    int64_t candidate_block_num = 0;
    BlockPreAllocLevel level = BlockPreAllocLevel::L1;
    ObSEArray<ObTmpFileBlockHandle, ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_BLOCK_NUM> candidate_blocks;
    while (OB_SUCC(ret) && level < BlockPreAllocLevel::MAX && level != BlockPreAllocLevel::INVALID &&
           candidate_alloc_page_num < necessary_page_num &&
           candidate_blocks.count() < ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_BLOCK_NUM) {
      GetAllocatableBlockOp alloc_op(necessary_page_num, candidate_alloc_page_num, alloc_lists_[level], candidate_blocks);
      alloc_lists_[level].for_each(alloc_op);
      level = get_next_level_(level);
    } // end while

    if (OB_SUCC(ret)) {
      remain_page_num = MIN(expected_page_num, MAX(necessary_page_num, candidate_alloc_page_num));
      for (int64_t i = 0; OB_SUCC(ret) && i < candidate_blocks.count(); ++i) {
        ObTmpFileBlockHandle block_handle = candidate_blocks.at(i);
        ObTmpFileBlock *block = block_handle.get();
        int64_t alloc_page_num = MIN(block->get_free_page_num_without_lock(), remain_page_num);
        ObTmpFileBlockRange range;
        // alloc_pages() will insert block into priority list again if it has free pages
        if (OB_FAIL(range.init(block->get_block_index(), block->get_begin_page_id_without_lock(), alloc_page_num))) {
          LOG_WARN("fail to init block pre alloc range", KR(ret), KPC(block), K(alloc_page_num));
        } else if (OB_FAIL(page_ranges.push_back(range))) {
          LOG_WARN("fail to push back range", KR(ret), K(range));
        } else if (OB_FAIL(block->alloc_pages(alloc_page_num))) {
          if (ret != OB_RESOURCE_RELEASED) {
            LOG_WARN("fail to alloc pages", KR(ret), KPC(block), K(alloc_page_num));
          } else {
            ret = OB_SUCCESS;
            LOG_DEBUG("the block has been released, skip it", KPC(block));
            page_ranges.pop_back();
          }
        } else {
          remain_page_num -= alloc_page_num;
        }
      } // end for
    }
  }
  return ret;
}

int ObTmpFileBlockAllocatingPriorityManager::insert_block_into_alloc_priority_list(const int64_t free_page_num,
                                                                                   ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;
  BlockPreAllocLevel level = get_block_list_level_(free_page_num);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockAllocatingPriorityManager is not inited", KR(ret));
  } else if (OB_UNLIKELY(level < BlockPreAllocLevel::L1 || level >= BlockPreAllocLevel::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(free_page_num), K(block));
  } else if (OB_UNLIKELY(!block.is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is not valid", KR(ret), K(block));
  } else if (OB_UNLIKELY(!block.is_shared_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is not shared block", KR(ret), K(block));
  } else if (OB_FAIL(alloc_lists_[level].append(&block))) {
    LOG_WARN("fail to append block into alloc_lists", KR(ret), K(block));
  }

  LOG_DEBUG("insert block into alloc priority list", KR(ret), K(free_page_num), K(block));
  return ret;
}

int ObTmpFileBlockAllocatingPriorityManager::remove_block_from_alloc_priority_list(const int64_t free_page_num,
                                                                                   ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;
  BlockPreAllocLevel level = get_block_list_level_(free_page_num);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockAllocatingPriorityManager is not inited", KR(ret));
  } else if (OB_UNLIKELY(level < BlockPreAllocLevel::L1 || level >= BlockPreAllocLevel::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(free_page_num), K(block));
  } else if (OB_UNLIKELY(!block.is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is not valid", KR(ret), K(block));
  } else if (OB_UNLIKELY(!block.is_shared_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is not shared block", KR(ret), K(block));
  } else if (OB_FAIL(alloc_lists_[level].remove(&block))) {
    LOG_WARN("fail to remove block from alloc_lists", KR(ret), K(block));
  }
  LOG_DEBUG("remove block from alloc priority list", KR(ret), K(free_page_num), K(block));
  return ret;
}

// currently, each page of block could only be allocated once.
// thus, we will require 'old_free_page_num' must be larger than 'free_page_num'
int ObTmpFileBlockAllocatingPriorityManager::adjust_block_alloc_priority(const int64_t old_free_page_num,
                                                                         const int64_t free_page_num,
                                                                         ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;
  BlockPreAllocLevel old_level = get_block_list_level_(old_free_page_num);
  BlockPreAllocLevel new_level = get_block_list_level_(free_page_num);
  bool is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockAllocatingPriorityManager is not inited", KR(ret));
  } else if (OB_UNLIKELY(old_level < BlockPreAllocLevel::L1 || old_level >= BlockPreAllocLevel::MAX ||
                          new_level < BlockPreAllocLevel::L1 || new_level >= BlockPreAllocLevel::MAX ||
                          old_free_page_num <= free_page_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(old_level), K(new_level), K(old_free_page_num), K(free_page_num), K(block));
  } else if (OB_UNLIKELY(!block.is_valid_without_lock())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is not valid", KR(ret), K(block));
  } else if (OB_UNLIKELY(!block.is_shared_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is not shared block", KR(ret), K(block));
  } else if (old_level == new_level) {
    // do nothing
  } else if (OB_FAIL(alloc_lists_[old_level].remove(&block, is_exist))) {
    LOG_WARN("fail to remove block from alloc_lists", KR(ret), K(old_level), K(block));
  } else if (is_exist &&
             OB_FAIL(alloc_lists_[new_level].append(&block))) {
    LOG_WARN("fail to append block into alloc_lists", KR(ret), K(new_level), K(block));
  }
  LOG_DEBUG("adjust block from alloc priority list", KR(ret), K(old_free_page_num),
           K(free_page_num), K(old_level), K(new_level), K(block));
  return ret;
}

ObTmpFileBlockAllocatingPriorityManager::BlockPreAllocLevel ObTmpFileBlockAllocatingPriorityManager::get_block_list_level_(const int64_t free_page_num) const
{
  BlockPreAllocLevel level = BlockPreAllocLevel::INVALID;
  if (free_page_num <= 0 || free_page_num >= ObTmpFileGlobal::BLOCK_PAGE_NUMS) {
    level = BlockPreAllocLevel::INVALID;
  } else if (free_page_num > 0 && free_page_num <= 64) {
    level = BlockPreAllocLevel::L1;
  } else if (free_page_num > 64 && free_page_num <= 128) {
    level = BlockPreAllocLevel::L2;
  } else if (free_page_num > 128 && free_page_num < ObTmpFileGlobal::BLOCK_PAGE_NUMS) {
    level = BlockPreAllocLevel::L3;
  } else {
    level = BlockPreAllocLevel::INVALID;
  }
  return level;
}

ObTmpFileBlockAllocatingPriorityManager::BlockPreAllocLevel ObTmpFileBlockAllocatingPriorityManager::get_next_level_(const BlockPreAllocLevel level) const
{
  BlockPreAllocLevel ret = BlockPreAllocLevel::INVALID;
  if (level != BlockPreAllocLevel::INVALID && level != BlockPreAllocLevel::MAX) {
    if (level == BlockPreAllocLevel::L1) {
      ret = BlockPreAllocLevel::L2;
    } else if (level == BlockPreAllocLevel::L2) {
      ret = BlockPreAllocLevel::L3;
    } else if (level == BlockPreAllocLevel::L3) {
      ret = BlockPreAllocLevel::MAX;
    } else {
      ret = BlockPreAllocLevel::INVALID;
    }
  }
  return ret;
}

int64_t ObTmpFileBlockAllocatingPriorityManager::get_block_count()
{
  int64_t size = 0;
  for (int64_t i = 0; i < BlockPreAllocLevel::MAX; i++) {
    size += alloc_lists_[i].size();
  }
  return size;
}

void ObTmpFileBlockAllocatingPriorityManager::print_blocks()
{
  int ret = OB_SUCCESS;
  bool cache_over = false;
  BlockPreAllocLevel alloc_level = BlockPreAllocLevel::L1;
  PrintOperator print_op("alloc_prio_mgr");
  while (OB_SUCC(ret) && BlockPreAllocLevel::MAX != alloc_level) {
    alloc_lists_[alloc_level].for_each(print_op);
    alloc_level = get_next_level_(alloc_level);
  }
}

} // end namespace tmp_file
} // end namespace oceanbase
