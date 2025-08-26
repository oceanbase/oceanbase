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

#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
namespace oceanbase
{
namespace tmp_file
{

ObTmpFileBlockManager::ObTmpFileBlockManager() :
                       is_inited_(false),
                       tenant_id_(OB_INVALID_TENANT_ID),
                       block_index_generator_(0),
                       block_map_(),
                       block_allocator_(),
                       flush_priority_mgr_(),
                       alloc_priority_mgr_()
                       {}

ObTmpFileBlockManager::~ObTmpFileBlockManager()
{
  destroy();
}

int ObTmpFileBlockManager::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTmpFileBlockManager init twice", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id));
  } else if (OB_FAIL(block_map_.init("TmpFileBlkMgr", tenant_id))) {
    LOG_WARN("fail to init tenant temporary file block manager", KR(ret), K(tenant_id));
  } else if (OB_FAIL(block_allocator_.init(common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                           ObModIds::OB_TMP_BLOCK_MANAGER, tenant_id_, INT64_MAX))) {
    LOG_WARN("fail to init temporary file block allocator", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(alloc_priority_mgr_.init())) {
    LOG_WARN("fail to init alloc priority manager", KR(ret));
  } else if (OB_FAIL(flush_priority_mgr_.init())) {
    LOG_WARN("fail to init flush priority manager", KR(ret));
  } else {
    block_allocator_.set_attr(ObMemAttr(tenant_id, "TmpFileBlk"));
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }

  return ret;
}

void ObTmpFileBlockManager::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    LOG_INFO("ObTmpFileBlockManager destroy", K(block_map_.count()));
    is_inited_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    block_index_generator_ = 0;
    block_allocator_.reset();

    if (block_map_.count() != 0) {
      LOG_ERROR("block map is not empty", K(block_map_.count()));
    }

    block_map_.destroy();
    flush_priority_mgr_.destroy();
    alloc_priority_mgr_.destroy();
  }
}

int ObTmpFileBlockManager::alloc_block(int64_t &block_index)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlock *blk = nullptr;
  block_index = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(alloc_block_(blk, ObTmpFileBlock::BlockType::EXCLUSIVE))) {
    LOG_WARN("fail to alloc block", KR(ret));
  } else if (OB_ISNULL(blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nullptr", KR(ret));
  } else if (OB_FAIL(blk->alloc_pages(ObTmpFileGlobal::BLOCK_PAGE_NUMS))) {
    LOG_WARN("fail to alloc pages", KR(ret), KPC(blk));
  } else {
    block_index = blk->get_block_index();
  }

  LOG_DEBUG("alloc block over", KR(ret), KPC(blk));
  return ret;
}

int ObTmpFileBlockManager::alloc_page_range(const int64_t necessary_page_num,
                                            const int64_t expected_page_num,
                                            ObIArray<ObTmpFileBlockRange>& page_ranges)
{
  int ret = OB_SUCCESS;
  page_ranges.reset();
  int64_t remain_page_num = necessary_page_num;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(necessary_page_num <= 0 ||
                         expected_page_num > ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM ||
                         necessary_page_num > expected_page_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(necessary_page_num), K(expected_page_num));
  } else if (OB_FAIL(alloc_priority_mgr_.alloc_page_range(necessary_page_num, expected_page_num, remain_page_num, page_ranges))) {
    LOG_WARN("fail to alloc page entry", KR(ret), K(necessary_page_num), K(expected_page_num));
  } else if (OB_UNLIKELY(remain_page_num < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", KR(ret), K(necessary_page_num), K(expected_page_num), K(remain_page_num));
  } else if (remain_page_num > 0) { // there are not enough continuous pages in alloc priority mgr
    ObTmpFileBlockHandle handle;
    ObTmpFileBlock *blk = nullptr;
    if (OB_FAIL(alloc_block_(blk, ObTmpFileBlock::BlockType::SHARED))) {
      LOG_WARN("fail to alloc block", KR(ret));
    } else if (OB_ISNULL(blk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, block should not be null", KR(ret));
    } else if (OB_FAIL(handle.init(blk))) {
      LOG_WARN("fail to init block handle", KR(ret), KPC(blk));
    } else {
      ObTmpFileBlockRange range;
      if (OB_FAIL(range.init(blk->get_block_index(), 0, remain_page_num))) {
        LOG_WARN("fail to init block pre alloc range", KR(ret), KPC(blk), K(remain_page_num));
      } else if (OB_FAIL(page_ranges.push_back(range))) {
        LOG_WARN("fail to push back", KR(ret), K(range));
      } else if (OB_FAIL(blk->alloc_pages(remain_page_num))) {
        LOG_WARN("fail to alloc pages", KR(ret), KPC(blk));
      }

      if (OB_FAIL(ret)) {
        page_ranges.pop_back();
      }
    }
  }
  return ret;
}

int ObTmpFileBlockManager::alloc_block_(ObTmpFileBlock *&block, ObTmpFileBlock::BlockType block_type)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlock* blk = nullptr;
  const uint64_t blk_size = sizeof(ObTmpFileBlock);
  void *buf = nullptr;
  int64_t block_index = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(buf = block_allocator_.alloc(blk_size, lib::ObMemAttr(tenant_id_, "TmpFileBlk")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for tmp file block", KR(ret), K(blk_size));
  } else if (FALSE_IT(blk = new (buf) ObTmpFileBlock())) {
  } else if (FALSE_IT(block_index = ATOMIC_AAF(&block_index_generator_, 1))) {
  } else if (OB_FAIL(blk->init(block_index, block_type, this))) {
    LOG_WARN("fail to init tmp file block", KR(ret), K(block_index), K(block_type));
  } else if (OB_FAIL(block_map_.insert(ObTmpFileBlockKey(block_index), blk))) {
    LOG_WARN("fail to insert block into block map", KR(ret), KPC(blk));
  } else {
    block = blk;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(blk)) {
    blk->~ObTmpFileBlock();
    block_allocator_.free(blk);
    blk = nullptr;
  }

  LOG_DEBUG("create tmp file block over", KR(ret), K(block_index), KPC(blk));
  return ret;
}

int ObTmpFileBlockManager::release_page(const int64_t block_index,
                                        const int64_t begin_page_id,
                                        const int64_t page_num)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle handle;
  ObTmpFileBlock *block = nullptr;
  bool can_remove = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.get(ObTmpFileBlockKey(block_index), handle))) {
    LOG_WARN("fail to get tmp file block", KR(ret), K(block_index));
  } else if (FALSE_IT(block = handle.get())) {
  } else if (OB_ISNULL(block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be null", KR(ret), K(block_index));
  } else if (OB_FAIL(block->release_pages(begin_page_id, page_num, can_remove))) {
    LOG_WARN("fail to release pages", KR(ret), K(begin_page_id), K(page_num), KPC(block));
  } else if (can_remove) {
    if (OB_FAIL(block_map_.erase(ObTmpFileBlockKey(block->get_block_index())))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_ERROR("fail to erase tmp file block", KR(ret), K(block_index));
      } else {
        LOG_WARN("erase tmp file block", KR(ret), K(block_index));
        ret = OB_SUCCESS;
      }
    }
    LOG_DEBUG("remove block over", KR(ret), KPC(block));
  }
  LOG_DEBUG("release_tmp_file_page", KR(ret), K(block_index), K(begin_page_id), K(page_num), K(handle));

  return ret;
}

int ObTmpFileBlockManager::insert_block_into_alloc_priority_mgr(const int64_t free_page_num,
                                                                ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(alloc_priority_mgr_.insert_block_into_alloc_priority_list(free_page_num, block))) {
    LOG_WARN("fail to insert tmp file block", KR(ret), K(block));
  }

  LOG_DEBUG("insert tmp file block into alloc priority mgr over", KR(ret), K(block));
  return ret;
}

int ObTmpFileBlockManager::remove_block_from_alloc_priority_mgr(const int64_t free_page_num, ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(alloc_priority_mgr_.remove_block_from_alloc_priority_list(free_page_num, block))) {
    LOG_WARN("fail to remove tmp file block", KR(ret), K(block));
  }

  LOG_DEBUG("remove tmp file block from alloc priority mgr over", KR(ret), K(block));
  return ret;
}

int ObTmpFileBlockManager::adjust_block_alloc_priority(const int64_t old_free_page_num,
                                                       const int64_t free_page_num,
                                                       ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(alloc_priority_mgr_.adjust_block_alloc_priority(old_free_page_num, free_page_num, block))) {
    LOG_WARN("fail to adjust alloc priority of tmp file block", KR(ret), K(block));
  }

  LOG_DEBUG("adjust alloc priority of tmp file block over", KR(ret), K(block));
  return ret;
}

int ObTmpFileBlockManager::insert_block_into_flush_priority_mgr(const int64_t flushing_page_num, ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(flush_priority_mgr_.insert_block_into_flush_priority_list(flushing_page_num, block))) {
    LOG_ERROR("fail to insert tmp file block", KR(ret), K(block));
  }

  LOG_DEBUG("insert tmp file block into flush priority mgr over", KR(ret), K(block));
  return ret;
}

int ObTmpFileBlockManager::remove_block_from_flush_priority_mgr(const int64_t flushing_page_num, ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(flush_priority_mgr_.remove_block_from_flush_priority_list(flushing_page_num, block))) {
    LOG_WARN("fail to remove tmp file block", KR(ret), K(block));
  }

  LOG_DEBUG("remove tmp file block from flush priority mgr over", KR(ret), K(block));

  return ret;
}

int ObTmpFileBlockManager::adjust_block_flush_priority(const int64_t old_flushing_page_num,
                                                       const int64_t flushing_page_num,
                                                       ObTmpFileBlock &block)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(flush_priority_mgr_.adjust_block_flush_priority(old_flushing_page_num, flushing_page_num, block))) {
    LOG_WARN("fail to adjust flush priority of tmp file block", KR(ret), K(block));
  }

  LOG_DEBUG("adjust flush priority of tmp file block over", KR(ret), K(block));
  return ret;
}

int ObTmpFileBlockManager::get_tmp_file_block_handle(const int64_t block_index, ObTmpFileBlockHandle &handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(handle.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(handle));
  } else if (OB_FAIL(block_map_.get(ObTmpFileBlockKey(block_index), handle))) {
    LOG_WARN("fail to get tmp file block", KR(ret), K(block_index));
  } else if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be null", KR(ret), K(block_index));
  } else if (OB_UNLIKELY(!handle.get()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be invalid", KR(ret), K(block_index), K(handle));
  }

  return ret;
}

int ObTmpFileBlockManager::get_macro_block_id(const int64_t block_index, blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.get(ObTmpFileBlockKey(block_index), handle))) {
    LOG_WARN("fail to get tmp file block", KR(ret), K(block_index));
  } else if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be null", KR(ret), K(block_index));
  } else if (OB_UNLIKELY(!handle.get()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, block should not be invalid", KR(ret), K(block_index), K(handle));
  } else {
    macro_block_id = handle.get()->get_macro_block_id();
  }

  return ret;
}

int ObTmpFileBlockManager::get_macro_block_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  CollectMacroBlockIdFunctor func(macro_id_list);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.for_each(func))) {
    LOG_WARN("fail to collect macro block ids", KR(ret));
  }

  return ret;
}

int ObTmpFileBlockManager::get_block_usage_stat(int64_t &flushed_page_num, int64_t &macro_block_count)
{
  int ret = OB_SUCCESS;
  CollectDiskUsageFunctor func;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(block_map_.for_each(func))) {
    LOG_WARN("fail to collect macro block ids", KR(ret));
  } else {
    macro_block_count = func.get_macro_block_count();
    flushed_page_num = func.get_flushed_page_num();
    if (flushed_page_num < 0 || macro_block_count < 0) {
      LOG_ERROR("invalid flushed_page_num or macro_block_count", K(flushed_page_num), K(macro_block_count));
    }
  }

  return ret;
}

void ObTmpFileBlockManager::print_block_usage()
{
  int ret = OB_SUCCESS;
  int64_t used_page_num = 0;
  int64_t block_num = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTmpFileBlockManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(get_block_usage_stat(used_page_num, block_num))) {
    LOG_WARN("fail to get block usage stat", KR(ret));
  } else if (OB_UNLIKELY(0 == block_num)) {
    LOG_INFO("temporary file module use no blocks");
  } else {
    int64_t occupied_page_num = block_num * ObTmpFileGlobal::BLOCK_PAGE_NUMS;
    double disk_fragment_ratio = static_cast<double>(used_page_num) / static_cast<double>(occupied_page_num);
    LOG_INFO("the block usage for temporary files",
             K(used_page_num), K(occupied_page_num), K(block_num), K(disk_fragment_ratio));
  }
}


bool ObTmpFileBlockManager::CollectMacroBlockIdFunctor::operator()(const ObTmpFileBlockKey &block_index, const ObTmpFileBlockHandle &handle)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId id;

  if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle should not be null", KR(ret), K(block_index));
  } else if (!handle.get()->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle should not be invalid", KR(ret), K(block_index), K(handle));
  } else if (FALSE_IT(id = handle.get()->get_macro_block_id())) {
  } else if (!id.is_valid()) {
    // do nothing
  } else if (OB_FAIL(macro_id_list_.push_back(id))) {
    LOG_WARN("failed to push back", KR(ret), K(block_index), K(handle));
  }
  return OB_SUCCESS == ret;
}

bool ObTmpFileBlockManager::CollectDiskUsageFunctor::operator()(const ObTmpFileBlockKey &block_index, const ObTmpFileBlockHandle &handle)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId id;
  int64_t flushed_page_num = 0;
  if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle should not be null", KR(ret), K(block_index));
  } else if (!handle.get()->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle should not be invalid", KR(ret), K(block_index), K(handle));
  } else if (FALSE_IT(id = handle.get()->get_macro_block_id())) {
  } else if (!id.is_valid()) {
    // do nothing
  } else if (OB_FAIL(handle.get()->get_disk_usage(flushed_page_num))) {
    LOG_WARN("failed to get disk usage", KR(ret), K(block_index), K(handle));
  } else if (0 == flushed_page_num) {
    // do nothing
  } else if (OB_UNLIKELY(flushed_page_num < 0 || flushed_page_num > ObTmpFileGlobal::BLOCK_PAGE_NUMS)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("flushed page num should be in (0, BLOCK_PAGE_NUMS]", KR(ret), K(block_index), K(handle));
  } else {
    flushed_page_num_ += flushed_page_num;
    macro_block_count_++;
  }
  return OB_SUCCESS == ret;
}

bool ObTmpFileBlockManager::PrintBlockOp::operator()(const ObTmpFileBlockKey &key, const ObTmpFileBlockHandle &value)
{
  LOG_INFO("print_block", K(key), K(value));
  return true;
}

void ObTmpFileBlockManager::print_blocks()
{
  LOG_INFO("printing blocks begin", K(block_map_.count()));
  int ret = OB_SUCCESS;
  PrintBlockOp print_op;
  if (OB_FAIL(block_map_.for_each(print_op))) {
    LOG_WARN("fail to print blocks", KR(ret));
  }
  LOG_INFO("printing blocks end", K(block_map_.count()));
}

} // end namespace tmp_file
} // end namespace oceanbase
