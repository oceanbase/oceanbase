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

#include "ob_tmp_file_store.h"
#include "ob_tmp_file.h"

namespace oceanbase {
namespace blocksstable {

const int64_t ObTmpMacroBlock::DEFAULT_PAGE_SIZE = 8192L;  // 8kb
int64_t ObTmpTenantMacroBlockManager::next_blk_id_ = 0;

ObTmpFilePageBuddy::ObTmpFilePageBuddy() : max_cont_page_nums_(0), buf_(NULL), allocator_(NULL), is_inited_(false)
{}

ObTmpFilePageBuddy::~ObTmpFilePageBuddy()
{
  destroy();
}

int ObTmpFilePageBuddy::init(common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  int64_t start_id = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFilePageBuddy has not been inited", K(ret));
  } else {
    allocator_ = &allocator;
    buf_ = reinterpret_cast<ObTmpFileArea*>(
        allocator_->alloc(sizeof(ObTmpFileArea) * std::pow(2, ObTmpFilePageBuddy::MAX_ORDER) - 1));
    if (NULL == buf_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc a buf", K(ret));
    } else {
      max_cont_page_nums_ = std::pow(2, ObTmpFilePageBuddy::MAX_ORDER - 1);
      /**
       *  page buddy free_list for a new block:
       *  -------------- --------- --------- --------- --------- --------- --------- --------- -------
       * |cont_page_nums|    1    |    2    |    4    |    8    |   16    |   32    |   64    |  128  |
       *  -------------- --------- --------- --------- --------- --------- --------- --------- -------
       * |  free_area   |[254,254]|[252,253]|[248,251]|[240,247]|[224,239]|[192,223]|[128,191]|[0,127]|
       *  -------------- --------- --------- --------- --------- --------- --------- --------- -------
       */
      int64_t nums = max_cont_page_nums_;
      for (int32_t i = MAX_ORDER - 1; i >= 0; --i) {
        char* buf = reinterpret_cast<char*>(&(buf_[start_id]));
        free_area_[i] = new (buf) ObTmpFileArea(start_id, nums);
        start_id += nums;
        nums /= 2;
      }
      is_inited_ = true;
    }
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObTmpFilePageBuddy::destroy()
{
  if (NULL != buf_) {
    allocator_->free(buf_);
    buf_ = NULL;
  }
  allocator_ = NULL;
  max_cont_page_nums_ = 0;
  is_inited_ = false;
}

int ObTmpFilePageBuddy::alloc_all_pages()
{
  int ret = OB_SUCCESS;
  ObTmpFileArea* tmp = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFilePageBuddy has not been inited", K(ret));
  } else if (is_empty()) {
    for (int32_t i = 0; i < ObTmpFilePageBuddy::MAX_ORDER; ++i) {
      while (NULL != free_area_[i]) {
        tmp = free_area_[i];
        free_area_[i] = tmp->next_;
        tmp->~ObTmpFileArea();
      }
    }
    max_cont_page_nums_ = 0;
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "this tmp block is not empty", K(ret));
  }
  return ret;
}

int ObTmpFilePageBuddy::alloc(const int32_t page_nums, int32_t& start_page_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFilePageBuddy has not been inited", K(ret));
  } else {
    int32_t index = std::ceil(std::log(page_nums) / std::log(2));
    bool is_alloced = false;
    for (int32_t i = index; i < ObTmpFilePageBuddy::MAX_ORDER && !is_alloced; ++i) {
      if (NULL != free_area_[i]) {
        int64_t num = i - index;
        ObTmpFileArea* tmp = free_area_[i];
        free_area_[i] = tmp->next_;
        tmp->next_ = NULL;
        while (num--) {
          tmp->page_nums_ /= 2;
          char* buf = reinterpret_cast<char*>(&(buf_[tmp->start_page_id_ + tmp->page_nums_]));
          ObTmpFileArea* area = new (buf) ObTmpFileArea(tmp->start_page_id_ + tmp->page_nums_, tmp->page_nums_);
          area->next_ = free_area_[static_cast<int32_t>(std::log(tmp->page_nums_) / std::log(2))];
          free_area_[static_cast<int32_t>(std::log(tmp->page_nums_) / std::log(2))] = area;
        }
        start_page_id = tmp->start_page_id_;
        is_alloced = true;
        tmp->~ObTmpFileArea();
      }
    }

    if (!is_alloced) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "cannot alloc the page", K(ret), K_(max_cont_page_nums), K(page_nums));
    } else {
      index = std::ceil(std::log(max_cont_page_nums_) / std::log(2));
      int64_t max = 0;
      for (int32_t i = index; i >= 0; --i) {
        if (NULL == free_area_[i]) {
          // nothing to do.
        } else {
          max = free_area_[i]->page_nums_;
          break;
        }
      }
      max_cont_page_nums_ = max;
    }
  }
  return ret;
}

void ObTmpFilePageBuddy::free_align(const int32_t start_page_id, const int32_t page_nums, ObTmpFileArea*& area)
{
  ObTmpFileArea* tmp = NULL;
  int64_t nums = page_nums;
  int32_t start_id = start_page_id;
  while (NULL != (tmp = find_buddy(nums, start_id))) {
    // combine free area and buddy area.
    if (0 != (start_id % (2 * nums))) {
      start_id = tmp->start_page_id_;
      std::swap(area, tmp);
    }
    nums *= 2;
    tmp->~ObTmpFileArea();
    tmp = NULL;
  }
  area->start_page_id_ = start_id;
  area->page_nums_ = nums;
  area->next_ = free_area_[static_cast<int32_t>(std::log(nums) / std::log(2))];
  free_area_[static_cast<int32_t>(std::log(nums) / std::log(2))] = area;
  if (nums > max_cont_page_nums_) {
    max_cont_page_nums_ = nums;
  }
}

bool ObTmpFilePageBuddy::is_empty() const
{
  bool is_empty = true;
  for (int32_t i = 0; i < ObTmpFilePageBuddy::MAX_ORDER; ++i) {
    if (NULL == free_area_[i]) {
      is_empty = false;
      break;
    }
  }
  return is_empty;
}

int64_t ObTmpFilePageBuddy::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  bool first = true;
  ObTmpFileArea* area = NULL;
  common::databuff_printf(buf, buf_len, pos, "{");
  for (int32_t i = 0; i < ObTmpFilePageBuddy::MAX_ORDER; ++i) {
    area = free_area_[i];
    if (NULL != area) {
      common::databuff_print_kv(buf, buf_len, pos, "page_nums", static_cast<int64_t>(std::pow(2, i)));
      common::databuff_printf(buf, buf_len, pos, "{");
      while (NULL != area) {
        if (first) {
          first = false;
        } else {
          common::databuff_printf(buf, buf_len, pos, ",");
        }
        common::databuff_printf(buf, buf_len, pos, "{");
        common::databuff_print_kv(buf,
            buf_len,
            pos,
            "start_page_id",
            area->start_page_id_,
            "end_page_id",
            area->start_page_id_ + area->page_nums_);
        common::databuff_printf(buf, buf_len, pos, "}");
        area = area->next_;
      }
      common::databuff_printf(buf, buf_len, pos, "}");
    }
  }
  common::databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

void ObTmpFilePageBuddy::free(const int32_t start_page_id, const int32_t page_nums)
{
  int32_t start_id = start_page_id;
  int32_t nums = page_nums;
  int32_t length = 0;
  while (nums > 0) {
    /**
     * PURPOSE: align free area into power of 2.
     *
     *   The probable value of alloc_start_id:
     *    page nums                  start page id
     *       128       0 ---------------- 128 ------------------- 256
     *       64        0 ------ 64 ------ 128 ------- 192 ------- 256
     *       32        0 - 32 - 64 - 96 - 128 - 160 - 192 - 224 - 256
     *       ...                          ...
     *   So, the maximum number of consecutive pages from a start_page_id is the
     *   gcd(greatest common divisor) between it and 512, except 0. The maximum
     *   consecutive page nums of 0 is 256.
     *
     *   The layout of free area in alocated area :
     *        |<---------------alloc_page_nums--------------->|
     *                             <---- |<--free_page_nums-->|
     *        |==========================|====================|
     *   alloc_start                free_page_id       alloc_end
     *
     *   So, free_end always equal to alloc_end.
     *
     *   Based on two observations above, the algorithm is designed as follows:
     */
    length = 2;
    while (0 == start_id % length && length <= nums) {
      length *= 2;
    }
    length = std::min(length / 2, nums);

    char* buf = reinterpret_cast<char*>(&(buf_[start_id]));
    ObTmpFileArea* area = new (buf) ObTmpFileArea(start_id, length);
    free_align(area->start_page_id_, area->page_nums_, area);
    start_id += length;
    nums -= length;
  }
}

ObTmpFileArea* ObTmpFilePageBuddy::find_buddy(const int32_t page_nums, const int32_t start_page_id)
{
  ObTmpFileArea* tmp = NULL;
  if (get_max_page_nums() < page_nums || page_nums <= 0 || start_page_id < 0 || start_page_id >= get_max_page_nums()) {
    STORAGE_LOG(WARN, "invalid argument", K(page_nums), K(start_page_id));
  } else if (get_max_page_nums() == page_nums) {
    // no buddy, so, nothing to do.
  } else {
    tmp = free_area_[static_cast<int32_t>(std::log(page_nums) / std::log(2))];
    ObTmpFileArea* pre = tmp;
    int64_t start_id = 0;
    /**
     * case 1:                                     case 2:
     *          |<--page_nums-->|<--page_nums-->|           |<--page_nums-->|<--page_nums-->|
     *          |===============|===============|           |===============|===============|
     *    start_page_id      start_id(buddy)          start_id(buddy)   start_page_id
     */
    if (0 == (start_page_id % (2 * page_nums))) {  // case 1
      start_id = start_page_id + page_nums;
    } else {  // case 2
      start_id = start_page_id - page_nums;
    }
    while (NULL != tmp) {
      if (tmp->start_page_id_ == start_id) {
        if (pre == tmp) {
          free_area_[static_cast<int>(std::log(page_nums) / std::log(2))] = tmp->next_;
        } else {
          pre->next_ = tmp->next_;
        }
        tmp->next_ = NULL;
        break;
      }
      pre = tmp;
      tmp = tmp->next_;
    }
  }
  return tmp;
}

ObTmpMacroBlock::ObTmpMacroBlock()
    : block_id_(-1),
      dir_id_(-1),
      tenant_id_(0),
      page_buddy_(),
      free_page_nums_(0),
      buffer_(NULL),
      handle_(),
      using_extents_(),
      macro_block_handle_(),
      is_disked_(false),
      is_inited_(false),
      is_washing_(false)
{}

ObTmpMacroBlock::~ObTmpMacroBlock()
{
  destroy();
}

int ObTmpMacroBlock::init(
    const int64_t block_id, const int64_t dir_id, const uint64_t tenant_id, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else if (OB_FAIL(page_buddy_.init(allocator))) {
    STORAGE_LOG(WARN, "Fail to init the page buddy", K(ret));
  } else {
    block_id_ = block_id;
    dir_id_ = dir_id;
    tenant_id_ = tenant_id;
    free_page_nums_ = OB_TMP_FILE_STORE.get_mblk_page_nums();
    is_disked_ = false;
    is_washing_ = false;
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObTmpMacroBlock::destroy()
{
  using_extents_.reset();
  page_buddy_.destroy();
  macro_block_handle_.reset();
  block_id_ = -1;
  dir_id_ = -1;
  tenant_id_ = 0;
  free_page_nums_ = 0;
  buffer_ = NULL;
  handle_.reset();
  is_disked_ = false;
  is_washing_ = false;
  is_inited_ = false;
}

int ObTmpMacroBlock::close(bool& is_all_close)
{
  int ret = OB_SUCCESS;
  is_all_close = true;
  if (OB_UNLIKELY(!is_inited_)) {
    is_all_close = false;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited");
  } else {
    ObTmpFileExtent* tmp = NULL;
    for (int32_t i = 0; OB_SUCC(ret) && i < using_extents_.count() && is_all_close; i++) {
      tmp = using_extents_.at(i);
      if (NULL != tmp && !tmp->is_closed()) {
        int32_t start_id;
        int32_t page_nums;
        if (tmp->close(start_id, page_nums)) {
          if (-1 == start_id && 0 == page_nums) {
            // nothing to do
          } else if (start_id < 0 || page_nums < 0) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "fail to close the extent", K(ret), K(start_id), K(page_nums), K(*tmp));
          } else if (OB_FAIL(free(start_id, page_nums))) {
            STORAGE_LOG(WARN, "fail to free the extent", K(ret));
          }
          if (OB_FAIL(ret)) {
            tmp->unclose(page_nums);
            is_all_close = false;
          }
        } else {
          is_all_close = false;
        }
      }
    }
  }
  return ret;
}

int ObTmpMacroBlock::get_block_cache_handle(ObTmpBlockValueHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTmpBlockCacheKey key(block_id_, tenant_id_);
  SpinRLockGuard guard(lock_);
  if (!is_disked_) {
    handle = handle_;
  } else if (OB_FAIL(ObTmpBlockCache::get_instance().get_block(key, handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "fail to get tmp block from cache", K(ret), K(key));
    } else {
      STORAGE_LOG(INFO, "block cache miss", K(ret), K(key));
    }
  }
  return ret;
}

int ObTmpMacroBlock::give_back_buf_into_cache(bool is_wash)
{
  int ret = OB_SUCCESS;
  ObTmpBlockCacheKey key(block_id_, tenant_id_);
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(ObTmpBlockCache::get_instance().put_block(key, handle_))) {
    STORAGE_LOG(WARN, "fail to put block into block cache", K(ret), K(key));
  } else if (is_wash) {
    set_disked();
  }
  return ret;
}

int ObTmpMacroBlock::alloc_all_pages(ObTmpFileExtent& extent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else if (OB_FAIL(page_buddy_.alloc_all_pages())) {
    STORAGE_LOG(WARN, "Fail to allocate the tmp extent", K(ret), K_(block_id), K_(page_buddy));
  } else {
    extent.set_block_id(get_block_id());
    extent.set_start_page_id(0);
    extent.set_page_nums(OB_TMP_FILE_STORE.get_mblk_page_nums());
    extent.alloced();
    free_page_nums_ -= extent.get_page_nums();
    if (OB_FAIL(using_extents_.push_back(&extent))) {
      STORAGE_LOG(WARN, "Fail to push back into using_extexts", K(ret));
    }
  }
  return ret;
}

int ObTmpMacroBlock::alloc(const int32_t page_nums, ObTmpFileExtent& extent)
{
  int ret = OB_SUCCESS;
  int32_t start_page_id = extent.get_start_page_id();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else if (OB_FAIL(page_buddy_.alloc(page_nums, start_page_id))) {
    STORAGE_LOG(WARN, "Fail to allocate the tmp extent", K(ret), K_(block_id), K_(page_buddy));
  } else {
    extent.set_block_id(block_id_);
    extent.set_page_nums(page_nums);
    extent.set_start_page_id(start_page_id);
    extent.alloced();
    free_page_nums_ -= page_nums;
    if (OB_FAIL(using_extents_.push_back(&extent))) {
      STORAGE_LOG(WARN, "Fail to push back into using_extexts", K(ret));
      page_buddy_.free(extent.get_start_page_id(), extent.get_page_nums());
      free_page_nums_ += extent.get_page_nums();
      extent.reset();
    }
  }
  return ret;
}

int ObTmpMacroBlock::free(ObTmpFileExtent& extent)
{
  int ret = OB_SUCCESS;
  ObTmpFileExtent* tmp = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else {
    page_buddy_.free(extent.get_start_page_id(), extent.get_page_nums());
    free_page_nums_ += extent.get_page_nums();
    for (int64_t i = using_extents_.count() - 1; i >= 0; --i) {
      if (&extent == using_extents_.at(i)) {
        using_extents_.remove(i);
        break;
      }
    }
  }
  return ret;
}

int ObTmpMacroBlock::free(const int32_t start_page_id, const int32_t page_nums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else {
    page_buddy_.free(start_page_id, page_nums);
    free_page_nums_ += page_nums;
  }
  return ret;
}

void ObTmpMacroBlock::set_io_desc(const common::ObIODesc& io_desc)
{
  io_desc_ = io_desc;
}

ObTmpTenantMacroBlockManager::ObTmpTenantMacroBlockManager()
    : mblk_page_nums_(OB_FILE_SYSTEM.get_macro_block_size() / ObTmpMacroBlock::get_default_page_size() - 1),
      allocator_(),
      blocks_(),
      is_inited_(false)
{}

ObTmpTenantMacroBlockManager::~ObTmpTenantMacroBlockManager()
{
  destroy();
}

int ObTmpTenantMacroBlockManager::init(common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has been inited", K(ret));
  } else if (OB_FAIL(blocks_.create(MBLK_HASH_BUCKET_NUM, ObModIds::OB_TMP_BLOCK_MAP))) {
    STORAGE_LOG(WARN, "Fail to create tmp macro block map, ", K(ret));
  } else {
    allocator_ = &allocator;
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

int ObTmpTenantMacroBlockManager::alloc_macro_block(
    const int64_t dir_id, const uint64_t tenant_id, ObTmpMacroBlock*& t_mblk)
{
  int ret = OB_SUCCESS;
  void* block_buf = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else if (OB_ISNULL(block_buf = allocator_->alloc(sizeof(ObTmpMacroBlock)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc a buf", K(ret));
  } else if (OB_ISNULL(t_mblk = new (block_buf) ObTmpMacroBlock())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObTmpMacroBlock", K(ret));
  } else if (OB_FAIL(t_mblk->init(get_next_blk_id(), dir_id, tenant_id, *allocator_))) {
    STORAGE_LOG(WARN, "fail to init tmp block", K(ret));
  } else if (OB_FAIL(blocks_.set_refactored(t_mblk->get_block_id(), t_mblk))) {
    STORAGE_LOG(WARN, "fail to set tmp macro block map", K(ret), K(t_mblk));
  }
  if (OB_FAIL(ret)) {
    if (NULL != t_mblk) {
      int tmp_ret = OB_SUCCESS;
      t_mblk->~ObTmpMacroBlock();
      allocator_->free(block_buf);
      if (OB_SUCCESS != (tmp_ret = blocks_.erase_refactored(t_mblk->get_block_id()))) {
        STORAGE_LOG(WARN, "fail to erase from tmp macro block map", K(tmp_ret), K(t_mblk));
      }
    }
  }
  return ret;
}

int ObTmpTenantMacroBlockManager::get_macro_block(const int64_t block_id, ObTmpMacroBlock*& t_mblk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else if (block_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(blocks_.get_refactored(block_id, t_mblk))) {
    STORAGE_LOG(WARN, "fail to get tmp macro block", K(ret));
  }
  return ret;
}

int ObTmpTenantMacroBlockManager::free_macro_block(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else if (block_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(blocks_.erase_refactored(block_id))) {
    STORAGE_LOG(WARN, "fail to erase tmp macro block", K(ret));
  }
  return ret;
}

int ObTmpTenantMacroBlockManager::get_disk_macro_block_list(common::ObIArray<MacroBlockId>& macro_id_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else {
    TmpMacroBlockMap::iterator iter;
    ObTmpMacroBlock* tmp = NULL;
    for (iter = blocks_.begin(); OB_SUCC(ret) && iter != blocks_.end(); ++iter) {
      if (OB_ISNULL(tmp = iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to iterate tmp macro block map", K(ret));
      } else if (tmp->is_disked() && tmp->get_macro_block_id().is_valid() &&
                 OB_FAIL(macro_id_list.push_back(tmp->get_macro_block_id()))) {
        STORAGE_LOG(WARN, "fail to push back macro block id", K(ret), K(tmp->get_macro_block_id()));
      }
    }
  }

  return ret;
}

void ObTmpTenantMacroBlockManager::print_block_usage()
{
  int64_t disk_count = 0;
  int64_t disk_fragment = 0;
  int64_t mem_count = 0;
  int64_t mem_fragment = 0;
  TmpMacroBlockMap::iterator iter;
  ObTmpMacroBlock* tmp = NULL;
  for (iter = blocks_.begin(); iter != blocks_.end(); ++iter) {
    tmp = iter->second;
    if (tmp->is_disked()) {
      disk_count++;
      disk_fragment += tmp->get_free_page_nums();
    } else {
      mem_count++;
      mem_fragment += tmp->get_free_page_nums();
    }
  }
  double disk_fragment_ratio = 0;
  if (0 != disk_count) {
    disk_fragment_ratio = disk_fragment * 1.0 / (disk_count * mblk_page_nums_);
  }
  double mem_fragment_ratio = 0;
  if (0 != mem_count) {
    mem_fragment_ratio = mem_fragment * 1.0 / (mem_count * mblk_page_nums_);
  }
  STORAGE_LOG(INFO,
      "the block usage for temporary files",
      K(disk_count),
      K(disk_fragment),
      K(disk_fragment_ratio),
      K(mem_count),
      K(mem_fragment),
      K(mem_fragment_ratio));
}

void ObTmpTenantMacroBlockManager::destroy()
{
  TmpMacroBlockMap::iterator iter;
  ObTmpMacroBlock* tmp = NULL;
  for (iter = blocks_.begin(); iter != blocks_.end(); ++iter) {
    if (OB_NOT_NULL(tmp = iter->second)) {
      tmp->~ObTmpMacroBlock();
      allocator_->free(tmp);
    }
  }
  blocks_.destroy();
  allocator_ = NULL;
  is_inited_ = false;
}

int64_t ObTmpTenantMacroBlockManager::get_next_blk_id()
{
  int64_t next_blk_id = -1;
  int64_t old_val = ATOMIC_LOAD(&next_blk_id_);
  int64_t new_val = 0;
  bool finish = false;
  while (!finish) {
    new_val = (old_val + 1) % INT64_MAX;
    next_blk_id = new_val;
    finish = (old_val == (new_val = ATOMIC_VCAS(&next_blk_id_, old_val, new_val)));
    old_val = new_val;
  }
  return next_blk_id;
}

ObTmpTenantFileStore::ObTmpTenantFileStore()
    : tmp_block_manager_(), tmp_mem_block_manager_(), page_cache_(NULL), file_handle_(), allocator_(), is_inited_(false)
{}

ObTmpTenantFileStore::~ObTmpTenantFileStore()
{
  destroy();
}

int ObTmpTenantFileStore::init(const uint64_t tenant_id, const ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  int n_len = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "fail to init allocator", K(ret));
  } else if (OB_ISNULL(page_cache_ = &ObTmpPageCache::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get the page cache", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.init(allocator_))) {
    STORAGE_LOG(WARN, "fail to init the block manager for ObTmpFileStore", K(ret));
  } else if (OB_FAIL(tmp_mem_block_manager_.init(tenant_id, allocator_, file_handle))) {
    STORAGE_LOG(WARN, "fail to init memory block manager", K(ret));
  } else if (OB_FAIL(file_handle_.assign(file_handle))) {
    STORAGE_LOG(WARN, "fail to assign file handle", K(ret), K(file_handle));
  } else {
    allocator_.set_label(ObModIds::OB_TMP_BLOCK_MANAGER);
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObTmpTenantFileStore::destroy()
{
  tmp_mem_block_manager_.destroy();
  tmp_block_manager_.destroy();
  if (NULL != page_cache_) {
    page_cache_ = NULL;
  }
  allocator_.destroy();
  file_handle_.reset();
  is_inited_ = false;
}

int ObTmpTenantFileStore::alloc(
    const int64_t dir_id, const uint64_t tenant_id, const int64_t size, ObTmpFileExtent& extent)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = size;
  int64_t block_size = tmp_block_manager_.get_block_size();
  // In buddy allocation, if free space in one block isn't powers of 2, need upper align.
  int64_t max_cont_size_per_block =
      (tmp_block_manager_.get_mblk_page_nums() + 1) / 2 * ObTmpMacroBlock::get_default_page_size();
  ObTmpMacroBlock* t_mblk = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (alloc_size <= 0 || alloc_size > block_size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(alloc_size));
  } else {
    SpinWLockGuard guard(lock_);
    common::ObSEArray<ObTmpMacroBlock*, 1> free_blocks;
    if (alloc_size > max_cont_size_per_block) {
      if (OB_FAIL(tmp_mem_block_manager_.try_wash(tenant_id, free_blocks))) {
        STORAGE_LOG(WARN, "fail to try wash tmp macro block", K(ret), K(dir_id), K(tenant_id));
      } else if (free_blocks.count() > 0) {
        for (int32_t i = free_blocks.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
          if (free_blocks.at(i)->is_empty()) {
            if (OB_FAIL(free_macro_block(free_blocks.at(i)))) {
              STORAGE_LOG(WARN, "fail to free tmp macro block", K(ret));
            }
          }
          free_blocks.at(i) = NULL;
        }
        free_blocks.reset();
      } else if (OB_FAIL(alloc_macro_block(dir_id, tenant_id, t_mblk))) {
        STORAGE_LOG(WARN, "cannot allocate a tmp macro block", K(ret), K(dir_id), K(tenant_id));
      } else if (OB_FAIL(t_mblk->alloc_all_pages(extent))) {
        STORAGE_LOG(WARN, "Fail to allocate the tmp extent", K(ret), K(t_mblk->get_block_id()));
      } else {
        if (alloc_size < block_size) {
          int64_t nums = std::ceil(alloc_size * 1.0 / ObTmpMacroBlock::get_default_page_size());
          if (OB_FAIL(free_extent(t_mblk->get_block_id(), nums, tmp_block_manager_.get_mblk_page_nums() - nums))) {
            STORAGE_LOG(WARN, "fail to free pages", K(ret), K(t_mblk->get_block_id()));
          } else {
            extent.set_page_nums(nums);
          }
        }
      }
    } else {
      if (OB_FAIL(tmp_mem_block_manager_.alloc_extent(dir_id, tenant_id, alloc_size, extent, free_blocks))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          if (free_blocks.count() > 0) {
            for (int32_t i = free_blocks.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
              if (free_blocks.at(i)->is_empty()) {
                if (OB_FAIL(free_macro_block(free_blocks.at(i)))) {
                  STORAGE_LOG(WARN, "fail to free tmp macro block", K(ret));
                }
              }
              free_blocks.at(i) = NULL;
            }
            free_blocks.reset();
          } else if (OB_FAIL(alloc_macro_block(dir_id, tenant_id, t_mblk))) {
            STORAGE_LOG(WARN, "cannot allocate a tmp macro block", K(ret), K(dir_id), K(tenant_id));
          } else if (OB_FAIL(tmp_mem_block_manager_.alloc_extent(dir_id, tenant_id, alloc_size, extent, free_blocks))) {
            STORAGE_LOG(WARN, "fail to alloc tmp extent", K(ret));
            int tmp_ret = OB_SUCCESS;
            if (free_blocks.count() > 0) {
              for (int32_t i = free_blocks.count() - 1; OB_SUCC(tmp_ret) && i >= 0; i--) {
                if (free_blocks.at(i)->is_empty()) {
                  if (OB_SUCCESS != (tmp_ret = free_macro_block(free_blocks.at(i)))) {
                    STORAGE_LOG(WARN, "fail to free tmp macro block", K(tmp_ret));
                  }
                }
                free_blocks.at(i) = NULL;
              }
              free_blocks.reset();
            }
          }
        } else {
          STORAGE_LOG(WARN, "fail to alloc tmp extent", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTmpTenantFileStore::free(ObTmpFileExtent* extent)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(free_extent(extent))) {
    STORAGE_LOG(WARN, "fail to free the extent", K(ret), K(*extent));
  }
  return ret;
}

int ObTmpTenantFileStore::free(const int64_t block_id, const int32_t start_page_id, const int32_t page_nums)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(free_extent(block_id, start_page_id, page_nums))) {
    STORAGE_LOG(WARN, "fail to free the extent", K(ret), K(block_id), K(start_page_id), K(page_nums));
  }
  return ret;
}

int ObTmpTenantFileStore::free_extent(ObTmpFileExtent* extent)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock* t_mblk = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_ISNULL(extent) || OB_UNLIKELY(!extent->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(*extent));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(extent->get_block_id(), t_mblk))) {
    STORAGE_LOG(WARN, "fail to get tmp macro block", K(ret));
  } else if (OB_FAIL(t_mblk->free(*extent))) {
    STORAGE_LOG(WARN, "fail to free extent", K(ret));
  } else {
    if (!t_mblk->is_disked()) {
      if (OB_FAIL(tmp_mem_block_manager_.free_extent(extent->get_page_nums(), t_mblk))) {
        STORAGE_LOG(WARN, "fail to free extent in block cache", K(ret));
      }
    }
    extent->reset();
    if (OB_SUCC(ret) && t_mblk->is_empty()) {
      if (OB_FAIL(free_macro_block(t_mblk))) {
        STORAGE_LOG(WARN, "fail to free tmp macro block", K(ret));
      }
    }
  }
  return ret;
}

int ObTmpTenantFileStore::free_extent(const int64_t block_id, const int32_t start_page_id, const int32_t page_nums)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock* t_mblk = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (start_page_id < 0 || page_nums < 0 || block_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id), K(start_page_id), K(page_nums));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(block_id, t_mblk))) {
    STORAGE_LOG(WARN, "fail to get tmp block", K(ret), K(block_id), K(start_page_id), K(page_nums));
  } else if (OB_FAIL(t_mblk->free(start_page_id, page_nums))) {
    STORAGE_LOG(WARN, "fail to free extent", K(ret));
  } else {
    if (!t_mblk->is_disked()) {
      if (OB_FAIL(tmp_mem_block_manager_.free_extent(page_nums, t_mblk))) {
        STORAGE_LOG(WARN, "fail to free extent in block cache", K(ret));
      }
    }
    if (OB_SUCC(ret) && t_mblk->is_empty()) {
      if (OB_FAIL(free_macro_block(t_mblk))) {
        STORAGE_LOG(WARN, "fail to free tmp macro block", K(ret));
      }
    }
  }
  return ret;
}

int ObTmpTenantFileStore::free_macro_block(ObTmpMacroBlock*& t_mblk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (NULL == t_mblk) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else {
    if (!t_mblk->is_disked()) {
      if (OB_FAIL(t_mblk->give_back_buf_into_cache())) {
        STORAGE_LOG(WARN, "fail to put tmp block cache", K(ret), K(t_mblk));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tmp_block_manager_.free_macro_block(t_mblk->get_block_id()))) {
        STORAGE_LOG(WARN, "fail to free tmp macro block for block manager", K(ret));
      } else if (!t_mblk->is_disked()) {
        if (OB_FAIL(tmp_mem_block_manager_.free_macro_block(t_mblk->get_block_id()))) {
          STORAGE_LOG(WARN, "fail to free tmp macro block for block cache", K(ret));
        } else {
          // in case of repeatedly put tmp block cache
          t_mblk->set_disked();
        }
      } else if (!t_mblk->get_macro_block_handle().get_io_handle().is_empty() &&
                 OB_FAIL(tmp_mem_block_manager_.wait_write_io_finish())) {  // in case of doing write io
        STORAGE_LOG(WARN, "fail to wait write io finish", K(ret), K(t_mblk));
      }
      STORAGE_LOG(INFO, "finish to free a block", K(ret), K(*t_mblk));
      t_mblk->~ObTmpMacroBlock();
      allocator_.free(t_mblk);
    }
  }
  return ret;
}

int ObTmpTenantFileStore::alloc_macro_block(const int64_t dir_id, const uint64_t tenant_id, ObTmpMacroBlock*& t_mblk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.alloc_macro_block(dir_id, tenant_id, t_mblk))) {
    STORAGE_LOG(WARN, "cannot allocate a tmp macro block", K(ret), K(dir_id), K(tenant_id));
  } else {
    ObTmpBlockCacheKey key(t_mblk->get_block_id(), tenant_id);
    if (OB_FAIL(tmp_mem_block_manager_.alloc_buf(key, t_mblk->get_handle()))) {
      STORAGE_LOG(WARN, "fail to alloc block cache buf", K(ret));
    } else {
      t_mblk->set_buffer(t_mblk->get_handle().value_->get_buffer());
      if (OB_FAIL(tmp_mem_block_manager_.add_macro_block(tenant_id, t_mblk))) {
        STORAGE_LOG(WARN, "fail to put meta into block cache", K(ret), K(t_mblk));
      }
      if (OB_FAIL(ret)) {
        tmp_block_manager_.free_macro_block(t_mblk->get_block_id());
        tmp_mem_block_manager_.free_macro_block(t_mblk->get_block_id());
        t_mblk->give_back_buf_into_cache();
      }
    }
  }
  return ret;
}

int ObTmpTenantFileStore::read(ObTmpBlockIOInfo& io_info, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTmpBlockValueHandle tb_handle;
  ObTmpMacroBlock* block = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (!handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(handle));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(io_info.block_id_, block))) {
    STORAGE_LOG(WARN, "fail to get block from tmp block manager", K(ret), K_(io_info.block_id));
  } else if (NULL == block) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the block is NULL", K(ret), K_(io_info.block_id));
  } else if (OB_SUCC(block->get_block_cache_handle(tb_handle))) {
    ObTmpFileIOHandle::ObBlockCacheHandle block_handle(tb_handle, io_info.buf_, io_info.offset_, io_info.size_);
    if (OB_FAIL(handle.get_block_cache_handles().push_back(block_handle))) {
      STORAGE_LOG(WARN, "Fail to push back into block_handles", K(ret), K(block_handle));
    }
  } else if (OB_SUCC(read_page(block, io_info, handle))) {
    // nothing to do.
  } else {
    STORAGE_LOG(WARN, "fail to read", K(ret));
  }

  if (OB_SUCC(ret)) {
    block->set_io_desc(io_info.io_desc_);
  }
  return ret;
}

int ObTmpTenantFileStore::read_page(ObTmpMacroBlock* block, ObTmpBlockIOInfo& io_info, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  int64_t page_start_id = io_info.offset_ / ObTmpMacroBlock::get_default_page_size();
  int64_t offset = io_info.offset_ % ObTmpMacroBlock::get_default_page_size();
  int64_t remain_size = io_info.size_;
  int64_t size = std::min(ObTmpMacroBlock::get_default_page_size() - offset, remain_size);
  int32_t page_nums = 0;
  common::ObSEArray<ObTmpPageIOInfo, 255> page_io_infos;
  do {
    ObTmpPageCacheKey key(io_info.block_id_, page_start_id, io_info.tenant_id_);
    ObTmpPageValueHandle p_handle;
    if (OB_SUCC(page_cache_->get_page(key, p_handle))) {
      ObTmpFileIOHandle::ObPageCacheHandle page_handle(p_handle,
          io_info.buf_ + ObTmpMacroBlock::calculate_offset(page_start_id, offset) - io_info.offset_,
          offset,
          size);
      if (OB_FAIL(handle.get_page_cache_handles().push_back(page_handle))) {
        STORAGE_LOG(WARN, "Fail to push back into page_handles", K(ret));
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // accumulate page io info.
      ObTmpPageIOInfo page_io_info;
      page_io_info.key_ = key;
      page_io_info.offset_ = offset;
      page_io_info.size_ = size;
      if (OB_FAIL(page_io_infos.push_back(page_io_info))) {
        STORAGE_LOG(WARN, "Fail to push back into page_io_infos", K(ret), K(page_io_info));
      }
    }
    page_nums++;
    page_start_id++;
    offset = 0;
    remain_size -= size;
    size = std::min(ObTmpMacroBlock::get_default_page_size(), remain_size);
  } while (OB_SUCC(ret) && size > 0);
  // guarantee read io after the finished write.
  if (OB_SUCC(ret) && OB_FAIL(wait_write_io_finish_if_need())) {
    STORAGE_LOG(WARN, "fail to wait previous write io", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (page_io_infos.count() > DEFAULT_PAGE_IO_MERGE_RATIO * page_nums) {
      // merge multi page io into one.
      ObMacroBlockHandle mb_handle;
      ObTmpBlockIOInfo info(io_info);
      info.offset_ = common::lower_align(io_info.offset_, ObTmpMacroBlock::get_default_page_size());
      info.size_ = page_nums * ObTmpMacroBlock::get_default_page_size();
      info.macro_block_id_ = block->get_macro_block_id();
      if (OB_FAIL(page_cache_->prefetch(info, page_io_infos, handle, mb_handle))) {
        STORAGE_LOG(WARN, "fail to prefetch multi tmp page", K(ret));
      } else {
        ObTmpFileIOHandle::ObIOReadHandle read_handle(
            mb_handle, io_info.buf_, io_info.offset_ - info.offset_, io_info.size_);
        if (OB_FAIL(handle.get_io_handles().push_back(read_handle))) {
          STORAGE_LOG(WARN, "Fail to push back into read_handles", K(ret));
        }
      }
    } else {
      // just do io, page by page.
      for (int i = 0; OB_SUCC(ret) && i < page_io_infos.count(); i++) {
        ObMacroBlockHandle mb_handle;
        ObTmpBlockIOInfo info(io_info);
        info.offset_ = page_io_infos.at(i).key_.get_page_id() * ObTmpMacroBlock::get_default_page_size();
        info.size_ = ObTmpMacroBlock::get_default_page_size();
        info.macro_block_id_ = block->get_macro_block_id();
        if (OB_FAIL(page_cache_->prefetch(page_io_infos.at(i).key_, info, handle, mb_handle))) {
          STORAGE_LOG(WARN, "fail to prefetch tmp page", K(ret));
        } else {
          char* buf =
              io_info.buf_ +
              ObTmpMacroBlock::calculate_offset(page_io_infos.at(i).key_.get_page_id(), page_io_infos.at(i).offset_) -
              io_info.offset_;
          ObTmpFileIOHandle::ObIOReadHandle read_handle(
              mb_handle, buf, page_io_infos.at(i).offset_, page_io_infos.at(i).size_);
          if (OB_FAIL(handle.get_io_handles().push_back(read_handle))) {
            STORAGE_LOG(WARN, "Fail to push back into read_handles", K(ret));
          }
        }
      }
    }
    page_io_infos.reset();
  } else {
    STORAGE_LOG(WARN, "fail to get page from page cache", K(ret));
  }
  return ret;
}

int ObTmpTenantFileStore::wait_write_io_finish_if_need()
{
  // guarantee read io after the finished write.
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (tmp_mem_block_manager_.check_need_wait_write()) {
    if (OB_FAIL(tmp_mem_block_manager_.wait_write_io_finish())) {
      STORAGE_LOG(WARN, "fail to wait previous write io", K(ret));
    }
  }
  return ret;
}

int ObTmpTenantFileStore::write(const ObTmpBlockIOInfo& io_info)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock* block = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(io_info.block_id_, block))) {
    STORAGE_LOG(WARN, "fail to get block from tmp block manager", K(ret), K_(io_info.block_id));
  } else if (NULL == block) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the block is NULL", K(ret), K_(io_info.block_id));
  } else if (!block->is_disked()) {
    block->set_io_desc(io_info.io_desc_);
    MEMCPY(block->get_buffer() + io_info.offset_, io_info.buf_, io_info.size_);
    // TODO: wash this block
    if (0 == block->get_free_page_nums()) {
      // NOTICE:
      // 1. The extent info must be update before this. But, it doesn't support
      //   this at present. So, the free_page_nums is not up to date. If no modify,
      //   it could bring the program into a error status.
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "fail to write tmp block, because of not support update-in-place", K(ret), K(io_info));
    // TODO: 1. read io. 2. write io.
  }
  return ret;
}

int ObTmpTenantFileStore::get_disk_macro_block_list(common::ObIArray<MacroBlockId>& macro_id_list)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.get_disk_macro_block_list(macro_id_list))) {
    STORAGE_LOG(WARN, "fail to get disk macro block list from tmp_block_manager_", K(ret));
  }
  return ret;
}

ObTmpFileStore& ObTmpFileStore::get_instance()
{
  static ObTmpFileStore instance;
  return instance;
}

ObTmpFileStore::ObTmpFileStore() : tenant_file_stores_(), file_handle_(), allocator_(), lock_(), is_inited_(false)
{}

ObTmpFileStore::~ObTmpFileStore()
{}

int ObTmpFileStore::init(const ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else if (OB_FAIL(allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "fail to init allocator", K(ret));
  } else if (OB_FAIL(
                 ObTmpPageCache::get_instance().init("tmp_page_cache", TMP_FILE_PAGE_CACHE_PRIORITY, file_handle))) {
    STORAGE_LOG(WARN, "Fail to init tmp page cache, ", K(ret));
  } else if (OB_FAIL(ObTmpBlockCache::get_instance().init("tmp_block_cache", TMP_FILE_BLOCK_CACHE_PRIORITY))) {
    STORAGE_LOG(WARN, "Fail to init tmp tenant block cache, ", K(ret));
  } else if (OB_FAIL(tenant_file_stores_.create(STORE_HASH_BUCKET_NUM, ObModIds::OB_TMP_FILE_STORE_MAP))) {
    STORAGE_LOG(WARN, "Fail to create tmp tenant file store map, ", K(ret));
  } else if (OB_FAIL(file_handle_.assign(file_handle))) {
    STORAGE_LOG(WARN, "Fail to assign file handle", K(ret), K(file_handle));
  } else {
    allocator_.set_label(ObModIds::OB_TMP_FILE_STORE_MAP);
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

int ObTmpFileStore::alloc(const int64_t dir_id, const uint64_t tenant_id, const int64_t size, ObTmpFileExtent& extent)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStore* store = NULL;
  if (OB_FAIL(OB_TMP_FILE_STORE.get_store(tenant_id, store))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else if (OB_FAIL(store->alloc(dir_id, tenant_id, size, extent))) {
    STORAGE_LOG(WARN, "fail to allocate extents", K(ret), K(tenant_id), K(dir_id), K(size), K(extent));
  }
  return ret;
}

int ObTmpFileStore::read(const uint64_t tenant_id, ObTmpBlockIOInfo& io_info, ObTmpFileIOHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStore* store = NULL;
  if (OB_FAIL(get_store(tenant_id, store))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id), K(tenant_id), K(io_info), K(handle));
  } else if (OB_FAIL(store->read(io_info, handle))) {
    STORAGE_LOG(WARN, "fail to read the extent", K(ret), K(tenant_id), K(io_info), K(handle));
  }
  return ret;
}

int ObTmpFileStore::write(const uint64_t tenant_id, const ObTmpBlockIOInfo& io_info)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStore* store = NULL;
  if (OB_FAIL(get_store(tenant_id, store))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id), K(io_info));
  } else if (OB_FAIL(store->write(io_info))) {
    STORAGE_LOG(WARN, "fail to write the extent", K(ret), K(tenant_id), K(io_info));
  }
  return ret;
}

int ObTmpFileStore::free(const uint64_t tenant_id, ObTmpFileExtent* extent)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStore* store = NULL;
  if (OB_FAIL(get_store(tenant_id, store))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id), K(*extent));
  } else if (OB_FAIL(store->free(extent))) {
    STORAGE_LOG(WARN, "fail to free extents", K(ret), K(tenant_id), K(*extent));
  }
  return ret;
}

int ObTmpFileStore::free(
    const uint64_t tenant_id, const int64_t block_id, const int32_t start_page_id, const int32_t page_nums)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStore* store = NULL;
  if (OB_FAIL(get_store(tenant_id, store))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else if (OB_FAIL(store->free(block_id, start_page_id, page_nums))) {
    STORAGE_LOG(WARN, "fail to free", K(ret), K(tenant_id), K(block_id), K(start_page_id), K(page_nums));
  }
  return ret;
}

int ObTmpFileStore::free_tenant_file_store(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStore* store = NULL;
  if (OB_FAIL(tenant_file_stores_.erase_refactored(tenant_id, &store))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      STORAGE_LOG(WARN, "fail to erase tmp tenant file store", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(store)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexcepted error, store is null", K(ret));
  } else {
    store->~ObTmpTenantFileStore();
    allocator_.free(store);
  }
  return ret;
}

int ObTmpFileStore::get_macro_block_list(common::ObIArray<MacroBlockId>& macro_id_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else {
    macro_id_list.reset();
    TenantFileStoreMap::iterator iter;
    ObTmpTenantFileStore* tmp = NULL;
    for (iter = tenant_file_stores_.begin(); OB_SUCC(ret) && iter != tenant_file_stores_.end(); ++iter) {
      if (OB_ISNULL(tmp = iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to iterate tmp tenant file store", K(ret));
      } else if (OB_FAIL(tmp->get_disk_macro_block_list(macro_id_list))) {
        STORAGE_LOG(WARN, "fail to get list of tenant macro block in disk", K(ret));
      }
    }
  }
  return ret;
}

int ObTmpFileStore::get_macro_block_list(ObIArray<TenantTmpBlockCntPair>& tmp_block_cnt_pairs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else {
    tmp_block_cnt_pairs.reset();
    common::ObSEArray<MacroBlockId, 64> macro_id_list;
    TenantFileStoreMap::iterator iter;
    ObTmpTenantFileStore* tmp = NULL;
    for (iter = tenant_file_stores_.begin(); OB_SUCC(ret) && iter != tenant_file_stores_.end(); ++iter) {
      TenantTmpBlockCntPair pair;
      macro_id_list.reset();
      if (OB_ISNULL(tmp = iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to iterate tmp tenant file store", K(ret));
      } else if (OB_FAIL(tmp->get_disk_macro_block_list(macro_id_list))) {
        STORAGE_LOG(WARN, "fail to get list of tenant macro block in disk", K(ret));
      } else if (OB_FAIL(pair.init(iter->first, macro_id_list.count()))) {
        STORAGE_LOG(WARN,
            "fail to init tenant tmp block count pair",
            K(ret),
            "tenant id",
            iter->first,
            "macro block count",
            macro_id_list.count());
      } else if (OB_FAIL(tmp_block_cnt_pairs.push_back(pair))) {
        STORAGE_LOG(WARN, "fail to push back tmp_block_cnt_pairs", K(ret), K(pair));
      }
    }
  }
  return ret;
}

int ObTmpFileStore::get_store(const uint64_t tenant_id, ObTmpTenantFileStore*& store)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret), K(tenant_id));
  } else {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(tenant_file_stores_.get_refactored(tenant_id, store))) {
      if (OB_HASH_NOT_EXIST == ret) {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTmpTenantFileStore)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to alloc a buf", K(ret), K(tenant_id));
        } else if (OB_ISNULL(store = new (buf) ObTmpTenantFileStore())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "fail to new a ObTmpTenantFileStore", K(ret), K(tenant_id));
        } else if (OB_FAIL(store->init(tenant_id, file_handle_))) {
          store->~ObTmpTenantFileStore();
          allocator_.free(store);
          store = NULL;
          STORAGE_LOG(WARN, "fail to init ObTmpTenantFileStore", K(ret), K(tenant_id));
        } else if (OB_FAIL(tenant_file_stores_.set_refactored(tenant_id, store))) {
          STORAGE_LOG(WARN, "fail to set tenant_file_stores_", K(ret), K(tenant_id));
        }
      } else {
        STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

void ObTmpFileStore::destroy()
{
  ObTmpPageCache::get_instance().destroy();
  TenantFileStoreMap::iterator iter;
  ObTmpTenantFileStore* tmp = NULL;
  for (iter = tenant_file_stores_.begin(); iter != tenant_file_stores_.end(); ++iter) {
    if (OB_NOT_NULL(tmp = iter->second)) {
      tmp->~ObTmpTenantFileStore();
      allocator_.free(tmp);
    }
  }
  tenant_file_stores_.destroy();
  file_handle_.reset();
  allocator_.destroy();
  ObTmpBlockCache::get_instance().destory();
  is_inited_ = false;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
