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
#include "share/ob_task_define.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace blocksstable
{

const int64_t ObTmpMacroBlock::DEFAULT_PAGE_SIZE = 8192L; // 8kb

ObTmpFilePageBuddy::ObTmpFilePageBuddy()
  : is_inited_(false),
    max_cont_page_nums_(0),
    buf_(NULL),
    allocator_(NULL)
{
  MEMSET(free_area_, 0, sizeof(free_area_));
}

ObTmpFilePageBuddy::~ObTmpFilePageBuddy()
{
  destroy();
}

int ObTmpFilePageBuddy::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  uint8_t start_id = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFilePageBuddy has not been inited", K(ret));
  } else {
    allocator_ = &allocator;
    buf_ = reinterpret_cast<ObTmpFileArea *>(allocator_->alloc(sizeof(ObTmpFileArea) * MAX_PAGE_NUMS));
    if (OB_ISNULL(buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc a buf", K(ret));
    } else {
      max_cont_page_nums_ = std::pow(2, MAX_ORDER - 1);
      /**
       *  page buddy free_list for a new block:
       *  -------------- --------- --------- --------- --------- --------- --------- --------- -------
       * |cont_page_nums|    1    |    2    |    4    |    8    |   16    |   32    |   64    |  128  |
       *  -------------- --------- --------- --------- --------- --------- --------- --------- -------
       * |  free_area   |[254,254]|[252,253]|[248,251]|[240,247]|[224,239]|[192,223]|[128,191]|[0,127]|
       *  -------------- --------- --------- --------- --------- --------- --------- --------- -------
       */
      uint8_t nums = max_cont_page_nums_;
      for (int32_t i = MIN_ORDER - 1; i >= 0; --i) {
        free_area_[i] = NULL;
      }
      for (int32_t i = MAX_ORDER - 1; i >= MIN_ORDER; --i) {
        char *buf = reinterpret_cast<char *>(&(buf_[start_id]));
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
  ObTmpFileArea *tmp = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFilePageBuddy has not been inited", K(ret));
  } else if (is_empty()) {
    for (int32_t i = 0; i < MAX_ORDER; ++i) {
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

int ObTmpFilePageBuddy::alloc(const uint8_t page_nums,
                              uint8_t &start_page_id,
                              uint8_t &alloced_page_nums)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFilePageBuddy has not been inited", K(ret));
  } else if (OB_UNLIKELY(page_nums <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(page_nums));
  } else {
    int32_t index = std::ceil(std::log(page_nums)/std::log(2));
    bool is_alloced = false;
    for (int32_t i = index; i < MAX_ORDER && !is_alloced; ++i) {
      if (NULL != free_area_[i]) {
        int64_t num = i - index;
        ObTmpFileArea *tmp = free_area_[i];
        free_area_[i] = tmp->next_;
        tmp->next_ = NULL;
        while (num--) {
          tmp->page_nums_ /= 2;
          char *buf = reinterpret_cast<char *>(&(buf_[tmp->start_page_id_ + tmp->page_nums_]));
          ObTmpFileArea *area = new (buf) ObTmpFileArea(tmp->start_page_id_ + tmp->page_nums_,
              tmp->page_nums_);
          area->next_ = free_area_[static_cast<int32_t>(std::log(tmp->page_nums_)/std::log(2))];
          free_area_[static_cast<int32_t>(std::log(tmp->page_nums_)/std::log(2))] = area;
        }
        start_page_id = tmp->start_page_id_;
        alloced_page_nums = std::pow(2, index);
        is_alloced = true;
        tmp->~ObTmpFileArea();
      }
    }

    if (!is_alloced) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "cannot alloc the page", K(ret), K_(max_cont_page_nums), K(page_nums));
    } else {
      index = std::ceil(std::log(max_cont_page_nums_)/std::log(2));
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

void ObTmpFilePageBuddy::free_align(const int32_t start_page_id, const int32_t page_nums,
    ObTmpFileArea *&area)
{
  ObTmpFileArea *tmp = NULL;
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
  area->next_ = free_area_[static_cast<int32_t>(std::log(nums)/std::log(2))];
  free_area_[static_cast<int32_t>(std::log(nums)/std::log(2))] = area;
  if (nums > max_cont_page_nums_) {
    max_cont_page_nums_ = nums;
  }
}

bool ObTmpFilePageBuddy::is_empty() const
{
  bool is_empty = true;
  for (int32_t i = 0; i < MIN_ORDER && is_empty; ++i) {
    if (NULL != free_area_[i]) {
      is_empty = false;
      break;
    }
  }
  for (int32_t i = MIN_ORDER; i < MAX_ORDER && is_empty; ++i) {
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
  ObTmpFileArea *area = NULL;
  common::databuff_printf(buf, buf_len, pos, "{");
  for (int32_t i = 0; i < MAX_ORDER; ++i) {
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
        common::databuff_print_kv(buf, buf_len, pos, "start_page_id", area->start_page_id_,
            "end_page_id", area->start_page_id_ + area->page_nums_);
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
  if (OB_UNLIKELY(start_page_id + page_nums >= std::pow(2, MAX_ORDER))) {
    STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "page id more than max numbers in block", K(start_page_id), K(page_nums));
    ob_abort();
  } else {
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
      while(0 == start_id % length && length <= nums) {
        length *= 2;
      }
      length = std::min(length / 2, nums);

      char *buf = reinterpret_cast<char *>(&(buf_[start_id]));
      ObTmpFileArea *area = new (buf) ObTmpFileArea(start_id, length);
      free_align(area->start_page_id_, area->page_nums_, area);
      start_id += length;
      nums -= length;
    }
  }
}

ObTmpFileArea *ObTmpFilePageBuddy::find_buddy(const int32_t page_nums, const int32_t start_page_id)
{
  ObTmpFileArea *tmp = NULL;
  if (MAX_PAGE_NUMS < page_nums || page_nums <= 0 || start_page_id < 0
      || start_page_id >= MAX_PAGE_NUMS) {
    STORAGE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid argument", K(page_nums), K(start_page_id));
  } else if (MAX_PAGE_NUMS == page_nums) {
    // no buddy, so, nothing to do.
  } else {
    tmp = free_area_[static_cast<int32_t>(std::log(page_nums)/std::log(2))];
    ObTmpFileArea *pre = tmp;
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
          free_area_[static_cast<int>(std::log(page_nums)/std::log(2))] = tmp->next_;
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

ObTmpFileMacroBlockHeader::ObTmpFileMacroBlockHeader()
  : version_(TMP_FILE_MACRO_BLOCK_HEADER_VERSION),
    magic_(TMP_FILE_MACRO_BLOCK_HEADER_MAGIC),
    block_id_(-1),
    dir_id_(-1),
    tenant_id_(0),
    free_page_nums_(-1)
{
}

bool ObTmpFileMacroBlockHeader::is_valid() const
{
  return TMP_FILE_MACRO_BLOCK_HEADER_VERSION == version_
      && TMP_FILE_MACRO_BLOCK_HEADER_MAGIC == magic_
      && block_id_ >= 0
      && dir_id_ >= 0
      && tenant_id_ > 0
      && free_page_nums_ >= 0;
}

int ObTmpFileMacroBlockHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(pos + get_serialize_size() > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "data buffer is not enough", K(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tmp file macro block header is invalid", K(ret), K(*this));
  } else {
    ObTmpFileMacroBlockHeader *header = reinterpret_cast<ObTmpFileMacroBlockHeader *>(buf + pos);
    header->version_ = version_;
    header->magic_ = magic_;
    header->block_id_ = block_id_;
    header->dir_id_ = dir_id_;
    header->tenant_id_ = tenant_id_;
    header->free_page_nums_ = free_page_nums_;
    pos += header->get_serialize_size();
  }
  return ret;
}

int ObTmpFileMacroBlockHeader::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(data_len - pos < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "buffer not enough", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObTmpFileMacroBlockHeader *ptr = reinterpret_cast<const ObTmpFileMacroBlockHeader *>(buf + pos);
    version_ = ptr->version_;
    magic_ = ptr->magic_;
    block_id_ = ptr->block_id_;
    dir_id_ = ptr->dir_id_;
    tenant_id_ = ptr->tenant_id_;
    free_page_nums_ = ptr->free_page_nums_;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      STORAGE_LOG(ERROR, "deserialize error", K(ret), K(*this));
    } else {
      pos += get_serialize_size();
    }
  }
  return ret;
}

void ObTmpFileMacroBlockHeader::reset()
{
  version_ = TMP_FILE_MACRO_BLOCK_HEADER_VERSION;
  magic_ = TMP_FILE_MACRO_BLOCK_HEADER_MAGIC;
  block_id_ = -1;
  dir_id_ = -1;
  tenant_id_ = 0;
  free_page_nums_ = 0;
}

ObTmpMacroBlock::ObTmpMacroBlock()
  : buffer_(NULL),
    handle_(),
    using_extents_(),
    macro_block_handle_(),
    tmp_file_header_(),
    io_desc_(),
    block_status_(MAX),
    is_sealed_(false),
    is_inited_(false),
    alloc_time_(0),
    access_time_(0)
{
  using_extents_.set_attr(ObMemAttr(MTL_ID(), "TMP_US_META"));
}

ObTmpMacroBlock::~ObTmpMacroBlock()
{
  destroy();
}

int ObTmpMacroBlock::init(const int64_t block_id, const int64_t dir_id, const uint64_t tenant_id,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else if (OB_FAIL(page_buddy_.init(allocator))) {
    STORAGE_LOG(WARN, "Fail to init the page buddy", K(ret));
  } else {
    tmp_file_header_.block_id_ = block_id;
    tmp_file_header_.dir_id_ = dir_id;
    tmp_file_header_.tenant_id_ = tenant_id;
    tmp_file_header_.free_page_nums_ = ObTmpFilePageBuddy::MAX_PAGE_NUMS;
    ATOMIC_STORE(&block_status_, MEMORY);
    is_inited_ = true;
    alloc_time_ = 0;
    ATOMIC_STORE(&access_time_, 0);
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
  tmp_file_header_.reset();
  buffer_ = NULL;
  handle_.reset();
  ATOMIC_STORE(&block_status_, MAX);
  is_sealed_ = false;
  alloc_time_ = 0;
  ATOMIC_STORE(&access_time_, 0);
  is_inited_ = false;
}

int ObTmpMacroBlock::seal(bool &is_sealed)
{
  int ret = OB_SUCCESS;
  is_sealed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret), KPC(this));
  } else {
    ObTmpFileExtent *tmp = NULL;
    SpinWLockGuard guard(lock_);
    bool is_all_closed = using_extents_.count() == 0 ? false : true;
    for (int32_t i = 0; OB_SUCC(ret) && i < using_extents_.count() && is_all_closed; ++i) {
      tmp = using_extents_.at(i);
      if (NULL != tmp && !tmp->is_closed()) {
        uint8_t start_id = ObTmpFilePageBuddy::MAX_PAGE_NUMS;
        uint8_t page_nums = 0;
        if (tmp->close(start_id, page_nums)) {
          if (ObTmpFilePageBuddy::MAX_PAGE_NUMS== start_id && 0 == page_nums) {
            //nothing to do
          } else if (OB_UNLIKELY(start_id > ObTmpFilePageBuddy::MAX_PAGE_NUMS - 1
                     || page_nums > ObTmpFilePageBuddy::MAX_PAGE_NUMS)) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "fail to close the extent", K(ret), K(start_id), K(page_nums), K(*tmp));
          } else if (OB_FAIL(free(start_id, page_nums))) {
            STORAGE_LOG(WARN, "fail to free the extent", K(ret));
          } else {
          }
          if (OB_FAIL(ret)) {
            tmp->unclose(page_nums);
            is_all_closed = false;
          }
        } else {
          is_all_closed = false;
        }
      }
    }
    if (OB_SUCC(ret) && is_all_closed) {
      is_sealed = true;
      ATOMIC_SET(&is_sealed_, true);
    }
  }
  return ret;
}

int ObTmpMacroBlock::is_extents_closed(bool &is_extents_closed)
{
  int ret = OB_SUCCESS;
  is_extents_closed = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited");
  } else {
    SpinRLockGuard guard(lock_);
    is_extents_closed = using_extents_.count() == 0 ? false : true;
    for (int64_t i = 0; i< using_extents_.count(); ++i) {
      if (!using_extents_.at(i)->is_closed()) {
        STORAGE_LOG(DEBUG, "the tmp macro block's extents is not all closed", K(tmp_file_header_.block_id_));
        is_extents_closed = false;
        break;
      }
    }
  }
  return ret;
}

int ObTmpMacroBlock::get_block_cache_handle(ObTmpBlockValueHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpBlockCacheKey key(tmp_file_header_.block_id_, tmp_file_header_.tenant_id_);
  SpinRLockGuard guard(lock_);
  if (!is_disked()) {
    handle = handle_;
  } else if (OB_FAIL(ObTmpBlockCache::get_instance().get_block(key, handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      STORAGE_LOG(WARN, "fail to get tmp block from cache", K(ret), K(key));
    } else if (REACH_COUNT_INTERVAL(100)) { // print one log per 100 times.
      STORAGE_LOG(DEBUG, "block cache miss", K(ret), K(key));
    }
  }
  return ret;
}

int ObTmpMacroBlock::get_wash_io_info(ObTmpBlockIOInfo &info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else {
    info.block_id_ = tmp_file_header_.block_id_;
    info.offset_ = 0;
    info.size_ = ObTmpFileStore::get_block_size();
    info.tenant_id_ = tmp_file_header_.tenant_id_;
    info.macro_block_id_ = get_macro_block_id();
    info.buf_ = buffer_;
    info.io_desc_ = io_desc_;
    info.io_timeout_ms_ = max(GCONF._data_storage_io_timeout / 1000L, DEFAULT_IO_WAIT_TIME_MS);
  }
  return ret;
}

int ObTmpMacroBlock::give_back_buf_into_cache(const bool is_wash)
{
  int ret = OB_SUCCESS;
  ObTmpBlockCacheKey key(tmp_file_header_.block_id_, tmp_file_header_.tenant_id_);
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(ObTmpBlockCache::get_instance().put_block(key, handle_))) {
    STORAGE_LOG(WARN, "fail to put block into block cache", K(ret), K(key));
  }
  if (is_wash) {// set block status disked in lock_ to avoid concurrency issues.
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(check_and_set_status(BlockStatus::WASHING, BlockStatus::MEMORY))) {
        STORAGE_LOG(ERROR, "fail to rollback block status", K(ret), K(tmp_ret), K(key), KPC(this));
      }
    } else if (OB_FAIL(check_and_set_status(BlockStatus::WASHING, BlockStatus::DISKED))) {
      STORAGE_LOG(WARN, "fail to check and set status", K(ret), K(key), KPC(this));
    }
  }
  return ret;
}

int ObTmpMacroBlock::alloc_all_pages(ObTmpFileExtent &extent)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const bool sealed = is_sealed();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_memory() || sealed)) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "the block is not in memory", K(ret), K(ATOMIC_LOAD(&block_status_)), K(sealed));
  } else if (OB_FAIL(page_buddy_.alloc_all_pages())) {
    STORAGE_LOG(WARN, "Fail to allocate the tmp extent", K(ret), K_(tmp_file_header), K_(page_buddy));
  } else {
    extent.set_block_id(get_block_id());
    extent.set_start_page_id(0);
    extent.set_page_nums(ObTmpFilePageBuddy::MAX_PAGE_NUMS);
    extent.alloced();
    if (OB_FAIL(using_extents_.push_back(&extent))) {
      STORAGE_LOG(WARN, "Fail to push back into using_extexts", K(ret));
      page_buddy_.free(extent.get_start_page_id(), extent.get_page_nums());
      extent.reset();
    } else {
      tmp_file_header_.free_page_nums_ -= extent.get_page_nums();
    }
    const int64_t cur_time = ObTimeUtility::fast_current_time();
    alloc_time_ = cur_time;
    ATOMIC_STORE(&access_time_, alloc_time_ + 1);
  }
  return ret;
}

int ObTmpMacroBlock::alloc(const uint8_t page_nums, ObTmpFileExtent &extent)
{
  int ret = OB_SUCCESS;
  uint8_t start_page_id = extent.get_start_page_id();
  uint8_t alloced_page_nums = 0;
  SpinWLockGuard guard(lock_);
  const bool sealed = is_sealed();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_memory() || sealed)) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "the block is not in memory", K(ret), K(ATOMIC_LOAD(&block_status_)), K(sealed));
  } else if (OB_FAIL(page_buddy_.alloc(page_nums, start_page_id, alloced_page_nums))) {
    STORAGE_LOG(WARN, "Fail to allocate the tmp extent", K(ret), K_(tmp_file_header), K_(page_buddy));
  } else {
    extent.set_block_id(tmp_file_header_.block_id_);
    extent.set_page_nums(alloced_page_nums);
    extent.set_start_page_id(start_page_id);
    extent.alloced();
    if (OB_FAIL(using_extents_.push_back(&extent))) {
      STORAGE_LOG(WARN, "Fail to push back into using_extexts", K(ret));
      page_buddy_.free(extent.get_start_page_id(), extent.get_page_nums());
      extent.reset();
    } else {
      tmp_file_header_.free_page_nums_ -= alloced_page_nums;
    }
    const int64_t cur_time = ObTimeUtility::fast_current_time();
    if (0 == alloc_time_) {
      alloc_time_ = cur_time;
    }
    if (OB_UNLIKELY(0 == ATOMIC_LOAD(&access_time_))) {
      ATOMIC_STORE(&access_time_,
        alloc_time_ + 60 * 1000000L * int64_t(alloced_page_nums) / get_max_cont_page_nums());
    } else {
      ATOMIC_STORE(&access_time_, cur_time);
    }
  }
  return ret;
}

int ObTmpMacroBlock::free(ObTmpFileExtent &extent)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    page_buddy_.free(extent.get_start_page_id(), extent.get_page_nums());
    for (int64_t i = using_extents_.count() - 1; i >= 0; --i) {
      if (&extent == using_extents_.at(i)) {
        using_extents_.remove(i);
        break;
      }
    }
    tmp_file_header_.free_page_nums_ += extent.get_page_nums();
    if (tmp_file_header_.free_page_nums_ == ObTmpFilePageBuddy::MAX_PAGE_NUMS) {
      alloc_time_ = 0;
      ATOMIC_STORE(&access_time_, 0);
    }
  }
  return ret;
}

int ObTmpMacroBlock::free(const int32_t start_page_id, const int32_t page_nums)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlock has not been inited", K(ret));
  } else {
    page_buddy_.free(start_page_id, page_nums);
    tmp_file_header_.free_page_nums_ += page_nums;
  }
  return ret;
}

int64_t ObTmpMacroBlock::get_used_page_nums() const
{
  SpinRLockGuard guard(lock_);
  int64_t used_page_nums = 0;
  for (int64_t i = using_extents_.count() - 1; i >= 0; --i) {
    used_page_nums += std::ceil((1.0 * using_extents_.at(i)->get_offset()) / DEFAULT_PAGE_SIZE);
  }
  return used_page_nums;
}

void ObTmpMacroBlock::set_io_desc(const common::ObIOFlag &io_desc)
{
  io_desc_ = io_desc;
}

int ObTmpMacroBlock::check_and_set_status(const BlockStatus old_block_status, const BlockStatus block_status)
{
  int ret = OB_SUCCESS;
  if (old_block_status != ATOMIC_VCAS(&block_status_, old_block_status, block_status)) {
    ret = OB_STATE_NOT_MATCH;
  }
  return ret;
}

ObTmpTenantMacroBlockManager::ObTmpTenantMacroBlockManager()
  : allocator_(),
    blocks_(),
    is_inited_(false)
{
}

ObTmpTenantMacroBlockManager::~ObTmpTenantMacroBlockManager()
{
  destroy();
}

int ObTmpTenantMacroBlockManager::init(const uint64_t tenant_id, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, ObModIds::OB_TMP_BLOCK_MAP);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has been inited", K(ret));
  } else if (OB_FAIL(blocks_.create(MBLK_HASH_BUCKET_NUM, attr, attr))) {
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

int ObTmpTenantMacroBlockManager::alloc_macro_block(const int64_t dir_id, const uint64_t tenant_id,
    ObTmpMacroBlock *&t_mblk)
{
  int ret = OB_SUCCESS;
  void *block_buf = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else if (OB_ISNULL(block_buf = allocator_->alloc(sizeof(ObTmpMacroBlock)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc a buf", K(ret));
  } else if (OB_ISNULL(t_mblk = new (block_buf) ObTmpMacroBlock())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObTmpMacroBlock", K(ret));
  } else if (OB_FAIL(t_mblk->init(OB_TMP_FILE_STORE.get_next_blk_id(), dir_id, tenant_id, *allocator_))) {
    STORAGE_LOG(WARN, "fail to init tmp block", K(ret));
  } else if (OB_FAIL(blocks_.set_refactored(t_mblk->get_block_id(), t_mblk))) {
    STORAGE_LOG(WARN, "fail to set tmp macro block map", K(ret), K(t_mblk));
  }
  if (OB_FAIL(ret)) {
    if (NULL != t_mblk) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = blocks_.erase_refactored(t_mblk->get_block_id()))) {
        STORAGE_LOG(WARN, "fail to erase from tmp macro block map", K(tmp_ret), K(t_mblk));
      }
      t_mblk->~ObTmpMacroBlock();
      allocator_->free(block_buf);
    }
  }
  return ret;
}

int ObTmpTenantMacroBlockManager::get_macro_block(const int64_t block_id, ObTmpMacroBlock *&t_mblk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else if (OB_UNLIKELY(block_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(blocks_.get_refactored(block_id, t_mblk))) {
    STORAGE_LOG(WARN, "fail to get tmp macro block", K(ret), K(block_id));
  }
  return ret;
}

int ObTmpTenantMacroBlockManager::free_macro_block(const int64_t block_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else if (OB_UNLIKELY(block_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(blocks_.erase_refactored(block_id))) {
    STORAGE_LOG(WARN, "fail to erase tmp macro block", K(ret));
  }
  return ret;
}

int ObTmpTenantMacroBlockManager::get_disk_macro_block_count(int64_t &count) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else {
    count = blocks_.size();
  }

  return ret;
}

int ObTmpTenantMacroBlockManager::get_disk_macro_block_list(
                                                common::ObIArray<MacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else {
    TmpMacroBlockMap::iterator iter;
    ObTmpMacroBlock *tmp = NULL;
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
  ObTmpMacroBlock *tmp = NULL;
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
    disk_fragment_ratio = disk_fragment * 1.0 / (disk_count * ObTmpFilePageBuddy::MAX_PAGE_NUMS);
  }
  double mem_fragment_ratio = 0;
  if (0 != mem_count) {
    mem_fragment_ratio = mem_fragment * 1.0 / (mem_count * ObTmpFilePageBuddy::MAX_PAGE_NUMS);
  }
  STORAGE_LOG(INFO, "the block usage for temporary files",
      K(disk_count), K(disk_fragment), K(disk_fragment_ratio),
      K(mem_count), K(mem_fragment), K(mem_fragment_ratio));
}

void ObTmpTenantMacroBlockManager::destroy()
{
  TmpMacroBlockMap::iterator iter;
  ObTmpMacroBlock *tmp = NULL;
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

ObTmpTenantFileStore::ObTmpTenantFileStore()
  : is_inited_(false),
    page_cache_num_(0),
    block_cache_num_(0),
    ref_cnt_(0),
    page_cache_(NULL),
    lock_(),
    allocator_(),
    io_allocator_(),
    tmp_block_manager_(),
    tmp_mem_block_manager_(*this),
    last_access_tenant_config_ts_(0),
    last_meta_mem_limit_(TOTAL_LIMIT)
{
}

ObTmpTenantFileStore::~ObTmpTenantFileStore()
{
  destroy();
}

void ObTmpTenantFileStore::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

int64_t ObTmpTenantFileStore::dec_ref()
{
  int ret = OB_SUCCESS;
  const int64_t tmp_ref = ATOMIC_SAF(&ref_cnt_, 1);
  if (tmp_ref < 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "bug: ref_cnt < 0", K(ret), K(tmp_ref), K(lbt()));
    ob_abort();
  }
  return tmp_ref;
}

int ObTmpTenantFileStore::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(allocator_.init(BLOCK_SIZE, ObModIds::OB_TMP_BLOCK_MANAGER, tenant_id, get_memory_limit(tenant_id)))) {
    STORAGE_LOG(WARN, "fail to init allocator", K(ret));
  } else if (OB_FAIL(io_allocator_.init(
                 lib::ObMallocAllocator::get_instance(),
                 OB_MALLOC_MIDDLE_BLOCK_SIZE,
                 ObMemAttr(tenant_id, ObModIds::OB_TMP_PAGE_CACHE, ObCtxIds::DEFAULT_CTX_ID)))) {
    STORAGE_LOG(WARN, "Fail to init io allocator, ", K(ret));
  } else if (OB_ISNULL(page_cache_ = &ObTmpPageCache::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get the page cache", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.init(tenant_id, allocator_))) {
    STORAGE_LOG(WARN, "fail to init the block manager for ObTmpFileStore", K(ret));
  } else if (OB_FAIL(tmp_mem_block_manager_.init(tenant_id, allocator_))) {
    STORAGE_LOG(WARN, "fail to init memory block manager", K(ret));
  } else {
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObTmpTenantFileStore::refresh_memory_limit(const uint64_t tenant_id)
{
  const int64_t old_limit = ATOMIC_LOAD(&last_meta_mem_limit_);
  const int64_t new_limit = get_memory_limit(tenant_id);
  if (old_limit != new_limit) {
    allocator_.set_total_limit(new_limit);
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "succeed to refresh temporary file meta memory limit", K(tenant_id), K(old_limit), K(new_limit));
  }
}

int64_t ObTmpTenantFileStore::get_memory_limit(const uint64_t tenant_id)
{
  const int64_t last_access_ts = ATOMIC_LOAD(&last_access_tenant_config_ts_);
  int64_t memory_limit = TOTAL_LIMIT;
  if (last_access_ts > 0 && common::ObClockGenerator::getClock() - last_access_ts < REFRESH_CONFIG_INTERVAL) {
    memory_limit = ATOMIC_LOAD(&last_meta_mem_limit_);
  } else {
    omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id));
    const int64_t tenant_mem_limit = lib::get_tenant_memory_limit(tenant_id);
    if (!config.is_valid() || 0 == tenant_mem_limit || INT64_MAX == tenant_mem_limit) {
      COMMON_LOG(INFO, "failed to get tenant config", K(tenant_id), K(tenant_mem_limit));
    } else {
      const int64_t limit_percentage_config = config->_temporary_file_meta_memory_limit_percentage;
      const int64_t limit_percentage = 0 == limit_percentage_config ? 70 : limit_percentage_config;
      memory_limit = tenant_mem_limit * limit_percentage / 100;
      if (OB_UNLIKELY(memory_limit <= 0)) {
        STORAGE_LOG(INFO, "memory limit isn't more than 0", K(memory_limit));
        memory_limit = ATOMIC_LOAD(&last_meta_mem_limit_);
      } else {
        ATOMIC_STORE(&last_meta_mem_limit_, memory_limit);
        ATOMIC_STORE(&last_access_tenant_config_ts_, common::ObClockGenerator::getClock());
      }
    }
  }
  return memory_limit;
}

void ObTmpTenantFileStore::destroy()
{
  tmp_mem_block_manager_.destroy();
  tmp_block_manager_.destroy();
  if (NULL != page_cache_) {
    page_cache_ = NULL;
  }
  allocator_.destroy();
  io_allocator_.reset();
  last_access_tenant_config_ts_ = 0;
  last_meta_mem_limit_ = TOTAL_LIMIT;
  is_inited_ = false;
  STORAGE_LOG(INFO, "cache num when destroy",
              K(ATOMIC_LOAD(&page_cache_num_)), K(ATOMIC_LOAD(&block_cache_num_)));
  page_cache_num_ = 0;
  block_cache_num_ = 0;
}

int ObTmpTenantFileStore::alloc(const int64_t dir_id, const uint64_t tenant_id, const int64_t alloc_size,
    ObTmpFileExtent &extent)
{
  int ret = OB_SUCCESS;
  const int64_t block_size = ObTmpFileStore::get_block_size();
  // In buddy allocation, if free space in one block isn't powers of 2, need upper align.
  int64_t max_order = std::ceil(std::log(ObTmpFilePageBuddy::MAX_PAGE_NUMS) / std::log(2));
  int64_t origin_max_cont_page_nums = std::pow(2, max_order - 1);
  int64_t max_cont_size_per_block = origin_max_cont_page_nums * ObTmpMacroBlock::get_default_page_size();
  ObTmpMacroBlock *t_mblk = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (alloc_size <= 0 || alloc_size > block_size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(alloc_size), K(block_size));
  } else {
    const int64_t timeout_ts = THIS_WORKER.get_timeout_ts();
    ret = OB_STATE_NOT_MATCH;
    while (OB_STATE_NOT_MATCH == ret || OB_SIZE_OVERFLOW == ret) {
      if (OB_UNLIKELY(timeout_ts <= ObTimeUtility::current_time())) {
        ret = OB_TIMEOUT;
        STORAGE_LOG(WARN, "it's timeout", K(ret), K(timeout_ts), K(ObTimeUtility::current_time()));
      } else if (OB_FAIL(tmp_mem_block_manager_.cleanup())) {
        if (OB_STATE_NOT_MATCH != ret) {
          STORAGE_LOG(WARN, "fail to try wash tmp macro block", K(ret), K(dir_id), K(tenant_id));
        }
      }
      SpinWLockGuard guard(lock_);
      if (OB_SUCC(ret)) {
        if (alloc_size > max_cont_size_per_block) {
          if (OB_FAIL(alloc_macro_block(dir_id, tenant_id, t_mblk))) {
            if (OB_SIZE_OVERFLOW != ret) {
              STORAGE_LOG(WARN, "cannot allocate a tmp macro block", K(ret), K(dir_id), K(tenant_id));
            }
          } else if (OB_ISNULL(t_mblk)) {
            ret = OB_ERR_NULL_VALUE;
            STORAGE_LOG(WARN, "block alloced is NULL", K(ret), K(dir_id), K(tenant_id));
          } else if (OB_FAIL(t_mblk->alloc_all_pages(extent))) {
            STORAGE_LOG(WARN, "Fail to allocate the tmp extent", K(ret), K(t_mblk->get_block_id()));
          } else if (alloc_size < block_size) {
            const int64_t nums = std::ceil(alloc_size * 1.0 / ObTmpMacroBlock::get_default_page_size());
            if (OB_FAIL(free_extent(t_mblk->get_block_id(), nums,
                ObTmpFilePageBuddy::MAX_PAGE_NUMS - nums))) {
              STORAGE_LOG(WARN, "fail to free pages", K(ret), K(t_mblk->get_block_id()));
            } else {
              extent.set_page_nums(nums);
            }
          }
        } else if (OB_FAIL(tmp_mem_block_manager_.alloc_extent(dir_id, tenant_id, alloc_size, extent))) {
          if (OB_STATE_NOT_MATCH == ret) {
            if (OB_FAIL(alloc_macro_block(dir_id, tenant_id, t_mblk))) {
              if (OB_SIZE_OVERFLOW != ret) {
                STORAGE_LOG(WARN, "cannot allocate a tmp macro block", K(ret), K(dir_id), K(tenant_id));
              }
            } else if (OB_ISNULL(t_mblk)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "unexpected error, t_mblk is nullptr", K(ret), KP(t_mblk));
            } else {
              const int64_t page_nums = std::ceil(alloc_size * 1.0 / ObTmpMacroBlock::get_default_page_size());
              if (OB_FAIL(t_mblk->alloc(page_nums, extent))){
                STORAGE_LOG(WARN, "fail to alloc tmp extent", K(ret));
              } else if (OB_FAIL(tmp_mem_block_manager_.refresh_dir_to_blk_map(dir_id, t_mblk))) {
                STORAGE_LOG(WARN, "fail to refresh dir_to_blk_map", K(ret), K(*t_mblk));
              }
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "fail to alloc tmp extent", K(ret));
    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      STORAGE_LOG(WARN, "alloc memory failed", K(ret), K(ATOMIC_LOAD(&block_cache_num_)), K(ATOMIC_LOAD(&page_cache_num_)));
    }
  }
  return ret;
}

int ObTmpTenantFileStore::free(ObTmpFileExtent *extent)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(free_extent(extent))) {
    STORAGE_LOG(WARN, "fail to free the extent", K(ret), K(*extent));
  }
  return ret;
}

int ObTmpTenantFileStore::free(const int64_t block_id, const int32_t start_page_id,
    const int32_t page_nums)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(free_extent(block_id, start_page_id, page_nums))) {
    STORAGE_LOG(WARN, "fail to free the extent", K(ret), K(block_id), K(start_page_id),
        K(page_nums));
  }
  return ret;
}

int ObTmpTenantFileStore::free_extent(ObTmpFileExtent *extent)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *t_mblk = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_ISNULL(extent) || OB_UNLIKELY(!extent->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(*extent));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(extent->get_block_id(), t_mblk))) {
    STORAGE_LOG(WARN, "fail to get tmp macro block", K(ret));
  } else if (OB_ISNULL(t_mblk)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "block is null", K(ret));
  } else if (OB_FAIL(t_mblk->free(*extent))) {
    STORAGE_LOG(WARN, "fail to free extent", K(ret));
  } else {
    extent->reset();
    if (OB_SUCC(ret) && t_mblk->is_empty()) {
      if (OB_FAIL(free_macro_block(t_mblk))) {
        STORAGE_LOG(WARN, "fail to free tmp macro block", K(ret));
      }
    }
  }
  return ret;
}

int ObTmpTenantFileStore::free_extent(const int64_t block_id, const int32_t start_page_id,
    const int32_t page_nums)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *t_mblk = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_UNLIKELY(start_page_id < 0 || page_nums < 0 || block_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id), K(start_page_id), K(page_nums));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(block_id, t_mblk))) {
    STORAGE_LOG(WARN, "fail to get tmp block", K(ret), K(block_id), K(start_page_id), K(page_nums));
  } else if (OB_ISNULL(t_mblk)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "block is null", K(ret));
  } else if (OB_FAIL(t_mblk->free(start_page_id, page_nums))) {
    STORAGE_LOG(WARN, "fail to free extent", K(ret));
  } else {
    if (OB_SUCC(ret) && t_mblk->is_empty()) {
      if (OB_FAIL(free_macro_block(t_mblk))) {
        STORAGE_LOG(WARN, "fail to free tmp macro block", K(ret));
      }
    }
  }
  return ret;
}

int ObTmpTenantFileStore::free_macro_block(ObTmpMacroBlock *&t_mblk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_ISNULL(t_mblk)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.free_macro_block(t_mblk->get_block_id()))) {
    STORAGE_LOG(WARN, "fail to free tmp macro block for block manager", K(ret));
  } else {
    while (OB_SUCC(ret) && !t_mblk->is_disked()) {
      if (t_mblk->is_memory() && OB_FAIL(tmp_mem_block_manager_.check_and_free_mem_block(t_mblk))) {
        STORAGE_LOG(WARN, "fail to check and free mem block", K(ret), KPC(t_mblk));
      } else if (t_mblk->is_washing() &&
          OB_FAIL(tmp_mem_block_manager_.wait_write_finish(t_mblk->get_block_id(),
              ObTmpTenantMemBlockManager::get_default_timeout_ms()))) {
        STORAGE_LOG(WARN, "fail to wait write io finish", K(ret), KPC(t_mblk));
      }
    }
    if (OB_SUCC(ret)) {
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "finish to free a block", K(ret), KPC(t_mblk));
      t_mblk->~ObTmpMacroBlock();
      allocator_.free(t_mblk);
      t_mblk = nullptr;
    }
  }
  return ret;
}

int ObTmpTenantFileStore::alloc_macro_block(const int64_t dir_id, const uint64_t tenant_id,
                                            ObTmpMacroBlock *&t_mblk)
{
  int ret = OB_SUCCESS;
  t_mblk = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpMacroBlockManager has not been inited", K(ret));
  } else if (tmp_mem_block_manager_.check_block_full()) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(DEBUG, "mem block is full", K(ret), K(tenant_id), K(dir_id));
  } else if (OB_FAIL(tmp_block_manager_.alloc_macro_block(dir_id, tenant_id, t_mblk))) {
    STORAGE_LOG(WARN, "cannot allocate a tmp macro block", K(ret), K(dir_id), K(tenant_id));
  } else if (OB_ISNULL(t_mblk)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "block is null", K(ret));
  } else {
    ObTmpBlockCacheKey key(t_mblk->get_block_id(), tenant_id);
    if (OB_FAIL(tmp_mem_block_manager_.alloc_buf(key, t_mblk->get_handle()))) {
      STORAGE_LOG(WARN, "fail to alloc block cache buf", K(ret));
    } else {
      t_mblk->set_buffer(t_mblk->get_handle().value_->get_buffer());
      if (OB_FAIL(tmp_mem_block_manager_.add_macro_block(t_mblk))) {
        STORAGE_LOG(WARN, "fail to put meta into block cache", K(ret), K(t_mblk));
      }
      inc_block_cache_num(1);
      if (OB_FAIL(ret)) {
        tmp_mem_block_manager_.free_macro_block(t_mblk->get_block_id());
        t_mblk->give_back_buf_into_cache();
        dec_block_cache_num(1);
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(t_mblk)) {
      tmp_block_manager_.free_macro_block(t_mblk->get_block_id());
      t_mblk->~ObTmpMacroBlock();
      allocator_.free(t_mblk);
      t_mblk = nullptr;
    }
  }

  return ret;
}

int ObTmpTenantFileStore::read(ObTmpBlockIOInfo &io_info, ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpBlockValueHandle tb_handle;
  ObTmpMacroBlock *block = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (!handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(handle));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(io_info.block_id_, block))) {
    STORAGE_LOG(WARN, "fail to get block from tmp block manager", K(ret), K_(io_info.block_id));
  } else if (OB_ISNULL(block)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the block is NULL", K(ret), K_(io_info.block_id));
  } else {
    if (OB_SUCC(block->get_block_cache_handle(tb_handle))) {
      ObTmpFileIOHandle::ObBlockCacheHandle block_handle(
          tb_handle,
          io_info.buf_,
          io_info.offset_,
          io_info.size_);
      OB_TMP_FILE_STORE.inc_block_cache_num(io_info.tenant_id_, 1);
      if (OB_FAIL(handle.get_block_cache_handles().push_back(block_handle))) {
        STORAGE_LOG(WARN, "Fail to push back into block_handles", K(ret), K(block_handle));
      }
    } else if (OB_SUCC(read_page(block, io_info, handle))) {
      //nothing to do.
    } else {
      STORAGE_LOG(WARN, "fail to read", K(ret));
    }

    if (OB_SUCC(ret)) {
      block->set_io_desc(io_info.io_desc_);
    }
  }
  return ret;
}

int ObTmpTenantFileStore::read_page(ObTmpMacroBlock *block, ObTmpBlockIOInfo &io_info,
    ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  int64_t page_start_id = io_info.offset_ / ObTmpMacroBlock::get_default_page_size();
  int64_t offset = io_info.offset_ % ObTmpMacroBlock::get_default_page_size();
  int64_t remain_size = io_info.size_;
  int64_t size = std::min(ObTmpMacroBlock::get_default_page_size() - offset, remain_size);
  int32_t page_nums = 0;
  common::ObIArray<ObTmpPageIOInfo> *page_io_infos = nullptr;

  void *buf =
      ob_malloc(sizeof(common::ObSEArray<ObTmpPageIOInfo, ObTmpFilePageBuddy::MAX_PAGE_NUMS>),
                ObMemAttr(MTL_ID(), "TmpReadPage"));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc a buf", K(ret));
  } else {
    page_io_infos = new (buf) common::ObSEArray<ObTmpPageIOInfo, ObTmpFilePageBuddy::MAX_PAGE_NUMS>();
    do {
      ObTmpPageCacheKey key(io_info.block_id_, page_start_id, io_info.tenant_id_);
      ObTmpPageValueHandle p_handle;
      if (OB_SUCC(page_cache_->get_page(key, p_handle))) {
        ObTmpFileIOHandle::ObPageCacheHandle page_handle(
            p_handle,
            io_info.buf_ + ObTmpMacroBlock::calculate_offset(page_start_id, offset) - io_info.offset_,
            offset,
            size);
        inc_page_cache_num(1);
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
        if (OB_FAIL(page_io_infos->push_back(page_io_info))) {
          STORAGE_LOG(WARN, "Fail to push back into page_io_infos", K(ret), K(page_io_info));
        }
      } else {
        STORAGE_LOG(WARN, "fail to get page from page cache", K(ret));
      }
      page_nums++;
      page_start_id++;
      offset = 0;
      remain_size -= size;
      size = std::min(ObTmpMacroBlock::get_default_page_size(), remain_size);
    } while (OB_SUCC(ret) && size > 0);
  }

  if (OB_SUCC(ret)) {
    if (page_io_infos->count() > DEFAULT_PAGE_IO_MERGE_RATIO * page_nums) {
      // merge multi page io into one.
      ObMacroBlockHandle mb_handle;
      ObTmpBlockIOInfo info(io_info);
      const int64_t p_offset = common::lower_align(io_info.offset_, ObTmpMacroBlock::get_default_page_size());
      // just skip header and padding.
      info.offset_ = p_offset + ObTmpMacroBlock::get_header_padding();
      info.size_ = page_nums * ObTmpMacroBlock::get_default_page_size();
      info.macro_block_id_ = block->get_macro_block_id();
      if (handle.is_disable_page_cache()) {
        if (OB_FAIL(page_cache_->direct_read(info, mb_handle, io_allocator_))) {
          STORAGE_LOG(WARN, "fail to direct read multi page", K(ret));
        }
      } else {
        if (OB_FAIL(page_cache_->prefetch(info, *page_io_infos, mb_handle, io_allocator_))) {
          STORAGE_LOG(WARN, "fail to prefetch multi tmp page", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObTmpFileIOHandle::ObIOReadHandle read_handle(mb_handle, io_info.buf_,
            io_info.offset_ - p_offset, io_info.size_);
        if (OB_FAIL(handle.get_io_handles().push_back(read_handle))) {
          STORAGE_LOG(WARN, "Fail to push back into read_handles", K(ret));
        }
      }
    } else {
      // just do io, page by page.
      for (int i = 0; OB_SUCC(ret) && i < page_io_infos->count(); i++) {
        ObMacroBlockHandle mb_handle;
        ObTmpBlockIOInfo info(io_info);
        info.offset_ = page_io_infos->at(i).key_.get_page_id() * ObTmpMacroBlock::get_default_page_size();
        // just skip header and padding.
        info.offset_ += ObTmpMacroBlock::get_header_padding();
        info.size_ = ObTmpMacroBlock::get_default_page_size();
        info.macro_block_id_ = block->get_macro_block_id();
        if (handle.is_disable_page_cache()) {
          if (OB_FAIL(page_cache_->direct_read(info, mb_handle, io_allocator_))) {
            STORAGE_LOG(WARN, "fail to direct read tmp page", K(ret));
          }
        } else {
          if (OB_FAIL(page_cache_->prefetch(page_io_infos->at(i).key_, info, mb_handle, io_allocator_))) {
            STORAGE_LOG(WARN, "fail to prefetch tmp page", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          char *buf = io_info.buf_ + ObTmpMacroBlock::calculate_offset(
              page_io_infos->at(i).key_.get_page_id(), page_io_infos->at(i).offset_) - io_info.offset_;
          ObTmpFileIOHandle::ObIOReadHandle read_handle(mb_handle, buf, page_io_infos->at(i).offset_,
              page_io_infos->at(i).size_);
          if (OB_FAIL(handle.get_io_handles().push_back(read_handle))) {
            STORAGE_LOG(WARN, "Fail to push back into read_handles", K(ret));
          }
        }
      }
    }
  }
  if (OB_NOT_NULL(page_io_infos)) {
    page_io_infos->destroy();
    ob_free(page_io_infos);
  }
  return ret;
}

int ObTmpTenantFileStore::write(const ObTmpBlockIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *block = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(io_info.block_id_, block))) {
    STORAGE_LOG(WARN, "fail to get block from tmp block manager", K(ret), K_(io_info.block_id));
  } else if (OB_ISNULL(block)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the block is NULL", K(ret), K_(io_info.block_id));
  } else if (block->is_memory()) {
    block->set_io_desc(io_info.io_desc_);
    MEMCPY(block->get_buffer() + io_info.offset_, io_info.buf_, io_info.size_);
  } else {
    // The washing and disked block shouldn't write data, Otherwise, it will cause data corrupt.
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "block status is not correct", K(ret), K(io_info));
  }
  return ret;
}

int ObTmpTenantFileStore::wash_block(const int64_t block_id,
                                     ObTmpTenantMemBlockManager::ObIOWaitInfoHandle &handle)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  ObTmpMacroBlock *block = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret), K(block_id));
  } else if (OB_UNLIKELY(block_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(block_id, block))) {
    STORAGE_LOG(WARN, "fail to get block from tmp block manager", K(ret), K(block_id));
  } else if (OB_ISNULL(block)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the block is NULL", K(ret), K(block_id));
  } else if (OB_FAIL(tmp_mem_block_manager_.wash_block(block_id, handle))) {
    STORAGE_LOG(WARN, "wash block failed", K(ret), K(block_id));
  }
  return ret;
}

int ObTmpTenantFileStore::sync_block(const int64_t block_id,
                                     ObTmpTenantMemBlockManager::ObIOWaitInfoHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *blk = NULL;
  SpinRLockGuard guard(lock_);
  bool is_closed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret), K(block_id));
  } else if (OB_UNLIKELY(block_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(block_id, blk))) {
    STORAGE_LOG(WARN, "fail to get macro block", K(ret), K(block_id));
  } else if (OB_FAIL(blk->is_extents_closed(is_closed))) {
    STORAGE_LOG(WARN, "check block closed failed", K(ret), K(block_id));
  } else if (!is_closed) {
    //do nothing
  } else if (OB_FAIL(tmp_mem_block_manager_.wash_block(block_id, handle))) {
    STORAGE_LOG(WARN, "wash block failed", K(ret), K(block_id));
  } else {
    STORAGE_LOG(DEBUG, "succeed to sync block", K(block_id));
  }
  return ret;
}

int ObTmpTenantFileStore::wait_write_finish(const int64_t block_id, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObTmpMacroBlock *blk = NULL;
  bool is_closed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret), K(block_id));
  } else if (OB_UNLIKELY(block_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(block_id));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(block_id, blk))) {
    STORAGE_LOG(WARN, "fail to get macro block", K(ret), K(block_id));
  } else if (OB_ISNULL(blk)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "macro block is NULL", K(ret), K(block_id));
  } else if (!blk->is_washing()) {
    // block has not been washed, nothing todo.
  } else if (blk->is_washing() &&
             OB_FAIL(tmp_mem_block_manager_.wait_write_finish(block_id, timeout_ms))){
    STORAGE_LOG(WARN, "wait write finish failed", K(ret), K(block_id));
  }
  return ret;
}

int ObTmpTenantFileStore::get_disk_macro_block_count(int64_t &count) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.get_disk_macro_block_count(count))) {
    STORAGE_LOG(WARN, "fail to get disk macro block count from tmp_block_manager_", K(ret));
  }
  return ret;
}

int ObTmpTenantFileStore::get_disk_macro_block_list(common::ObIArray<MacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.get_disk_macro_block_list(macro_id_list))) {
    STORAGE_LOG(WARN, "fail to get disk macro block list from tmp_block_manager_", K(ret));
  }
  return ret;
}

int ObTmpTenantFileStore::get_macro_block(const int64_t block_id, ObTmpMacroBlock *&t_mblk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpTenantFileStore has not been inited", K(ret));
  } else if (OB_FAIL(tmp_block_manager_.get_macro_block(block_id, t_mblk))) {
    STORAGE_LOG(WARN, "ObTmpTenantFileStore get macro block failed", K(ret));
  }

  return ret;
}

ObTmpTenantFileStoreHandle::ObTmpTenantFileStoreHandle()
  : tenant_store_(), allocator_()
{
}

ObTmpTenantFileStoreHandle::~ObTmpTenantFileStoreHandle()
{
  reset();
}
void ObTmpTenantFileStoreHandle::set_tenant_store(ObTmpTenantFileStore *tenant_store,
                                                  common::ObConcurrentFIFOAllocator *allocator)
{
  if (OB_NOT_NULL(tenant_store)) {
    reset();
    tenant_store->inc_ref();
    tenant_store_ = tenant_store;
    allocator_ = allocator;
  }
}

ObTmpTenantFileStoreHandle&
ObTmpTenantFileStoreHandle::operator=(const ObTmpTenantFileStoreHandle &other)
{
  if (&other != this) {
    set_tenant_store(other.tenant_store_, other.allocator_);
  }
  return *this;
}

bool ObTmpTenantFileStoreHandle::is_empty() const
{
  return NULL == tenant_store_;
}

bool ObTmpTenantFileStoreHandle::is_valid() const
{
  return NULL != tenant_store_;
}

void ObTmpTenantFileStoreHandle::reset()
{
  if (OB_NOT_NULL(tenant_store_)) {
    int64_t tmp_ref = tenant_store_->dec_ref();
    if (0 == tmp_ref) {
      tenant_store_->~ObTmpTenantFileStore();
      allocator_->free(tenant_store_);
    }
    tenant_store_ = NULL;
  }
}

ObTmpFileStore &ObTmpFileStore::get_instance()
{
  static ObTmpFileStore instance;
  return instance;
}

ObTmpFileStore::ObTmpFileStore()
  : next_blk_id_(0),
    tenant_file_stores_(),
    lock_(),
    is_inited_(false),
    allocator_()
{
}

ObTmpFileStore::~ObTmpFileStore()
{
}

int ObTmpFileStore::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr = SET_USE_500(ObModIds::OB_TMP_FILE_STORE_MAP);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else if (OB_FAIL(allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "fail to init allocator", K(ret));
  } else if (OB_FAIL(ObTmpPageCache::get_instance().init("tmp_page_cache",
                                                         TMP_FILE_PAGE_CACHE_PRIORITY))) {
    STORAGE_LOG(WARN, "Fail to init tmp page cache, ", K(ret));
  } else if (OB_FAIL(ObTmpBlockCache::get_instance().init("tmp_block_cache",
      TMP_FILE_BLOCK_CACHE_PRIORITY))) {
    STORAGE_LOG(WARN, "Fail to init tmp tenant block cache, ", K(ret));
  } else if (OB_FAIL(tenant_file_stores_.create(STORE_HASH_BUCKET_NUM,
      attr, attr))) {
    STORAGE_LOG(WARN, "Fail to create tmp tenant file store map, ", K(ret));
  } else {
    allocator_.set_attr(attr);
    is_inited_ = true;
  }
  if (!is_inited_) {
    destroy();
  }
  return ret;
}

int ObTmpFileStore::alloc(const int64_t dir_id, const uint64_t tenant_id, const int64_t size,
    ObTmpFileExtent &extent)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else if (OB_FAIL(store_handle.get_tenant_store()->alloc(dir_id, tenant_id, size, extent))) {
    STORAGE_LOG(WARN, "fail to allocate extents", K(ret), K(tenant_id), K(dir_id), K(size),
        K(extent));
  }
  return ret;
}

int ObTmpFileStore::read(const uint64_t tenant_id, ObTmpBlockIOInfo &io_info,
    ObTmpFileIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id), K(tenant_id),
        K(io_info), K(handle));
  } else if (OB_FAIL(store_handle.get_tenant_store()->read(io_info, handle))) {
    STORAGE_LOG(WARN, "fail to read the extent", K(ret), K(tenant_id), K(io_info), K(handle));
  }
  return ret;
}

int ObTmpFileStore::write(const uint64_t tenant_id, const ObTmpBlockIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id), K(io_info));
  } else if (OB_FAIL(store_handle.get_tenant_store()->write(io_info))) {
    STORAGE_LOG(WARN, "fail to write the extent", K(ret), K(tenant_id), K(io_info));
  }
  return ret;
}

int ObTmpFileStore::wash_block(const uint64_t tenant_id, const int64_t block_id,
                               ObTmpTenantMemBlockManager::ObIOWaitInfoHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id), K(block_id));
  } else if (OB_FAIL(store_handle.get_tenant_store()->wash_block(block_id, handle))) {
    STORAGE_LOG(WARN, "fail to write the extent", K(ret), K(tenant_id), K(block_id));
  }
  return ret;
}

int ObTmpFileStore::wait_write_finish(const uint64_t tenant_id, const int64_t block_id, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else if (OB_FAIL(store_handle.get_tenant_store()->wait_write_finish(block_id, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to write the extent", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTmpFileStore::sync_block(const uint64_t tenant_id, const int64_t block_id,
                               ObTmpTenantMemBlockManager::ObIOWaitInfoHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else if (OB_FAIL(store_handle.get_tenant_store()->sync_block(block_id, handle))) {
    STORAGE_LOG(WARN, "fail to write the extent", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTmpFileStore::inc_block_cache_num(const uint64_t tenant_id, const int64_t num)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else {
    store_handle.get_tenant_store()->inc_block_cache_num(num);
  }
  return ret;
}

int ObTmpFileStore::dec_block_cache_num(const uint64_t tenant_id, const int64_t num)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else {
    store_handle.get_tenant_store()->dec_block_cache_num(num);
  }
  return ret;
}

int ObTmpFileStore::inc_page_cache_num(const uint64_t tenant_id, const int64_t num)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else {
    store_handle.get_tenant_store()->inc_page_cache_num(num);
  }
  return ret;
}

int ObTmpFileStore::dec_page_cache_num(const uint64_t tenant_id, const int64_t num)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else {
    store_handle.get_tenant_store()->dec_page_cache_num(num);
  }
  return ret;
}

int ObTmpFileStore::free(const uint64_t tenant_id, ObTmpFileExtent *extent)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id), K(*extent));
  } else if (OB_FAIL(store_handle.get_tenant_store()->free(extent))) {
    STORAGE_LOG(WARN, "fail to free extents", K(ret), K(tenant_id), K(*extent));
  }
  return ret;
}

int ObTmpFileStore::free(const uint64_t tenant_id, const int64_t block_id,
    const int32_t start_page_id, const int32_t page_nums)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else if (OB_FAIL(store_handle.get_tenant_store()->free(block_id, start_page_id, page_nums))) {
    STORAGE_LOG(WARN, "fail to free", K(ret), K(tenant_id), K(block_id), K(start_page_id),
        K(page_nums));
  }
  return ret;
}

int ObTmpFileStore::free_tenant_file_store(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_file_stores_.erase_refactored(tenant_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      STORAGE_LOG(WARN, "fail to erase tmp tenant file store", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObTmpFileStore::get_macro_block(const int64_t tenant_id, const int64_t block_id, ObTmpMacroBlock *&t_mblk)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else if (OB_FAIL(store_handle.get_tenant_store()->get_macro_block(block_id, t_mblk))) {
    STORAGE_LOG(WARN, "fail to free", K(ret), K(tenant_id), K(block_id));
  }
  return ret;
}

int ObTmpFileStore::get_macro_block_list(common::ObIArray<MacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else {
    macro_id_list.reset();
    TenantFileStoreMap::iterator iter;
    ObTmpTenantFileStore *tmp = NULL;
    for (iter = tenant_file_stores_.begin(); OB_SUCC(ret) && iter != tenant_file_stores_.end();
        ++iter) {
      if (OB_ISNULL(tmp = iter->second.get_tenant_store())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to iterate tmp tenant file store", K(ret));
      } else if (OB_FAIL(tmp->get_disk_macro_block_list(macro_id_list))){
        STORAGE_LOG(WARN, "fail to get list of tenant macro block in disk", K(ret));
      }
    }
  }
  return ret;
}

int ObTmpFileStore::get_macro_block_list(ObIArray<TenantTmpBlockCntPair> &tmp_block_cnt_pairs)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else {
    tmp_block_cnt_pairs.reset();
    TenantFileStoreMap::iterator iter;
    ObTmpTenantFileStore *tmp = NULL;
    for (iter = tenant_file_stores_.begin(); OB_SUCC(ret) && iter != tenant_file_stores_.end();
        ++iter) {
      int64_t macro_id_count = 0;
      TenantTmpBlockCntPair pair;
      if (OB_ISNULL(tmp = iter->second.get_tenant_store())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to iterate tmp tenant file store", K(ret));
      } else if (OB_FAIL(tmp->get_disk_macro_block_count(macro_id_count))){
        STORAGE_LOG(WARN, "fail to get list of tenant macro block in disk", K(ret));
      } else if (OB_FAIL(pair.init(iter->first, macro_id_count))) {
        STORAGE_LOG(WARN, "fail to init tenant tmp block count pair", K(ret), "tenant id",
            iter->first, "macro block count", macro_id_count);
      } else if (OB_FAIL(tmp_block_cnt_pairs.push_back(pair))) {
        STORAGE_LOG(WARN, "fail to push back tmp_block_cnt_pairs", K(ret), K(pair));
      }
    }
  }
  return ret;
}

int ObTmpFileStore::get_all_tenant_id(common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret));
  } else {
    tenant_ids.reset();
    TenantFileStoreMap::iterator iter;
    SpinRLockGuard guard(lock_);
    for (iter = tenant_file_stores_.begin(); OB_SUCC(ret) && iter != tenant_file_stores_.end();
        ++iter) {
      if (OB_ISNULL(iter->second.get_tenant_store())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to iterate tmp tenant file store", K(ret));
      } else if (OB_FAIL(tenant_ids.push_back(iter->first))) {
        STORAGE_LOG(WARN, "fail to push back tmp_block_cnt_pairs", K(ret));
      }
    }
  }
  return ret;
}

int ObTmpFileStore::get_store(const uint64_t tenant_id, ObTmpTenantFileStoreHandle &handle)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  void *buf = NULL;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret), K(tenant_id));
  } else {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(tenant_file_stores_.get_refactored(tenant_id, handle))) {
      if (OB_HASH_NOT_EXIST == ret) {
        STORAGE_LOG(DEBUG, "ObTmpFileStore get tenant store failed", K(ret), K(tenant_id));
      } else {
        STORAGE_LOG(WARN, "ObTmpFileStore get tenant store failed", K(ret), K(tenant_id));
      }
    }
  }

  if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(tenant_file_stores_.get_refactored(tenant_id, handle))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ObTmpTenantFileStore *store = NULL;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTmpTenantFileStore)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to alloc a buf", K(ret), K(tenant_id));
        } else if (OB_ISNULL(store = new (buf) ObTmpTenantFileStore())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "fail to new a ObTmpTenantFileStore", K(ret), K(tenant_id));
        } else if (OB_FAIL(store->init(tenant_id))) {
          store->~ObTmpTenantFileStore();
          allocator_.free(store);
          store = NULL;
          STORAGE_LOG(WARN, "fail to init ObTmpTenantFileStore", K(ret), K(tenant_id));
        } else if (FALSE_IT(handle.set_tenant_store(store, &allocator_))) {
        } else if (OB_FAIL(tenant_file_stores_.set_refactored(tenant_id, handle))) {
          STORAGE_LOG(WARN, "fail to set tenant_file_stores_", K(ret), K(tenant_id));
        }
      } else {
        STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, invalid tenant file store handle", K(ret), K(handle));
    }
  }
  return ret;
}

int64_t ObTmpFileStore::get_next_blk_id()
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

int ObTmpFileStore::get_tenant_extent_allocator(const int64_t tenant_id, common::ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  ObTmpTenantFileStoreHandle store_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTmpFileStore has not been inited", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_store(tenant_id, store_handle))) {
    STORAGE_LOG(WARN, "fail to get tmp tenant file store", K(ret), K(tenant_id));
  } else {
    allocator = &(store_handle.get_tenant_store()->get_extent_allocator());
  }
  return ret;
}

void ObTmpFileStore::destroy()
{
  ObTmpPageCache::get_instance().destroy();
  tenant_file_stores_.destroy();
  allocator_.destroy();
  ObTmpBlockCache::get_instance().destroy();
  is_inited_ = false;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
