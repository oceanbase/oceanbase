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

#include "storage/tmp_file/ob_tmp_file_write_cache.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_shrink_ctx.h"

namespace oceanbase
{
namespace tmp_file
{

WriteCacheShrinkContext::WriteCacheShrinkContext()
  : is_inited_(false),
    is_auto_(false),
    shrink_state_(SHRINK_STATE::INVALID),
    max_allow_alloc_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
    lower_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
    upper_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
    current_max_used_watermark_(0),
    last_shrink_complete_ts_(ObTimeUtility::current_time()),
    shrink_begin_ts_(0),
    shrink_list_()
{}

int WriteCacheShrinkContext::init(
    const uint32_t lower_page_id,
    const uint32_t upper_page_id,
    const bool is_auto)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("shrink context is inited twice", K(ret));
  } else if (OB_UNLIKELY(lower_page_id < 0 || lower_page_id >= upper_page_id ||
                         lower_page_id % ObTmpFileWriteCache::BLOCK_PAGE_NUMS != 0 ||
                         (upper_page_id + 1) % ObTmpFileWriteCache::BLOCK_PAGE_NUMS != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(lower_page_id), K(upper_page_id));
  } else {
    is_auto_ = is_auto;
    shrink_begin_ts_ = ObTimeUtility::current_time();
    lower_page_id_ = lower_page_id;
    upper_page_id_ = upper_page_id;
    max_allow_alloc_page_id_ = upper_page_id; // init as upper_page_id
    ATOMIC_SET(&is_inited_, true);
  }
  return ret;
}

void WriteCacheShrinkContext::reset()
{
  ATOMIC_SET(&is_inited_, false);
  is_auto_ = false;
  shrink_state_ = SHRINK_STATE::INVALID;

  max_allow_alloc_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  lower_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  upper_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  current_max_used_watermark_ = 0;
  last_shrink_complete_ts_ = ObTimeUtility::current_time();
  shrink_begin_ts_ = 0;
  shrink_list_.reset();
}

bool WriteCacheShrinkContext::is_valid()
{
  return ATOMIC_LOAD(&is_inited_) &&
         lower_page_id_ < upper_page_id_ &&
         max_allow_alloc_page_id_ + 1 >= lower_page_id_ &&
         SHRINK_STATE::INVALID != shrink_state_ &&
         (upper_page_id_ - lower_page_id_ + 1) % ObTmpFileWriteCache::BLOCK_PAGE_NUMS == 0;
}

bool WriteCacheShrinkContext::in_not_alloc_range(uint32_t page_id)
{
  return ATOMIC_LOAD(&is_inited_) &&
         page_id > ATOMIC_LOAD(&max_allow_alloc_page_id_) &&
         page_id <= upper_page_id_;
}

// @assert node != nullptr
bool WriteCacheShrinkContext::in_shrinking_range(PageNode *node)
{
  ObTmpFilePage &page = node->page_;
  uint32_t index = page.get_array_index();
  return ATOMIC_LOAD(&is_inited_) && index >= lower_page_id_ && index <= upper_page_id_;
}

bool WriteCacheShrinkContext::is_execution_too_long() const
{
  return ATOMIC_LOAD(&is_inited_) && shrink_begin_ts_ > 0 &&
         ObTimeUtility::current_time() - shrink_begin_ts_ > MAX_SHRINKING_DURATION;
}

int64_t WriteCacheShrinkContext::get_auto_shrinking_percent(const int64_t current_watermark)
{
  int64_t shrink_percent = 0;
  if (current_watermark == AUTO_SHRINKING_WATERMARK_L3) {
    shrink_percent = AUTO_SHRINKING_TARGET_SIZE_L3;
  } else if (current_watermark <= AUTO_SHRINKING_WATERMARK_L2) {
    shrink_percent = AUTO_SHRINKING_TARGET_SIZE_L2;
  } else if (current_watermark <= AUTO_SHRINKING_WATERMARK_L1) {
    shrink_percent = AUTO_SHRINKING_TARGET_SIZE_L1;
  } else {
    LOG_INFO("used page number watermark increases, should stop shrinking", K(current_watermark));
  }
  return shrink_percent;
}

int64_t WriteCacheShrinkContext::get_not_alloc_page_num()
{
  int64_t not_alloc_page_num = is_valid() ? upper_page_id_ - max_allow_alloc_page_id_ : 0;
  return max(0, not_alloc_page_num);
}

}  // end namespace tmp_file
}  // end namespace oceanbase
