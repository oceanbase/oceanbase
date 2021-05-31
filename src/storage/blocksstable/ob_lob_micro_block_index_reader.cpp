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

#include "ob_lob_micro_block_index_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObLobMicroBlockIndexReader::ObLobMicroBlockIndexReader()
    : offset_array_(NULL), size_array_(NULL), micro_block_cnt_(0), cursor_(0)
{}

int ObLobMicroBlockIndexReader::transform(
    const char* index_buf, const int32_t size_array_offset, const int64_t micro_block_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_buf) || size_array_offset <= 0 || micro_block_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(index_buf), K(size_array_offset), K(micro_block_cnt));
  } else {
    offset_array_ = reinterpret_cast<const int32_t*>(index_buf);
    size_array_ = reinterpret_cast<const ObLobMicroIndexSizeItem*>(index_buf + size_array_offset);
    micro_block_cnt_ = micro_block_cnt;
    cursor_ = 0;
    LOG_DEBUG("lob index reader info", K(micro_block_cnt_), K(cursor_), K(common::lbt()));
  }
  return ret;
}

int ObLobMicroBlockIndexReader::get_next_index_item(ObLobMicroIndexItem& next_item)
{
  int ret = OB_SUCCESS;
  if (cursor_ >= micro_block_cnt_) {
    ret = OB_ITER_END;
  } else {
    next_item.offset_ = offset_array_[cursor_];
    next_item.data_size_ = offset_array_[cursor_ + 1] - offset_array_[cursor_];
    next_item.size_item_ = size_array_[cursor_];
    ++cursor_;
  }
  return ret;
}
