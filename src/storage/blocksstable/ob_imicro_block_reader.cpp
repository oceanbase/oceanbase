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

#include "ob_imicro_block_reader.h"

#include "storage/ob_i_store.h"
#include "ob_column_map.h"
#include "ob_row_reader.h"
#include "ob_row_cache.h"
#include "ob_fuse_row_cache.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {

void ObIMicroBlockReader::reset()
{
  is_inited_ = false;
  begin_ = 0;
  end_ = ObIMicroBlockReader::INVALID_ROW_INDEX;
  column_map_ = nullptr;
}

int ObIMicroBlockReader::locate_rowkey(const common::ObStoreRowkey& rowkey, int64_t& row_idx)
{
  int ret = OB_SUCCESS;
  row_idx = INVALID_ROW_INDEX;
  bool is_equal = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(find_bound(rowkey, true /*lower_bound*/, begin(), end(), row_idx, is_equal))) {
    LOG_WARN("fail to lower_bound rowkey", K(ret));
  } else if (end() == row_idx || !is_equal) {
    row_idx = INVALID_ROW_INDEX;
    ret = OB_BEYOND_THE_RANGE;
  }
  return ret;
}

int ObIMicroBlockReader::locate_range(const ObStoreRange& range, const bool is_left_border, const bool is_right_border,
    int64_t& begin_idx, int64_t& end_idx)
{
  int ret = OB_SUCCESS;
  begin_idx = ObIMicroBlockReader::INVALID_ROW_INDEX;
  end_idx = ObIMicroBlockReader::INVALID_ROW_INDEX;
  bool equal = false;
  if (!is_left_border || range.get_start_key().is_min()) {
    begin_idx = begin();
  } else if (OB_FAIL(find_bound(range.get_start_key(), true /*lower_bound*/, begin(), end(), begin_idx, equal))) {
    LOG_WARN("fail to get lower bound start key", K(ret));
  } else if (begin_idx == end()) {
    ret = OB_BEYOND_THE_RANGE;
  } else if (!range.get_border_flag().inclusive_start()) {
    if (equal) {
      ++begin_idx;
      if (begin_idx == end()) {
        ret = OB_BEYOND_THE_RANGE;
      }
    }
  }
  LOG_DEBUG("locate range for start key",
      K(is_left_border),
      K(is_right_border),
      K(range),
      K(begin_idx),
      K(end_idx),
      K(equal));
  if (OB_SUCC(ret)) {
    if (!is_right_border || range.get_end_key().is_max()) {
      end_idx = end() - 1;
    } else {
      // we should use upper_bound if the range include endkey
      if (OB_FAIL(find_bound(range.get_end_key(),
              !range.get_border_flag().inclusive_end() /*lower_bound*/,
              begin_idx,
              end(),
              end_idx,
              equal))) {
        LOG_WARN("fail to get lower bound endkey", K(ret));
      } else if (end_idx == end()) {
        --end_idx;
      } else if (end_idx == begin()) {
        ret = OB_BEYOND_THE_RANGE;
      } else {
        --end_idx;
      }
    }
  }
  LOG_DEBUG(
      "locate range for end key", K(is_left_border), K(is_right_border), K(range), K(begin_idx), K(end_idx), K(equal));
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
