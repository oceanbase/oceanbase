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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/chunk_store/ob_default_block_reader.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_bitmap.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ObDefaultBlockReader::get_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_blk_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur block is null", K(ret));
  } else if (!blk_has_next_row()) {
    ret = OB_ITER_END;
  } else if (cur_pos_in_blk_ > cur_blk_->raw_size_ - sizeof(ObTempBlockStore::Block)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("get unexpected index", K(ret));
  } else {
    const ObChunkDatumStore::StoredRow *row = reinterpret_cast<const ObChunkDatumStore::StoredRow *>(&cur_blk_->payload_[cur_pos_in_blk_]);
    sr = row;
    cur_pos_in_blk_ += row->row_size_;
    cur_row_in_blk_ += 1;
  }

  return ret;
}

int ObDefaultBlockReader::prepare_blk_for_read(ObTempBlockStore::Block *blk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to block is null", K(ret));
  } else {
    int64_t cur_row = 0;
    int64_t cur_pos = 0;
    const int64_t buf_size = blk->raw_size_ - sizeof(ObTempBlockStore::Block);
    while (cur_row < blk->cnt_ && cur_pos < buf_size) {
      ObChunkDatumStore::StoredRow *sr = reinterpret_cast<ObChunkDatumStore::StoredRow*>(blk->payload_ + cur_pos);
      sr->swizzling();
      cur_pos += sr->row_size_;
      cur_row++;
    }
  }

  return ret;
}

}
}