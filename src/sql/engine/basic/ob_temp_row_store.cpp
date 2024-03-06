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

#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ObTempRowStore::RowBlock::get_store_row(int64_t &cur_pos, const ObCompactRow *&sr) const
{
  int ret = OB_SUCCESS;
  if (cur_pos >= raw_size_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid index", K(ret), K(cur_pos), K_(cnt));
  } else {
    const ObCompactRow *row = reinterpret_cast<const ObCompactRow *>(&payload_[cur_pos]);
    cur_pos += row->get_row_size();
    sr = row;
  }
  return ret;
}

int ObTempRowStore::RowBlock::add_row(const ObCompactRow *src_row, ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src row is null", K(ret));
  } else {
    ShrinkBuffer *buf = get_buffer();
    stored_row = new(buf->head())ObCompactRow();
    MEMCPY(stored_row, src_row, src_row->get_row_size());
    buf->fast_advance(src_row->get_row_size());
    ++cnt_;
  }
  return ret;
}

int ObTempRowStore::Iterator::init(ObTempRowStore *store)
{
  reset();
  row_store_ = store;
  return BlockReader::init(store);
}

int ObTempRowStore::Iterator::get_next_row(const ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == cur_blk_ || !cur_blk_->contain(cur_blk_id_))) {
    if (OB_FAIL(next_block())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next block", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!cur_blk_->contain(cur_blk_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current block is invalid", K(ret));
    } else if (OB_FAIL(cur_blk_->get_store_row(read_pos_, stored_row))) {
      LOG_WARN("fail to get batch from block", K(ret));
    } else {
      cur_blk_id_++;
    }
  }
  return ret;
}

int ObTempRowStore::Iterator::next_block()
{
  int ret = OB_SUCCESS;
  const Block *read_blk = NULL;
  if (cur_blk_id_ >= get_row_cnt()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_block(cur_blk_id_, read_blk))) {
    LOG_WARN("fail to get block from store", K(ret), K(cur_blk_id_));
  } else {
    LOG_DEBUG("next block", KP(read_blk), K(*read_blk), K(read_blk->checksum()));
    cur_blk_ = static_cast<const RowBlock*>(read_blk);
    row_idx_ = 0;
    read_pos_ = 0;
  }
  return ret;
}

int ObTempRowStore::init(const int64_t mem_limit,
                         bool enable_dump,
                         const uint64_t tenant_id,
                         const int64_t mem_ctx_id,
                         const char *label,
                         const common::ObCompressorType compressor_type /*NONE_COMPRESSOR*/,
                         const bool enable_trunc /*false*/)
{
  int ret = OB_SUCCESS;
  OZ(ObTempBlockStore::init(mem_limit, enable_dump, tenant_id, mem_ctx_id, label,
                            compressor_type, enable_trunc));
  inited_ = true;
  return ret;
}

int ObTempRowStore::add_row(const ObCompactRow *src_row, ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_row) || src_row->get_row_size() <= 0) {
  } else if (OB_FAIL(ensure_write_blk(src_row->get_row_size()))) {
    LOG_WARN("ensure write block failed", K(ret), K(src_row->get_row_size()));
  } else if (OB_FAIL(cur_blk_->add_row(src_row, stored_row))) {
    LOG_WARN("fail to add row", K(ret));
  } else {
    block_id_cnt_ += 1;
    inc_mem_used(src_row->get_row_size());
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTempRowStore)
{
  int ret = OB_ERR_UNEXPECTED;
  return ret;
}


OB_DEF_DESERIALIZE(ObTempRowStore)
{
  int ret = OB_ERR_UNEXPECTED;
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTempRowStore)
{
  int64_t len = 0;
  return len;
}


} // end namespace sql
} // end namespace oceanbase
