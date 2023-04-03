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

#include "ob_hj_batch.h"
#include "ob_hj_partition.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
using namespace join;
using namespace blocksstable;


ObHJBatch::~ObHJBatch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(close())) {
    LOG_WARN("close hash join batch failed", K(ret));
  }
}

int ObHJBatch::rescan()
{
  int ret = OB_SUCCESS;
  row_store_iter_.reset();
  chunk_iter_.reset();
  return ret;
}

int ObHJBatch::load_next_chunk()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(chunk_iter_.load_next_chunk(row_store_iter_))) {
    LOG_WARN("faild to load next chunk", K(ret));
  }
  return ret;
}

int ObHJBatch::finish_dump(bool memory_need_dump)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(chunk_row_store_.finish_add_row(memory_need_dump))) {
    LOG_WARN("failed to finish chunk row store", K(ret));
  } else if (memory_need_dump && 0 != chunk_row_store_.get_mem_used()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect memroy is 0", K(ret), K(chunk_row_store_.get_mem_used()));
  }
  return ret;
}

int ObHJBatch::dump(bool all_dump)
{
  int ret = OB_SUCCESS;
  if (0 >= chunk_row_store_.get_mem_used()) {
    // ret = OB_ERR_UNEXPECTED;
    // LOG_WARN("unexpect chunk row store use memory", K(ret), K(chunk_row_store_.get_mem_used()));
  } else if (OB_FAIL(chunk_row_store_.dump(false, all_dump))) {
    LOG_WARN("failed to dump data to chunk row store", K(ret));
  }
  return ret;
}

int ObHJBatch::add_row(const common::ObNewRow &row, ObStoredJoinRow *&stored_row)
{
  int ret = OB_SUCCESS;
  ObChunkRowStore::StoredRow *hash_store_row = nullptr;
  if (OB_FAIL(chunk_row_store_.add_row(row, &hash_store_row))) {
    LOG_WARN("failed to add row", K(ret));
  } else {
    stored_row = static_cast<ObStoredJoinRow *>(hash_store_row);
    ++n_add_rows_;
  }
  return ret;
}

int ObHJBatch::convert_row(const common::ObNewRow *&row, const ObStoredJoinRow *&stored_row)
{
  int ret = OB_SUCCESS;
  ObNewRow *inner_row = nullptr;
  if (OB_FAIL(row_store_iter_.convert_to_row_full(inner_row, stored_row))) {
    LOG_WARN("failed to convert row", K(ret));
  } else {
    row = static_cast<const ObNewRow *>(inner_row);
  }
  return ret;
}

int ObHJBatch::get_next_row(const common::ObNewRow *&row, const ObStoredJoinRow *&stored_row)
{
  int ret = OB_SUCCESS;
  stored_row = nullptr;
  row = nullptr;
  const ObChunkRowStore::StoredRow *inner_stored_row = nullptr;
  while (OB_SUCC(ret) && nullptr == inner_stored_row) {
    if (OB_FAIL(row_store_iter_.get_next_row(inner_stored_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (!is_chunk_iter_) {
        ret = OB_SUCCESS;
        if (OB_FAIL(chunk_iter_.load_next_chunk(row_store_iter_))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row", K(ret));
          } else if (n_get_rows_ != chunk_row_store_.get_row_cnt()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Got row count is not match with row count of chunk row store", K(ret), K(n_get_rows_), K(chunk_row_store_.get_row_cnt()));
          } else {
            // LOG_TRACE("trace get row count", K(part_level_), K(batchno_), K(ret), K(n_get_rows_), K(chunk_row_store_.get_row_cnt()));
          }
        }
      }
    } else {
      ++n_get_rows_;
      stored_row = static_cast<const ObStoredJoinRow *>(inner_stored_row);
      if (OB_FAIL(convert_row(row, stored_row))) {
        LOG_WARN("failed to convert row", K(ret));
      }
    }
  }
  return ret;
}

int ObHJBatch::get_next_row(const ObStoredJoinRow *&stored_row)
{
  int ret = OB_SUCCESS;
  stored_row = nullptr;
  const ObChunkRowStore::StoredRow *inner_stored_row = nullptr;
  while (OB_SUCC(ret) && nullptr == inner_stored_row) {
    if (OB_FAIL(row_store_iter_.get_next_row(inner_stored_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (!is_chunk_iter_) {
        ret = OB_SUCCESS;
        if (OB_FAIL(chunk_iter_.load_next_chunk(row_store_iter_))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row", K(ret));
          } else if (n_get_rows_ != chunk_row_store_.get_row_cnt()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Got row count is not match with row count of chunk row store", K(ret), K(n_get_rows_), K(chunk_row_store_.get_row_cnt()));
          } else {
            LOG_TRACE("trace get row count", K(part_level_), K(batchno_), K(ret), K(n_get_rows_), K(chunk_row_store_.get_row_cnt()));
          }
        }
      }
    } else {
      ++n_get_rows_;
      stored_row = static_cast<const ObStoredJoinRow *>(inner_stored_row);
    }
  }
  return ret;
}

// 可能会读多次，所以每次都应该set iterator，同时reset
int ObHJBatch::set_iterator(bool is_chunk_iter)
{
  int ret = OB_SUCCESS;
  int64_t chunk_size = 0;
  row_store_iter_.reset();
  chunk_iter_.reset();
  if (OB_ISNULL(buf_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf_mgr_ is null", K(ret));
  } else if (is_chunk_iter) {
    // 上层逻辑手动调load_next_chunk来驱动获取一批数据
    chunk_size = buf_mgr_->get_reserve_memory_size();
    if (buf_mgr_->get_reserve_memory_size() > chunk_row_store_.get_mem_used() + chunk_row_store_.get_file_size()) {
      chunk_size = chunk_row_store_.get_mem_used() + chunk_row_store_.get_file_size();
    }
    if (buf_mgr_->get_page_size() > chunk_size) {
      chunk_size = buf_mgr_->get_page_size();
    }
    if (OB_FAIL(chunk_row_store_.begin(chunk_iter_, chunk_size))) {
      LOG_WARN("failed to set chunk iter", K(ret), K(buf_mgr_->get_reserve_memory_size()));
    }
  } else {
    chunk_size = buf_mgr_->get_page_size();
    if (OB_FAIL(chunk_row_store_.begin(chunk_iter_, chunk_size))) {
      LOG_WARN("failed to set row store iterator", K(ret), K(chunk_size));
    } else if (OB_FAIL(load_next_chunk())) {
      LOG_WARN("failed to load next chunk", K(ret));
    }
  }
  n_get_rows_ = 0;
  is_chunk_iter_ = is_chunk_iter;
  LOG_TRACE("set iterator: ", K(chunk_size), K(is_chunk_iter));
  return ret;
}

int ObHJBatch::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(chunk_row_store_.init(0, tenant_id_,
    common::ObCtxIds::WORK_AREA,
    common::ObModIds::OB_ARENA_HASH_JOIN,
    true, ObChunkRowStore::STORE_MODE::FULL, 8))) {
    LOG_WARN("failed to init chunk row store", K(ret));
  } else {
    chunk_row_store_.set_callback(buf_mgr_);
  }
  return ret;
}

int ObHJBatch::open()
{
  return OB_SUCCESS;
}

int ObHJBatch::close()
{
  int ret = OB_SUCCESS;
  row_store_iter_.reset();
  chunk_iter_.reset();
  chunk_row_store_.reset();
  buf_mgr_ = nullptr;
  return ret;
}


