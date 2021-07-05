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

#include "sql/engine/join/ob_hash_join_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {
namespace sql {

//****************************** ObHashJoinBatch ******************************
ObHashJoinBatch::~ObHashJoinBatch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(close())) {
    LOG_WARN("close hash join batch failed", K(ret));
  }
}

int ObHashJoinBatch::rescan()
{
  int ret = OB_SUCCESS;
  row_store_iter_.reset();
  chunk_iter_.reset();
  return ret;
}

int ObHashJoinBatch::load_next_chunk()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(chunk_iter_.load_next_chunk(row_store_iter_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to load next chunk", K(ret));
    }
  }
  return ret;
}

int ObHashJoinBatch::finish_dump(bool memory_need_dump)
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

int ObHashJoinBatch::dump(bool all_dump)
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

int ObHashJoinBatch::add_row(const ObIArray<ObExpr*>& exprs, ObEvalCtx* eval_ctx, ObHashJoinStoredJoinRow*& stored_row)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow* hash_store_row = nullptr;
  if (OB_FAIL(chunk_row_store_.add_row(exprs, eval_ctx, &hash_store_row))) {
    LOG_WARN("failed to add row", K(ret));
  } else {
    stored_row = static_cast<ObHashJoinStoredJoinRow*>(hash_store_row);
    ++n_add_rows_;
  }
  return ret;
}

int ObHashJoinBatch::add_row(const ObHashJoinStoredJoinRow* src_stored_row, ObHashJoinStoredJoinRow*& stored_row)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow* hash_store_row = nullptr;
  if (OB_FAIL(chunk_row_store_.add_row(*src_stored_row, &hash_store_row))) {
    LOG_WARN("failed to add row", K(ret));
  } else {
    stored_row = static_cast<ObHashJoinStoredJoinRow*>(hash_store_row);
    ++n_add_rows_;
  }
  return ret;
}

int ObHashJoinBatch::convert_row(
    const ObHashJoinStoredJoinRow* stored_row, const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_store_iter_.convert_to_row(stored_row, exprs, eval_ctx))) {
    LOG_WARN("failed to convert row", K(ret));
  }
  return ret;
}

int ObHashJoinBatch::get_next_row(
    const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, const ObHashJoinStoredJoinRow*& stored_row)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow* inner_stored_row = nullptr;
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
            LOG_WARN("Got row count is not match with row count of chunk row store",
                K(ret),
                K(n_get_rows_),
                K(chunk_row_store_.get_row_cnt()));
          }
        }
      }
    } else {
      stored_row = static_cast<const ObHashJoinStoredJoinRow*>(inner_stored_row);
      if (OB_FAIL(convert_row(stored_row, exprs, eval_ctx))) {
        LOG_WARN("failed to convert row", K(ret));
      }
      ++n_get_rows_;
    }
  }
  return ret;
}

int ObHashJoinBatch::get_next_row(const ObHashJoinStoredJoinRow*& stored_row)
{
  int ret = OB_SUCCESS;
  stored_row = nullptr;
  const ObChunkDatumStore::StoredRow* inner_stored_row = nullptr;
  while (OB_SUCC(ret) && nullptr == stored_row) {
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
            LOG_WARN("Got row count is not match with row count of chunk row store",
                K(ret),
                K(n_get_rows_),
                K(chunk_row_store_.get_row_cnt()));
          }
        }
      }
    } else {
      stored_row = static_cast<const ObHashJoinStoredJoinRow*>(inner_stored_row);
      ++n_get_rows_;
    }
  }
  return ret;
}

int ObHashJoinBatch::get_next_block_row(const ObHashJoinStoredJoinRow*& stored_row)
{
  int ret = OB_SUCCESS;
  if (!row_store_iter_.is_valid()) {
    if (OB_FAIL(load_next_chunk())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to load next chunk", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    stored_row = nullptr;
    const ObChunkDatumStore::StoredRow* inner_stored_row = nullptr;
    while (OB_SUCC(ret) && nullptr == stored_row) {
      if (OB_FAIL(row_store_iter_.get_next_block_row(inner_stored_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        } else {
          chunk_iter_.reset();
          row_store_iter_.reset();
        }
      } else {
        stored_row = static_cast<const ObHashJoinStoredJoinRow*>(inner_stored_row);
        ++n_get_rows_;
      }
    }
  }
  return ret;
}

int ObHashJoinBatch::init_progressive_iterator()
{
  int ret = OB_SUCCESS;
  int64_t chunk_size = 0;
  row_store_iter_.reset();
  chunk_iter_.reset();
  chunk_size = buf_mgr_->get_page_size();
  if (chunk_size != ObChunkDatumStore::BLOCK_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: chunk size", K(ret), K(chunk_size));
  } else if (OB_FAIL(chunk_row_store_.begin(chunk_iter_, chunk_size))) {
    LOG_WARN("failed to set row store iterator", K(ret), K(chunk_size));
  }
  n_get_rows_ = 0;
  LOG_TRACE("set iterator: ", K(chunk_size));
  return ret;
}

int ObHashJoinBatch::set_iterator(bool is_chunk_iter)
{
  int ret = OB_SUCCESS;
  int64_t chunk_size = 0;
  row_store_iter_.reset();
  chunk_iter_.reset();
  if (OB_ISNULL(buf_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf_mgr_ is null", K(ret));
  } else if (is_chunk_iter) {
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

int ObHashJoinBatch::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(chunk_row_store_.init(
          0, tenant_id_, common::ObCtxIds::WORK_AREA, common::ObModIds::OB_ARENA_HASH_JOIN, true, 8))) {
    LOG_WARN("failed to init chunk row store", K(ret));
  } else {
    chunk_row_store_.set_callback(buf_mgr_);
  }
  return ret;
}

int ObHashJoinBatch::open()
{
  return OB_SUCCESS;
}

int ObHashJoinBatch::close()
{
  int ret = OB_SUCCESS;
  row_store_iter_.reset();
  chunk_iter_.reset();
  chunk_row_store_.reset();
  buf_mgr_ = nullptr;
  return ret;
}

//****************************** ObHashJoinBatchMgr ******************************

ObHashJoinBatchMgr::~ObHashJoinBatchMgr()
{
  reset();
}

void ObHashJoinBatchMgr::reset()
{
  FOREACH(p, batch_list_)
  {
    if (NULL != p->left_) {
      free(p->left_);
      p->left_ = NULL;
    }
    if (NULL != p->right_) {
      free(p->right_);
      p->right_ = NULL;
    }
  }
  batch_list_.clear();
}

int ObHashJoinBatchMgr::next_batch(ObHashJoinBatchPair& batch_pair)
{
  int ret = batch_list_.pop_front(batch_pair);
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_ITER_END;
  } else if (OB_SUCCESS != ret) {
    LOG_WARN("fail to pop front", K(ret));
  }
  return ret;
}

int ObHashJoinBatchMgr::remove_undumped_batch()
{
  int ret = OB_SUCCESS;
  int64_t size = batch_list_.size();
  int64_t erase_cnt = 0;
  hj_batch_pair_list_type::iterator iter = batch_list_.begin();
  hj_batch_pair_list_type::iterator pre_iter = batch_list_.end();
  while (OB_SUCC(ret) && iter != batch_list_.end()) {
    ObHashJoinBatch* left = iter->left_;
    ObHashJoinBatch* right = iter->right_;
    bool erased = false;
    if (nullptr != left && nullptr != right) {
      if (left->is_dumped()) {
        // right maybe empty
        // if (!right->is_dumped()) {
        //   ret = OB_ERR_UNEXPECTED;
        //   LOG_WARN("unexpect batch is not match", K(ret), K(left), K(right));
        // }
      } else if (right->is_dumped()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect batch is not match", K(ret), K(left), K(right));
      } else {
        ++erase_cnt;
        erased = true;
        if (OB_FAIL(batch_list_.erase(iter))) {
          LOG_WARN("failed to remove iter", K(left->get_part_level()), K(left->get_batchno()));
        } else {
          free(left);
          free(right);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect batch is null", K(ret), K(left), K(right));
    }
    if (!erased) {
      pre_iter = iter;
      ++iter;
    } else if (pre_iter != batch_list_.end()) {
      iter = pre_iter;
    } else {
      iter = batch_list_.begin();
    }
  }
  if (0 < erase_cnt) {
    LOG_TRACE("trace remove undumped batch", K(ret), K(erase_cnt));
  }
  if (OB_SUCC(ret) && erase_cnt + batch_list_.size() != size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to remove", K(ret));
  }
  return ret;
}

int ObHashJoinBatchMgr::get_or_create_batch(
    int32_t level, int32_t batchno, bool is_left, ObHashJoinBatch*& batch, bool only_get)
{
  int ret = OB_SUCCESS;
  bool flag = false;
  for (hj_batch_pair_list_type::iterator iter = batch_list_.begin(); iter != batch_list_.end(); iter++) {
    if (is_left) {
      if (iter->left_->get_part_level() == level && iter->left_->get_batchno() == batchno) {
        batch = iter->left_;
        flag = true;
        break;
      }
    } else {
      if (iter->right_->get_part_level() == level && iter->right_->get_batchno() == batchno) {
        batch = iter->right_;
        flag = true;
        break;
      }
    }
  }

  if (!flag && !only_get) {  // not found, should new one
    ObHashJoinBatchPair batch_pair;
    void* buf = alloc_.alloc(sizeof(ObHashJoinBatch));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      batch_count_++;
      batch_pair.left_ = new (buf) ObHashJoinBatch(alloc_, buf_mgr_, tenant_id_, level, batchno);
    }
    if (OB_SUCC(ret)) {
      buf = alloc_.alloc(sizeof(ObHashJoinBatch));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        batch_count_++;
        batch_pair.right_ = new (buf) ObHashJoinBatch(alloc_, buf_mgr_, tenant_id_, level, batchno);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(batch_list_.push_front(batch_pair))) {
        LOG_WARN("fail to push batch pair to batch list", K(ret));
      } else {
        if (is_left) {
          batch = batch_pair.left_;
        } else {
          batch = batch_pair.right_;
        }
      }
    }
  }

  return ret;
}

//****************************** ObHashJoinPartition ******************************
int ObHashJoinPartition::init(int32_t part_level, int32_t part_id, bool is_left, ObHashJoinBufMgr* buf_mgr,
    ObHashJoinBatchMgr* batch_mgr, ObHashJoinBatch* pre_batch, ObOperator* phy_op, ObSqlMemoryCallback* callback,
    int64_t dir_id)
{
  int ret = OB_SUCCESS;
  UNUSED(pre_batch);
  UNUSED(phy_op);
  part_level_ = part_level;
  part_id_ = part_id;
  buf_mgr_ = buf_mgr;
  batch_mgr_ = batch_mgr;
  if (OB_ISNULL(buf_mgr) || OB_ISNULL(batch_mgr)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("buf mgr or batch_mgr is null", K(ret), K(part_level_), K(part_id_), K(is_left), K(buf_mgr), K(batch_mgr));
  } else if (OB_FAIL(batch_mgr_->get_or_create_batch(part_level_, part_id_, is_left, batch_))) {
    LOG_WARN("fail to get batch", K(ret), K(part_level_), K(part_id_), K(is_left));
  } else if (OB_ISNULL(batch_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fail to get batch ", K(ret), K(part_level_), K(part_id_), K(is_left));
  } else if (OB_FAIL(check())) {
    LOG_WARN("fail to check partition", K(ret), K(part_level_), K(part_id_), K(is_left));
  } else if (OB_FAIL(batch_->init())) {
    LOG_WARN("fail to init batch", K(ret), K(part_level_), K(part_id_), K(is_left));
  } else {
    batch_->get_chunk_row_store().set_block_size(buf_mgr_->get_page_size());
    batch_->get_chunk_row_store().set_callback(callback);
    batch_->get_chunk_row_store().set_dir_id(dir_id);
  }
  return ret;
}

int ObHashJoinPartition::record_pre_batch_info(int64_t pre_part_count, int64_t pre_bucket_number, int64_t total_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(batch_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fail to get batch ", K(ret), K(pre_part_count), K(pre_bucket_number), K(total_size));
  } else {
    batch_->set_pre_part_count(pre_part_count);
    batch_->set_pre_bucket_number(pre_bucket_number);
    batch_->set_pre_total_size(total_size);
  }
  return ret;
}

int ObHashJoinPartition::init_iterator(bool is_chunk_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(batch_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch_ is null", K(ret));
  } else if (OB_FAIL(batch_->set_iterator(is_chunk_iter))) {
    LOG_WARN("failed to set iterator", K(ret));
  }
  return ret;
}

int ObHashJoinPartition::init_progressive_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(batch_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch_ is null", K(ret));
  } else if (OB_FAIL(batch_->init_progressive_iterator())) {
    LOG_WARN("failed to set iterator", K(ret));
  }
  return ret;
}

int ObHashJoinPartition::check()
{
  int ret = common::OB_SUCCESS;
  if (part_level_ == -1 || part_id_ == -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part_level_ and part_id_ should not be null");
  } else if (buf_mgr_ == NULL || batch_mgr_ == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf_mgr_ and batch_mgr_ should not be null");
  }
  return ret;
}

void ObHashJoinPartition::reset()
{
  buf_mgr_ = nullptr;
  batch_mgr_ = nullptr;
  batch_ = nullptr;
}

int ObHashJoinPartition::add_row(
    const ObIArray<ObExpr*>& exprs, ObEvalCtx* eval_ctx, ObHashJoinStoredJoinRow*& stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->add_row(exprs, eval_ctx, stored_row))) {
    LOG_WARN("failed to add row to chunk row store");
  }
  return ret;
}

int ObHashJoinPartition::add_row(const ObHashJoinStoredJoinRow* src_stored_row, ObHashJoinStoredJoinRow*& stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->add_row(src_stored_row, stored_row))) {
    LOG_WARN("failed to add row to chunk row store");
  }
  return ret;
}

int ObHashJoinPartition::finish_dump(bool memory_need_dump)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->finish_dump(memory_need_dump))) {
    LOG_WARN("fail to finish batch dump", K(ret));
  }
  return ret;
}

int ObHashJoinPartition::dump(bool all_dump)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->dump(all_dump))) {
    LOG_WARN("failed to dump data to chunk row store", K(ret));
  }
  return ret;
}

int ObHashJoinPartition::get_next_row(const ObHashJoinStoredJoinRow*& stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->get_next_row(stored_row)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next row", K(ret));
  }
  return ret;
}

int ObHashJoinPartition::get_next_block_row(const ObHashJoinStoredJoinRow*& stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_->get_next_block_row(stored_row)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next row", K(ret));
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
