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
#include "sql/engine/basic/chunk_store/ob_compact_store.h"
#include "sql/engine/basic/chunk_store/ob_block_ireader.h"
#include "sql/engine/basic/chunk_store/ob_default_block_writer.h"
#include "sql/engine/basic/chunk_store/ob_default_block_reader.h"
#include "sql/engine/basic/chunk_store/ob_compact_block_reader.h"
#include "sql/engine/basic/chunk_store/ob_compact_block_writer.h"
#include "storage/ddl/ob_direct_load_struct.h"

namespace oceanbase
{
namespace sql
{

int ObCompactStore::prepare_blk_for_write(Block *blk)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the store is not inited", K(ret));
  } else if (OB_FAIL(writer_->prepare_blk_for_write(blk))) {
    LOG_WARN("fail to prepare blk for write", K(ret));
  }

  return ret;
}

int ObCompactStore::prepare_blk_for_read(Block *blk)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the store is not inited", K(ret));
  } else if (OB_FAIL(reader_->prepare_blk_for_read(blk))) {
    LOG_WARN("fail to prepare blk for write", K(ret));
  }

  return ret;
}

void ObCompactStore::rescan()
{
  cur_blk_id_ = 0;
  start_iter_ = false;
  if (OB_NOT_NULL(reader_)) {
    reader_->reuse();
  }
  block_reader_.reuse();
}

int ObCompactStore::inner_get_next_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  const ObTempBlockStore::Block* tmp_blk = nullptr;

  if (inited_) {
    if (!start_iter_) {
      cur_blk_id_ = 0;
      if (cur_blk_id_ >= get_block_id_cnt()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(block_reader_.get_block(cur_blk_id_, tmp_blk))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get block", K(ret), K(cur_blk_id_));
        }
      } else {
        start_iter_ = true;
        reader_->reuse();
        reader_->set_block(tmp_blk);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(reader_->get_row(sr))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get row", K(ret), K(cur_blk_id_));
      } else if (cur_blk_id_ >= get_block_id_cnt()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(block_reader_.get_block(cur_blk_id_, tmp_blk))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get block", K(ret), K(cur_blk_id_));
        }
      } else {
        reader_->reuse();
        reader_->set_block(tmp_blk);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(reader_->get_row(sr))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("fail to get row", K(ret));
            }
          } else {
            cur_blk_id_++;
          }
        }
      }
    } else {
      cur_blk_id_++;
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("block reader read row", KPC(sr));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store is not init", K(ret));
  }
  return ret;
}

int ObCompactStore::inner_add_batch(const common::ObDatum **datums, const common::ObIArray<ObExpr *> &exprs,
                                    const uint16_t selector[], const int64_t size,
                                    ObChunkDatumStore::StoredRow **stored_rows)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    if (OB_FAIL(writer_->add_batch(datums, exprs, selector, size, stored_rows, batch_ctx_))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      row_cnt_ += size;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should init writer first", K(ret));
  }

  return ret;
}

int ObCompactStore::add_batch_fallback(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                                       const ObBitVector &skip, const int64_t batch_size,
                                       const uint16_t selector[], const int64_t size,
                                       ObChunkDatumStore::StoredRow **stored_rows)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(batch_size);
  for (int64_t i = 0; i < size && OB_SUCC(ret); i++) {
    int64_t idx = selector[i];
    batch_info_guard.set_batch_idx(idx);
    ObChunkDatumStore::StoredRow *srow = NULL;
    if (OB_FAIL(add_row(exprs, ctx, &srow))) {
      LOG_WARN("add row failed", K(ret), K(i), K(idx));
    } else {
      if (NULL != stored_rows) {
        stored_rows[i] = srow;
      }
    }
  }
  return ret;
}

int ObCompactStore::add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                              const ObBitVector &skip, const int64_t batch_size,
                              int64_t &stored_rows_count,
                              ObChunkDatumStore::StoredRow **stored_rows,
                              const int64_t start_pos /* 0 */)
{
  int ret = OB_SUCCESS;
  CK(is_inited());
  OZ(init_batch_ctx(exprs.count(), ctx.max_batch_size_));
  int64_t size = 0;
  if (OB_SUCC(ret)) {
    for (int64_t i = start_pos; i < batch_size; i++) {
      if (skip.at(i)) {
        continue;
      } else {
        batch_ctx_->selector_[size++] = i;
      }
    }
  }
  if (OB_SUCC(ret)) {
    stored_rows_count = size;
    if (OB_FAIL(add_batch(exprs, ctx, skip, batch_size,
                          batch_ctx_->selector_, size, stored_rows))) {
      LOG_WARN("add batch failed");
    }
  }
  return ret;
}

int ObCompactStore::add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                              const ObBitVector &skip, const int64_t batch_size,
                              const uint16_t selector[], const int64_t size,
                              ObChunkDatumStore::StoredRow **stored_rows)
{
  int ret = OB_SUCCESS;
  CK(is_inited());
  OZ(init_batch_ctx(exprs.count(), ctx.max_batch_size_));
  bool all_batch_res = false;
  for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
    ObExpr *e = exprs.at(i);
    if (OB_ISNULL(e)) {
      batch_ctx_->datums_[i] = nullptr;
    } else if (OB_FAIL(e->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("evaluate batch failed", K(ret));
    } else {
      if (!e->is_batch_result()) {
        all_batch_res = false;
        break;
      } else {
        batch_ctx_->datums_[i] = e->locate_batch_datums(ctx);
      }
    }
  }
  if (OB_SUCC(ret) && !all_batch_res) {
    if (OB_FAIL(add_batch_fallback(exprs, ctx, skip, batch_size, selector, size, stored_rows))) {
      LOG_WARN("add batch fallback failed", K(batch_size), K(size));
    }
  }

  if (OB_SUCC(ret) && all_batch_res) {
    if (OB_FAIL(inner_add_batch(batch_ctx_->datums_, exprs, selector, size,
                                NULL == stored_rows ? batch_ctx_->stored_rows_ : stored_rows))) {
      LOG_WARN("inner add batch failed", K(ret), K(batch_size), K(size));
    }
  }

  return ret;
}
int ObCompactStore::has_next(bool &has_next)
{
  int ret = OB_SUCCESS;
  has_next = false;
  if (inited_) {
    if (cur_blk_id_ < block_id_cnt_) {
      has_next = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should init writer first", K(ret));
  }

  return ret;
}

int ObCompactStore::add_row(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx, ObChunkDatumStore::StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    if (OB_FAIL(writer_->add_row(exprs, ctx, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      row_cnt_++;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should init writer first", K(ret));
  }
  return ret;
}

int ObCompactStore::add_row(const blocksstable::ObDatumRow &datum_row, const ObStorageColumnGroupSchema &cg_schema,
                            const int64_t extra_size, ObChunkDatumStore::StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    if (OB_FAIL(writer_->add_row(datum_row.storage_datums_, cg_schema, extra_size, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      row_cnt_++;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should init writer first", K(ret));
  }
  return ret;
}

int ObCompactStore::add_row(const ObChunkDatumStore::StoredRow &src_sr, ObChunkDatumStore::StoredRow **dst_sr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    if (OB_FAIL(writer_->add_row(src_sr, dst_sr))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      row_cnt_++;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should init writer first", K(ret));
  }

  return ret;
}

int ObCompactStore::get_next_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    if (OB_FAIL(inner_get_next_row(sr))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get row", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should init reader first", K(ret));
  }
  return ret;
}

int ObCompactStore::init_batch_ctx(const int64_t col_cnt, const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == batch_ctx_)) {
    const int64_t size = sizeof(*batch_ctx_)
        + sizeof(ObDatum *) * col_cnt
        + sizeof(*batch_ctx_->row_size_array_) * max_batch_size
        + sizeof(*batch_ctx_->selector_) * max_batch_size
        + sizeof(*batch_ctx_->stored_rows_) * max_batch_size;
    char *mem = static_cast<char *>(alloc(size));
    if (OB_UNLIKELY(max_batch_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max batch size is not positive when init batch ctx", K(ret), K(max_batch_size));
    } else if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(size), K(col_cnt), K(max_batch_size));
    } else {
      char* begin = mem;
      batch_ctx_ = reinterpret_cast<BatchCtx *>(mem);
      mem += sizeof(*batch_ctx_);
#define SET_BATCH_CTX_FIELD(X, N) \
      batch_ctx_->X = reinterpret_cast<typeof(batch_ctx_->X)>(mem); \
      mem += sizeof(*batch_ctx_->X) * N;

      SET_BATCH_CTX_FIELD(datums_, col_cnt);
      SET_BATCH_CTX_FIELD(stored_rows_, max_batch_size);
      SET_BATCH_CTX_FIELD(row_size_array_, max_batch_size);
      SET_BATCH_CTX_FIELD(selector_, max_batch_size);
#undef SET_BATCH_CTX_FIELD

      if (mem - begin != size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("size mismatch", K(ret), K(mem - begin), K(size), K(col_cnt), K(max_batch_size));
      }
    }
  }
  return ret;
}

int ObCompactStore::init(const int64_t mem_limit,
                         const uint64_t tenant_id,
                         const int64_t mem_ctx_id,
                         const char *label,
                         const bool enable_dump,
                         const uint32_t row_extra_size,
                         const bool enable_trunc,
                         const ObCompressorType compress_type,
                         const ExprFixedArray *exprs)
{
  int ret = OB_SUCCESS;
  inited_ = true;
  OZ(ObTempBlockStore::init(mem_limit, enable_dump, tenant_id, mem_ctx_id, label, compress_type, enable_trunc));
  OZ(block_reader_.init(this));
  if (OB_NOT_NULL(exprs)) {
    OZ(row_meta_.init(*exprs, row_extra_size));
  }
  OZ(init_writer_reader());
  LOG_INFO("success to init compact store", K(enable_dump), K(enable_trunc), K(compress_type),
            K(exprs), K(ret));
  return ret;
}

int ObCompactStore::init(const int64_t mem_limit,
                         const ObIArray<storage::ObColumnSchemaItem> &col_array,
                         const uint64_t tenant_id,
                         const int64_t mem_ctx_id,
                         const char *label,
                         const bool enable_dump,
                         const uint32_t row_extra_size,
                         const bool enable_trunc,
                         const ObCompressorType compress_type)
{
  int ret = OB_SUCCESS;
  inited_ = true;
  OZ(row_meta_.init(col_array, row_extra_size));
  OZ(ObTempBlockStore::init(mem_limit, enable_dump, tenant_id, mem_ctx_id, label, compress_type, enable_trunc));
  OZ(block_reader_.init(this));
  OZ(init_writer_reader());
  LOG_INFO("success to init compact store", K(enable_dump), K(enable_trunc), K(compress_type),
            K(col_array), K(ret));
  return ret;
}

void ObCompactStore::reset()
{
  if (OB_NOT_NULL(reader_)) {
    reader_->reset();
    get_inner_allocator().free(reader_);
  }
  if (OB_NOT_NULL(writer_)) {
    writer_->reset();
    get_inner_allocator().free(writer_);
  }
  writer_ = nullptr;
  reader_ = nullptr;
  batch_ctx_ = nullptr;
  row_cnt_ = 0;
  start_iter_ = false;
  block_reader_.reset();
  cur_blk_id_ = 0;
}

int ObCompactStore::finish_write()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(writer_) || !inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the store in not proper status", K(ret));
  } else if (OB_FAIL(writer_->close())) {
    LOG_WARN("fail to flush buffer", K(ret));
  }
  return ret;
}


int ObCompactStore::init_writer_reader()
{
  int ret = OB_SUCCESS;
  void *writer_buf = nullptr;
  void *reader_buf = nullptr;
  writer_buf = get_inner_allocator().alloc(sizeof(ObCompactBlockWriter));
  reader_buf = get_inner_allocator().alloc(sizeof(ObCompactBlockReader));
  if (OB_ISNULL(writer_buf) || OB_ISNULL(reader_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for writer", K(ret));
  } else {
    writer_ = new (writer_buf)ObCompactBlockWriter(this, &row_meta_);
    reader_ = new (reader_buf)ObCompactBlockReader(this, &row_meta_);
  }

  return ret;
}

int ObCompactStore::get_last_stored_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store is not inited", K(ret));
  } else if (OB_FAIL(writer_->get_last_stored_row(sr))) {
    LOG_WARN("fail to get last stored row", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObCompactStore)
{
  int ret = OB_ERR_UNEXPECTED;
  return ret;
}


OB_DEF_DESERIALIZE(ObCompactStore)
{
  int ret = OB_ERR_UNEXPECTED;
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCompactStore)
{
  int64_t len = 0;
  return len;
}

}
}
