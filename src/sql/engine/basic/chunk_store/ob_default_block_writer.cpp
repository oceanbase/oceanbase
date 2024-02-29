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

#include "sql/engine/basic/chunk_store/ob_default_block_writer.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_bitmap.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ObDefaultBlockWriter::add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, ObChunkDatumStore::StoredRow **stored_row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ensure_init())) {
    LOG_WARN("fail to ensure init", K(ret));
  } else if (OB_FAIL(ensure_write(exprs, ctx))) {
    LOG_WARN("fail to ensure write", K(ret));
  } else {
    if (OB_FAIL(inner_add_row(exprs, ctx, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    }
  }

  return ret;
}

int ObDefaultBlockWriter::add_row(const ObChunkDatumStore::StoredRow &src_sr, ObChunkDatumStore::StoredRow **dst_sr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ensure_init())) {
    LOG_WARN("fail to ensure init", K(ret));
  } else if (OB_FAIL(ensure_write(src_sr))) {
    LOG_WARN("fail to ensure write", K(ret));
  } else {
    ObChunkDatumStore::StoredRow *sr = new (get_cur_buf())ObChunkDatumStore::StoredRow;
    sr->assign(&src_sr);
    if (OB_FAIL(advance(sr->row_size_))) {
      LOG_WARN("fill buffer head failed", K(ret));
    } else {
      if (nullptr != dst_sr) {
        *dst_sr = sr;
      }
    }
  }

  return ret;
}

int ObDefaultBlockWriter::add_row(const blocksstable::ObStorageDatum *storage_datums, const ObStorageColumnGroupSchema &cg_schema,
                               const int64_t extra_size, ObChunkDatumStore::StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ensure_init())) {
    LOG_WARN("fail to ensure init", K(ret));
  } else if (OB_FAIL(ensure_write(storage_datums, cg_schema, extra_size))) {
    LOG_WARN("fail to ensure write", K(ret));
  } else if (OB_FAIL(inner_add_row(storage_datums, cg_schema, extra_size, stored_row))) {
    LOG_WARN("add row to block failed", K(ret), K(storage_datums), K(cg_schema), K(extra_size));
  }
  return ret;
}

template <typename T>
static void assign_datums(const ObDatum **datums, const uint16_t selector[], const int64_t start_pos,
                          const int64_t end_pos, ObChunkDatumStore::StoredRow **stored_rows, int64_t col_idx)
{
  const ObDatum *cur_datums = datums[col_idx];
  for (int64_t i = start_pos; i < end_pos; i++) {
    ObChunkDatumStore::StoredRow *srow = stored_rows[i];
    const ObDatum &src = cur_datums[selector[i]];
    ObDatum &dst = srow->cells()[col_idx];
    dst.pack_ = src.pack_;
    dst.ptr_ = reinterpret_cast<char *>(srow) + srow->row_size_;
    if (!src.is_null()) {
      T::assign_datum_value((void *)dst.ptr_, src.ptr_, src.len_);
    }
    srow->row_size_ += src.len_;
  }
}

int ObDefaultBlockWriter::add_batch(const common::ObDatum **datums, const common::ObIArray<ObExpr *> &exprs,
                                    const uint16_t selector[], const int64_t size,
                                    ObChunkDatumStore::StoredRow **stored_rows, BatchCtx *batch_ctx)
{
  int ret = OB_SUCCESS;
  uint32_t *size_array = batch_ctx->row_size_array_;
  int64_t col_cnt = exprs.count();
  const int64_t base_row_size = sizeof(ObChunkDatumStore::StoredRow)
      + sizeof(ObDatum) * col_cnt;
  for (int64_t i = 0; i < size; i++) {
    size_array[i] = base_row_size;
  }
  for (int64_t col_idx = 0; col_idx < col_cnt; col_idx++) {
    if (OB_ISNULL(datums[col_idx])) {
      continue;
    }
    const ObDatum *cur_datums = datums[col_idx];
    for (int64_t i = 0; i < size; i++) {
      size_array[i] += cur_datums[selector[i]].len_;
    }
  }

  // allocate block and assign stored rows
  int64_t idx = 0;
  OZ(ensure_init());
  while (idx < size && OB_SUCC(ret)) {
    if (OB_FAIL(ensure_write(size_array[idx]))) {
      LOG_WARN("ensure write block failed", K(ret), K(size_array[idx]), K(col_cnt), K(size));
      for (int64_t col_idx = 0; col_idx < col_cnt; col_idx++) {
        if (OB_ISNULL(datums[col_idx])) {
          continue;
        }
        const ObDatum *cur_datums = datums[col_idx];
      }
    } else {
      int64_t rows = 0;
      for (int64_t i = idx; i < size; i++) {
        const int64_t remain = get_remain();
        if (size_array[i] <= remain) {
          char *buf = get_cur_buf();
          if (OB_ISNULL(buf)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get cur buf", K(ret));
          }
          ObChunkDatumStore::StoredRow *srow = reinterpret_cast<ObChunkDatumStore::StoredRow *>(buf);
          stored_rows[i] = srow;
          srow->cnt_ = col_cnt;
          srow->row_size_ = base_row_size;
          rows += 1;
          advance(size_array[i]);
        } else {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t col_idx = 0; col_idx < col_cnt; col_idx++) {
          if (OB_ISNULL(exprs.at(col_idx))) {
            for (int64_t i = idx; i < idx + rows; i++) {
              stored_rows[i]->cells()[col_idx].set_null();
            }
            continue;
          }
          ObObjType meta_type = exprs.at(col_idx)->datum_meta_.type_;
          const ObObjDatumMapType datum_map_type = ObDatum::get_obj_datum_map_type(meta_type);
          switch (datum_map_type) {
          case OBJ_DATUM_NUMBER:
            assign_datums<AssignNumberDatumValue>(datums, selector, idx, idx + rows, stored_rows, col_idx);
            break;
          case OBJ_DATUM_DECIMALINT:
            switch (get_decimalint_type(exprs.at(col_idx)->datum_meta_.precision_)) {
            case DECIMAL_INT_32:
              assign_datums<AssignFixedLenDatumValue<4>>(datums, selector, idx, idx + rows, stored_rows, col_idx);
              break;
            case DECIMAL_INT_64:
              assign_datums<AssignFixedLenDatumValue<8>>(datums, selector, idx, idx + rows, stored_rows, col_idx);
              break;
            case DECIMAL_INT_128:
              assign_datums<AssignFixedLenDatumValue<16>>(datums, selector, idx, idx + rows, stored_rows, col_idx);
              break;
            case DECIMAL_INT_256:
              assign_datums<AssignFixedLenDatumValue<32>>(datums, selector, idx, idx + rows, stored_rows, col_idx);
              break;
            case DECIMAL_INT_512:
              assign_datums<AssignFixedLenDatumValue<64>>(datums, selector, idx, idx + rows, stored_rows, col_idx);
              break;
            default:
              assign_datums<AssignDefaultDatumValue>(datums, selector, idx, idx + rows, stored_rows, col_idx);
              break;
            }
            break;
          case OBJ_DATUM_8BYTE_DATA:
            assign_datums<AssignFixedLenDatumValue<8>>(datums, selector, idx, idx + rows, stored_rows, col_idx);
            break;
          case OBJ_DATUM_4BYTE_DATA:
            assign_datums<AssignFixedLenDatumValue<4>>(datums, selector, idx, idx + rows, stored_rows, col_idx);
            break;
          case OBJ_DATUM_1BYTE_DATA:
            assign_datums<AssignFixedLenDatumValue<1>>(datums, selector, idx, idx + rows, stored_rows, col_idx);
            break;
          default:
            assign_datums<AssignDefaultDatumValue>(datums, selector, idx, idx + rows, stored_rows, col_idx);
            break;
          }
        }
      }

      idx += rows;
    }
  }

  return ret;
}

int ObDefaultBlockWriter::inner_add_row(const blocksstable::ObStorageDatum *storage_datums, const ObStorageColumnGroupSchema &cg_schema,
                                        const int64_t extra_size, ObChunkDatumStore::StoredRow **dst_sr)
{
  int ret = OB_SUCCESS;
  int64_t head_size = sizeof(ObChunkDatumStore::StoredRow);
  int64_t datum_size = sizeof(ObDatum) * cg_schema.column_cnt_;
  int64_t row_size = head_size + datum_size + extra_size;
  ObChunkDatumStore::StoredRow *sr = static_cast<ObChunkDatumStore::StoredRow *>((void*)get_cur_buf());
  if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get buffer", K(ret));
  } else {
    sr->cnt_ = cg_schema.column_cnt_;
    for (int64_t i = 0; i < cg_schema.column_cnt_; ++i) {
      int64_t column_idx = cg_schema.column_idxs_ ? cg_schema.column_idxs_[i] : i;
      const ObDatum *tmp_datum = static_cast<const ObDatum *>(&storage_datums[column_idx]);
      MEMCPY(sr->payload_ + i * sizeof(ObDatum), tmp_datum, sizeof(ObDatum));
    }
    char* data_start = sr->payload_ + datum_size + extra_size;
    int64_t pos = 0;
    for (int64_t i = 0; i < cg_schema.column_cnt_; ++i) {
      int64_t column_idx = cg_schema.column_idxs_ ? cg_schema.column_idxs_[i] : i;
      MEMCPY(data_start + pos, storage_datums[column_idx].ptr_, storage_datums[column_idx].len_);
      sr->cells()[i].ptr_ = data_start + pos;
      pos += storage_datums[column_idx].len_;
      row_size += storage_datums[column_idx].len_;
    }
    sr->row_size_ = row_size;
    if (OB_FAIL(advance(row_size))) {
      LOG_WARN("fill buffer head failed", K(ret), K(row_size));
    } else if (OB_NOT_NULL(dst_sr)) {
      *dst_sr = sr;
    }
  }
  return ret;
}

// before call this function -- we need to ensure the size if enough.
int ObDefaultBlockWriter::inner_add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, ObChunkDatumStore::StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow *sr = static_cast<ObChunkDatumStore::StoredRow *>((void*)get_cur_buf());
  if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get buffer", K(ret));
  } else {
    int64_t pos = sizeof(*sr) + sizeof(ObDatum) * exprs.count();
    if (pos > get_remain()) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("wirte buffer is not enough", K(ret));
    } else {
      sr->cnt_ = exprs.count();
      ObDatum *datums = sr->cells();
      for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
        ObExpr *expr = exprs.at(i);
        ObDatum *in_datum = NULL;
        if (OB_UNLIKELY(NULL == expr)) {
          // Set datum to NULL for NULL expr
          datums[i].set_null();
        } else if (OB_FAIL(expr->eval(ctx, in_datum))) {
          LOG_WARN("expression evaluate failed", K(ret));
        } else {
          datums[i].deep_copy(*in_datum, get_cur_buf(), get_remain(), pos);
        }
      }
      if (OB_SUCC(ret)) {
        sr->row_size_ = static_cast<int32_t>(pos);
        if (OB_FAIL(advance(sr->row_size_))) {
          LOG_WARN("fail to advance buf", K(ret));
        } else if (OB_NOT_NULL(stored_row)) {
          *stored_row = sr;
        }
      }
    }
  }
  return ret;
}

int ObDefaultBlockWriter::get_row_stored_size(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, uint64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  ObExpr *expr = nullptr;
  common::ObDatum *datum = nullptr;
  size = sizeof(ObDatum) * exprs.count() + sizeof(ObChunkDatumStore::StoredRow);
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
    } else if (OB_FAIL(expr->eval(ctx, datum))) {
      SQL_ENG_LOG(WARN, "failed to eval expr datum", KPC(expr), K(ret));
    } else {
      size += datum->len_;
    }
  }
  return ret;
}

int ObDefaultBlockWriter::close()
{
  int ret = OB_SUCCESS;
  inited_ = false;
  return ret;
}

int ObDefaultBlockWriter::ensure_write(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  uint64_t row_size;
  if (OB_FAIL(get_row_stored_size(exprs, ctx, row_size))) {
    LOG_WARN("fail to get row_size", K(exprs), K(ret));
  } else if (OB_FAIL(ensure_write(row_size))) {
    LOG_WARN("fail to call inner ensure write", K(ret));
  }
  return ret;
}

int ObDefaultBlockWriter::ensure_write(const ObChunkDatumStore::StoredRow &sr)
{
  int ret = OB_SUCCESS;
  uint64_t row_size;
  if (OB_FAIL(ensure_write(sr.row_size_))) {
    LOG_WARN("fail to call inner ensure write", K(ret));
  }
  return ret;
}

int ObDefaultBlockWriter::block_unswizzling(ObTempBlockStore::Block *blk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to unswizzling block", K(ret));
  } else {
    int64_t cur_row = 0;
    int64_t cur_pos = 0;
    const int64_t buf_size = blk->raw_size_ - sizeof(ObTempBlockStore::Block);
    while (cur_row < blk->cnt_ && cur_pos < buf_size) {
      ObChunkDatumStore::StoredRow *sr = reinterpret_cast<ObChunkDatumStore::StoredRow*>(blk->payload_ + cur_pos);
      sr->unswizzling();
      cur_pos += sr->row_size_;
      cur_row++;
    }
  }

  return ret;
}

int ObDefaultBlockWriter::prepare_blk_for_write(ObTempBlockStore::Block *blk)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(block_unswizzling(blk))) {
    LOG_WARN("fail to unswizzling block", K(ret));
  }
  return ret;
}

int ObDefaultBlockWriter::ensure_write(const blocksstable::ObStorageDatum *storage_datums,
                                       const ObStorageColumnGroupSchema &cg_schema,
                                       const int64_t extra_size)
{
  int ret = OB_SUCCESS;
  uint64_t row_size;
  if (OB_FAIL(get_row_stored_size(storage_datums, cg_schema, extra_size, row_size))) {
    LOG_WARN("fail to get row_size", K(cg_schema), K(extra_size), K(row_size), K(ret));
  } else if (OB_FAIL(ensure_write(row_size))) {
    LOG_WARN("fail to call inner ensure write", K(ret));
  }

  return ret;
}

int ObDefaultBlockWriter::get_row_stored_size(const blocksstable::ObStorageDatum *storage_datums,
                                              const ObStorageColumnGroupSchema &cg_schema,
                                              const int64_t extra_size, uint64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t head_size = sizeof(ObChunkDatumStore::StoredRow);
  int64_t datum_size = sizeof(ObDatum) * cg_schema.column_cnt_;
  int64_t data_size = 0;
  for (int64_t i = 0; i < cg_schema.column_cnt_; ++i) {
    int64_t column_idx = cg_schema.column_idxs_ ? cg_schema.column_idxs_[i] : i;
    data_size += storage_datums[column_idx].len_;
  }
  size = head_size + datum_size + extra_size + data_size;
  return ret;
}

int ObDefaultBlockWriter::ensure_write(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (is_overflow(size)) {
    // need to alloc a new block to write.
    int64_t new_blk_size = size < DEFAULT_BUF_SIZE ? DEFAULT_BUF_SIZE : size;
    ObTempBlockStore::Block *tmp_blk = nullptr;
    if (OB_FAIL(store_->new_block(new_blk_size, tmp_blk, true))) {
      LOG_WARN("fail to alloc block", K(ret));
    } else if (OB_ISNULL(tmp_blk)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc block", K(ret));
    } else {
      cur_blk_ = tmp_blk;
    }
  }

  return ret;
}

}
}
