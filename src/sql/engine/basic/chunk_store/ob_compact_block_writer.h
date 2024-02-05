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

#ifndef OCEANBASE_BASIC_OB_COMPACT_BLOCK_WRITER_H_
#define OCEANBASE_BASIC_OB_COMPACT_BLOCK_WRITER_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "src/share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/chunk_store/ob_chunk_block.h"
#include "sql/engine/basic/chunk_store/ob_block_iwriter.h"
#include "sql/ob_sql_define.h"
#include "sql/engine/basic/chunk_store/ob_compact_block_reader.h"

namespace oceanbase
{
namespace sql
{


/*
 * compact row format
 * +----------+--------------+-------------+-------------------+------------+---------+
 * | row_size | offset_width | null_bitmap | var_column_offset | fixed_data | var_data|
 * +----------+--------------+-------------+-------------------+------------+---------+
 * offset_width:  the width of offset in var_column_offset (e.g., 2 bytes /4 bytes),
 *                4 bytes is for long row.
 * null_bitmap:   mark wether the i-th datum is null. equal to datum->desc.null_
 *
 * to get i-th datum(fixed data):   1. get offset by row_meta.column_offset[i].
 *                                  2. use the offset to get fixed data ( ObDatum *datum = fixed_data + offset)
 * to get i-th datum(var_data):     1. find the var_data is the j-th var_data.
 *                                  2. get offset in var_column_offset. (T *offset = var_column_offset + j * offset_width)
 *                                     T is int16_ or int32_t.
 *                                  3. use the offset to get datum from var_data. (datum = var_data + offset)
 */
class ObTempBlockStore;
class ChunkRowMeta;

class ObCompactBlockWriter final : public ObBlockIWriter
{
  static const int HEAD_SIZE = 5;
  static const int BASE_OFFSET_SIZE = 2;
  static const int EXTENDED_OFFSET_SIZE = 4;

  struct CurRowInfo final
  {
  public:
    CurRowInfo() : buf_(nullptr), var_column_cnt_(0), cur_var_offset_pos_(0), bitmap_size_(0),
                   bit_vec_(nullptr), data_offset_(0), var_offset_(0) {}

    ~CurRowInfo() { reset(); }
    int init(const ChunkRowMeta *row_meta, const uint8_t offset_width, char *buf);
    void reset();
    TO_STRING_KV(K_(cur_var_offset_pos), K_(var_column_cnt), K_(bitmap_size),
                 K_(data_offset), K_(var_offset));
  public:
    char *buf_;
    int64_t var_column_cnt_;
    int64_t cur_var_offset_pos_; // the i-th in the var_array
    int64_t bitmap_size_;

    // Use BitVector to set the result of filter here, because the memory of ObBitMap is not continuous
    // null_bitmap
    sql::ObBitVector *bit_vec_ = nullptr;
    int64_t data_offset_; // the start of fixed data buffer.
    int64_t var_offset_;
  };

public:
  ObCompactBlockWriter(ObTempBlockStore *store = nullptr) : ObBlockIWriter(store), row_meta_(nullptr), cur_row_offset_width_(0),
                                                  cur_row_size_(0), row_info_(), last_stored_row_(nullptr), last_sr_size_(0) {};

  ObCompactBlockWriter(ObTempBlockStore *store, const ChunkRowMeta *row_meta) : ObBlockIWriter(store), row_meta_(row_meta),
                                                  cur_row_offset_width_(0), cur_row_size_(0), row_info_(), last_stored_row_(nullptr),
                                                  last_sr_size_(0) {};
  virtual ~ObCompactBlockWriter() { reset(); };

  void reset()
  {
    cur_row_offset_width_  = 0;
    cur_row_size_ = 0;
    row_meta_ = nullptr;
    row_info_.reset();
    if (OB_NOT_NULL(last_stored_row_)) {
      store_->free(last_stored_row_, last_sr_size_);
    }
    last_sr_size_ = 0;
  }
  virtual int add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, ObChunkDatumStore::StoredRow **stored_row = nullptr) override;
  // if full, construct the block and use the block's block_mgr. return block
  virtual int add_row(const ObChunkDatumStore::StoredRow &src_sr, ObChunkDatumStore::StoredRow **dst_sr = nullptr) override;
  //virtual int try_add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx);
  virtual int add_row(const blocksstable::ObStorageDatum *storage_datums, const ObStorageColumnGroupSchema &cg_schema,
              const int64_t extra_size, ObChunkDatumStore::StoredRow **stored_row) override;

  virtual int add_batch(const common::ObDatum **datums, const common::ObIArray<ObExpr *> &exprs,
                        const uint16_t selector[], const int64_t size,
                        ObChunkDatumStore::StoredRow **stored_rows, BatchCtx *batch_ctx) override { return OB_NOT_IMPLEMENT; }

  void set_meta(const ChunkRowMeta *row_meta) override { row_meta_ = row_meta; };
  const ChunkRowMeta *get_meta() { return row_meta_; }
  int close() override;
  virtual int prepare_blk_for_write(ObTempBlockStore::Block *blk) final override { return OB_SUCCESS; }
  int get_last_stored_row(const ObChunkDatumStore::StoredRow *&sr);

protected:
  /*
   * before add_row we should call ensure_write
   * 1. if the write buffer could hold the next row:
   * 2. if the write buffer couldn't hold next row:
   *    2.1 if the write buffer isn't empty, construct_block use the write buffer. and reset writer buffer.
   *    2.2 if the write buffer is emptry (large row), construct block use the large row.
   */
  int ensure_write(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx);
  int ensure_write(const ObChunkDatumStore::StoredRow &stored_row);
  int ensure_write(const blocksstable::ObStorageDatum *storage_datums, const ObStorageColumnGroupSchema &cg_schema,
                  const int64_t extra_size);
  int ensure_write(const int64_t size);

  // get the stored size in writer buffer for a row.
  int get_row_stored_size(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, uint64_t &size);
  int get_row_stored_size(const ObChunkDatumStore::StoredRow &sr, uint64_t &size);
  int get_row_stored_size(const blocksstable::ObStorageDatum *storage_datums, const ObStorageColumnGroupSchema &cg_schema,
                          const int64_t extra_size, uint64_t &size);

private:
  template <typename T>
  int inner_process_datum(const ObDatum &src_datum, const int64_t cur_pos, const ChunkRowMeta &row_meta,
                          CurRowInfo &row_info);
  template <typename T>
  int inner_build_from_stored_row(const ObChunkDatumStore::StoredRow &sr);
  inline uint8_t get_offset_width(const int64_t data_size) {
    return (data_size < (1 << 16)) ? BASE_OFFSET_SIZE : EXTENDED_OFFSET_SIZE;
  }
  template <typename T>
  int inner_add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx);
  template <typename T>
  int inner_add_row(const blocksstable::ObStorageDatum *storage_datums, const ObStorageColumnGroupSchema &cg_schema,
                    const int64_t extra_size, ObChunkDatumStore::StoredRow **stored_row);

  inline int ensure_init()
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(row_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "the row meta is null", K(ret));
    } else if (!inited_) {
      inited_ = true;
    }
    return ret;
  }

  int inner_get_stored_row_size(const char *compact_row, int64_t &size);
  template <typename T>
  int convert_to_stored_row(const char *compact_row, ObChunkDatumStore::StoredRow *sr);

private:
  const ChunkRowMeta *row_meta_;
  uint8_t cur_row_offset_width_;
  int32_t cur_row_size_;
  CurRowInfo row_info_;
  ObChunkDatumStore::StoredRow *last_stored_row_;
  int64_t last_sr_size_;
};

} // end namespace sql
} // end namespace oceanbase
#endif // OCEANBASE_BASIC_OB_COMPACT_BLOCK_WRITER_H_
