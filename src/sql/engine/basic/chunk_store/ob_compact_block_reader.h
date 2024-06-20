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

#ifndef OCEANBASE_BASIC_OB_COMPACT_BLOCK_READER_H_
#define OCEANBASE_BASIC_OB_COMPACT_BLOCK_READER_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "src/share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/chunk_store/ob_block_ireader.h"

namespace oceanbase
{
namespace sql
{

class ObTempBlockStore;
class ChunkChunkRowMeta;
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
class ObCompactBlockReader final : public ObBlockIReader
{
  static const int HEAD_SIZE = 5;
  static const int BASE_OFFSET_SIZE = 2;
  static const int EXTENDED_OFFSET_SIZE = 4;
  struct CurRowInfo final
  {
  public:
    CurRowInfo() : buf_(nullptr), row_size_(nullptr), var_column_cnt_(0), cur_var_offset_pos_(0), bitmap_size_(0),
                    bit_vec_(nullptr), data_offset_(0), fix_offset_(0), var_col_end_offset_(0) {}

    ~CurRowInfo() {reset();}
    int init(const ChunkRowMeta *row_meta, const uint8_t offset_width, const char *buf);

    void reset()
    {
      buf_ = nullptr;
      row_size_ = nullptr;
      cur_var_offset_pos_ = 0;
      cur_data_ptr_ = 0;
      var_column_cnt_ = 0;
      bitmap_size_ = 0;
      bit_vec_ = nullptr;
      data_offset_ = 0;
      fix_offset_ = 0;
      var_col_end_offset_ = 0;
    }
    TO_STRING_KV(K_(cur_var_offset_pos), K_(var_column_cnt), K_(bitmap_size),
                 K_(cur_data_ptr), K_(data_offset), K_(fix_offset));

  public:
    const char *buf_;
    const uint32_t *row_size_;
    int64_t cur_data_ptr_;
    int64_t var_column_cnt_;
    int64_t cur_var_offset_pos_; // the i-th in the var_array
    int64_t bitmap_size_;

    // Use BitVector to set the result of filter here, because the memory of ObBitMap is not continuous
    // null_bitmap
    const sql::ObBitVector *bit_vec_ = nullptr;
    int64_t data_offset_; // the start of fixed data buffer.
    int64_t fix_offset_;
    int64_t var_col_end_offset_;
  };


public:
  ObCompactBlockReader(ObTempBlockStore *store = nullptr) : ObBlockIReader(store), row_meta_(nullptr),
                                                  sr_buffer_(nullptr), sr_size_(0), cur_row_offset_width_(0),
                                                  cur_pos_in_blk_(0), cur_row_in_blk_(0) {};
  ObCompactBlockReader(ObTempBlockStore *store, const ChunkRowMeta *row_meta) : ObBlockIReader(store), row_meta_(row_meta),
                                                      sr_buffer_(nullptr), sr_size_(0), cur_row_offset_width_(0),
                                                      cur_pos_in_blk_(0), cur_row_in_blk_(0) {};
  virtual ~ObCompactBlockReader() { reset(); };

  void reset()
  {
    cur_pos_in_blk_ = 0;
    if (OB_NOT_NULL(sr_buffer_)) {
      store_->free(sr_buffer_, sr_size_);
      sr_buffer_ = nullptr;
    }
    sr_size_ = 0;
    cur_row_in_blk_ = 0;
    cur_row_offset_width_ = 0;
    row_meta_ = nullptr;
    cur_blk_ = nullptr;
  }

  void reuse()
  {
    cur_pos_in_blk_ = 0;
    if (OB_NOT_NULL(sr_buffer_)) {
      MEMSET(sr_buffer_, 0, sr_size_);
    }
    cur_row_in_blk_ = 0;
    cur_row_offset_width_ = 0;
    row_info_.reset();
    cur_blk_ = nullptr;
  }

  int get_row(const ObChunkDatumStore::StoredRow *&sr) override;

  void set_meta(const ChunkRowMeta *row_meta) { row_meta_ = row_meta; };
  const ChunkRowMeta *get_meta() { return row_meta_; }
  virtual int prepare_blk_for_read(ObTempBlockStore::Block *blk) final override { return OB_SUCCESS; }

public:
  static int calc_stored_row_size(const char *compact_row, const ChunkRowMeta *row_meta, int64_t &size);

private:
  inline bool blk_has_next_row() { return cur_blk_ != NULL && cur_blk_->cnt_ > cur_row_in_blk_; }
  int get_stored_row_size(int64_t &size);
  template <typename T>
  int get_stored_row(ObChunkDatumStore::StoredRow *&sr);

  int alloc_stored_row(ObChunkDatumStore::StoredRow *&sr, const int64_t size);

private:
  const ChunkRowMeta *row_meta_;
  char *sr_buffer_;
  int64_t sr_size_;
  int64_t cur_row_offset_width_;
  int64_t cur_pos_in_blk_;
  int64_t cur_row_in_blk_;
  CurRowInfo row_info_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_COMPACT_BLOCK_READER_H_
