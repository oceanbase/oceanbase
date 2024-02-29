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

#ifndef OCEANBASE_BASIC_OB_TEMP_ROW_STORE_H_
#define OCEANBASE_BASIC_OB_TEMP_ROW_STORE_H_

#include "share/ob_define.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "share/vector/ob_i_vector.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase
{
namespace sql
{

class ObTempRowStore : public ObTempBlockStore
{
  OB_UNIS_VERSION_V(1);
public:
  class Iterator;
  // RowBlock provides functions for writing and reading, and does not occupy memory
  struct RowBlock : public Block
  {
    int add_row(const common::ObIArray<ObExpr*> &exprs,
                const RowMeta &row_meta,
                ObEvalCtx &ctx,
                ObCompactRow *&stored_row);
    int add_row(const ObCompactRow *src_row, ObCompactRow *&stored_row);
    int add_batch(const IVectorPtrs &vectors,
                  const RowMeta &row_meta,
                  const uint16_t selector[],
                  const int64_t size,
                  const uint32_t row_size_arr[],
                  int64_t batch_mem_size,
                  ObCompactRow **stored_rows);
    int get_next_batch(ObTempRowStore::Iterator &iter,
                       const int64_t max_rows,
                       int64_t &read_rows,
                       const ObCompactRow **stored_rows) const;
    static int calc_rows_size(const IVectorPtrs &vectors,
                              const RowMeta &row_meta,
                              const uint16_t selector[],
                              const int64_t size,
                              uint32_t row_size_arr[],
                              const common::ObIArray<int64_t> *dup_length = nullptr);
    static int calc_row_size(const common::ObIArray<ObExpr*> &exprs,
                             const RowMeta &row_meta,
                             ObEvalCtx &ctx,
                             int64_t &size);
    int32_t rows() const { return cnt_; }
    int get_store_row(int64_t &cur_pos, const ObCompactRow *&sr);

  private:
    static int vector_to_nulls(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                               const uint16_t selector[], const int64_t size,
                               const int64_t col_idx);
  };

  const static int64_t BLOCK_SIZE = (64L << 10);
  class Iterator : public ObTempBlockStore::BlockReader
  {
  public:
    friend struct RowBlock;
    friend class ObTempRowStore;
    Iterator() : ObTempBlockStore::BlockReader(), row_store_(NULL), cur_blk_(NULL),
                 cur_blk_id_(0), row_idx_(0), read_pos_(0) {}
    virtual ~Iterator() {}

    int init(ObTempRowStore *store);
    bool is_valid() { return nullptr != row_store_; }
    inline bool has_next() const { return cur_blk_id_ < get_row_cnt(); }
    inline int64_t get_row_cnt() const { return row_store_->get_row_cnt(); }
    int get_next_batch(const ObExprPtrIArray &exprs,
                       ObEvalCtx &ctx,
                       const int64_t max_rows,
                       int64_t &read_rows,
                       const ObCompactRow **stored_rows = NULL);
    int get_next_batch(const int64_t max_rows,
                       int64_t &read_rows,
                       const ObCompactRow **stored_rows);
    static int attach_rows(const ObExprPtrIArray &exprs,
                           ObEvalCtx &ctx,
                           const RowMeta &row_meta,
                           const ObCompactRow **srows,
                           const int64_t read_rows);
    void reset()
    {
      cur_blk_id_ = 0;
      row_idx_ = 0;
      read_pos_ = 0;
      cur_blk_ = NULL;
      ObTempBlockStore::BlockReader::reset();
    }
    inline sql::RowMeta &get_row_meta() const { return row_store_->row_meta_; }

  private:
    int get_next_batch(const IVectorPtrs &vectors,
                       const int64_t max_rows,
                       int64_t &read_rows,
                       const ObCompactRow **stored_rows = NULL);
    static int attach_rows(const RowMeta &row_meta, const ObCompactRow **srows,
                           const int64_t read_rows, const IVectorPtrs &vectors);
    int next_block();

  private:
    ObTempRowStore *row_store_;
    const RowBlock *cur_blk_;
    int64_t cur_blk_id_; // block id(row_id) for iterator, from 0 to row_cnt_
    int32_t row_idx_; // current row index in reader block
    int32_t read_pos_; // current memory read position in reader block
  };

  struct BatchCtx
  {
    ~BatchCtx() {
      vectors_.reset();
      rows_ = nullptr;
      row_size_array_ = nullptr;
      selector_ = nullptr;
    }
    ObArray<ObIVector *> vectors_;
    ObCompactRow **rows_;
    uint32_t *row_size_array_;
    int64_t max_batch_size_;
    uint16_t *selector_;
  };

public:
  explicit ObTempRowStore(common::ObIAllocator *alloc = NULL);

  virtual ~ObTempRowStore();
  void destroy();
  void reset();

  int init(const ObExprPtrIArray &exprs,
           const int64_t max_batch_size,
           const lib::ObMemAttr &mem_attr,
           const int64_t mem_limit,
           bool enable_dump,
           uint32_t row_extra_size,
           const bool reorder_fixed_expr = true,
           const bool enable_trunc = false,
           const common::ObCompressorType compressor_type = NONE_COMPRESSOR);

  int init(const RowMeta &row_meta,
           const int64_t max_batch_size,
           const lib::ObMemAttr &mem_attr,
           const int64_t mem_limit,
           bool enable_dump,
           const bool enable_trunc = false,
           const common::ObCompressorType compressor_type = NONE_COMPRESSOR);

  int init_batch_ctx();

  int begin(Iterator &it)
  {
    return it.init(this);
  }

  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBatchRows &brs, int64_t &stored_rows_count,
                ObCompactRow **stored_rows = NULL,
                const int64_t start_pos = 0);
  int add_row(const common::ObIArray<ObExpr*> &exprs,
              ObEvalCtx &ctx,
              ObCompactRow *&stored_row);

  int add_row(const ObCompactRow *src_row, ObCompactRow *&stored_row);
  int add_row(const common::ObIArray<ObExpr *> &exprs, const int64_t batch_idx,
              ObEvalCtx &ctx, ObCompactRow *&stored_row);

  // 1. calc need mem size for all rows
  // 2. alloc mem for all rows and init stored_rows
  // 3. set values by column from vectors
  int add_batch(const IVectorPtrs &vectors,
                const ObBitVector &skip, const int64_t batch_size,
                int64_t &stored_rows_count,
                ObCompactRow **stored_rows = NULL);

  int add_batch(const IVectorPtrs &vectors,
                const uint16_t selector[], const int64_t size,
                ObCompactRow **stored_rows = NULL,
                const common::ObIArray<int64_t> *dup_length = nullptr);

  // Try add a batch to row store if memory is enough to hold this batch
  int try_add_batch(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx *ctx,
                    const int64_t batch_size, const int64_t memory_limit,
                    bool &batch_added);

  int try_add_batch(const ObCompactRow **stored_rows,
                    const int64_t batch_size,
                    const int64_t memory_limit,
                    bool &batch_added);

  int calc_rows_size(const IVectorPtrs &vectors,
                     const uint16_t selector[],
                     const int64_t size,
                     uint32_t row_size_arr[],
                     const common::ObIArray<int64_t> *dup_length = nullptr);

  inline int ensure_write_blk(const int64_t mem_size)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == cur_blk_ || mem_size > cur_blk_->remain()) {
      if (OB_FAIL(new_block(mem_size))) {
        SQL_ENG_LOG(WARN, "fail to new block", K(ret), K(mem_size));
      } else {
        cur_blk_ = static_cast<RowBlock *>(blk_);
      }
    }
    return ret;
  }
  const RowMeta &get_row_meta() const { return row_meta_; }
  inline int64_t get_max_batch_size() const { return max_batch_size_; }
  inline int64_t get_row_cnt() const { return block_id_cnt_; }
  inline int64_t get_row_cnt_on_disk() const { return dumped_block_id_cnt_; }
  inline int64_t get_row_cnt_in_memory() const { return get_row_cnt() - get_row_cnt_on_disk(); }

  INHERIT_TO_STRING_KV("ObTempBlockStore", ObTempBlockStore,
                       K_(mem_attr),
                       K_(col_cnt),
                       K_(row_meta),
                       K_(max_batch_size));


private:
  lib::ObMemAttr mem_attr_;
  RowBlock *cur_blk_;
  int64_t col_cnt_;
  BatchCtx *batch_ctx_;
  RowMeta row_meta_;
  int64_t max_batch_size_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_TEMP_ROW_STORE_H_
