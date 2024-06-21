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

template<bool RA>
class ObTempRowStoreBase : public ObTempBlockStore
{
  OB_UNIS_VERSION_V(1);
public:
  class ReaderBase;
  class Iterator;
  class RAReader;
  typedef uint32_t row_idx_t;
  const static int64_t ROW_INDEX_SIZE = sizeof(row_idx_t);
  /*
   * RowBlock provides functions for writing and reading
   *
   * RA = false means supports sequential access, the payload layout when RA = false is as follows:
   *
   *    +-----------------------------------------------------------------------------------------+
   *    | compact_row1(variable_length) | compact_row2 |...|
   *    +-----------------------------------------------------------------------------------------+
   *
   * Compact rows are variable-length and are read or written from head to tail.
   *
   *
   * RA = true means suppport Random Access, the payload layout when RA = true is as follows:
   *    get_buffer() ____________________________________________________________________
   *                                                                                     |
   *                                                                                     V
   *    +-----------------------------------------------------------------------------------------+
   *    | compact_row1(variable_length) | compact_row2 |...|           |...| pos2 | pos1 |  buf   |
   *    +-----------------------------------------------------------------------------------------+
   *    ^                               ^                                      |      |
   *    |                               |______________________________________|      |
   *    |_____________________________________________________________________________|
   *
   * Compact rows are variable-length and are written from head to tail. The index area at
   * the tial that records the position of each compact row is written from tail to head.
   *
   * The buffer is at the end of payload, the begin of buffer is the end of indexes.
   * During random reading, use get_buffer() to find the end of indexes, then get the position by
   * index, and finally obtain the compact row based on the position.
   */
  struct RowBlock : public Block
  {
    int add_row(ShrinkBuffer &buf,
                const common::ObIArray<ObExpr*> &exprs,
                const RowMeta &row_meta,
                ObEvalCtx &ctx,
                ObCompactRow *&stored_row);
    int add_row(ShrinkBuffer &buf, const ObCompactRow *src_row, ObCompactRow *&stored_row);
    int add_batch(ShrinkBuffer &buf,
                  const IVectorPtrs &vectors,
                  const RowMeta &row_meta,
                  const uint16_t selector[],
                  const int64_t size,
                  const uint32_t row_size_arr[],
                  int64_t batch_mem_size,
                  ObCompactRow **stored_rows);
    int get_next_batch(ObTempRowStoreBase::ReaderBase &iter,
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
    int get_row(const int64_t row_id, const ObCompactRow *&sr) const;

  private:
    static int vector_to_nulls(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                               const uint16_t selector[], const int64_t size,
                               const int64_t col_idx);
    inline int post_add_row(ShrinkBuffer &buf, const int32_t row_size)
    {
      int ret = OB_SUCCESS;
      if (!RA) {
        // do nothing
      } else if (OB_FAIL(buf.fill_tail(ROW_INDEX_SIZE))) {
        SQL_LOG(WARN, "fill buffer tail failed", K(ret), K(buf), LITERAL_K(ROW_INDEX_SIZE));
      } else {
        *reinterpret_cast<row_idx_t *>(buf.tail()) = static_cast<row_idx_t>(buf.head() - payload_);
      }
      buf.fast_advance(row_size);
      return ret;
    }
    int32_t get_row_location(const int64_t row_id) const;
  };

  struct DtlRowBlock : public RowBlock {
    ObTempBlockStore::ShrinkBuffer *get_buffer() {
      return static_cast<ObTempBlockStore::ShrinkBuffer *>(
        static_cast<void *>(reinterpret_cast<char *>(this) + this->buf_off_));
    }
    static int calc_rows_size(const IVectorPtrs &vectors,
                              const RowMeta &row_meta,
                              const ObBatchRows &brs,
                              uint32_t row_size_arr[]);
  };

  const static int64_t BLOCK_SIZE = (64L << 10);

  class ReaderBase : public ObTempBlockStore::BlockReader
  {
  public:
    friend struct RowBlock;
    friend class ObTempRowStoreBase;
    ReaderBase() : ObTempBlockStore::BlockReader(), row_store_(NULL), cur_blk_(NULL),
                 cur_blk_id_(0), row_idx_(0), read_pos_(0) {}
    virtual ~ReaderBase() {}

    int init(ObTempRowStoreBase *store);
    bool is_valid() { return nullptr != row_store_; }
    inline int64_t get_row_cnt() const { return row_store_->get_row_cnt(); }
    int get_next_batch(const ObExprPtrIArray &exprs,
                       ObEvalCtx &ctx,
                       const int64_t max_rows,
                       int64_t &read_rows,
                       const ObCompactRow **stored_rows = NULL);
    int get_next_batch(const int64_t max_rows,
                       int64_t &read_rows,
                       const ObCompactRow **stored_rows);
    void reset()
    {
      cur_blk_id_ = 0;
      cur_blk_ = NULL;
      row_idx_ = 0;
      read_pos_ = 0;
      ObTempBlockStore::BlockReader::reset();
    }
    inline sql::RowMeta &get_row_meta() const { return row_store_->row_meta_; }

  private:
    int get_next_batch(const IVectorPtrs &vectors,
                       const int64_t max_rows,
                       int64_t &read_rows,
                       const ObCompactRow **stored_rows = NULL);
    int next_block();

  private:
    ObTempRowStoreBase<RA> *row_store_;
    const RowBlock *cur_blk_;
    int64_t cur_blk_id_; // block id(row_id) for iterator, from 0 to row_cnt_
    int32_t row_idx_; // current row index in reader block
    int32_t read_pos_; // current memory read position in reader block
  };

  class Iterator : public ReaderBase
  {
  public:
    Iterator() {}
    virtual ~Iterator() {}

    inline bool has_next() const { return this->cur_blk_id_ < this->get_row_cnt(); }
    static int attach_rows(const ObExprPtrIArray &exprs,
                           ObEvalCtx &ctx,
                           const RowMeta &row_meta,
                           const ObCompactRow **srows,
                           const int64_t read_rows);
  };


  class RAReader : public ReaderBase {
    friend class ObTempRowStoreBase;
  public:
    explicit RAReader() {}
    virtual ~RAReader() { this->reset(); }
    int get_row(const int64_t row_id, const ObCompactRow *&sr);

    int get_batch_rows(const ObExprPtrIArray &exprs,
                       ObEvalCtx &ctx,
                       const int64_t start_idx,
                       const int64_t end_idx,
                       int64_t &read_rows,
                       const ObCompactRow **stored_rows);
    int get_batch_rows(const int64_t start_idx, const int64_t end_idx, int64_t &read_rows,
                       const ObCompactRow **stored_rows); // TODO:

  private:
    DISALLOW_COPY_AND_ASSIGN(RAReader);
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
  explicit ObTempRowStoreBase(common::ObIAllocator *alloc = NULL);

  virtual ~ObTempRowStoreBase();
  void destroy();
  void reset();

  int init(const ObExprPtrIArray &exprs,
           const int64_t max_batch_size,
           const lib::ObMemAttr &mem_attr,
           const int64_t mem_limit,
           bool enable_dump,
           uint32_t row_extra_size,
           const common::ObCompressorType compressor_type,
           const bool reorder_fixed_expr = true,
           const bool enable_trunc = false);

  int init(const RowMeta &row_meta,
           const int64_t max_batch_size,
           const lib::ObMemAttr &mem_attr,
           const int64_t mem_limit,
           bool enable_dump,
           const common::ObCompressorType compressor_type,
           const bool enable_trunc = false);

  int init_batch_ctx();

  int begin(Iterator &it)
  {
    return it.init(this);
  }

  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBatchRows &brs, int64_t &stored_rows_count,
                ObCompactRow **stored_rows = NULL,
                const int64_t start_pos = 0);
  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx, const EvalBound &bound,
                const ObBitVector &skip, int64_t &stored_rows_count,
                ObCompactRow **stored_rows); // TODO
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
    if (NULL == blk_ || mem_size > blk_buf_.remain()) {
      if (OB_FAIL(new_block(mem_size))) {
        SQL_ENG_LOG(WARN, "fail to new block", K(ret), K(mem_size));
      }
    }
    return ret;
  }

  virtual int prepare_blk_for_switch(Block *blk) override
  {
    int ret = OB_SUCCESS;
    if (!RA) {
      // do nothing, non ra store dont need compact
    } else if (OB_FAIL(blk_buf_.compact())) {
      SQL_LOG(WARN, "fail to compact block", K(ret));
    }
    return ret;
  }

  const RowMeta &get_row_meta() const { return row_meta_; }
  inline int64_t get_max_batch_size() const { return max_batch_size_; }
  inline int64_t get_row_cnt() const { return block_id_cnt_; }
  inline int64_t get_row_cnt_on_disk() const { return dumped_block_id_cnt_; }
  inline int64_t get_row_cnt_in_memory() const { return get_row_cnt() - get_row_cnt_on_disk(); }
  const lib::ObMemAttr &get_mem_attr() const { return mem_attr_; }

  INHERIT_TO_STRING_KV("ObTempBlockStore", ObTempBlockStore,
                       K_(mem_attr),
                       K_(col_cnt),
                       K_(row_meta),
                       K_(max_batch_size));

private:
  inline RowBlock *cur_blk() {
    return reinterpret_cast<RowBlock *>(blk_);
  }

private:
  lib::ObMemAttr mem_attr_;
  int64_t col_cnt_;
  BatchCtx *batch_ctx_;
  RowMeta row_meta_;
  int64_t max_batch_size_;
};

using ObRATempRowStore = ObTempRowStoreBase<true>;
using ObTempRowStore = ObTempRowStoreBase<false>;

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_TEMP_ROW_STORE_H_
