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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/optimizer/ob_join_order.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObDASIndexMergeCtDef;
struct ObDASIndexMergeRtDef;
class ObDASScanIter;

struct ObDASIndexMergeIterParam : public ObDASIterParam
{
public:
  ObDASIndexMergeIterParam()
    : ObDASIterParam(DAS_ITER_INDEX_MERGE),
      merge_type_(INDEX_MERGE_INVALID),
      rowkey_exprs_(nullptr),
      ctdef_(nullptr),
      rtdef_(nullptr),
      child_iters_(nullptr),
      child_scan_rtdefs_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      is_reverse_(false),
      main_scan_iter_(nullptr)
  {}

  ObIndexMergeType merge_type_;
  const ExprFixedArray *rowkey_exprs_;
  const ObDASIndexMergeCtDef *ctdef_;
  ObDASIndexMergeRtDef *rtdef_;
  const common::ObIArray<ObDASIter*> *child_iters_;
  const common::ObIArray<ObDASScanRtDef*> *child_scan_rtdefs_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  bool is_reverse_;
  ObDASScanIter *main_scan_iter_;

  virtual bool is_valid() const
  {
    return rowkey_exprs_ != nullptr &&
           ctdef_ != nullptr &&
           rtdef_ != nullptr &&
           child_iters_ != nullptr &&
           child_scan_rtdefs_ != nullptr &&
           child_iters_->count() == child_scan_rtdefs_->count() &&
           merge_type_ != INDEX_MERGE_INVALID &&
           ObDASIterParam::is_valid();
  }
};

class ObDASIndexMergeIter : public ObDASIter
{

public:
  /* used to store and merge child rows */
  struct IndexMergeRowStore
  {
  public:
    typedef ObChunkDatumStore::StoredRow StoredRow;

    IndexMergeRowStore()
      : exprs_(nullptr),
        rowkey_is_uint64_(false),
        is_reverse_(false),
        eval_ctx_(nullptr),
        capacity_(0),
        count_(0),
        idx_(0),
        mock_skip_(nullptr),
        row_store_(nullptr),
        stored_rows_(nullptr),
        rowids_(),
        iter_end_(false),
        drained_(false),
        relevances_(nullptr)
    {}
    int init(common::ObIAllocator &allocator,
             const common::ObIArray<ObExpr*> *exprs,
             ObEvalCtx *eval_ctx,
             int64_t max_size,
             bool is_reverse,
             bool rowkey_is_uint64,
             ObBitVector *mock_skip);
    void reuse();
    void reset();
    int save(bool is_vectorized, int64_t size, ObExpr *relevance_expr = nullptr);
    int to_expr(int64_t size);
    // no data or data was used up
    OB_INLINE bool is_empty() const { return count_ == 0 || idx_ >= count_; }
    OB_INLINE int64_t count() const { return count_; }

    OB_INLINE const StoredRow *first_row() { return stored_rows_[idx_]; }
    OB_INLINE const StoredRow *last_row() { return stored_rows_[count_ - 1]; }
    OB_INLINE const StoredRow *at(int64_t idx) { return stored_rows_[idx]; }
    OB_INLINE void incre_idx(int64_t step = 1) { idx_ += step; }
    int lower_bound(uint64_t target, uint64_t &lower_bound);

    int save_distance(bool is_vectorized, int64_t size, ObExpr *relevance_expr);
    double get_relevance() const { return relevances_[idx_]; }

    TO_STRING_KV(K_(exprs),
                 K_(is_reverse),
                 K_(rowkey_is_uint64),
                 K_(capacity),
                 K_(count),
                 K_(idx),
                 K_(iter_end),
                 K_(drained),
                 K_(rowids));

  public:
    const common::ObIArray<ObExpr*> *exprs_;
    bool rowkey_is_uint64_;
    bool is_reverse_;
    ObEvalCtx *eval_ctx_;
    int64_t capacity_;
    int64_t count_;
    // indicate current position
    int64_t idx_;
    ObBitVector *mock_skip_;
    ObChunkDatumStore *row_store_;
    ObChunkDatumStore::StoredRow **stored_rows_;
    ObFixedArray<uint64_t, common::ObIAllocator> rowids_;
    // scan iter end, maybe still have rows in store_rows_
    bool iter_end_;
    // there is no data at all and no need to revisit
    bool drained_;
    double *relevances_;
  };

  /* shared exprs may cause the results on the frame to be overwritten by get_next_rows() of child iters,
   * thus we use a result buffer to temporarily store a batch of result rows and @to_expr() at the end of
   * each iteration.
   */
  struct MergeResultBuffer
  {
  public:
    MergeResultBuffer()
      : exprs_(nullptr),
        eval_ctx_(nullptr),
        max_size_(1),
        row_cnt_(0),
        result_store_("IndexMergeRes"),
        result_store_iter_()
    {}
    int init( int64_t max_size, ObEvalCtx *eval_ctx, const common::ObIArray<ObExpr*> *exprs, common::ObIAllocator &alloc);
    int reuse();
    void reset();
    int add_rows(int64_t size);
    int add_rows_by_exprs(int64_t size, const common::ObIArray<ObExpr*> *exprs);
    int to_expr(int64_t size);
    inline int64_t get_row_cnt() const { return row_cnt_; }

  public:
    const common::ObIArray<ObExpr*> *exprs_;
    ObEvalCtx *eval_ctx_;
    int64_t max_size_;
    int64_t row_cnt_;
    ObChunkDatumStore result_store_;
    ObChunkDatumStore::Iterator result_store_iter_;
  };

public:
  ObDASIndexMergeIter()
    : ObDASIter(ObDASIterType::DAS_ITER_INDEX_MERGE),
      merge_type_(INDEX_MERGE_INVALID),
      child_scan_rtdefs_(),
      child_scan_params_(),
      child_tablet_ids_(),
      ls_id_(),
      merge_ctdef_(nullptr),
      merge_rtdef_(nullptr),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      disable_bitmap_(false),
      rowkey_is_uint64_(false),
      is_reverse_(false),
      force_merge_mode_(0),
      child_empty_count_(0),
      iter_end_count_(0),
      result_buffer_(),
      result_bitmap_iter_(nullptr),
      rowkey_exprs_(nullptr),
      mem_ctx_(),
      child_iters_(),
      child_stores_(),
      child_bitmaps_(),
      mock_skip_(nullptr),
      skip_id_threshold_(),
      current_skip_id_(0)
  {}

  virtual ~ObDASIndexMergeIter() {}

public:
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;
  int set_ls_tablet_ids(const ObLSID &ls_id, const ObDASRelatedTabletID &related_tablet_ids);
  ObTableScanParam *get_child_scan_param(int64_t idx) const { return child_scan_params_.at(idx); }

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

protected:
  // compare the first row of two stores
  int compare(IndexMergeRowStore &cur_store, IndexMergeRowStore &cmp_store, int &cmp_ret) const;
  int fill_child_stores(int64_t capacity, common::ObIArray<ObExpr*> *relevance_exprs = nullptr);
  int fill_child_bitmaps();
  int save_row_to_result_buffer(int64_t size);
  // expect_size is the expected count of rows, that is the 'count'
  int result_buffer_rows_to_expr(int64_t expect_size);
  void reset_datum_ptr(const common::ObIArray<ObExpr*> *exprs, int64_t size) const;
  int rowkey_range_is_all_intersected(bool &intersected) const;
  int rowkey_range_dense(uint64_t &dense) const;
  static const uint64_t SKIP_ID_THRESHOLD = 10000;
  int update_skip_id(int64_t child_idx, uint64_t *skip_id);
  int init_scan_param(const share::ObLSID &ls_id,
                      const common::ObTabletID &tablet_id,
                      const sql::ObDASScanCtDef *ctdef,
                      sql::ObDASScanRtDef *rtdef,
                      ObTableScanParam &scan_param) const;
private:
  int prepare_scan_ranges(ObTableScanParam &scan_param, const ObDASScanRtDef *rtdef) const;
  int check_disable_bitmap();
  int check_rowkey_is_uint64();
  int init_mock_skip(common::ObIAllocator &alloc, int64_t max_size);

public:
  ObIndexMergeType merge_type_;
  // now child_scan_rtdefs.count() == child_iters.count(), and so do child_scan_params and
  // child_tablet_ids
  // TODO: eliminate those we actually don't need, include index merge child and fts child.
  common::ObFixedArray<ObDASScanRtDef*, common::ObIAllocator> child_scan_rtdefs_;
  common::ObFixedArray<ObTableScanParam*, common::ObIAllocator> child_scan_params_;
  common::ObFixedArray<ObTabletID, common::ObIAllocator> child_tablet_ids_;

  ObLSID ls_id_;
  ObTabletID main_scan_tablet_id_;
  const ObDASIndexMergeCtDef *merge_ctdef_;
  ObDASIndexMergeRtDef *merge_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

protected:
  bool disable_bitmap_;
  bool rowkey_is_uint64_;
  bool is_reverse_;
  // tracepoint 1 for sort, 2 for bitmap
  int64_t force_merge_mode_;
  // count of child iters that drained_ = true
  int64_t child_empty_count_;
  // count of child iters that iter_end_ = true
  int64_t iter_end_count_;
  MergeResultBuffer result_buffer_;
  ObRoaringBitmapIter* result_bitmap_iter_;
  const ExprFixedArray *rowkey_exprs_;
  lib::MemoryContext mem_ctx_;
  common::ObFixedArray<ObDASIter*, common::ObIAllocator> child_iters_;
  common::ObFixedArray<IndexMergeRowStore, common::ObIAllocator> child_stores_;
  common::ObFixedArray<ObRoaringBitmap*, common::ObIAllocator> child_bitmaps_;
  // mock skip vector for add batch rows
  ObBitVector *mock_skip_;
  uint64_t skip_id_threshold_;
  uint64_t current_skip_id_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_ITER_H_ */
