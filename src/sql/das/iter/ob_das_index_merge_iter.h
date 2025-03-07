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

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObDASIndexMergeCtDef;
struct ObDASIndexMergeRtDef;

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
      is_reverse_(false)
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
    IndexMergeRowStore()
      : exprs_(nullptr),
        eval_ctx_(nullptr),
        max_size_(1),
        saved_size_(0),
        cur_idx_(OB_INVALID_INDEX),
        store_rows_(nullptr),
        iter_end_(false)
    {}
    IndexMergeRowStore(const common::ObIArray<ObExpr*> *exprs,
                       ObEvalCtx *eval_ctx,
                       int64_t max_size)
      : exprs_(exprs),
        eval_ctx_(eval_ctx),
        max_size_(max_size),
        saved_size_(0),
        cur_idx_(OB_INVALID_INDEX),
        store_rows_(nullptr),
        iter_end_(false)
    {}

    int init(common::ObIAllocator &allocator,
             const common::ObIArray<ObExpr*> *exprs,
             ObEvalCtx *eval_ctx,
             int64_t max_size);
    int save(bool is_vectorized, int64_t size);
    int to_expr();
    inline bool have_data() const { return cur_idx_ != OB_INVALID_INDEX && cur_idx_ < saved_size_; }
    int64_t rows_cnt() const { return have_data() ? saved_size_ - cur_idx_ : 0; }
    const ObDatum *cur_datums();
    void reuse();
    void reset();
    TO_STRING_KV(K_(exprs),
                 K_(saved_size),
                 K_(cur_idx),
                 K_(iter_end));

  public:
    typedef ObChunkDatumStore::LastStoredRow LastDASStoreRow;
    const common::ObIArray<ObExpr*> *exprs_;
    ObEvalCtx *eval_ctx_;
    int64_t max_size_;
    int64_t saved_size_;
    int64_t cur_idx_;
    LastDASStoreRow *store_rows_;
    bool iter_end_;
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
        result_store_("DASIndexMerge"),
        result_store_iter_()
    {}
    int init(int64_t max_size, ObEvalCtx *eval_ctx, const common::ObIArray<ObExpr*> *exprs, common::ObIAllocator &alloc);
    int add_row();
    int to_expr(int64_t size);
    void reuse();
    void reset();

  public:
    const common::ObIArray<ObExpr*> *exprs_;
    ObEvalCtx *eval_ctx_;
    int64_t max_size_;

    ObChunkDatumStore result_store_;
    ObChunkDatumStore::Iterator result_store_iter_;
  };

public:
  ObDASIndexMergeIter()
    : ObDASIter(ObDASIterType::DAS_ITER_INDEX_MERGE),
      merge_type_(INDEX_MERGE_INVALID),
      child_iters_(),
      child_stores_(),
      child_scan_rtdefs_(),
      child_scan_params_(),
      child_tablet_ids_(),
      rowkey_exprs_(nullptr),
      mem_ctx_(),
      get_next_row_(nullptr),
      get_next_rows_(nullptr),
      ls_id_(),
      merge_ctdef_(nullptr),
      merge_rtdef_(nullptr),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      is_reverse_(false),
      child_match_against_exprs_(),
      result_buffer_()
  {}

  virtual ~ObDASIndexMergeIter() {}

public:
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;
  int set_ls_tablet_ids(const ObLSID &ls_id, const ObDASRelatedTabletID &related_tablet_ids);
  ObTableScanParam *get_child_scan_param(int64_t idx) { return child_scan_params_.at(idx); }

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int compare(int64_t cur_idx, int64_t &output_idx, int &cmp_ret);
  int intersect_get_next_row();
  int intersect_get_next_rows(int64_t &count, int64_t capacity);
  int union_get_next_row();
  int union_get_next_rows(int64_t &count, int64_t capacity);
  int init_scan_param(const share::ObLSID &ls_id,
                      const common::ObTabletID &tablet_id,
                      const sql::ObDASScanCtDef *ctdef,
                      sql::ObDASScanRtDef *rtdef,
                      ObTableScanParam &scan_param);
  int prepare_scan_ranges(ObTableScanParam &scan_param, const ObDASScanRtDef *rtdef);
  void reset_datum_ptr(const common::ObIArray<ObExpr*> *exprs, int64_t size);
  int extract_match_against_exprs(const common::ObIArray<ObExpr*> &exprs, common::ObIArray<ObExpr*> &match_against_exprs);
  int fill_default_values_for_union(const common::ObIArray<ObExpr*> &exprs);
  int save_row_to_result_buffer();

private:
  ObIndexMergeType merge_type_;
  common::ObFixedArray<ObDASIter*, common::ObIAllocator> child_iters_;
  common::ObFixedArray<IndexMergeRowStore, common::ObIAllocator> child_stores_;
  // now child_scan_rtdefs.count() == child_iters.count(), and so do child_scan_params and
  // child_tablet_ids
  // TODO: eliminate those we actually don't need, include index merge child and fts child.

  common::ObFixedArray<ObDASScanRtDef*, common::ObIAllocator> child_scan_rtdefs_;
  common::ObFixedArray<ObTableScanParam*, common::ObIAllocator> child_scan_params_;
  common::ObFixedArray<ObTabletID, common::ObIAllocator> child_tablet_ids_;
  const ExprFixedArray *rowkey_exprs_;
  lib::MemoryContext mem_ctx_;
  int (ObDASIndexMergeIter::*get_next_row_)();
  int (ObDASIndexMergeIter::*get_next_rows_)(int64_t&, int64_t);
  ObLSID ls_id_;
  const ObDASIndexMergeCtDef *merge_ctdef_;
  ObDASIndexMergeRtDef *merge_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  bool is_reverse_;
  // need to fill default value for columns that do not have corresponding output when union merge,
  // only relevance score in fulltext search for now.
  common::ObFixedArray<ExprFixedArray, common::ObIAllocator> child_match_against_exprs_;
  MergeResultBuffer result_buffer_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_ITER_H_ */
