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
      left_iter_(nullptr),
      left_output_(nullptr),
      right_iter_(nullptr),
      right_output_(nullptr),
      ctdef_(nullptr),
      rtdef_(nullptr),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      is_reverse_(false)
  {}

  ObIndexMergeType merge_type_;
  const ExprFixedArray *rowkey_exprs_;
  ObDASIter *left_iter_;
  const common::ObIArray<ObExpr*> *left_output_;
  ObDASIter *right_iter_;
  const common::ObIArray<ObExpr*> *right_output_;
  const ObDASIndexMergeCtDef *ctdef_;
  ObDASIndexMergeRtDef *rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  bool is_reverse_;

  virtual bool is_valid() const
  {
    return rowkey_exprs_ != nullptr &&
           left_iter_ != nullptr &&
           left_output_ != nullptr &&
           right_iter_ != nullptr &&
           right_output_ != nullptr &&
           ctdef_ != nullptr &&
           rtdef_ != nullptr &&
           merge_type_ != INDEX_MERGE_INVALID &&
           ObDASIterParam::is_valid();
  }
};

class ObDASIndexMergeIter : public ObDASIter
{

public:
  struct RowStore
  {
  public:
    RowStore()
      : exprs_(nullptr),
        eval_ctx_(nullptr),
        max_size_(1),
        saved_size_(0),
        cur_idx_(OB_INVALID_INDEX),
        store_rows_(nullptr)
    {}
    RowStore(const common::ObIArray<ObExpr*> *exprs,
             ObEvalCtx *eval_ctx,
             int64_t max_size)
      : exprs_(exprs),
        eval_ctx_(eval_ctx),
        max_size_(max_size),
        saved_size_(0),
        cur_idx_(OB_INVALID_INDEX),
        store_rows_(nullptr)
    {}

    int init(common::ObIAllocator &allocator);
    int save(bool is_vectorized, int64_t size);
    int to_expr(bool is_vectorized, int64_t size);
    bool have_data() const { return cur_idx_ != OB_INVALID_INDEX && cur_idx_ < saved_size_; }
    int64_t rows_cnt() const { return have_data() ? saved_size_ - cur_idx_ : 0; }
    const ObDatum *cur_datums();
    void reuse();
    void reset();
    TO_STRING_KV(K_(exprs),
                 K_(saved_size),
                 K_(cur_idx));

  public:
    typedef ObChunkDatumStore::LastStoredRow LastDASStoreRow;
    const common::ObIArray<ObExpr*> *exprs_;
    ObEvalCtx *eval_ctx_;
    int64_t max_size_;
    int64_t saved_size_;
    int64_t cur_idx_;
    LastDASStoreRow *store_rows_;
  };

public:
  ObDASIndexMergeIter()
    : ObDASIter(ObDASIterType::DAS_ITER_INDEX_MERGE),
      merge_type_(INDEX_MERGE_INVALID),
      state_(FILL_LEFT_ROW),
      left_iter_(nullptr),
      right_iter_(nullptr),
      left_row_store_(nullptr),
      right_row_store_(nullptr),
      left_scan_param_(),
      right_scan_param_(),
      left_iter_end_(false),
      right_iter_end_(false),
      rowkey_exprs_(nullptr),
      left_output_(nullptr),
      right_output_(nullptr),
      mem_ctx_(),
      get_next_row_(nullptr),
      get_next_rows_(nullptr),
      ls_id_(),
      left_tablet_id_(),
      right_tablet_id_(),
      merge_ctdef_(nullptr),
      merge_rtdef_(nullptr),
      left_scan_ctdef_(nullptr),
      left_scan_rtdef_(nullptr),
      right_scan_ctdef_(nullptr),
      right_scan_rtdef_(nullptr),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      is_reverse_(false)
  {}

  virtual ~ObDASIndexMergeIter() {}

public:
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;
  int set_ls_tablet_ids(const ObLSID &ls_id, const ObDASRelatedTabletID &related_tablet_ids);
  ObTableScanParam &get_left_param() { return left_scan_param_; }
  ObTableScanParam &get_right_param() { return right_scan_param_; }

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int compare(int &cmp_ret);
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

private:
  enum MergeState : uint32_t
  {
    FILL_LEFT_ROW,
    FILL_RIGHT_ROW,
    MERGE_AND_OUTPUT,
    FINISHED
  };

  ObIndexMergeType merge_type_;
  MergeState state_;
  ObDASIter *left_iter_;
  ObDASIter *right_iter_;
  RowStore *left_row_store_;
  RowStore *right_row_store_;
  ObTableScanParam left_scan_param_;
  ObTableScanParam right_scan_param_;
  bool left_iter_end_;
  bool right_iter_end_;
  const ExprFixedArray *rowkey_exprs_;
  const common::ObIArray<ObExpr*> *left_output_;
  const common::ObIArray<ObExpr*> *right_output_;
  lib::MemoryContext mem_ctx_;
  int (ObDASIndexMergeIter::*get_next_row_)();
  int (ObDASIndexMergeIter::*get_next_rows_)(int64_t&, int64_t);
  ObLSID ls_id_;
  ObTabletID left_tablet_id_;
  ObTabletID right_tablet_id_;
  const ObDASIndexMergeCtDef *merge_ctdef_;
  ObDASIndexMergeRtDef *merge_rtdef_;
  // nullptr if left child is not a leaf node
  const ObDASScanCtDef *left_scan_ctdef_;
  ObDASScanRtDef *left_scan_rtdef_;
  const ObDASScanCtDef *right_scan_ctdef_;
  ObDASScanRtDef *right_scan_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  bool is_reverse_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_ITER_H_ */
