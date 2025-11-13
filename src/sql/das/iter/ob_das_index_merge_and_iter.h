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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_AND_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_AND_ITER_H_

#include "sql/das/iter/ob_das_index_merge_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

class ObDASIndexMergeAndIter : public ObDASIndexMergeIter
{
public:
  /* abstract cursor interface for rowid-based index merge optimization */
  class IndexMergeRowIdCursor
  {
  public:
    IndexMergeRowIdCursor() {}
    virtual ~IndexMergeRowIdCursor() {}
    virtual int advance_to(uint64_t target, uint64_t &rowid) = 0;
    virtual uint64_t current_rowid() const = 0;
    virtual bool is_exhausted() const = 0;
    virtual int to_expr() = 0;
    virtual void reuse() = 0;
    virtual void reset() = 0;
    DECLARE_PURE_VIRTUAL_TO_STRING;
  };

  class SortedRowIdCursor : public IndexMergeRowIdCursor
  {
  public:
    SortedRowIdCursor() : store_(nullptr) {}
    int init(ObDASIndexMergeIter::IndexMergeRowStore *store);
    int advance_to(uint64_t target, uint64_t &rowid) override { return store_->lower_bound(target, rowid); }
    uint64_t current_rowid() const override { return store_->rowids_[store_->idx_]; }
    bool is_exhausted() const override { return store_->is_empty(); }
    int to_expr() override { return store_->to_expr(1); }
    void reuse() override {}
    void reset() override { store_ = nullptr; }
    VIRTUAL_TO_STRING_KV(KPC_(store));
  private:
    ObDASIndexMergeIter::IndexMergeRowStore *store_;
  };

  class BitmapRowIdCursor : public IndexMergeRowIdCursor
  {
  public:
    BitmapRowIdCursor()
      : bitmap_(nullptr),
        bitmap_iter_(nullptr),
        exhausted_(true),
        allocator_(nullptr),
        scan_iter_(nullptr),
        scan_size_(0),
        rowid_expr_(nullptr),
        eval_ctx_(nullptr),
        counted_as_empty_(false)
    {}
    int init(common::ObIAllocator &allocator,
             ObDASScanIter *scan_iter,
             int64_t scan_size,
             ObExpr *rowid_expr,
             ObEvalCtx *eval_ctx);
    int build_bitmap();
    int advance_to(uint64_t target, uint64_t &rowid) override;
    uint64_t current_rowid() const override { return bitmap_iter_->get_curr_value(); }
    bool is_exhausted() const override { return exhausted_; }
    int to_expr() override;
    void reuse() override;
    void reset() override;
    VIRTUAL_TO_STRING_KV(K_(exhausted), KP_(bitmap), KP_(bitmap_iter), KP_(scan_iter), K_(scan_size));

  private:
    ObRoaringBitmap *bitmap_;
    ObRoaringBitmapIter *bitmap_iter_;
    bool exhausted_;
    common::ObIAllocator *allocator_;
    ObDASScanIter *scan_iter_;
    int64_t scan_size_;
    ObExpr *rowid_expr_;
    ObEvalCtx *eval_ctx_;

  public:
    bool counted_as_empty_;  // track if this cursor has been counted in child_empty_count_
  };

public:
  ObDASIndexMergeAndIter()
    : ObDASIndexMergeIter(),
      can_be_shorted_(false),
      shorted_child_idx_(OB_INVALID_INDEX),
      main_scan_param_(nullptr),
      main_scan_iter_(nullptr),
      first_main_scan_(true),
      lookup_memctx_(),
      child_cursors_(),
      enable_bitmap_cursor_(false)
  {}

  virtual ~ObDASIndexMergeAndIter() {}

  OB_INLINE ObTableScanParam *get_main_scan_param() const { return main_scan_param_; }

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int do_table_scan() override;
  virtual int rescan() override;

private:
  int prepare_main_scan_param(ObDASScanIter *main_scan_iter);
  int get_next_merge_rows(int64_t &count, int64_t capacity);
  int sort_get_next_row();
  int sort_get_next_rows(int64_t &count, int64_t capacity);
  // short circuit path
  int shorted_get_next_row();
  int shorted_get_next_rows(int64_t &count, int64_t capacity);

  int check_can_be_shorted();

  bool should_use_bitmap_for_child(int64_t child_idx) const;
  ObDASScanIter* extract_scan_iter_from_child(int64_t child_idx) const;
  int init_child_cursors();
  int check_child_cursors();
  int build_bitmaps_for_children();

private:
  bool can_be_shorted_;
  int64_t shorted_child_idx_;
  ObTableScanParam *main_scan_param_;
  ObDASScanIter *main_scan_iter_;
  bool first_main_scan_;
  lib::MemoryContext lookup_memctx_;
  common::ObFixedArray<IndexMergeRowIdCursor*, common::ObIAllocator> child_cursors_;
  bool enable_bitmap_cursor_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_AND_ITER_H_ */
