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

#ifndef OBDEV_SRC_SQL_DAS_OB_GROUP_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_OB_GROUP_SCAN_ITER_H_
#include "common/row/ob_row_iterator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
namespace oceanbase
{
namespace sql
{
class ObGroupResultRows
{
public:
  ObGroupResultRows() : inited_(false), exprs_(NULL), eval_ctx_(NULL),
                        saved_size_(0), max_size_(1), start_pos_(0), group_id_expr_pos_(0),
                        rows_(NULL), need_check_output_datum_(false),reuse_alloc_(nullptr),access_exprs_(nullptr)
  {
  }

  ~ObGroupResultRows() { reset(); }

  int init(const common::ObIArray<ObExpr *> &exprs,
           ObEvalCtx &eval_ctx,
           const ExprFixedArray &access_exprs,
           common::ObIAllocator &das_op_allocator,
           int64_t max_size,
           ObExpr *group_id_expr,
           bool need_check_output_datum,
           ObMemAttr& attr);
  int save(bool is_vectorized, int64_t start_pos, int64_t size);
  int to_expr(bool is_vectorized, int64_t start_pos, int64_t size);
  int64_t cur_group_idx();
  void next_start_pos() { start_pos_++; }
  int64_t get_start_pos() { return start_pos_; }
  void reset() {
    inited_ = false;
    exprs_ = NULL;
    eval_ctx_ = NULL;
    saved_size_ = 0;
    max_size_ = 1;
    start_pos_ = 0;
    group_id_expr_pos_ = 0;
    rows_ = NULL;
    need_check_output_datum_ = false;
    //Temp fix
    if (reuse_alloc_ != nullptr) {
      reuse_alloc_->reset();
      reuse_alloc_->~ObArenaAllocator();
      reuse_alloc_ = nullptr;
    }
    access_exprs_ = nullptr;
  }
  TO_STRING_KV(K_(saved_size),
               K_(start_pos),
               K_(max_size),
               K_(group_id_expr_pos));

public:
  typedef ObChunkDatumStore::LastStoredRow LastDASStoreRow;

  bool inited_;
  const common::ObIArray<ObExpr *> *exprs_;
  ObEvalCtx *eval_ctx_;
  int64_t saved_size_;
  int64_t max_size_;
  int64_t start_pos_;
  int64_t group_id_expr_pos_;
  LastDASStoreRow *rows_;
  bool need_check_output_datum_;
  //Temp fix
  //Current implement group iter is eval in das task context.
  //Whe Das task retry we hard to ues allocator pass from eval ctx, we can not free or
  //reuse LastDASStoreRow memory because when das task is remote the alloctor is change,
  //only memory from eval ctx can be reuse.
  //So we just introduce this temp fix use a new alloctor make LastDASStoreRow have a same
  //life cycle with ObGroupResultRows.
  //After next version @xiyang.gjc will refactor group rescan, then ObGroupResultRows will
  //be move from das into Table scan op, every thing will be easy.
  common::ObArenaAllocator *reuse_alloc_;
  char reuse_alloc_buf_[sizeof(common::ObArenaAllocator)];
  const ExprFixedArray *access_exprs_;
};

class ObGroupScanIter : public ObNewRowIterator
{
  OB_UNIS_VERSION(1);
public:
  ObGroupScanIter();
  virtual ~ObGroupScanIter() = default;
  //virtual int rescan() override;
  int switch_scan_group();
  int set_scan_group(int64_t group_id);
  virtual int get_next_row(ObNewRow *&row) { return common::OB_NOT_IMPLEMENT; } ;
  virtual int get_next_row() override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;

  int64_t get_cur_group_idx() const { return cur_group_idx_; }
  int64_t get_group_size() const { return group_size_; }
  ObNewRowIterator *&get_iter() { return *iter_; }

  void reset_expr_datum_ptr();
  void reset() override;
  void init_group_range(int64_t cur_group_idx, int64_t group_size)
  {
    last_group_idx_ = MIN_GROUP_INDEX;
    cur_group_idx_ = cur_group_idx;
    group_size_ = group_size;
  }
  ObExpr *get_group_id_expr() { return group_id_expr_; }
  int init_row_store(const common::ObIArray<ObExpr *> &exprs,
                     ObEvalCtx &eval_ctx,
                     const ExprFixedArray &access_exprs,
                     common::ObIAllocator &das_op_allocator,
                     int64_t max_size,
                     ObExpr *group_id_expr,
                     ObNewRowIterator **iter,
                     bool need_check_output_datum,
                     ObMemAttr& attr)
  {
    group_id_expr_ = group_id_expr;
    iter_ = iter;
    return row_store_.init(exprs,
                           eval_ctx,
                           access_exprs,
                           das_op_allocator,
                           max_size,
                           group_id_expr,
                           need_check_output_datum,
                           attr);
  }
  ObNewRowIterator *&get_result_tmp_iter() { return result_tmp_iter_; }

  TO_STRING_KV(K_(cur_group_idx),
               K_(last_group_idx),
               K_(group_size),
               K_(row_store));
private:
  static const int64_t  MIN_GROUP_INDEX = -1;
private:
  int64_t cur_group_idx_;
  int64_t last_group_idx_;
  int64_t group_size_;
  ObExpr *group_id_expr_;
  ObGroupResultRows row_store_;
  // used for local index lookup iter
  ObNewRowIterator *result_tmp_iter_;
  // use secondary pointer, because when init das group scan,
  // the pointer of result iter in ObDasScanOp not init, so here should
  // hold the address of result iter point
  ObNewRowIterator **iter_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_BATCH_SCAN_OP_H_ */
