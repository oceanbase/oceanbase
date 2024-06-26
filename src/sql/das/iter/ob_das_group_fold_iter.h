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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_GROUP_FOLD_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_GROUP_FOLD_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_merge_iter.h"
#include "common/row/ob_row_iterator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace sql
{

class ObGroupResultSaveRows
{
public:
  ObGroupResultSaveRows()
    : inited_(false),
      exprs_(NULL),
      eval_ctx_(NULL),
      saved_size_(0),
      max_size_(1),
      start_pos_(0),
      group_id_idx_(0),
      store_rows_(NULL),
      need_check_output_datum_(false)
  {}

  int init(const common::ObIArray<ObExpr*> &exprs,
           ObEvalCtx &eval_ctx,
           int64_t max_size,
           int64_t group_id_idx,
           bool need_check_output_datum,
           common::ObIAllocator &allocator);
  int save(bool is_vectorized, int64_t start_pos, int64_t size);
  int to_expr(bool is_vectorized, int64_t start_pos, int64_t size);
  int64_t cur_group_idx();
  void next_start_pos() { start_pos_++; }
  int64_t get_start_pos() { return start_pos_; }
  void reuse();
  void reset();
  TO_STRING_KV(K_(saved_size),
               K_(start_pos),
               K_(max_size),
               K_(group_id_idx));

public:
  typedef ObChunkDatumStore::LastStoredRow LastDASStoreRow;

  bool inited_;
  const common::ObIArray<ObExpr*> *exprs_;
  ObEvalCtx *eval_ctx_;
  int64_t saved_size_;
  int64_t max_size_;
  int64_t start_pos_;
  int64_t group_id_idx_;
  LastDASStoreRow *store_rows_;
  bool need_check_output_datum_;
};

struct ObDASGroupFoldIterParam : public ObDASIterParam
{
public:
  ObDASGroupFoldIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_GROUP_FOLD)
  {}
  bool need_check_output_datum_;
  ObDASIter *iter_tree_;

  virtual bool is_valid() const
  {
    return ObDASIterParam::is_valid() && iter_tree_ != nullptr;
  }
};

class ObDASGroupFoldIter : public ObDASIter
{
public:
  ObDASGroupFoldIter()
    : ObDASIter(ObDASIterType::DAS_ITER_GROUP_FOLD),
      cur_group_idx_(0),
      available_group_idx_(MIN_GROUP_INDEX),
      group_size_(0),
      need_check_output_datum_(false),
      group_save_rows_(),
      iter_tree_(nullptr),
      iter_alloc_(nullptr)
  {}

  virtual ~ObDASGroupFoldIter() {}
  int set_scan_group(int64_t group_id);
  void init_group_range(int64_t cur_group_idx, int64_t group_size);

  INHERIT_TO_STRING_KV("ObDASIter", ObDASIter, K_(cur_group_idx), K_(available_group_idx),
      K_(group_size), K_(group_save_rows));

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  void reset_expr_datum_ptr();
  static const int64_t MIN_GROUP_INDEX = -1;
  int64_t cur_group_idx_;        // 本次要读的group_idx
  int64_t available_group_idx_;  // 当前row_store中可读的最小group_idx
  int64_t group_size_;
  bool need_check_output_datum_;
  ObGroupResultSaveRows group_save_rows_;

  ObDASIter *iter_tree_;
  common::ObArenaAllocator *iter_alloc_;
  char iter_alloc_buf_[sizeof(common::ObArenaAllocator)];
};


}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_GROUP_FOLD_ITER_H_ */
