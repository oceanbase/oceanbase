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

#ifndef OB_STORAGE_OB_VECTOR_STORE_H_
#define OB_STORAGE_OB_VECTOR_STORE_H_

#include "lib/container/ob_bitmap.h"
#include "share/schema/ob_table_param.h"
#include "sql/engine/expr/ob_expr.h"
#include "ob_block_batched_row_store.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{

namespace blocksstable
{
class ObIMicroBlockReader;
class ObIMicroBlockReader;
}

namespace storage
{
class ObGroupByCell;
class ObVectorStore : public ObBlockBatchedRowStore {
public:
  ObVectorStore(
      const int64_t batch_size,
      sql::ObEvalCtx &eval_ctx,
      ObTableAccessContext &context);
  virtual ~ObVectorStore();
  virtual int init(const ObTableAccessParam &param) override;
  virtual void reset() override;
  // shallow copy
  virtual int fill_rows(
      const int64_t group_idx,
      blocksstable::ObIMicroBlockRowScanner *scanner,
      int64_t &begin_index,
      const int64_t end_index,
      const ObFilterResult &res) override;
  virtual int fill_row(blocksstable::ObDatumRow &row) override;
  virtual int fill_rows(const int64_t group_idx, const int64_t row_count) override;
  virtual void set_end() override
  {
    if (count_ > 0) {
      iter_end_flag_ = IterEndState::ITER_END;
      eval_ctx_.set_batch_idx(0);
    }
  }
  OB_INLINE int64_t get_row_count() { return count_; }
  OB_INLINE ObGroupByCell *get_group_by_cell() { return group_by_cell_; }
  virtual int reuse_capacity(const int64_t capacity) override;
  virtual bool is_empty() const override final { return 0 == count_; }
  DECLARE_TO_STRING;
private:
  int fill_group_idx(const int64_t group_idx);
  int fill_output_rows(
      const int64_t group_idx,
      blocksstable::ObIMicroBlockRowScanner *scanner,
      int64_t &begin_index,
      const int64_t end_index,
      const ObFilterResult &res);
  int fill_group_by_rows(
      const int64_t group_idx,
      blocksstable::ObIMicroBlockReader *reader,
      int64_t &begin_index,
      const int64_t end_index,
      const ObFilterResult &res);
  int check_can_group_by(
      blocksstable::ObIMicroBlockReader *reader,
      int64_t &begin_index,
      const int64_t end_index,
      const ObFilterResult &res,
      bool &can_group_by);

  int do_group_by(
      const int64_t group_idx,
      blocksstable::ObIMicroBlockReader *reader,
      int64_t begin_index,
      const int64_t end_index,
      const ObFilterResult &res);

  int64_t count_;
  // exprs needed fill in
  sql::ExprFixedArray exprs_;
  common::ObFixedArray<int32_t, common::ObIAllocator> cols_projector_;
  common::ObFixedArray<blocksstable::ObSqlDatumInfo, common::ObIAllocator> datum_infos_;
  common::ObFixedArray<const share::schema::ObColumnParam*, common::ObIAllocator> col_params_;
  sql::ObExpr *group_idx_expr_;
  blocksstable::ObDatumRow default_row_;
  ObGroupByCell *group_by_cell_;
  const ObTableIterParam *iter_param_;
};

}
}
#endif //OB_STORAGE_OB_VECTOR_STORE_H_
