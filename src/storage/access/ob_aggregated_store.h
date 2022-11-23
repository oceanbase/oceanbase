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

#ifndef OB_STORAGE_OB_AGGREGATED_STORE_H_
#define OB_STORAGE_OB_AGGREGATED_STORE_H_

#include "sql/engine/expr/ob_expr.h"
#include "storage/ob_i_store.h"
#include "ob_block_batched_row_store.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_index_block_row_struct.h"

namespace oceanbase
{
namespace blocksstable
{
class ObMicroBlockDecoder;
struct ObMicroIndexInfo;
}
namespace storage
{

class ObAggCell
{
public:
  ObAggCell(
      const int32_t col_idx,
      const share::schema::ObColumnParam *col_param,
      sql::ObExpr *expr,
      common::ObIAllocator &allocator);
  virtual ~ObAggCell();
  virtual void reset();
  virtual void reuse();
  virtual int process(blocksstable::ObDatumRow &row) = 0;
  virtual int process(
      blocksstable::ObIMicroBlockReader *reader,
      int64_t *row_ids,
      const int64_t row_count) = 0;
  virtual int process(const blocksstable::ObMicroIndexInfo &index_info) = 0;
  virtual int fill_result(sql::ObEvalCtx &ctx, bool need_padding);
  TO_STRING_KV(K_(col_idx), K_(datum), KPC(col_param_), K_(expr));
protected:
  int fill_default_if_need(blocksstable::ObStorageDatum &datum);
  int pad_column_if_need(blocksstable::ObStorageDatum &datum);
  int32_t col_idx_;
  blocksstable::ObStorageDatum datum_;
  const share::schema::ObColumnParam *col_param_;
  sql::ObExpr *expr_;
  common::ObIAllocator &allocator_;
};

// mysql compatibility, select a,count(a), output first value of a
class ObFirstRowAggCell : public ObAggCell
{
public:
  ObFirstRowAggCell(
      const int32_t col_idx,
      const share::schema::ObColumnParam *col_param,
      sql::ObExpr *expr,
      common::ObIAllocator &allocator);
  virtual ~ObFirstRowAggCell() { reset(); };
  virtual void reset() override;
  virtual int process(blocksstable::ObDatumRow &row) override;
  virtual int process(
      blocksstable::ObIMicroBlockReader *reader,
      int64_t *row_ids,
      const int64_t row_count) override;
  virtual int process(const blocksstable::ObMicroIndexInfo &index_info) override;
  virtual int fill_result(sql::ObEvalCtx &ctx, bool need_padding) override;
  TO_STRING_KV(K_(col_idx), K_(datum), K_(col_param), K_(expr), K_(aggregated));
private:
  bool aggregated_;
};

class ObCountAggCell : public ObAggCell
{
public:
  ObCountAggCell(
      const int32_t col_idx,
      const share::schema::ObColumnParam *col_param,
      sql::ObExpr *expr,
      common::ObIAllocator &allocator,
      bool exclude_null);
  virtual ~ObCountAggCell() { reset(); };
  virtual void reset() override;
  virtual void reuse() override;
  virtual int process(blocksstable::ObDatumRow &row) override;
  virtual int process(
      blocksstable::ObIMicroBlockReader *reader,
      int64_t *row_ids,
      const int64_t row_count) override;
  virtual int process(const blocksstable::ObMicroIndexInfo &index_info) override;
   virtual int fill_result(sql::ObEvalCtx &ctx, bool need_padding) override;
   TO_STRING_KV(K_(col_idx), K_(datum), K_(col_param), K_(expr), K_(exclude_null), K_(row_count));
private:
  bool exclude_null_;
  int64_t row_count_;
};
// TODO sum/min/max

class ObAggRow
{
public:
  ObAggRow(common::ObIAllocator &allocator);
  ~ObAggRow();
  void reset();
  void reuse();
  int init(const ObTableAccessParam &param);
  int64_t get_agg_count() const { return agg_cells_.count(); }
  bool need_exclude_null() const { return need_exclude_null_; };
  // void set_firstrow_aggregated(bool aggregated) { is_firstrow_aggregated_ = aggregated; }
  // bool is_firstrow_aggregated() const { return is_firstrow_aggregated_; }
  ObAggCell* at(int64_t idx) { return agg_cells_.at(idx); }
  TO_STRING_KV(K_(agg_cells));
private:
  common::ObFixedArray<ObAggCell *, common::ObIAllocator> agg_cells_;
  bool need_exclude_null_;
  common::ObIAllocator &allocator_;
};

class ObAggregatedStore : public ObBlockBatchedRowStore
{
public:
  static const int64_t BATCH_SIZE = 1024;
  ObAggregatedStore(
      const int64_t batch_size,
      sql::ObEvalCtx &eval_ctx,
      ObTableAccessContext &context);
  virtual ~ObAggregatedStore();
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(const ObTableAccessParam &param) override;
  int fill_index_info(const blocksstable::ObMicroIndexInfo &index_info);
  virtual int fill_rows(
      const int64_t group_idx,
      blocksstable::ObIMicroBlockReader *reader,
      int64_t &begin_index,
      const int64_t end_index,
      const common::ObBitmap *bitmap = nullptr) override;
  virtual int fill_row(blocksstable::ObDatumRow &out_row) override;
  int collect_aggregated_row(blocksstable::ObDatumRow *&row);
  OB_INLINE void reuse_aggregated_row() { agg_row_.reuse(); }
  OB_INLINE bool can_batched_aggregate() const { return is_firstrow_aggregated_; }
  OB_INLINE bool can_agg_index_info(const blocksstable::ObMicroIndexInfo &index_info) const
  { 
    return filter_is_null() && !agg_row_.need_exclude_null() && can_batched_aggregate() &&
           index_info.can_blockscan() &&
           !index_info.is_left_border() &&
           !index_info.is_right_border();
  }
  OB_INLINE void set_end() { iter_end_flag_ = IterEndState::ITER_END; }
  TO_STRING_KV(K_(agg_row));

private:
  bool is_firstrow_aggregated_;
  ObAggRow agg_row_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OB_STORAGE_OB_AGGREGATED_STORE_H_ */
