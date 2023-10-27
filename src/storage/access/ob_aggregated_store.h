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

static const int64_t AGG_ROW_MODE_COUNT_THRESHOLD = 3;
static const double AGG_ROW_MODE_RATIO_THRESHOLD = 0.5;

class ObAggCell
{
public:
  enum ObAggCellType
  {
    COUNT,
    MINMAX,
    FIRST_ROW,
  };
  ObAggCell(
      const int32_t col_idx,
      const share::schema::ObColumnParam *col_param,
      sql::ObExpr *expr,
      common::ObIAllocator &allocator);
  virtual ~ObAggCell();
  virtual void reset();
  virtual void reuse();
  virtual ObAggCellType get_type() const = 0;
  virtual int process(blocksstable::ObDatumRow &row) = 0;
  virtual int process(
      blocksstable::ObIMicroBlockReader *reader,
      int64_t *row_ids,
      const int64_t row_count) = 0;
  virtual int process(const blocksstable::ObMicroIndexInfo &index_info) = 0;
  virtual int fill_result(sql::ObEvalCtx &ctx, bool need_padding);
  OB_INLINE bool is_lob_col() const { return is_lob_col_; }
  OB_INLINE int32_t get_col_idx() const { return col_idx_; }
  TO_STRING_KV(K_(col_idx), K_(is_lob_col), K_(datum), KPC(col_param_), K_(expr));
protected:
  int prepare_def_datum();
  int fill_default_if_need(blocksstable::ObStorageDatum &datum);
  int pad_column_if_need(blocksstable::ObStorageDatum &datum);
protected:
  int32_t col_idx_;
  bool is_lob_col_;
  blocksstable::ObStorageDatum datum_;
  blocksstable::ObStorageDatum def_datum_;
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
  virtual ObAggCellType get_type() const override { return FIRST_ROW; }
  virtual int process(blocksstable::ObDatumRow &row) override;
  virtual int process(
      blocksstable::ObIMicroBlockReader *reader,
      int64_t *row_ids,
      const int64_t row_count) override;
  virtual int process(const blocksstable::ObMicroIndexInfo &index_info) override;
  virtual int fill_result(sql::ObEvalCtx &ctx, bool need_padding) override;
  INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(aggregated));
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
  virtual ObAggCellType get_type() const override { return COUNT; }
  virtual int process(blocksstable::ObDatumRow &row) override;
  virtual int process(
      blocksstable::ObIMicroBlockReader *reader,
      int64_t *row_ids,
      const int64_t row_count) override;
  virtual int process(const blocksstable::ObMicroIndexInfo &index_info) override;
   virtual int fill_result(sql::ObEvalCtx &ctx, bool need_padding) override;
   INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(exclude_null), K_(row_count));
private:
  bool exclude_null_;
  int64_t row_count_;
};

class ObAggDatumBuf {
public:
  ObAggDatumBuf(common::ObIAllocator &allocator);
  ~ObAggDatumBuf() { reset(); };
    int init(const int64_t size);
    void reuse();
    void reset();
    OB_INLINE ObDatum *get_datums() { return datums_; }
    TO_STRING_KV(K_(size), K_(datums), K_(buf));
  private:
    int64_t size_;
    ObDatum *datums_;
    char *buf_;
    common::ObIAllocator &allocator_;
};

class ObMinMaxAggCell : public ObAggCell
{
public:
  ObMinMaxAggCell(
      bool is_min,
      const int32_t col_idx,
      const share::schema::ObColumnParam *col_param,
      sql::ObExpr *expr,
      common::ObIAllocator &allocator);
  virtual ~ObMinMaxAggCell() { reset(); };
  virtual void reset() override;
  virtual void reuse() override;
  virtual ObAggCellType get_type() const override { return MINMAX; }
  int init(sql::ObPushdownOperator *op, sql::ObExpr *col_expr, const int64_t batch_size);
  virtual int process(blocksstable::ObDatumRow &row) override;
  virtual int process(
      blocksstable::ObIMicroBlockReader *reader,
      int64_t *row_ids,
      const int64_t row_count) override;
  virtual int process(const blocksstable::ObMicroIndexInfo &index_info) override;
  INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(is_min), K_(cmp_fun), K_(agg_datum_buf));
private:
  int deep_copy_datum(const blocksstable::ObStorageDatum &src);
  int process(blocksstable::ObStorageDatum &datum);
  bool is_min_;
  ObDatumCmpFuncType cmp_fun_;
  ObAggDatumBuf agg_datum_buf_;
  const char **cell_data_ptrs_;
  common::ObArenaAllocator datum_allocator_;
};

// TODO sum

class ObAggRow
{
public:
  ObAggRow(common::ObIAllocator &allocator);
  ~ObAggRow();
  void reset();
  void reuse();
  int init(const ObTableAccessParam &param, const int64_t batch_size);
  OB_INLINE int64_t get_agg_count() const { return agg_cells_.count(); }
  OB_INLINE bool need_exclude_null() const { return need_exclude_null_; };
  OB_INLINE bool has_lob_column_out() const { return has_lob_column_out_; }
  // void set_firstrow_aggregated(bool aggregated) { is_firstrow_aggregated_ = aggregated; }
  // bool is_firstrow_aggregated() const { return is_firstrow_aggregated_; }
  OB_INLINE ObAggCell* at(int64_t idx) { return agg_cells_.at(idx); }
  OB_INLINE common::ObIArray<ObAggCell*>& get_agg_cells() { return agg_cells_; }
  TO_STRING_KV(K_(agg_cells));
private:
  common::ObFixedArray<ObAggCell *, common::ObIAllocator> agg_cells_;
  bool need_exclude_null_;
  bool has_lob_column_out_;
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
           index_info.can_blockscan(agg_row_.has_lob_column_out()) &&
           !index_info.is_left_border() &&
           !index_info.is_right_border();
  }
  OB_INLINE void set_end() { iter_end_flag_ = IterEndState::ITER_END; }
  int check_agg_in_row_mode(const ObTableIterParam &iter_param);
  TO_STRING_KV(K_(is_firstrow_aggregated), K_(agg_row), K_(agg_flat_row_mode));

private:
  bool is_firstrow_aggregated_;
  ObAggRow agg_row_;
  bool agg_flat_row_mode_;
  blocksstable::ObDatumRow row_buf_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OB_STORAGE_OB_AGGREGATED_STORE_H_ */
