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
#include "ob_block_batched_row_store.h"
#include "storage/access/ob_pushdown_aggregate.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"

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

// for column store
class ObCGAggCells
{
public:
  ObCGAggCells() : agg_cells_() {}
  ~ObCGAggCells() { reset(); }
  void reset();
  bool check_finished() const;
  int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info, bool &can_agg);
  int add_agg_cell(ObAggCell *cell);
  int process(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t col_idx,
      blocksstable::ObIMicroBlockReader *reader,
      const int32_t *row_ids,
      const int64_t row_count);
  int process(blocksstable::ObStorageDatum &datum, const uint64_t row_count);
  int process(const blocksstable::ObMicroIndexInfo &index_info);
  TO_STRING_KV(K_(agg_cells));
private:
  common::ObSEArray<ObAggCell*, 1> agg_cells_;
};

class ObAggRow
{
public:
  ObAggRow(common::ObIAllocator &allocator);
  ~ObAggRow();
  void reset();
  void reuse();
  int init(const ObTableAccessParam &param, const ObTableAccessContext &context, const int64_t batch_size);
  OB_INLINE int64_t get_agg_count() const { return agg_cells_.count(); }
  OB_INLINE int64_t get_dummy_agg_count() const { return dummy_agg_cells_.count(); }
  OB_INLINE bool has_lob_column_out() const { return has_lob_column_out_; }
  bool check_need_access_data();
  OB_INLINE ObAggCell* at(int64_t idx) { return agg_cells_.at(idx); }
  OB_INLINE ObAggCell* at_dummy(int64_t idx) { return dummy_agg_cells_.at(idx); }
  OB_INLINE common::ObIArray<ObAggCell*>& get_agg_cells() { return agg_cells_; }
  TO_STRING_KV(K_(agg_cells), K_(dummy_agg_cells), K_(can_use_index_info), K_(need_access_data), K_(has_lob_column_out));
private:
  bool found_ref_column(const ObTableAccessParam &param, const int32_t agg_col_offset);
  common::ObFixedArray<ObAggCell *, common::ObIAllocator> agg_cells_;
  // TODO(yht146439) remove this after DAS eliminate unused output
  common::ObFixedArray<ObAggCell *, common::ObIAllocator> dummy_agg_cells_;
  bool can_use_index_info_;
  bool need_access_data_;
  bool has_lob_column_out_;
  common::ObIAllocator &allocator_;
  ObPDAggFactory agg_cell_factory_;
};

class ObAggregatedStore : public ObBlockBatchedRowStore
{
public:
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
      blocksstable::ObIMicroBlockRowScanner *scanner,
      int64_t &begin_index,
      const int64_t end_index,
      const ObFilterResult &res) override;
  virtual int fill_rows(const int64_t group_idx, const int64_t row_count) override;
  virtual int fill_row(blocksstable::ObDatumRow &out_row) override;
  int collect_aggregated_row(blocksstable::ObDatumRow *&row);
  int get_agg_cell(const sql::ObExpr *expr, ObAggCell *&agg_cell);
  OB_INLINE void reuse_aggregated_row() { agg_row_.reuse(); }
  OB_INLINE bool can_agg_index_info(const blocksstable::ObMicroIndexInfo &index_info)
  {
    // TODO(yht146439) can not use pre-aggregated data in row store now
    // as 'has_agg_data()' only tells there is pre-aggregated data, but without info of which column
    // only use row count in index tree to calc COUNT(*/NOT NULLABLE COL) now
    // enable this after solving this problem.
    // return filter_is_null() && (!agg_row_.check_need_access_data() ||
    //        (agg_row_.can_use_index_info() && index_info.has_agg_data())) &&
    //        index_info.can_blockscan(agg_row_.has_lob_column_out()) &&
    //        !index_info.is_left_border() &&
    //        !index_info.is_right_border();
    return filter_is_null() &&
           !agg_row_.check_need_access_data() &&
           index_info.can_blockscan(agg_row_.has_lob_column_out()) &&
           !index_info.is_left_border() &&
           !index_info.is_right_border();
  }
  OB_INLINE void set_end() { iter_end_flag_ = IterEndState::ITER_END; }
  int check_agg_in_row_mode(const ObTableIterParam &iter_param);
  bool has_data();
  INHERIT_TO_STRING_KV("ObBlockBatchedRowStore", ObBlockBatchedRowStore, K_(agg_row), K_(agg_flat_row_mode));

private:
  ObAggRow agg_row_;
  bool agg_flat_row_mode_;
  blocksstable::ObDatumRow row_buf_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OB_STORAGE_OB_AGGREGATED_STORE_H_ */
