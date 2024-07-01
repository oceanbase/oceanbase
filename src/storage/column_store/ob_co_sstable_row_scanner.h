/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_STORAGE_COLUMN_STORE_OB_CO_SSTABLE_ROW_SCANNER_H_
#define OB_STORAGE_COLUMN_STORE_OB_CO_SSTABLE_ROW_SCANNER_H_
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/access/ob_block_batched_row_store.h"
#include "ob_column_store_util.h"
#include "ob_co_sstable_rows_filter.h"
#include "ob_i_cg_iterator.h"
#include "ob_cg_iter_param_pool.h"

namespace oceanbase
{
namespace storage
{
class ObBlockBatchedRowStore;
class ObGroupByCell;
class ObCGGroupByScanner;
class ObCOSSTableRowScanner : public ObStoreRowIterator
{
enum ScanState
{
  BEGIN,
  FILTER_ROWS,
  PROJECT_ROWS,
  END,
};

public:
  ObCOSSTableRowScanner();
  virtual ~ObCOSSTableRowScanner();
  virtual int init(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range) override;
  virtual void reset() override;
  virtual void reuse() override;
  virtual bool can_blockscan() const override
  {
    return is_scan(type_) &&
           nullptr != block_row_store_ &&
           MAX_STATE != blockscan_state_;
  }
  virtual bool can_batch_scan() const override
  {
     return can_blockscan() &&
            iter_param_->vectorized_enabled_ &&
            iter_param_->enable_pd_filter();
  }
  virtual int get_next_rows() override;
  TO_STRING_KV(KPC_(iter_param),
               KP_(access_ctx),
               K_(row_scanner),
               K_(range_idx),
               KP_(rows_filter),
               KP_(project_iter),
               KP_(getter_project_iter),
               K_(group_by_project_idx),
               K_(group_by_iters),
               KP_(group_by_cell),
               K_(is_new_group),
               KP_(batched_row_store),
               KP_(cg_param_pool),
               K_(current),
               K_(end),
               K_(group_size),
               K_(batch_size),
               K_(reverse_scan),
               K_(state),
               K_(blockscan_state),
               K_(range),
               K_(pending_end_row_id),
               K_(column_group_cnt),
               K_(getter_projector));
protected:
  virtual int inner_get_next_row(const ObDatumRow *&store_row) override;
  virtual int refresh_blockscan_checker(const blocksstable::ObDatumRowkey &rowkey) override;
private:
  static const ScanState STATE_TRANSITION[BlockScanState::MAX_STATE];
  virtual int init_row_scanner(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObITable *table,
      const void *query_range);
  virtual int get_group_idx(int64_t &group_idx);
  int init_cg_param_pool(ObTableAccessContext &context);
  int init_rows_filter(
      const ObTableIterParam &row_param,
      ObTableAccessContext &context,
      ObITable *table);
  int init_project_iter(
      const ObTableIterParam &row_param,
      ObTableAccessContext &context,
      ObITable *table);
  int init_project_iter_for_single_row(
      const ObTableIterParam &row_param,
      ObTableAccessContext &context,
      ObITable *table);
  int extract_group_by_iters();
  int construct_cg_iter_params_for_single_row(
      const ObTableIterParam &row_param,
      ObTableAccessContext &context,
      common::ObIArray<ObTableIterParam*> &iter_params);
  int construct_cg_iter_params(
      const ObTableIterParam &row_param,
      ObTableAccessContext &context,
      common::ObIArray<ObTableIterParam*> &iter_params);
  int construct_cg_agg_iter_params(
      const ObTableIterParam &row_param,
      ObTableAccessContext &context,
      common::ObIArray<ObTableIterParam*> &iter_params);
  int filter_rows(BlockScanState &blockscan_state);
  int filter_rows_with_limit(BlockScanState &blockscan_state);
  int filter_rows_without_limit(BlockScanState &blockscan_state);
  int inner_filter(
      ObCSRowId &begin,
      int64_t &group_size,
      const ObCGBitmap *&result_bitmap,
      BlockScanState &blockscan_state);
  int update_continuous_range(
    ObCSRowId &current_start_row_id,
    const int64_t current_group_size,
    const ObCGBitmap *result_bitmap,
    ObCSRowId &continuous_end_row_id,
    bool &continue_filter);
  int fetch_rows();
  int get_next_group_size(const ObCSRowId begin, int64_t &group_size);
  int check_limit(
      const ObCGBitmap *bitmap,
      bool &limit_end,
      int64_t &begin,
      int64_t &end);
  int fetch_output_rows();
  int filter_group_by_rows();
  int fetch_group_by_rows();
  int do_group_by();
  OB_INLINE bool end_of_scan()
  { return OB_INVALID_CS_ROW_ID == current_ ||
           (reverse_scan_ && current_ < end_) ||
           (!reverse_scan_ && current_ > end_); }
  OB_INLINE bool can_forward_row_scanner()
  { return BLOCKSCAN_RANGE == blockscan_state_; }
  OB_INLINE void update_current(const int64_t count)
  {
    if (reverse_scan_) {
      current_ -= count;
    } else {
      current_ += count;
    }
  }
  OB_INLINE bool is_group_idx_expr(sql::ObExpr *e) const
  {
    return T_PSEUDO_GROUP_ID == e->type_;
  }
protected:
  ObSSTableRowScanner<ObCOPrefetcher> *row_scanner_;
  int32_t range_idx_;
private:
  int init_group_by_info(ObTableAccessContext &context);
  int push_group_by_processor(ObICGIterator *cg_iterator);
  bool is_new_group_;
  bool reverse_scan_;
  bool is_limit_end_;
  ScanState state_;
  BlockScanState blockscan_state_;
  int32_t group_by_project_idx_;
  int64_t group_size_;
  int64_t batch_size_;
  int64_t column_group_cnt_;
  ObCSRowId current_;
  ObCSRowId end_;
  ObCSRowId pending_end_row_id_;
  const ObTableIterParam *iter_param_;
  ObTableAccessContext *access_ctx_;
  ObCOSSTableRowsFilter *rows_filter_;
  ObICGIterator *project_iter_;
  ObICGIterator *getter_project_iter_;
  ObGroupByCell *group_by_cell_;
  ObBlockBatchedRowStore *batched_row_store_;
  ObCGIterParamPool *cg_param_pool_;
  const blocksstable::ObDatumRange *range_;
  ObSEArray<ObICGGroupByProcessor*, 2> group_by_iters_;
  common::ObFixedArray<int32_t, common::ObIAllocator> getter_projector_;
};

}
}

#endif
