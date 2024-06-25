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

#ifndef OCEANBASE_STORAGE_OB_MULTIPLE_MERGE_
#define OCEANBASE_STORAGE_OB_MULTIPLE_MERGE_

#include "lib/statistic_event/ob_stat_event.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "storage/ob_i_store.h"
#include "storage/ob_row_fuse.h"
#include "ob_store_row_iterator.h"
#include "share/schema/ob_table_param.h"
#include "ob_table_scan_range.h"
#include "storage/tablet/ob_table_store_util.h"
#include "lib/stat/ob_diagnose_info.h"
#include "ob_table_access_context.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/lob/ob_lob_data_reader.h"

namespace oceanbase
{
namespace storage
{
class ObBlockRowStore;
class ObMultipleMerge : public ObQueryRowIterator
{
public:
  typedef common::ObSEArray<ObStoreRowIterator *, 8> MergeIterators;
public:

  ObMultipleMerge();
  virtual ~ObMultipleMerge();
  virtual int init(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param);
  virtual int switch_param(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param);
  virtual int switch_table(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param);
  virtual int get_next_row(blocksstable::ObDatumRow *&row);
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset();
  virtual void reuse();
  // used for global cached query iterator
  virtual void reclaim();

  void disable_padding() { need_padding_ = false; }
  void disable_fill_default() { need_fill_default_ = false; }
  void disable_fill_virtual_column() { need_fill_virtual_columns_ = false; }
  void disable_output_row_with_nop() { need_output_row_with_nop_ = false; }
  OB_INLINE bool is_inited() { return inited_; }
  OB_INLINE bool is_read_memtable_only() const { return read_memtable_only_; }
  OB_INLINE const common::ObIArray<share::schema::ObColDesc> &get_out_project_cells() { return out_project_cols_; }

protected:
  int open();
  virtual int calc_scan_range() = 0;
  virtual int construct_iters() = 0;
  virtual int is_range_valid() const = 0;
  virtual OB_INLINE int prepare() { return common::OB_SUCCESS; }
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row) = 0;
  virtual int inner_get_next_rows() { return OB_SUCCESS; };
  virtual int can_batch_scan(bool &can_batch) { can_batch = false; return OB_SUCCESS; }
  int add_iterator(ObStoreRowIterator &iter); // for unit test
  const ObTableIterParam * get_actual_iter_param(const ObITable *table) const;
  int project_row(const blocksstable::ObDatumRow &unprojected_row,
      const common::ObIArray<int32_t> *projector,
      const int64_t range_idx_delta,
      blocksstable::ObDatumRow &projected_row);
  void reset_iter_array(const bool can_reuse = false);
  void reuse_iter_array();
  void reclaim_iter_array();
  int handle_4377(const char* func);
  void dump_tx_statistic_for_4377(ObStoreCtx *store_ctx);
  void dump_table_statistic_for_4377();
  int set_base_version() const;
private:
  int get_next_normal_row(blocksstable::ObDatumRow *&row);
  int get_next_normal_rows(int64_t &count, int64_t capacity);
  int get_next_aggregate_row(blocksstable::ObDatumRow *&row);
  int fuse_default(blocksstable::ObDatumRow &row);
  int fill_lob_locator(blocksstable::ObDatumRow &row);
  int fuse_lob_default(ObObj &def_cell, const uint64_t col_id);
  int pad_columns(blocksstable::ObDatumRow &row);
  int fill_virtual_columns(blocksstable::ObDatumRow &row);
  int project_row(const blocksstable::ObDatumRow &unprojected_row, blocksstable::ObDatumRow &projected_row);
  // project to output expressions
  int project2output_exprs(blocksstable::ObDatumRow &unprojected_row, blocksstable::ObDatumRow &cur_row);
  int prepare_read_tables(bool refresh = false);
  int prepare_mds_tables(bool refresh);
  int prepare_tables_from_iterator(ObTableStoreIterator &table_iter, const common::SampleInfo *sample_info = nullptr);
  int refresh_table_on_demand();
  int refresh_tablet_iter();
  OB_INLINE int check_need_refresh_table(bool &need_refresh);
  int save_curr_rowkey();
  int reset_tables();
  int check_filtered(const blocksstable::ObDatumRow &row, bool &filtered);
  int alloc_row_store(ObTableAccessContext &context, const ObTableAccessParam &param);
  int process_fuse_row(const bool not_using_static_engine,
                       blocksstable::ObDatumRow &in_row,
                       blocksstable::ObDatumRow *&out_row);
  int fill_group_idx_if_need(blocksstable::ObDatumRow &row);
  int init_lob_reader(const ObTableIterParam &iter_param,
                     ObTableAccessContext &access_ctx);
  int read_lob_columns_full_data(blocksstable::ObDatumRow &row);
  bool need_read_lob_columns(const blocksstable::ObDatumRow &row);
  int handle_lob_before_fuse_row();
  void reuse_lob_locator();
  void report_tablet_stat();
  int update_and_report_tablet_stat();
  void inner_reset();

protected:
  common::ObArenaAllocator padding_allocator_;
  MergeIterators iters_;
  ObTableAccessParam *access_param_;
  ObTableAccessContext *access_ctx_;
  common::ObSEArray<storage::ObITable *, common::DEFAULT_STORE_CNT_IN_STORAGE> tables_;
  blocksstable::ObDatumRow cur_row_;
  blocksstable::ObDatumRow unprojected_row_;
  int64_t curr_scan_index_;
  blocksstable::ObDatumRowkey curr_rowkey_;
  ObNopPos nop_pos_;
  ObRowStat row_stat_;
  int64_t scan_cnt_;
  bool need_padding_;
  bool need_fill_default_; // disabled by join mv scan
  bool need_fill_virtual_columns_; // disabled by join mv scan
  bool need_output_row_with_nop_; // for sampling increment data
  bool inited_;
  int64_t range_idx_delta_;
  ObGetTableParam *get_table_param_;
  bool read_memtable_only_;
  ObBlockRowStore *block_row_store_;
  ObGroupByCell *group_by_cell_;
  sql::ObBitVector *skip_bit_;
  ObIAllocator *long_life_allocator_; // used for memory which will be chached in ObGlobalIterPool
  ObStoreRowIterPool<ObStoreRowIterator> *stmt_iter_pool_;
  common::ObSEArray<share::schema::ObColDesc, 32> out_project_cols_;
  ObLobDataReader lob_reader_;
private:
  enum ScanState
  {
    NONE,
    SINGLE_ROW,
    BATCH,
  };
  ScanState scan_state_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleMerge);
};

OB_INLINE int ObMultipleMerge::check_need_refresh_table(bool &need_refresh)
{
  int ret = OB_SUCCESS;

  if (access_param_->iter_param_.is_mds_query_) {
    need_refresh = false;
  } else {
    need_refresh = get_table_param_->tablet_iter_.table_iter()->check_store_expire();
  }
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_FORCE_REFRESH_TABLE) ret;
  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    need_refresh = true;
  }
#endif
  return ret;
}

}
}

#endif // OCEANBASE_STORAGE_OB_MULTIPLE_MERGE_
