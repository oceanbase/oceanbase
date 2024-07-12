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

#ifndef OB_STORAGE_TABLE_ACCESS_PARAM_H
#define OB_STORAGE_TABLE_ACCESS_PARAM_H

#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "common/ob_tablet_id.h"
#include "share/ob_i_tablet_scan.h"
#include "share/schema/ob_table_param.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "ob_global_iterator_pool.h"

namespace oceanbase
{
namespace share{
namespace schema {
class ObTableSchemaParam;
}
}
namespace blocksstable {
class ObSSTable;
}
namespace storage
{
class ObRelativeTable;
class ObSSTableIndexFilter;
static const int64_t AGGREGATE_STORE_BATCH_SIZE = 1024;

// Parameters for row iteration like ObITable get, scan, multi_get, multi_scan
// and ObStoreRowIterator::init
struct ObTableIterParam
{
public:
  ObTableIterParam();
  virtual ~ObTableIterParam();
  void reset();
  bool is_valid() const;
  int refresh_lob_column_out_status();
  bool enable_fuse_row_cache(const ObQueryFlag &query_flag) const;
  //temp solution
  int get_cg_column_param(const share::schema::ObColumnParam *&column_param) const;
  const ObITableReadInfo *get_read_info(const bool is_get = false) const
  {
	  return is_get ? rowkey_read_info_ : read_info_;
  }
  int get_index_read_info(const bool is_cg, const ObITableReadInfo *&index_read_info) const
  {
    int ret = OB_SUCCESS;
    if (is_cg) {
      ret = MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info);
    } else {
      index_read_info = read_info_;
    }
    return ret;
  }
  OB_INLINE int64_t get_out_col_cnt() const
  {
    return read_info_ != nullptr ? read_info_->get_request_count() : 0;
  }
  OB_INLINE int64_t get_full_out_col_cnt() const
  {
    return (rowkey_read_info_ != nullptr) ? rowkey_read_info_->get_request_count() : 0;
  }
  OB_INLINE int64_t get_schema_rowkey_count() const
  {
    return (nullptr != read_info_) ? read_info_->get_schema_rowkey_count() : 0;
  }
  OB_INLINE int64_t get_max_out_col_cnt() const
  {
    return MAX(get_full_out_col_cnt(), get_out_col_cnt());
  }
  OB_INLINE int64_t get_buffered_out_col_cnt() const
  {
    return is_use_global_iter_pool() ? ObGlobalIteratorPool::get_max_col_count() : get_max_out_col_cnt();
  }
  OB_INLINE int64_t get_buffered_request_cnt(const ObITableReadInfo *read_info) const
  {
    return is_use_global_iter_pool() ? ObGlobalIteratorPool::get_max_col_count() : read_info->get_request_count();
  }
  OB_INLINE const ObIArray<share::schema::ObColumnParam *> *get_col_params() const
  {
    return (read_info_ != nullptr) ? read_info_->get_columns() : nullptr;
  }
  OB_INLINE const ObColDescIArray *get_out_col_descs() const
  {
    return (read_info_ != nullptr) ? &read_info_->get_columns_desc() : nullptr;
  }
  OB_INLINE const ObColDescIArray *get_rowkey_col_descs() const
  {
    return (rowkey_read_info_ != nullptr) ? &rowkey_read_info_->get_columns_desc() : nullptr;
  }
  OB_INLINE bool read_with_same_schema() const
  {
    return is_same_schema_column_;
  }
  OB_INLINE int64_t get_group_idx_col_index() const
  {
    return (read_info_ != nullptr && read_info_ != rowkey_read_info_) ? read_info_->get_group_idx_col_index() : common::OB_INVALID_INDEX;
  }
  bool can_be_reused(const uint32_t cg_idx, const common::ObIArray<sql::ObExpr*> &exprs, const bool is_aggregate)
  {
    bool can_reuse = cg_idx == cg_idx_ && enable_pd_aggregate() == is_aggregate
                      && nullptr != output_exprs_ && output_exprs_->count() == exprs.count() ;
    if (can_reuse) {
      for (int64_t i = 0; i < exprs.count(); ++i) {
        if (output_exprs_->at(i) != exprs.at(i)) {
          can_reuse = false;
          break;
        }
      }
    }
    return can_reuse;
  }
  OB_INLINE bool need_fill_group_idx() const
  { return get_group_idx_col_index() != common::OB_INVALID_INDEX; }
  OB_INLINE int64_t get_ss_rowkey_prefix_cnt() const
  { return ss_rowkey_prefix_cnt_; }
  OB_INLINE void disable_blockscan()
  { pd_storage_flag_.set_blockscan_pushdown(false); }
  OB_INLINE bool enable_pd_blockscan() const
  { return pd_storage_flag_.is_blockscan_pushdown(); }
  OB_INLINE bool enable_pd_filter() const
  { return pd_storage_flag_.is_filter_pushdown(); }
  OB_INLINE void disable_pd_aggregate()
  { pd_storage_flag_.set_aggregate_pushdown(false); }
  OB_INLINE bool enable_pd_aggregate() const // just indicate scalar agg
  { return !enable_pd_group_by() && pd_storage_flag_.is_aggregate_pushdown(); }
  OB_INLINE bool enable_pd_group_by() const
  { return pd_storage_flag_.is_group_by_pushdown(); }
  OB_INLINE bool enable_pd_filter_reorder() const
  { return pd_storage_flag_.is_filter_reorder(); }
  OB_INLINE bool enable_skip_index() const
  { return pd_storage_flag_.is_apply_skip_index(); }
  OB_INLINE bool is_use_stmt_iter_pool() const
  { return pd_storage_flag_.is_use_stmt_iter_pool(); }
  OB_INLINE void set_use_stmt_iter_pool()
  { pd_storage_flag_.set_use_stmt_iter_pool(true);}
  OB_INLINE bool has_lob_column_out() const
  { return has_lob_column_out_; }
  OB_INLINE bool is_tablet_spliting() const
  { return is_tablet_spliting_; }
  bool need_trans_info() const;
  OB_INLINE bool is_use_column_store() const
  { return !(get_read_info()->has_all_column_group()) || pd_storage_flag_.is_use_column_store(); }
  OB_INLINE void set_use_column_store()
  { return pd_storage_flag_.set_use_column_store(true); }
  OB_INLINE void set_not_use_column_store()
  { return pd_storage_flag_.set_use_column_store(false); }
  OB_INLINE bool is_use_global_iter_pool() const
  { return pd_storage_flag_.is_use_global_iter_pool(); }
  OB_INLINE void set_use_global_iter_pool()
  { pd_storage_flag_.set_use_global_iter_pool(true); }
  OB_INLINE void diable_use_global_iter_pool()
  { pd_storage_flag_.set_use_global_iter_pool(false); }
  OB_INLINE void set_tablet_handle(const ObTabletHandle *tablet_handle)
  { tablet_handle_ = tablet_handle; }
  OB_INLINE bool use_uniform_format() const
  {
    return op_->enable_rich_format_ &&
        (pd_storage_flag_.is_group_by_pushdown() || pd_storage_flag_.is_aggregate_pushdown());
  }
  OB_INLINE bool use_new_format() const
  {
    return op_->enable_rich_format_ &&
        !pd_storage_flag_.is_group_by_pushdown() &&
        !pd_storage_flag_.is_aggregate_pushdown();
  }
  OB_INLINE int64_t get_io_read_batch_size() const
  { return table_scan_opt_.io_read_batch_size_; }
  OB_INLINE int64_t get_io_read_gap_size() const
  { return table_scan_opt_.io_read_gap_size_; }
  OB_INLINE int64_t get_storage_rowsets_size() const
  { return table_scan_opt_.storage_rowsets_size_; }
  DECLARE_TO_STRING;
public:
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  uint32_t cg_idx_;
  const ObITableReadInfo *read_info_;
  const ObITableReadInfo *rowkey_read_info_;
  const ObTabletHandle *tablet_handle_; //for ddl merge_query
  ObCGReadInfoHandle cg_read_info_handle_;
  //TODO(huronghui.hrh):temp solution
  const ObColumnParam *cg_col_param_;
  const common::ObIArray<ObTableReadInfo *> *cg_read_infos_;
  const common::ObIArray<int32_t> *out_cols_project_;
  const common::ObIArray<int32_t> *agg_cols_project_;
  const common::ObIArray<int32_t> *group_by_cols_project_;
  sql::ObPushdownFilterExecutor *pushdown_filter_;
  sql::ObPushdownOperator *op_;
  ObSSTableIndexFilter *sstable_index_filter_;
  const sql::ObExprPtrIArray *output_exprs_;
  const sql::ObExprPtrIArray *aggregate_exprs_;
  const common::ObIArray<bool> *output_sel_mask_;
  // only used in ObMemTable
  bool is_multi_version_minor_merge_;
  bool need_scn_;
  bool need_trans_info_;
  bool is_same_schema_column_;
  bool vectorized_enabled_;
  bool has_virtual_columns_;
  // use the flag to optimize blockscan for tables with text columns in mysql mode
  // fuse row cache will be disabled when a table contains lob columns
  // so we can generate from the request cols in readinfo without considering fuse row cache
  bool has_lob_column_out_;
  bool is_for_foreign_check_;
  bool limit_prefetch_;
  bool is_mds_query_;
  bool is_non_unique_local_index_;
  int64_t ss_rowkey_prefix_cnt_;
  sql::ObStoragePushdownFlag pd_storage_flag_;
  ObTableScanOption table_scan_opt_;
  uint64_t auto_split_filter_type_;
  const sql::ObExpr *auto_split_filter_;
  sql::ExprFixedArray *auto_split_params_;
  bool is_tablet_spliting_;
};

struct ObTableAccessParam
{
public:
  ObTableAccessParam();
  virtual ~ObTableAccessParam();
  void reset();
  OB_INLINE bool is_valid() const { return is_inited_ && iter_param_.is_valid(); }
  // used for query
  int init(
      const ObTableScanParam &scan_param,
      const ObTabletHandle *tablet_handle,
      const ObITableReadInfo *rowkey_read_info = nullptr);
  // used for merge
  int init_merge_param(const uint64_t table_id,
                       const common::ObTabletID &tablet_id,
                       const ObITableReadInfo &read_info,
                       const bool is_multi_version_merge = false);
  // used for get unique index conflict row
  int init_dml_access_param(const ObRelativeTable &table,
                            const ObITableReadInfo &rowkey_read_info,
                            const share::schema::ObTableSchemaParam &schema_param,
                            const common::ObIArray<int32_t> *out_cols_project);
  int get_prefix_cnt_for_skip_scan(const ObTableScanParam &scan_param, ObTableIterParam &iter_param);
  // used for index back when query
  OB_INLINE int64_t get_out_col_cnt() const
  {
    return is_use_global_iter_pool() ? ObGlobalIteratorPool::get_max_col_count() : iter_param_.get_out_col_cnt();
  }
  OB_INLINE int64_t get_max_out_col_cnt() const
  {
    return is_use_global_iter_pool() ? ObGlobalIteratorPool::get_max_col_count() : iter_param_.get_max_out_col_cnt();
  }
  // get push down operator
  OB_INLINE sql::ObPushdownOperator *get_op() { return iter_param_.op_; }
  OB_INLINE sql::ObPushdownOperator *get_op() const { return iter_param_.op_; }
  OB_INLINE void set_use_global_iter_pool() { iter_param_.set_use_global_iter_pool(); }
  OB_INLINE void diable_use_global_iter_pool() { iter_param_.diable_use_global_iter_pool(); }
  OB_INLINE bool is_use_global_iter_pool() const { return iter_param_.is_use_global_iter_pool(); }
public:
  DECLARE_TO_STRING;
public:
  // 1. Basic Param for Table Iteration
  ObTableIterParam iter_param_;

  // 2. Adjustment Param for Output Rows
  const common::ObIArray<int32_t> *padding_cols_;
  int64_t projector_size_;

  // output for sql static typing engine, NULL for old sql engine scan.
  const sql::ObExprPtrIArray *output_exprs_;
  const sql::ObExprPtrIArray *aggregate_exprs_;
  const sql::ObExprPtrIArray *op_filters_;
  ObRow2ExprsProjector *row2exprs_projector_;
  const common::ObIArray<bool> *output_sel_mask_;
  bool is_inited_;
};

//TODO @hanhui remove this func
int set_row_scn(
    const ObTableIterParam &iter_param,
    const blocksstable::ObDatumRow *store_row);

} // namespace storage
} // namespace oceanbase
#endif
