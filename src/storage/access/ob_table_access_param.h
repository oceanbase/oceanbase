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

enum ObFastAggregationType
{
  INVALID_AGG_TYPE = 0,
  AGG_COUNT_ALL,
  AGG_COUNT_COL,
  AGG_MAX,
  AGG_MIN,
};

struct ObFastAggProjectCell
{
  ObFastAggProjectCell() : agg_project_(0) {}
  ~ObFastAggProjectCell() { reset(); }
  void reset() { agg_project_ = 0; }
  inline void set_project_id(const uint32_t id) { project_id_ = id & OBSF_MASK_AGG_PROJECT; }
  inline void set_type(const ObFastAggregationType type) { type_ = type & OBSF_MASK_AGG_TYPE; }
  inline int32_t get_project_id() const { return project_id_; }
  inline ObFastAggregationType get_type() const { return static_cast<ObFastAggregationType>(type_); }
  TO_STRING_KV(K_(agg_project), K_(project_id), K_(type));

  static const uint32_t OB_AGG_PROJECT_ID = 16;
  static const uint32_t OB_AGG_TYPE = 8;
  static const uint32_t OB_AGG_RESERVED = 8;

  static const uint32_t OBSF_MASK_AGG_PROJECT = (0x1UL << OB_AGG_PROJECT_ID) - 1;
  static const uint32_t OBSF_MASK_AGG_TYPE = (0x1UL << OB_AGG_TYPE) - 1;
  union
  {
    int32_t agg_project_;
    struct
    {
      int32_t project_id_ : OB_AGG_PROJECT_ID;
      uint32_t type_ : OB_AGG_TYPE;
      uint32_t reserved_ : OB_AGG_RESERVED;
    };
  };
};

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
  const ObITableReadInfo *get_read_info(const bool is_get = false) const
  {
	  return is_get ? rowkey_read_info_ : read_info_;
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
  OB_INLINE const ObIArray<share::schema::ObColumnParam *> *get_col_params() const
  {
    return (read_info_ != nullptr) ? read_info_->get_columns() : nullptr;
  }
  OB_INLINE const ObColDescIArray *get_out_col_descs() const
  {
    return (read_info_ != nullptr) ? &read_info_->get_columns_desc() : nullptr;
  }
  OB_INLINE bool read_with_same_schema() const
  {
    return is_same_schema_column_;
  }
  OB_INLINE int64_t get_group_idx_col_index() const
  {
    return (read_info_ != nullptr && read_info_ != rowkey_read_info_) ? read_info_->get_group_idx_col_index() : common::OB_INVALID_INDEX;
  }
  OB_INLINE bool need_fill_group_idx() const
  { return get_group_idx_col_index() != common::OB_INVALID_INDEX; }
  OB_INLINE int64_t get_ss_rowkey_prefix_cnt() const
  { return ss_rowkey_prefix_cnt_; }
  OB_INLINE void disable_blockscan()
  { pd_blockscan_ = 0; }
  OB_INLINE bool enable_pd_blockscan() const
  { return pd_blockscan_; }
  OB_INLINE bool enable_pd_filter() const
  { return  pd_filter_; }
  OB_INLINE bool enable_pd_aggregate() const
  { return pd_aggregate_; }
  OB_INLINE bool is_use_iter_pool() const
  { return use_iter_pool_; }
  OB_INLINE void set_use_iter_pool_flag()
  { use_iter_pool_ = 1; }
  OB_INLINE bool has_lob_column_out() const
  { return has_lob_column_out_; }
  bool need_trans_info() const;
  DECLARE_TO_STRING;
public:
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  const ObITableReadInfo *read_info_;
  const ObITableReadInfo *rowkey_read_info_;
  const common::ObIArray<int32_t> *out_cols_project_;
  const common::ObIArray<int32_t> *agg_cols_project_;
  sql::ObPushdownFilterExecutor *pushdown_filter_;
  // only used in ObMemTable
  bool is_multi_version_minor_merge_;
  bool need_scn_;
  bool is_same_schema_column_;
  bool vectorized_enabled_;
  bool has_virtual_columns_;
  // use the flag to optimize blockscan for tables with text columns in mysql mode
  // fuse row cache will be disabled when a table contains lob columns
  // so we can generate from the request cols in readinfo without considering fuse row cache
  bool has_lob_column_out_;
  bool is_for_foreign_check_;
  bool limit_prefetch_;
  int64_t ss_rowkey_prefix_cnt_;
  sql::ObPushdownOperator *op_;
  union {
    struct {
      int32_t pd_blockscan_:1;
      int32_t pd_filter_:1;
      int32_t pd_aggregate_:1;
      int32_t use_iter_pool_:1;
      int32_t reserved_:28;
    };
    int32_t pd_storage_flag_;
  };
};

struct ObTableAccessParam
{
public:
  ObTableAccessParam();
  virtual ~ObTableAccessParam();
  void reset();
  OB_INLINE bool is_valid() const { return is_inited_ && iter_param_.is_valid(); }
  // used for query
  int init(const ObTableScanParam &scan_param, const ObTabletHandle &tablet_handle);
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
  OB_INLINE int64_t get_out_col_cnt() const { return iter_param_.get_out_col_cnt(); }
  OB_INLINE int64_t get_max_out_col_cnt() const { return iter_param_.get_max_out_col_cnt(); }
  // get push down operator
  OB_INLINE sql::ObPushdownOperator *get_op() { return iter_param_.op_; }
  OB_INLINE sql::ObPushdownOperator *get_op() const { return iter_param_.op_; }
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

  // for fast agg project
  const common::ObIArray<ObFastAggProjectCell> *fast_agg_project_;

  bool is_inited_;
};

//TODO @hanhui remove this func
int set_row_scn(
    const ObTableIterParam &iter_param,
    const blocksstable::ObSSTable &sstable,
    const blocksstable::ObDatumRow *store_row);

} // namespace storage
} // namespace oceanbase
#endif
