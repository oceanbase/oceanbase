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

#ifndef OCEANBASE_OB_I_TABLET_SCAN_H_
#define OCEANBASE_OB_I_TABLET_SCAN_H_

#include "common/ob_common_types.h"
#include "common/ob_tablet_id.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "lib/container/ob_array_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/geo/ob_s2adapter.h"
#include "share/ob_i_sql_expression.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/tx/ob_trans_define.h"
#include "sql/engine/cmd/ob_load_data_parser.h"

namespace oceanbase
{
namespace share
{
class ObLSID;
}
namespace sql
{
class ObPushdownFilterExecutor;
class ObExpr;
class ObPushdownOperator;
typedef ObIArray<ObExpr *> ObExprPtrIArray;
typedef ObFixedArray<ObExpr *, ObIAllocator> ExprFixedArray;
}
namespace storage
{
class ObTableScanParam;
class ObRow2ExprsProjector;
}
namespace common
{
class ObTabletID;
class ObNewRowIterator;
class ObNewIterIterator;

struct ObEstRowCountRecord
{
  int64_t table_id_;
  int64_t table_type_;
  ObVersionRange version_range_;
  int64_t logical_row_count_;
  int64_t physical_row_count_;
  TO_STRING_KV(K_(table_id), K_(table_type), K_(version_range), K_(logical_row_count), K_(physical_row_count));
  OB_UNIS_VERSION(1);
};

/** Record sampling information */
struct SampleInfo
{
  SampleInfo() { reset(); }
  enum SampleMethod { NO_SAMPLE = 0, ROW_SAMPLE = 1, BLOCK_SAMPLE = 2 };
  enum SampleScope
  {
    SAMPLE_ALL_DATA = 0,
    SAMPLE_BASE_DATA = 1,
    SAMPLE_INCR_DATA = 2
  };
  bool is_row_sample() const { return ROW_SAMPLE == method_; }
  bool is_block_sample() const { return BLOCK_SAMPLE == method_; }
  bool is_no_sample() const { return NO_SAMPLE == method_; }
  uint64_t hash(uint64_t seed) const;
  void reset()
  {
    table_id_ = OB_INVALID;
    method_ = NO_SAMPLE;
    scope_ = SAMPLE_ALL_DATA;
    percent_ = 100;
    seed_ = -1;
    force_block_ = false;
  }

  uint64_t table_id_;
  SampleMethod method_;
  SampleScope scope_;
  double percent_; // valid value: [0.000001, 100)
  int64_t seed_; // valid value: [0, 4294967296], -1 stands for random seed
  bool force_block_;//force sample block
  TO_STRING_KV(K_(method), K_(percent), K_(seed), K_(table_id), K_(scope), K_(force_block));
  OB_UNIS_VERSION(1);
};

struct ObTableScanOption
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t MAX_IO_READ_BATCH_SIZE = 16_MB;
  static const int64_t MAX_STORAGE_ROWSETS_SIZE = (1 << 20);
  ObTableScanOption() :
      io_read_batch_size_(0),
      io_read_gap_size_(0),
      storage_rowsets_size_(1)
  {}
  bool is_io_valid() const
  {
    return (io_read_batch_size_ >= 0 && io_read_batch_size_ <= MAX_IO_READ_BATCH_SIZE &&
            io_read_gap_size_ >= 0 && io_read_gap_size_ < io_read_batch_size_);
  }
  bool is_rowsets_valid() const
  {
    return storage_rowsets_size_ > 0 && storage_rowsets_size_ <= MAX_STORAGE_ROWSETS_SIZE;
  }
  void reset()
  {
    io_read_batch_size_ = 0;
    io_read_gap_size_ = 0;
    storage_rowsets_size_ = 1;
  }
  ObTableScanOption &operator=(const ObTableScanOption &opt)
  {
    if (this == &opt) {
    } else {
      io_read_batch_size_ = opt.io_read_batch_size_;
      io_read_gap_size_ = opt.io_read_gap_size_;
      storage_rowsets_size_ = opt.storage_rowsets_size_;
    }
    return *this;
  }
  TO_STRING_KV(K_(io_read_batch_size), K_(io_read_gap_size), K_(storage_rowsets_size));

  int64_t io_read_batch_size_;
  int64_t io_read_gap_size_;
  int64_t storage_rowsets_size_;
};

struct ObLimitParam
{
  int64_t offset_;
  int64_t limit_;
  bool is_valid() const { return -1 != limit_; }
  ObLimitParam() : offset_(0), limit_(-1) {}
  TO_STRING_KV("offset_", offset_,
               "limit_", limit_);
  OB_UNIS_VERSION(1);
};

struct ObTableScanStatistic
{
  //storage access row cnt before filter
  int64_t access_row_cnt_;
  //storage output row cnt after filter
  int64_t out_row_cnt_;
  int64_t bf_filter_cnt_;
  int64_t bf_access_cnt_;
  int64_t empty_read_cnt_;
  int64_t fuse_row_cache_hit_cnt_;
  int64_t fuse_row_cache_miss_cnt_;
  int64_t row_cache_hit_cnt_;
  int64_t row_cache_miss_cnt_;
  int64_t block_cache_hit_cnt_;
  int64_t block_cache_miss_cnt_;
  int64_t rowkey_prefix_;
  ObTableScanStatistic()
    : access_row_cnt_(0),
      out_row_cnt_(0),
      bf_filter_cnt_(0),
      bf_access_cnt_(0),
      empty_read_cnt_(0),
      fuse_row_cache_hit_cnt_(0),
      fuse_row_cache_miss_cnt_(0),
      row_cache_hit_cnt_(0),
      row_cache_miss_cnt_(0),
      block_cache_hit_cnt_(0),
      block_cache_miss_cnt_(0),
      rowkey_prefix_(0)
  {}
  OB_INLINE void reset()
  {
    access_row_cnt_ = 0;
    out_row_cnt_ = 0;
    bf_filter_cnt_ = 0;
    bf_access_cnt_ = 0;
    empty_read_cnt_ = 0;
    fuse_row_cache_hit_cnt_ = 0;
    fuse_row_cache_miss_cnt_ = 0;
    row_cache_hit_cnt_ = 0;
    row_cache_miss_cnt_ = 0;
    block_cache_hit_cnt_ = 0;
    block_cache_miss_cnt_ = 0;
    rowkey_prefix_ = 0;
  }
  OB_INLINE void reset_cache_stat()
  {
    bf_filter_cnt_ = 0;
    bf_access_cnt_ = 0;
    fuse_row_cache_hit_cnt_ = 0;
    fuse_row_cache_miss_cnt_ = 0;
    row_cache_hit_cnt_ = 0;
    row_cache_miss_cnt_ = 0;
    block_cache_hit_cnt_ = 0;
    block_cache_miss_cnt_ = 0;
  }
  TO_STRING_KV(
      K_(access_row_cnt),
      K_(out_row_cnt),
      K_(bf_filter_cnt),
      K_(bf_access_cnt),
      K_(empty_read_cnt),
      K_(row_cache_hit_cnt),
      K_(row_cache_miss_cnt),
      K_(fuse_row_cache_hit_cnt),
      K_(fuse_row_cache_miss_cnt),
      K_(rowkey_prefix));
};

static const int64_t OB_DEFAULT_FILTER_EXPR_COUNT = 4;
static const int64_t OB_DEFAULT_RANGE_COUNT = 4;
static const int64_t OB_DEFAULT_MBR_FILTER_COUNT = 1;
typedef ObSEArray<ObISqlExpression*, OB_DEFAULT_FILTER_EXPR_COUNT, ModulePageAllocator> ObFilterArray;
typedef ObSEArray<const ObIColumnExpression*, 4, ModulePageAllocator> ObColumnExprArray;
typedef ObSEArray<ObNewRange, OB_DEFAULT_RANGE_COUNT, ModulePageAllocator> ObRangeArray;
typedef ObSEArray<int64_t, OB_DEFAULT_RANGE_COUNT, ModulePageAllocator> ObPosArray;
typedef ObSEArray<uint64_t, OB_PREALLOCATED_COL_ID_NUM, ModulePageAllocator> ObColumnIdArray;
typedef common::ObSEArray<common::ObSpatialMBR, OB_DEFAULT_MBR_FILTER_COUNT> ObMbrFilterArray;

/**
 *  This is the common interface for storage service.
 *
 *  So far there are three components that implement the interface:
 *    1. partition storage
 *    2. virtual table
 *    3. external table
 */
class ObVTableScanParam
{
public:

ObVTableScanParam() :
      tenant_id_(OB_INVALID_ID),
      index_id_(OB_INVALID_ID),
      timeout_(-1),
      sql_mode_(SMO_DEFAULT),
      reserved_cell_count_(-1),
      schema_version_(-1),
      tenant_schema_version_(-1),
      projector_size_(0),
      for_update_(false),
      for_update_wait_timeout_(-1),
      frozen_version_(-1),
      scan_allocator_(&CURRENT_CONTEXT->get_arena_allocator()),
      fb_snapshot_(),
      is_get_(false),
      force_refresh_lc_(false),
      is_for_foreign_check_(false),
      output_exprs_(NULL),
      calc_exprs_(NULL),
      aggregate_exprs_(NULL),
      op_(NULL),
      op_filters_(NULL),
      pd_storage_filters_(nullptr),
      pd_storage_flag_(false),
      row2exprs_projector_(NULL),
      table_scan_opt_(),
      ext_file_column_exprs_(NULL),
      ext_column_convert_exprs_(NULL),
      schema_guard_(NULL),
      auto_split_filter_type_(OB_INVALID_ID),
      auto_split_filter_(NULL),
      auto_split_params_(NULL),
      is_tablet_spliting_(false)
  { }

  virtual ~ObVTableScanParam()
  {
    destroy_schema_guard();
  }

  void destroy()
  {
    if (OB_UNLIKELY(column_ids_.get_capacity() > OB_PREALLOCATED_COL_ID_NUM)) {
      column_ids_.destroy();
    }
    if (OB_UNLIKELY(key_ranges_.get_capacity() > OB_DEFAULT_RANGE_COUNT)) {
      key_ranges_.destroy();
    }
    if (OB_UNLIKELY(range_array_pos_.get_capacity() > OB_DEFAULT_RANGE_COUNT)) {
      range_array_pos_.destroy();
    }
    destroy_schema_guard();
  }
  ObObjectID tenant_id_;
  // data tablet id
  ObTabletID tablet_id_;
  // log stream id
  share::ObLSID ls_id_;
  // columns to output
  ObColumnIdArray column_ids_; // output column(s)
  //ObColDescArray column_descs_;
  uint64_t index_id_;           // index to be used
  //ranges of all range array, no index key range means full partition scan
  ObRangeArray key_ranges_;
  ObMbrFilterArray mbr_filters_;
  // remember the end position of each range array, array size of (0 or 1) represents there is only one range array(for most cases except blocked nested loop join)
  ObPosArray range_array_pos_;
  int64_t timeout_;             // process timeout
  ObQueryFlag scan_flag_;       // read consistency, cache policy, result ordering etc.
  ObSQLMode sql_mode_;  // sql mode
  int64_t reserved_cell_count_; // reserved cell of output row
  int64_t schema_version_;
  int64_t tenant_schema_version_; // Get the latest global schema version when Query starts
  int64_t projector_size_;
  ObLimitParam limit_param_;
  bool      for_update_;       // FOR UPDATE clause
  int64_t   for_update_wait_timeout_; // Positive value: the absolute time to wait for the lock to end, beyond this time,
  //       Return to lock failure
  // 0: lock without waiting, try lock
  // Negative value: do not set the lock end time until the lock is successful.
  int64_t frozen_version_;  // the frozen version to read
  // storage scan/rescan interface level allocator, will be reclaimed in every scan/rescan call
  ObIAllocator *scan_allocator_;
  ObTableScanStatistic main_table_scan_stat_;
  ObTableScanStatistic idx_table_scan_stat_;
  share::SCN fb_snapshot_;
  bool is_get_;
  bool force_refresh_lc_;
  bool is_for_foreign_check_;

  //
  // for static typing engine, set to NULL if the old engine is used
  //
  // output column expressions, same size as %column_ids_.
  const sql::ExprFixedArray *output_exprs_;
  const sql::ExprFixedArray *calc_exprs_;
  // aggregate expressions, for calculating aggregation in storage layer.
  const sql::ExprFixedArray *aggregate_exprs_;
  sql::ObPushdownOperator *op_;
  const sql::ObExprPtrIArray *op_filters_;
  sql::ObPushdownFilterExecutor *pd_storage_filters_;
  int32_t pd_storage_flag_;
  // project storage output row to %output_exprs_
  storage::ObRow2ExprsProjector *row2exprs_projector_;
  ObTableScanOption table_scan_opt_;

  // external table
  const sql::ExprFixedArray *ext_file_column_exprs_;
  const sql::ExprFixedArray *ext_column_convert_exprs_;
  sql::ObExternalFileFormat external_file_format_;
  ObString external_file_location_;
  ObString external_file_access_info_;

  virtual bool is_valid() const {
    return (tablet_id_.is_valid()
            && OB_INVALID_ID != index_id_
            && timeout_ > 0
            && reserved_cell_count_ >= column_ids_.count()
            && schema_version_ >= 0 );
  }

  bool is_estimate_valid() const {
    return (tablet_id_.is_valid()
            && ls_id_.is_valid()
            && OB_INVALID_ID != index_id_
            && schema_version_ >= 0);
  }

  share::schema::ObSchemaGetterGuard &get_schema_guard()
  {
    if (OB_UNLIKELY(NULL == schema_guard_)) {
      schema_guard_ = new (schema_guard_buf_) share::schema::ObSchemaGetterGuard(share::schema::ObSchemaMgrItem::MOD_VTABLE_SCAN_PARAM);
    }
    return *schema_guard_;
  }

  const share::schema::ObSchemaGetterGuard &get_schema_guard() const
  {
    return const_cast<ObVTableScanParam *>(this)->get_schema_guard();
  }

  void destroy_schema_guard()
  {
    call_dtor(schema_guard_);
  }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // New schema, used throughout the life cycle of table_scan
  share::schema::ObSchemaGetterGuard *schema_guard_;
  char schema_guard_buf_[sizeof(share::schema::ObSchemaGetterGuard)];

public:
  uint64_t auto_split_filter_type_;
  const sql::ObExpr *auto_split_filter_;
  sql::ExprFixedArray *auto_split_params_;
  bool is_tablet_spliting_;
};

class ObITabletScan
{
public:
  // OLD interface, remove below ones after SQL fully adopt
  virtual int table_rescan(ObVTableScanParam &param, ObNewIterIterator *&result)
  {
    UNUSEDx(param, result);
    return OB_NOT_SUPPORTED;
  }
  virtual int table_scan(ObVTableScanParam &param, ObNewIterIterator *&result)
  {
    UNUSEDx(param, result);
    return OB_NOT_SUPPORTED;
  }
  virtual int reuse_scan_iter(const bool switch_param, ObNewIterIterator *iter)
  {
    UNUSEDx(switch_param, iter);
    return OB_NOT_SUPPORTED;
  }

  // NEW interface
  virtual int table_scan(ObVTableScanParam &param, ObNewRowIterator *&result)
  {
    UNUSEDx(param, result);
    return OB_NOT_SUPPORTED;
  }

  virtual int table_rescan(ObVTableScanParam &param, ObNewRowIterator *result)
  {
    UNUSEDx(param, result);
    return OB_NOT_SUPPORTED;
  }

  virtual int reuse_scan_iter(const bool switch_param, ObNewRowIterator *iter)
  {
    UNUSEDx(switch_param, iter);
    return OB_NOT_SUPPORTED;
  }

  virtual int revert_scan_iter(ObNewRowIterator *iter)
  {
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  virtual int get_multi_ranges_cost(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      const common::ObIArray<ObStoreRange> &ranges,
      int64_t &total_size)
  {
    UNUSEDx(ls_id, tablet_id, timeout_us, ranges, total_size);
    return OB_SUCCESS;
  }

  virtual int split_multi_ranges(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us,
      const common::ObIArray<ObStoreRange> &ranges,
      const int64_t expected_task_count,
      ObIAllocator &allocator,
      ObArrayArray<ObStoreRange> &multi_range_split_array)
  {
    UNUSEDx(ls_id, tablet_id, timeout_us, ranges, expected_task_count, allocator, multi_range_split_array);
    return OB_SUCCESS;
  }
};
}
}
#endif
