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

#ifndef OCEANBASE_OB_I_DATA_ACCESS_SERVICE_H_
#define OCEANBASE_OB_I_DATA_ACCESS_SERVICE_H_

#include "lib/container/ob_array_array.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_i_sql_expression.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/ob_common_types.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace share {
namespace schema {}
}  // namespace share

namespace sql {
class ObOperator;
class ObPushdownFilterExecutor;
class ObExpr;
typedef common::ObIArray<ObExpr*> ObExprPtrIArray;
}  // namespace sql

namespace storage {
class ObTableScanParam;
class ObRow2ExprsProjector;
}  // namespace storage
namespace common {
struct ObEstRowCountRecord {
  int64_t table_id_;
  int64_t table_type_;
  common::ObVersionRange version_range_;
  int64_t logical_row_count_;
  int64_t physical_row_count_;
  TO_STRING_KV(K_(table_id), K_(table_type), K_(version_range), K_(logical_row_count), K_(physical_row_count));
  OB_UNIS_VERSION(1);
};

/** Record sampling information */
struct SampleInfo {
  SampleInfo()
  {
    reset();
  }
  enum SampleMethod { NO_SAMPLE = 0, ROW_SAMPLE = 1, BLOCK_SAMPLE = 2 };
  enum SampleScope { SAMPLE_ALL_DATA = 0, SAMPLE_BASE_DATA = 1, SAMPLE_INCR_DATA = 2 };
  bool is_row_sample() const
  {
    return ROW_SAMPLE == method_;
  }
  bool is_block_sample() const
  {
    return BLOCK_SAMPLE == method_;
  }
  bool is_no_sample() const
  {
    return NO_SAMPLE == method_;
  }
  uint64_t hash(uint64_t seed) const;
  void reset()
  {
    table_id_ = OB_INVALID;
    method_ = NO_SAMPLE;
    scope_ = SAMPLE_ALL_DATA;
    percent_ = 100;
    seed_ = -1;
  }

  uint64_t table_id_;
  SampleMethod method_;
  SampleScope scope_;
  double percent_;  // valid value: [0.000001, 100)
  int64_t seed_;    // valid value: [0, 4294967296], -1 stands for random seed
  TO_STRING_KV(K_(method), K_(percent), K_(seed), K_(table_id), K_(scope));
  OB_UNIS_VERSION(1);
};

struct ObLimitParam {
  int64_t offset_;
  int64_t limit_;
  bool is_valid() const
  {
    return -1 != limit_;
  }
  ObLimitParam() : offset_(0), limit_(-1)
  {}
  TO_STRING_KV("offset_", offset_, "limit_", limit_);
  OB_UNIS_VERSION(1);
};

struct ObTableScanStatistic {
  // storage access row cnt before filter
  int64_t access_row_cnt_;
  // storage output row cnt after filter
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
  TO_STRING_KV(K_(access_row_cnt), K_(out_row_cnt), K_(bf_filter_cnt), K_(bf_access_cnt), K_(empty_read_cnt),
      K_(row_cache_hit_cnt), K_(row_cache_miss_cnt), K_(fuse_row_cache_hit_cnt), K_(fuse_row_cache_miss_cnt),
      K_(rowkey_prefix));
};

static const int64_t OB_DEFAULT_FILTER_EXPR_COUNT = 4;
static const int64_t OB_DEFAULT_RANGE_COUNT = 4;
typedef common::ObSEArray<common::ObISqlExpression*, OB_DEFAULT_FILTER_EXPR_COUNT, common::ModulePageAllocator, true>
    ObFilterArray;
typedef common::ObSEArray<const common::ObIColumnExpression*, 4, common::ModulePageAllocator, true> ObColumnExprArray;
typedef common::ObSEArray<common::ObNewRange, OB_DEFAULT_RANGE_COUNT, common::ModulePageAllocator, true> ObRangeArray;
typedef common::ObSEArray<int64_t, OB_DEFAULT_RANGE_COUNT, common::ModulePageAllocator, true> ObPosArray;
typedef common::ObSEArray<uint64_t, OB_PREALLOCATED_COL_ID_NUM, common::ModulePageAllocator, true> ObColumnIdArray;

/**
 *  This is the common interface for storage service.
 *
 *  So far there are two components that implement the interface:
 *    1. partition storage
 *    2. virtual table
 */
class ObVTableScanParam {
public:
  ObVTableScanParam()
      : index_id_(common::OB_INVALID_ID),
        timeout_(-1),
        sql_mode_(SMO_DEFAULT),
        reserved_cell_count_(-1),
        schema_version_(-1),
        query_begin_schema_version_(-1),
        index_projector_(NULL),
        projector_size_(0),
        for_update_(false),
        for_update_wait_timeout_(-1),
        frozen_version_(-1),
        scan_allocator_(NULL),
        flashback_query_snapshot_version_(transaction::ObTransVersion::INVALID_TRANS_VERSION),
        is_get_(false),
        force_refresh_lc_(false),
        output_exprs_(NULL),
        op_(NULL),
        op_filters_(NULL),
        op_filters_before_index_back_(NULL),
        pd_storage_filters_(nullptr),
        pd_storage_index_back_filters_(nullptr),
        pd_storage_flag_(false),
        row2exprs_projector_(NULL),
        schema_guard_(NULL)
  {}

  virtual ~ObVTableScanParam()
  {
    destroy_schema_guard();
  }

  void destroy()
  {
    if (OB_UNLIKELY(column_ids_.count() > OB_PREALLOCATED_COL_ID_NUM)) {
      column_ids_.destroy();
    }
    if (OB_UNLIKELY(key_ranges_.count() > OB_DEFAULT_RANGE_COUNT)) {
      key_ranges_.destroy();
    }
    if (OB_UNLIKELY(range_array_pos_.count() > OB_DEFAULT_RANGE_COUNT)) {
      range_array_pos_.destroy();
    }
    if (OB_UNLIKELY(filters_.count() > OB_DEFAULT_FILTER_EXPR_COUNT)) {
      filters_.destroy();
    }
    if (OB_UNLIKELY(filters_before_index_back_.count() > OB_DEFAULT_FILTER_EXPR_COUNT)) {
      filters_before_index_back_.destroy();
    }
    destroy_schema_guard();
  }

  common::ObPartitionKey pkey_;  // partition key
  // During the table scan process, the value is assigned by the storage layer
  common::ObPGKey pg_key_;  // pg key
  // columns to output
  ObColumnIdArray column_ids_;  // output column(s)
  // ObColDescArray column_descs_;
  uint64_t index_id_;  // index to be used
  // ranges of all range array, no index key range means full partition scan
  ObRangeArray key_ranges_;
  // remember the end position of each range array, array size of (0 or 1) represents there is only one range array(for
  // most cases except blocked nested loop join)
  ObPosArray range_array_pos_;
  int64_t timeout_;              // process timeout
  ObQueryFlag scan_flag_;        // read consistency, cache policy, result ordering etc.
  ObSQLMode sql_mode_;           // sql mode
  int64_t reserved_cell_count_;  // reserved cell of output row
  int64_t schema_version_;
  int64_t query_begin_schema_version_;  // Get the latest global schema version when Query starts
  ObFilterArray filters_;
  ObFilterArray filters_before_index_back_;
  ObColumnExprArray virtual_column_exprs_;
  int32_t* index_projector_;
  int64_t projector_size_;
  common::ObExprCtx expr_ctx_;
  ObLimitParam limit_param_;
  bool for_update_;                  // FOR UPDATE clause
  int64_t for_update_wait_timeout_;  // Positive value: the absolute time to wait for the lock to end, beyond this time,
  //       Return to lock failure
  // 0: lock without waiting, try lock
  // Negative value: do not set the lock end time until the lock is successful.
  ObVersion frozen_version_;      // the frozen version to read
  ObIAllocator* scan_allocator_;  // Available through the lifetime of scan, so far only used
                                  // to create virtual table iterator
  ObTableScanStatistic main_table_scan_stat_;
  ObTableScanStatistic idx_table_scan_stat_;
  int64_t flashback_query_snapshot_version_;
  bool is_get_;
  bool force_refresh_lc_;

  //
  // for static typing engine, set to NULL if the old engine is used
  //
  // output column expressions, same size as %column_ids_.
  const sql::ObExprPtrIArray* output_exprs_;
  sql::ObOperator* op_;
  const sql::ObExprPtrIArray* op_filters_;
  const sql::ObExprPtrIArray* op_filters_before_index_back_;
  sql::ObPushdownFilterExecutor* pd_storage_filters_;
  sql::ObPushdownFilterExecutor* pd_storage_index_back_filters_;
  int32_t pd_storage_flag_;
  // project storage output row to %output_exprs_
  storage::ObRow2ExprsProjector* row2exprs_projector_;

  virtual bool is_valid() const
  {
    return (pkey_.is_valid() && column_ids_.count() > 0 && common::OB_INVALID_ID != index_id_ && timeout_ > 0 &&
            reserved_cell_count_ >= column_ids_.count() && schema_version_ >= 0 &&
            (filters_.count() > 0 ? NULL != expr_ctx_.phy_plan_ctx_ : true) &&
            (scan_flag_.is_index_back() ? pkey_.get_table_id() != index_id_ : true));
  }

  virtual bool is_index_limit() const;

  bool is_estimate_valid() const
  {
    return (pkey_.is_valid() && column_ids_.count() > 0 && common::OB_INVALID_ID != index_id_ && schema_version_ >= 0);
  }

  share::schema::ObSchemaGetterGuard& get_schema_guard()
  {
    if (OB_UNLIKELY(NULL == schema_guard_)) {
      schema_guard_ = new (schema_guard_buf_) share::schema::ObSchemaGetterGuard;
    }
    return *schema_guard_;
  }

  const share::schema::ObSchemaGetterGuard& get_schema_guard() const
  {
    return const_cast<ObVTableScanParam*>(this)->get_schema_guard();
  }

  void destroy_schema_guard()
  {
    call_dtor(schema_guard_);
  }

  TO_STRING_KV(N_KEY, pkey_, N_PG_KEY, pg_key_, N_COLUMN_IDS, column_ids_, N_INDEX_ID, index_id_, N_KEY_RANGES,
      key_ranges_, N_TIMEOUT, timeout_, N_SCAN_FLAG, scan_flag_, N_SQL_MODE, sql_mode_, N_RESERVED_CELL_COUNT,
      reserved_cell_count_, N_SCHEMA_VERSION, schema_version_, N_QUERY_BEGIN_SCHEMA_VERSION,
      query_begin_schema_version_, N_FILTER_EXPRS, filters_, N_INDEX_FILTER_EXPRS, filters_before_index_back_,
      N_LIMIT_OFFSET, limit_param_, N_FOR_UPDATE, for_update_, N_WAIT, for_update_wait_timeout_, N_FROZEN_VERSION,
      frozen_version_, K_(is_get));

private:
  // New schema, used throughout the life cycle of table_scan
  share::schema::ObSchemaGetterGuard* schema_guard_;
  char schema_guard_buf_[sizeof(share::schema::ObSchemaGetterGuard)];
};
// @FIXME
class ObNewRowIterator;
class ObNewIterIterator;
class ObIDataAccessService {
public:
  //
  // table scan
  //     scan table partition
  //
  // @param parm [in] scan param
  // @param result [out] result iterator
  //
  // @retval OB_TRANS_CTX_MGR_NOT_EXIST
  // @retval OB_TRANS_CTX_NOT_EXIST
  // @retval OB_PARTITION_IS_FROZEN
  // @retval OB_NOT_MASTER
  // @retval OB_TRANS_CTX_ALLOC_ERROR
  // @retval OB_NOT_INIT
  // @retval OB_INIT_TWICE
  // @retval OB_INVALID_ARGUMENT
  // @retval OB_ALLOCATE_MEMORY_FAILED
  // @retval OB_SCHEMA_ERROR
  // @retval OB_ENTRY_EXIST
  // @retval OB_ENTRY_NOT_EXIST
  //
  virtual int table_scan(ObVTableScanParam& param, ObNewRowIterator*& result) = 0;

  virtual int table_scan(ObVTableScanParam& param, ObNewIterIterator*& result)
  {
    UNUSEDx(param, result);
    return OB_NOT_SUPPORTED;
  }

  // join materialized view scan
  virtual int join_mv_scan(storage::ObTableScanParam& left_param, storage::ObTableScanParam& right_param,
      common::ObNewRowIterator*& result) = 0;

  //
  // revert scan iterator
  //     revert iterator returned by table scan
  //
  // @param iter [in/out] the interator to be reverted
  //
  // @retval OB_INVALID_ARGUMENT
  // @retval OB_TRANS_CTX_MGR_NOT_EXIST
  // @retval OB_TRANS_CTX_NOT_EXIST
  // @retval OB_PARTITION_IS_FROZEN
  // @retval OB_NOT_MASTER
  // @retval OB_TRANS_CTX_ALLOC_ERROR
  // @retval OB_NOT_INIT
  virtual int revert_scan_iter(ObNewRowIterator* iter) = 0;

  virtual int revert_scan_iter(ObNewIterIterator* iter)
  {
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  virtual int query_range_to_macros(ObIAllocator& allocator, const ObPartitionKey& pkey,
      const ObIArray<ObStoreRange>& ranges, const int64_t type, uint64_t* macros_count, const int64_t* total_task_count,
      ObIArray<ObStoreRange>* splitted_ranges, ObIArray<int64_t>* split_index)
  {
    UNUSED(allocator);
    UNUSED(pkey);
    UNUSED(ranges);
    UNUSED(type);
    UNUSED(macros_count);
    UNUSED(total_task_count);
    UNUSED(splitted_ranges);
    UNUSED(split_index);
    return OB_SUCCESS;
  }
  virtual int get_multi_ranges_cost(
      const common::ObPartitionKey& pkey, const common::ObIArray<common::ObStoreRange>& ranges, int64_t& total_size)
  {
    UNUSED(pkey);
    UNUSED(ranges);
    UNUSED(total_size);
    return OB_SUCCESS;
  }
  virtual int split_multi_ranges(const common::ObPartitionKey& pkey,
      const common::ObIArray<common::ObStoreRange>& ranges, const int64_t expected_task_count,
      common::ObIAllocator& allocator, common::ObArrayArray<common::ObStoreRange>& multi_range_split_array)
  {
    UNUSED(pkey);
    UNUSED(ranges);
    UNUSED(expected_task_count);
    UNUSED(allocator);
    UNUSED(multi_range_split_array);
    return OB_SUCCESS;
  }

  // Provide a general interface for ObNewRange to call query_range_to_macros
  int query_range_to_macros(ObIAllocator& allocator, const ObPartitionKey& pkey, const ObIArray<ObNewRange>& ranges,
      const int64_t type, uint64_t* macros_count, const int64_t* total_task_count,
      ObIArray<ObNewRange>* splitted_ranges, ObIArray<int64_t>* split_index);
};
}  // namespace common
}  // namespace oceanbase
#endif
