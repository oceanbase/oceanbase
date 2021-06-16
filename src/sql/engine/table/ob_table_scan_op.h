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

#ifndef OCEANBASE_TABLE_OB_TABLE_SCAN_OP_H_
#define OCEANBASE_TABLE_OB_TABLE_SCAN_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_operator_reg.h"
#include "share/ob_i_data_access_service.h"
#include "storage/ob_dml_param.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "storage/ob_partition_service.h"
#include "share/ob_i_sql_expression.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "sql/engine/basic/ob_pushdown_filter.h"

namespace oceanbase {
namespace common {
class ObIDataAccessService;
}
namespace storage {
class ObPartitionService;
}

namespace sql {

class ObTableScanOp;

// table scan operator input
// copy from ObTableScanInput
class ObTableScanOpInput : public ObOpInput {
  OB_UNIS_VERSION_V(1);
  friend ObTableScanOp;

public:
  ObTableScanOpInput(ObExecContext& ctx, const ObOpSpec& spec);
  virtual ~ObTableScanOpInput();

  virtual int init(ObTaskInfo& task_info) override;
  virtual void reset() override;

  int64_t get_location_idx() const
  {
    return location_idx_;
  }
  void set_location_idx(int64_t idx)
  {
    location_idx_ = idx;
  }
  int reassign_ranges(common::ObIArray<common::ObNewRange>& range)
  {
    return key_ranges_.assign(range);
  }

  int translate_pid_to_ldx(
      int64_t partition_id, int64_t table_location_key, int64_t ref_table_id, int64_t& location_idx);

protected:
  int64_t location_idx_;
  common::ObSEArray<common::ObNewRange, 3> key_ranges_;
  common::ObPosArray range_array_pos_;
  // FIXME : partition_ranges_ not used, ObTableScanInput keep it for compatibility.
  // common::ObSEArray<ObPartitionScanRanges, 16> partition_ranges_;

  DISALLOW_COPY_AND_ASSIGN(ObTableScanOpInput);
};

class ObTableScanSpec : public ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableScanSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);

  uint64_t get_location_table_id() const
  {
    return is_vt_mapping_ ? vt_table_id_ : (is_index_global_ ? index_id_ : ref_table_id_);
  }
  bool should_scan_index() const
  {
    return index_id_ != ref_table_id_ && common::OB_INVALID_ID != index_id_ && common::OB_INVALID_ID != ref_table_id_ &&
           !sql::ObSQLMockSchemaUtils::is_mock_index(index_id_);
  }

  int set_pruned_index_name(
      const common::ObIArray<common::ObString>& pruned_index_name, common::ObIAllocator& phy_alloc);
  int set_available_index_name(
      const common::ObIArray<common::ObString>& available_index_name, common::ObIAllocator& phy_alloc);
  int set_est_row_count_record(const common::ObIArray<common::ObEstRowCountRecord>& est_records);

  int explain_index_selection_info(char* buf, int64_t buf_len, int64_t& pos) const;

  virtual bool is_table_scan() const override
  {
    return true;
  }
  inline const ObQueryRange& get_query_range() const
  {
    return pre_query_range_;
  }
  inline uint64_t get_scan_key_id() const
  {
    return should_scan_index() ? index_id_ : ref_table_id_;
  }
  inline uint64_t get_table_location_key() const
  {
    return table_location_key_;
  }

  void set_pushdown_storage()
  {
    pd_storage_flag_ |= 0x01;
  }
  void set_pushdown_storage_index_back()
  {
    pd_storage_flag_ |= 0x02;
  }
  inline bool is_pushdown_storage()
  {
    return ObPushdownFilterUtils::is_pushdown_storage(pd_storage_flag_);
  }
  inline bool is_pushdown_storage_index_back()
  {
    return ObPushdownFilterUtils::is_pushdown_storage_index_back(pd_storage_flag_);
  }

  DECLARE_VIRTUAL_TO_STRING;

public:
  /*
   * ref_table_id_  is the physical id of main table of table scan, such as 1101710651081732
   * index_id_ is the physical id of global/local index of the table scan, such as 1101710651081734
   */
  uint64_t ref_table_id_;  // real table id for table to scan
  // @param: table_name_
  //         index_name_
  // Currently, those fields will only be used in (g)v$plan_cache_plan so as to find
  // table name of the operator, which means those fields will be used by local plan
  // and remote plan will not use those fields. Therefore, those fields NEED NOT TO BE SERIALIZED.
  common::ObString table_name_;  // table name of the tabel to scan
  common::ObString index_name_;  // name of the index to be used
  uint64_t index_id_;            // index to be used
  uint64_t table_location_key_;  // used to map table_location_key_ of ObPhyTableLocation
  bool is_index_global_;

  common::ObFixedArray<uint64_t, common::ObIAllocator> output_column_ids_;
  ExprFixedArray storage_output_;

  // filters push down to storage.
  ExprFixedArray pushdown_filters_;
  ExprFixedArray filters_before_index_back_;

  // read consistency, cache policy, result order
  int64_t flags_;
  ObExpr* limit_;
  ObExpr* offset_;
  // is for update
  bool for_update_;
  // 0 means nowait, -1 means infinite
  int64_t for_update_wait_us_;
  // hints for table scan
  common::ObTableScanHint hint_;
  // %hint_ exist or not
  bool exist_hint_;
  bool is_get_;
  ObQueryRange pre_query_range_;
  bool is_top_table_scan_;
  share::schema::ObTableParam table_param_;
  int64_t schema_version_;

  //
  // for dynamic query range prune
  // part_expr_ and subpart_expr_ are partition exprs.
  // part_dep_cols_ and subpart_dep_cols_ are columns that partition exprs depend on.
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_type_;
  share::schema::ObPartitionFuncType subpart_type_;
  ObExpr* part_expr_;
  ObExpr* subpart_expr_;
  common::ObFixedArray<int64_t, common::ObIAllocator> part_range_pos_;
  common::ObFixedArray<int64_t, common::ObIAllocator> subpart_range_pos_;
  ExprFixedArray part_dep_cols_;
  ExprFixedArray subpart_dep_cols_;

  // for virtual table with real table
  bool is_vt_mapping_;
  bool use_real_tenant_id_;
  bool has_tenant_id_col_;
  uint64_t vt_table_id_;
  // for check real table schema version
  // only used in select virtual table that is mapping real table
  int64_t real_schema_version_;
  ExprFixedArray mapping_exprs_;
  common::ObFixedArray<common::ObObjMeta, common::ObIAllocator> output_row_types_;
  common::ObFixedArray<common::ObObjMeta, common::ObIAllocator> key_types_;
  common::ObFixedArray<bool, common::ObIAllocator> key_with_tenant_ids_;
  common::ObFixedArray<bool, common::ObIAllocator> has_extra_tenant_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> org_output_column_ids_;

  //
  // for acs
  //
  OptimizationMethod optimization_method_;
  int64_t available_index_count_;

  //
  // for plan explain
  //
  int64_t table_row_count_;
  int64_t output_row_count_;
  int64_t phy_query_range_row_count_;
  int64_t query_range_row_count_;
  int64_t index_back_row_count_;
  RowCountEstMethod estimate_method_;
  common::ObFixedArray<common::ObEstRowCountRecord, common::ObIAllocator> est_records_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> available_index_name_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> pruned_index_name_;

  bool gi_above_;
  ObExpr* expected_part_id_;
  bool need_scn_;
  bool batch_scan_flag_;

  int32_t pd_storage_flag_;
  ObPushdownFilter pd_storage_filters_;
  ObPushdownFilter pd_storage_index_back_filters_;
};

class ObTableScanOp : public ObOperator {
public:
  static constexpr int64_t CHECK_STATUS_ROWS_INTERVAL = 1 << 13;

  ObTableScanOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  ~ObTableScanOp();

  int inner_open() override;
  int rescan() override;
  int switch_iterator() override;
  int bnl_switch_iterator();
  int inner_get_next_row() override;
  int inner_close() override;
  void destroy() override;

  // renew the TSC by a new granule task info and do table scan
  int reassign_task_and_do_table_scan(ObGranuleTaskInfo& info);
  // get task from exection ctx
  int get_gi_task_and_restart();

  /*
   * the following three functions are used for blocked nested loop join
   * reset_qeury_range, add_query_range and rescan after setting query range
   */
  int reset_query_range();
  virtual int add_query_range();
  virtual int rescan_after_adding_query_range();

  // batch_rescan for ObNestedLoopJoin(self join)
  int batch_rescan_init();
  int batch_rescan_add_key();
  int batch_rescan();

  // group_rescan for ObNestedLoopJoin
  int group_rescan_init(int64_t size);
  int group_add_query_range();
  int group_rescan();
  common::ObNewIterIterator* get_bnl_iters()
  {
    return bnl_iters_;
  }
  void set_iter_end(bool iter_end)
  {
    iter_end_ = iter_end;
  }

  int init_converter();

protected:
  int init_pushdown_storage_filter();
  int calc_expr_int_value(const ObExpr& expr, int64_t& retval, bool& is_null_value);
  virtual int do_table_scan(bool is_rescan, bool need_prepare = true);
  virtual int prepare_scan_param();
  int update_scan_param_pkey();
  int prepare(bool is_rescan);
  int get_partition_service(ObTaskExecutorCtx& executor_ctx, common::ObIDataAccessService*& das) const;
  bool need_extract_range() const
  {
    return MY_SPEC.pre_query_range_.has_range();
  }

  virtual int init_table_allocator();

  bool partition_list_is_empty(const ObPhyTableLocationIArray& phy_table_locs) const;

  int rt_rescan();
  int vt_rescan();

  // TODO : to be removed after expr_ctx_ not needed.
  int init_old_expr_ctx();

  int prune_query_range_by_partition_id(common::ObIArray<common::ObNewRange>& scan_ranges);
  int can_prune_by_partition_id(share::schema::ObSchemaGetterGuard& schema_guard, const int64_t partition_id,
      ObNewRange& scan_range, bool& can_prune);
  int construct_partition_range(ObArenaAllocator& allocator, const share::schema::ObPartitionFuncType part_type,
      const common::ObIArray<int64_t>& part_range_pos, const ObNewRange& scan_range, const ObExpr* part_expr,
      const ExprFixedArray& part_dep_cols, bool& can_prune, ObNewRange& part_range);
  int check_cache_table_version(int64_t schema_version);

  int fill_storage_feedback_info();
  int extract_scan_ranges();
  void fill_table_scan_stat(const ObTableScanStatistic& statistic, ObTableScanStat& scan_stat) const;
  void set_cache_stat(const ObPlanStat& plan_stat, storage::ObTableScanParam& param);

private:
  int get_next_row_with_mode();

protected:
  common::ObNewRowIterator* result_;
  // result iterator of array binding
  common::ObNewIterIterator* ab_iters_;
  // result iterator of batch nestloop join
  common::ObNewIterIterator* bnl_iters_;
  storage::ObRow2ExprsProjector row2exprs_projector_;
  storage::ObTableScanParam scan_param_;
  storage::ObIPartitionGroupGuard partition_guard_;
  // This allocator is generated when the table scan context is created, until the table scan is closed and reset
  common::ObArenaAllocator* table_allocator_;
  // this is used for found rows, reset in rescan.
  int64_t output_row_count_;
  bool iter_end_;
  int64_t partition_id_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  int64_t iterated_rows_;
  bool is_partition_list_empty_;
  bool got_feedback_;
  int64_t expected_part_id_;

  sql::ObVirtualTableResultConverter* vt_result_converter_;
  // TODO : remove the old expr ctx
  // virtual table need the expr ctx to get calc_buf_ right now.
  // prune_query_range_by_partition_id() also need it for partition expr calc.
  common::ObArenaAllocator expr_ctx_alloc_;
  ObExprCtx expr_ctx_;
  ObPushdownFilterExecutor* filter_executor_;
  ObPushdownFilterExecutor* index_back_filter_executor_;

  const uint64_t* cur_trace_id_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_TABLE_OB_TABLE_SCAN_OP_H_
