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

#ifndef OCEANBASE_SQL_ENGINE_TABLE_TABLE_SCAN_
#define OCEANBASE_SQL_ENGINE_TABLE_TABLE_SCAN_

#include "common/ob_partition_key.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/ob_phy_table_location.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "storage/ob_i_partition_storage.h"
#include "share/ob_i_sql_expression.h"
#include "ob_table_partition_ranges.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
namespace common {
class ObIDataAccessService;
}
namespace storage {
class ObPartitionService;
}

namespace sql {
class ObTaskExecutorCtx;
class ObDomainIndex;
class ObTableScanWithIndexBack;
class ObTableScanWithChecksum;
class ObTableScanCreateDomainIndex;
class ObGranuleTaskInfo;

class ObTableScanInput : public ObIPhyOperatorInput {
  friend class ObTableScan;
  friend class ObMVTableScan;
  friend class ObMultiPartitionTableScan;
  OB_UNIS_VERSION_V(1);

public:
  ObTableScanInput();
  virtual ~ObTableScanInput();
  virtual void reset() override;
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  int64_t get_location_idx() const;
  inline void set_location_idx(int64_t location_idx)
  {
    location_idx_ = location_idx;
  }
  virtual ObPhyOperatorType get_phy_op_type() const;
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator);
  int reassign_ranges(ObIArray<ObNewRange>& ranges);

  int translate_pid_to_ldx(ObExecContext& ctx, int64_t partition_id, int64_t table_location_key, int64_t ref_table_id,
      int64_t& location_idx);

private:
  int deep_copy_range(common::ObIAllocator* allocator, const common::ObNewRange& src, common::ObNewRange& dst);

protected:
  int64_t location_idx_;

protected:
  common::ObSEArray<common::ObNewRange, 16> key_ranges_;
  common::ObPosArray range_array_pos_;
  // This variable is reserved for compatibility. Because 2.0 will
  // serialize and deserialize this variable
  common::ObSEArray<ObPartitionScanRanges, 16> partition_ranges_;

protected:
  common::ObIAllocator* deserialize_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObTableScanInput);
};

class ObTableScan : public ObNoChildrenPhyOperator {
  OB_UNIS_VERSION_V(1);

public:
  class ObTableScanCtx : public ObPhyOperatorCtx {
    friend class ObTableScan;
    friend class ObDomainIndex;
    friend class ObTableScanWithIndexBack;
    friend class ObMVTableScan;
    friend class ObRowSampleScan;
    friend class ObBlockSampleScan;
    friend class ObTableScanWithChecksum;
    friend class ObTableScanCreateDomainIndex;
    friend class ObMultiPartTableScan;

  public:
    explicit ObTableScanCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx),
          result_(NULL),
          iter_result_(NULL),
          result_iters_(NULL),
          scan_param_(),
          partition_guard_(),
          table_allocator_(NULL),
          output_row_count_(-1),
          iter_end_(false),
          partition_id_(OB_INVALID_INDEX),
          iterated_rows_(0),
          is_partition_list_empty_(false),
          got_feedback_(false),
          vt_result_converter_(nullptr)
    {
      scan_param_.partition_guard_ = &partition_guard_;
    }
    virtual ~ObTableScanCtx()
    {
      result_ = NULL;
      result_iters_ = NULL;
      scan_param_.partition_guard_ = NULL;
      partition_guard_.reset();
    }
    virtual void destroy()
    {
      scan_param_.destroy_schema_guard();
      schema_guard_.~ObSchemaGetterGuard();
      ObPhyOperatorCtx::destroy_base();
      scan_param_.partition_guard_ = NULL;
      partition_guard_.reset();
      if (OB_NOT_NULL(vt_result_converter_)) {
        vt_result_converter_->destroy();
        vt_result_converter_->~ObVirtualTableResultConverter();
        vt_result_converter_ = nullptr;
      }
    }
    inline void set_partition_list_empty(const bool partition_list_empty)
    {
      is_partition_list_empty_ = partition_list_empty;
    }
    inline bool get_partition_list_empty() const
    {
      return is_partition_list_empty_;
    }
    inline void set_table_allocator(common::ObArenaAllocator* alloc)
    {
      table_allocator_ = alloc;
    }
    inline void set_scan_iterator_mementity(lib::MemoryContext* mem)
    {
      scan_param_.iterator_mementity_ = mem;
      scan_param_.allocator_ = &mem->get_arena_allocator();
    }
    inline storage::ObTableScanParam& get_table_scan_param()
    {
      return scan_param_;
    }
    const storage::ObTableScanParam& get_table_scan_param() const
    {
      return scan_param_;
    }
    inline share::schema::ObSchemaGetterGuard& get_schema_guard()
    {
      return schema_guard_;
    }
    virtual int init_table_allocator(ObExecContext& ctx);

  public:
    common::ObNewRowIterator* result_;
    // used for array binding
    common::ObNewIterIterator* iter_result_;
    // used for batch nestloop join
    common::ObNewIterIterator* result_iters_;
    storage::ObTableScanParam scan_param_;
    storage::ObIPartitionGroupGuard partition_guard_;
    // This allocator is generated when the table scan context is created, until the table scan is closed and reset
    common::ObArenaAllocator* table_allocator_;
    int64_t output_row_count_;  // this is used for found rows, if rescan. output_row_count_ is not accurate
    bool iter_end_;
    int64_t partition_id_;
    share::schema::ObSchemaGetterGuard schema_guard_;
    int64_t iterated_rows_;
    bool is_partition_list_empty_;
    bool got_feedback_;
    sql::ObVirtualTableResultConverter* vt_result_converter_;
  };

public:
  explicit ObTableScan(common::ObIAllocator& allocator);
  virtual ~ObTableScan();
  virtual void reset();
  virtual void reuse();
  // renew the TSC by a new granule task info and do table scan
  int reassign_task_and_do_table_scan(ObExecContext& ctx, ObGranuleTaskInfo& info) const;
  // get task from exection ctx
  int get_gi_task_and_restart(ObExecContext& ctx) const;
  // virtual int get_next_row(ObExecContext &ctx, const common::ObNewRow *&row) const;
  virtual int rescan(ObExecContext& ctx) const;
  virtual int create_operator_input(ObExecContext& ctx) const;

  DECLARE_VIRTUAL_TO_STRING;

  // construct ObTableScanParam  using following interface
  inline int init_output_column(const int64_t size);
  inline int add_output_column(const uint64_t column_id);
  inline int set_filter_before_indexback_flags(const common::ObIArray<bool>& flags);
  inline int set_partition_range_pos(const common::ObIArray<int64_t>& partition_range_pos);
  const common::ObIArray<int64_t>& get_partition_range_pos()
  {
    return part_range_pos_;
  }
  inline int set_subpartition_range_pos(const common::ObIArray<int64_t>& subpartition_range_pos);
  const common::ObIArray<int64_t>& get_subpartition_range_pos()
  {
    return subpart_range_pos_;
  }
  inline void set_partition_level(const share::schema::ObPartitionLevel partition_level);
  inline void set_partition_type(const share::schema::ObPartitionFuncType part_type);
  share::schema::ObPartitionFuncType get_partition_type()
  {
    return part_type_;
  };
  inline void set_subpartition_type(const share::schema::ObPartitionFuncType subpart_type);
  share::schema::ObPartitionFuncType get_subpartition_type()
  {
    return subpart_type_;
  };
  inline void set_schema_version(const int64_t schema_version);
  int64_t get_schema_version() const
  {
    return schema_version_;
  };
  inline void set_ref_table_id(const uint64_t ref_table_id);
  inline uint64_t get_ref_table_id() const;
  inline void set_table_name(const common::ObString& table_name);
  inline const common::ObString& get_table_name() const;
  inline uint64_t get_location_table_id() const;
  inline void set_index_table_id(const uint64_t idx_table_id);
  inline uint64_t get_index_table_id() const;
  inline const common::ObString& get_index_name() const;
  inline void set_index_name(const common::ObString& index_name);
  inline void set_is_index_global(const bool is_index_global);
  inline bool get_is_index_global() const;
  inline void add_flag(const int64_t flag);
  inline void set_flags(const int64_t flags)
  {
    flags_ = flags;
  }
  inline int64_t get_flags() const
  {
    return flags_;
  }
  inline void set_table_location_key(uint64_t table_location_key)
  {
    table_location_key_ = table_location_key;
  }
  inline uint64_t get_table_location_key() const
  {
    return table_location_key_;
  }
  inline int set_query_range(const ObQueryRange& query_range)
  {
    return pre_query_range_.deep_copy(query_range);
  }
  inline const ObQueryRange& get_query_range() const
  {
    return pre_query_range_;
  }
  inline void set_is_get(bool v)
  {
    is_get_ = v;
  }
  inline const bool& is_get() const
  {
    return is_get_;
  }
  inline void set_table_row_count(int64_t table_row_count)
  {
    table_row_count_ = table_row_count;
  }
  inline int64_t get_table_row_count() const
  {
    return table_row_count_;
  }
  inline void set_output_row_count(int64_t output_row_count)
  {
    output_row_count_ = output_row_count;
  }
  inline int64_t get_output_row_count() const
  {
    return output_row_count_;
  }
  inline void set_phy_query_range_row_count(int64_t phy_query_range_row_count)
  {
    phy_query_range_row_count_ = phy_query_range_row_count;
  }
  inline int64_t get_phy_query_range_row_count() const
  {
    return phy_query_range_row_count_;
  }
  inline void set_query_range_row_count(int64_t query_range_row_count)
  {
    query_range_row_count_ = query_range_row_count;
  }
  inline int64_t get_query_range_row_count() const
  {
    return query_range_row_count_;
  }
  inline void set_index_back_row_count(int64_t index_back_row_count)
  {
    index_back_row_count_ = index_back_row_count;
  }
  inline int64_t get_index_back_row_count() const
  {
    return index_back_row_count_;
  }
  int explain_index_selection_info(char* buf, int64_t buf_len, int64_t& pos) const;
  inline void set_estimate_method(RowCountEstMethod method)
  {
    estimate_method_ = method;
  }
  inline RowCountEstMethod get_estimate_method() const
  {
    return estimate_method_;
  }
  int set_pruned_index_name(const common::ObIArray<common::ObString>& pruned_index_name, ObIAllocator& phy_alloc);
  int set_available_index_name(const common::ObIArray<common::ObString>& available_index_name, ObIAllocator& phy_alloc);
  int set_est_row_count_record(const common::ObIArray<common::ObEstRowCountRecord>& est_records);
  inline const common::ObIArray<common::ObEstRowCountRecord>& get_est_row_count_record() const
  {
    return est_records_;
  }
  void set_for_update(bool for_update, int64_t wait_us)
  {
    for_update_ = for_update;
    for_update_wait_us_ = wait_us;
  }
  int64_t get_for_update_wait_us() const
  {
    return for_update_wait_us_;
  };
  bool is_for_update() const
  {
    return for_update_;
  }
  void set_is_top_table_scan(bool is_top)
  {
    is_top_table_scan_ = is_top;
  }

  void set_hint(const common::ObTableScanHint& hint)
  {
    hint_ = hint;
  }
  const common::ObTableScanHint& get_const_hint() const
  {
    return hint_;
  }
  common::ObTableScanHint& get_hint()
  {
    return hint_;
  }

  void set_exist_hint(bool exist_hint)
  {
    exist_hint_ = exist_hint;
  }
  bool exist_hint()
  {
    return exist_hint_;
  }

  // for virtual table with real table
  inline void set_is_vt_mapping()
  {
    is_vt_mapping_ = true;
  }
  inline void set_has_real_tenant_id(bool has_real_tenant_id)
  {
    use_real_tenant_id_ = has_real_tenant_id;
  }
  inline void set_has_tenant_id_col(bool has_tenant_id_col)
  {
    has_tenant_id_col_ = has_tenant_id_col;
  }
  inline void set_vt_table_id(uint64_t vt_table_id)
  {
    vt_table_id_ = vt_table_id;
  }
  inline void set_real_schema_version(int64_t schema_version)
  {
    real_schema_version_ = schema_version;
  }

  inline uint64_t get_vt_table_id() const
  {
    return vt_table_id_;
  }

  inline int init_key_types(const int64_t size)
  {
    return init_array_size<>(key_types_, size);
  }
  inline int init_key_tenant_ids(const int64_t size)
  {
    return init_array_size<>(key_with_tenant_ids_, size);
  }
  inline int init_extra_tenant_ids(const int64_t size)
  {
    return init_array_size<>(has_extra_tenant_ids_, size);
  }
  inline int init_output_columns_type(const int64_t size)
  {
    return init_array_size<>(output_row_types_, size);
  }

  inline int add_key_type(const ObObjMeta& obj_type)
  {
    return key_types_.push_back(obj_type);
  }
  inline int add_key_with_tenant_ids(bool has_tenant_id)
  {
    return key_with_tenant_ids_.push_back(has_tenant_id);
  }
  inline int add_extra_column_ids(bool has_tenant_id)
  {
    return has_extra_tenant_ids_.push_back(has_tenant_id);
  }
  inline int add_output_column_type(const ObObjMeta& obj_meta)
  {
    return output_row_types_.push_back(obj_meta);
  }

  inline int assign_org_output_column_ids(const common::ObIArray<uint64_t>& column_ids)
  {
    org_output_column_ids_.reset();
    return org_output_column_ids_.assign(column_ids);
  }

  inline const common::ObIArray<bool>& get_extra_tenant_ids() const
  {
    return has_extra_tenant_ids_;
  }
  inline const common::ObIArray<uint64_t>& get_org_output_column_ids() const
  {
    return org_output_column_ids_;
  }
  inline const common::ObIArray<ObObjMeta>& get_key_types() const
  {
    return key_types_;
  }
  inline const common::ObIArray<bool>& get_key_with_tenant_ids() const
  {
    return key_with_tenant_ids_;
  }

  void set_gi_above(bool above)
  {
    gi_above_ = above;
  }

  inline bool should_scan_index() const
  {
    return index_id_ != ref_table_id_ && common::OB_INVALID_ID != index_id_ && common::OB_INVALID_ID != ref_table_id_ &&
           !sql::ObSQLMockSchemaUtils::is_mock_index(index_id_);
  }

  inline void set_batch_scan_flag(const bool opt)
  {
    batch_scan_flag_ = opt;
  }
  inline bool get_batch_scan_flag() const
  {
    return batch_scan_flag_;
  }

  int set_limit_offset(const ObSqlExpression* limit, const ObSqlExpression* offset);
  int set_part_expr(const ObSqlExpression* part_expr);
  const ObSqlExpression& get_part_expr()
  {
    return part_expr_;
  };
  int set_subpart_expr(const ObSqlExpression* subpart_expr);
  const ObSqlExpression& get_subpart_expr()
  {
    return subpart_expr_;
  }
  // batch_rescan for ObNestedLoopJoin(self join)
  int batch_rescan_init(ObExecContext& ctx) const;
  int batch_rescan_add_key(ObExecContext& ctx) const;
  int batch_rescan(ObExecContext& ctx) const;
  share::schema::ObTableParam& get_table_param()
  {
    return table_param_;
  }

  // group_rescan for ObNestedLoopJoin
  int group_rescan_init(ObExecContext& ctx, int64_t size) const;
  int group_add_query_range(ObExecContext& ctx) const;
  int group_rescan(ObExecContext& ctx) const;
  /*
   * the following three functions are used for blocked nested loop join
   * reset_qeury_range, add_query_range and rescan after setting query range
   */
  int reset_query_range(ObExecContext& ctx) const;
  virtual int add_query_range(ObExecContext& ctx) const;
  virtual int rescan_after_adding_query_range(ObExecContext& ctx) const;
  int set_part_filter(const ObTableLocation& table_loc);
  virtual bool is_table_scan() const override
  {
    return true;
  }
  void set_whole_range_scan(bool whole_range_scan)
  {
    is_whole_range_scan_ = whole_range_scan;
  }
  bool is_whole_range_scan() const
  {
    return is_whole_range_scan_;
  }
  void set_optimization_method(OptimizationMethod m)
  {
    optimization_method_ = m;
  }
  OptimizationMethod get_optimization_method() const
  {
    return optimization_method_;
  }
  void set_available_index_count(int64_t v)
  {
    available_index_count_ = v;
  }
  int64_t get_available_index_count() const
  {
    return available_index_count_;
  }
  void set_need_scn(bool need_scn)
  {
    need_scn_ = need_scn;
  }
  bool need_scn() const
  {
    return need_scn_;
  }
  /*
   * the range from opitimier must change id to storage scan key id.
   * If the optimizer deside to use index idx to access table A, the origin
   * ObNewRange use A's real table id, but the range is a A(idx) range.
   * We must change this id before send this range to storage layer.
   */
  inline uint64_t get_scan_key_id() const
  {
    return should_scan_index() ? get_index_table_id() : get_ref_table_id();
  }
  virtual int switch_iterator(ObExecContext& ctx) const override;
  const ObIArray<uint64_t>& get_output_column_ids() const
  {
    return output_column_ids_;
  }

  static int transform_rowid_ranges(common::ObIAllocator& allocator, const share::schema::ObTableParam& table_param,
      const uint64_t index_id, ObQueryRangeArray& key_ranges);
  static int transform_rowid_range(
      common::ObIAllocator& allocator, const ObIArray<share::schema::ObColDesc>& rowkey_descs, ObNewRange& key_range);

protected:
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual bool need_filter_row() const override;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  // helper
  int calc_expr_int_value(
      common::ObExprCtx& expr_ctx, const ObSqlExpression& expr, int64_t& retval, bool& is_null_value) const;
  virtual int do_table_scan(ObExecContext& ctx, bool is_rescan, bool need_prepare = true) const;
  int sim_err(ObSQLSessionInfo* my_session) const;
  virtual int prepare_scan_param(ObExecContext& ctx) const;
  int update_scan_param_pkey(ObExecContext& ctx) const;
  int prepare(ObExecContext& ctx, bool is_rescan) const;
  int get_partition_service(ObTaskExecutorCtx& executor_ctx, common::ObIDataAccessService*& das) const;
  inline bool need_extract_range() const
  {
    return pre_query_range_.has_range();
  }
  bool partition_list_is_empty(const ObPhyTableLocationIArray& phy_table_locs) const;
  int rt_rescan(ObExecContext& ctx) const;
  int vt_rescan(ObExecContext& ctx) const;
  // used to optimize partition pruning
  int prune_query_range_by_partition_id(ObExecContext& ctx, common::ObIArray<common::ObNewRange>& scan_ranges) const;
  int can_prune_by_partition_id(share::schema::ObSchemaGetterGuard& schema_guard, common::ObExprCtx& expr_ctx,
      const int64_t partition_id, ObNewRange& scan_range, bool& can_prune) const;
  int construct_partition_range(ObArenaAllocator& allocator, common::ObExprCtx& expr_ctx,
      const share::schema::ObPartitionFuncType part_type, const common::ObIArray<int64_t>& part_range_pos,
      const ObNewRange& scan_range, const ObSqlExpression& part_expr, bool& can_prune, ObNewRange& part_range) const;
  int revise_hash_part_object(common::ObObj& obj) const;
  int check_cache_table_version(int64_t schema_version) const;
  int fill_storage_feedback_info(ObExecContext& ctx) const;
  int extract_scan_ranges(ObTableScanInput& scan_input, ObTableScanCtx& scan_ctx) const;
  void fill_table_scan_stat(const ObTableScanStatistic& statistic, ObTableScanStat& scan_stat) const;
  void set_cache_stat(const ObPlanStat& plan_stat, storage::ObTableScanParam& param) const;

  int init_converter(ObExecContext& ctx) const;
  int get_next_row_with_mode(ObTableScanCtx* scan_ctx, ObNewRow*& row) const;
  static int transform_rowid_rowkey(
      common::ObIAllocator& allocator, const ObIArray<share::schema::ObColDesc>& rowkey_descs, ObRowkey& row_key);

protected:
  // params
  uint64_t ref_table_id_;  // real table id for table to scan
  // @param: table_name_ & index_name_
  // Currently, those fields will only be used in (g)v$plan_cache_plan so as to find
  // table name of the operator, which means those fields will be used by local plan
  // and remote plan will not use those fields. Therefore, those fields NEED NOT TO BE SERIALIZED.
  common::ObString table_name_;  // table name of the table to scan;
  common::ObString index_name_;  // name of the index to be used
  uint64_t index_id_;            // index to be used
  bool is_index_global_;         // whether index is global
  // columns to output
  common::ObFixedArray<uint64_t, common::ObIAllocator> output_column_ids_;
  common::ObFixedArray<bool, common::ObIAllocator> filter_before_index_back_;
  int64_t flags_;  // read consistency, cache policy, result order
  ObSqlExpression limit_;
  ObSqlExpression offset_;
  // is for update
  bool for_update_;               // FOR UPDATE clause
  int64_t for_update_wait_us_;    // 0 means nowait, -1 means infinite
  common::ObTableScanHint hint_;  // hints for table scan
  bool exist_hint_;
  bool is_get_;
  ObQueryRange pre_query_range_;
  uint64_t table_location_key_;  // used to map table_location_key_ in ObPhyTableLocation
  bool is_top_table_scan_;       // need serialize
  share::schema::ObTableParam table_param_;
  int64_t schema_version_;
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_type_;
  share::schema::ObPartitionFuncType subpart_type_;
  ObSqlExpression part_expr_;
  ObSqlExpression subpart_expr_;
  common::ObFixedArray<int64_t, common::ObIAllocator> part_range_pos_;
  common::ObFixedArray<int64_t, common::ObIAllocator> subpart_range_pos_;

  //***** used for acs *******
  OptimizationMethod optimization_method_;
  int64_t available_index_count_;

  //***** used for plan explain *******
  int64_t table_row_count_;
  int64_t output_row_count_;
  int64_t phy_query_range_row_count_;
  int64_t query_range_row_count_;
  int64_t index_back_row_count_;
  RowCountEstMethod estimate_method_;
  common::ObFixedArray<common::ObEstRowCountRecord, common::ObIAllocator> est_records_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> available_index_name_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> pruned_index_name_;
  //***********************************
  bool gi_above_;

  // for virtual table with real table
  bool is_vt_mapping_;
  bool use_real_tenant_id_;
  bool has_tenant_id_col_;
  uint64_t vt_table_id_;
  // for check real table schema version
  // only used in select virtual table that is mapping real table
  int64_t real_schema_version_;
  common::ObFixedArray<common::ObObjMeta, common::ObIAllocator> output_row_types_;
  common::ObFixedArray<common::ObObjMeta, common::ObIAllocator> key_types_;
  common::ObFixedArray<bool, common::ObIAllocator> key_with_tenant_ids_;
  common::ObFixedArray<bool, common::ObIAllocator> has_extra_tenant_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> org_output_column_ids_;

  // used for partition cutting
  ObTableLocation part_filter_;

private:
  bool is_whole_range_scan_;
  // used for batch nested loop join
  bool batch_scan_flag_;
  bool need_scn_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableScan);
};

inline int ObTableScan::init_output_column(const int64_t size)
{
  output_column_ids_.reset();
  return init_array_size<>(output_column_ids_, size);
}

inline int ObTableScan::add_output_column(const uint64_t column_id)
{
  return output_column_ids_.push_back(column_id);
}

inline int ObTableScan::set_filter_before_indexback_flags(const common::ObIArray<bool>& flags)
{
  return filter_before_index_back_.assign(flags);
}

inline int ObTableScan::set_partition_range_pos(const common::ObIArray<int64_t>& partition_range_pos)
{
  return part_range_pos_.assign(partition_range_pos);
}

inline int ObTableScan::set_subpartition_range_pos(const common::ObIArray<int64_t>& subpartition_range_pos)
{
  return subpart_range_pos_.assign(subpartition_range_pos);
}

inline void ObTableScan::set_partition_level(const share::schema::ObPartitionLevel partition_level)
{
  part_level_ = partition_level;
}

inline void ObTableScan::set_partition_type(const share::schema::ObPartitionFuncType part_type)
{
  part_type_ = part_type;
}

inline void ObTableScan::set_subpartition_type(const share::schema::ObPartitionFuncType subpart_type)
{
  subpart_type_ = subpart_type;
}

inline void ObTableScan::set_schema_version(const int64_t schema_version)
{
  schema_version_ = schema_version;
}

inline void ObTableScan::set_ref_table_id(const uint64_t ref_table_id)
{
  ref_table_id_ = ref_table_id;
}

inline uint64_t ObTableScan::get_ref_table_id() const
{
  return ref_table_id_;
}

inline void ObTableScan::set_table_name(const common::ObString& table_name)
{
  table_name_ = table_name;
}

inline const common::ObString& ObTableScan::get_table_name() const
{
  return table_name_;
}

inline uint64_t ObTableScan::get_location_table_id() const
{
  return is_vt_mapping_ ? vt_table_id_ : (is_index_global_ ? index_id_ : ref_table_id_);
}

inline void ObTableScan::set_index_table_id(const uint64_t idx_table_id)
{
  index_id_ = idx_table_id;
}

inline uint64_t ObTableScan::get_index_table_id() const
{
  return index_id_;
}

inline void ObTableScan::set_index_name(const common::ObString& index_name)
{
  index_name_ = index_name;
}

inline const common::ObString& ObTableScan::get_index_name() const
{
  return index_name_;
}

inline void ObTableScan::set_is_index_global(const bool is_index_global)
{
  is_index_global_ = is_index_global;
}

inline bool ObTableScan::get_is_index_global() const
{
  return is_index_global_;
}

inline void ObTableScan::add_flag(const int64_t flag)
{
  flags_ |= flag;
}

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_TABLE_TABLE_SCAN_ */
