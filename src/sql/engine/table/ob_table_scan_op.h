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

#include "share/ob_i_tablet_scan.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_operator_reg.h"
#include "storage/access/ob_dml_param.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "share/ob_i_sql_expression.h"
#include "sql/das/ob_das_ref.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/table/ob_index_lookup_op_impl.h"
#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_merge_iter.h"
#include "sql/das/iter/ob_das_group_fold_iter.h"

namespace oceanbase
{
namespace common
{
  class ObITabletScan;
}

namespace sql
{

class ObTableScanOp;
class ObDASScanOp;
class ObGlobalIndexLookupOpImpl;

struct FlashBackItem
{
public:
  FlashBackItem()
    : need_scn_(false),
      flashback_query_expr_(nullptr),
      flashback_query_type_(TableItem::NOT_USING),
      fq_read_tx_uncommitted_(false)
  { }
  int set_flashback_query_info(ObEvalCtx &eval_ctx, ObDASScanRtDef &scan_rtdef) const;
  TO_STRING_KV(K_(need_scn),
               KPC_(flashback_query_expr),
               K_(flashback_query_type),
               K_(fq_read_tx_uncommitted));
  bool need_scn_;
  ObExpr *flashback_query_expr_; //flashback query expr
  TableItem::FlashBackQueryType flashback_query_type_; //flashback query type
  bool fq_read_tx_uncommitted_; // whether read uncommitted changes in transaction
};

struct ObSpatialIndexCache
{
public:
  ObSpatialIndexCache() :
      spat_rows_(nullptr),
      spat_row_index_(0),
      mbr_buffer_(nullptr),
      obj_buffer_(nullptr)
  {}
  ~ObSpatialIndexCache() {};
  ObDomainIndexRow *spat_rows_;
  uint8_t spat_row_index_;
  void *mbr_buffer_;
  void *obj_buffer_;
};

//for the oracle virtual agent table access the real table
struct AgentVtAccessMeta
{
  OB_UNIS_VERSION(1);
public:
  AgentVtAccessMeta(common::ObIAllocator &alloc)
    : vt_table_id_(UINT64_MAX),
      access_exprs_(alloc),
      access_column_ids_(alloc),
      access_row_types_(alloc),
      key_types_(alloc)
  { }
  TO_STRING_KV(K_(vt_table_id),
               K_(access_exprs),
               K_(access_column_ids),
               K_(access_row_types),
               K_(key_types));
  // for virtual table with real table
  uint64_t vt_table_id_;
  //the virtual agent table's access_exprs logically corresponds to DAS.result_output_exprs
  //but DAS.result_outputs is the column of the real table
  //so the pointer of the expr is not equal
  ExprFixedArray access_exprs_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> access_column_ids_;
  common::ObFixedArray<common::ObObjMeta, common::ObIAllocator> access_row_types_;
  common::ObFixedArray<common::ObObjMeta, common::ObIAllocator> key_types_;
};

typedef common::ObFixedArray<int64_t, common::ObIAllocator> Int64FixedArray;
struct GroupRescanParamInfo
{
  GroupRescanParamInfo()
    : param_idx_(common::OB_INVALID_ID),
      gr_param_(nullptr),
      cur_param_()
  { }
  GroupRescanParamInfo(int64_t param_idx, ObSqlArrayObj *gr_param)
  : param_idx_(param_idx),
    gr_param_(gr_param)
  { }
  TO_STRING_KV(K_(param_idx),
               KPC_(gr_param),
               K_(cur_param));
  int64_t param_idx_;
  ObSqlArrayObj *gr_param_; //group rescan param
  common::ObObjParam cur_param_; //current param in param store, used to restore paramstore state after the completion of group rescan.
};
typedef common::ObFixedArray<GroupRescanParamInfo, common::ObIAllocator> GroupRescanParamArray;
struct ObTableScanCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObTableScanCtDef(common::ObIAllocator &allocator)
    : pre_query_range_(allocator),
      flashback_item_(),
      bnlj_param_idxs_(allocator),
      scan_flags_(),
      scan_ctdef_(allocator),
      lookup_ctdef_(nullptr),
      lookup_loc_meta_(nullptr),
      das_dppr_tbl_(nullptr),
      allocator_(allocator),
      calc_part_id_expr_(NULL),
      global_index_rowkey_exprs_(allocator),
      attach_spec_(allocator_, &scan_ctdef_),
      flags_(0)
  { }
  const ExprFixedArray &get_das_output_exprs() const
  {
    return lookup_ctdef_ != nullptr ? lookup_ctdef_->result_output_ : scan_ctdef_.result_output_;
  }
  const UIntFixedArray &get_full_acccess_cids() const
  {
    return lookup_ctdef_ != nullptr ?
        lookup_ctdef_->access_column_ids_ :
        scan_ctdef_.access_column_ids_;
  }
  int allocate_dppr_table_loc();
  ObDASScanCtDef *get_lookup_ctdef();
  TO_STRING_KV(K_(pre_query_range),
               K_(flashback_item),
               K_(bnlj_param_idxs),
               K_(scan_flags),
               K_(scan_ctdef),
               KPC_(lookup_ctdef),
               KPC_(lookup_loc_meta),
               KPC_(das_dppr_tbl),
               KPC_(calc_part_id_expr),
               K_(global_index_rowkey_exprs),
               K_(attach_spec));
  //the query range of index scan/table scan
  ObQueryRange pre_query_range_;
  FlashBackItem flashback_item_;
  Int64FixedArray bnlj_param_idxs_;
  // read consistency, cache policy, result order
  common::ObQueryFlag scan_flags_;
  //scan_ctdef_ means the scan action performed initially:
  //When the query directly scan the main table,
  //scan_ctdef means the parameter required by the scan main table
  //When the query needs to access the index,
  //scan_ctdef means the parameter required by the scan index table
  ObDASScanCtDef scan_ctdef_;
  //lookup_ctdef is a pointer,
  //which is used only when accessing index table and lookup the main table,
  //it means to the lookup parameter required by the main table
  ObDASScanCtDef *lookup_ctdef_;
  //lookup_loc_meta_ used to calc the main table tablet location
  //when query access the global index and lookup the main table
  ObDASTableLocMeta *lookup_loc_meta_;
  //used for dynamic partition pruning
  ObTableLocation *das_dppr_tbl_;
  common::ObIAllocator &allocator_;
  // Begin for Global Index Lookup
  ObExpr *calc_part_id_expr_;
  ExprFixedArray global_index_rowkey_exprs_;
  // end for Global Index Lookup
  ObDASAttachSpec attach_spec_;
  union {
    uint64_t flags_;
    struct {
      uint64_t is_das_keep_order_            : 1; // whether das need keep ordering
      uint64_t reserved_                     : 63;
    };
  };
};

struct ObTableScanRtDef
{
  ObTableScanRtDef(common::ObIAllocator &allocator)
    : bnlj_params_(allocator),
      scan_rtdef_(),
      lookup_rtdef_(nullptr),
      range_buffers_(nullptr),
      range_buffer_idx_(0),
      group_size_(0),
      max_group_size_(0),
      attach_rtinfo_(nullptr)
  { }

  void prepare_multi_part_limit_param();
  bool has_lookup_limit() const
  { return lookup_rtdef_ != nullptr && lookup_rtdef_->limit_param_.is_valid(); }
  TO_STRING_KV(K_(scan_rtdef),
               KPC_(lookup_rtdef),
               K_(group_size),
               K_(max_group_size));

  GroupRescanParamArray bnlj_params_;
  ObDASScanRtDef scan_rtdef_;
  ObDASScanRtDef *lookup_rtdef_;
  // for equal_query_range opt
  void *range_buffers_;
  int64_t range_buffer_idx_;
  // for equal_query_range opt end
  int64_t group_size_;
  int64_t max_group_size_;
  ObDASAttachRtInfo *attach_rtinfo_;
};

// table scan operator input
// copy from ObTableScanInput
class ObTableScanOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
  friend ObTableScanOp;
public:
  ObTableScanOpInput(ObExecContext &ctx, const ObOpSpec &spec);
  virtual ~ObTableScanOpInput();

  virtual int init(ObTaskInfo &task_info) override;
  virtual void reset() override;
  bool get_need_extract_query_range() const { return !not_need_extract_query_range_; }
  void set_need_extract_query_range(bool need_extract) { not_need_extract_query_range_ = !need_extract; }

  int reassign_ranges(common::ObIArray<common::ObNewRange> &range)
  {
    return key_ranges_.assign(range);
  }
protected:
  ObDASTabletLoc *tablet_loc_;
  common::ObSEArray<common::ObNewRange, 1> key_ranges_;
  common::ObSEArray<common::ObNewRange, 1> ss_key_ranges_;
  common::ObSEArray<common::ObSpatialMBR, 1> mbr_filters_;
  common::ObPosArray range_array_pos_;
  // if the query range was extracted before(include whole range), tsc not need to extract every time
  bool not_need_extract_query_range_;
  // FIXME bin.lb: partition_ranges_ not used, ObTableScanInput keep it for compatibility.
  // common::ObSEArray<ObPartitionScanRanges, 16> partition_ranges_;

  DISALLOW_COPY_AND_ASSIGN(ObTableScanOpInput);
};


class ObTableScanSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  common::ObTableID get_table_loc_id() { return table_loc_id_; }
  uint64_t get_loc_ref_table_id() const { return tsc_ctdef_.scan_ctdef_.ref_table_id_; }
  common::ObTableID get_ref_table_id() const { return ref_table_id_; }
  bool should_scan_index() const { return tsc_ctdef_.scan_ctdef_.ref_table_id_ != ref_table_id_; }
  bool is_index_back() const { return tsc_ctdef_.lookup_ctdef_ != nullptr; }
  bool is_global_index_back() const { return is_index_back() && is_index_global_; }
  /*
   * the range from optimizer must change id to storage scan key id.
   * If the optimizer desired to use index idx to access table A, the origin
   * ObNewRange use A's real table id, but the range is a A(idx) range.
   * We must change this id before send this range to storage layer.
   */
  inline uint64_t get_scan_key_id() const { return tsc_ctdef_.scan_ctdef_.ref_table_id_; }
  int set_pruned_index_name(
      const common::ObIArray<common::ObString> &pruned_index_name,
      common::ObIAllocator &phy_alloc);
  int set_unstable_index_name(
      const common::ObIArray<common::ObString> &unstable_index_name,
      common::ObIAllocator &phy_alloc);
  int set_available_index_name(
      const common::ObIArray<common::ObString> &available_index_name,
      common::ObIAllocator &phy_alloc);
  int set_est_row_count_record(const common::ObIArray<common::ObEstRowCountRecord> &est_records);

  int explain_index_selection_info(char *buf, int64_t buf_len, int64_t &pos) const;

  virtual bool is_table_scan() const override { return true; }
  inline const ObQueryRange &get_query_range() const { return tsc_ctdef_.pre_query_range_; }
  inline uint64_t get_table_loc_id() const { return table_loc_id_; }
  bool use_dist_das() const { return use_dist_das_; }
  int64_t get_rowkey_cnt() const {
    return tsc_ctdef_.scan_ctdef_.table_param_.get_read_info().get_schema_rowkey_count(); }
  const ObIArray<ObColDesc> &get_columns_desc() const {
    return tsc_ctdef_.scan_ctdef_.table_param_.get_read_info().get_columns_desc(); }
  inline void set_spatial_ddl(bool is_spatial_ddl) { is_spatial_ddl_ = is_spatial_ddl; }
  inline bool is_spatial_ddl() const { return is_spatial_ddl_; }
  DECLARE_VIRTUAL_TO_STRING;

public:
  // @param: table_name_
  //         index_name_
  // Currently, those fields will only be used in (g)v$plan_cache_plan so as to find
  // table name of the operator, which means those fields will be used by local plan
  // and remote plan will not use those fields. Therefore, those fields NEED NOT TO BE SERIALIZED.
  common::ObString table_name_; // table name of the table to scan
  common::ObString index_name_; // name of the index to be used
  common::ObTableID table_loc_id_; //table location id
  common::ObTableID ref_table_id_; //main table ref table id
  ObExpr *limit_;
  ObExpr *offset_;
  int64_t frozen_version_; // from hint

  //
  // for dynamic query range prune
  // part_expr_和subpart_expr_为分区表达式,
  // part_dep_cols_和subpart_dep_cols_为分区表达式依赖的cols,
  // 在计算分区表达式前, 会将这些cols对应的datum设置上,数据来源于
  // 该col对应在主键query range中的值; 具体的映射关系由part_range_pos_记录
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_type_;
  share::schema::ObPartitionFuncType subpart_type_;
  ObExpr *part_expr_;
  ObExpr *subpart_expr_;
  common::ObFixedArray<int64_t, common::ObIAllocator> part_range_pos_;
  common::ObFixedArray<int64_t, common::ObIAllocator> subpart_range_pos_;
  ExprFixedArray part_dep_cols_;
  ExprFixedArray subpart_dep_cols_;
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
  common::ObFixedArray<common::ObString, common::ObIAllocator> unstable_index_name_;
  common::ObFixedArray<common::ObTableID, common::ObIAllocator> ddl_output_cids_; //ddl output column ids

  /**
   * the relationship between TableScan and DASScan
   * such as: select c1, c2 from t1 where udf(c3)>0 and c4=0;
   *  +----------------------+
   *  |   output:(c1, c2)    |
   *  |   TableScanOp        |
   *  |   filter:(udf(c3)>0) |
   *  |   access:c1, c2, c3  |
   *  +-----------^----------+
   *              |
   *              |
   *              |
   * +-------------------------+
   * |   output:(c1,c2,c3)     |
   * |   DASScanOp/Storage     |
   * |   filter(c4=0)          |
   * |   access:(c1,c2,c3,c4)  |
   * +-------------------------+
   *
   *
   */
  ObTableScanCtDef tsc_ctdef_;
  ObExpr *pdml_partition_id_;
  AgentVtAccessMeta agent_vt_meta_;

  //all flags
  union {
    uint64_t flags_;
    struct {
      uint64_t use_dist_das_                    : 1; //mark whether this table touch data through distributed DAS
      uint64_t is_vt_mapping_                   : 1; //mark if this table is virtual agent table
      uint64_t is_index_global_                 : 1; //mark if this table is a duplicated table
      uint64_t force_refresh_lc_                : 1;
      uint64_t is_top_table_scan_               : 1;
      uint64_t gi_above_                        : 1;
      uint64_t batch_scan_flag_                 : 1;
      uint64_t report_col_checksum_             : 1;
      uint64_t has_tenant_id_col_               : 1;
      uint64_t is_spatial_ddl_                  : 1;
      uint64_t is_external_table_               : 1;
      uint64_t reserved_                        : 53;
    };
  };
  int64_t tenant_id_col_idx_;
  int64_t partition_id_calc_type_;
};

class ObTableScanOp : public ObOperator
{
  friend class ObDASScanOp;
  friend class ObGlobalIndexLookupOpImpl;
public:
  static constexpr int64_t CHECK_STATUS_ROWS_INTERVAL =  1 << 13;

  ObTableScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObTableScanOp();

  int inner_open() override;
  int inner_rescan() override;
  int switch_iterator() override;
  int inner_get_next_row() override;
  int inner_get_next_batch(const int64_t max_row_cnt) override;
  int inner_close() override;
  int do_init_before_get_row() override;
  void destroy() override;

  void set_iter_end(bool iter_end) { iter_end_ = iter_end; }

  int init_converter();

  void set_report_checksum(bool flag) { report_checksum_ = flag; }
  int reset_sample_scan() { tsc_rtdef_.scan_rtdef_.sample_info_ = nullptr; return close_and_reopen(); }
  virtual void set_need_sample(bool flag) { UNUSED(flag); }
  static int transform_physical_rowid(common::ObIAllocator &allocator,
                                      const common::ObTabletID &scan_tablet_id,
                                      const common::ObArrayWrap<share::schema::ObColDesc> &rowkey_descs,
                                      common::ObNewRange &new_range);

  OB_INLINE bool can_partition_retry()
  {
    return (
         ctx_.get_my_session()->is_user_session() &&
         (! ObStmt::is_dml_write_stmt(ctx_.get_physical_plan_ctx()->get_phy_plan()->get_stmt_type()) )&&
         (! ctx_.get_physical_plan_ctx()->get_phy_plan()->has_for_update() )
        );
  }
protected:
  // Get GI task then update location_idx and $cur_access_tablet_
  // NOTE: set $iter_end_ if no task found.
  int get_access_tablet_loc(ObGranuleTaskInfo &info);
  // Assign GI task ranges to INPUT
  int reassign_task_ranges(ObGranuleTaskInfo &info);

  int local_iter_reuse();
  int set_batch_iter(int64_t group_id);
  void reset_iter_tree_for_rescan();
  bool is_group_rescan() const { return OB_NOT_NULL(output_) && output_ == fold_iter_; }
  int calc_expr_int_value(const ObExpr &expr, int64_t &retval, bool &is_null_value);
  int init_table_scan_rtdef();
  int init_das_scan_rtdef(const ObDASScanCtDef &das_ctdef,
                          ObDASScanRtDef &das_rtdef,
                          const ObDASTableLocMeta *loc_meta);
  int init_attach_scan_rtdef(const ObDASBaseCtDef *attach_ctdef, ObDASBaseRtDef *&attach_rtdef);
  int prepare_scan_range();
  int prepare_batch_scan_range();
  int build_bnlj_params();
  int single_equal_scan_check_type(const ParamStore &param_store, bool& is_same_type);
  bool need_extract_range() const { return MY_SPEC.tsc_ctdef_.pre_query_range_.has_range(); }
  int prepare_single_scan_range(int64_t group_idx = 0);

  int reuse_table_rescan_allocator();

  int local_iter_rescan();
  int close_and_reopen();

  int cherry_pick_range_by_tablet_id(ObDASScanOp *scan_op);
  int can_prune_by_tablet_id(const common::ObTabletID &tablet_id,
                                const common::ObNewRange &scan_range,
                                bool &can_prune);
  int construct_partition_range(ObArenaAllocator &allocator,
                                const share::schema::ObPartitionFuncType part_type,
                                const common::ObIArray<int64_t> &part_range_pos,
                                const ObNewRange &scan_range,
                                const ObExpr *part_expr,
                                const ExprFixedArray &part_dep_cols,
                                bool &can_prune,
                                ObNewRange &part_range);

  int fill_storage_feedback_info();
  void fill_sql_plan_monitor_info();
  //int extract_scan_ranges();
  void fill_table_scan_stat(const ObTableScanStatistic &statistic,
                            ObTableScanStat &scan_stat) const;
  void set_cache_stat(const ObPlanStat &plan_stat);
  int inner_get_next_row_implement();
  int fill_generated_cellid_mbr(const ObObj &cellid, const ObObj &mbr);
  int inner_get_next_spatial_index_row();
  int init_spatial_index_rows();
  void set_real_rescan_cnt(int64_t real_rescan_cnt) { group_rescan_cnt_ = real_rescan_cnt; }
  int64_t get_real_rescan_cnt() { return group_rescan_cnt_; }

  // in_batch_rescan_subplan means the ancestor operator(NLJ/SPF) of TSC uses bacth rescan
  bool in_batch_rescan_subplan()
  {
    return !tsc_rtdef_.bnlj_params_.empty();
  }
  // because of adptive batch rescan in TSC, TSC may performs single-line rescan in some scenarios;
  // need_perform_real_batch_rescan means TSC needs perform a real batch rescan in the adaptive batch-rescan process
  // and the return value changes during execution
  bool need_perform_real_batch_rescan()
  {
    return (OB_NOT_NULL(fold_iter_) && output_ == fold_iter_);
  }
protected:
  int prepare_das_task();
  int prepare_all_das_tasks();
  int prepare_pushdown_limit_param();
  bool has_das_scan_op(const ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_op);
  int create_one_das_task(ObDASTabletLoc *tablet_loc);
  int pushdown_normal_lookup_to_das(ObDASScanOp &target_op);
  int pushdown_attach_task_to_das(ObDASScanOp &target_op);
  int attach_related_taskinfo(ObDASScanOp &target_op, ObDASBaseRtDef *attach_rtdef);
  int do_table_scan();
  int get_next_row_with_das();
  bool need_init_checksum();
  int init_ddl_column_checksum();
  int add_ddl_column_checksum();
  int add_ddl_column_checksum_batch(const int64_t row_count);
  static int corrupt_obj(ObObj &obj);
  int report_ddl_column_checksum();
  int get_next_batch_with_das(int64_t &count, int64_t capacity);
  void replace_bnlj_param(int64_t batch_idx);
  bool need_real_rescan();
  int check_need_real_rescan(bool &bret);
  static int check_is_physical_rowid(ObIAllocator &allocator,
                                     ObRowkey &row_key,
                                     bool &is_physical_rowid,
                                     ObURowIDData &urowid_data);
  static int transform_physical_rowid_rowkey(common::ObIAllocator &allocator,
                                             const common::ObURowIDData &urowid_data,
                                             const common::ObTabletID &scan_tablet_id,
                                             const common::ObArrayWrap<share::schema::ObColDesc> &rowkey_descs,
                                             const bool is_start_key,
                                             common::ObNewRange &new_range,
                                             bool &is_transform_end);
  inline void access_expr_sanity_check() {
    if (OB_UNLIKELY(spec_.need_check_output_datum_ && !MY_SPEC.is_external_table_)) {
      const ObPushdownExprSpec &pd_expr_spec = MY_SPEC.tsc_ctdef_.scan_ctdef_.pd_expr_spec_;
      ObSQLUtils::access_expr_sanity_check(pd_expr_spec.access_exprs_,
                               eval_ctx_, pd_expr_spec.max_batch_size_);


      int64_t stmt_used = tsc_rtdef_.scan_rtdef_.stmt_allocator_.get_alloc()->used();
      if (stmt_used > 2L*1024*1024*1024) {
        SQL_LOG_RET(WARN,OB_ERR_UNEXPECTED,"stmt memory used over the threshold",K(stmt_used));
      }

      int64_t scan_used = tsc_rtdef_.scan_rtdef_.scan_allocator_.get_alloc()->used();
      if (scan_used > 2L*1024*1024*1024) {
        SQL_LOG_RET(WARN,OB_ERR_UNEXPECTED,"scan memory used over the threshold",K(scan_used));
      }
    }
  }
  bool is_foreign_check_nested_session() { return ObSQLUtils::is_fk_nested_sql(&ctx_);}

  class GroupRescanParamGuard
  {
  public:
    GroupRescanParamGuard(ObTableScanRtDef &tsc_rtdef, ParamStore &param_store)
      : tsc_rtdef_(tsc_rtdef),
        param_store_(param_store),
        range_buffer_idx_(0)
    {
      //Save the original state in param store.
      //The param store may be modified during the execution of group rescan.
      //After the execution is completed, the original state needs to be restored.
      for (int64_t i = 0; i < tsc_rtdef_.bnlj_params_.count(); ++i) {
        int64_t param_idx = tsc_rtdef_.bnlj_params_.at(i).param_idx_;
        common::ObObjParam &cur_param = param_store_.at(param_idx);
        tsc_rtdef_.bnlj_params_.at(i).cur_param_ = cur_param;
      }
      range_buffer_idx_ = tsc_rtdef_.range_buffer_idx_;
    }

    void switch_group_rescan_param(int64_t group_idx)
    {
      //replace real param to param store to execute group rescan in TSC
      for (int64_t i = 0; i < tsc_rtdef_.bnlj_params_.count(); ++i) {
        ObSqlArrayObj *array_obj = tsc_rtdef_.bnlj_params_.at(i).gr_param_;
        int64_t param_idx = tsc_rtdef_.bnlj_params_.at(i).param_idx_;
        common::ObObjParam &dst_param = param_store_.at(param_idx);
        dst_param = array_obj->data_[group_idx];
        dst_param.set_param_meta();
      }
      tsc_rtdef_.range_buffer_idx_ = group_idx;
    }

    ~GroupRescanParamGuard()
    {
      //restore the original state to param store
      for (int64_t i = 0; i < tsc_rtdef_.bnlj_params_.count(); ++i) {
        int64_t param_idx = tsc_rtdef_.bnlj_params_.at(i).param_idx_;
        common::ObObjParam &cur_param = param_store_.at(param_idx);
        cur_param = tsc_rtdef_.bnlj_params_.at(i).cur_param_;
      }
      tsc_rtdef_.range_buffer_idx_ = range_buffer_idx_;
    }
  private:
    ObTableScanRtDef &tsc_rtdef_;
    ParamStore &param_store_;
    int64_t range_buffer_idx_;
  };

private:
  const ObTableScanSpec& get_tsc_spec() {return MY_SPEC;}
  const ObTableScanCtDef& get_tsc_ctdef() {return MY_SPEC.tsc_ctdef_;}
  int inner_get_next_row_for_tsc();
  int inner_get_next_batch_for_tsc(const int64_t max_row_cnt);
  int inner_rescan_for_tsc();

  void gen_rand_size_and_skip_bits(const int64_t batch_size, int64_t &rand_size, int64_t &skip_bits);

  void adjust_rand_output_brs(const int64_t rand_skip_bits);
protected:
  DASOpResultIter scan_result_;
  ObTableScanRtDef tsc_rtdef_;
  bool need_final_limit_;
  common::ObLimitParam limit_param_;
  //这个allocator的周期是当table scan context被创建的时候生成，直到table scan被close被reset
  //主要是由于在nested loop join中，table scan operator会被反复的rescan，这个过程中有些数据需要allocator
  //但是用query级别的allocator来说，不合适，会导致这个allocator的内存膨胀厉害，中间结果得不到释放
  //用行级allocator生命周期太短，满足不了需求
  common::ObArenaAllocator *table_rescan_allocator_;
  // this is used for found rows, reset in rescan.
  int64_t input_row_cnt_;
  int64_t output_row_cnt_;
  // 用于保证在没有数据的时候多次调用get_next_row都能返回OB_ITER_END
  bool iter_end_;
  int64_t iterated_rows_;//记录已经迭代的行数
  bool got_feedback_;

  sql::ObVirtualTableResultConverter *vt_result_converter_;

  const uint64_t *cur_trace_id_;
  // for ddl
  common::ObFixedArray<bool, common::ObIAllocator> col_need_reshape_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> column_checksum_;
  int64_t scan_task_id_;
  bool report_checksum_;
  bool in_rescan_;
  ObSpatialIndexCache spat_index_;

  // output_ is used to output data, TSC operator directly invokes output_::get_next_row(s),
  // it points to fold_iter_ in group rescan and iter_tree_ in normal scan.
  ObDASIter *output_;

  // fold_iter_ is used for group rescan, it folds the output of iter_tree_ according to group_idx.
  ObDASGroupFoldIter *fold_iter_;

  // iter_tree_ is used to produce data,
  // for table scan and local index lookup:
  //   iter_tree_ and scan_iter_ are the same, both refer to a ObDASMergeIter,
  // for global index lookup:
  //   iter_tree_ refers to a ObDASLookupIter for lookup and scan_iter_ refers to a ObDASMergeIter for index scan.
  ObDASIter *iter_tree_;
  ObDASMergeIter *scan_iter_;
  int64_t group_rescan_cnt_;
  int64_t group_id_;
 };

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_TABLE_OB_TABLE_SCAN_OP_H_
