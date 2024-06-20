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

#ifndef OCEANBASE_SQL_OB_EXEC_CONTEXT_H
#define OCEANBASE_SQL_OB_EXEC_CONTEXT_H
#include "share/interrupt/ob_global_interrupt_call.h"
#include "lib/net/ob_addr.h"
#include "lib/allocator/page_arena.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/table/ob_virtual_table_ctx.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/monitor/ob_exec_stat.h"
#include "sql/monitor/ob_exec_stat_collector.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/engine/user_defined_function/ob_udf_ctx_mgr.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/optimizer/ob_pwj_comparer.h"
#include "sql/das/ob_das_context.h"
#include "sql/engine/cmd/ob_table_direct_insert_ctx.h"
#include "pl/ob_pl_package_guard.h"
#include "lib/udt/ob_udt_type.h"

#define GET_PHY_PLAN_CTX(ctx) ((ctx).get_physical_plan_ctx())
#define GET_MY_SESSION(ctx) ((ctx).get_my_session())
#define GET_TASK_EXECUTOR_CTX(ctx) ((ctx).get_task_executor_ctx())
#define CREATE_PHY_OPERATOR_CTX(ctx_type, exec_ctx, op_id, op_type, op_ctx) \
  ({void *ptr = NULL; \
    int _ret_ = oceanbase::common::OB_SUCCESS; \
    op_ctx = NULL; \
    if (oceanbase::common::OB_SUCCESS == (_ret_ = exec_ctx.create_phy_op_ctx(op_id, \
                                                                  sizeof(ctx_type), \
                                                                  op_type, ptr))) { \
      op_ctx = new (ptr) ctx_type(exec_ctx); \
      int64_t tenant_id = GET_MY_SESSION(exec_ctx)->get_effective_tenant_id(); \
      if (oceanbase::common::OB_SUCCESS != (_ret_ = op_ctx->init_base(tenant_id))) { \
        SQL_ENG_LOG_RET(WARN, _ret_, "init operator ctx failed", K(_ret_)); \
      } else { \
        op_ctx->set_op_id(op_id); \
        op_ctx->set_op_type(op_type); \
        op_ctx->set_tenant_id(tenant_id); \
        op_ctx->get_monitor_info().open_time_ = oceanbase::common::ObClockGenerator::getClock(); \
      } \
   } \
    _ret_; \
  })
#define GET_PHY_OPERATOR_CTX(ctx_type, exec_ctx, op_id)  \
  static_cast<ctx_type *>(exec_ctx.get_phy_op_ctx(op_id))
#define DAS_CTX(ctx) ((ctx).get_das_ctx())

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}

namespace storage
{
class ObLobAccessCtx;
}

namespace pl
{
class ObPL;

class ObPLExecState;
struct ExecCtxBak;
struct ObPLCtx;
struct ObPLExecRecursionCtx;
class ObPLPackageGuard;
class LinkPLStackGuard;
} // namespace pl

namespace sql
{
class ObPhysicalPlanCtx;
class ObIPhyOperatorInput;
class ObTaskExecutorCtx;
class ObSQLSessionInfo;
class ObSQLSessionMgr;
class ObExprOperatorCtx;
class ObPxSqcHandler;
class ObOpSpec;
class ObOperator;
class ObOpInput;
class ObSql;
struct ObEvalCtx;
typedef  common::ObArray<const common::ObIArray<int64_t> *> ObRowIdListArray;
struct ColumnContent;
typedef common::ObFixedArray<ColumnContent, common::ObIAllocator> ColContentFixedArray;
// Physical operator kit: operator specification, operator, operator input
struct ObOperatorKit
{
  ObOperatorKit() : spec_(NULL), op_(NULL), input_(NULL)
  {
  }
  const ObOpSpec *spec_;
  ObOperator *op_;
  ObOpInput *input_;
};

// Physical operator kit store
class ObOpKitStore
{
public:
  ObOpKitStore() : size_(0), kits_(NULL)
  {
  }

  int init(common::ObIAllocator &alloc, const int64_t size);
  ObOperatorKit *get_operator_kit(const uint64_t id) const
  {
    return id < size_ ? &kits_[id] : NULL;
  }

  // destroy ObOperator, and deconstruct ObOpInput
  void destroy();

  void reset() { size_ = 0; kits_ = NULL; }

  int64_t size_;
  ObOperatorKit *kits_;
};

struct ObUserLoggingCtx
{
  friend class ObExecContext;
  friend class Guard;
  class Guard
  {
  public:
    explicit Guard(ObUserLoggingCtx &ctx) : ctx_(ctx) {}
    ~Guard() { ctx_.reset(); }
  private:
    ObUserLoggingCtx &ctx_;
  };
  ObUserLoggingCtx() : column_name_(NULL), row_num_(-1) {}
  inline bool skip_logging() const { return NULL == column_name_ || row_num_ <= 0; }
  inline const ObString *get_column_name() const  { return column_name_; }
  inline int64_t get_row_num() const { return row_num_; }
private:
  inline void reset()
  {
    column_name_ = NULL;
    row_num_ = -1;
  }
private:
  const ObString *column_name_;
  int64_t row_num_;
};

class ObIExtraStatusCheck;
struct ObTempExprBackupCtx;

// ObExecContext可以序列化，但不能反序列化；
// 而ObDesExecContext不能序列化，但可以反序列化；
// 用ObExecContext序列化，然后相对应地用ObDesExecContext反序列化
class ObExecContext
{
public:
  friend struct pl::ExecCtxBak;
  friend class pl::LinkPLStackGuard;
  friend class LinkExecCtxGuard;

public:
  explicit ObExecContext(common::ObIAllocator &allocator);
  virtual ~ObExecContext();

  // 用于result_set遇到violation重试的时候，重新生成plan
  void reset_op_env();
  void reset_op_ctx();

  bool is_valid() const
  {
    return (NULL != phy_plan_ctx_ && NULL != my_session_);
  }
  /**
   * @brief initialize execute context, must call before calling any function
   */
  int init_phy_op(uint64_t phy_op_size);
  int init_expr_op(const uint64_t expr_op_size, ObIAllocator *allocator = NULL);
  void reset_expr_op();
  inline bool is_expr_op_ctx_inited() { return expr_op_size_ > 0 && NULL != expr_op_ctx_store_; }
  int get_convert_charset_allocator(common::ObArenaAllocator *&allocator);
  void try_reset_convert_charset_allocator();

  void destroy_eval_allocator();

  /**
   * @brief query created phy op space size
   */
  inline int64_t get_phy_op_size() const { return phy_op_size_; }

  /**
   * @brief allocate the memory of expr operator context.
   */
  template<typename ObExprCtxType>
  int create_expr_op_ctx(uint64_t op_id, ObExprCtxType *&op_ctx);
  int create_expr_op_ctx(uint64_t op_id, int64_t op_ctx_size, void *&op_ctx);
  /**
   * @brief get expr operator context object from exec context.
   * @param op_type: for regexp and like expr, whose id is not unique, but (type, id) is unique.
   */
  void *get_expr_op_ctx(uint64_t op_id);

  ObExprOperatorCtx **get_expr_op_ctx_store() { return expr_op_ctx_store_; }
  void set_expr_op_ctx_store(ObExprOperatorCtx **expr_op_ctx_store) { expr_op_ctx_store_ = expr_op_ctx_store; }
  uint64_t get_expr_op_size() const { return expr_op_size_; }
  void set_expr_op_size(uint64_t expr_op_size) { expr_op_size_ = expr_op_size; }

  /**
   * @brief create physical plan context object from exec context.
   */
  int create_physical_plan_ctx();
  /**
   * @brief set physical plan context object from exec context.
   */
  inline void set_physical_plan_ctx(ObPhysicalPlanCtx *plan_ctx);
  void reference_my_plan(const ObPhysicalPlan *my_plan);
  /**
   * @brief get physical plan context from exec context.
   */
  inline ObPhysicalPlanCtx *get_physical_plan_ctx() const;
  /**
   * @brief set session info, for trans control
   */
  inline void set_my_session(ObSQLSessionInfo *session);
  /**
   * @brief get session info, for trans control
   */
  inline ObSQLSessionInfo *get_my_session() const;
  //get the parent execute context in nested sql
  ObExecContext *get_parent_ctx() { return parent_ctx_; }
  int64_t get_nested_level() const { return nested_level_; }
  /**
   * @brief set sql proxy
   */
  inline void set_sql_proxy(common::ObMySQLProxy *sql_proxy);
  /**
   * @brief get sql proxy
   */
  inline common::ObMySQLProxy *get_sql_proxy();

  /**
   * @brief get add, for plan cache show stat
   */
  const common::ObAddr& get_addr() const;

  inline void set_virtual_table_ctx(const ObVirtualTableCtx &virtual_table_ctx);
  /**
   * @brief get virtual table scannerable factory,
   * for creating virtual table iterator
   */
  ObVirtualTableCtx get_virtual_table_ctx();
  /**
   * @brief get executor context from exec context.
   */
  inline const ObTaskExecutorCtx &get_task_exec_ctx() const;
  inline ObTaskExecutorCtx &get_task_exec_ctx();
  inline ObTaskExecutorCtx *get_task_executor_ctx();
  inline ObDASCtx &get_das_ctx() { return das_ctx_; }
  /**
   * @brief get session_mgr.
   */
  inline ObSQLSessionMgr *get_session_mgr() const;

  /**
   * @brief get execution stat from all tasks
   */
  ObExecStatCollector &get_exec_stat_collector();

  /**
   * @brief set admission version
   */
  void set_admission_version(uint64_t admission_version);

  /**
   * @brief get admission version
   */
  uint64_t get_admission_version() const;

  /**
   * @brief get admission addr set
   */
  hash::ObHashMap<ObAddr, int64_t> &get_admission_addr_map();

  /**
   * @brief get allocator.
   */
  common::ObIAllocator &get_sche_allocator();
  common::ObIAllocator &get_allocator();

  int64_t to_string(char *buf, const int64_t buf_len) const { UNUSED(buf); UNUSED(buf_len); return 0; }
  static const uint64_t VERSION_SHIFT = 32;
  static const uint64_t PHY_OP_SIZE_MASK = 0xFFFFFFFF;
  uint64_t combine_version_and_op_size(uint64_t ser_version, uint64_t phy_op_size) const
  {
    return (ser_version << VERSION_SHIFT) | phy_op_size;
  }
  uint64_t get_ser_version(uint64_t combine_value) const
  {
    return combine_value >> VERSION_SHIFT;
  }
  uint64_t get_phy_op_size(uint64_t combine_value) const
  {
    return combine_value & PHY_OP_SIZE_MASK;
  }
  ObGIPruningInfo &get_gi_pruning_info() { return gi_pruning_info_; }
  const ObGIPruningInfo &get_gi_pruning_info() const { return gi_pruning_info_; }

  bool has_non_trivial_expr_op_ctx() const { return has_non_trivial_expr_op_ctx_; }
  void set_non_trivial_expr_op_ctx(bool v) { has_non_trivial_expr_op_ctx_ = v; }
  inline bool &get_tmp_alloc_used() { return tmp_alloc_used_; }
  // set write branch id for DML write
  void set_branch_id(const int16_t branch_id) { das_ctx_.set_write_branch_id(branch_id); }
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
protected:
  uint64_t get_ser_version() const;
  const static uint64_t SER_VERSION_0 = 0;
  const static uint64_t SER_VERSION_1 = 1;

public:
  ObStmtFactory *get_stmt_factory();
  ObRawExprFactory *get_expr_factory();

  int check_status();
  int fast_check_status(const int64_t n = 0xFF);
  int check_status_ignore_interrupt();
  int fast_check_status_ignore_interrupt(const int64_t n = 0xFF);

  void set_outline_params_wrapper(const share::schema::ObOutlineParamsWrapper *params)
  {
    outline_params_wrapper_ = params;
  }
  const share::schema::ObOutlineParamsWrapper *get_outline_params_wrapper() const
  {
    return outline_params_wrapper_;
  }

  void set_execution_id(uint64_t execution_id) { execution_id_ = execution_id; }
  uint64_t get_execution_id() const { return execution_id_; }

//  const common::ObInterruptibleTaskID &get_interrupt_id() { return interrupt_id_;}
//  void set_interrupt_id(const common::ObInterruptibleTaskID &int_id) { interrupt_id_ = int_id; }

  void set_sql_ctx(ObSqlCtx *ctx) { sql_ctx_ = ctx; das_ctx_.set_sql_ctx(ctx); }
  ObSqlCtx *get_sql_ctx() { return sql_ctx_; }
  const ObSqlCtx *get_sql_ctx() const { return sql_ctx_; }
  pl::ObPLContext *get_pl_stack_ctx() { return pl_stack_ctx_; }
  inline bool use_remote_sql() const
  {
    bool bret = false;
    if (OB_NOT_NULL(phy_plan_ctx_)) {
      bret = (!phy_plan_ctx_->get_remote_sql_info().remote_sql_.empty());
    }
    return bret;
  }

  bool &get_need_disconnect_for_update() { return need_disconnect_; }
  bool need_disconnect() const { return need_disconnect_; }
  void set_need_disconnect(bool need_disconnect) { need_disconnect_ = need_disconnect; }
  inline pl::ObPL *get_pl_engine() { return GCTX.pl_engine_; }
  inline pl::ObPLCtx *get_pl_ctx() { return pl_ctx_; }
  inline void set_pl_ctx(pl::ObPLCtx *pl_ctx) { pl_ctx_ = pl_ctx; }
  pl::ObPLPackageGuard* get_package_guard();
  int get_package_guard(pl::ObPLPackageGuard *&package_guard);
  inline pl::ObPLPackageGuard* get_original_package_guard() { return package_guard_; }
  inline void set_package_guard(pl::ObPLPackageGuard* v) { package_guard_ = v; }
  int init_pl_ctx();

  ObPartIdRowMapManager& get_part_row_manager() { return part_row_map_manager_; }

  uint64_t get_min_cluster_version() const;
  int reset_one_row_id_list(const common::ObIArray<int64_t> *row_id_list);
  const ObRowIdListArray &get_row_id_list_array() const { return row_id_list_array_; }
  int add_row_id_list(const common::ObIArray<int64_t> *row_id_list);
  void reset_row_id_list() { row_id_list_array_.reset(); total_row_count_ = 0;}
  int64_t get_row_id_list_total_count() const { return total_row_count_; }
  void set_plan_start_time(int64_t t) { phy_plan_ctx_->set_plan_start_time(t); }
  int64_t get_plan_start_time() const { return phy_plan_ctx_->get_plan_start_time(); }
  void set_is_ps_prepare_stage(bool v) { is_ps_prepare_stage_ = v; }
  bool is_ps_prepare_stage() const { return is_ps_prepare_stage_; }

  bool is_reusable_interm_result() const { return reusable_interm_result_; }
  void set_reusable_interm_result(const bool reusable) { reusable_interm_result_ = reusable; }
  void set_end_trans_async(bool is_async) {is_async_end_trans_ = is_async;}
  bool is_end_trans_async() {return is_async_end_trans_;}
  inline TransState &get_trans_state() {return trans_state_;}
  inline const TransState &get_trans_state() const {return trans_state_;}
  int add_temp_table_interm_result_ids(uint64_t temp_table_id,
                                       const common::ObAddr &sqc_addr,
                                       const ObIArray<uint64_t> &interm_result_ids);
  // for granule iterator
  int get_gi_task_map(GIPrepareTaskMap *&gi_prepare_task_map);

  void set_use_temp_expr_ctx_cache(bool v) { use_temp_expr_ctx_cache_ = v; }

  // for udf
  int get_udf_ctx_mgr(ObUdfCtxMgr *&udf_ctx_mgr);

  //for call procedure
  ObNewRow *get_output_row() { return output_row_; }
  void set_output_row(ObNewRow *row) { output_row_ = row; }

  ColumnsFieldIArray *get_field_columns() { return field_columns_; }
  void set_field_columns(ColumnsFieldIArray *field_columns)
  {
    field_columns_ = field_columns;
  }

  void set_direct_local_plan(bool v) { is_direct_local_plan_ = v; }
  bool get_direct_local_plan() const { return is_direct_local_plan_; }

  ObPxSqcHandler *get_sqc_handler() { return sqc_handler_; }
  void set_sqc_handler(ObPxSqcHandler *sqc_handler) { sqc_handler_ = sqc_handler; }
  void set_px_task_id(const int64_t task_id) { px_task_id_ = task_id; }
  int64_t get_px_task_id() const { return px_task_id_; }
  void set_px_sqc_id(const int64_t sqc_id) { px_sqc_id_ = sqc_id; }
  int64_t get_px_sqc_id() const { return px_sqc_id_; }

  common::ObIArray<ObJoinFilterDataCtx> &get_bloom_filter_ctx_array() { return bloom_filter_ctx_array_; }

  char **get_frames() const { return frames_; }
  void set_frames(char **frames) { frames_ = frames; }
  uint64_t get_frame_cnt() const { return frame_cnt_; }
  void set_frame_cnt(uint64_t frame_cnt) { frame_cnt_ = frame_cnt; }

  ObOperatorKit *get_operator_kit(const uint64_t id) const
  {
    return op_kit_store_.get_operator_kit(id);
  }
  ObOpKitStore &get_kit_store()
  {
    return op_kit_store_;
  }
  common::ObArenaAllocator &get_eval_res_allocator() { return eval_res_allocator_; }
  common::ObArenaAllocator &get_eval_tmp_allocator() { return eval_tmp_allocator_; }
  int get_temp_expr_eval_ctx(const ObTempExpr &temp_expr, ObTempExprCtx *&temp_expr_ctx);

  void clean_resolve_ctx();
  int init_physical_plan_ctx(const ObPhysicalPlan &plan);

  ObIArray<ObSqlTempTableCtx>& get_temp_table_ctx() { return temp_ctx_; }

  int get_pwj_map(PWJTabletIdMap *&pwj_map);
  PWJTabletIdMap *get_pwj_map() { return pwj_map_; }
  void set_partition_id_calc_type(PartitionIdCalcType calc_type) { calc_type_ = calc_type; }
  PartitionIdCalcType get_partition_id_calc_type() { return calc_type_; }
  void set_fixed_id(ObObjectID fixed_id) { fixed_id_ = fixed_id; }
  ObObjectID get_fixed_id() { return fixed_id_; }
  const Ob2DArray<ObPxTabletRange> &get_partition_ranges() const { return part_ranges_; }
  int set_partition_ranges(const Ob2DArray<ObPxTabletRange> &part_ranges,
                           char *buf = NULL, int64_t max_size = 0);
  int fill_px_batch_info(
      ObBatchRescanParams &params,
      int64_t batch_id,
      sql::ObExpr::ObExprIArray &array);
  int64_t get_px_batch_id() { return px_batch_id_; }

  ObDmlEventType get_dml_event() const { return dml_event_; }
  void set_dml_event(const ObDmlEventType dml) { dml_event_ = dml; }
  const ColContentFixedArray *get_update_columns() const { return update_columns_; }
  void set_update_columns(const ColContentFixedArray *update_columns) { update_columns_ = update_columns; }
  void set_expect_range_count(int64_t cnt) { expect_range_count_ = cnt; }
  int64_t get_expect_range_count() { return expect_range_count_; }
  int add_extra_check(ObIExtraStatusCheck &extra_check)
  {
    return extra_status_check_.add_last(&extra_check)
        ? common::OB_SUCCESS
        : common::OB_ERR_UNEXPECTED;
  }
  int del_extra_check(ObIExtraStatusCheck &extra_check)
  {
    extra_status_check_.remove(&extra_check);
    return common::OB_SUCCESS;
  }
  int64_t get_register_op_id() { return register_op_id_; }
  void set_register_op_id(int64_t id) { register_op_id_ = id; }
  bool is_rt_monitor_node_registered() { return OB_INVALID_ID != register_op_id_; }
  void set_mem_attr(const common::ObMemAttr& attr)
  {
    sche_allocator_.set_attr(attr);
    eval_res_allocator_.set_attr(attr);
    eval_tmp_allocator_.set_attr(attr);
  }
  ObTableDirectInsertCtx &get_table_direct_insert_ctx() { return table_direct_insert_ctx_; }
  void set_errcode(const int errcode) { ATOMIC_STORE(&errcode_, errcode); }
  int get_errcode() const { return ATOMIC_LOAD(&errcode_); }
  hash::ObHashMap<uint64_t, void*> &get_dblink_snapshot_map() { return dblink_snapshot_map_; }
  int get_sqludt_meta_by_subschema_id(uint16_t subschema_id, ObSqlUDTMeta &udt_meta);
  int get_subschema_id_by_udt_id(uint64_t udt_type_id,
                                 uint16_t &subschema_id,
                                 share::schema::ObSchemaGetterGuard *schema_guard = NULL);

  ObExecFeedbackInfo &get_feedback_info() { return fb_info_; };
  inline void set_cur_rownum(int64_t cur_rownum) { user_logging_ctx_.row_num_ = cur_rownum; }
  inline int64_t get_cur_rownum() const { return user_logging_ctx_.row_num_; }
  inline void set_cur_column_name(const ObString *column_name)
  { user_logging_ctx_.column_name_ = column_name; }
  inline ObUserLoggingCtx *get_user_logging_ctx() { return &user_logging_ctx_; }
  bool use_temp_expr_ctx_cache() const { return use_temp_expr_ctx_cache_; }
  bool has_dynamic_values_table() const {
    bool ret = false;
    if (NULL != phy_plan_ctx_) {
      ret = phy_plan_ctx_->get_array_param_groups().count() > 0;
    }
    return ret;
  }
  int get_local_var_array(int64_t local_var_array_id, const ObSolidifiedVarsContext *&var_array);
  void set_is_online_stats_gathering(bool v) { is_online_stats_gathering_ = v; }
  bool is_online_stats_gathering() const { return is_online_stats_gathering_; }

  int get_lob_access_ctx(ObLobAccessCtx *&lob_access_ctx);

private:
  int build_temp_expr_ctx(const ObTempExpr &temp_expr, ObTempExprCtx *&temp_expr_ctx);
  int set_phy_op_ctx_ptr(uint64_t index, void *phy_op);
  int check_extra_status();
  void *get_phy_op_ctx_ptr(uint64_t index) const;
  void set_pl_stack_ctx(pl::ObPLContext *pl_stack_ctx) { pl_stack_ctx_ = pl_stack_ctx; }
  //set the parent execute context in nested sql
  void set_parent_ctx(ObExecContext *parent_ctx) { parent_ctx_ = parent_ctx; }
  void set_nested_level(int64_t nested_level) { nested_level_ = nested_level; }
protected:
  /**
   * @brief the memory of exec context.
   * ------------------------------------------------
   * execute alloc memory for executor
   * such as, create ObJob, split ObTask
   * ------------------------------------------------
   * phy_op_ctx_store_ -> an array of dynamic size,
   * the data type is void*, allocated by allocator_
   * ------------------------------------------------
   * phy_plan_ctx_ -> an object of ObPhysicalPlanCtx
   * allocated after phy_op_ctx_store_ by allocator_
   * ------------------------------------------------
   * physical operator input parameter ->
   * is not necessary, corresonds with specified operator,
   * be created when executor schedule physical plan
   * ------------------------------------------------
   * memory hold by physical operator ->
   * each operator corresponds to an operator context
   * and a cur_row
   * they are created when operator is opened, and
   * cur_row is referenced by operator context,
   * operator context can be find in exec context by
   * operator id
   * ------------------------------------------------
   * temporary running memory -> is used
   * when operator is executed
   * ------------------------------------------------
   */
  // 用于分布式执行的调度线程（allocator不能并发alloc和free）
  common::ObArenaAllocator sche_allocator_;
  common::ObIAllocator &allocator_;
  /**
   * @brief phy_op_size_, the physical operator size in physical plan
   * phy_op_store_, an array of dynamic size
   */
  uint64_t  phy_op_size_;
  void **phy_op_ctx_store_;
  ObIPhyOperatorInput **phy_op_input_store_;
  ObPhysicalPlanCtx *phy_plan_ctx_;
  uint64_t expr_op_size_;
  ObExprOperatorCtx **expr_op_ctx_store_;
  ObTaskExecutorCtx task_executor_ctx_;
  ObSQLSessionInfo *my_session_;
  common::ObMySQLProxy *sql_proxy_;
  ObExecStatCollector exec_stat_collector_;
  ObStmtFactory *stmt_factory_;
  ObRawExprFactory *expr_factory_;
  const share::schema::ObOutlineParamsWrapper *outline_params_wrapper_;
  uint64_t execution_id_;
  //common::ObInterruptibleTaskID interrupt_id_;
  bool has_non_trivial_expr_op_ctx_;
  ObSqlCtx *sql_ctx_;
  pl::ObPLContext *pl_stack_ctx_;
  bool need_disconnect_; // 是否需要断掉与客户端的连接
  //@todo: (linlin.xll) ObPLCtx is ambiguous with ObPLContext, need to rename it
  pl::ObPLCtx *pl_ctx_;
  pl::ObPLPackageGuard *package_guard_;

  ObPartIdRowMapManager part_row_map_manager_;
  const common::ObIArray<int64_t> *row_id_list_;
  // for px insert into values
  ObRowIdListArray row_id_list_array_;
  //判断现在执行的计划是否为演进过程中的计划
  int64_t total_row_count_;
  // Interminate result of index building is reusable, reused in build index retry with same snapshot.
  // Reusable intermediate result is not deleted in the close phase, deleted deliberately after
  // execution is completed.
  bool reusable_interm_result_;
  // end_trans时是否使用异步end trans
  bool is_async_end_trans_;
  /*
   * 用于记录事务语句是否执行过，然后判断对应的end语句是否需执行
   */
  TransState trans_state_;
  /*
   * gi task buffer, no need to serialize.
   * @brief The key is table scan operator's id,
   *        The value is the gi task info.
   * */
  GIPrepareTaskMap *gi_task_map_;

  /*
   * for dll udf
   * */
  ObUdfCtxMgr *udf_ctx_mgr_;
  // for call procedure_;
  ObNewRow *output_row_;
  ColumnsFieldIArray *field_columns_;
  //记录当前执行plan是否为直接获取的local计划
  bool is_direct_local_plan_;

  ObPxSqcHandler *sqc_handler_;

  // for ddl sstable insert
  int64_t px_task_id_;
  int64_t px_sqc_id_;

  //bloom filter ctx array
  common::ObArray<ObJoinFilterDataCtx> bloom_filter_ctx_array_;

  // data frames and count
  char **frames_;
  uint64_t frame_cnt_;

  ObOpKitStore op_kit_store_;

  // expression evaluating allocator
  common::ObArenaAllocator eval_res_allocator_;
  common::ObArenaAllocator eval_tmp_allocator_;
  ObTMArray<ObSqlTempTableCtx> temp_ctx_;

  // 用于 NLJ 场景下对右侧分区表 TSC 扫描做动态 pruning
  ObGIPruningInfo gi_pruning_info_;

  // just for convert charset in query response result
  lib::MemoryContext convert_allocator_;
  PWJTabletIdMap* pwj_map_;
  // the following two parameters only used in calc_partition_id expr
  PartitionIdCalcType calc_type_;
  ObObjectID fixed_id_;    // fixed part id or fixed subpart ids

  // sample result
  Ob2DArray<ObPxTabletRange> part_ranges_;
  int64_t check_status_times_;
  ObIVirtualTableIteratorFactory *vt_ift_;

  // for px batch rescan
  int64_t px_batch_id_;

  uint64_t admission_version_;
  hash::ObHashMap<ObAddr, int64_t> admission_addr_map_;
  // used for temp expr ctx manager
  bool use_temp_expr_ctx_cache_;
  hash::ObHashMap<int64_t, int64_t> temp_expr_ctx_map_;
  // for pl/trigger
  ObDmlEventType dml_event_;
  const ColContentFixedArray *update_columns_;
  // -----------------
  // for object sample
  int64_t expect_range_count_;
  common::ObDList<ObIExtraStatusCheck> extra_status_check_;
  // -----------------
  // ObDASCtx contain ALL table locations of this query
  // Note: NOT ONLY the locations processed by one of the query task
  // ObDASCtx contain the query snapshot info of this query
  ObDASCtx das_ctx_;
  //to link the parent exec ctx in the nested sql
  //in order to access the parent sql attributes,
  //such as the mutating option checking or nested sql constraining checking
  ObExecContext *parent_ctx_;
  int64_t nested_level_; //the number of recursive SQL levels
  bool is_ps_prepare_stage_;
  // for sql plan monitor
  int64_t register_op_id_;
  // indicate if eval_tmp_allocator_ is used
  bool tmp_alloc_used_;
  // -------------------
  // for direct insert
  ObTableDirectInsertCtx table_direct_insert_ctx_;
  // for deadlock detect, set in do_close_plan
  int errcode_;
  hash::ObHashMap<uint64_t, void*> dblink_snapshot_map_;
  // for feedback
  ObExecFeedbackInfo fb_info_;
  // for dml report user warning/error at specific row and column
  ObUserLoggingCtx user_logging_ctx_;
  // for online stats gathering
  bool is_online_stats_gathering_;
  //---------------

  ObLobAccessCtx *lob_access_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExecContext);
};

template<typename ObExprCtxType>
int ObExecContext::create_expr_op_ctx(uint64_t op_id, ObExprCtxType *&op_ctx)
{
  void *op_ctx_ptr = NULL;
  int ret = create_expr_op_ctx(op_id, sizeof(ObExprCtxType), op_ctx_ptr);
  op_ctx = (OB_SUCC(ret) && !OB_ISNULL(op_ctx_ptr)) ? new (op_ctx_ptr) ObExprCtxType() : NULL;
  return ret;
}

inline void ObExecContext::set_physical_plan_ctx(ObPhysicalPlanCtx *plan_ctx)
{
  phy_plan_ctx_ = plan_ctx;
}

inline void ObExecContext::reference_my_plan(const ObPhysicalPlan *my_plan)
{
  if (sql_ctx_ != nullptr) {
    sql_ctx_->cur_plan_ = my_plan;
  }
  if (phy_plan_ctx_ != nullptr) {
    phy_plan_ctx_->set_phy_plan(my_plan);
  }
}

inline void ObExecContext::set_my_session(ObSQLSessionInfo *session)
{
  my_session_ = session;
  set_mem_attr(ObMemAttr(session->get_effective_tenant_id(),
                         ObModIds::OB_SQL_EXEC_CONTEXT,
                         ObCtxIds::EXECUTE_CTX_ID));
}

inline ObSQLSessionInfo *ObExecContext::get_my_session() const
{
  return my_session_;
}

inline void ObExecContext::set_sql_proxy(common::ObMySQLProxy *sql_proxy)
{
  UNUSED(sql_proxy);
}

inline common::ObMySQLProxy *ObExecContext::get_sql_proxy()
{
  return GCTX.sql_proxy_;
}

inline void ObExecContext::set_virtual_table_ctx(const ObVirtualTableCtx &virtual_table_ctx)
{
  UNUSED(virtual_table_ctx);
}

inline ObPhysicalPlanCtx *ObExecContext::get_physical_plan_ctx() const
{
  return phy_plan_ctx_;
}

inline const ObTaskExecutorCtx &ObExecContext::get_task_exec_ctx() const
{
  return task_executor_ctx_;
}

inline ObTaskExecutorCtx &ObExecContext::get_task_exec_ctx()
{
  return task_executor_ctx_;
}

inline ObTaskExecutorCtx *ObExecContext::get_task_executor_ctx()
{
  return &task_executor_ctx_;
}

inline ObSQLSessionMgr *ObExecContext::get_session_mgr() const
{
  return GCTX.session_mgr_;
}

inline ObExecStatCollector &ObExecContext::get_exec_stat_collector()
{
  return exec_stat_collector_;
}

inline void ObExecContext::set_admission_version(uint64_t admission_version)
{
  admission_version_ = admission_version;
}

inline uint64_t ObExecContext::get_admission_version() const
{
  return admission_version_;
}

inline hash::ObHashMap<ObAddr, int64_t> &ObExecContext::get_admission_addr_map()
{
  return admission_addr_map_;
}

struct ObTempExprCtxReplaceGuard
{
public:
  ObTempExprCtxReplaceGuard(ObExecContext &exec_ctx, ObTempExprCtx &temp_expr_ctx)
    : exec_ctx_(exec_ctx),
      frames_(exec_ctx.get_frames()),
      frame_cnt_(exec_ctx.get_frame_cnt()),
      expr_op_size_(exec_ctx.get_expr_op_size()),
      expr_op_ctx_store_(exec_ctx.get_expr_op_ctx_store())
  {
    exec_ctx.set_frame_cnt(temp_expr_ctx.frame_cnt_);
    exec_ctx.set_frames(temp_expr_ctx.frames_);
    exec_ctx.set_expr_op_ctx_store(temp_expr_ctx.expr_op_ctx_store_);
    exec_ctx.set_expr_op_size(temp_expr_ctx.expr_op_size_);
  }

  ~ObTempExprCtxReplaceGuard()
  {
    exec_ctx_.set_frames(frames_);
    exec_ctx_.set_frame_cnt(frame_cnt_);
    exec_ctx_.set_expr_op_size(expr_op_size_);
    exec_ctx_.set_expr_op_ctx_store(expr_op_ctx_store_);
  }

private:
  ObExecContext &exec_ctx_;
  char **frames_;
  uint64_t frame_cnt_;
  uint64_t expr_op_size_;
  ObExprOperatorCtx **expr_op_ctx_store_;
};

class ObIExtraStatusCheck : public common::ObDLinkBase<ObIExtraStatusCheck>
{
public:
  virtual ~ObIExtraStatusCheck() {}
  virtual const char *name() const = 0;
  virtual int check() const = 0;
  class Guard
  {
  public:
    Guard(ObExecContext &ctx, ObIExtraStatusCheck &checker);
    ~Guard();
  private:
    ObExecContext &ctx_;
    ObIExtraStatusCheck &checker_;
  };
};

inline ObIExtraStatusCheck::Guard::Guard(ObExecContext &ctx, ObIExtraStatusCheck &checker)
  : ctx_(ctx), checker_(checker)
{
  int ret = ctx.add_extra_check(checker);
  if (OB_SUCCESS != ret) {
    SQL_ENG_LOG(ERROR, "add extra checker failed", K(ret));
  }
}

inline ObIExtraStatusCheck::Guard::~Guard()
{
  ctx_.del_extra_check(checker_);
}

}
}

#endif //OCEANBASE_SQL_OB_EXEC_CONTEXT_H
