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
#include "sql/executor/ob_scheduler_thread_ctx.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/monitor/ob_exec_stat.h"
#include "sql/monitor/ob_exec_stat_collector.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/engine/user_defined_function/ob_udf_ctx_mgr.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/optimizer/ob_pwj_comparer.h"

#define GET_PHY_PLAN_CTX(ctx) ((ctx).get_physical_plan_ctx())
#define GET_MY_SESSION(ctx) ((ctx).get_my_session())
#define GET_TASK_EXECUTOR_CTX(ctx) ((ctx).get_task_executor_ctx())
#define CREATE_PHY_OPERATOR_CTX(ctx_type, exec_ctx, op_id, op_type, op_ctx)                      \
  ({                                                                                             \
    void* ptr = NULL;                                                                            \
    int _ret_ = oceanbase::common::OB_SUCCESS;                                                   \
    op_ctx = NULL;                                                                               \
    if (oceanbase::common::OB_SUCCESS ==                                                         \
        (_ret_ = exec_ctx.create_phy_op_ctx(op_id, sizeof(ctx_type), op_type, ptr))) {           \
      op_ctx = new (ptr) ctx_type(exec_ctx);                                                     \
      int64_t tenant_id = GET_MY_SESSION(exec_ctx)->get_effective_tenant_id();                   \
      if (oceanbase::common::OB_SUCCESS != (_ret_ = op_ctx->init_base(tenant_id))) {             \
        SQL_ENG_LOG(WARN, "init operator ctx failed", K(_ret_));                                 \
      } else {                                                                                   \
        op_ctx->set_op_id(op_id);                                                                \
        op_ctx->set_op_type(op_type);                                                            \
        op_ctx->set_tenant_id(tenant_id);                                                        \
        op_ctx->get_monitor_info().open_time_ = oceanbase::common::ObClockGenerator::getClock(); \
      }                                                                                          \
    }                                                                                            \
    _ret_;                                                                                       \
  })
#define GET_PHY_OPERATOR_CTX(ctx_type, exec_ctx, op_id) static_cast<ctx_type*>(exec_ctx.get_phy_op_ctx(op_id))

#define CREATE_PHY_OP_INPUT(input_param_type, ctx, op_id, op_type, op_input)                         \
  ({                                                                                                 \
    oceanbase::sql::ObIPhyOperatorInput* _ptr_ = NULL;                                               \
    int _ret_ = oceanbase::common::OB_SUCCESS;                                                       \
    op_input = NULL;                                                                                 \
    if (oceanbase::common::OB_SUCCESS == (_ret_ = ctx.create_phy_op_input(op_id, op_type, _ptr_))) { \
      op_input = static_cast<input_param_type*>(_ptr_);                                              \
    }                                                                                                \
    _ret_;                                                                                           \
  })
#define GET_PHY_OP_INPUT(input_param_type, ctx, op_id) static_cast<input_param_type*>(ctx.get_phy_op_input(op_id))

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}

namespace sql {
class ObPhysicalPlanCtx;
class ObIPhyOperatorInput;
class ObTaskExecutorCtx;
class ObSQLSessionInfo;
class ObPlanCacheManager;
class ObSQLSessionMgr;
class ObExprOperatorCtx;
class ObDistributedExecContext;
class ObPxSqcHandler;
class ObOpSpec;
class ObOperator;
class ObOpInput;
class ObSql;
class ObEvalCtx;
typedef common::ObArray<const common::ObIArray<int64_t>*> ObRowIdListArray;
// Physical operator kit: operator specification, operator, operator input
struct ObOperatorKit {
  ObOperatorKit() : spec_(NULL), op_(NULL), input_(NULL)
  {}
  const ObOpSpec* spec_;
  ObOperator* op_;
  ObOpInput* input_;
};

// Physical operator kit store
class ObOpKitStore {
public:
  ObOpKitStore() : size_(0), kits_(NULL)
  {}

  int init(common::ObIAllocator& alloc, const int64_t size);
  ObOperatorKit* get_operator_kit(const uint64_t id) const
  {
    return id < size_ ? &kits_[id] : NULL;
  }

  // destroy ObOperator, and deconstruct ObOpInput
  void destroy();

  void reset()
  {
    size_ = 0;
    kits_ = NULL;
  }

  int64_t size_;
  ObOperatorKit* kits_;
};

class ObQueryExecCtx;
class ObPhyOperator;
// ObExecContext serialize
// ObDesExecContext for deseralize
class ObExecContext {
public:
  /**
   * some operator need different rescan logic for GI
   */
  class ObPlanRestartGuard {
  public:
    explicit ObPlanRestartGuard(ObExecContext& ctx) : restart_plan_(ctx.get_restart_plan())
    {
      restart_plan_ = true;
    }
    virtual ~ObPlanRestartGuard()
    {
      restart_plan_ = false;
    }

  private:
    bool& restart_plan_;
  };

public:
  ObExecContext();
  explicit ObExecContext(common::ObIAllocator& allocator);
  virtual ~ObExecContext();

  void reset_op_env();
  void reset_op_ctx();

  bool is_valid() const
  {
    return (NULL != phy_plan_ctx_ && NULL != my_session_ && NULL != plan_cache_manager_);
  }
  /**
   * @brief initialize execute context, must call before calling any function
   */
  int init_phy_op(uint64_t phy_op_size);
  int init_expr_op(const uint64_t expr_op_size);
  void reset_expr_op();
  inline bool is_expr_op_ctx_inited()
  {
    return expr_op_size_ > 0 && NULL != expr_op_ctx_store_;
  }
  int get_convert_charset_allocator(common::ObArenaAllocator *&allocator);

  int init_eval_ctx();
  void destroy_eval_ctx();

  /**
   * @brief query created phy op space size
   */
  inline int64_t get_phy_op_size() const
  {
    return phy_op_size_;
  }

  /**
   * @brief allocate the memory of physical operator context.
   * @param phy_op_id[in], physical operator id in the physical plan.
   * @param nbyte[in], the size of physical operator context.
   * @param type[in], physical operator type.
   * @param op_ctx[out], the pointer of operator context
   * @return is success, return OB_SUCCESS, otherwise, return errno
   */
  int create_phy_op_ctx(const uint64_t phy_op_id, int64_t nbyte, const ObPhyOperatorType type, void*& op_ctx);
  /**
   * @brief get physical operator context object from exec context.
   * @param phy_op_id, physical operator id in the physical plan.
   * @return if success, return the physical operator context object,
              otherwise, return NULL.
   */
  inline void* get_phy_op_ctx(uint64_t phy_op_id) const
  {
    return get_phy_op_ctx_ptr(phy_op_id);
  }
  /**
   * @brief allocate the memory of expr operator context.
   */
  template <typename ObExprCtxType>
  int create_expr_op_ctx(uint64_t op_id, ObExprCtxType*& op_ctx);
  int create_expr_op_ctx(uint64_t op_id, int64_t op_ctx_size, void*& op_ctx);
  /**
   * @brief get expr operator context object from exec context.
   * @param op_type: for regexp and like expr, whose id is not unique, but (type, id) is unique.
   */
  void* get_expr_op_ctx(uint64_t op_id);

  ObExprOperatorCtx** get_expr_op_ctx_store()
  {
    return expr_op_ctx_store_;
  }
  void set_expr_op_ctx_store(ObExprOperatorCtx** expr_op_ctx_store)
  {
    expr_op_ctx_store_ = expr_op_ctx_store;
  }
  uint64_t get_expr_op_size() const
  {
    return expr_op_size_;
  }
  void set_expr_op_size(uint64_t expr_op_size)
  {
    expr_op_size_ = expr_op_size;
  }

  /**
   * @brief allocate the memory of physical operator input param
   * @param phy_op_id[in], physical operator id in the physical plan.
   * @param type[in], physical operator type.
   * @param op_input[out], the pointer of operator input
   */
  int create_phy_op_input(const uint64_t phy_op_id, const ObPhyOperatorType type, ObIPhyOperatorInput*& op_input);
  /**
   * @brief get physical operator input param object from exec context.
   * @param phy_op_id[in], physical operator id in the physical plan.
   */
  ObIPhyOperatorInput* get_phy_op_input(const uint64_t phy_op_id) const;
  /**
   * @brief create physical plan context object from exec context.
   */
  int create_physical_plan_ctx();
  /**
   * @brief set physical plan context object from exec context.
   */
  inline void set_physical_plan_ctx(ObPhysicalPlanCtx* plan_ctx);
  /**
   * @brief get physical plan context from exec context.
   */
  inline ObPhysicalPlanCtx* get_physical_plan_ctx() const;
  /**
   * @brief set session info, for trans control
   */
  inline void set_my_session(ObSQLSessionInfo* session);
  /**
   * @brief get session info, for trans control
   */
  inline ObSQLSessionInfo* get_my_session() const;
  /**
   * @brief set sql proxy
   */
  inline void set_sql_proxy(common::ObMySQLProxy* sql_proxy);
  /**
   * @brief get sql proxy
   */
  inline common::ObMySQLProxy* get_sql_proxy();
  /**
   * @brief set add, for plan cache show stat
   */
  void set_addr(common::ObAddr addr);

  /**
   * @brief get add, for plan cache show stat
   */
  const common::ObAddr& get_addr() const;

  inline void set_virtual_table_ctx(const ObVirtualTableCtx& virtual_table_ctx);
  /**
   * @brief get virtual table scannerable factory,
   * for creating virtual table iterator
   */
  inline ObVirtualTableCtx& get_virtual_table_ctx();

  /**
   * @brief get scheduler thread context,
   * only used by distributed scheduler thread.
   */
  inline ObSchedulerThreadCtx& get_scheduler_thread_ctx();

  /**
   * @brief merge scheduler thread's all info to main thread.
   */
  int merge_scheduler_info();
  /**
   * @brief merge scheduler thread's retry info into retry control.
   */
  int merge_scheduler_retry_info();
  /**
   * @brief merge scheduler thread's failed partitions to main thread.
   */
  int merge_last_failed_partitions();
  int merge_final_trans_result();
  /**
   * @brief get executor context from exec context.
   */
  inline const ObTaskExecutorCtx& get_task_exec_ctx() const;
  inline ObTaskExecutorCtx& get_task_exec_ctx();
  inline ObTaskExecutorCtx* get_task_executor_ctx();
  /**
   * @brief set session_mgr.
   */
  inline void set_session_mgr(ObSQLSessionMgr* session_mgr);
  /**
   * @brief get session_mgr.
   */
  inline ObSQLSessionMgr* get_session_mgr() const;

  /**
   * @brief get execution stat from all tasks
   */
  ObExecStatCollector& get_exec_stat_collector();

  /**
   * @brief get allocator.
   */
  common::ObIAllocator& get_sche_allocator();
  common::ObIAllocator& get_allocator();

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return 0;
  }
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
  ObQueryExecCtx* get_query_exec_ctx()
  {
    return query_exec_ctx_;
  }
  void set_query_exec_ctx(ObQueryExecCtx* query_exec_ctx)
  {
    query_exec_ctx_ = query_exec_ctx;
  }

  ObGIPruningInfo& get_gi_pruning_info()
  {
    return gi_pruning_info_;
  }
  const ObGIPruningInfo& get_gi_pruning_info() const
  {
    return gi_pruning_info_;
  }

  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;

protected:
  uint64_t get_ser_version() const;
  const static uint64_t SER_VERSION_0 = 0;
  const static uint64_t SER_VERSION_1 = 1;

public:
  ObPlanCacheManager* get_plan_cache_manager()
  {
    return plan_cache_manager_;
  }

  void set_plan_cache_manager(ObPlanCacheManager* plan_cache_manager)
  {
    plan_cache_manager_ = plan_cache_manager;
  }

  ObStmtFactory* get_stmt_factory();
  ObRawExprFactory* get_expr_factory();

  int check_status();
  int fast_check_status(const int64_t n = 0xFF);

  void set_outline_params_wrapper(const share::schema::ObOutlineParamsWrapper* params)
  {
    outline_params_wrapper_ = params;
  }
  const share::schema::ObOutlineParamsWrapper* get_outline_params_wrapper() const
  {
    return outline_params_wrapper_;
  }

  void set_execution_id(uint64_t execution_id)
  {
    execution_id_ = execution_id;
  }
  uint64_t get_execution_id() const
  {
    return execution_id_;
  }

  const common::ObInterruptibleTaskID& get_interrupt_id()
  {
    return interrupt_id_;
  }
  void set_interrupt_id(const common::ObInterruptibleTaskID& int_id)
  {
    interrupt_id_ = int_id;
  }

  void set_sql_ctx(ObSqlCtx* ctx)
  {
    sql_ctx_ = ctx;
  }
  ObSqlCtx* get_sql_ctx()
  {
    return sql_ctx_;
  }
  inline bool use_remote_sql() const
  {
    bool bret = false;
    if (OB_NOT_NULL(phy_plan_ctx_)) {
      bret = (!phy_plan_ctx_->get_remote_sql_info().remote_sql_.empty());
    }
    return bret;
  }

  bool& get_need_disconnect_for_update()
  {
    return need_disconnect_;
  }
  bool need_disconnect() const
  {
    return need_disconnect_;
  }
  void set_need_disconnect(bool need_disconnect)
  {
    need_disconnect_ = need_disconnect;
  }

  ObPartIdRowMapManager& get_part_row_manager()
  {
    return part_row_map_manager_;
  }
  void set_need_change_timeout_ret(bool need_change_timeout_ret)
  {
    need_change_timeout_ret_ = need_change_timeout_ret;
  }
  bool need_change_timeout_ret() const
  {
    return need_change_timeout_ret_;
  }

  uint64_t get_min_cluster_version() const;
  int reset_one_row_id_list(const common::ObIArray<int64_t>* row_id_list);
  const ObRowIdListArray& get_row_id_list_array() const
  {
    return row_id_list_array_;
  }
  int add_row_id_list(const common::ObIArray<int64_t>* row_id_list);
  void reset_row_id_list()
  {
    row_id_list_array_.reset();
    total_row_count_ = 0;
  }
  int64_t get_row_id_list_total_count() const
  {
    return total_row_count_;
  }
  void set_is_evolution(bool v)
  {
    is_evolution_ = v;
  }
  bool get_is_evolution() const
  {
    return is_evolution_;
  }

  bool is_reusable_interm_result() const
  {
    return reusable_interm_result_;
  }
  void set_reusable_interm_result(const bool reusable)
  {
    reusable_interm_result_ = reusable;
  }
  void set_end_trans_async(bool is_async)
  {
    is_async_end_trans_ = is_async;
  }
  bool is_end_trans_async()
  {
    return is_async_end_trans_;
  }
  inline TransState& get_trans_state()
  {
    return trans_state_;
  }
  inline const TransState& get_trans_state() const
  {
    return trans_state_;
  }
  int add_temp_table_interm_result_ids(
      int64_t temp_table_id, int64_t sqc_id, const ObIArray<uint64_t>& interm_result_ids);
  // for granule iterator
  int get_gi_task_map(GIPrepareTaskMap*& gi_prepare_task_map);
  inline void set_gi_restart()
  {
    gi_restart_ = true;
  }
  inline bool is_gi_restart() const
  {
    return gi_restart_;
  }
  inline void reset_gi_restart()
  {
    gi_restart_ = false;
  }

  // for restart plan
  inline bool& get_restart_plan()
  {
    return restart_plan_;
  }
  inline bool is_restart_plan() const
  {
    return restart_plan_;
  }

  // for udf
  int get_udf_ctx_mgr(ObUdfCtxMgr*& udf_ctx_mgr);

  // for call procedure
  ObNewRow* get_output_row()
  {
    return output_row_;
  }
  void set_output_row(ObNewRow* row)
  {
    output_row_ = row;
  }

  ColumnsFieldIArray* get_field_columns()
  {
    return field_columns_;
  }
  void set_field_columns(ColumnsFieldIArray* field_columns)
  {
    field_columns_ = field_columns;
  }

  void set_direct_local_plan(bool v)
  {
    is_direct_local_plan_ = v;
  }
  bool get_direct_local_plan() const
  {
    return is_direct_local_plan_;
  }

  ObPxSqcHandler* get_sqc_handler()
  {
    return sqc_handler_;
  }
  void set_sqc_handler(ObPxSqcHandler* sqc_handler)
  {
    sqc_handler_ = sqc_handler;
  }

  char** get_frames() const
  {
    return frames_;
  }
  void set_frames(char** frames)
  {
    frames_ = frames;
  }
  uint64_t get_frame_cnt() const
  {
    return frame_cnt_;
  }
  void set_frame_cnt(uint64_t frame_cnt)
  {
    frame_cnt_ = frame_cnt;
  }

  ObOperatorKit* get_operator_kit(const uint64_t id) const
  {
    return op_kit_store_.get_operator_kit(id);
  }
  ObOpKitStore& get_kit_store()
  {
    return op_kit_store_;
  }
  ObEvalCtx* get_eval_ctx() const
  {
    return eval_ctx_;
  }
  lib::MemoryContext get_eval_res_mem()
  {
    return eval_res_mem_;
  }
  lib::MemoryContext get_eval_tmp_mem()
  {
    return eval_tmp_mem_;
  }

  void clean_resolve_ctx();
  int init_physical_plan_ctx(const ObPhysicalPlan& plan);

  ObIArray<ObSqlTempTableCtx>& get_temp_table_ctx()
  {
    return temp_ctx_;
  }

  ObSchedInfo& get_sched_info()
  {
    return sched_info_;
  }

  void set_root_op(const ObPhyOperator* root_op)
  {
    root_op_ = root_op;
  }
  const ObPhyOperator* get_root_op()
  {
    return root_op_;
  }
  int get_pwj_map(PWJPartitionIdMap*& pwj_map);
  PWJPartitionIdMap* get_pwj_map()
  {
    return pwj_map_;
  }
  void set_partition_id_calc_type(PartitionIdCalcType calc_type)
  {
    calc_type_ = calc_type;
  }
  PartitionIdCalcType get_partition_id_calc_type()
  {
    return calc_type_;
  }
  void set_fixed_id(int64_t fixed_id)
  {
    fixed_id_ = fixed_id;
  }
  int64_t get_fixed_id()
  {
    return fixed_id_;
  }
  void set_expr_partition_id(int64_t partition_id)
  {
    expr_partition_id_ = partition_id;
  }
  int64_t get_expr_partition_id()
  {
    return expr_partition_id_;
  }

  int push_back_iter(common::ObNewRowIterator *iter);
  int remove_iter(common::ObNewRowIterator *iter);
private:
  int set_phy_op_ctx_ptr(uint64_t index, void* phy_op);
  void* get_phy_op_ctx_ptr(uint64_t index) const;
  /**
   * @brief alloc physical operator input obj by operator type
   * @param type[in] the physical operator type
   * @return if success, return the pointer, otherwise, return NULL.
   */
  ObIPhyOperatorInput* alloc_operator_input_by_type(ObPhyOperatorType type);
  int serialize_operator_input(char*& buf, int64_t buf_len, int64_t& pos, int32_t& real_input_count) const;
  int serialize_operator_input_recursively(const ObPhyOperator* op, char*& buf, int64_t buf_len, int64_t& pos,
      int32_t& real_input_count, bool is_full_tree) const;
  int serialize_operator_input_len_recursively(const ObPhyOperator* op, int64_t& len, bool is_full_tree) const;
  int release_table_ref();
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
  // for distribute scheduler
  common::ObArenaAllocator sche_allocator_;
  common::ObArenaAllocator inner_allocator_;
  common::ObIAllocator& allocator_;
  /**
   * @brief phy_op_size_, the physical operator size in physical plan
   * phy_op_store_, an array of dynamic size
   */
  uint64_t phy_op_size_;
  void** phy_op_ctx_store_;
  ObIPhyOperatorInput** phy_op_input_store_;
  ObPhysicalPlanCtx* phy_plan_ctx_;
  uint64_t expr_op_size_;
  ObExprOperatorCtx** expr_op_ctx_store_;
  ObSchedulerThreadCtx scheduler_thread_ctx_;  // used in scheduler thread
  ObTaskExecutorCtx task_executor_ctx_;
  ObSQLSessionInfo* my_session_;
  ObPlanCacheManager* plan_cache_manager_;
  common::ObMySQLProxy* sql_proxy_;
  ObVirtualTableCtx virtual_table_ctx_;
  ObSQLSessionMgr* session_mgr_;
  ObExecStatCollector exec_stat_collector_;
  ObStmtFactory* stmt_factory_;
  ObRawExprFactory* expr_factory_;
  const share::schema::ObOutlineParamsWrapper* outline_params_wrapper_;
  uint64_t execution_id_;
  common::ObInterruptibleTaskID interrupt_id_;
  bool has_non_trivial_expr_op_ctx_;
  ObSqlCtx* sql_ctx_;
  bool need_disconnect_;  // nedd close client connection
  ObPartIdRowMapManager part_row_map_manager_;
  bool need_change_timeout_ret_;
  // for px insert into values
  ObRowIdListArray row_id_list_array_;
  int64_t total_row_count_;
  // -----------------------

  bool is_evolution_;
  // Interminate result of index building is reusable, reused in build index retry with same snapshot.
  // Reusable intermediate result is not deleted in the close phase, deleted deliberately after
  // execution is completed.
  bool reusable_interm_result_;
  bool is_async_end_trans_;

  TransState trans_state_;
  /*
   * gi task buffer, no need to serialize.
   * @brief The key is table scan operator's id,
   *        The value is the gi task info.
   * */
  GIPrepareTaskMap* gi_task_map_;
  /*
   * no need to serialize.
   * a gi operator invoke child's rescan may expect a
   * different behavior. we use this var to tell child
   * do the real 'rescan'. for instance, material op will
   * invoke his children's rescan to refill his buffer.
   * */
  bool gi_restart_;
  /*
   * for dll udf
   * */
  ObUdfCtxMgr* udf_ctx_mgr_;
  // for call procedure_;
  ObNewRow* output_row_;
  ColumnsFieldIArray* field_columns_;
  bool is_direct_local_plan_;
  // restart the plan
  bool restart_plan_;

  ObPxSqcHandler* sqc_handler_;

  // data frames and count
  char** frames_;
  uint64_t frame_cnt_;

  ObOpKitStore op_kit_store_;

  // expression evaluating memory and context.
  lib::MemoryContext eval_res_mem_;
  lib::MemoryContext eval_tmp_mem_;
  ObEvalCtx* eval_ctx_;
  ObQueryExecCtx* query_exec_ctx_;
  ObSEArray<ObSqlTempTableCtx, 1> temp_ctx_;

  // dynamic partition pruning for right TSC of NLJ
  ObGIPruningInfo gi_pruning_info_;

  ObSchedInfo sched_info_; 

  // just for convert charset in query response result
  lib::MemoryContext convert_allocator_;

  // serialize operator inputs of %root_op_ subplan if root_op_ is not NULL
  const ObPhyOperator* root_op_;
  // partition idx and partition id mapping used in PWJ
  PWJPartitionIdMap* pwj_map_;
  // the following two parameters only used in calc_partition_id expr
  PartitionIdCalcType calc_type_;
  int64_t fixed_id_;  // fixed part id or fixed subpart ids
  // for expr values op use
  int64_t expr_partition_id_;
  ObSEArray<common::ObNewRowIterator*, 1, common::ObIAllocator&> iters_;
  int64_t check_status_times_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExecContext);
};

template <typename ObExprCtxType>
int ObExecContext::create_expr_op_ctx(uint64_t op_id, ObExprCtxType*& op_ctx)
{
  void* op_ctx_ptr = NULL;
  int ret = create_expr_op_ctx(op_id, sizeof(ObExprCtxType), op_ctx_ptr);
  op_ctx = (OB_SUCC(ret) && !OB_ISNULL(op_ctx_ptr)) ? new (op_ctx_ptr) ObExprCtxType() : NULL;
  return ret;
}

inline void ObExecContext::set_physical_plan_ctx(ObPhysicalPlanCtx* plan_ctx)
{
  phy_plan_ctx_ = plan_ctx;
}

inline void ObExecContext::set_my_session(ObSQLSessionInfo* session)
{
  my_session_ = session;
}

inline ObSQLSessionInfo* ObExecContext::get_my_session() const
{
  return my_session_;
}

inline void ObExecContext::set_sql_proxy(common::ObMySQLProxy* sql_proxy)
{
  sql_proxy_ = sql_proxy;
}

inline common::ObMySQLProxy* ObExecContext::get_sql_proxy()
{
  return sql_proxy_;
}

inline void ObExecContext::set_virtual_table_ctx(const ObVirtualTableCtx& virtual_table_ctx)
{
  virtual_table_ctx_ = virtual_table_ctx;
}

inline ObVirtualTableCtx& ObExecContext::get_virtual_table_ctx()
{
  return virtual_table_ctx_;
}

inline ObPhysicalPlanCtx* ObExecContext::get_physical_plan_ctx() const
{
  return phy_plan_ctx_;
}

inline ObSchedulerThreadCtx& ObExecContext::get_scheduler_thread_ctx()
{
  return scheduler_thread_ctx_;
}

inline const ObTaskExecutorCtx& ObExecContext::get_task_exec_ctx() const
{
  return task_executor_ctx_;
}

inline ObTaskExecutorCtx& ObExecContext::get_task_exec_ctx()
{
  return task_executor_ctx_;
}

inline ObTaskExecutorCtx* ObExecContext::get_task_executor_ctx()
{
  return &task_executor_ctx_;
}

inline void ObExecContext::set_session_mgr(ObSQLSessionMgr* session_mgr)
{
  session_mgr_ = session_mgr;
}

inline ObSQLSessionMgr* ObExecContext::get_session_mgr() const
{
  return session_mgr_;
}

inline ObExecStatCollector& ObExecContext::get_exec_stat_collector()
{
  return exec_stat_collector_;
}

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_EXEC_CONTEXT_H
