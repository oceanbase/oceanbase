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

#ifndef OCEANBASE_ENGINE_OB_OPERATOR_H_
#define OCEANBASE_ENGINE_OB_OPERATOR_H_

#include "lib/time/ob_tsc_timestamp.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/ash/ob_active_session_guard.h"
#include "sql/engine/basic/ob_batch_result_holder.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_operator_reg.h"
#include "sql/engine/px/ob_px_op_size_factor.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/ob_io_event_observer.h"
#include "sql/ob_sql_define.h"
#include "sql/engine/ob_batch_rows.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_trigger_info.h"
#include "common/ob_common_utility.h"

namespace oceanbase
{
namespace sql
{

struct ObExpr;
class ObPhysicalPlan;
class ObOpSpec;
class ObOperator;
class ObOpInput;
class ObTaskInfo;
class ObExecFeedbackNode;

struct ObPhyOpSeriCtx
{
  public:
  ObPhyOpSeriCtx() : row_id_list_(NULL), exec_ctx_(NULL) {}
  const void *row_id_list_;
  const ObExecContext *exec_ctx_;
};

class ObOpSchemaObj
{
  OB_UNIS_VERSION(1);
public:
  ObOpSchemaObj()
    : obj_type_(common::ObMaxType),
      is_not_null_(false),
      order_type_(default_asc_direction()) {
  }
  ObOpSchemaObj(common::ObObjType obj_type)
    : obj_type_(obj_type),
      is_not_null_(false),
      order_type_(default_asc_direction()) {}
  ObOpSchemaObj(common::ObObjType obj_type, ObOrderDirection order_type)
    : obj_type_(obj_type), is_not_null_(false), order_type_(order_type) {}

  bool is_null_first() const {
    return NULLS_FIRST_ASC == order_type_ || NULLS_FIRST_DESC == order_type_;
  }

  bool is_ascending_direction() const {
    return NULLS_FIRST_ASC == order_type_ || NULLS_LAST_ASC == order_type_;
  }
  TO_STRING_KV(K_(obj_type), K_(is_not_null), K_(order_type));
public:
  common::ObObjType obj_type_;
  bool is_not_null_;
  // 因为ObSortColumn是需要序列化的，但是ObSortColumn没有使用信息的序列化框架
  // 在ObSortColumn里面加任何一个字段都会有兼容性问题
  // 所以就把null first last的信息放在这里了
  ObOrderDirection order_type_;
};

enum OperatorOpenOrder
{
  OPEN_CHILDREN_FIRST = 0, //默认先open children
  OPEN_SELF_FIRST = 1, //先open自己
  OPEN_CHILDREN_LATER = 2, //再open children
  OPEN_SELF_LATER = 3,
  OPEN_SELF_ONLY = 4, //不打开children，但是打开自己
  OPEN_NONE = 5, //不打开自己，也不打开children, unused since 4.0
  OPEN_EXIT = 6
};

struct ObBatchRescanParams {
  OB_UNIS_VERSION_V(1);
public:
  ObBatchRescanParams() :allocator_("PxBatchRescan"), params_(),
      param_idxs_(), param_expr_idxs_(0) {}
  ~ObBatchRescanParams() = default;
  int64_t get_count() const { return params_.count(); }
  common::ObIArray<common::ObObjParam>& get_one_batch_params(int64_t idx) {
    return params_.at(idx);
  }
  int64_t get_param_idx(int64_t idx) {
    return param_idxs_.at(idx);
  }
  int append_batch_rescan_param(const common::ObIArray<int64_t> &param_idxs,
      const sql::ObTMArray<common::ObObjParam> &res_objs);
  int append_batch_rescan_param(const common::ObIArray<int64_t> &param_idxs,
      const sql::ObTMArray<common::ObObjParam> &res_objs,
      const common::ObIArray<int64_t> &param_expr_idxs);
  void reset()
  {
    params_.reset();
    param_idxs_.reset();
    param_expr_idxs_.reset();
    allocator_.reset();
  }

  void reuse()
  {
    params_.reset();
    param_idxs_.reset();
    param_expr_idxs_.reset();
    allocator_.reuse();
  }

  int assign(const ObBatchRescanParams &other);
  int deep_copy_param(const common::ObObjParam &org_param, common::ObObjParam &new_param);
public:
  common::ObArenaAllocator allocator_;
  common::ObSArray<sql::ObTMArray<common::ObObjParam>> params_;
  common::ObSEArray<int64_t, 8> param_idxs_;
  common::ObSEArray<int64_t, 8> param_expr_idxs_;
  TO_STRING_KV(K(param_idxs_), K(params_), K_(param_expr_idxs));
};

struct ObBatchRescanCtl
{
  ObBatchRescanCtl() : cur_idx_(0), params_(), param_version_()
  {
  }

  void reset()
  {
    cur_idx_ = 0;
    params_.reset();
    param_version_ += 1;
  }

  void reuse()
  {
    cur_idx_ = 0;
    params_.reuse();
    param_version_ += 1;
  }

  int64_t cur_idx_;
  ObBatchRescanParams params_;
  // %param_version_ increase when %params_ changes, used to detect %params_ change.
  int64_t param_version_;
  TO_STRING_KV(K_(cur_idx), K_(params), K_(param_version));
};

// Adapt get_next_batch() to the old style get_next_row() interface.
// (return OB_ITER_END for iterate end)
class ObBatchRowIter
{
public:
  ObBatchRowIter() : op_(NULL), brs_(NULL), idx_(0) {}
  explicit ObBatchRowIter(ObOperator *op) : op_(op), brs_(NULL), idx_(0) {}

  void set_operator(ObOperator *op) { op_ = op; }

  int get_next_row();
  // TODO qubin.qb: move into class ObOperator
  int get_next_row(ObEvalCtx &eval_ctx, const ObOpSpec &spec);

  int64_t cur_idx() const { return idx_ - 1; }
  const ObBatchRows *get_brs() const { return brs_; }
  void rescan() { brs_ = NULL; idx_ = 0;}

private:
  ObOperator *op_;
  const ObBatchRows *brs_;
  int64_t idx_;
public:
  ObBatchResultHolder brs_holder_;
};


struct ObDynamicParamSetter
{
  OB_UNIS_VERSION_V(1);
public:
  ObDynamicParamSetter()
    : param_idx_(common::OB_INVALID_ID), src_(NULL), dst_(NULL)
  {}

  ObDynamicParamSetter(int64_t param_idx, ObExpr *src_expr, ObExpr *dst_expr)
    : param_idx_(param_idx), src_(src_expr), dst_(dst_expr)
  {}
  virtual ~ObDynamicParamSetter() {}

  int set_dynamic_param(ObEvalCtx &eval_ctx) const;
  int set_dynamic_param(ObEvalCtx &eval_ctx, common::ObObjParam *&param) const;
  int update_dynamic_param(ObEvalCtx &eval_ctx, common::ObDatum &datum) const;

  static void clear_parent_evaluated_flag(ObEvalCtx &eval_ctx, ObExpr &expr);

public:
  int64_t param_idx_; // param idx in param store
  ObExpr *src_; // original expr which replaced by param expr
  ObExpr *dst_; // dynamic param expr

  TO_STRING_KV(K_(param_idx), K_(src), K_(dst));
};

class ObOpSpecVisitor;
// Physical operator specification, immutable in execution.
// (same with the old ObPhyOperator)
class ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObOpSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  virtual ~ObOpSpec();

  DECLARE_VIRTUAL_TO_STRING;
  const char *op_name() const { return ob_phy_operator_type_str(type_, use_rich_format_); }

  // Pre-order recursive create execution components (ObOperator and ObOperatorInput)
  // for current DFO.
  int create_operator(ObExecContext &exec_ctx, ObOperator *&op) const;
  int create_op_input(ObExecContext &exec_ctx) const;
  // 将算子注册到 datahub，用于并行计算场景
  //
  virtual int register_to_datahub(ObExecContext &exec_ctx) const
    { UNUSED(exec_ctx); return common::OB_SUCCESS; }
  // register init channel msg to sqc_ctx, used for px
  virtual int register_init_channel_msg(ObExecContext &ctx)
    { UNUSED(ctx); return common::OB_SUCCESS; }

  uint32_t get_child_cnt() const { return child_cnt_; }
  ObOpSpec **get_children() const { return children_; }

  int set_children_pointer(ObOpSpec **children, const uint32_t child_cnt);
  int set_child(const uint32_t idx, ObOpSpec *child);

  ObOpSpec *get_parent() const { return parent_; }
  void set_parent(ObOpSpec *parent) { parent_ = parent; }

  int64_t get_output_count() const { return output_.count(); }

  ObPhysicalPlan *get_phy_plan() { return plan_; }
  const ObPhysicalPlan *get_phy_plan() const { return plan_; }

  const ObOpSpec *get_child() const { return child_; }
  const ObOpSpec *get_left() const { return left_; }
  const ObOpSpec *get_right() const { return right_; }
  const ObOpSpec *get_child(uint32_t idx) const { return children_[idx]; }
  ObOpSpec *get_child(uint32_t idx) { return children_[idx]; }

  virtual bool is_table_scan() const { return false; }
  virtual bool is_dml_operator() const { return false; }
  virtual bool is_pdml_operator() const { return false; }
  virtual bool is_receive() const { return false; }

  // same with ObPhyOperator to make template works.
  int32_t get_child_num() const { return get_child_cnt(); }
  ObPhyOperatorType get_type() const { return type_; }
  uint64_t get_id() const { return id_; }
  const char *get_name() const { return get_phy_op_name(type_, use_rich_format_); }

  int accept(ObOpSpecVisitor &visitor) const;
  int64_t get_rows() const { return rows_; }
  int64_t get_cost() const { return cost_; }
  int64_t get_plan_depth() const { return plan_depth_; }

  // find all specs of the DFO (stop when reach receive)
  template <typename T, typename FILTER>
  static int find_target_specs(T &spec, const FILTER &f, common::ObIArray<T *> &res);

  bool is_vectorized() const { return 0 != max_batch_size_; }
  virtual int serialize(char *buf, int64_t buf_len, int64_t &pos, ObPhyOpSeriCtx &seri_ctx) const
  {
    UNUSED(seri_ctx);
    return serialize(buf, buf_len, pos);
  }
  virtual int64_t get_serialize_size(const ObPhyOpSeriCtx &seri_ctx) const
  {
    UNUSED(seri_ctx);
    return get_serialize_size();
  }
private:
  int create_operator_recursive(ObExecContext &exec_ctx, ObOperator *&op) const;
  int create_op_input_recursive(ObExecContext &exec_ctx) const;
  // assign spec ptr to ObOpKitStore
  int assign_spec_ptr_recursive(ObExecContext &exec_ctx) const;
  int link_sql_plan_monitor_node_recursive(ObExecContext &exec_ctx, ObMonitorNode *&pre_node) const;
  int create_exec_feedback_node_recursive(ObExecContext &exec_ctx) const;
  // Data members are accessed in ObOperator class, exposed for convenience.
public:
  // Operator type, serializing externally.
  ObPhyOperatorType type_;
  // Operator id, unique in DFO
  uint64_t id_;
  ObPhysicalPlan *plan_;


  // The %child_, %left_, %right_ should be set when set %children_,
  // make it protected and exposed by interface.
protected:
  ObOpSpec *parent_;
  ObOpSpec **children_;
  uint32_t child_cnt_;
  union {
    ObOpSpec *child_;
    ObOpSpec *left_;
  };
  ObOpSpec *right_;

public:

  // Operator output expressions
  ExprFixedArray output_;
  // Startup filter expressions
  ExprFixedArray startup_filters_;
  // Filter expressions
  ExprFixedArray filters_;
  // All expressions used in this operator but not exists in children's output.
  // Need to reset those expressions' evaluated_ flag after fetch row from child.
  ExprFixedArray calc_exprs_;

  // Optimizer estimated info
  int64_t cost_;
  int64_t rows_;
  int64_t width_;
  PxOpSizeFactor px_est_size_factor_;
  int64_t plan_depth_;
  int64_t max_batch_size_;
  bool need_check_output_datum_;
  bool use_rich_format_;
  ObCompressorType compress_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOpSpec);
};

class ObOpSpecVisitor
{
public:
  virtual int pre_visit(const ObOpSpec &spec) = 0;
  virtual int post_visit(const ObOpSpec &spec) = 0;
};

// Physical operator, mutable in execution.
// (same with the old ObPhyOperatorCtx)
class ObOperator
{
public:
  const static uint64_t CHECK_STATUS_TRY_TIMES = 1024;
  const static uint64_t CHECK_STATUS_ROWS = 8192;
  const static int64_t MONITOR_RUNNING_TIME_THRESHOLD = 5000000; //5s
  const static int64_t REAL_TIME_MONITOR_THRESHOLD = 1000000; //1s
  const static uint64_t REAL_TIME_MONITOR_TRY_TIMES = 256;

public:
  ObOperator(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObOperator();

  const ObOpSpec &get_spec() const { return spec_; }
  ObOpInput *get_input() const { return input_; }

  int set_children_pointer(ObOperator **children, uint32_t child_cnt);
  int set_child(const uint32_t idx, ObOperator *child);

  int init();

  // open operator, cascading open child operators.
  virtual int open();
  // open operator, not including child operators.
  virtual int inner_open() { return common::OB_SUCCESS; }

  /**
   * @brief reopen the operator, with new params
   * do cascade rescan and reset some op status in inner_rescan
   * No need to override in most scenarios
   */
  virtual int rescan();
  /**
   * @brief reset brs_.end_, start_passed_ && ++rescan_times_
   * Derived op can override it, but be sure ObOperator::inner_rescan is called
  */
  virtual int inner_rescan();

  // switch iterator for array binding.
  // do cascade switch_iterator and reset some op status in inner_switch_iterator
  virtual int switch_iterator();
  /**
   * Derived op can override it, but be sure ObOperator::inner_switch_iterator is called
  */
  virtual int inner_switch_iterator();

  // fetch next row
  // return OB_ITER_END if reach end.
  virtual int get_next_row();
  virtual int inner_get_next_row() = 0;

  // Get next batch rows, %max_row_cnt limits the maximum number of rows returned.
  // When iterate end return OB_SUCCESS with ObBatchRows::end_ set to true.
  // set brs_.end_ = true when end of data stream is touched
  // (ensure that the current batch has no data)
  virtual int get_next_batch(const int64_t max_row_cnt, const ObBatchRows *&batch_rows);
  // inner get next batch, no need to pass %batch_rows, set result to %brs_ member.
  // set brs_.end_ = true when end of data stream is touched
  // (don't care if there is data in the current batch)
  virtual int inner_get_next_batch(const int64_t max_row_cnt)
  {
    UNUSED(max_row_cnt);
    int ret = common::OB_SUCCESS;
    COMMON_LOG(WARN, "interface not implement", K(ret));
    return common::OB_NOT_IMPLEMENT;
  }

  // close operator, cascading close child operators
  virtual int close();
  // close operator, not including child operators.
  virtual int inner_close() { return common::OB_SUCCESS; }

  // We call destroy() instead of destructor to free resources for performance purpose.
  // Every operator should implement this and must invoke the base class's destroy method.
  // e.g.:
  //  ObOperator::destroy();
  inline virtual void destroy() = 0;

  // The open order of children and operator differs, material operator need to open children
  // first, px coord should be opened before children to schedule the DFO in open.
  virtual OperatorOpenOrder get_operator_open_order() const
  {
    return OPEN_CHILDREN_FIRST;
  }

  virtual int do_init_before_get_row() { return common::OB_SUCCESS; }
  const char *op_name() const { return spec_.op_name(); }

  OB_INLINE void clear_evaluated_flag();

  // clear evaluated flag of current datum (ObEvalCtx::batch_idx_) of batch.
  inline void clear_datum_eval_flag();

  void reset_output_format();

  // check execution status: timeout, session kill, interrupted ..
  int check_status();
  // check execution status every CHECK_STATUS_TRY_TIMES tries.
  int try_check_status();

  // check execution status every CHECK_STATUS_ROWS processed.
  int try_check_status_by_rows(const int64_t rows);

  ObEvalCtx &get_eval_ctx() { return eval_ctx_; }
  ObExecContext &get_exec_ctx() { return ctx_; }
  ObIOEventObserver &get_io_event_observer() { return io_event_observer_; }

  uint32_t get_child_cnt() const { return child_cnt_; }
  ObOperator *get_child() { return child_; }
  ObOperator *get_child(int32_t child_idx) { return children_[child_idx]; }
  bool is_opened() { return opened_; }
  ObMonitorNode &get_monitor_info() { return op_monitor_info_; }
  bool is_vectorized() const { return spec_.is_vectorized(); }
  int init_evaluated_flags();
  static int filter_row(ObEvalCtx &eval_ctx,
                        const common::ObIArray<ObExpr *> &exprs,
                        bool &filtered);
  static int filter_row_vector(ObEvalCtx &eval_ctx,
                               const common::ObIArray<ObExpr *> &exprs,
                               const sql::ObBitVector &skip_bit,
                               bool &filtered);
  ObBatchRows &get_brs() { return brs_; }
  // Drain exchange in data for PX, or producer DFO will be blocked.
  int drain_exch();
  void set_pushdown_param_null(const common::ObIArray<ObDynamicParamSetter> &rescan_params);
  void set_feedback_node_idx(int64_t idx)
  { fb_node_idx_ = idx; }

  bool is_operator_end() { return batch_reach_end_ ||  row_reach_end_ ; }
protected:
  virtual int do_drain_exch();
  int init_skip_vector();
  // Execute filter
  // Calc buffer does not reset internally, you need to reset it appropriately.
  int filter(const common::ObIArray<ObExpr *> &exprs, bool &filtered);

  int filter_rows(const ObExprPtrIArray &exprs,
                  ObBitVector &skip,
                  const int64_t bsize,
                  bool &all_filtered,
                  bool &all_active);

  int startup_filter(bool &filtered) { return filter(spec_.startup_filters_, filtered); }
  int filter_row(bool &filtered) { return filter(spec_.filters_, filtered); }

  // try open operator
  int try_open() { return opened_ ? common::OB_SUCCESS : open(); }


  virtual void do_clear_datum_eval_flag();
  void clear_batch_end_flag() { brs_.end_ = false; }
  inline void reset_batchrows()
  {
    if (brs_.size_ > 0) {
      brs_.reset_skip(brs_.size_);
      brs_.size_ = 0;
    }
    brs_.all_rows_active_ = false;
  }
  inline int get_next_row_vectorizely();
  inline int get_next_batch_with_onlyone_row()
  {
    int ret = OB_SUCCESS;
    SQL_ENG_LOG(DEBUG,
        "operator does NOT support batch interface, call get_next_row instead",
        K(eval_ctx_), K(spec_), KCSTRING(op_name()));
    clear_evaluated_flag();//TODO qubin.qb: remove this line as inner_get_next_row() calls it
    if (OB_FAIL(get_next_row())) {
      if (ret == OB_ITER_END) {
        brs_.size_ = 0;
        brs_.end_ = true;
        ret = OB_SUCCESS;
      } else {
        SQL_ENG_LOG(WARN, "Failed to get_next_row", K(ret));
      }
    } else {
      brs_.size_ = 1;
      brs_.end_ = false;
      // project
      FOREACH_CNT_X(e, spec_.output_, OB_SUCC(ret)) {
        if (OB_FAIL((*e)->eval_batch(eval_ctx_, *brs_.skip_, brs_.size_))) {
          SQL_ENG_LOG(WARN, "expr evaluate failed", K(ret), K(*e));
        } else {
          (*e)->get_eval_info(eval_ctx_).projected_ = true;
        }
      }
    }
    return ret;
  }

private:
  int filter_batch_rows(const ObExprPtrIArray &exprs,
                        ObBitVector &skip,
                        const int64_t bsize,
                        bool &all_filtered,
                        bool &all_active);
  int filter_vector_rows(const ObExprPtrIArray &exprs,
                         ObBitVector &skip,
                         const int64_t bsize,
                         bool &all_filtered,
                         bool &all_active);
  int convert_vector_format();
  // for sql plan monitor
  int try_register_rt_monitor_node(int64_t rows);
  int try_deregister_rt_monitor_node();
  int submit_op_monitor_node();
  bool match_rt_monitor_condition(int64_t rows);
  int check_stack_once();
  int output_expr_sanity_check();
  int output_expr_sanity_check_batch();
  int output_expr_decint_datum_len_check();
  int output_expr_decint_datum_len_check_batch();
  int setup_op_feedback_info();
  // child can implement this interface, but can't call this directly
  virtual int inner_drain_exch() { return common::OB_SUCCESS; };
protected:
  const ObOpSpec &spec_;
  ObExecContext &ctx_;
  ObEvalCtx eval_ctx_;
  common::ObFixedArray<ObEvalInfo *, common::ObIAllocator> eval_infos_;
  ObOpInput *input_;

  ObOperator *parent_;
  ObOperator **children_;
  uint32_t child_cnt_;
  union {
    ObOperator *child_;
    ObOperator *left_;
  };
  ObOperator *right_;

  uint64_t try_check_tick_; //for check status

  uint64_t try_monitor_tick_; // for real time sql plan monitor

  bool opened_;
  // pass the startup filter (not filtered)
  bool startup_passed_;
  // the exchange operator has been drained
  bool exch_drained_;
  // mark first row emitted
  bool got_first_row_;
  // do some init in inner_get_next_row
  bool need_init_before_get_row_;
  // gv$sql_plan_monitor
  ObMonitorNode op_monitor_info_;
  // exec feedback info
  int64_t fb_node_idx_;

  ObIOEventObserver io_event_observer_;
  // batch rows struct for get_next_row result.
  ObBatchRows brs_;
  ObBatchRowIter *br_it_ = nullptr;
  ObBatchResultHolder *brs_checker_= nullptr;

  inline void begin_cpu_time_counting()
  {
    cpu_begin_time_ = rdtsc();
  }
  inline void end_cpu_time_counting()
  {
    total_time_ += (rdtsc() - cpu_begin_time_);
  }
  inline void begin_ash_line_id_reg()
  {
    // begin with current operator
    ObActiveSessionGuard::get_stat().plan_line_id_ = static_cast<int32_t>(spec_.id_);//TODO(xiaochu.yh): fix uint64 to int32
  }
  inline void end_ash_line_id_reg()
  {
    // move back to parent operator
    // known issue: when switch from batch to row in same op,
    // we shift line id to parent op un-intently. but we tolerate this inaccuracy
    if (OB_LIKELY(spec_.get_parent())) {
      common::ObActiveSessionGuard::get_stat().plan_line_id_ = static_cast<int32_t>(spec_.get_parent()->id_);//TODO(xiaochu.yh): fix uint64 to int32
    } else {
      common::ObActiveSessionGuard::get_stat().plan_line_id_ = -1;
    }
  }
  #ifdef ENABLE_DEBUG_LOG
  inline int init_dummy_mem_context(uint64_t tenant_id);
  #endif
  uint64_t cpu_begin_time_; // start of counting cpu time
  uint64_t total_time_; //  total time cost on this op, including io & cpu time
protected:
  bool batch_reach_end_;
  bool row_reach_end_;
  int64_t output_batches_b4_rescan_;
  //The following two variables are used to track whether the operator
  //has been closed && destroyed in test mode. We apply for a small
  //memory for each operator in open. If there is
  //no close, mem_context will release it and give an alarm
  #ifdef ENABLE_DEBUG_LOG
  lib::MemoryContext dummy_mem_context_;
  char *dummy_ptr_;
  #endif
  bool check_stack_overflow_;
  DISALLOW_COPY_AND_ASSIGN(ObOperator);
};

// Operator input is information of task execution. Some operator need the to get information
// from task executor.
//
// e.g.:
//    table scan need this to get scan partition.
//    exchange operator need this to get DTL channels
//
class ObOpInput
{
  OB_UNIS_VERSION_PV();
public:
  ObOpInput(ObExecContext &ctx, const ObOpSpec &spec): exec_ctx_(ctx), spec_(spec) {}
  virtual ~ObOpInput() {}

  virtual int init(ObTaskInfo &task_info) = 0;
  virtual void set_deserialize_allocator(common::ObIAllocator *allocator) { UNUSED(allocator); }
  virtual void reset() = 0;

  ObPhyOperatorType get_type() const { return spec_.type_; }
  const ObOpSpec &get_spec() const { return spec_; }
  TO_STRING_KV(K(spec_));
protected:
  ObExecContext &exec_ctx_;
  const ObOpSpec &spec_;
};


template <typename T, typename FILTER>
int ObOpSpec::find_target_specs(T &spec, const FILTER &f, common::ObIArray<T *> &res)
{
  int ret = common::check_stack_overflow();
  if (OB_SUCC(ret) && !spec.is_receive()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < spec.get_child_cnt(); i++) {
      if (NULL == spec.get_child(i)) {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "NULL child", K(ret), K(spec));
      } else if (OB_FAIL(find_target_specs(*spec.get_child(i), f, res))) {
        SQL_ENG_LOG(WARN, "find target specs failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && f(spec)) {
    if (OB_FAIL(res.push_back(&spec))) {
      SQL_ENG_LOG(WARN, "array push back failed", K(ret));
    }
  }
  return ret;
}

inline void ObOperator::destroy()
{
  #ifdef ENABLE_DEBUG_LOG
   if (OB_LIKELY(nullptr != dummy_mem_context_)) {
    DESTROY_CONTEXT(dummy_mem_context_);
    dummy_mem_context_ = nullptr;
  }
  #endif
}

OB_INLINE void ObOperator::clear_evaluated_flag()
{
  for (int i = 0; i < eval_infos_.count(); i++) {
    eval_infos_.at(i)->clear_evaluated_flag();
  }
}

void ObOperator::clear_datum_eval_flag()
{
  if (eval_ctx_.get_batch_size() <= 0) {
    clear_evaluated_flag();
  } else {
    do_clear_datum_eval_flag();
  }
}

inline int ObOperator::try_check_status()
{
  return ((++try_check_tick_) % CHECK_STATUS_TRY_TIMES == 0)
      ? check_status()
      : common::OB_SUCCESS;
}

inline int ObOperator::try_check_status_by_rows(const int64_t rows)
{
  int ret = common::OB_SUCCESS;
  try_check_tick_ += rows;
  if (try_check_tick_ > CHECK_STATUS_ROWS) {
    try_check_tick_ = 0;
    ret = check_status();
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_OB_OPERATOR_H_
