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

#include "lib/container/ob_fixed_array.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_operator_reg.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/px/ob_px_op_size_factor.h"
#include "sql/engine/px/ob_px_basic_info.h"

namespace oceanbase {
namespace sql {

class ObExpr;
class ObPhysicalPlan;
class ObOpSpec;
class ObOperator;
class ObOpInput;

typedef common::ObFixedArray<ObExpr*, common::ObIAllocator> ExprFixedArray;
typedef common::Ob2DArray<common::ObObjParam, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator, false>
    ParamStore;

struct ObDynamicParamSetter {
  OB_UNIS_VERSION_V(1);

public:
  ObDynamicParamSetter() : param_idx_(common::OB_INVALID_ID), src_(NULL), dst_(NULL)
  {}

  ObDynamicParamSetter(int64_t param_idx, ObExpr* src_expr, ObExpr* dst_expr)
      : param_idx_(param_idx), src_(src_expr), dst_(dst_expr)
  {}
  ~ObDynamicParamSetter()
  {}

  int set_dynamic_param(ObEvalCtx& eval_ctx) const;
  int update_dynamic_param(ObEvalCtx& eval_ctx, common::ObDatum& datum) const;

private:
  void clear_parent_evaluated_flag(ObEvalCtx& eval_ctx, ObExpr& expr) const;

public:
  int64_t param_idx_;  // param idx in param store
  ObExpr* src_;        // original expr which replaced by param expr
  ObExpr* dst_;        // dynamic param expr

  TO_STRING_KV(K_(param_idx), K_(src), K_(dst));
};

class ObOpSpecVisitor;
// Physical operator specification, immutable in execution.
// (same with the old ObPhyOperator)
class ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObOpSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  virtual ~ObOpSpec();

  DECLARE_VIRTUAL_TO_STRING;
  const char* op_name() const
  {
    return ob_phy_operator_type_str(type_);
  }

  // Pre-order recursive create execution components (ObOperator and ObOperatorInput)
  // for current DFO.
  int create_operator(ObExecContext& exec_ctx, ObOperator*& op) const;
  int create_op_input(ObExecContext& exec_ctx) const;
  virtual int register_to_datahub(ObExecContext& exec_ctx) const
  {
    UNUSED(exec_ctx);
    return common::OB_SUCCESS;
  }

  uint32_t get_child_cnt() const
  {
    return child_cnt_;
  }
  ObOpSpec** get_children() const
  {
    return children_;
  }

  int set_children_pointer(ObOpSpec** children, const uint32_t child_cnt);
  int set_child(const uint32_t idx, ObOpSpec* child);

  ObOpSpec* get_parent() const
  {
    return parent_;
  }
  void set_parent(ObOpSpec* parent)
  {
    parent_ = parent;
  }

  int64_t get_output_count() const
  {
    return output_.count();
  }

  ObPhysicalPlan* get_phy_plan()
  {
    return plan_;
  }
  const ObPhysicalPlan* get_phy_plan() const
  {
    return plan_;
  }

  const ObOpSpec* get_child() const
  {
    return child_;
  }
  const ObOpSpec* get_left() const
  {
    return left_;
  }
  const ObOpSpec* get_right() const
  {
    return right_;
  }
  const ObOpSpec* get_child(uint32_t idx) const
  {
    return children_[idx];
  }
  ObOpSpec* get_child(uint32_t idx)
  {
    return children_[idx];
  }

  virtual bool is_table_scan() const
  {
    return false;
  }
  virtual bool is_dml_operator() const
  {
    return false;
  }
  virtual bool is_pdml_operator() const
  {
    return false;
  }
  virtual bool is_receive() const
  {
    return false;
  }

  // same with ObPhyOperator to make template works.
  int32_t get_child_num() const
  {
    return get_child_cnt();
  }
  ObPhyOperatorType get_type() const
  {
    return type_;
  }
  uint64_t get_id() const
  {
    return id_;
  }
  const char* get_name() const
  {
    return get_phy_op_name(type_);
  }

  int accept(ObOpSpecVisitor& visitor) const;
  int64_t get_rows() const
  {
    return rows_;
  }
  int64_t get_cost() const
  {
    return cost_;
  }
  int64_t get_plan_depth() const
  {
    return plan_depth_;
  }

  // find all specs of the DFO (stop when reach receive)
  template <typename T, typename FILTER>
  static int find_target_specs(T& spec, const FILTER& f, common::ObIArray<T*>& res);

  virtual int serialize(char* buf, int64_t buf_len, int64_t& pos, ObPhyOpSeriCtx& seri_ctx) const
  {
    UNUSED(seri_ctx);
    return serialize(buf, buf_len, pos);
  }
  virtual int64_t get_serialize_size(const ObPhyOpSeriCtx& seri_ctx) const
  {
    UNUSED(seri_ctx);
    return get_serialize_size();
  }

private:
  int create_operator_recursive(ObExecContext& exec_ctx, ObOperator*& op) const;
  int create_op_input_recursive(ObExecContext& exec_ctx) const;

  // Data members are accessed in ObOperator class, exposed for convenience.
public:
  // Operator type, serializing externally.
  ObPhyOperatorType type_;
  // Operator id, unique in DFO
  uint64_t id_;
  ObPhysicalPlan* plan_;

  // The %child_, %left_, %right_ should be set when set %children_,
  // make it protected and exposed by interface.
protected:
  ObOpSpec* parent_;
  ObOpSpec** children_;
  uint32_t child_cnt_;
  union {
    ObOpSpec* child_;
    ObOpSpec* left_;
  };
  ObOpSpec* right_;

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

private:
  DISALLOW_COPY_AND_ASSIGN(ObOpSpec);
};

class ObOpSpecVisitor {
public:
  virtual int pre_visit(const ObOpSpec& spec) = 0;
  virtual int post_visit(const ObOpSpec& spec) = 0;
};

// Physical operator, mutable in execution.
// (same with the old ObPhyOperatorCtx)
class ObOperator {
public:
  const static uint64_t CHECK_STATUS_TRY_TIMES = 1024;

public:
  ObOperator(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObOperator();

  const ObOpSpec& get_spec() const
  {
    return spec_;
  }
  ObOpInput* get_input() const
  {
    return input_;
  }

  int set_children_pointer(ObOperator** children, uint32_t child_cnt);
  int set_child(const uint32_t idx, ObOperator* child);

  int get_real_child(ObOperator*& child, const int32_t child_idx);

  int init();

  // open operator, cascading open child operators.
  virtual int open();
  // open operator, not including child operators.
  virtual int inner_open()
  {
    return common::OB_SUCCESS;
  }

  /**
   * @brief reopen the operator, with new params
   * if any operator needs rescan, code generator must have told 'him' that
   * where should 'he' get params, e.g. op_id
   */
  virtual int rescan();

  // switch iterator for array binding.
  virtual int switch_iterator();

  // fetch next row
  // return OB_ITER_END if reach end.
  virtual int get_next_row();
  virtual int inner_get_next_row() = 0;

  // close operator, cascading close child operators
  virtual int close();
  // close operator, not including child operators.
  virtual int inner_close()
  {
    return common::OB_SUCCESS;
  }

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

  const char* op_name() const
  {
    return spec_.op_name();
  }

  OB_INLINE void clear_evaluated_flag();

  // filter row for storage callback.
  // clear expression evaluated flag if row filtered.
  OB_INLINE int filter_row_outside(bool is_before_indexback, const ObExprPtrIArray& exprs, bool& filtered);

  // check execution status: timeout, session kill, interrupted ..
  int check_status();
  // check execution status every CHECK_STATUS_TRY_TIMES tries.
  int try_check_status();

  ObEvalCtx& get_eval_ctx()
  {
    return eval_ctx_;
  }
  ObExecContext& get_exec_ctx()
  {
    return ctx_;
  }

  uint32_t get_child_cnt() const
  {
    return child_cnt_;
  }
  ObOperator* get_child()
  {
    return child_;
  }
  ObOperator* get_child(int32_t child_idx)
  {
    return children_[child_idx];
  }
  bool is_opened()
  {
    return opened_;
  }
  ObMonitorNode& get_monitor_info()
  {
    return op_monitor_info_;
  }

protected:
  int init_evaluated_flags();
  // Execute filter
  // Calc buffer does not reset internally, you need to reset it appropriately.
  int filter(const common::ObIArray<ObExpr*>& exprs, bool& filtered);

  int startup_filter(bool& filtered)
  {
    return filter(spec_.startup_filters_, filtered);
  }
  int filter_row(bool& filtered)
  {
    return filter(spec_.filters_, filtered);
  }

  // try open operator
  int try_open()
  {
    return opened_ ? common::OB_SUCCESS : open();
  }

  // Drain exchange in data for PX, or producer DFO will be blocked.
  virtual int drain_exch();

protected:
  const ObOpSpec& spec_;
  ObExecContext& ctx_;
  ObEvalCtx& eval_ctx_;
  common::ObFixedArray<ObEvalInfo*, common::ObIAllocator> eval_infos_;
  ObOpInput* input_;

  ObOperator* parent_;
  ObOperator** children_;
  uint32_t child_cnt_;
  union {
    ObOperator* child_;
    ObOperator* left_;
  };
  ObOperator* right_;

  uint64_t try_check_times_;

  bool opened_;
  // pass the startup filter (not filtered)
  bool startup_passed_;
  // the exchange operator has been drained
  bool exch_drained_;
  // mark first row emitted
  bool got_first_row_;
  // gv$sql_plan_monitor
  ObMonitorNode op_monitor_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOperator);
};

// Operator input is information of task execution. Some operator need the to get information
// from task executor.
//
// e.g.:
//    table scan need this to get scan partition.
//    exchange operator need this to get DTL channels
//
class ObOpInput {
  OB_UNIS_VERSION_PV();

public:
  ObOpInput(ObExecContext& ctx, const ObOpSpec& spec) : exec_ctx_(ctx), spec_(spec)
  {}
  virtual ~ObOpInput()
  {}

  virtual int init(ObTaskInfo& task_info) = 0;
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator)
  {
    UNUSED(allocator);
  }
  virtual void reset() = 0;

  ObPhyOperatorType get_type() const
  {
    return spec_.type_;
  }
  const ObOpSpec& get_spec() const
  {
    return spec_;
  }

protected:
  ObExecContext& exec_ctx_;
  const ObOpSpec& spec_;
};

template <typename T, typename FILTER>
int ObOpSpec::find_target_specs(T& spec, const FILTER& f, common::ObIArray<T*>& res)
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
{}

OB_INLINE void ObOperator::clear_evaluated_flag()
{
  for (int i = 0; i < eval_infos_.count(); i++) {
    eval_infos_.at(i)->clear_evaluated_flag();
  }
}

inline int ObOperator::try_check_status()
{
  return ((++try_check_times_) % CHECK_STATUS_TRY_TIMES == 0) ? check_status() : common::OB_SUCCESS;
}

// filter row for storage callback.
OB_INLINE int ObOperator::filter_row_outside(bool is_before_indexback, const ObExprPtrIArray& exprs, bool& filtered)
{
  int ret = common::OB_SUCCESS;
  if (is_before_indexback) {
    ret = filter(exprs, filtered);
    // For filter on index, we need to clear evaluated flag for every row after filter(),
    // because the storage will do filter and fetch batch rows for index back, we need to make
    // sure the evaluated flag is cleared when do index back.
    clear_evaluated_flag();
  } else {
    ret = filter(exprs, filtered);
    // For filter on data table (not before index back), clear evaluated flag if row filtered,
    // to reuse the expression result. Table scan inner_get_next_row() will do the clear work
    // if row not filtered.
    if (OB_SUCC(ret) && filtered) {
      clear_evaluated_flag();
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_OB_OPERATOR_H_
