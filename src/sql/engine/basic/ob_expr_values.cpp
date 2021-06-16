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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_expr_values.h"
#include "share/object/ob_obj_cast.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_iter_expr_range_param.h"
#include "sql/engine/px/ob_dfo.h"
namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {
OB_SERIALIZE_MEMBER(ObExprValuesInput, partition_id_values_);

class ObExprValues::ObExprValuesCtx : public ObPhyOperatorCtx {
public:
  explicit ObExprValuesCtx(ObExecContext& ctx)
      : ObPhyOperatorCtx(ctx),
        node_idx_(0),
        iter_expr_ctx_(ctx, ctx.get_allocator()),
        value_count_(0),
        switch_value_(false)
  {}
  ~ObExprValuesCtx()
  {}
  int get_value_count(ObExecContext& ctx, bool is_range_param, int64_t partition_id_values, int64_t origin_value_count,
      int64_t col_num);
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }

private:
  int64_t node_idx_;
  ObIterExprCtx iter_expr_ctx_;
  int64_t value_count_;
  bool switch_value_;
  friend class ObExprValues;
};

int ObExprValues::ObExprValuesCtx::get_value_count(
    ObExecContext& ctx, bool is_range_param, int64_t partition_id_values, int64_t origin_value_count, int64_t col_num)
{
  int ret = OB_SUCCESS;
  if (partition_id_values != 0) {
    common::ObIArray<ObPxSqcMeta::PartitionIdValue>* pid_values =
        reinterpret_cast<common::ObIArray<ObPxSqcMeta::PartitionIdValue>*>(partition_id_values);
    int64_t partition_id = ctx.get_expr_partition_id();
    bool find = false;
    CK(partition_id != OB_INVALID_ID);
    if (is_range_param) {
      col_num = 1;
    }
    for (int i = 0; OB_SUCC(ret) && i < pid_values->count() && !find; ++i) {
      if (partition_id == pid_values->at(i).partition_id_) {
        node_idx_ = pid_values->at(i).value_begin_idx_ * col_num;
        value_count_ = node_idx_ + pid_values->at(i).value_count_ * col_num;
        if (OB_FAIL(value_count_ > origin_value_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value count", K(ret), K(value_count_), K(origin_value_count));
        }
        find = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (!find) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected partition id", K(partition_id));
      } else {
        switch_value_ = false;
      }
    }
  } else {
    value_count_ = origin_value_count;
  }
  return ret;
}

ObExprValues::ObExprValues(ObIAllocator& alloc) : ObNoChildrenPhyOperator(alloc), values_(alloc), range_params_(alloc)
{}

ObExprValues::~ObExprValues()
{}

int ObExprValues::init_value_count(int64_t value_cnt)
{
  return values_.init(value_cnt);
}

int ObExprValues::add_value(ObSqlExpression* value)
{
  return values_.push_back(value);
}

int ObExprValues::init_range_params(int64_t range_param_count)
{
  return range_params_.init(range_param_count);
}

int ObExprValues::add_range_param(ObIterExprRangeParam* range_param_expr)
{
  return range_params_.push_back(range_param_expr);
}

int ObExprValues::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObExprValuesCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
    LOG_WARN("create current row failed", K(ret));
  } else {
    op_ctx->iter_expr_ctx_.set_cur_row(&op_ctx->get_cur_row());
  }
  return ret;
}

int ObExprValues::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesCtx* values_ctx = NULL;

  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator context failed", K(ret));
  } else if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else {
    values_ctx->node_idx_ = 0;
    values_ctx->value_count_ = (values_.empty() ? range_params_.count() : values_.count());
    values_ctx->switch_value_ = true;
  }
  return ret;
}

int ObExprValues::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesCtx* values_ctx = NULL;
  ObExprValuesInput* op_input = GET_PHY_OP_INPUT(ObExprValuesInput, ctx, get_id());
  if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("values_ctx is null");
  } else if (OB_NOT_NULL(op_input) && 0 != op_input->partition_id_values_ && ctx.is_gi_restart()) {
    values_ctx->switch_value_ = true;
  } else {
    values_ctx->node_idx_ = 0;
  }
  OZ(ObNoChildrenPhyOperator::rescan(ctx));
  return ret;
}

int ObExprValues::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesCtx* values_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("values_ctx is null");
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx is null", K(ret));
  } else {
    values_ctx->node_idx_ = 0;
    if (values_ctx->expr_ctx_.cur_array_index_ >= plan_ctx->get_bind_array_count() - 1) {
      ret = OB_ITER_END;
    } else {
      ++values_ctx->expr_ctx_.cur_array_index_;
    }
  }
  return ret;
}

int ObExprValues::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesCtx* values_ctx = NULL;
  if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    values_ctx->node_idx_ = 0;
  }
  return ret;
}

int ObExprValues::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, value_start_next_row);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObExprValuesCtx* values_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesCtx, ctx, id_);
  ObExprValuesInput* op_input = GET_PHY_OP_INPUT(ObExprValuesInput, ctx, get_id());
  // reset temp auto-increment value before calculate a row
#if !defined(NDEBUG)
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is null");
  } else if (OB_ISNULL(values_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("values_cts is null");
  }
#endif
  if (OB_SUCC(ret)) {
    plan_ctx->set_autoinc_id_tmp(0);
    if (get_rows() > 10) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check physical plan status failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(op_input)) {
    if (values_ctx->switch_value_ && OB_FAIL(values_ctx->get_value_count(ctx,
                                         !range_params_.empty(),
                                         op_input->partition_id_values_,
                                         range_params_.empty() ? values_.count() : range_params_.count(),
                                         get_column_count()))) {
      LOG_WARN("fail to get value count", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!range_params_.empty()) {
      if (OB_FAIL(calc_next_row_by_range_param(*values_ctx, row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("calc next row by range param failed", K(ret));
        }
      }
    } else if (OB_FAIL(calc_next_row(ctx))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from row store failed", K(ret));
      }
    } else {
      row = &(values_ctx->get_cur_row());
      NG_TRACE_TIMES(2, value_end_next_row);
    }
  }
  return ret;
}

OB_INLINE int ObExprValues::calc_next_row_by_range_param(ObExprValuesCtx& values_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObIterExprRangeParam* iter_range_param = NULL;
  if (OB_UNLIKELY(values_ctx.node_idx_ >= values_ctx.value_count_)) {
    ret = OB_ITER_END;
    LOG_DEBUG("get next row from values iterator end", K(ret), K(values_ctx.node_idx_), K(range_params_.count()));
  } else if (OB_ISNULL(iter_range_param = range_params_.at(values_ctx.node_idx_++))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter range param is null", K(ret), K(values_ctx.node_idx_), K(range_params_));
  } else if (OB_FAIL(iter_range_param->get_next_row(values_ctx.iter_expr_ctx_, row))) {
    LOG_WARN("get next row from range params failed", K(ret), KPC(iter_range_param));
  } else {
    LOG_DEBUG("calc next row by range param", KPC(row), KPC(iter_range_param));
  }
  return ret;
}

OB_INLINE int ObExprValues::calc_next_row(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesCtx* value_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesCtx, ctx, id_);
  ObSQLSessionInfo* session = ctx.get_my_session();
  int64_t col_num = get_column_count();
  int64_t col_index = 0;
  NG_TRACE_TIMES(2, value_start_calc_row);
#if !defined(NDEBUG)
  if (OB_UNLIKELY(get_size() <= 0 || get_column_count() <= 0 || get_size() % get_column_count() != 0 ||
                  OB_ISNULL(my_phy_plan_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr values status is unexpected", K(values_), K(get_column_count()), K_(my_phy_plan));
  } else if (OB_ISNULL(value_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value_ctx is null");
  }
#endif  //! NDEBUG
  if (OB_SUCC(ret)) {
    ObExprCtx& expr_ctx = value_ctx->expr_ctx_;
    ObNewRow& cur_row = value_ctx->get_cur_row();
    if (OB_UNLIKELY(value_ctx->node_idx_ == value_ctx->value_count_)) {
      // there is no values any more
      ret = OB_ITER_END;
    } else if (OB_UNLIKELY(cur_row.is_invalid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur row is not init", K(cur_row));
    } else {
      bool is_break = false;
      while (OB_SUCC(ret) && value_ctx->node_idx_ < value_ctx->value_count_ && !is_break) {
        if (OB_ISNULL(values_.at(value_ctx->node_idx_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("value exprs is null", K(values_.at(value_ctx->node_idx_)));
        } else if (OB_FAIL(values_.at(value_ctx->node_idx_)->calc(expr_ctx, cur_row, cur_row.cells_[col_index]))) {
          LOG_WARN("failed to calc expr", K(ret));
        } else {
          ++value_ctx->node_idx_;
          if (col_index == col_num - 1) {
            // last cell values resolved, output row now
            is_break = true;
          } else {
            col_index = (col_index + 1) % col_num;
          }
        }
      }
    }
  }
  NG_TRACE_TIMES(2, value_after_calc_row);
  return ret;
}

int ObExprValues::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObExprValuesInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("failed to create phy op input.", K(ret));
  } else { /*do nothing.*/
  }
  UNUSED(input);
  return ret;
}

int64_t ObExprValues::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(values));
  return pos;
}

int ObExprValues::serialize(char* buf, int64_t buf_len, int64_t& pos, ObPhyOpSeriCtx& seri_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t len = get_serialize_size_(seri_ctx);
  int64_t value_count = 0;
  bool use_range_param = !range_params_.empty();
  int64_t range_param_count = 0;
  int64_t col_num = get_column_count();
  const ObIArray<int64_t>* row_id_list = static_cast<const ObIArray<int64_t>*>(seri_ctx.row_id_list_);
  if (OB_SUCC(ret)) {
    if (use_range_param) {
      value_count = 0;
      if (row_id_list != NULL) {
        range_param_count = row_id_list->count();
      } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
        range_param_count = seri_ctx.exec_ctx_->get_row_id_list_total_count();
      } else {
        range_param_count = range_params_.count();
      }
    } else {
      range_param_count = 0;
      if (row_id_list != NULL) {
        value_count = col_num * row_id_list->count();
      } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
        value_count = seri_ctx.exec_ctx_->get_row_id_list_total_count() * col_num;
      } else {
        value_count = values_.count();
      }
    }
  }
  OB_UNIS_ENCODE(UNIS_VERSION);
  OB_UNIS_ENCODE(len);
  OB_UNIS_ENCODE(value_count);
  if (OB_SUCC(ret) && !use_range_param) {
    if (row_id_list != NULL) {
      ARRAY_FOREACH(*row_id_list, idx)
      {
        int64_t start_idx = row_id_list->at(idx) * col_num;
        int64_t end_idx = start_idx + col_num;
        for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
          if (OB_ISNULL(values_.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("node is null");
          } else if (OB_FAIL(values_.at(i)->serialize(buf, buf_len, pos))) {
            LOG_WARN("serialize expr node failed", K(ret));
          }
        }
      }
    } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
      const ObIArray<int64_t>* new_row_id_list = NULL;
      for (int array_idx = 0; OB_SUCC(ret) && array_idx < seri_ctx.exec_ctx_->get_row_id_list_array().count();
           ++array_idx) {
        if (OB_ISNULL(new_row_id_list = seri_ctx.exec_ctx_->get_row_id_list_array().at(array_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row id list is null", K(ret));
        } else {
          ARRAY_FOREACH(*new_row_id_list, idx)
          {
            int64_t start_idx = new_row_id_list->at(idx) * col_num;
            int64_t end_idx = start_idx + col_num;
            for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
              if (OB_ISNULL(values_.at(i))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("node is null");
              } else if (OB_FAIL(values_.at(i)->serialize(buf, buf_len, pos))) {
                LOG_WARN("serialize expr node failed", K(ret));
              }
            }
          }
        }
      }
    } else {
      ARRAY_FOREACH(values_, i)
      {
        if (OB_ISNULL(values_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr node is null");
        } else if (OB_FAIL(values_.at(i)->serialize(buf, buf_len, pos))) {
          LOG_WARN("serialize expr node failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObNoChildrenPhyOperator::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize physical operator failed", K(ret));
    }
  }
  OB_UNIS_ENCODE(range_param_count);
  if (OB_SUCC(ret) && use_range_param) {
    if (row_id_list != NULL) {
      ARRAY_FOREACH(*row_id_list, idx)
      {
        int64_t row_index = row_id_list->at(idx);
        if (OB_ISNULL(range_params_.at(row_index))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("range params is invalid", K(ret), K(row_index));
        } else if (OB_FAIL(range_params_.at(row_index)->serialize(buf, buf_len, pos))) {
          LOG_WARN("serialize range params failed", K(ret), K(row_index));
        }
      }
    } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
      const ObIArray<int64_t>* new_row_id_list = NULL;
      for (int array_idx = 0; OB_SUCC(ret) && array_idx < seri_ctx.exec_ctx_->get_row_id_list_array().count();
           ++array_idx) {
        if (OB_ISNULL(new_row_id_list = seri_ctx.exec_ctx_->get_row_id_list_array().at(array_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row id list is null", K(ret));
        } else {
          ARRAY_FOREACH(*new_row_id_list, idx)
          {
            int64_t row_index = new_row_id_list->at(idx);
            if (OB_ISNULL(range_params_.at(row_index))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("range params is invalid", K(ret), K(row_index));
            } else if (OB_FAIL(range_params_.at(row_index)->serialize(buf, buf_len, pos))) {
              LOG_WARN("serialize range params failed", K(ret), K(row_index));
            }
          }
        }
      }
    } else {
      ARRAY_FOREACH(range_params_, i)
      {
        if (OB_ISNULL(range_params_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("range param node is null");
        } else if (OB_FAIL(range_params_.at(i)->serialize(buf, buf_len, pos))) {
          LOG_WARN("serialize range param failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int64_t ObExprValues::get_serialize_size(const ObPhyOpSeriCtx& seri_ctx) const
{
  int64_t len = get_serialize_size_(seri_ctx);
  OB_UNIS_ADD_LEN(len);
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  return len;
}

OB_DEF_DESERIALIZE(ObExprValues)
{
  int ret = OB_SUCCESS;
  int32_t value_size = 0;
  int64_t range_param_count = 0;
  ObSqlExpression* expr = NULL;
  ObExprOperatorFactory expr_factory(my_phy_plan_->get_allocator());
  if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("phy_plan is null");
  }
  OB_UNIS_DECODE(value_size);
  if (OB_FAIL(init_value_count(value_size))) {
    LOG_WARN("init value count failed", K(ret));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < value_size; ++i) {
    if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, expr))) {
      LOG_WARN("make sql expression failed", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null");
    } else if (OB_FAIL(expr->deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize expression failed", K(ret), K(i));
    } else if (OB_FAIL(values_.push_back(expr))) {
      LOG_WARN("add expr node value failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObNoChildrenPhyOperator::deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize physical operator failed", K(ret));
    }
  }
  OB_UNIS_DECODE(range_param_count);
  if (OB_SUCC(ret) && range_param_count > 0) {
    if (OB_FAIL(range_params_.init(range_param_count))) {
      LOG_WARN("init range param store failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_param_count; ++i) {
    ObIterExprRangeParam* range_param_expr = NULL;
    if (OB_FAIL(expr_factory.alloc(T_OP_RANGE_PARAM, range_param_expr))) {
      LOG_WARN("allocate range param expr failed", K(ret));
    } else if (OB_FAIL(range_param_expr->deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize range param expr failed", K(ret));
    } else if (OB_FAIL(range_params_.push_back(range_param_expr))) {
      LOG_WARN("store range param expr failed", K(ret));
    }
  }
  return ret;
}

int64_t ObExprValues::get_serialize_size_(const ObPhyOpSeriCtx& seri_ctx) const
{
  int64_t len = 0;
  int64_t value_count = 0;
  bool use_range_param = !range_params_.empty();
  int64_t range_param_count = 0;
  int64_t col_num = get_column_count();
  const ObIArray<int64_t>* row_id_list = static_cast<const ObIArray<int64_t>*>(seri_ctx.row_id_list_);
  if (use_range_param) {
    value_count = 0;
    if (row_id_list != NULL) {
      range_param_count = row_id_list->count();
    } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
      range_param_count = seri_ctx.exec_ctx_->get_row_id_list_total_count();
    } else {
      range_param_count = range_params_.count();
    }
  } else {
    range_param_count = 0;
    if (row_id_list != NULL) {
      value_count = col_num * row_id_list->count();
    } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
      value_count = seri_ctx.exec_ctx_->get_row_id_list_total_count() * col_num;
    } else {
      value_count = values_.count();
    }
  }
  if (!use_range_param) {
    if (row_id_list != NULL) {
      OB_UNIS_ADD_LEN(value_count);
      ARRAY_FOREACH_NORET(*row_id_list, idx)
      {
        int64_t start_idx = row_id_list->at(idx) * col_num;
        int64_t end_idx = start_idx + col_num;
        for (int64_t i = start_idx; i < end_idx; ++i) {
          if (OB_NOT_NULL(values_.at(i))) {
            OB_UNIS_ADD_LEN(*values_.at(i));
          }
        }
      }
    } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
      int64_t real_value_count = 0;
      len += serialization::encoded_length_i64(real_value_count);
      const ObIArray<int64_t>* new_row_id_list = NULL;
      for (int array_idx = 0; array_idx < seri_ctx.exec_ctx_->get_row_id_list_array().count(); ++array_idx) {
        if (OB_ISNULL(new_row_id_list = seri_ctx.exec_ctx_->get_row_id_list_array().at(array_idx))) {
          LOG_WARN("row id list is null");
        } else {
          for (int idx = 0; idx < new_row_id_list->count(); ++idx) {
            int64_t start_idx = new_row_id_list->at(idx) * col_num;
            int64_t end_idx = start_idx + col_num;
            for (int64_t i = start_idx; i < end_idx; ++i) {
              OB_UNIS_ADD_LEN(*values_.at(i));
            }
          }
        }
      }
    } else {
      OB_UNIS_ADD_LEN(value_count);
      ARRAY_FOREACH_NORET(values_, i)
      {
        if (OB_NOT_NULL(values_.at(i))) {
          OB_UNIS_ADD_LEN(*values_.at(i));
        }
      }
    }
  } else {
    OB_UNIS_ADD_LEN(value_count);
  }
  len += ObNoChildrenPhyOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(range_param_count);
  if (use_range_param) {
    if (row_id_list != NULL) {
      ARRAY_FOREACH_NORET(*row_id_list, idx)
      {
        int64_t row_index = row_id_list->at(idx);
        if (OB_NOT_NULL(range_params_.at(row_index))) {
          OB_UNIS_ADD_LEN(*range_params_.at(row_index));
        }
      }
    } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
      const ObIArray<int64_t>* new_row_id_list = NULL;
      for (int array_idx = 0; array_idx < seri_ctx.exec_ctx_->get_row_id_list_array().count(); ++array_idx) {
        if (OB_ISNULL(new_row_id_list = seri_ctx.exec_ctx_->get_row_id_list_array().at(array_idx))) {
          LOG_WARN("row id list is null");
        } else {
          ARRAY_FOREACH_NORET(*new_row_id_list, idx)
          {
            int64_t row_index = new_row_id_list->at(idx);
            if (OB_ISNULL(range_params_.at(row_index))) {
              LOG_WARN("range params is invalid", K(row_index));
            } else {
              OB_UNIS_ADD_LEN(*range_params_.at(row_index));
            }
          }
        }
      }
    } else {
      ARRAY_FOREACH_NORET(range_params_, i)
      {
        if (OB_NOT_NULL(range_params_.at(i))) {
          OB_UNIS_ADD_LEN(*range_params_.at(i));
        }
      }
    }
  }
  return len;
}
}  // namespace sql
}  // namespace oceanbase
