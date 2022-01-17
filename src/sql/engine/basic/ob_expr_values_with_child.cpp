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
#include "sql/engine/basic/ob_expr_values_with_child.h"
#include "share/object/ob_obj_cast.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {
class ObExprValuesWithChild::ObExprValuesWithChildCtx : public ObPhyOperatorCtx {
public:
  explicit ObExprValuesWithChildCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx), node_idx_(0)
  {}
  ~ObExprValuesWithChildCtx()
  {}
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }

private:
  int64_t node_idx_;
  friend class ObExprValuesWithChild;
};

ObExprValuesWithChild::ObExprValuesWithChild(ObIAllocator& alloc) : ObSingleChildPhyOperator(alloc), values_(alloc)
{}

ObExprValuesWithChild::~ObExprValuesWithChild()
{}

int ObExprValuesWithChild::init_value_count(int64_t value_cnt)
{
  return values_.init(value_cnt);
}

int ObExprValuesWithChild::add_value(ObSqlExpression* value)
{
  return values_.push_back(value);
}

int ObExprValuesWithChild::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObExprValuesWithChildCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
    LOG_WARN("create current row failed", K(ret));
  }
  return ret;
}

int ObExprValuesWithChild::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesWithChildCtx* values_ctx = NULL;

  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is not set", K(ret));
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator context failed", K(ret));
  } else if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesWithChildCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else {
    values_ctx->node_idx_ = 0;
  }
  return ret;
}

int ObExprValuesWithChild::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesWithChildCtx* values_ctx = NULL;
  if (OB_FAIL(ObSingleChildPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan ObSingleChildPhyOperator failed", K(ret));
  } else if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesWithChildCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("values_ctx is null");
  } else {
    values_ctx->node_idx_ = 0;
  }
  return ret;
}

int ObExprValuesWithChild::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesWithChildCtx* values_ctx = NULL;
  if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesWithChildCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    values_ctx->node_idx_ = 0;
  }
  return ret;
}

int ObExprValuesWithChild::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, value_start_next_row);
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObExprValuesWithChildCtx* values_ctx = NULL;
  // reset temp auto-increment value before calculate a row

  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is null");
  } else {
    plan_ctx->set_autoinc_id_tmp(0);
  }
  if (!OB_SUCC(ret)) {
    // do nothing
  } else if (OB_ISNULL(values_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesWithChildCtx, ctx, id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(calc_next_row(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from row store failed", K(ret));
    }
  } else {
    row = &(values_ctx->get_cur_row());
    LOG_DEBUG("get next row from expr values", K(*row));
    NG_TRACE_TIMES(2, value_end_next_row);
  }
  return ret;
}

int ObExprValuesWithChild::calc_next_row(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObExprValuesWithChildCtx* value_ctx = NULL;
  ObSQLSessionInfo* session = ctx.get_my_session();
  int64_t col_num = get_column_count();
  int64_t col_index = 0;
  NG_TRACE_TIMES(2, value_start_calc_row);
  if (OB_UNLIKELY(get_size() <= 0 || get_column_count() <= 0 || get_size() % get_column_count() != 0 ||
                  OB_ISNULL(my_phy_plan_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr values status is unexpected", K(get_size()), K(get_column_count()), K_(my_phy_plan));
  } else if (OB_ISNULL(value_ctx = GET_PHY_OPERATOR_CTX(ObExprValuesWithChildCtx, ctx, id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value_ctx is null");
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(my_phy_plan_, session, value_ctx->expr_ctx_.cast_mode_))) {
    LOG_WARN("get cast mode failed", K(ret));
  } else {
    ObExprCtx& expr_ctx = value_ctx->expr_ctx_;
    ObNewRow& cur_row = value_ctx->get_cur_row();
    if (OB_UNLIKELY(value_ctx->node_idx_ == get_size())) {
      // there is no values any more
      ret = OB_ITER_END;
    } else if (OB_UNLIKELY(cur_row.is_invalid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur row is not init", K(cur_row));
    } else if (OB_FAIL(try_get_next_row(ctx))) {
      LOG_WARN("fail get next row from child", K(ret));
    } else {
      bool is_break = false;
      while (OB_SUCC(ret) && value_ctx->node_idx_ < get_size() && !is_break) {
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

int ObExprValuesWithChild::try_get_next_row(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* sequence_row = nullptr;
  if (OB_FAIL(child_op_->get_next_row(ctx, sequence_row))) {
    LOG_WARN("fail get row from child", KR(ret));
  }
  return ret;
}

int64_t ObExprValuesWithChild::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(values));
  return pos;
}

int ObExprValuesWithChild::serialize(char* buf, int64_t buf_len, int64_t& pos, ObPhyOpSeriCtx& seri_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t len = get_serialize_size_(seri_ctx);
  int64_t value_count = get_size();
  OB_UNIS_ENCODE(UNIS_VERSION);
  OB_UNIS_ENCODE(len);
  int64_t col_num = get_column_count();
  const ObIArray<int64_t>* row_id_list = static_cast<const ObIArray<int64_t>*>(seri_ctx.row_id_list_);
  if (OB_SUCC(ret) && row_id_list != NULL) {
    value_count = col_num * row_id_list->count();
  }
  OB_UNIS_ENCODE(value_count);
  if (OB_SUCC(ret) && row_id_list != NULL) {
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
  }
  if (OB_SUCC(ret) && row_id_list == NULL) {
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
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSingleChildPhyOperator::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize physical operator failed", K(ret));
    }
  }
  return ret;
}

int64_t ObExprValuesWithChild::get_serialize_size(const ObPhyOpSeriCtx& seri_ctx) const
{
  int64_t len = get_serialize_size_(seri_ctx);
  OB_UNIS_ADD_LEN(len);
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  return len;
}

OB_DEF_DESERIALIZE(ObExprValuesWithChild)
{
  int ret = OB_SUCCESS;
  int32_t value_size = 0;
  ObSqlExpression* expr = NULL;
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
    if (OB_FAIL(ObSingleChildPhyOperator::deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize physical operator failed", K(ret));
    }
  }
  return ret;
}

int64_t ObExprValuesWithChild::get_serialize_size_(const ObPhyOpSeriCtx& seri_ctx) const
{
  int64_t len = 0;
  int64_t col_num = get_column_count();
  int64_t value_size = get_size();
  const ObIArray<int64_t>* row_id_list = static_cast<const ObIArray<int64_t>*>(seri_ctx.row_id_list_);
  if (row_id_list != NULL) {
    value_size = col_num * row_id_list->count();
  }
  OB_UNIS_ADD_LEN(value_size);
  if (row_id_list != NULL) {
    ARRAY_FOREACH_NORET(*row_id_list, idx)
    {
      int64_t start_idx = row_id_list->at(idx) * col_num;
      int64_t end_idx = start_idx + col_num;
      for (int64_t i = start_idx; i < end_idx; ++i) {
        if (values_.at(i) != NULL) {
          OB_UNIS_ADD_LEN(*values_.at(i));
        }
      }
    }
  } else {
    ARRAY_FOREACH_NORET(values_, i)
    {
      if (values_.at(i) != NULL) {
        OB_UNIS_ADD_LEN(*values_.at(i));
      }
    }
  }
  len += ObSingleChildPhyOperator::get_serialize_size();
  return len;
}

}  // namespace sql
}  // namespace oceanbase
