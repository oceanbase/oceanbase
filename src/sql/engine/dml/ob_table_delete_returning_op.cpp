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
#include "sql/engine/dml/ob_table_delete_returning_op.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_task_executor_ctx.h"

namespace oceanbase {
using namespace storage;
using namespace share;
namespace sql {
OB_SERIALIZE_MEMBER((ObTableDeleteReturningSpec, ObTableDeleteSpec));

OB_SERIALIZE_MEMBER((ObTableDeleteReturningOpInput, ObTableModifyOpInput));

int ObTableDeleteReturningOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* ec = GET_TASK_EXECUTOR_CTX(ctx_);
  CK(NULL != ec);
  OX(partition_service_ = ec->get_partition_service());
  CK(NULL != partition_service_);
  OZ(ObTableDeleteOp::inner_open());
  return ret;
}

int ObTableDeleteReturningOp::get_next_row()
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* my_session = ctx_.get_my_session();
  if (OB_ISNULL(my_session) || OB_ISNULL(MY_SPEC.get_child())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid operator", K(ret), KP(my_session), KP(MY_SPEC.get_child()));
  } else if (OB_FAIL(try_check_status())) {
    LOG_WARN("check status failed", K(ret));
  } else if (OB_FAIL(ObTableDeleteOp::inner_get_next_row())) {
    if (OB_ITER_END == ret) {
    } else {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (1 != part_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part_infos", K(ret));
  } else {
    if (NULL == delete_row_ceils_ && OB_INVALID_COUNT == child_row_count_) {
      child_row_count_ = MY_SPEC.get_child()->get_output_count();
      delete_row_ceils_ = reinterpret_cast<ObObj*>(ctx_.get_allocator().alloc(sizeof(ObObj) * child_row_count_));
    } else if (OB_UNLIKELY(NULL == delete_row_ceils_ || OB_INVALID_COUNT == child_row_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delete_row_ceils_ and child_row_count_ should both init or not", K(ret));
    }

    ObNewRow real_delete_row;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_row_count_; ++i) {
      const ObExpr* child_expr = MY_SPEC.get_child()->output_.at(i);
      ObDatum* datum = NULL;
      if (OB_FAIL(child_expr->eval(get_eval_ctx(), datum))) {
        LOG_WARN("eval child_expr failed", K(ret));
      } else if (OB_FAIL(datum->to_obj(delete_row_ceils_[i], child_expr->obj_meta_, child_expr->obj_datum_map_))) {
        LOG_WARN("datum to obj failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      real_delete_row.cells_ = delete_row_ceils_;
      real_delete_row.count_ = child_row_count_;
      if (OB_FAIL(partition_service_->delete_row(my_session->get_trans_desc(),
              dml_param_,
              part_infos_.at(0).partition_key_,
              MY_SPEC.column_ids_,
              real_delete_row))) {
        LOG_WARN("delete row failed", K(ret));
      } else {
        ctx_.get_physical_plan_ctx()->add_affected_rows(1);
        clear_evaluated_flag();
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
