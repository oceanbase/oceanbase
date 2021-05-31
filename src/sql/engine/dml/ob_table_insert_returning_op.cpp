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

#include "ob_table_insert_returning_op.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
namespace sql {

OB_SERIALIZE_MEMBER((ObTableInsertReturningOpInput, ObTableInsertOpInput));

OB_SERIALIZE_MEMBER((ObTableInsertReturningSpec, ObTableInsertSpec));

int ObTableInsertReturningOp::inner_open()
{
  int ret = OB_SUCCESS;
  CK(NULL != child_);
  OZ(ObTableInsertOp::inner_open());
  CK(1 == part_infos_.count());
  OZ(set_autoinc_param_pkey(part_infos_.at(0).partition_key_));
  return ret;
}

int ObTableInsertReturningOp::get_next_row()
{
  int ret = OB_SUCCESS;
  const ObExprPtrIArray* output = NULL;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  storage::ObPartitionService* partition_service = executor_ctx->get_partition_service();
  CK(NULL != partition_service);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_next_storage_row(output))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  } else if (NULL == output) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL output returned", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::project_row(output->get_data(), output->count(), new_row_))) {
    LOG_WARN("project row failed", K(ret));
  } else if (OB_FAIL(partition_service->insert_row(my_session->get_trans_desc(),
                 dml_param_,
                 part_infos_.at(0).partition_key_,
                 MY_SPEC.column_ids_,
                 new_row_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("insert row to partition storage failed", K(ret));
    }
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      plan_ctx->set_last_insert_id_cur_stmt(0);
    }
  } else {
    plan_ctx->add_row_matched_count(1);
    plan_ctx->add_affected_rows(1);
  }
  return ret;
}

int ObTableInsertReturningOp::inner_close()
{
  int ret = OB_SUCCESS;
  int sync_ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  OZ(plan_ctx->sync_last_value_local());
  if (OB_SUCCESS != (sync_ret = (plan_ctx->sync_last_value_global()))) {
    LOG_WARN("sync last value global failed", K(sync_ret));
  }
  sync_ret = OB_SUCCESS == ret ? sync_ret : ret;
  // call ObTableInsertOp::inner_close() no matter sync last value success or not.
  ret = ObTableInsertOp::inner_close();
  ret = OB_SUCCESS == ret ? sync_ret : ret;
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
