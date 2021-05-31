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

#include "ob_table_update_returning_op.h"
#include "share/partition_table/ob_partition_location.h"

namespace oceanbase {
namespace sql {

OB_SERIALIZE_MEMBER((ObTableUpdateReturningOpInput, ObTableUpdateOpInput));

OB_SERIALIZE_MEMBER((ObTableUpdateReturningSpec, ObTableUpdateSpec));

int ObTableUpdateReturningOp::do_table_update()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_));
  } else if (OB_FAIL(get_part_location(part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (part_infos_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of part info is error", K(ret), K(part_infos_.count()));
  } else {
    part_key_ = part_infos_.at(0).partition_key_;
  }
  return ret;
}

int ObTableUpdateReturningOp::get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_row_update())) {
    if (OB_ITER_END != ret && OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("update row failed", K(ret));
    }
    // update affected rows on iterate end
    ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
    ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
    plan_ctx->set_row_matched_count(found_rows_);
    plan_ctx->set_row_duplicated_count(changed_rows_);
    plan_ctx->set_affected_rows(
        my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS ? found_rows_ : affected_rows_);
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
