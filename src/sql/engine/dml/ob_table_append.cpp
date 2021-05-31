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

#include "ob_table_append.h"
#include "sql/ob_phy_table_location.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER(ObTableAppendInput, location_idx_);

int ObTableAppendInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(op);
  location_idx_ = task_info.get_location_idx();
  return ret;
}

OB_SERIALIZE_MEMBER((ObTableAppend, ObSingleChildPhyOperator), table_id_);

int ObTableAppend::Operator2RowIter::get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  const ObNewRow* crow = NULL;
  if (OB_FAIL(op_.get_next_row(ctx_, crow))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row failed", K(ret));
    }
  } else {
    row = const_cast<ObNewRow*>(crow);
  }
  return ret;
}

ObTableAppend::ObTableAppend(ObIAllocator& allocator) : ObSingleChildPhyOperator(allocator), table_id_(OB_INVALID_ID)
{}

ERRSIM_POINT_DEF(ERRSIM_TABLE_APPEND_INNER_OPEN);

int ObTableAppend::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("fail to init operator context", K(ret), K_(id));
  } else if (OB_FAIL(handle_op_ctx(ctx))) {
    LOG_WARN("fail to handle operator context", K(ret));
  }
  ret = ERRSIM_TABLE_APPEND_INNER_OPEN ?: ret;
  return ret;
}

int ObTableAppend::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObTableAppend::get_part_location(
    ObExecContext& ctx, const ObPhyTableLocation& table_location, const ObPartitionReplicaLocation*& out) const
{
  int ret = OB_SUCCESS;
  out = NULL;
  int64_t location_idx = 0;
  if (OB_UNLIKELY(table_location.get_partition_location_list().count() > 1)) {
    ObTableAppendInput* append_input = NULL;
    if ((append_input = GET_PHY_OP_INPUT(ObTableAppendInput, ctx, get_id())) != NULL) {
      location_idx = append_input->get_location_idx();
      LOG_INFO("localtion_idx", K(location_idx));
      if (location_idx >= table_location.get_partition_location_list().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid location idx",
            K(ret),
            K(location_idx),
            "total_count",
            table_location.get_partition_location_list().count());
      }
    } else {
      ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
      LOG_WARN("Multi-partition DML not supported", K(ret), K(table_location));
    }
  }
  if (OB_SUCC(ret)) {
    out = &(table_location.get_partition_location_list().at(location_idx));
  }
  return ret;
}
