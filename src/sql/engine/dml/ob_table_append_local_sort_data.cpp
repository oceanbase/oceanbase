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

#include "ob_table_append_local_sort_data.h"
#include "share/ob_build_index_struct.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_interm_macro_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER((ObTableAppendLocalSortDataInput, ObTableAppendInput), task_id_);

int ObTableAppendLocalSortDataInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableAppendInput::init(ctx, task_info, op))) {
    LOG_WARN("table append input init failed", K(ret));
  } else {
    task_id_ = task_info.get_task_location().get_ob_task_id();
  }
  return ret;
}

ObTableAppendLocalSortData::ObTableAppendLocalSortData(ObIAllocator& allocator)
    : ObTableAppend(allocator), row_(), iter_end_(false)
{}

ObTableAppendLocalSortData::~ObTableAppendLocalSortData()
{}

int ObTableAppendLocalSortData::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* part_service = NULL;
  ObTableAppendLocalSortDataInput* input = NULL;
  const ObPhyTableLocation* table_location = NULL;
  const ObPartitionReplicaLocation* part_location = NULL;
  ObTableAppendLocalSortDataCtx* append_ctx = NULL;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(ObTableAppend::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(part_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, part service must not be NULL", K(ret));
  } else if (OB_ISNULL(append_ctx = GET_PHY_OPERATOR_CTX(ObTableAppendLocalSortDataCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_FAIL(init_cur_row(*append_ctx, true))) {
    LOG_WARN("fail to init cur row", K(ret));
  } else if (OB_ISNULL(input = GET_PHY_OP_INPUT(ObTableAppendLocalSortDataInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, input must not be NULL", K(ret));
  } else if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid physical plan", K(ret), KP(my_phy_plan_));
  } else if (OB_FAIL(my_phy_plan_->get_base_table_version(table_id_, schema_version))) {
    LOG_WARN("fail to get base table version", K(ret), K(table_id_));
  } else if (OB_FAIL(
                 ObTaskExecutorCtxUtil::get_phy_table_location(*executor_ctx, table_id_, table_id_, table_location))) {
    LOG_WARN("fail to get phy table location", K(ret), K(table_id_));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, table location must not be NULL", K(ret));
  } else if (table_location->get_partition_location_list().count() < 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("append macro block does not support zero partition",
        K(ret),
        "partition_cnt",
        table_location->get_partition_location_list().count());
  } else if (OB_FAIL(get_part_location(ctx, *table_location, part_location))) {
    LOG_WARN("fail to get part location", K(ret));
  } else if (OB_ISNULL(part_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, part location must not be NULL", K(ret), K(part_location));
  } else if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, child op must not be NULL", K(ret));
  } else {
    const share::ObPartitionReplicaLocation& part_loc = *part_location;
    ObPartitionKey pkey(table_id_, part_loc.get_partition_id(), part_loc.get_partition_cnt());
    const ObTaskID& task_id = input->get_ob_task_id();
    Operator2RowIter row_iter(ctx, *child_op_);
    ObBuildIndexAppendLocalDataParam param;
    param.execution_id_ = task_id.get_execution_id();
    param.task_id_ = task_id.get_task_id();
    param.index_id_ = table_id_;
    param.schema_version_ = schema_version;
    param.task_cnt_ = task_id.get_task_cnt();
    LOG_INFO("append local sort data", K(pkey), K(param));
    if (OB_FAIL(part_service->append_local_sort_data(pkey, param, row_iter))) {
      LOG_WARN("fail to append local sort data", K(ret), K(table_id_));
    }
  }
  return ret;
}

int ObTableAppendLocalSortData::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableAppendLocalSortDataInput* input = NULL;
  row = NULL;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else {
    if (OB_ISNULL(input = GET_PHY_OP_INPUT(ObTableAppendLocalSortDataInput, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, input must not be NULL", K(ret));
    } else {
      const ObTaskID& task_id = input->get_ob_task_id();
      storage::ObIntermMacroKey key;
      key.execution_id_ = task_id.get_execution_id();
      key.task_id_ = task_id.get_task_id();
      const int64_t serialize_size = key.get_serialize_size();
      void* buf = NULL;
      void* cell_buf = NULL;
      int64_t pos = 0;
      if (OB_ISNULL(buf = ctx.get_allocator().alloc(serialize_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(serialize_size));
      } else if (OB_FAIL(key.serialize(static_cast<char*>(buf), serialize_size, pos))) {
        LOG_WARN("fail to serialize key", K(ret));
      } else if (OB_ISNULL(cell_buf = ctx.get_allocator().alloc(sizeof(ObObj)))) {
        LOG_WARN("fail to alloc ObObj", K(ret));
      } else {
        ObString str(serialize_size, static_cast<char*>(buf));
        row_.assign(static_cast<ObObj*>(cell_buf), 1L);
        row_.cells_[0].set_binary(str);
        row = &row_;
      }
    }
    iter_end_ = true;
  }
  return ret;
}

void ObTableAppendLocalSortData::reclaim_macro_block(const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (1 != row.count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(row));
  } else {
    const char* buf = row.cells_[0].get_string_ptr();
    const int64_t data_len = row.cells_[0].get_string_len();
    ObIntermMacroKey key;
    int64_t pos = 0;
    if (OB_FAIL(key.deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize interm macro key", K(ret));
    } else if (OB_FAIL(INTERM_MACRO_MGR.remove(key))) {
      LOG_WARN("fail to erase interm macro blocks", K(ret), K(key));
    }
  }
}

int ObTableAppendLocalSortData::init_op_ctx(ObExecContext& ctx) const
{
  ObPhyOperatorCtx* op_ctx = NULL;
  return CREATE_PHY_OPERATOR_CTX(ObTableAppendLocalSortDataCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObTableAppendLocalSortData::inner_close(ObExecContext& ctx) const
{
  return ObTableAppend::inner_close(ctx);
}

int ObTableAppendLocalSortData::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableAppendLocalSortDataInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTableAppendLocalSortDataInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create operator input", K(ret), "op_id", get_id(), "op_type", get_type());
  }
  UNUSED(input);
  return ret;
}
