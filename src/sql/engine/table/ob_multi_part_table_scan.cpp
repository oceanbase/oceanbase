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

#include "ob_multi_part_table_scan.h"
#include "sql/executor/ob_task_spliter.h"
#include "common/ob_range.h"

namespace oceanbase {
namespace sql {

OB_DEF_SERIALIZE(ObMultiPartTableScanInput)
{
  int ret = OK_;
  ret = ObTableScanInput::serialize(buf, buf_len, pos);
  OB_UNIS_ENCODE(partitions_ranges_);
  return ret;
}

OB_DEF_DESERIALIZE(ObMultiPartTableScanInput)
{
  int ret = OK_;
  ret = ObTableScanInput::deserialize(buf, data_len, pos);
  OB_UNIS_DECODE(partitions_ranges_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMultiPartTableScanInput)
{
  int64_t len = 0;
  len = ObTableScanInput::get_serialize_size();
  OB_UNIS_ADD_LEN(partitions_ranges_);
  return len;
}

ObMultiPartTableScanInput::~ObMultiPartTableScanInput()
{
  partitions_ranges_.release();
}

void ObMultiPartTableScanInput::reset()
{
  partitions_ranges_.release();
}

int ObMultiPartTableScanInput::reassign_ranges(
    ObExecContext& ctx, int64_t table_location_key, int64_t ref_table_id, int64_t& partition_offset)
{
  int ret = OB_SUCCESS;
  int64_t partition_id = OB_INVALID_INDEX;
  ObSEArray<common::ObNewRange, 16> ranges;
  const int64_t cur_partition_offset = partition_offset;
  int64_t next_partition_offset = OB_INVALID_INDEX;
  if (partition_offset >= partitions_ranges_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The partition_offset is out of range", K(ret), K(partition_offset), K(partitions_ranges_.count()));
  } else if (OB_FAIL(partitions_ranges_.get_next_ranges(
                 cur_partition_offset, partition_id, ranges, next_partition_offset))) {
    LOG_WARN("Failed to get partition ranges", K(ret));
  } else if (OB_FAIL(translate_pid_to_ldx(ctx, partition_id, table_location_key, ref_table_id, location_idx_))) {
    LOG_WARN("Failed to translate pid to idx", K(ret));
  } else if (OB_FAIL(key_ranges_.assign(ranges))) {
    LOG_WARN("Assign ranges failed", K(ret));
  } else {
    partition_offset = next_partition_offset;
  }
  LOG_TRACE("Table scan has been reassigned a new range",
      K(ret),
      K(table_location_key),
      K(ref_table_id),
      K(partition_offset),
      K(partition_id),
      K(key_ranges_));
  return ret;
}

int ObMultiPartTableScanInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  if (PHY_MULTI_PART_TABLE_SCAN != op.get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Op type is not PHY_TABLE_SCAN", K(ret), "op_type", op.get_type());
  } else if (ObTaskSpliter::DISTRIBUTED_SPLIT != task_info.get_task_split_type()) {
    ret = OB_NOT_INIT;
    LOG_WARN("Exec type is INVALID_SPLIT", K(ret), K(task_info.get_task_split_type()));
  } else if (OB_FAIL(ObTableScanInput::init(ctx, task_info, op))) {
    LOG_WARN("Init table scan failed", K(ret));
  } else {
    /**
     * For multi partition scan
     */
    partitions_ranges_.release();
    ObPartitionScanRanges* partition_range;
    ObIArray<ObTaskInfo::ObPartLoc>& partition_scan_ranges = task_info.get_range_location().part_locs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_scan_ranges.count(); ++i) {
      const ObTaskInfo::ObPartLoc& part_loc = partition_scan_ranges.at(i);
      if (OB_INVALID_ID == part_loc.part_key_ref_id_ || part_loc.part_key_ref_id_ == op.get_id()) {
        if (OB_FAIL(partitions_ranges_.get_new_partition_ranges(partition_range))) {
          LOG_WARN("Failed to get new partition ranges", K(ret));
        } else {
          partition_range->partition_id_ = partition_scan_ranges.at(i).partition_key_.get_partition_id();
          auto& ranges = partition_scan_ranges.at(i).scan_ranges_;
          ObNewRange hold_range;
          for (int i = 0; i < ranges.count(); ++i) {
            if (OB_FAIL(common::deep_copy_range(allocator_, ranges.at(i), hold_range))) {
              LOG_WARN("Hold the range in this op ctx failed", K(ret));
            } else if (OB_FAIL(partition_range->ranges_.push_back(hold_range))) {
              LOG_WARN("Failed push back range failed", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && partitions_ranges_.count() > 0 && OB_NOT_NULL(partition_range)) {
      // Magic num, point to the first partition
      location_idx_ = 0;
      // Take is easy, because this range and partition never be scan.
      ret = key_ranges_.assign(partition_range->ranges_);
    }
  }
  return ret;
}

ObMultiPartTableScan::ObMultiPartTableScan(ObIAllocator& allocator) : ObTableScan(allocator)
{}

ObMultiPartTableScan::~ObMultiPartTableScan()
{}

int ObMultiPartTableScan::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMultiPartTableScanCtx* scan_ctx = nullptr;
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table scan ctx", K(ret));
  } else {
    scan_ctx->iter_end_ = false;
    // scan from first partition
    scan_ctx->input_part_offset_ = NOT_INIT;
    scan_ctx->multi_part_scan_state_ = DO_PARTITION_SCAN;
  }
  return ret;
}

int ObMultiPartTableScan::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMultiPartTableScanCtx* scan_ctx = nullptr;
  bool get_next_row = false;
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to get op ctx", K(ret));
  } else {
    while (OB_SUCC(ret) && !get_next_row) {
      switch (scan_ctx->multi_part_scan_state_) {
        case DO_PARTITION_SCAN: {
          if (OB_FAIL(do_next_partition_scan(ctx))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("Failed to do table scan", K(ret));
            } else {
              scan_ctx->multi_part_scan_state_ = EXECUTION_FINISHED;
            }
          } else {
            scan_ctx->multi_part_scan_state_ = OUTPUT_ROWS;
          }
          break;
        }
        case OUTPUT_ROWS: {
          if (OB_FAIL(ObTableScan::inner_get_next_row(ctx, row))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              scan_ctx->multi_part_scan_state_ = DO_PARTITION_SCAN;
            } else {
              LOG_WARN("Failed to get next row", K(ret));
            }
          } else {
            get_next_row = true;
            LOG_DEBUG("Multi table scan got a new row", KPC(row));
          }
          break;
        }
        case EXECUTION_FINISHED: {
          ret = OB_ITER_END;
          break;
        }
        default: { /* Do nothing */
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
  }
  return ret;
}

int ObMultiPartTableScan::do_next_partition_scan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMultiPartTableScanInput* scan_input = nullptr;
  ObMultiPartTableScanCtx* scan_ctx = nullptr;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObMultiPartTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to get op ctx", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else {
    if (scan_ctx->input_part_offset_ >= scan_input->partitions_ranges_.count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(scan_input->reassign_ranges(
                   ctx, table_location_key_, ref_table_id_, scan_ctx->input_part_offset_))) {
      LOG_WARN("Failed to reassign ranges", K(ret), K(scan_ctx->input_part_offset_));
    } else if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
      LOG_WARN("Failed to do rescan", K(ret));
    } else if (OB_FAIL(ObTableScan::inner_close(ctx))) {
      LOG_WARN("Failed to close table scan", K(ret));
    } else if (OB_FAIL(ObTableScan::update_scan_param_pkey(ctx))) {
      LOG_WARN("Failed to update scan param parition key", K(ret));
    } else if (OB_FAIL(do_table_scan(ctx, true, false))) {
      // deal with is_data_not_readable_err first
      if (is_data_not_readable_err(ret)) {
        ObQueryRetryInfo& retry_info = my_session->get_retry_info_for_update();
        int add_ret = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS != (add_ret = retry_info.add_invalid_server_distinctly(ctx.get_addr(), true)))) {
          LOG_WARN(
              "fail to add addr to invalid servers distinctly", K(ret), K(add_ret), K(ctx.get_addr()), K(retry_info));
        }
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to do table scan", K(ret));
      }
    } else {
      scan_ctx->add_scan_times();
    }
  }
  return ret;
}

int ObMultiPartTableScan::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMultiPartTableScanCtx* scan_ctx = nullptr;
  ObTaskExecutorCtx* task_exec_ctx = ctx.get_task_executor_ctx();
  LOG_DEBUG("Open multi part table scan operator", K(get_id()));
  if (OB_ISNULL(task_exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Task exec ctx is nullptr", K(ret));
  } else if (OB_NOT_NULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartTableScanCtx, ctx, get_id()))) {
    scan_ctx->iter_end_ = false;
    scan_ctx->input_part_offset_ = NOT_INIT;
    scan_ctx->multi_part_scan_state_ = DO_PARTITION_SCAN;
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do thing
  } else if (OB_UNLIKELY(ObTableScan::partition_list_is_empty(task_exec_ctx->get_table_locations()))) {
    // do nothing
  } else if (OB_FAIL(ObTableScan::prepare_scan_param(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("prepare scan param failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObMultiPartTableScan::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScan::inner_close(ctx))) {
    LOG_WARN("inner close failed", K(ret));
  }
  return ret;
}

int ObMultiPartTableScan::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = nullptr;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObMultiPartTableScanInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
  } else {
    UNUSED(input);
  }
  return ret;
}

int ObMultiPartTableScan::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = nullptr;
  ObSQLSessionInfo* my_session = nullptr;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMultiPartTableScanCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
    LOG_WARN("init current row failed", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(static_cast<ObMultiPartTableScanCtx*>(op_ctx)->init_table_allocator(ctx))) {
    LOG_WARN("fail to init tabl allocator", K(ret));
  }
  return ret;
}

void ObMultiPartTableScan::get_used_range_count(ObExecContext& ctx, int64_t& range_count) const
{
  ObMultiPartTableScanCtx* scan_ctx = nullptr;
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartTableScanCtx, ctx, get_id()))) {
    range_count = 0;
  } else {
    range_count = scan_ctx->scan_time();
  }
}

}  // namespace sql
}  // namespace oceanbase
