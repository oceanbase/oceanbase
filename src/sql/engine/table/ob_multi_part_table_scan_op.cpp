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

#include "ob_multi_part_table_scan_op.h"
#include "sql/executor/ob_task_spliter.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER((ObMultiPartTableScanOpInput, ObTableScanOpInput), partitions_ranges_);

OB_SERIALIZE_MEMBER((ObMultiPartTableScanSpec, ObTableScanSpec));

ObMultiPartTableScanOpInput::ObMultiPartTableScanOpInput(ObExecContext& ctx, const ObOpSpec& spec)
    : ObTableScanOpInput(ctx, spec), allocator_(common::ObModIds::OB_SQL_TABLE_LOOKUP), partitions_ranges_()
{}

void ObMultiPartTableScanOpInput::reset()
{
  partitions_ranges_.release();
}

int ObMultiPartTableScanOpInput::reassign_ranges(
    int64_t table_location_key, int64_t ref_table_id, int64_t& partition_offset)
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
  } else if (OB_FAIL(translate_pid_to_ldx(partition_id, table_location_key, ref_table_id, location_idx_))) {
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

int ObMultiPartTableScanOpInput::init(ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (PHY_MULTI_PART_TABLE_SCAN != MY_SPEC.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Op type is not PHY_TABLE_SCAN", K(ret), "op_type", MY_SPEC.type_);
  } else if (ObTaskSpliter::DISTRIBUTED_SPLIT != task_info.get_task_split_type()) {
    ret = OB_NOT_INIT;
    LOG_WARN("Exec type is INVALID_SPLIT", K(ret), K(task_info.get_task_split_type()));
  } else if (OB_FAIL(ObTableScanOpInput::init(task_info))) {
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
      if (OB_INVALID_ID == part_loc.part_key_ref_id_ || part_loc.part_key_ref_id_ == MY_SPEC.id_) {
        if (OB_FAIL(partitions_ranges_.get_new_partition_ranges(partition_range))) {
          LOG_WARN("Failed to get new partition ranges", K(ret));
        } else {
          partition_range->partition_id_ = partition_scan_ranges.at(i).partition_key_.get_partition_id();
          if (OB_FAIL(partition_range->ranges_.assign(partition_scan_ranges.at(i).scan_ranges_))) {
            LOG_WARN("assign ranges failed", K(ret));
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

int ObMultiPartTableScanOp::rescan()
{
  int ret = OB_SUCCESS;
  iter_end_ = false;
  input_part_offset_ = NOT_INIT;
  multi_part_scan_state_ = DO_PARTITION_SCAN;
  return ret;
}

int ObMultiPartTableScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool get_next_row = false;
  while (OB_SUCC(ret) && !get_next_row) {
    switch (multi_part_scan_state_) {
      case DO_PARTITION_SCAN: {
        if (OB_FAIL(do_next_partition_scan())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("Failed to do table scan", K(ret));
          } else {
            multi_part_scan_state_ = EXECUTION_FINISHED;
          }
        } else {
          multi_part_scan_state_ = OUTPUT_ROWS;
        }
        break;
      }
      case OUTPUT_ROWS: {
        if (OB_FAIL(ObTableScanOp::inner_get_next_row())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            multi_part_scan_state_ = DO_PARTITION_SCAN;
          } else {
            LOG_WARN("Failed to get next row", K(ret));
          }
        } else {
          get_next_row = true;
          LOG_DEBUG("Multi table scan got a new row", "output", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
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
  return ret;
}

int ObMultiPartTableScanOp::do_next_partition_scan()
{
  int ret = OB_SUCCESS;
  if (input_part_offset_ >= MY_INPUT.partitions_ranges_.count()) {
    // The last partition has been scanned, and this is the real end.
    ret = OB_ITER_END;
  } else if (OB_FAIL(
                 MY_INPUT.reassign_ranges(MY_SPEC.table_location_key_, MY_SPEC.ref_table_id_, input_part_offset_))) {
    LOG_WARN("Failed to reassign ranges", K(ret), K(input_part_offset_));
  } else if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("Failed to do rescan", K(ret));
  } else if (OB_FAIL(ObTableScanOp::inner_close())) {
    LOG_WARN("Failed to close table scan", K(ret));
  } else if (OB_FAIL(ObTableScanOp::update_scan_param_pkey())) {
    LOG_WARN("Failed to update scan param parition key", K(ret));
  } else if (OB_FAIL(do_table_scan(true, false))) {
    LOG_WARN("Failed to do table scan", K(ret));
  } else {
    add_scan_times();
  }

  return ret;
}

// Cover the inner open of the table scan, the main reason is not to do
// table scan and just prepare the scan param.
int ObMultiPartTableScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  LOG_DEBUG("Open multi part table scan operator", K(MY_SPEC.id_));
  if (OB_FAIL(ObTableScanOp::init_table_allocator())) {
    LOG_WARN("init table allocator failed", K(ret));
  } else if (OB_UNLIKELY(ObTableScanOp::partition_list_is_empty(task_exec_ctx->get_table_locations()))) {
    // The number of partitions involved is 0, do nothing
  } else if (OB_FAIL(ObTableScanOp::prepare_scan_param())) {
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

ObMultiPartTableScanOp::ObMultiPartTableScanOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObTableScanOp(exec_ctx, spec, input),
      input_part_offset_(NOT_INIT),
      multi_part_scan_state_(DO_PARTITION_SCAN),
      scan_times_(0)
{}

int ObMultiPartTableScanOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::inner_close())) {
    LOG_WARN("inner close failed", K(ret));
  } else {
    input_part_offset_ = NOT_INIT;
    multi_part_scan_state_ = DO_PARTITION_SCAN;
    scan_times_ = 0;
  }
  return ret;
}

void ObMultiPartTableScanOp::get_used_range_count(int64_t& range_count) const
{
  range_count = scan_times_;
}

}  // end namespace sql
}  // end namespace oceanbase
