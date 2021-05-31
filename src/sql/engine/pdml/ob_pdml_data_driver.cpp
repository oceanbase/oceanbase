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

#include "sql/engine/pdml/ob_pdml_data_driver.h"
#include "sql/engine/pdml/ob_px_multi_part_modify.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObPDMLDataDriver::~ObPDMLDataDriver()
{
  destroy();
}

// data_service is subdivided into two interfaces, DMLDataReader and DMLDataWriter,
// Conceptually completely decompress manually read data, write data
// Initialize the cache structure in the pdml data driver
int ObPDMLDataDriver::init(
    ObIAllocator& allocator, const ObDMLTableDesc& tdesc, const ObDMLDataReader* reader, const ObDMLDataWriter* writer)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  op_monitor_info_.otherstat_1_value_ = 0;
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::PDML_PARTITION_FLUSH_TIME;
  op_monitor_info_.otherstat_2_value_ = 0;
  op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::PDML_PARTITION_FLUSH_COUNT;
  if (OB_ISNULL(reader) || OB_ISNULL(writer) || !tdesc.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(reader), K(writer), K(tdesc));
  } else {
    reader_ = reader;
    writer_ = writer;
    tdesc_ = tdesc;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cache_.init(extract_tenant_id(tdesc.index_tid_), 1, with_barrier_))) {
      LOG_WARN("failed to init batch row cache", K(ret));
    } else {
      LOG_TRACE("init pdml data driver", K(tdesc));
    }
  }
  return ret;
}

int ObPDMLDataDriver::destroy()
{
  int ret = OB_SUCCESS;
  reader_ = nullptr;
  writer_ = nullptr;
  modify_input_ = nullptr;
  state_ = FILL_CACHE;
  last_row_ = nullptr;
  last_row_part_id_ = OB_INVALID_ID;
  op_id_ = OB_INVALID_ID;
  cache_.destroy();
  return ret;
}

int ObPDMLDataDriver::get_next_row(ObExecContext& ctx, const ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  do {
    // STEP1. Every time get_next_row, try to drive to fetch a batch of data and flash it to the storage layer
    if (FILL_CACHE == state_) {
      cache_.reuse();                                                     // reuse cache
      if (OB_FAIL(fill_cache_unitl_cache_full_or_child_iter_end(ctx))) {  // fill cache
        LOG_WARN("failed to fill the cache", K(ret));
      } else if (!cache_.empty()) {
        if (OB_FAIL(write_partitions(ctx))) {  // dml operation with cache
          LOG_WARN("fail write partitions", K(ret));
        } else if (OB_FAIL(switch_to_returning_state(ctx))) {
          LOG_WARN("fail init returning state, fail transfer state to ROW_RETURNING", K(ret));
        } else {
          state_ = ROW_RETURNING;
        }
      } else {
        state_ = ROW_RETURNING;
        ret = OB_ITER_END;
      }

      if (with_barrier_ && (OB_ITER_END == ret || OB_SUCCESS == ret)) {
        int tmp_ret = barrier(ctx);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("barrier fail. fail wait all dml op finish", K(tmp_ret), K(ret));
          ret = tmp_ret;
        }
      }
    }

    // STEP2. get_next_row from cache
    if (OB_SUCC(ret) && ROW_RETURNING == state_) {
      if (OB_FAIL(next_row_from_cache(row))) {
        if (OB_ITER_END == ret) {
          if (!with_barrier_) {
            // need fill again
            ret = OB_SUCCESS;
            state_ = FILL_CACHE;
          }
        } else {
          // execept
          LOG_WARN("failed to next row from cache", K(ret));
        }
      } else {
        found = true;
        LOG_TRACE("read row from cache", K(*row), K(state_));
      }
    }
  } while (OB_SUCC(ret) && FILL_CACHE == state_ && !found);

  return ret;
}

int ObPDMLDataDriver::fill_cache_unitl_cache_full_or_child_iter_end(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the reader is null", K(ret));
  } else if (OB_FAIL(try_write_last_pending_row())) {
    LOG_WARN("fail write last pending row into cache", K(ret));
  } else {
    do {
      const ObNewRow* row = nullptr;
      int64_t part_id = 0;
      if (OB_FAIL(reader_->read_row(ctx, row, part_id))) {
        if (OB_ITER_END == ret) {
          // do nothing
        } else {
          LOG_WARN("failed to read row from reader", K(ret));
        }
      } else if (OB_FAIL(cache_.add_row(*row, part_id))) {
        if (!with_barrier_ && OB_EXCEED_MEM_LIMIT == ret) {
          LOG_TRACE(
              "the cache is overflow, the current row will be cached in the last row", K(ret), K(*row), K(part_id));
          last_row_ = row;
          last_row_part_id_ = part_id;
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to add row to cache", K_(with_barrier), K(ret));
        }
      } else {
        LOG_TRACE("add row to cache successfully", K(*row), K(part_id));
      }
    } while (OB_SUCCESS == ret);

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPDMLDataDriver::write_partitions(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  PartitionIdArray part_id_array;
  ObPDMLRowIterator* row_iter = nullptr;
  if (OB_ISNULL(writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the writer is null", K(ret));
  } else if (OB_FAIL(cache_.get_part_id_array(part_id_array))) {
    LOG_WARN("fail get part index iterator", K(ret));
  } else {
    // flush times
    op_monitor_info_.otherstat_2_value_++;
    // time cost in storage layer
    TimingGuard g(op_monitor_info_.otherstat_1_value_);
    // write part by part to storage layer
    FOREACH_X(it, part_id_array, OB_SUCC(ret))
    {
      int64_t part_id = *it;
      ObPartitionKey pkey(tdesc_.index_tid_, part_id, tdesc_.partition_cnt_);
      if (OB_FAIL(cache_.get_row_iterator(part_id, row_iter))) {
        LOG_WARN("fail get row iterator", K(part_id), K(pkey), K(ret));
      } else if (OB_FAIL(writer_->write_rows(ctx, pkey, *row_iter))) {
        LOG_WARN("fail write rows", K(part_id), K(pkey), K(ret));
      }
    }
  }
  return ret;
}

inline int ObPDMLDataDriver::try_write_last_pending_row()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(last_row_)) {
    if (OB_FAIL(cache_.add_row(*last_row_, last_row_part_id_))) {
      LOG_WARN("fail add cached last row", K(ret), K(*last_row_), K(last_row_part_id_));
    } else {
      last_row_ = nullptr;
      last_row_part_id_ = OB_INVALID_ID;
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPDMLDataDriver::switch_to_returning_state(ObExecContext& ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (cache_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache is empty for init returning state", K(ret));
  } else {
    returning_ctx_.reset();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cache_.get_part_id_array(returning_ctx_.part_id_array_))) {
      LOG_WARN("failed to get part id array for init returning state", K(ret));
    } else if (0 == returning_ctx_.part_id_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part id array is empty for init returning state", K(ret));
    }
  }
  return ret;
}

int ObPDMLDataDriver::barrier(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler* handler = ctx.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("barrier only supported in parallel execution mode", K_(with_barrier));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "barrier in non-px mode");
  } else if ((!with_barrier_) || OB_ISNULL(modify_input_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K_(with_barrier), KP_(modify_input), K(ret));
  } else {
    ObPxSQCProxy& proxy = handler->get_sqc_proxy();
    ObBarrierPieceMsg piece;
    piece.dfo_id_ = modify_input_->get_dfo_id();
    piece.op_id_ = op_id_;
    piece.thread_id_ = GETTID();
    const ObBarrierWholeMsg* whole = nullptr;
    if (OB_FAIL(proxy.get_dh_msg(op_id_, piece, whole, ctx.get_physical_plan_ctx()->get_timeout_timestamp()))) {
      LOG_WARN("fail get barrier msg", K(ret));
    }
  }
  return ret;
}

int ObPDMLDataDriver::next_row_from_cache(const ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObNewRow* next_row = nullptr;
  do {
    if (OB_ISNULL(returning_ctx_.row_iter_)) {
      // do nothing
    } else if (OB_FAIL(returning_ctx_.row_iter_->get_next_row(next_row))) {
      if (OB_ITER_END == ret) {
        // siwtch partition
        ret = OB_SUCCESS;
        LOG_TRACE("current partition row iter has been iterated to end", K(returning_ctx_.next_idx_));
      } else {
        LOG_WARN("failed to get next row from returning ctx row iter", K(ret));
      }
    } else {
      row = next_row;
      found = true;
    }
    if (OB_SUCC(ret) && !found) {
      // switch next partition
      if (OB_FAIL(switch_row_iter_to_next_partition())) {
        if (OB_ITER_END == ret) {
          // no more partition
          LOG_TRACE("no next partition row iter can be switched to", K(ret));
        } else {
          LOG_WARN("failed to switch next partition row iter", K(ret));
        }
      } else {
        // do nothing
      }
    }
  } while (OB_SUCC(ret) && !found);

  return ret;
}

int ObPDMLDataDriver::switch_row_iter_to_next_partition()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && returning_ctx_.next_idx_ >= returning_ctx_.part_id_array_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(cache_.get_row_iterator(
                 returning_ctx_.part_id_array_.at(returning_ctx_.next_idx_), returning_ctx_.row_iter_))) {
    int64_t next_idx = returning_ctx_.next_idx_;
    LOG_WARN("failed to get next partition iterator",
        K(ret),
        "part_id",
        returning_ctx_.part_id_array_.at(next_idx),
        K(next_idx));
  } else {
    returning_ctx_.next_idx_++;
  }

  return ret;
}
