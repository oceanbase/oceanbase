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

#include "sql/engine/pdml/static/ob_pdml_op_data_driver.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObPDMLOpDataDriver::~ObPDMLOpDataDriver()
{
  destroy();
}

// data_service was divided into DMLDataReader,DMLDataWriter,
// decoupling read data and write data

// initialization cache in pdml data driver
int ObPDMLOpDataDriver::init(common::ObIAllocator& allocator, const ObDMLOpTableDesc& tdesc, ObDMLOpDataReader* reader,
    ObDMLOpDataWriter* writer)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  last_row_.reuse_ = true;  // reusing the memory of the previous row,that is not require reallocation of memory every
                            // time the row is staged
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

    // when initializing the cache, we need to consider about the barrier
    // 1. no dump without barrier
    // 2. dump with barrier
    // TODO: the number of partitions processed by the current operator is 1,
    //       witch determines the number of hashmap buckets
    //       it's better that CG cal the actual number of partitions
    if (OB_FAIL(cache_.init(extract_tenant_id(tdesc.index_tid_), 1, with_barrier_))) {
      LOG_WARN("failed to init batch row cache", K(ret));
    } else {
      LOG_TRACE("init pdml data driver", K(tdesc));
    }
  }
  return ret;
}

int ObPDMLOpDataDriver::destroy()
{
  int ret = OB_SUCCESS;
  returning_ctx_.reset();
  cache_.destroy();
  eval_ctx_ = nullptr;
  reader_ = NULL;
  writer_ = NULL;
  state_ = FILL_CACHE;
  last_row_.reset();
  last_row_part_id_ = OB_INVALID_ID;
  last_row_expr_ = nullptr;
  op_id_ = OB_INVALID_ID;
  with_barrier_ = false;
  dfo_id_ = OB_INVALID_ID;
  return ret;
}

int ObPDMLOpDataDriver::set_with_barrier(uint64_t op_id, const ObPxMultiPartModifyOpInput* modify_input)
{
  int ret = OB_SUCCESS;
  op_id_ = op_id;
  with_barrier_ = true;
  if (OB_ISNULL(modify_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table modify is null", K(ret));
  } else {
    dfo_id_ = modify_input->get_dfo_id();
  }
  return ret;
}

int ObPDMLOpDataDriver::get_next_row(ObExecContext& ctx, const ObExprPtrIArray& row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  do {
    // STEP1. Every time get_next_row tries to drive the next batch of data and flash it to the storage
    if (FILL_CACHE == state_) {
      cache_.reuse();  // reuse cache,clear state;will not release memory, convenient memory reuse
      if (OB_FAIL(fill_cache_unitl_cache_full_or_child_iter_end(ctx))) {  // filling cache
        LOG_WARN("failed to fill the cache", K(ret));
      } else if (!cache_.empty()) {
        if (OB_FAIL(write_partitions(ctx))) {  // perform dml operations on the data in cache
          LOG_WARN("fail write partitions", K(ret));
        } else if (OB_FAIL(switch_to_returning_state(ctx))) {
          LOG_WARN("fail init returning state, fail transfer state to ROW_RETURNING", K(ret));
        } else {
          state_ = ROW_RETURNING;
        }
      } else {
        // after filling the data, there is still no data in the cache,
        // indicating that there is no data
        state_ = ROW_RETURNING;
        ret = OB_ITER_END;
      }

      // if there is no data, or the data has been written to the storage,
      // the barrier waits for the global write to be completed
      if (with_barrier_ && (OB_ITER_END == ret || OB_SUCCESS == ret)) {
        int tmp_ret = barrier(ctx);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("barrier fail. fail wait all dml op finish", K(tmp_ret), K(ret));
          ret = tmp_ret;
        }
      }
    }

    // STEP2. get_next_row retrun the data from cache
    if (OB_SUCC(ret) && ROW_RETURNING == state_) {
      if (OB_FAIL(next_row_from_cache(row))) {
        if (OB_ITER_END == ret) {
          if (!with_barrier_) {
            // Indicates that the data in the cache has been read and needs to be refilled
            ret = OB_SUCCESS;
            state_ = FILL_CACHE;
          }
        } else {
          LOG_WARN("failed to next row from cache", K(ret));
        }
      } else {
        found = true;
        LOG_DEBUG("read row from cache", K(row), K(state_));
      }
    }
  } while (OB_SUCC(ret) && FILL_CACHE == state_ && !found);

  return ret;
}

int ObPDMLOpDataDriver::fill_cache_unitl_cache_full_or_child_iter_end(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the reader is null", K(ret));
    // try to append the row data that was read from the child but not added to the cache last time
  } else if (OB_FAIL(try_write_last_pending_row())) {
    LOG_WARN("fail write last pending row into cache", K(ret));
  } else {
    do {
      const ObExprPtrIArray* row = nullptr;
      int64_t part_id = 0;
      if (OB_FAIL(reader_->read_row(ctx, row, part_id))) {
        if (OB_ITER_END == ret) {
          // do nothing
        } else {
          LOG_WARN("failed to read row from reader", K(ret));
        }
      } else if (OB_FAIL(cache_.add_row(*row, part_id))) {
        if (!with_barrier_ && OB_EXCEED_MEM_LIMIT == ret) {
          // currently does not support caching the last row of data
          // if the last line of data cannot be pushed into the memory, it will report an error and return
          LOG_TRACE(
              "the cache is overflow, the current row will be cached in the last row", K(ret), K(*row), K(part_id));
          // temporarily keep the current row in last_row, waiting for the next round of filling the cache,
          // write the data of the last_row to the cache through the `try_write_last_pending_row` func
          if (OB_FAIL(last_row_.save_store_row(*row, *eval_ctx_))) {
            LOG_WARN("fail cache last row", K(*row), K(ret));
          } else {
            last_row_part_id_ = part_id;
            last_row_expr_ = row;
          }
          break;
        } else {
          LOG_WARN("failed to add row to cache", K_(with_barrier), K(ret));
        }
      } else {
        LOG_DEBUG("add row to cache successfully", "row", ROWEXPR2STR(*ctx.get_eval_ctx(), *row), K(part_id));
      }
    } while (OB_SUCCESS == ret);

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

// write data of all partition in the cache to the storage
// Note: the data cannot be free from the cache after the data is written, because the data still useful
// return to the operator above DML to continue to use
int ObPDMLOpDataDriver::write_partitions(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  PartitionIdArray part_id_array;
  ObPDMLOpRowIterator* row_iter = nullptr;
  if (OB_ISNULL(writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the writer is null", K(ret));
  } else if (OB_FAIL(cache_.get_part_id_array(part_id_array))) {
    LOG_WARN("fail get part index iterator", K(ret));
  } else {
    // the number of times the storage write interface is called(the number of flushes)
    op_monitor_info_.otherstat_2_value_++;
    // total time spent in the storage
    TimingGuard g(op_monitor_info_.otherstat_1_value_);
    // write to the storage one by one according to the partition
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

// the last time the row read from data_service tried to write to the cache,
// the cache reported size overflow, and this row was recorded as last_row_,
// after the cache data is written to the storage, write last_row_ into the cache again
inline int ObPDMLOpDataDriver::try_write_last_pending_row()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID != last_row_part_id_) {
    ObChunkDatumStore::StoredRow* store_row = last_row_.store_row_;
    if (OB_ISNULL(store_row) || OB_ISNULL(eval_ctx_) || OB_ISNULL(last_row_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected", KP(store_row), KP_(last_row_expr), K(ret));
    } else if (OB_FAIL(store_row->to_expr(*last_row_expr_, *eval_ctx_))) {
      LOG_WARN("fail store row to expr", K(ret));
    } else if (OB_FAIL(cache_.add_row(*last_row_expr_, last_row_part_id_))) {
      LOG_WARN("fail add cached last row", K(ret), K(last_row_part_id_));
    } else {
      // after adding a row left over from the last time to the cache,
      // clean up the value of the last row pointer and last row part id.
      // However, for memory reuse, the memory of last_row_ is not cleaned up
      last_row_part_id_ = OB_INVALID_ID;
      last_row_expr_ = nullptr;
    }
  }
  return ret;
}

// every time the fill cache ends, the state is changed to ROW_RETURNING state
int ObPDMLOpDataDriver::switch_to_returning_state(ObExecContext& ctx)
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

int ObPDMLOpDataDriver::barrier(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler* handler = ctx.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("barrier only supported in parallel execution mode", K_(with_barrier));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "barrier in non-px mode");
  } else if ((!with_barrier_) || (dfo_id_ == OB_INVALID_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K_(with_barrier), K(dfo_id_ == OB_INVALID_ID), K(ret));
  } else {
    ObPxSQCProxy& proxy = handler->get_sqc_proxy();
    ObBarrierPieceMsg piece;
    piece.dfo_id_ = dfo_id_;
    piece.op_id_ = op_id_;
    piece.thread_id_ = GETTID();
    const ObBarrierWholeMsg* whole = nullptr;
    if (OB_FAIL(proxy.get_dh_msg(op_id_, piece, whole, ctx.get_physical_plan_ctx()->get_timeout_timestamp()))) {
      LOG_WARN("fail get barrier msg", K(ret));
    }
  }
  return ret;
}

int ObPDMLOpDataDriver::next_row_from_cache(const ObExprPtrIArray& row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  do {
    if (OB_ISNULL(returning_ctx_.row_iter_)) {
      // do nothing
    } else if (OB_FAIL(returning_ctx_.row_iter_->get_next_row(row))) {
      if (OB_ITER_END == ret) {
        // the row iter data of the current partition is iterated, and needs to switch to the next partition
        ret = OB_SUCCESS;
        LOG_TRACE("current partition row iter has been iterated to end", K(returning_ctx_.next_idx_));
      } else {
        LOG_WARN("failed to get next row from returning ctx row iter", K(ret));
      }
    } else {
      found = true;
    }
    if (OB_SUCC(ret) && !found) {
      // switch to the next partition
      if (OB_FAIL(switch_row_iter_to_next_partition())) {
        if (OB_ITER_END == ret) {
          // indicated that there is no next partition, return OB_ITER_END
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

int ObPDMLOpDataDriver::switch_row_iter_to_next_partition()
{
  int ret = OB_SUCCESS;
  // currently only one row of data is cached
  // next idx is equal to 0, if next idx is equal to 1, it means that there is no data
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
