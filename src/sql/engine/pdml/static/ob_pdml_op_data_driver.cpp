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
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;


ObPDMLOpDataDriver::~ObPDMLOpDataDriver()
{
}

// data_service 拆分为 DMLDataReader、DMLDataWriter 两个接口，
// 从概念上彻底解耦读数据、写数据
// 初始化pdml data driver中的cache结构
int ObPDMLOpDataDriver::init(const ObTableModifySpec &spec,
                             ObIAllocator &allocator,
                             ObDMLBaseRtDef &dml_rtdef,
                             ObDMLOpDataReader *reader,
                             ObDMLOpDataWriter *writer,
                             ObDMLOpUniqueRowChecker *uniq_checker,
                             const bool is_heap_table_insert,
                             const bool with_barrier/*false*/)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  last_row_.reuse_ = true; // 不需要每次暂存行都重新分配内存，重用上一行的内存
  op_monitor_info_.otherstat_1_value_ = 0;
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::PDML_PARTITION_FLUSH_TIME;
  op_monitor_info_.otherstat_2_value_ = 0;
  op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::PDML_PARTITION_FLUSH_COUNT;
  if (OB_ISNULL(reader)
      || OB_ISNULL(writer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(reader), K(writer));
  } else {
    reader_ = reader;
    writer_ = writer;
    uniq_checker_ = uniq_checker;
    dml_rtdef_ = &dml_rtdef;
    is_heap_table_insert_ = is_heap_table_insert;
    with_barrier_ = with_barrier;
  }
  // 初始化cache对象
  if (OB_SUCC(ret)) {
    // 初始化cache的时候，需要考虑上barrier
    // 1. 无barrier情况下不dump
    // 2. 有barier情况下需要进行dump
    // TODO: 当前算子实际处理的分区数先填写为1，决定 hashmap bucket数
    //       需要 CG 实际计算好传进来
    if (OB_FAIL(cache_.init(MTL_ID(), 1, with_barrier_, spec))) {
      LOG_WARN("failed to init batch row cache", K(ret));
    } else {
      LOG_TRACE("init pdml data driver", KPC(dml_rtdef_));
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
  last_row_tablet_id_ = OB_INVALID_ID;
  last_row_expr_ = nullptr;
  op_id_ = OB_INVALID_ID;
  is_heap_table_insert_ = false;
  with_barrier_ = false;
  dfo_id_ = OB_INVALID_ID;
  return ret;
}

int ObPDMLOpDataDriver::set_dh_barrier_param(uint64_t op_id,
                                             const ObPxMultiPartModifyOpInput *modify_input)
{
    int ret = OB_SUCCESS;
    op_id_ = op_id;
    if (OB_ISNULL(modify_input)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table modify is null", K(ret));
    } else {
      dfo_id_ = modify_input->get_dfo_id();
    }
    return ret;
}

int ObPDMLOpDataDriver::get_next_row(ObExecContext &ctx, const ObExprPtrIArray &row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  do {
    // STEP1. 每次 get_next_row 时都尝试驱动取下一批数据，并将其刷入存储层
    if (FILL_CACHE == state_) {
      if (OB_FAIL(cache_.reuse_after_rows_processed())) { // reuse cache，只会清理状态；不会释放内存，方便内存重用
        LOG_WARN("fail reuse cache", K(ret));
      } else if (OB_FAIL(fill_cache_unitl_cache_full_or_child_iter_end(ctx))) { // 填充cache
        LOG_WARN("failed to fill the cache", K(ret));
      } else if (!cache_.empty()) {
        if (OB_FAIL(write_partitions(ctx))) { // 将cache中的数据进行dml操作
          LOG_WARN("fail write partitions", K(ret));
        } else if (OB_FAIL(switch_to_returning_state(ctx))) {
          LOG_WARN("fail init returning state, fail transfer state to ROW_RETURNING", K(ret));
        } else {
          state_ = ROW_RETURNING;
        }
      } else {
        // 填充数据后，cache中仍然没有数据，表示没有数据
        state_ = ROW_RETURNING;
        ret = OB_ITER_END;
      }

      // 没有数据，或者数据都已经写入存储层，则 barrier 等待全局完成写入
      if (with_barrier_ && (OB_ITER_END == ret || OB_SUCCESS == ret)) {
        int tmp_ret = barrier(ctx);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("barrier fail. fail wait all dml op finish", K(tmp_ret), K(ret));
          ret = tmp_ret;
        }
      }
    }

    // STEP2. get_next_row 从 cache 中返回数据
    if (OB_SUCC(ret) && ROW_RETURNING == state_) {
      if (OB_FAIL(next_row_from_cache_for_returning(row))) {
        if (OB_ITER_END == ret) {
          if (!with_barrier_) {
            // 表示cache中的数据已经读取完毕，需要重新填充
            ret = OB_SUCCESS;
            state_ = FILL_CACHE;
          }
        } else {
          // 出现异常错误
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


int ObPDMLOpDataDriver::fill_cache_unitl_cache_full_or_child_iter_end(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the reader is null", K(ret));
    // 尝试追加上一次从child中读取出来，但是没有添加到cache中的row数据
  } else if (OB_FAIL(try_write_last_pending_row())) {
    LOG_WARN("fail write last pending row into cache", K(ret));
  } else {
    do {
      const ObExprPtrIArray *row = nullptr;
      ObTabletID tablet_id;
      if (OB_FAIL(reader_->read_row(ctx, row, tablet_id))) {
        if (OB_ITER_END == ret) {
          // 当前reader的数据已经读取结束
          // do nothing
        } else {
          LOG_WARN("failed to read row from reader", K(ret));
        }
      } else if (is_heap_table_insert_ && OB_FAIL(set_heap_table_hidden_pk(row, tablet_id))) {
        LOG_WARN("fail to set heap table hidden pk", K(ret), K(*row), K(tablet_id));
      } else if (OB_FAIL(cache_.add_row(*row, tablet_id))) {
        if (!with_barrier_ && OB_EXCEED_MEM_LIMIT == ret) {
          // 目前暂时不支持缓存最后一行数据
          // 如果出现了最后一行数据无法push到内存中，就直接报错返回
          LOG_TRACE("the cache is overflow, the current row will be cached in the last row",
                    K(ret), KPC(row), K(tablet_id));
          // 暂时保留当前行到last_row中，等待下一轮填充cache的时候，
          // 通过`try_write_last_pending_row`函数将last row的数据写入到cache中
          if (OB_FAIL(last_row_.save_store_row(*row, *eval_ctx_))) {
            LOG_WARN("fail cache last row", K(*row), K(ret));
          } else {
            last_row_tablet_id_ = tablet_id;
            last_row_expr_ = row;
          }
          break;
        } else {
          LOG_WARN("failed to add row to cache", K_(with_barrier), K(ret));
        }
      } else {
        LOG_DEBUG("add row to cache successfully", "row", ROWEXPR2STR(*eval_ctx_, *row), K(tablet_id));
      }
    } while (OB_SUCCESS == ret);

    // reader已经读取完毕的错误，可以处理
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}


// 将 cache 中缓存的所有 partition 数据都写入到存储层
// 注意：数据写入完成后不能从 cache 中释放，因为这些数据还需要
// return 到 DML 上面的算子继续使用
int ObPDMLOpDataDriver::write_partitions(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObTabletIDArray tablet_id_array;
  ObPDMLOpRowIterator *row_iter = nullptr;
  if (OB_ISNULL(writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the writer is null", K(ret));
  } else if (OB_FAIL(cache_.get_part_id_array(tablet_id_array))) {
    LOG_WARN("fail get part index iterator", K(ret));
  } else {
    // 调用存储层写接口的次数 (flush 的次数)
    op_monitor_info_.otherstat_2_value_++;
    // 消耗在存储层的总时间
    TimingGuard g(op_monitor_info_.otherstat_1_value_);
    // 按照分区逐个写入存储层
    FOREACH_X(it, tablet_id_array, OB_SUCC(ret)) {
      ObTabletID tablet_id = *it;
      ObDASTabletLoc *tablet_loc = nullptr;
      ObDASTableLoc *table_loc = dml_rtdef_->das_base_rtdef_.table_loc_;
      if (OB_FAIL(cache_.get_row_iterator(tablet_id, row_iter))) {
        LOG_WARN("fail get row iterator", K(tablet_id), K(ret));
      } else if (OB_FAIL(DAS_CTX(ctx).extended_tablet_loc(*table_loc, tablet_id, tablet_loc))) {
        LOG_WARN("extended tablet location failed", K(ret));
      } else if (FALSE_IT(row_iter->set_uniq_row_checker(uniq_checker_))) {
        // nop
      } else if (OB_FAIL(writer_->write_rows(ctx, tablet_loc, *row_iter))) {
        LOG_WARN("fail write rows", K(tablet_id), K(ret));
      }
      if (NULL != row_iter) {
        row_iter->close();
        row_iter = NULL;
      }
    }
  }
  return ret;
}

// 上次从 data_service 读出的行尝试写入 cache 时遭遇 cache
// 报 size overflow，这一行被记录为 last_row_，待 cache
// 数据写到存储层后再次将 last_row_ 写入 cache
inline int ObPDMLOpDataDriver::try_write_last_pending_row()
{
  int ret = OB_SUCCESS;
  if (last_row_tablet_id_.is_valid()) {
    ObChunkDatumStore::StoredRow *store_row = last_row_.store_row_;
    if (OB_ISNULL(store_row) || OB_ISNULL(eval_ctx_) || OB_ISNULL(last_row_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected", KP(store_row), KP_(last_row_expr), K(ret));
    } else if (OB_FAIL(store_row->to_expr(*last_row_expr_, *eval_ctx_))) {
      LOG_WARN("fail store row to expr", K(ret));
    } else if (OB_FAIL(cache_.add_row(*last_row_expr_, last_row_tablet_id_))) {
      LOG_WARN("fail add cached last row", K(ret), K(last_row_tablet_id_));
    } else {
      // 将上一次遗留下来的一行添加到cache中后，清理last row指针与last row part id的值
      // 但是，为了内存重用，last_row_ 的内存不清理
      last_row_tablet_id_.reset();
      last_row_expr_ = nullptr;
    }
  }
  return ret;
}

// 每次 fill cache 结束后，将状态转换到 ROW_RETURNING 态
int ObPDMLOpDataDriver::switch_to_returning_state(ObExecContext &ctx)
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
    if (OB_FAIL(cache_.get_part_id_array(returning_ctx_.tablet_id_array_))) {
      LOG_WARN("failed to get part id array for init returning state", K(ret));
    } else if (0 == returning_ctx_.tablet_id_array_.count()) { // TODO: 冗余判断
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part id array is empty for init returning state", K(ret));
    }
  }
  return ret;
}

int ObPDMLOpDataDriver::barrier(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler *handler = ctx.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("barrier only supported in parallel execution mode", K_(with_barrier));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "barrier in non-px mode");
  } else if ((!with_barrier_) || (dfo_id_ == OB_INVALID_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K_(with_barrier), K(dfo_id_ == OB_INVALID_ID), K(ret));
  } else {
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    ObBarrierPieceMsg piece;
    piece.source_dfo_id_ = dfo_id_;
    piece.target_dfo_id_ = dfo_id_;
    piece.op_id_ = op_id_;
    piece.thread_id_ = GETTID();
    const ObBarrierWholeMsg *whole = nullptr;
    if (OB_FAIL(proxy.get_dh_msg_sync(op_id_,
                                      dtl::DH_BARRIER_WHOLE_MSG,
                                      piece,
                                      whole,
                                      ctx.get_physical_plan_ctx()->get_timeout_timestamp()))) {
      LOG_WARN("fail get barrier msg", K(ret));
    }
  }
  return ret;
}

int ObPDMLOpDataDriver::next_row_from_cache_for_returning(const ObExprPtrIArray &row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  do {
    if (OB_ISNULL(returning_ctx_.row_iter_)) {
      // do nothing
    } else if (OB_FAIL(returning_ctx_.row_iter_->get_next_row(row))) {
      if (OB_ITER_END == ret) {
        // 当前partition的row iter数据迭代完，需要切换下一个partition
        ret = OB_SUCCESS;
        LOG_TRACE("current partition row iter has been iterated to end",
          K(returning_ctx_.next_idx_));
      } else {
        LOG_WARN("failed to get next row from returning ctx row iter", K(ret));
      }
    } else {
      found = true;
    }
    if (OB_SUCC(ret) && !found) {
      // 切换下一个partition
      if (OB_FAIL(switch_row_iter_to_next_partition())) {
        if (OB_ITER_END == ret) {
          // 表示没有下一个partition，返回OB_ITER_END
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
  if (returning_ctx_.row_iter_) {
    returning_ctx_.row_iter_->close();
  }
  // 当前仅仅cache一行数据
  // next idx仅仅等于0，如果next idx等于1，表示没有数据
  if (OB_SUCC(ret) && returning_ctx_.next_idx_ >= returning_ctx_.tablet_id_array_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(cache_.get_row_iterator(
              returning_ctx_.tablet_id_array_.at(returning_ctx_.next_idx_),
              returning_ctx_.row_iter_))) {
    int64_t next_idx = returning_ctx_.next_idx_;
    LOG_WARN("failed to get next partition iterator", K(ret),
        "part_id", returning_ctx_.tablet_id_array_.at(next_idx), K(next_idx));
  } else {
    returning_ctx_.row_iter_->set_uniq_row_checker(nullptr);
    returning_ctx_.next_idx_++;
  }
  return ret;
}

int ObPDMLOpDataDriver::set_heap_table_hidden_pk(const ObExprPtrIArray *&row, ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  uint64_t autoinc_seq = 0;
  ObSQLSessionInfo *my_session = eval_ctx_->exec_ctx_.get_my_session();
  uint64_t tenant_id = my_session->get_effective_tenant_id();
  if (OB_FAIL(ObDMLService::get_heap_table_hidden_pk(tenant_id,
                                                    tablet_id,
                                                    autoinc_seq))) {
    LOG_WARN("fail to het hidden pk", K(ret), K(tablet_id), K(tenant_id));
  } else {
    ObExpr *auto_inc_expr = nullptr;
    uint64_t next_autoinc_val = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < row->count(); ++i) {
      if (row->at(i)->type_ == T_TABLET_AUTOINC_NEXTVAL) {
        auto_inc_expr = row->at(i);
        break;
      }
    }
    if (OB_ISNULL(auto_inc_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot find tablet autoinc expr", KPC(row));
    } else {
      ObDatum &datum = auto_inc_expr->locate_datum_for_write(*eval_ctx_);
      datum.set_uint(autoinc_seq);
      auto_inc_expr->set_evaluated_projected(*eval_ctx_);
    }
  }
  return ret;
}
