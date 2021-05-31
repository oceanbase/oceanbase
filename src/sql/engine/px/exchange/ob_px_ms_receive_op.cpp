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

#include "ob_px_ms_receive_op.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/px/ob_px_scheduler.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace sql {

OB_SERIALIZE_MEMBER((ObPxMSReceiveOpInput, ObPxReceiveOpInput));

OB_SERIALIZE_MEMBER((ObPxMSReceiveSpec, ObPxReceiveSpec), all_exprs_, sort_collations_, sort_cmp_funs_, local_order_);

ObPxMSReceiveSpec::ObPxMSReceiveSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObPxReceiveSpec(alloc, type),
      all_exprs_(alloc),
      sort_collations_(alloc),
      sort_cmp_funs_(alloc),
      local_order_(false)
{}

ObPxMSReceiveOp::ObPxMSReceiveOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObPxReceiveOp(exec_ctx, spec, input),
      ptr_px_row_(&px_row_),
      ptr_row_msg_loop_(&msg_loop_),
      interrupt_proc_(),
      row_heap_(),
      merge_inputs_(),
      finish_(false)
{}

void ObPxMSReceiveOp::destroy()
{
  merge_inputs_.reset();
  row_heap_.reset();
  // no need to reset interrupt_proc_
  ObPxReceiveOp::destroy();
}

// init input information in global order and local order
int ObPxMSReceiveOp::init_merge_sort_input(int64_t n_channel)
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.local_order_) {
    // global order, init merge sort input
    if (0 >= n_channel) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channels are not init", K(ret));
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < n_channel; ++idx) {
        void* buf = ctx_.get_allocator().alloc(sizeof(GlobalOrderInput));
        if (nullptr == buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("create merge sort input fail", K(idx), K(ret));
        } else {
          MergeSortInput* msi = new (buf) GlobalOrderInput(ctx_.get_my_session()->get_effective_tenant_id());
          if (OB_FAIL(merge_inputs_.push_back(msi))) {
            LOG_WARN("push back merge sort input fail", K(idx), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPxMSReceiveOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxReceiveOp::inner_open())) {
    LOG_WARN("initialize operator context failed", K(ret));
  }
  return ret;
}

int ObPxMSReceiveOp::release_merge_inputs()
{
  int ret = OB_SUCCESS;
  int release_merge_sort_ret = OB_SUCCESS;
  while (0 < merge_inputs_.count()) {
    MergeSortInput* msi = NULL;
    if (OB_SUCCESS != (release_merge_sort_ret = merge_inputs_.pop_back(msi))) {
      ret = release_merge_sort_ret;
      LOG_WARN("pop back merge sort input failed", K(release_merge_sort_ret));
    } else {
      msi->clean_row_store(ctx_);
      msi->destroy();
      msi->~MergeSortInput();
      ctx_.get_allocator().free(msi);
    }
  }
  return ret;
}

int ObPxMSReceiveOp::inner_close()
{
  int ret = OB_SUCCESS;
  int release_channel_ret = ObPxChannelUtil::flush_rows(task_channels_);
  if (release_channel_ret != common::OB_SUCCESS) {
    LOG_WARN("release dtl channel failed", K(release_channel_ret));
  }

  release_channel_ret = msg_loop_.unregister_all_channel();
  if (release_channel_ret != common::OB_SUCCESS) {
    // the following unlink actions is not safe is any unregister failure happened
    LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
  }

  release_channel_ret = ObPxChannelUtil::unlink_ch_set(get_ch_set(), &dfc_);
  if (release_channel_ret != common::OB_SUCCESS) {
    LOG_WARN("release dtl channel failed", K(release_channel_ret));
  }

  int release_merge_sort_ret = OB_SUCCESS;
  release_merge_sort_ret = release_merge_inputs();
  if (release_merge_sort_ret != common::OB_SUCCESS) {
    LOG_WARN("release dtl channel failed", K(release_merge_sort_ret));
  }
  return ret;
}

// Local Order Function
int ObPxMSReceiveOp::LocalOrderInput::open()
{
  return reader_.init(get_row_store_, ObChunkDatumStore::BLOCK_SIZE);
}

int ObPxMSReceiveOp::LocalOrderInput::add_row(ObExecContext& ctx, const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(exprs);
  UNUSED(eval_ctx);
  if (OB_ISNULL(add_row_store_) || OB_ISNULL(get_row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row store is not init", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("all data are added", K(ret));
  }
  return ret;
}

int ObPxMSReceiveOp::LocalOrderInput::get_row(ObPxMSReceiveOp* ms_receive_op, ObPhysicalPlanCtx* phy_plan_ctx,
    int64_t channel_idx, const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx,
    const ObChunkDatumStore::StoredRow*& store_row)
{
  int ret = OB_SUCCESS;
  UNUSED(ms_receive_op);
  UNUSED(phy_plan_ctx);
  UNUSED(channel_idx);
  UNUSED(exprs);
  UNUSED(eval_ctx);
  if (OB_FAIL(reader_.get_next_row(store_row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail get row", K(ret));
    } else {
      LOG_TRACE("finish to fetch all data from one input", K(channel_idx), K(datum_store_.get_row_cnt()), K(ret));
    }
  }
  return ret;
}

int64_t ObPxMSReceiveOp::LocalOrderInput::max_pos()
{
  return datum_store_.get_row_cnt();
}

void ObPxMSReceiveOp::LocalOrderInput::clean_row_store(ObExecContext& ctx)
{
  UNUSED(ctx);
  reader_.reset();
  datum_store_.reset();
  get_row_store_ = nullptr;
  add_row_store_ = nullptr;
}

void ObPxMSReceiveOp::LocalOrderInput::destroy()
{
  if (nullptr != get_row_store_ || nullptr != add_row_store_) {
    LOG_ERROR("unexpected status: row store is not null", K(get_row_store_), K(add_row_store_));
  }
  get_row_store_ = nullptr;
  add_row_store_ = nullptr;
}
// end Local Order function

// Global Order Function
int ObPxMSReceiveOp::GlobalOrderInput::reset_add_row_store(bool& reset)
{
  int ret = OB_SUCCESS;
  reset = false;
  if (OB_ISNULL(add_row_store_) || add_row_store_ == get_row_store_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get row store, all row store is empty", K(ret));
  } else if (nullptr != add_row_reader_ && !add_row_reader_->has_next() &&
             MAX_ROWS_PER_STORE < add_row_store_->get_row_cnt()) {
    reset = true;
  }
  return ret;
}

bool ObPxMSReceiveOp::GlobalOrderInput::is_empty()
{
  bool is_empty = false;
  if (OB_ISNULL(get_row_store_)) {
    is_empty = true;
  } else if (!get_row_reader_->has_next()) {
    is_empty = true;
  }
  if (is_empty) {
    is_empty = false;
    if (OB_ISNULL(add_row_store_)) {
      is_empty = true;
    } else if (!add_row_reader_->has_next()) {
      is_empty = true;
    }
  }
  return is_empty;
}

int ObPxMSReceiveOp::GlobalOrderInput::switch_get_row_store()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_row_store_)) {
    get_row_store_ = add_row_store_;
    if (OB_FAIL(get_reader_.init(get_row_store_, ObChunkDatumStore::BLOCK_SIZE))) {
      LOG_WARN("failed to init chunk store iterator", K(ret));
    } else {
      get_row_reader_ = &get_reader_;
    }
  }
  if (add_row_store_ == get_row_store_) {
    add_row_reader_->reset();
    add_row_reader_ = nullptr;
    add_row_store_ = nullptr;
  }
  if (OB_SUCC(ret) && !get_row_reader_->has_next()) {
    if (OB_ISNULL(add_row_store_) || !add_row_reader_->has_next()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get row store, all row store is empty", K(ret));
    } else {
      // switch row store that has data
      ObChunkDatumStore::Iterator* tmp_reader = get_row_reader_;
      get_row_reader_ = add_row_reader_;
      add_row_reader_ = tmp_reader;

      ObChunkDatumStore* tmp_store = get_row_store_;
      get_row_store_ = add_row_store_;
      add_row_store_ = tmp_store;

      if (OB_FAIL(get_row_store_->update_iterator(*get_row_reader_))) {
        LOG_WARN("failed to update iterator", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && (add_row_store_ == get_row_store_ || get_row_reader_ == add_row_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get and add row store are same", K(ret));
  }
  return ret;
}

int ObPxMSReceiveOp::GlobalOrderInput::get_one_row_from_channels(ObPxMSReceiveOp* ms_receive_op,
    ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx, const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx)
{
  int ret = OB_SUCCESS;
  int64_t hint_channel_idx = channel_idx;
  int64_t got_channel_idx = OB_INVALID_INDEX_INT64;
  bool fetched = false;
  while (OB_SUCC(ret) && !fetched && !is_finish()) {
    ms_receive_op->clear_evaluated_flag();
    int64_t timeout_us = phy_plan_ctx->get_timeout_timestamp() - ms_receive_op->get_timestamp();
    got_channel_idx = hint_channel_idx;
    if (OB_FAIL(ms_receive_op->ptr_row_msg_loop_->process_one(got_channel_idx, timeout_us))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        if (phy_plan_ctx->get_timeout_timestamp() < ObTimeUtility::current_time()) {
          ret = OB_TIMEOUT;
          LOG_WARN("get row timeout", K(channel_idx), K(ret));
        }
      } else {
        LOG_WARN("failed to process", K(channel_idx), K(got_channel_idx), K(ret));
      }
    } else {
      ret = ms_receive_op->ptr_px_row_->get_next_row(
          (static_cast<const ObPxReceiveSpec*>(&ms_receive_op->get_spec()))->child_exprs_, eval_ctx);
      if (OB_SUCCESS == ret) {
        MergeSortInput* tmp_msi = ms_receive_op->merge_inputs_.at(got_channel_idx);
        if (OB_FAIL(tmp_msi->add_row(ms_receive_op->get_exec_ctx(), exprs, eval_ctx))) {
          LOG_WARN("fail to add row", K(got_channel_idx), K(ret));
        } else {
          LOG_DEBUG("receive row",
              K(got_channel_idx),
              K(tmp_msi->max_pos()),
              K(ret),
              K(ObToStringExprRow(ms_receive_op->eval_ctx_,
                  (static_cast<const ObPxReceiveSpec*>(&ms_receive_op->get_spec()))->child_exprs_)));
          fetched = (channel_idx == got_channel_idx);
        }
      } else if (OB_ITER_END == ret) {
        if (0 > got_channel_idx || got_channel_idx >= ms_receive_op->get_channel_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail get data from wrong channel idx", K(got_channel_idx), K(ret));
        } else {
          MergeSortInput* tmp_msi = ms_receive_op->merge_inputs_.at(got_channel_idx);
          tmp_msi->set_finish(true);
          // All data is fetched for one input
          LOG_TRACE("channel finish get data", K(got_channel_idx), K(tmp_msi->max_pos()), K(ret), K(finish_));
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("fail get row from row store", K(ret));
      }
    }
    hint_channel_idx = OB_INVALID_INDEX_INT64;
  }
  if (OB_SUCC(ret)) {
    if (got_channel_idx != ms_receive_op->row_heap_.writable_channel_idx() && !is_finish()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel idx is not same as writable channel idx", K(got_channel_idx), K(channel_idx), K(ret));
    }
  }
  return ret;
}

int ObPxMSReceiveOp::GlobalOrderInput::get_row(ObPxMSReceiveOp* ms_receive_op, ObPhysicalPlanCtx* phy_plan_ctx,
    int64_t channel_idx, const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx,
    const ObChunkDatumStore::StoredRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    if (OB_FAIL(get_one_row_from_channels(ms_receive_op, phy_plan_ctx, channel_idx, exprs, eval_ctx))) {
      LOG_WARN("fail to get one row in global order", K(ret));
    } else if (nullptr != get_row_store_ && OB_FAIL(get_row_store_->update_iterator(*get_row_reader_))) {
      LOG_WARN("failed to update iterator", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (is_empty()) {
      ret = OB_ITER_END;
      LOG_TRACE("finish to fetch all data from one input", K(ret));
      if (!finish_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fetch last row but merge input isn't finish", K(ret));
      } else {
        reader_.reset();
        get_reader_.reset();
        if (nullptr != add_row_store_) {
          add_row_store_->reset();
        }
        if (nullptr != get_row_store_) {
          get_row_store_->reset();
        }
      }
    } else if (OB_FAIL(switch_get_row_store())) {
      LOG_WARN("fail to switch get row store", K(ret));
    } else if (OB_FAIL(get_row_reader_->get_next_row(store_row))) {
      LOG_WARN("fail to get row", K(ret));
    }
  }
  return ret;
}

int64_t ObPxMSReceiveOp::GlobalOrderInput::max_pos()
{
  int64_t rn = 0;
  if (nullptr == get_row_store_) {
    rn = 0;
  } else {
    rn = get_row_store_->get_row_cnt();
  }
  return rn;
}

void ObPxMSReceiveOp::GlobalOrderInput::clean_row_store(ObExecContext& ctx)
{
  reader_.reset();
  get_reader_.reset();
  add_row_reader_ = nullptr;
  get_row_reader_ = nullptr;
  if (nullptr != add_row_store_) {
    if (add_row_store_ == get_row_store_) {
      get_row_store_ = nullptr;
    }
    add_row_store_->reset();
    add_row_store_->~ObChunkDatumStore();
    ctx.get_allocator().free(add_row_store_);
    add_row_store_ = nullptr;
  }
  if (nullptr != get_row_store_) {
    get_row_store_->reset();
    get_row_store_->~ObChunkDatumStore();
    ctx.get_allocator().free(get_row_store_);
    get_row_store_ = nullptr;
  }
}

void ObPxMSReceiveOp::GlobalOrderInput::destroy()
{
  if (nullptr != add_row_store_ || nullptr != get_row_store_) {
    LOG_ERROR("unexpect status: row store is not null", K(add_row_store_), K(get_row_store_));
  }
  get_row_store_ = nullptr;
  add_row_store_ = nullptr;
}

int ObPxMSReceiveOp::GlobalOrderInput::create_chunk_datum_store(
    ObExecContext& ctx, uint64_t tenant_id, ObChunkDatumStore*& row_store)
{
  int ret = OB_SUCCESS;
  void* buf = ctx.get_allocator().alloc(sizeof(ObChunkDatumStore));
  row_store = nullptr;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ra row store fail", K(ret));
  } else {
    row_store = new (buf) ObChunkDatumStore();
    int64_t mem_limit = 0;
    if (OB_FAIL(row_store->init(mem_limit, tenant_id, ObCtxIds::WORK_AREA, ObModIds::OB_SQL_CHUNK_ROW_STORE, false))) {
      ctx.get_allocator().free(buf);
      row_store = nullptr;
      LOG_WARN("row store init fail", K(ret));
    }
  }
  return ret;
}

int ObPxMSReceiveOp::GlobalOrderInput::add_row(ObExecContext& ctx, const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(add_row_store_)) {
    ObChunkDatumStore* row_store = nullptr;
    if (OB_FAIL(create_chunk_datum_store(ctx, tenant_id_, row_store))) {
      LOG_WARN("failed to create row store", K(ret));
    } else {
      add_row_store_ = row_store;
      if (OB_FAIL(reader_.init(add_row_store_, ObChunkDatumStore::BLOCK_SIZE))) {
        LOG_WARN("failed to init chunk store iterator", K(ret));
      } else {
        add_row_reader_ = &reader_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(add_row_store_) || add_row_store_ == get_row_store_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add row store is null or is same as get row store", K(ret));
    } else {
      bool reset = false;
      if (OB_FAIL(reset_add_row_store(reset))) {
        LOG_WARN("fail to switch add row store", K(ret));
      } else if (reset) {
        LOG_TRACE("reset add row store", K(add_row_reader_), K(*add_row_store_));
        int64_t mem_limit = 0;
        add_row_reader_->reset();
        add_row_store_->reset();
        if (OB_FAIL(add_row_store_->init(
                mem_limit, tenant_id_, ObCtxIds::WORK_AREA, ObModIds::OB_SQL_CHUNK_ROW_STORE, false))) {
        } else if (OB_FAIL(add_row_reader_->init(add_row_store_, ObChunkDatumStore::BLOCK_SIZE))) {
          LOG_WARN("failed to init chunk store iterator", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_row_store_->add_row(exprs, &eval_ctx))) {
    LOG_WARN("fail to add row", K(ret));
  }
  return ret;
}
// end Global Order input

int ObPxMSReceiveOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Get operator context failed", K(ret), K(MY_SPEC.id_));
  } else if (OB_FAIL(try_link_channel())) {
    LOG_WARN("failed to init channel", K(ret));
  }

  if (OB_SUCC(ret) && MY_SPEC.local_order_ && !finish_) {
    ret = get_all_rows_from_channels(phy_plan_ctx);
  }
  if (OB_SUCC(ret)) {
    const ObChunkDatumStore::StoredRow* store_row = nullptr;
    while (OB_SUCC(ret) && row_heap_.capacity() > row_heap_.count()) {
      clear_evaluated_flag();
      if (OB_FAIL(get_one_row_from_channels(
              phy_plan_ctx, row_heap_.writable_channel_idx(), MY_SPEC.all_exprs_, eval_ctx_, store_row))) {
        if (OB_ITER_END == ret) {
          row_heap_.shrink();
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(store_row)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(row_heap_.push(store_row))) {
        LOG_WARN("fail push row to heap", K(ret));
      } else { /* nothing */
      }
    }

    if (OB_SUCC(ret)) {
      if (0 == row_heap_.capacity()) {
        ret = OB_ITER_END;
        iter_end_ = true;
        metric_.mark_last_out();
        int release_ret = OB_SUCCESS;
        if (OB_SUCCESS != (release_ret = release_merge_inputs())) {
          LOG_WARN("failed to release merge sort and row store", K(ret), K(release_ret));
        }
      } else if (row_heap_.capacity() == row_heap_.count()) {
        if (OB_FAIL(row_heap_.pop(store_row))) {
          LOG_WARN("fail pop row from heap", K(ret));
        } else if (OB_FAIL(store_row->to_expr(MY_SPEC.all_exprs_, eval_ctx_))) {
          LOG_WARN("failed to convert store row", K(ret));
        } else {
          LOG_TRACE("trace output row", K(ret), K(ObToStringExprRow(eval_ctx_, MY_SPEC.all_exprs_)));
        }
        metric_.count();
        metric_.mark_first_out();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid row heap state", K(row_heap_), K(ret));
      }
    }
  }
  return ret;
}

int ObPxMSReceiveOp::get_one_row_from_channels(ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx,
    const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx,
    const ObChunkDatumStore::StoredRow*& store_row)  // row heap require data from the channel_idx channel
{
  int ret = OB_SUCCESS;
  if (0 > channel_idx || channel_idx > merge_inputs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid channel idx", K(channel_idx), K(ret));
  } else {
    MergeSortInput* msi = merge_inputs_.at(channel_idx);
    if (OB_ISNULL(msi)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge sort input is null", K(ret));
    } else if (OB_FAIL(msi->get_row(this, phy_plan_ctx, channel_idx, exprs, eval_ctx, store_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get row from merge sort input", K(ret));
      }
    } else { /* nothing */
    }
  }
  return ret;
}

int ObPxMSReceiveOp::new_local_order_input(MergeSortInput*& out_msi)
{
  int ret = OB_SUCCESS;
  void* buf = ctx_.get_allocator().alloc(sizeof(LocalOrderInput));
  out_msi = nullptr;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ra row store fail", K(ret));
  } else {
    LocalOrderInput* local_input = static_cast<LocalOrderInput*>(new (buf) LocalOrderInput());
    local_input = static_cast<LocalOrderInput*>(new (buf) LocalOrderInput());
    if (OB_FAIL(local_input->datum_store_.init(0,
            ctx_.get_my_session()->get_effective_tenant_id(),
            ObCtxIds::WORK_AREA,
            ObModIds::OB_SQL_CHUNK_ROW_STORE,
            false))) {
      LOG_WARN("failed to init chunk store", K(ret));
    } else if (OB_FAIL(merge_inputs_.push_back(local_input))) {
      LOG_WARN("fail push back MergeSortInput", K(ret));
    } else {
      out_msi = local_input;
    }
  }
  return ret;
}

int ObPxMSReceiveOp::get_all_rows_from_channels(ObPhysicalPlanCtx* phy_plan_ctx)
{
  int ret = OB_SUCCESS;
  if (!finish_) {
    int64_t n_channel = get_channel_count();
    int64_t channel_idx = 0;
    common::ObArray<ObChunkDatumStore::StoredRow*> last_store_row_array;
    common::ObArray<ObChunkDatumStore*> chunk_store_array;
    if (OB_FAIL(last_store_row_array.prepare_allocate(n_channel)) ||
        OB_FAIL(chunk_store_array.prepare_allocate(n_channel))) {
      LOG_WARN("fail to prepare allocate array", K(ret));
    } else {
      int n_finish = 0;
      Compare cmp_fun;
      ObChunkDatumStore* cur_chunk_store = nullptr;
      ObChunkDatumStore::StoredRow* last_store_row = nullptr;
      if (OB_FAIL(cmp_fun.init(&MY_SPEC.sort_collations_, &MY_SPEC.sort_cmp_funs_))) {
        LOG_WARN("failed to init cmp function", K(ret));
      }
      while (OB_SUCC(ret) && !finish_) {
        clear_evaluated_flag();
        int64_t timeout_us = phy_plan_ctx->get_timeout_timestamp() - get_timestamp();
        channel_idx %= n_channel;
        int64_t got_channel_idx = OB_INVALID_INDEX_INT64;
        if (OB_FAIL(ptr_row_msg_loop_->process_one(got_channel_idx, timeout_us))) {
          if (OB_EAGAIN == ret) {
            // If no data fetch, then return OB_EAGAIN after OB_ITER_END
            ret = OB_SUCCESS;
            if (phy_plan_ctx->get_timeout_timestamp() < ObTimeUtility::current_time()) {
              ret = OB_TIMEOUT;
            }
          }
        } else {
          ret = ptr_px_row_->get_next_row(MY_SPEC.child_exprs_, eval_ctx_);
          if (OB_SUCCESS == ret) {
            if (0 > got_channel_idx) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid channel idx", K(got_channel_idx), K(ret));
            } else {
              cur_chunk_store = chunk_store_array.at(got_channel_idx);
              last_store_row = last_store_row_array.at(got_channel_idx);
              if (nullptr == last_store_row) {
                MergeSortInput* new_msi = nullptr;
                // first row
                if (nullptr != cur_chunk_store) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected status: it's first row", K(ret));
                } else if (OB_FAIL(new_local_order_input(new_msi))) {
                  LOG_WARN("failed to create new local order input", K(ret));
                } else {
                  LocalOrderInput* local_msi = static_cast<LocalOrderInput*>(new_msi);
                  cur_chunk_store = &local_msi->datum_store_;
                  chunk_store_array.at(got_channel_idx) = cur_chunk_store;
                  if (OB_FAIL(cur_chunk_store->add_row(MY_SPEC.all_exprs_, &eval_ctx_, &last_store_row))) {
                    LOG_WARN("fail to add row to row store", K(got_channel_idx), K(ret));
                  } else {
                    last_store_row_array.at(got_channel_idx) = last_store_row;
                    LOG_DEBUG("get new row and new group",
                        K(ret),
                        K(got_channel_idx),
                        K(ObToStringExprRow(eval_ctx_, MY_SPEC.all_exprs_)));
                  }
                }
              } else if (nullptr == cur_chunk_store) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected status: cur chunk store is null", K(ret));
              } else {
                bool is_new_group = cmp_fun(last_store_row, &MY_SPEC.all_exprs_, eval_ctx_);
                if (OB_FAIL(cmp_fun.ret_)) {
                  LOG_WARN("fail split new group", K(got_channel_idx), K(ret));
                } else if (is_new_group) {
                  MergeSortInput* new_msi = nullptr;
                  if (merge_inputs_.count() > MAX_INPUT_NUMBER) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("too much local order inputs", K(ret));
                  } else if (OB_FAIL(new_local_order_input(new_msi))) {
                    LOG_WARN("failed to create new local order input", K(ret));
                  } else {
                    LocalOrderInput* local_msi = static_cast<LocalOrderInput*>(new_msi);
                    cur_chunk_store = &local_msi->datum_store_;
                    chunk_store_array.at(got_channel_idx) = cur_chunk_store;
                    if (OB_FAIL(cur_chunk_store->add_row(MY_SPEC.all_exprs_, &eval_ctx_, &last_store_row))) {
                      LOG_WARN("fail to add row to row store", K(got_channel_idx), K(ret));
                    } else {
                      last_store_row_array.at(got_channel_idx) = last_store_row;
                      LOG_DEBUG("get new row and new group",
                          K(ret),
                          K(got_channel_idx),
                          K(ObToStringExprRow(eval_ctx_, MY_SPEC.all_exprs_)));
                    }
                  }
                } else if (OB_FAIL(cur_chunk_store->add_row(MY_SPEC.all_exprs_, &eval_ctx_, &last_store_row))) {
                  LOG_WARN("fail to add row to row store", K(got_channel_idx), K(ret));
                } else {
                  last_store_row_array.at(got_channel_idx) = last_store_row;
                  LOG_DEBUG("get new row",
                      K(ret),
                      K(got_channel_idx),
                      K(ObToStringExprRow(eval_ctx_, MY_SPEC.all_exprs_)),
                      K(is_new_group));
                }
              }
            }
          } else if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            n_finish++;
            if (n_finish == n_channel) {
              finish_ = true;
            }
          } else {
            LOG_WARN("fail get row from row store", K(ret));
          }
        }
        channel_idx++;
      }
    }

    // build row heap
    if (OB_SUCC(ret)) {
      if (0 >= merge_inputs_.count()) {
      } else if (OB_FAIL(row_heap_.init(merge_inputs_.count(), &MY_SPEC.sort_collations_, &MY_SPEC.sort_cmp_funs_))) {
        LOG_WARN("fail to init row heap", K(ret));
      } else {
        for (int64_t i = 0; i < merge_inputs_.count() && OB_SUCC(ret); ++i) {
          LocalOrderInput* local_order_input = static_cast<LocalOrderInput*>(merge_inputs_.at(i));
          if (OB_FAIL(local_order_input->open())) {
            LOG_WARN("failed to open local order input", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
// end get all row in local order

int ObPxMSReceiveOp::try_link_channel()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Get operator context failed", K(ret), K(MY_SPEC.id_));
  } else if (!channel_linked()) {
    ObPxReceiveOpInput* recv_input = reinterpret_cast<ObPxReceiveOpInput*>(input_);
    ret = init_channel(*recv_input, task_ch_set_, task_channels_, msg_loop_, px_row_msg_proc_, interrupt_proc_);
    metric_.set_id(MY_SPEC.id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("Fail to init channel", K(ret));
    } else if (!MY_SPEC.local_order_ &&
               OB_FAIL(row_heap_.init(get_channel_count(), &MY_SPEC.sort_collations_, &MY_SPEC.sort_cmp_funs_))) {
      LOG_WARN("Row heap init failed", "count", get_channel_count(), K(ret));
    } else if (OB_FAIL(init_merge_sort_input(get_channel_count()))) {
      LOG_WARN("Merge sort input init failed", K(ret));
    }
  }
  return ret;
}

ObPxMSReceiveOp::Compare::Compare()
    : ret_(OB_SUCCESS), sort_collations_(nullptr), sort_cmp_funs_(nullptr), rows_(nullptr)
{}

int ObPxMSReceiveOp::Compare::init(
    const ObIArray<ObSortFieldCollation>* sort_collations, const ObIArray<ObSortCmpFunc>* sort_cmp_funs)
{
  int ret = OB_SUCCESS;
  bool is_static_cmp = false;
  if (nullptr == sort_collations || nullptr == sort_cmp_funs) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sort_collations), KP(sort_cmp_funs));
  } else if (sort_cmp_funs->count() != sort_cmp_funs->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count miss match", K(ret), K(sort_cmp_funs->count()), K(sort_cmp_funs->count()));
  } else {
    sort_collations_ = sort_collations;
    sort_cmp_funs_ = sort_cmp_funs;
  }
  return ret;
}

bool ObPxMSReceiveOp::Compare::operator()(
    const ObChunkDatumStore::StoredRow* l, const common::ObIArray<ObExpr*>* r, ObEvalCtx& eval_ctx)
{
  bool less = false;
  int& ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(l), KP(r));
  } else {
    const ObDatum* lcells = l->cells();
    ObDatum* other_datum = nullptr;
    int cmp = 0;
    for (int64_t i = 0; 0 == cmp && i < sort_cmp_funs_->count() && OB_SUCC(ret); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      if (OB_FAIL(r->at(idx)->eval(eval_ctx, other_datum))) {
        LOG_WARN("failed to eval expr", K(ret));
      } else {
        cmp = sort_cmp_funs_->at(i).cmp_func_(lcells[idx], *other_datum);
        if (cmp < 0) {
          less = !sort_collations_->at(i).is_ascending_;
        } else if (cmp > 0) {
          less = sort_collations_->at(i).is_ascending_;
        }
      }
    }
  }
  return less;
}

}  // end namespace sql
}  // end namespace oceanbase
