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

#include "sql/engine/px/exchange/ob_px_merge_sort_receive.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/engine/px/ob_px_scheduler.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER((ObPxMergeSortReceiveInput, ObPxReceiveInput));
OB_SERIALIZE_MEMBER((ObPxMergeSortReceive, ObPxReceive), sort_columns_, local_order_);

int ObPxMergeSortReceive::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMergeSortReceiveCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObPxMergeSortReceiveCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("fail create op ctx", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {  // true: alloc cell's memory
    LOG_WARN("fail init px merge sort receive cur row", K(ret));
  }
  return ret;
}

int ObPxMergeSortReceive::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObPxMergeSortReceiveInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  }
  UNUSED(input);
  return ret;
}

int ObPxMergeSortReceive::ObPxMergeSortReceiveCtx::create_ra_row_store(uint64_t tenant_id, ObRARowStore*& row_store)
{
  int ret = OB_SUCCESS;
  void* buf = exec_ctx_.get_allocator().alloc(sizeof(ObRARowStore));
  row_store = nullptr;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ra row store fail", K(ret));
  } else {
    row_store = new (buf) ObRARowStore(nullptr, true /* keep projector */);
    int64_t mem_limit = 0;
    if (OB_FAIL(row_store->init(mem_limit, tenant_id, ObCtxIds::WORK_AREA))) {
      exec_ctx_.get_allocator().free(buf);
      row_store = nullptr;
      LOG_WARN("row store init fail", K(ret));
    }
  }
  return ret;
}

// init input information in global order and local order
int ObPxMergeSortReceive::init_merge_sort_input(
    ObExecContext& ctx, ObPxMergeSortReceiveCtx* recv_ctx, int64_t n_channel) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(recv_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the phy operator ctx is null", K(ret));
  } else {
    if (local_order_) {
      // local order, init row store
      if (0 >= n_channel) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("channels are not init", K(ret));
      } else {
        for (int64_t idx = 0; OB_SUCC(ret) && idx < n_channel; ++idx) {
          ObRARowStore* row_store = nullptr;
          if (OB_FAIL(recv_ctx->create_ra_row_store(ctx.get_my_session()->get_effective_tenant_id(), row_store))) {
            LOG_WARN("failed to create row store", K(idx), K(ret));
          } else if (OB_FAIL(recv_ctx->row_stores_.push_back(row_store))) {
            LOG_WARN("push back ra row store fail", K(idx), K(ret));
          }
        }
      }
    } else {
      // global order, init merge sort input
      if (0 >= n_channel) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("channels are not init", K(ret));
      } else {
        for (int64_t idx = 0; OB_SUCC(ret) && idx < n_channel; ++idx) {
          void* buf = ctx.get_allocator().alloc(sizeof(GlobalOrderInput));
          if (nullptr == buf) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("create merge sort input fail", K(idx), K(ret));
          } else {
            MergeSortInput* msi = new (buf) GlobalOrderInput(ctx.get_my_session()->get_effective_tenant_id());
            if (OB_FAIL(recv_ctx->merge_inputs_.push_back(msi))) {
              LOG_WARN("push back merge sort input fail", K(idx), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPxMergeSortReceive::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;

  LOG_TRACE("Inner open px merge sort receive", "op_id", get_id());

  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_FAIL(ObPxReceive::inner_open(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  }

  LOG_TRACE("Inner open px merge sort receive ok", "op_id", get_id());

  return ret;
}

// release row store data in advance
int ObPxMergeSortReceive::release_merge_inputs(ObExecContext& ctx, ObPxMergeSortReceiveCtx* recv_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(recv_ctx)) {
    int release_merge_sort_ret = OB_SUCCESS;
    while (0 < recv_ctx->merge_inputs_.count()) {
      MergeSortInput* msi = NULL;
      if (OB_SUCCESS != (release_merge_sort_ret = recv_ctx->merge_inputs_.pop_back(msi))) {
        ret = release_merge_sort_ret;
        LOG_WARN("pop back merge sort input failed", K(release_merge_sort_ret));
      } else {
        msi->clean_row_store(*recv_ctx);
        msi->destroy();
        msi->~MergeSortInput();
        ctx.get_allocator().free(msi);
      }
    }
    while (OB_SUCC(ret) && 0 < recv_ctx->row_stores_.count()) {
      ObRARowStore* rs = NULL;
      if (OB_SUCCESS != (release_merge_sort_ret = recv_ctx->row_stores_.pop_back(rs))) {
        ret = release_merge_sort_ret;
        LOG_WARN("pop back row store failed", K(release_merge_sort_ret));
      } else {
        rs->reset();
        rs->~ObRARowStore();
        ctx.get_allocator().free(rs);
      }
    }
  }
  return ret;
}

int ObPxMergeSortReceive::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMergeSortReceiveCtx* recv_ctx = NULL;

  if (OB_ISNULL(recv_ctx = GET_PHY_OPERATOR_CTX(ObPxMergeSortReceiveCtx, ctx, get_id()))) {
    LOG_DEBUG("get_phy_operator_ctx failed", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  }

  /* we must release channel even if there is some error happen before */
  if (OB_NOT_NULL(recv_ctx)) {
    int release_channel_ret = ObPxChannelUtil::flush_rows(recv_ctx->task_channels_);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }

    dtl::ObDtlChannelLoop& loop = recv_ctx->msg_loop_;
    release_channel_ret = loop.unregister_all_channel();
    if (release_channel_ret != common::OB_SUCCESS) {
      // the following unlink actions is not safe is any unregister failure happened
      LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
    }

    release_channel_ret = ObPxChannelUtil::unlink_ch_set(recv_ctx->get_ch_set(), &recv_ctx->dfc_);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }

    int release_merge_sort_ret = OB_SUCCESS;
    release_merge_sort_ret = release_merge_inputs(ctx, recv_ctx);
    if (release_merge_sort_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_merge_sort_ret));
    }
  }
  return ret;
}

// Local Order Function
int ObPxMergeSortReceive::LocalOrderInput::add_row(ObPxMergeSortReceiveCtx& recv_ctx, const common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  UNUSED(recv_ctx);
  if (OB_ISNULL(add_row_store_) || OB_ISNULL(get_row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row store is not init", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("all data are added", K(ret));
  }
  return ret;
}

int ObPxMergeSortReceive::LocalOrderInput::get_row(ObPxMergeSortReceiveCtx* recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx,
    int64_t channel_idx, const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  UNUSED(recv_ctx);
  UNUSED(phy_plan_ctx);
  UNUSED(channel_idx);
  if (pos_ < 0 || pos_ > max_pos()) {
    // last row should be max_pos
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid row pos", K(pos_), K(ret));
  } else if (OB_ISNULL(get_row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row store is null", K(pos_), K(ret));
  } else {
    if (pos_ >= max_pos()) {
      // fetch all data already
      ret = OB_ITER_END;
      LOG_TRACE(
          "finish to fetch all data from one input", K(pos_), K(channel_idx), K(org_start_pos_), K(max_pos()), K(ret));
    } else if (OB_FAIL(reader_.get_row(pos_, row))) {
      LOG_WARN("fail get row", K(pos_), K(ret));
    } else {
      pos_++;
    }
  }
  return ret;
}

int64_t ObPxMergeSortReceive::LocalOrderInput::max_pos()
{
  return end_count_;
}

void ObPxMergeSortReceive::LocalOrderInput::clean_row_store(ObPxMergeSortReceiveCtx& recv_ctx)
{
  UNUSED(recv_ctx);
  reader_.reset();
  if (can_free_ && nullptr != get_row_store_) {
    get_row_store_->reset();
    get_row_store_ = nullptr;
  }
  get_row_store_ = nullptr;
  add_row_store_ = nullptr;
}

void ObPxMergeSortReceive::LocalOrderInput::destroy()
{
  if (nullptr != get_row_store_ || nullptr != add_row_store_) {
    LOG_ERROR("unexpected status: row store is not null", K(get_row_store_), K(add_row_store_));
  }
  get_row_store_ = nullptr;
  add_row_store_ = nullptr;
}
// end Local Order function

// Global Order Function
int ObPxMergeSortReceive::GlobalOrderInput::reset_add_row_store(bool& reset)
{
  int ret = OB_SUCCESS;
  reset = false;
  if (OB_ISNULL(add_row_store_) || add_row_store_ == get_row_store_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get row store, all row store is empty", K(ret));
  } else if (add_saved_pos_ > add_row_store_->get_row_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the pos is greater than the count of add row store",
        K(add_saved_pos_),
        K(add_row_store_->get_row_cnt()),
        K(ret));
  } else if (add_saved_pos_ == add_row_store_->get_row_cnt() && MAX_ROWS_PER_STORE < add_saved_pos_) {
    reset = true;
  }
  return ret;
}

bool ObPxMergeSortReceive::GlobalOrderInput::is_empty()
{
  bool is_empty = false;
  if (OB_ISNULL(get_row_store_)) {
    is_empty = true;
  } else if (get_pos_ >= get_row_store_->get_row_cnt()) {
    is_empty = true;
  }
  if (is_empty) {
    is_empty = false;
    if (OB_ISNULL(add_row_store_)) {
      is_empty = true;
    } else if (add_saved_pos_ >= add_row_store_->get_row_cnt()) {
      is_empty = true;
    }
  }
  return is_empty;
}

int ObPxMergeSortReceive::GlobalOrderInput::switch_get_row_store()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_row_store_)) {
    get_row_store_ = add_row_store_;
    get_pos_ = 0;
  }
  if (add_row_store_ == get_row_store_) {
    add_row_store_ = nullptr;
    add_saved_pos_ = 0;
  }
  if (get_pos_ >= get_row_store_->get_row_cnt()) {
    if (OB_ISNULL(add_row_store_) || add_saved_pos_ >= add_row_store_->get_row_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get row store, all row store is empty", K(ret));
    } else {
      // switch row store that has data
      int64_t tmp_pos = get_pos_;
      get_pos_ = add_saved_pos_;
      add_saved_pos_ = tmp_pos;

      ObRARowStore* tmp_store = get_row_store_;
      get_row_store_ = add_row_store_;
      add_row_store_ = tmp_store;
    }
  }
  if (OB_SUCC(ret) && add_row_store_ == get_row_store_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get and add row store are same", K(ret));
  }
  return ret;
}

int ObPxMergeSortReceive::GlobalOrderInput::get_one_row_from_channels(
    ObPxMergeSortReceiveCtx& recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx) const
{
  int ret = OB_SUCCESS;
  int64_t hint_channel_idx = channel_idx;
  int64_t got_channel_idx = OB_INVALID_INDEX_INT64;
  bool fetched = false;
  while (OB_SUCC(ret) && !fetched && !is_finish()) {
    int64_t timeout_us = phy_plan_ctx->get_timeout_timestamp() - recv_ctx.get_timestamp();
    got_channel_idx = hint_channel_idx;
    if (OB_FAIL(recv_ctx.get_dtl_channel_loop()->process_one(got_channel_idx, timeout_us))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        if (phy_plan_ctx->get_timeout_timestamp() < ObTimeUtility::current_time()) {
          ret = OB_TIMEOUT;
          LOG_WARN("get row timeout", K(channel_idx), K(ret));
        }
      }
    } else {
      const ObNewRow* row = NULL;
      ret = recv_ctx.ptr_px_row_->get_row(recv_ctx.get_cur_row());
      if (OB_SUCCESS == ret) {
        row = &recv_ctx.get_cur_row();
        if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row is null", K(got_channel_idx), K(ret));
        } else {
          MergeSortInput* tmp_msi = recv_ctx.merge_inputs_.at(got_channel_idx);
          if (OB_FAIL(tmp_msi->add_row(recv_ctx, *row))) {
            LOG_WARN("fail to add row", K(got_channel_idx), K(ret));
          } else {
            fetched = (channel_idx == got_channel_idx);
          }
        }
      } else if (OB_ITER_END == ret) {
        if (0 > got_channel_idx || got_channel_idx >= recv_ctx.get_channel_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail get data from wrong channel idx", K(got_channel_idx), K(ret));
        } else {
          MergeSortInput* tmp_msi = recv_ctx.merge_inputs_.at(got_channel_idx);
          tmp_msi->set_finish(true);
          // All data is fetched for one input
          LOG_TRACE("channel finish get data", K(got_channel_idx), K(tmp_msi->max_pos()), K(ret));
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("fail get row from row store", K(ret));
      }
    }
    hint_channel_idx = OB_INVALID_INDEX_INT64;
  }
  if (OB_SUCC(ret)) {
    if (got_channel_idx != recv_ctx.row_heap_.writable_channel_idx() && !is_finish()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel idx is not same as writable channel idx", K(got_channel_idx), K(channel_idx), K(ret));
    }
  }
  return ret;
}

int ObPxMergeSortReceive::GlobalOrderInput::get_row(ObPxMergeSortReceiveCtx* recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx,
    int64_t channel_idx, const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    if (OB_FAIL(get_one_row_from_channels(*recv_ctx, phy_plan_ctx, channel_idx))) {
      LOG_WARN("fail to get one row in global order", K(ret));
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
        if (nullptr != add_row_store_) {
          add_row_store_->reset();
        }
        if (nullptr != get_row_store_) {
          get_row_store_->reset();
        }
        get_pos_ = 0;
        add_saved_pos_ = 0;
      }
    } else if (OB_FAIL(switch_get_row_store())) {
      LOG_WARN("fail to switch get row store", K(ret));
    } else if (OB_FAIL(get_row_store_->get_row(get_pos_, row))) {
      LOG_WARN("fail to get row", K(get_pos_), K(ret));
    } else {
      get_pos_++;
    }
  }
  return ret;
}

int64_t ObPxMergeSortReceive::GlobalOrderInput::max_pos()
{
  int64_t rn = 0;
  if (nullptr == get_row_store_) {
    rn = 0;
  } else {
    rn = get_row_store_->get_row_cnt();
  }
  return rn;
}

void ObPxMergeSortReceive::GlobalOrderInput::clean_row_store(ObPxMergeSortReceiveCtx& recv_ctx)
{
  if (nullptr != add_row_store_) {
    if (add_row_store_ == get_row_store_) {
      get_row_store_ = nullptr;
    }
    add_row_store_->reset();
    add_row_store_->~ObRARowStore();
    recv_ctx.exec_ctx_.get_allocator().free(add_row_store_);
    add_row_store_ = nullptr;
  }
  if (nullptr != get_row_store_) {
    get_row_store_->reset();
    get_row_store_->~ObRARowStore();
    recv_ctx.exec_ctx_.get_allocator().free(get_row_store_);
    get_row_store_ = nullptr;
  }
}

void ObPxMergeSortReceive::GlobalOrderInput::destroy()
{
  if (nullptr != add_row_store_ || nullptr != get_row_store_) {
    LOG_ERROR("unexpect status: row store is not null", K(add_row_store_), K(get_row_store_));
  }
  get_row_store_ = nullptr;
  add_row_store_ = nullptr;
}

int ObPxMergeSortReceive::GlobalOrderInput::add_row(ObPxMergeSortReceiveCtx& recv_ctx, const common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(add_row_store_)) {
    ObRARowStore* row_store = nullptr;
    if (OB_FAIL(recv_ctx.create_ra_row_store(tenant_id_, row_store))) {
      LOG_WARN("failed to create row store", K(ret));
    } else {
      add_row_store_ = row_store;
      add_saved_pos_ = 0;
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
        int64_t mem_limit = 0;
        add_row_store_->reset();
        if (OB_FAIL(add_row_store_->init(mem_limit, tenant_id_, ObCtxIds::WORK_AREA))) {}
        add_saved_pos_ = 0;
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_row_store_->add_row(row))) {
    LOG_WARN("fail to add row", K(ret));
  }
  return ret;
}
// end Global Order input

int ObPxMergeSortReceive::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  // read data from channel
  ObPxMergeSortReceiveCtx* recv_ctx = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  row = NULL;
  if (OB_ISNULL(recv_ctx = GET_PHY_OPERATOR_CTX(ObPxMergeSortReceiveCtx, ctx, get_id())) ||
      OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Get operator context failed", K(ret), K(get_id()));
  } else if (OB_FAIL(try_link_channel(ctx))) {
    LOG_WARN("failed to init channel", K(ret));
  }

  if (OB_SUCC(ret) && local_order_ && !recv_ctx->finish_) {
    ret = get_all_rows_from_channels(*recv_ctx, phy_plan_ctx);
  }
  if (OB_SUCC(ret)) {
    ObRowHeap<>& row_heap = recv_ctx->row_heap_;
    // (1) adds one or many elements to heap until heap is full
    while (OB_SUCC(ret) && row_heap.capacity() > row_heap.count()) {
      const ObNewRow* in_row = NULL;
      if (OB_FAIL(get_one_row_from_channels(recv_ctx, phy_plan_ctx, row_heap.writable_channel_idx(), in_row))) {
        if (OB_ITER_END == ret) {
          row_heap.shrink();
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(in_row)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(row_heap.push(in_row))) {
        LOG_WARN("fail push row to heap", K(ret));
      } else { /* nothing */
      }
    }

    // (2) pop max val
    if (OB_SUCC(ret)) {
      if (0 == row_heap.capacity()) {
        ret = OB_ITER_END;
        recv_ctx->iter_end_ = true;
        recv_ctx->metric_.mark_last_out();
        int release_ret = OB_SUCCESS;
        if (OB_SUCCESS != (release_ret = release_merge_inputs(ctx, recv_ctx))) {
          LOG_WARN("failed to release merge sort and row store", K(ret), K(release_ret));
        }
      } else if (row_heap.capacity() == row_heap.count()) {
        if (OB_FAIL(row_heap.pop(row))) {
          LOG_WARN("fail pop row from heap", K(ret));
        }
        recv_ctx->metric_.count();
        recv_ctx->metric_.mark_first_out();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid row heap state", K(row_heap), K(ret));
      }
    }
  }

  return ret;
}

int ObPxMergeSortReceive::get_one_row_from_channels(ObPxMergeSortReceiveCtx* recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx,
    int64_t channel_idx,  // row heap require data from the channel_idx channel
    const common::ObNewRow*& in_row) const
{
  int ret = OB_SUCCESS;
  if (0 > channel_idx || channel_idx > recv_ctx->merge_inputs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid channel idx", K(channel_idx), K(ret));
  } else {
    MergeSortInput* msi = recv_ctx->merge_inputs_.at(channel_idx);
    if (OB_ISNULL(msi)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge sort input is null", K(ret));
    } else if (OB_FAIL(msi->get_row(recv_ctx, phy_plan_ctx, channel_idx, in_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get row from merge sort input", K(ret));
      }
    } else { /* nothing */
    }
  }
  return ret;
}

int ObPxMergeSortReceive::get_all_rows_from_channels(
    ObPxMergeSortReceiveCtx& recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx) const
{
  int ret = OB_SUCCESS;
  if (!recv_ctx.finish_) {
    int64_t n_channel = recv_ctx.get_channel_count();
    int64_t channel_idx = 0;
    common::ObArray<int64_t> last_end_array;
    common::ObArray<const ObNewRow*> cmp_row_arr;  // compare last_row and cur_row, so it only need 2 element
    if (OB_FAIL(last_end_array.prepare_allocate(n_channel)) || OB_FAIL(cmp_row_arr.prepare_allocate(2))) {
      LOG_WARN("fail to prepare allocate array", K(ret));
    } else {
      int n_finish = 0;
      ObRowComparer cmp_fun;
      cmp_fun.init(get_sort_columns(), cmp_row_arr);
      while (OB_SUCC(ret) && !recv_ctx.finish_) {
        int64_t timeout_us = phy_plan_ctx->get_timeout_timestamp() - recv_ctx.get_timestamp();
        channel_idx %= n_channel;
        int64_t got_channel_idx = OB_INVALID_INDEX_INT64;
        if (OB_FAIL(recv_ctx.get_dtl_channel_loop()->process_one(got_channel_idx, timeout_us))) {
          if (OB_EAGAIN == ret) {
            // If no data fetch, then return OB_EAGAIN after OB_ITER_END
            ret = OB_SUCCESS;
            if (phy_plan_ctx->get_timeout_timestamp() < ObTimeUtility::current_time()) {
              ret = OB_TIMEOUT;
            }
          }
        } else {
          const ObNewRow* row = NULL;
          ret = recv_ctx.ptr_px_row_->get_row(recv_ctx.get_cur_row());
          if (OB_SUCCESS == ret) {
            row = &recv_ctx.get_cur_row();
            if (OB_ISNULL(row)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fetch row is null", K(got_channel_idx), K(ret));
            } else {
              if (0 > got_channel_idx || got_channel_idx >= recv_ctx.row_stores_.count()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid channel idx", K(got_channel_idx), K(ret));
              } else {
                ObRARowStore* row_store = recv_ctx.row_stores_.at(got_channel_idx);
                int64_t cur_pos = row_store->get_row_cnt();
                int64_t& end_count = last_end_array.at(got_channel_idx);
                const ObNewRow* last_row = NULL;
                if (cur_pos == 0) {
                  // first row
                  if (OB_FAIL(row_store->add_row(*row))) {
                    LOG_WARN("fail to add row to row store", K(got_channel_idx), K(ret));
                  }
                } else if (OB_SUCC(row_store->get_row(cur_pos - 1, last_row))) {
                  cmp_row_arr.at(0) = last_row;
                  cmp_row_arr.at(1) = row;
                  bool is_new_group = cmp_fun(0, 1);
                  if (OB_SUCC(cmp_fun.get_ret())) {
                    if (is_new_group) {
                      if (recv_ctx.merge_inputs_.count() > MAX_INPUT_NUMBER) {
                        ret = OB_ERR_UNEXPECTED;
                        LOG_WARN("too much local order inputs", K(ret));
                      } else {
                        // new group, the range is [start_pos, end_pos)
                        void* buf = recv_ctx.exec_ctx_.get_allocator().alloc(sizeof(LocalOrderInput));
                        if (OB_ISNULL(buf)) {
                          ret = OB_ALLOCATE_MEMORY_FAILED;
                          LOG_WARN("create ra row store fail", K(ret));
                        } else {
                          MergeSortInput* new_msi = new (buf) LocalOrderInput(row_store, end_count, cur_pos);
                          if (OB_FAIL(recv_ctx.merge_inputs_.push_back(new_msi))) {
                            LOG_WARN("fail push back MergeSortInput", K(got_channel_idx), K(ret));
                          } else {
                            end_count = cur_pos;
                          }
                        }
                      }
                    }
                    if (OB_SUCC(ret) && OB_FAIL(row_store->add_row(*row))) {
                      LOG_WARN("fail to add row to row store", K(got_channel_idx), K(ret));
                    }
                  } else {
                    LOG_WARN("fail split new group", K(got_channel_idx), K(ret));
                  }
                } else {
                  LOG_WARN("fail to get row", K(cur_pos - 1), K(got_channel_idx), K(ret));
                }
              }
            }
          } else if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            ObRARowStore* row_store = recv_ctx.row_stores_.at(got_channel_idx);
            int64_t cur_pos = row_store->get_row_cnt();
            int64_t& end_count = last_end_array.at(got_channel_idx);
            void* buf = recv_ctx.exec_ctx_.get_allocator().alloc(sizeof(LocalOrderInput));
            if (nullptr == buf) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("create local order input fail", K(ret));
            } else {
              MergeSortInput* new_msi = new (buf) LocalOrderInput(row_store, end_count, cur_pos);
              new_msi->set_finish(true);
              if (OB_SUCC(recv_ctx.merge_inputs_.push_back(new_msi))) {
                // All data is fetched for one input
                ret = OB_SUCCESS;
                n_finish++;
                if (n_finish == n_channel) {
                  recv_ctx.finish_ = true;
                }
              } else {
                LOG_WARN("fail push back MergeSortInput", K(got_channel_idx), K(ret));
              }
            }
          } else {
            LOG_WARN("fail get row from row store", K(ret));
          }
        }
        channel_idx++;
      }
    }

    // build row heap
    if (OB_SUCC(ret) && OB_FAIL(recv_ctx.row_heap_.init(recv_ctx.merge_inputs_.count(), sort_columns_))) {
      LOG_WARN("fail to init row heap", K(ret));
    }
  }
  return ret;
}
// end get all row in local order

int ObPxMergeSortReceive::try_link_channel(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMergeSortReceiveCtx* recv_ctx = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  if (OB_ISNULL(recv_ctx = GET_PHY_OPERATOR_CTX(ObPxMergeSortReceiveCtx, ctx, get_id())) ||
      OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Get operator context failed", K(ret), K(get_id()));
  } else if (!recv_ctx->channel_linked()) {
    ObPxReceiveInput* recv_input = NULL;
    if (OB_ISNULL(recv_input = GET_PHY_OP_INPUT(ObPxReceiveInput, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Fail to get op input", K(ret), "op_id", get_id(), "op_type", get_type());
    } else {
      ret = recv_ctx->init_channel(ctx,
          *recv_input,
          recv_ctx->task_ch_set_,
          recv_ctx->task_channels_,
          recv_ctx->msg_loop_,
          recv_ctx->px_row_msg_proc_,
          recv_ctx->interrupt_proc_);
      recv_ctx->metric_.set_id(get_id());
      if (OB_FAIL(ret)) {
        LOG_WARN("Fail to init channel", K(ret));
      } else if (!local_order_ && OB_FAIL(recv_ctx->row_heap_.init(recv_ctx->get_channel_count(), sort_columns_))) {
        LOG_WARN("Row heap init failed", "count", recv_ctx->get_channel_count(), K(ret));
      } else if (OB_FAIL(init_merge_sort_input(ctx, recv_ctx, recv_ctx->get_channel_count()))) {
        LOG_WARN("Merge sort input init failed", K(ret));
      }
    }
  }
  return ret;
}
