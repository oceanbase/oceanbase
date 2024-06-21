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

#include "ob_px_ms_receive_vec_op.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/engine/basic/ob_temp_row_store.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace sql
{

OB_SERIALIZE_MEMBER((ObPxMSReceiveVecOpInput, ObPxReceiveOpInput));

OB_SERIALIZE_MEMBER((ObPxMSReceiveVecSpec, ObPxReceiveSpec),
                    all_exprs_,
                    sort_collations_,
                    sort_cmp_funs_,
                    local_order_);

ObPxMSReceiveVecSpec::ObPxMSReceiveVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObPxReceiveSpec(alloc, type),
    all_exprs_(alloc),
    sort_collations_(alloc),
    sort_cmp_funs_(alloc),
    local_order_(false)
{
}

ObPxMSReceiveVecOp::ObPxMSReceiveVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObPxReceiveOp(exec_ctx, spec, input),
    ptr_row_msg_loop_(&msg_loop_),
    interrupt_proc_(),
    row_heap_(),
    merge_inputs_(),
    finish_(false),
    mem_context_(nullptr),
    profile_(ObSqlWorkAreaType::SORT_WORK_AREA),
    sql_mem_processor_(profile_, op_monitor_info_),
    processed_cnt_(0),
    all_expr_vectors_(exec_ctx.get_allocator()),
    skip_(nullptr),
    selector_(nullptr),
    row_meta_(nullptr),
    stored_compact_rows_(nullptr),
    output_store_()
{}

void ObPxMSReceiveVecOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  output_store_.~ObTempRowStore();
  if (nullptr != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  merge_inputs_.reset();
  row_heap_.reset();
  //no need to reset interrupt_proc_
  ObPxReceiveOp::destroy();
}

// init input information in global order and local order
int ObPxMSReceiveVecOp::init_merge_sort_input(int64_t n_channel)
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.local_order_) {
    // global order, init merge sort input
    if (0 >= n_channel) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channels are not init", K(ret));
    } else {
      for(int64_t idx = 0; OB_SUCC(ret) && idx < n_channel; ++idx) {
        void *buf = mem_context_->get_malloc_allocator().alloc(sizeof(GlobalOrderInput));
        if (nullptr == buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("create merge sort input fail", K(idx), K(ret));
        } else {
          MergeSortInput *msi = new (buf) GlobalOrderInput(
            ctx_.get_my_session()->get_effective_tenant_id());
          msi->alloc_ = &mem_context_->get_malloc_allocator();
          msi->sql_mem_processor_ = &sql_mem_processor_;
          msi->io_event_observer_ = &io_event_observer_;
          msi->compressor_type_ = MY_SPEC.compress_type_;
          if (OB_FAIL(merge_inputs_.push_back(msi))) {
            LOG_WARN("push back merge sort input fail", K(idx), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPxMSReceiveVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(ctx_.get_my_session()->get_effective_tenant_id(),
        "PxMsReceiveOp",
        ObCtxIds::WORK_AREA);
  if (OB_FAIL(ObPxReceiveOp::inner_open())) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("failed to create context", K(ret));
  } else if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null memory entity returned", K(ret));
  } else {
    int64_t row_count = MY_SPEC.rows_;
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
        &ctx_, MY_SPEC.px_est_size_factor_, row_count, row_count))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(
        &mem_context_->get_malloc_allocator(),
        ctx_.get_my_session()->get_effective_tenant_id(),
        row_count * MY_SPEC.width_, MY_SPEC.type_, MY_SPEC.id_, &ctx_))) {
      LOG_WARN("failed to init sql memory manager processor", K(ret));
    } else {
      void *mem = NULL;
      ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(),
                     "PxMsOutputStore", ObCtxIds::EXECUTE_CTX_ID);
      if (OB_FAIL(output_store_.init(MY_SPEC.all_exprs_, get_spec().max_batch_size_,
                                     attr, 0 /*mem_limit*/, false /*enable_dump*/,
                                     0 /*row_extra_size*/, NONE_COMPRESSOR))) {
        LOG_WARN("init output store failed", K(ret));
      } else if (OB_ISNULL(mem = ctx_.get_allocator().alloc(
            ObBitVector::memory_size(get_spec().max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for skip", K(ret));
      } else if (OB_ISNULL(selector_ = static_cast<uint16_t *>(ctx_.get_allocator().alloc(sizeof(uint16_t) * get_spec().max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for selector", K(ret));
      } else if (OB_ISNULL(stored_compact_rows_ = static_cast<const ObCompactRow **>(
                      ctx_.get_allocator().alloc(spec_.max_batch_size_ * sizeof(ObCompactRow *))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for stored compact rows", K(ret));
      } else {
        skip_ = to_bit_vector(mem);
        skip_->reset(get_spec().max_batch_size_);
        for (int64_t i = 0; i < get_spec().max_batch_size_; i++) {
          selector_[i] = i;
        }
      }
    }
  }
  return ret;
}

//提前释放row store数据，而不是等到close时释放
int ObPxMSReceiveVecOp::release_merge_inputs()
{
  int ret = OB_SUCCESS;
  int release_merge_sort_ret = OB_SUCCESS;
  while (0 < merge_inputs_.count()) {
    MergeSortInput *msi = NULL;
    if (OB_SUCCESS != (release_merge_sort_ret = merge_inputs_.pop_back(msi))) {
      ret = release_merge_sort_ret;
      LOG_WARN("pop back merge sort input failed", K(release_merge_sort_ret));
    } else {
      msi->clean_row_store(ctx_);
      msi->destroy();
      msi->~MergeSortInput();
      mem_context_->get_malloc_allocator().free(msi);
    }
  }
  return ret;
}


int ObPxMSReceiveVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  output_iter_.reset();
  output_store_.reset();
  int release_channel_ret = ObPxChannelUtil::flush_rows(task_channels_);
  if (release_channel_ret != common::OB_SUCCESS) {
    LOG_WARN("release dtl channel failed", K(release_channel_ret));
  }

  release_channel_ret = msg_loop_.unregister_all_channel();
  if (release_channel_ret != common::OB_SUCCESS) {
    // the following unlink actions is not safe is any unregister failure happened
    LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
  }

  release_channel_ret = ObPxChannelUtil::unlink_ch_set(get_ch_set(), &dfc_, true);
  if (release_channel_ret != common::OB_SUCCESS) {
    LOG_WARN("release dtl channel failed", K(release_channel_ret));
  }

  int release_merge_sort_ret = OB_SUCCESS;
  release_merge_sort_ret = release_merge_inputs();
  if (release_merge_sort_ret != common::OB_SUCCESS) {
    LOG_WARN("release dtl channel failed", K(release_merge_sort_ret));
  }

  release_channel_ret = erase_dtl_interm_result();
  if (release_channel_ret != common::OB_SUCCESS) {
    LOG_TRACE("release interm result failed", KR(release_channel_ret));
  }
  sql_mem_processor_.unregister_profile();
  return ret;
}

// Local Order Function
int ObPxMSReceiveVecOp::LocalOrderInput::open()
{
  return temp_row_reader_.init(get_row_store_);
}

int ObPxMSReceiveVecOp::LocalOrderInput::add_batch(
  ObPxMSReceiveVecOp &ms_receive_op,
  const ObIArray<ObExpr*> &exprs,
  ObEvalCtx &eval_ctx,
  const int64_t size)
{
  int ret = OB_SUCCESS;
  UNUSED(ms_receive_op);
  UNUSED(exprs);
  UNUSED(eval_ctx);
  UNUSED(size);
  if (OB_ISNULL(add_row_store_) || OB_ISNULL(get_row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row store is not init", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("all data are added", K(ret));
  }
  return ret;
}

int ObPxMSReceiveVecOp::LocalOrderInput::get_row(
  ObPxMSReceiveVecOp *ms_receive_op,
  ObPhysicalPlanCtx *phy_plan_ctx,
  int64_t channel_idx,
  const ObIArray<ObExpr*> &exprs,
  ObEvalCtx &eval_ctx,
  const ObCompactRow *&store_row)
{
  int ret = OB_SUCCESS;
  UNUSED(phy_plan_ctx);
  UNUSED(exprs);
  UNUSED(eval_ctx);
  const int64_t max_rows = 1;
  int64_t read_rows = 0;
  if (OB_FAIL(temp_row_reader_.get_next_batch(max_rows, read_rows, ms_receive_op->stored_compact_rows_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail get row", K(ret));
    } else {
      LOG_TRACE("finish to fetch all data from one input",
        K(channel_idx), K(row_store_.get_row_cnt()), K(ret));
    }
  } else {
    store_row = ms_receive_op->stored_compact_rows_[0];
  }
  return ret;
}

int64_t ObPxMSReceiveVecOp::LocalOrderInput::max_pos()
{
  return row_store_.get_row_cnt();
}

void ObPxMSReceiveVecOp::LocalOrderInput::clean_row_store(ObExecContext &ctx)
{
  UNUSED(ctx);
  temp_row_reader_.reset();
  row_store_.reset();
  get_row_store_ = nullptr;
  add_row_store_ = nullptr;
}

void ObPxMSReceiveVecOp::LocalOrderInput::destroy()
{
  if (nullptr != get_row_store_ || nullptr != add_row_store_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected status: row store is not null", K(get_row_store_), K(add_row_store_));
  }
  get_row_store_ = nullptr;
  add_row_store_ = nullptr;
}
// end Local Order function

// Global Order Function
int ObPxMSReceiveVecOp::GlobalOrderInput::reset_add_row_store(bool &reset)
{
  int ret = OB_SUCCESS;
  reset = false;
  if (OB_ISNULL(add_row_store_) || add_row_store_ == get_row_store_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get row store, all row store is empty", K(ret));
  } else if (nullptr != add_row_reader_
          && !add_row_reader_->has_next()) {
    reset = true;
  }
  return ret;
}

bool ObPxMSReceiveVecOp::GlobalOrderInput::is_empty()
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

// 拿行前调用此接口，进行get_row_store_切换，调用前确保了两个row_store_中至少有一个有行。
int ObPxMSReceiveVecOp::GlobalOrderInput::switch_get_row_store() {
  int ret = OB_SUCCESS;
  // get_row_store_为NULL那么去add_row_store_中读数据
  if (OB_ISNULL(get_row_store_)) {
    get_row_store_ = add_row_store_;
    if (OB_FAIL(get_reader_.init(get_row_store_))) {
      LOG_WARN("failed to init chunk store iterator", K(ret));
    } else {
      get_row_reader_ = &get_reader_;
    }
  }
  // 如果两个指针相等，因为之后要从这个store中读数据了，因此把add_row_store_置空。
  // 这个只有上面的if条件也满足时，即之前只写过数据，没有读过，get_row_store_为NULL
  // 此时要把get_row_store_指向add_row_store_，并把add_row_store_置为NULL。
  // 稍微add_row时发现add_row_store_为NULL，会创建第二个store
  if (add_row_store_ == get_row_store_) {
    add_row_reader_->reset();
    add_row_reader_ = nullptr;
    add_row_store_ = nullptr;
  }
  if (OB_SUCC(ret) && !get_row_reader_->has_next()) {
    // get_row_reader_中没数据了，add_row_reader_一定还有，因为调这个函数前判断过一定还有数据没读完
    // 那么交换两个指针，开始读有数据的store，向空store中写数据。
    if (OB_ISNULL(add_row_store_) || !add_row_reader_->has_next()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get row store, all row store is empty", K(ret));
    } else if (OB_FAIL(add_row_store_->finish_add_row())) {
      LOG_WARN("failed to finish add row", K(ret));
    } else {
      // switch row store that has data
      // 这里之前为了避免频繁的将row store来回切的同时来回reset，所以数据是append到一定量后才开始真正清空
      ObTempRowStore::Iterator *tmp_reader = get_row_reader_;
      get_row_reader_ = add_row_reader_;
      add_row_reader_ = tmp_reader;

      ObTempRowStore *tmp_store = get_row_store_;
      get_row_store_ = add_row_store_;
      add_row_store_ = tmp_store;

    }
  }
  if (OB_SUCC(ret) && (add_row_store_ == get_row_store_ || get_row_reader_ == add_row_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get and add row store are same", K(ret));
  }
  return ret;
}

// heap中弹出的值是第channel_idx个channel的数据，因此要从channel_idx中拿数据填入heap中，
// 如果这个channel对应的store此时是空的，那么调用此接口从channel中读消息把数据填入store中
int ObPxMSReceiveVecOp::GlobalOrderInput::get_rows_from_channels(
  ObPxMSReceiveVecOp *ms_receive_op,
  ObPhysicalPlanCtx *phy_plan_ctx,
  int64_t channel_idx,
  const ObIArray<ObExpr*> &exprs,
  ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  int64_t hint_channel_idx = channel_idx;
  int64_t got_channel_idx = OB_INVALID_INDEX_INT64;
  bool fetched = false;
  // 一直拿数据直到拿到的数据是channel_idx相同，则row_heap可以进行堆排序返回一行
  // 当前策略是，如果channel_idx的数据没有拿到，则需要轮询所有channel之后再拿channel_idx的数据
  while (OB_SUCC(ret) && !fetched && !is_finish()) {
    got_channel_idx = hint_channel_idx;
    if (OB_FAIL(ms_receive_op->ptr_row_msg_loop_->process_one(got_channel_idx))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(eval_ctx.exec_ctx_.check_status())) {
          LOG_WARN("check status failed", K(channel_idx), K(ret));
        }
      } else {
        LOG_WARN("failed to process", K(channel_idx), K(got_channel_idx), K(ret));
      }
    } else {
      // rows in reader belong to %got_channel_idx, should be all consumed here.
      int64_t add_rows = 0;
      while (OB_SUCC(ret) && ms_receive_op->row_reader_.has_more()) {
        ms_receive_op->clear_evaluated_flag();
        ms_receive_op->clear_dynamic_const_parent_flag();
        int64_t read_rows = 0;
        const ObPxMSReceiveVecSpec &spec = ms_receive_op->my_spec();
        if (OB_FAIL(ms_receive_op->row_reader_.get_next_batch_vec(spec.child_exprs_,
                                                              spec.dynamic_const_exprs_,
                                                              eval_ctx,
                                                              spec.max_batch_size_,
                                                              read_rows,
                                                              ms_receive_op->vector_rows_))) {
          LOG_WARN("get row failed", K(ret));
        } else {
          processed_cnt_ += read_rows;
          add_rows += read_rows;
          MergeSortInput *tmp_msi = ms_receive_op->merge_inputs_.at(got_channel_idx);
          LOG_DEBUG("[VEC2.0 PX] global input get batch from channel", K(read_rows),
                    K(got_channel_idx), K(tmp_msi));
          if (OB_FAIL(process_dump(*ms_receive_op))) {
            LOG_WARN("failed to process dump", K(ret));
          } else if (OB_FAIL(ms_receive_op->eval_all_exprs(read_rows))) {
            LOG_WARN("eval all exprs failed", K(ret));
          } else if (OB_FAIL(tmp_msi->add_batch(*ms_receive_op, exprs, eval_ctx, read_rows))) {
            LOG_WARN("fail to add row", K(got_channel_idx), K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        auto got_ch = ms_receive_op->ptr_row_msg_loop_->get_channel(got_channel_idx);
        auto ch = ms_receive_op->ptr_row_msg_loop_->get_channel(channel_idx);
        if (NULL == ch || NULL == got_ch) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get channel failed", K(ret), K(channel_idx), K(got_channel_idx));
        } else {
          if (got_ch->is_eof()) {
            MergeSortInput *tmp_msi = ms_receive_op->merge_inputs_.at(got_channel_idx);
            tmp_msi->set_finish(true);
            LOG_TRACE("channel finish get data",
                      K(got_channel_idx), K(tmp_msi->max_pos()), K(ret), K(finish_));
          }
          if (ch->is_eof() || (got_channel_idx == channel_idx && add_rows > 0)) {
            fetched = true;
          }
        }
      }
    }
    hint_channel_idx = OB_INVALID_INDEX_INT64;
  }
  if(OB_SUCC(ret)) {
    if (got_channel_idx != ms_receive_op->row_heap_.writable_channel_idx() && !is_finish()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel idx is not same as writable channel idx", K(got_channel_idx), K(channel_idx), K(ret));
    }
  }
  return ret;
}

int ObPxMSReceiveVecOp::GlobalOrderInput::get_row(
  ObPxMSReceiveVecOp *ms_receive_op,
  ObPhysicalPlanCtx *phy_plan_ctx,
  int64_t channel_idx,
  const ObIArray<ObExpr*> &exprs,
  ObEvalCtx &eval_ctx,
  const ObCompactRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    if (OB_FAIL(get_rows_from_channels(ms_receive_op, phy_plan_ctx, channel_idx, exprs, eval_ctx))) {
      LOG_WARN("fail to get one row in global order", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t max_rows = 1;
    int64_t read_rows = 0;
    int64_t get_row_reader_cnt = NULL == get_row_reader_ ? 0 : get_row_reader_->get_row_cnt();
    int64_t add_row_reader_cnt = NULL == add_row_reader_ ? 0 : add_row_reader_->get_row_cnt();
    LOG_DEBUG("[VEC2.0 PX] global input get row", K(ret), K(get_row_reader_), K(get_row_reader_cnt),
              K(add_row_reader_), K(add_row_reader_cnt), K(is_empty()), K(output_rows_));
    if (is_empty()) {
      ret = OB_ITER_END;
      LOG_TRACE("finish to fetch all data from one input", K(ret));
      if (!finish_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fetch last row but merge input isn't finish", K(ret));
      } else {
        temp_row_reader_.reset();
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
    } else if (OB_FAIL(get_row_reader_->get_next_batch(max_rows, read_rows,
                        static_cast<const ObCompactRow **>(ms_receive_op->stored_compact_rows_)))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail get row", K(ret));
      } else {
        LOG_TRACE("finish to fetch data from one global input", K(channel_idx), K(ret));
      }
    } else {
      output_rows_++;
      store_row = ms_receive_op->stored_compact_rows_[0];
    }
  }
  return ret;
}

int64_t ObPxMSReceiveVecOp::GlobalOrderInput::max_pos()
{
  int64_t rn = 0;
  if (nullptr == get_row_store_) {
    rn = 0;
  } else {
    rn = get_row_store_->get_row_cnt();
  }
  return rn;
}

void ObPxMSReceiveVecOp::GlobalOrderInput::clean_row_store(
  ObExecContext &ctx)
{
  temp_row_reader_.reset();
  get_reader_.reset();
  add_row_reader_ = nullptr;
  get_row_reader_ = nullptr;
  if (nullptr != add_row_store_) {
    if (add_row_store_ == get_row_store_) {
      get_row_store_ = nullptr;
    }
    add_row_store_->reset();
    add_row_store_->~ObTempRowStore();
    ctx.get_allocator().free(add_row_store_);
    add_row_store_ = nullptr;
  }
  if (nullptr != get_row_store_) {
    get_row_store_->reset();
    get_row_store_->~ObTempRowStore();
    ctx.get_allocator().free(get_row_store_);
    get_row_store_ = nullptr;
  }
}

void ObPxMSReceiveVecOp::GlobalOrderInput::destroy()
{
  if (nullptr != add_row_store_ || nullptr != get_row_store_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpect status: row store is not null", K(add_row_store_), K(get_row_store_));
  }
  get_row_store_ = nullptr;
  add_row_store_ = nullptr;
}

int ObPxMSReceiveVecOp::GlobalOrderInput::create_temp_row_store(
  ObPxMSReceiveVecOp &ms_receive_op, uint64_t tenant_id, ObTempRowStore *&row_store)
{
  int ret = OB_SUCCESS;
  ObExecContext &ctx = ms_receive_op.get_exec_ctx();
  void *buf = ctx.get_allocator().alloc(sizeof(ObTempRowStore));
  row_store = nullptr;
  if (OB_ISNULL(alloc_) || OB_ISNULL(sql_mem_processor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global input is not init", KP(alloc_), KP(sql_mem_processor_), K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ra row store fail", K(ret));
  } else {
    row_store = new (buf) ObTempRowStore(alloc_);
    // TODO: llongzhong.wlz 这里应该使用一个参数来控制row_store存储的数据量，或者SQL内存管理自动控制
    int64_t mem_limit = 0;
    row_store->set_allocator(*alloc_);
    row_store->set_callback(sql_mem_processor_);
    row_store->set_io_event_observer(io_event_observer_);
    ObMemAttr mem_attr(tenant_id, "PxMSRecvGlobalV", ObCtxIds::WORK_AREA);
    const ObPxMSReceiveVecSpec &spec = ms_receive_op.my_spec();
    if (OB_FAIL(row_store->init(spec.all_exprs_, spec.max_batch_size_,
                                mem_attr, mem_limit, true /* enable_dump*/,
                                0 /*row_extra_size*/,
                                compressor_type_))) {
      row_store->~ObTempRowStore();
      ctx.get_allocator().free(buf);
      row_store = nullptr;
      LOG_WARN("row store init fail", K(ret));
    } else {
      row_store->set_dir_id(sql_mem_processor_->get_dir_id());
    }
  }
  return ret;
}

// 向add_row_store_中写入行
int ObPxMSReceiveVecOp::GlobalOrderInput::add_batch(
  ObPxMSReceiveVecOp &ms_receive_op,
  const ObIArray<ObExpr*> &exprs,
  ObEvalCtx &eval_ctx,
  const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(add_row_store_)) {
    ObTempRowStore *row_store = nullptr;
    if (OB_FAIL(create_temp_row_store(ms_receive_op, tenant_id_, row_store))) {
      LOG_WARN("failed to create row store", K(ret));
    } else {
      add_row_store_ = row_store;
      if (OB_FAIL(temp_row_reader_.init(add_row_store_))) {
        LOG_WARN("failed to init chunk store iterator", K(ret));
      } else {
        add_row_reader_ = &temp_row_reader_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(add_row_store_) || add_row_store_ == get_row_store_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add row store is null or is same as get row store", K(ret));
    } else {
      bool reset = false;
      // 往add_row_store_中写行前先调一次reset，如果store没数据了，reset返回true，此时重置add_row_store
      if (OB_FAIL(reset_add_row_store(reset))) {
        LOG_WARN("fail to switch add row store", K(ret));
      } else if (reset) {
        LOG_TRACE("reset add row store", K(add_row_reader_), K(*add_row_store_));
        add_row_reader_->reset();
        add_row_store_->reset();
        if (OB_FAIL(add_row_reader_->init(add_row_store_))) {
          LOG_WARN("failed to init chunk store iterator", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_row_store_->add_batch(ms_receive_op.all_expr_vectors_,
                                               ms_receive_op.selector_, size))) {
    LOG_WARN("fail to add row", K(ret));
  } else {
    LOG_DEBUG("[VEC2.0 PX]add row store add_batch", K(add_row_store_), K(size),
             K(ObArrayWrap<uint16_t>(ms_receive_op.selector_, size)));
    ms_receive_op.row_heap_.set_row_meta(add_row_store_->get_row_meta());
    ms_receive_op.row_meta_ = &add_row_store_->get_row_meta();
  }
  return ret;
}
// end Global Order input

int ObPxMSReceiveVecOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const int64_t max_row_cnt = 1;
  if (OB_FAIL(inner_get_next_batch(max_row_cnt))) {
    LOG_WARN("inner get next batch failed", K(ret));
  } else if (OB_UNLIKELY((brs_.end_ && brs_.size_ == 0))) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObPxMSReceiveVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  clear_dynamic_const_parent_flag();
  // 从channel sets 读取数据，并向上迭代
  const ObPxReceiveSpec &spec = static_cast<const ObPxReceiveSpec &>(get_spec());
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Get operator context failed", K(ret), K(MY_SPEC.id_));
  } else if (OB_FAIL(try_link_channel())) {
    LOG_WARN("failed to init channel", K(ret));
  } else if (OB_FAIL(try_send_bloom_filter())) {
    LOG_WARN("fail to send bloom filter", K(ret));
  }
  if (OB_SUCC(ret) && MY_SPEC.local_order_ && !finish_) {
    // local order需要先把channel里的数据拿完，每个channel的数据分段后生成一组inputs
    ret = get_all_rows_from_channels(phy_plan_ctx);
  }
  if (OB_FAIL(ret)) {
  } else if (output_iter_.is_valid() && output_iter_.has_next()) {
    // do nothing
  } else {
    const ObCompactRow *store_row = nullptr;
    output_iter_.reset();
    output_store_.reset();
    while (OB_SUCC(ret) && output_store_.get_row_cnt() < max_row_cnt) {
      // (1) 向 heap 中添加一个或多个元素，直至 heap 满
      while (OB_SUCC(ret) && row_heap_.capacity() > row_heap_.count()) {
        // 不断从inputs中拿行放入row_heap_中，直到放满。
        if (OB_FAIL(get_one_row_from_channels(phy_plan_ctx,
                                              row_heap_.writable_channel_idx(),
                                              MY_SPEC.all_exprs_,
                                              eval_ctx_,
                                              store_row))) {
          if (OB_ITER_END == ret) {
            row_heap_.shrink();
            ret = OB_SUCCESS;
          }
        } else if (OB_ISNULL(store_row)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(row_heap_.push(store_row))) {
          LOG_WARN("fail push row to heap", K(ret));
        }
      }

      // (2) 从 heap 中弹出最大值
      if (OB_SUCC(ret)) {
        ObCompactRow *out_row = NULL;
        if (0 == row_heap_.capacity()) {
          ret = OB_ITER_END;
          iter_end_ = true;
          metric_.mark_eof();
          int release_ret = OB_SUCCESS;
          if (OB_SUCCESS != (release_ret = release_merge_inputs())) {
            LOG_WARN("failed to release merge sort and row store", K(ret), K(release_ret));
          }
        } else if (row_heap_.capacity() == row_heap_.count()) {
          //弹出最大值，放入表达式datum中
          if (OB_FAIL(row_heap_.pop(store_row))) {
            LOG_WARN("fail pop row from heap", K(ret));
          } else if (OB_FAIL(output_store_.add_row(store_row, out_row))) {
            LOG_WARN("failed to push back", K(ret));
          }
          metric_.count();
          metric_.mark_first_out();
          metric_.set_last_out_ts(::oceanbase::common::ObTimeUtility::current_time());
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid row heap state", K(row_heap_), K(ret));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && OB_FAIL(output_store_.begin(output_iter_))) {
      LOG_WARN("init temp row store iterator failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObExpr *> &all_exprs = MY_SPEC.all_exprs_;
    brs_.all_rows_active_ = true;
    brs_.reset_skip(brs_.size_);
    if (OB_SUCC(ret)) {
      int64_t read_rows = 0;
      if (OB_FAIL(output_iter_.get_next_batch(MY_SPEC.all_exprs_, eval_ctx_, max_row_cnt, read_rows))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next batch failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          if (iter_end_) {
            brs_.end_ = true;
          }
        }
      }
      brs_.size_ = read_rows;
    }
  }
  return ret;
}

// 从指定channel中读一行数据
// local order场景：每个channel包含一批inputs，所有channel的input都存在merge_inputs_中，
//                 数据也都从channel读过来存在input的chunk store中了，input->get_row直接从store中拿行。
// global order场景：数据可能还没从channel中读上来，先判断下reader里还有没有行，没有的话从channel里拿msg
int ObPxMSReceiveVecOp::get_one_row_from_channels(
  ObPhysicalPlanCtx *phy_plan_ctx,
  int64_t channel_idx,
  const ObIArray<ObExpr*> &exprs,
  ObEvalCtx &eval_ctx,
  const ObCompactRow *&store_row) // row heap require data from the channel_idx channel
{
  int ret = OB_SUCCESS;
  if (0 > channel_idx || channel_idx > merge_inputs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid channel idx", K(channel_idx), K(ret));
  } else {
    MergeSortInput *msi = merge_inputs_.at(channel_idx);
    if (OB_ISNULL(msi)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge sort input is null", K(ret));
    } else if (OB_FAIL(msi->get_row(this, phy_plan_ctx, channel_idx, exprs, eval_ctx, store_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get row from merge sort input", K(ret));
      }
    } else {
      ++processed_cnt_;
      msi->processed_cnt_ = processed_cnt_;
    }
  }
  return ret;
}

int ObPxMSReceiveVecOp::new_local_order_input(MergeSortInput *&out_msi)
{
  int ret = OB_SUCCESS;
  void *buf = mem_context_->get_malloc_allocator().alloc(sizeof(LocalOrderInput));
  out_msi = nullptr;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ra row store fail", K(ret));
  } else {
    LocalOrderInput *local_input = static_cast<LocalOrderInput*>(new (buf) LocalOrderInput());
    local_input = static_cast<LocalOrderInput*>(new (buf) LocalOrderInput());
    local_input->row_store_.set_allocator(mem_context_->get_malloc_allocator());
    local_input->row_store_.set_callback(&sql_mem_processor_);
    local_input->row_store_.set_io_event_observer(&io_event_observer_);
    ObMemAttr mem_attr(ctx_.get_my_session()->get_effective_tenant_id(), "PxMSRecvLocal", ObCtxIds::WORK_AREA);
    if (OB_FAIL(local_input->row_store_.init(MY_SPEC.all_exprs_, get_spec().max_batch_size_,
                                         mem_attr, 0 /* mem_limit */, true /* enable_dump*/,
                                         0 /*row_extra_size*/,
                                         local_input->compressor_type_))) {
      LOG_WARN("failed to init temp row store", K(ret));
    } else if (FALSE_IT(local_input->row_store_.set_dir_id(sql_mem_processor_.get_dir_id()))) {
      LOG_WARN("failed to allocate dir id for temp row store", K(ret));
    } else if (OB_FAIL(merge_inputs_.push_back(local_input))) {
      LOG_WARN("fail push back MergeSortInput", K(ret));
    } else {
      out_msi = local_input;
    }
  }
  return ret;
}


// local order场景，每个channel把所有数据都收上来，每个channel的数据分段有序，分段后创建一组LocalOrderInput
// 利用分组情况，将datum内的数据加入TempStore中, 加入时会拿到compact row数据，将最后一个记录到last_rows中
int ObPxMSReceiveVecOp::get_all_rows_from_channels(ObPhysicalPlanCtx *phy_plan_ctx)
{
  int ret = OB_SUCCESS;
  if (!finish_) {
    int64_t n_channel = get_channel_count();
    common::ObArray<const ObCompactRow *> last_store_row_array;
    // 存放每个channel上当前最后一个LocalOrderInput中的ObTempRowStore
    common::ObArray<ObTempRowStore*> temp_store_array;
    common::ObArray<ObTempRowStore *> full_dump_array;
    ObTempRowStore *cur_temp_store = nullptr;
    if (OB_FAIL(last_store_row_array.prepare_allocate(n_channel))
      || OB_FAIL(temp_store_array.prepare_allocate(n_channel))) {
      LOG_WARN("fail to prepare allocate array", K(ret));
    } else {
      Compare cmp_fun(MY_SPEC.all_exprs_);
      const ObCompactRow *last_store_row = nullptr;
      if (OB_FAIL(cmp_fun.init(&MY_SPEC.sort_collations_, &MY_SPEC.sort_cmp_funs_))) {
        LOG_WARN("failed to init cmp function", K(ret));
      }
      // 每个channel的数据是local sort，所以需要切分出哪段有序，同时生成MergeSortInput信息
      while (OB_SUCC(ret) && !finish_) {
        if (ptr_row_msg_loop_->all_eof(task_channels_.count())) {
          finish_ = true;
          break;
        }

        int64_t got_channel_idx = OB_INVALID_INDEX_INT64;
        if (OB_FAIL(ptr_row_msg_loop_->process_one(got_channel_idx))) {
          if (OB_EAGAIN == ret) {
            // If no data fetch, then return OB_EAGAIN after OB_ITER_END
            ret = OB_SUCCESS;
            if (OB_FAIL(ctx_.check_status())) {
              LOG_WARN("check status failed", K(ret));
            }
          }
        }
        while (OB_SUCC(ret) && row_reader_.has_more()) {
          clear_evaluated_flag();
          clear_dynamic_const_parent_flag();
          int64_t read_rows = 0;
          // Get row to %child_exprs_ instead of %all_exprs_ which contain sort expressions
          if (OB_FAIL(row_reader_.get_next_batch_vec(MY_SPEC.child_exprs_,
                                                        MY_SPEC.dynamic_const_exprs_,
                                                        eval_ctx_,
                                                        get_spec().max_batch_size_,
                                                        read_rows,
                                                        vector_rows_))) {
            LOG_WARN("get row from reader failed", K(ret));
          } else if (OB_FAIL(eval_all_exprs(read_rows))) {
            LOG_WARN("eval all exprs failed", K(ret));
          } else if (0 > got_channel_idx) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid channel idx", K(got_channel_idx), K(ret));
          } else {
            LOG_DEBUG("[VEC2.0 PX] get all rows from local channel", K(got_channel_idx), K(read_rows));
            processed_cnt_ += read_rows;
            cur_temp_store = temp_store_array.at(got_channel_idx);
            last_store_row = last_store_row_array.at(got_channel_idx);
            // rows before start_idx all have been stored in temp_row_store.
            int64_t start_idx = 0;
            int64_t cur_idx = 0;
            while (cur_idx < read_rows && OB_SUCC(ret)) {
              bool is_new_group = false;
              if (0 == cur_idx) {
                // first row of the batch, compare with last_store_row if it's not null.
                if (OB_NOT_NULL(last_store_row)) {
                  if (OB_ISNULL(cur_temp_store)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("cur temp store is null", K(ret));
                  } else {
                    is_new_group = cmp_fun(last_store_row, cur_temp_store->get_row_meta(),
                                           all_expr_vectors_, cur_idx, eval_ctx_);
                  }
                } else {
                  is_new_group = true;
                }
              } else {
                // compare with the previous row of the batch.
                is_new_group = cmp_fun(all_expr_vectors_, cur_idx - 1, cur_idx, eval_ctx_);
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(cmp_fun.ret_)) {
                LOG_WARN("fail split new group", K(got_channel_idx), K(ret));
              } else if (is_new_group) {
                // rows in [start_idx, cur_idx) belong to the last group.
                int64_t store_row_cnt = NULL == cur_temp_store ? 0 : cur_temp_store->get_row_cnt();
                LOG_DEBUG("[VEC2.0 PX] is new group", K(start_idx), K(cur_idx), K(cur_temp_store),
                        K(store_row_cnt), K(got_channel_idx));
                if (cur_idx > start_idx) {
                  if (OB_ISNULL(cur_temp_store)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("last temp store should not be null", K(ret));
                  } else if (OB_FAIL(process_dump(full_dump_array, temp_store_array))) {
                    LOG_WARN("failed to process dump", K(ret), K(got_channel_idx));
                  } else if (OB_FAIL(cur_temp_store->add_batch(all_expr_vectors_, &selector_[start_idx],
                                     cur_idx - start_idx,
                                     const_cast<ObCompactRow **>(stored_compact_rows_)))) {
                    LOG_WARN("temp row store add batch failed", K(ret));
                  } else {
                    start_idx = cur_idx;
                  }
                }
                MergeSortInput *new_msi = nullptr;
                if (OB_FAIL(ret)) {
                } else if (OB_NOT_NULL(cur_temp_store)
                           && OB_FAIL(full_dump_array.push_back(cur_temp_store))) {
                  LOG_WARN("push back failed", K(ret));
                // create a new local input
                } else if (OB_UNLIKELY(merge_inputs_.count() > MAX_INPUT_NUMBER)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("too much local order inputs", K(ret));
                } else if (OB_FAIL(new_local_order_input(new_msi))) {
                  LOG_WARN("failed to create new local order input", K(ret));
                } else {
                  LocalOrderInput *local_msi = static_cast<LocalOrderInput*>(new_msi);
                  cur_temp_store = &local_msi->row_store_;
                  temp_store_array.at(got_channel_idx) = cur_temp_store;
                }
              } // end of is_new_group
              cur_idx++;
            } // end of loop of a batch
            LOG_DEBUG("[VEC2.0 PX] get new rows from local channel, store remain rows",
                       K(start_idx), K(cur_idx));
            if (OB_SUCC(ret) && OB_LIKELY(cur_idx > start_idx)) {
              // store remaining rows of this batch
              if (OB_FAIL(process_dump(full_dump_array, temp_store_array))) {
                LOG_WARN("failed to process dump", K(ret), K(got_channel_idx));
              } else if (OB_FAIL(cur_temp_store->add_batch(all_expr_vectors_, &selector_[start_idx],
                                     cur_idx - start_idx,
                                     const_cast<ObCompactRow **>(stored_compact_rows_)))) {
                LOG_WARN("temp row store add batch failed", K(ret));
              } else {
                last_store_row = stored_compact_rows_[cur_idx - start_idx - 1];
                last_store_row_array.at(got_channel_idx) = last_store_row;
              }
            }
          }
        } // end of read all rows in row_reader_
      }
    }

    // build row heap
    if (OB_SUCC(ret)) {
      if (0 >= merge_inputs_.count()) {
      } else if (OB_ISNULL(cur_temp_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(cur_temp_store));
      } else if (OB_FAIL(row_heap_.init(merge_inputs_.count(),
          &MY_SPEC.sort_collations_,
          &MY_SPEC.sort_cmp_funs_))) {
        LOG_WARN("fail to init row heap", K(ret));
      } else {
        row_heap_.set_row_meta(cur_temp_store->get_row_meta());
        // TODO: shanting store row_meta_ more gracefully
        row_meta_ = &(cur_temp_store->get_row_meta());
        for (int64_t i = 0; i < merge_inputs_.count() && OB_SUCC(ret); ++i) {
          LocalOrderInput *local_order_input = static_cast<LocalOrderInput*>(merge_inputs_.at(i));
          if (OB_FAIL(local_order_input->row_store_.finish_add_row())) {
            LOG_WARN("failed to finish add row", K(ret));
          // open时初始化reader_, 后续get_row时将直接从reader_中拿行。
          } else if (OB_FAIL(local_order_input->open())) {
            LOG_WARN("failed to open local order input", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
// end get all row in local order

int ObPxMSReceiveVecOp::eval_all_exprs(const int64_t size)
{
  int ret = OB_SUCCESS;
  all_expr_vectors_.clear();
  const ObIArray<ObExpr *> &all_exprs = MY_SPEC.all_exprs_;
  if (all_expr_vectors_.count() != all_exprs.count()
      && OB_FAIL(all_expr_vectors_.reserve(all_exprs.count()))) {
    LOG_WARN("all expr vectors reserve failed", K(ret));
  } else {
    EvalBound bound(size, true /* all_active */);
    for (int64_t i = 0; i < all_exprs.count() && OB_SUCC(ret); i++) {
      ObExpr *expr = all_exprs.at(i);
      if (OB_FAIL(expr->eval_vector(eval_ctx_, *skip_, bound))) {
        LOG_WARN("eval failed", K(ret));
      } else if (OB_FAIL(all_expr_vectors_.push_back(expr->get_vector(eval_ctx_)))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPxMSReceiveVecOp::try_link_channel()
{
  int ret = OB_SUCCESS;
  // 从channel sets 读取数据，并向上迭代
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Get operator context failed", K(ret), K(MY_SPEC.id_));
  } else if (!channel_linked()) {
    ObPxReceiveOpInput *recv_input = reinterpret_cast<ObPxReceiveOpInput*>(input_);
    ret = init_channel(*recv_input, task_ch_set_,
        task_channels_, msg_loop_, px_row_msg_proc_,
        interrupt_proc_);
    metric_.set_id(MY_SPEC.id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("Fail to init channel", K(ret));
    } else if (!MY_SPEC.local_order_
        && OB_FAIL(row_heap_.init(get_channel_count(),
          &MY_SPEC.sort_collations_,
          &MY_SPEC.sort_cmp_funs_))) {
      LOG_WARN("Row heap init failed", "count", get_channel_count(), K(ret));
    } else if (OB_FAIL(init_merge_sort_input(get_channel_count()))) {
      LOG_WARN("Merge sort input init failed", K(ret));
    }
  }
  return ret;
}

int ObPxMSReceiveVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  row_heap_.reset_heap();
  finish_ = false;
  processed_cnt_ = 0;
  output_iter_.reset();
  output_store_.reset();
  if (OB_FAIL(ObPxReceiveOp::inner_rescan())) {
    LOG_WARN("fail to do recieve op rescan", K(ret));
  } else if (!MY_SPEC.local_order_
             && OB_FAIL(row_heap_.init(get_channel_count(),
                                       &MY_SPEC.sort_collations_,
                                       &MY_SPEC.sort_cmp_funs_))) {
    LOG_WARN("Row heap init failed", "count", get_channel_count(), K(ret));
  } else if (OB_FAIL(release_merge_inputs())) {
    LOG_WARN("fail to release merge sort input", K(ret));
  } else if (OB_FAIL(init_merge_sort_input(task_channels_.count()))) {
    LOG_WARN("Merge sort input init failed", K(ret));
  }
  return ret;
}

int ObPxMSReceiveVecOp::process_dump(const common::ObIArray<ObTempRowStore *> &full_dump_array,
                                  const common::ObIArray<ObTempRowStore *> &part_dump_array)
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
      &mem_context_->get_malloc_allocator(),
      [&](int64_t cur_cnt){ return processed_cnt_ > cur_cnt; },
      updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (OB_FAIL(MergeSortInput::need_dump(sql_mem_processor_, mem_context_->get_malloc_allocator(), dumped))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    for (int64_t i = 0; OB_SUCC(ret) && i < full_dump_array.count(); ++i) {
      CK (OB_NOT_NULL(full_dump_array.at(i)));
      OZ (full_dump_array.at(i)->dump(true));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_dump_array.count(); ++i) {
      if (nullptr == part_dump_array.at(i)) {
        continue;
      }
      OZ (part_dump_array.at(i)->dump(false));
    }
    if (OB_SUCC(ret)) {
      sql_mem_processor_.set_number_pass(1);
      LOG_TRACE("trace px ms receive dump",
        K(sql_mem_processor_.get_data_size()),
        K(sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

int ObPxMSReceiveVecOp::GlobalOrderInput::process_dump(ObPxMSReceiveVecOp &ms_receive_op)
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  if (OB_FAIL(sql_mem_processor_->update_max_available_mem_size_periodically(
      alloc_,
      [&](int64_t cur_cnt){ return processed_cnt_ > cur_cnt; },
      updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (need_dump(*sql_mem_processor_, *alloc_, dumped)) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ms_receive_op.merge_inputs_.count(); ++i) {
      if (OB_NOT_NULL(ms_receive_op.merge_inputs_.at(i)->add_row_store_)) {
        OZ (ms_receive_op.merge_inputs_.at(i)->add_row_store_->dump(false));
      }
    }
    if (OB_SUCC(ret)) {
      sql_mem_processor_->set_number_pass(1);
      LOG_TRACE("trace px ms receive dump",
        K(sql_mem_processor_->get_data_size()),
        K(sql_mem_processor_->get_mem_bound()));
    }
  }
  return ret;
}

ObPxMSReceiveVecOp::Compare::Compare(const common::ObIArray<ObExpr *> &exprs)
  : ret_(OB_SUCCESS), sort_collations_(nullptr), sort_cmp_funs_(nullptr), exprs_(exprs)
{
}

int ObPxMSReceiveVecOp::Compare::init(
    const ObIArray<ObSortFieldCollation> *sort_collations,
    const ObIArray<ObSortCmpFunc> *sort_cmp_funs)
{
  int ret = OB_SUCCESS;
  if (nullptr == sort_collations || nullptr == sort_cmp_funs) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sort_collations), KP(sort_cmp_funs));
  } else if (sort_cmp_funs->count() != sort_cmp_funs->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count miss match", K(ret),
      K(sort_cmp_funs->count()), K(sort_cmp_funs->count()));
  } else {
    sort_collations_ = sort_collations;
    sort_cmp_funs_ = sort_cmp_funs;
  }
  return ret;
}

bool ObPxMSReceiveVecOp::Compare::operator()(const ObCompactRow *l,
                                             const RowMeta &row_meta,
                                             const common::ObIArray<ObIVector *> &vectors,
                                             int64_t r_idx,
                                             ObEvalCtx &eval_ctx)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_UNLIKELY(0 == vectors.count())) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), K(l), K(vectors.count()));
  } else {
    int cmp = 0;
    const char *r_data = NULL;
    ObLength r_len = 0;
    for (int64_t i = 0; 0 == cmp && i < sort_cmp_funs_->count() && OB_SUCC(ret); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      const ObIVector *vec = vectors.at(idx);
      const ObExpr *expr = exprs_.at(idx);
      ObDatum l_datum = l->get_datum(row_meta, idx);

      vec->get_payload(r_idx, r_data, r_len);
      NullSafeRowCmpFunc &cmp_func = NULL_FIRST == sort_collations_->at(i).null_pos_ ?
                             expr->basic_funcs_->row_null_first_cmp_ :
                             expr->basic_funcs_->row_null_last_cmp_;
      const ObObjMeta &obj_meta = expr->obj_meta_;
      if (OB_FAIL(cmp_func(obj_meta, obj_meta, l_datum.ptr_, l_datum.len_, l_datum.is_null(),
                      r_data, r_len, vec->is_null(r_idx), cmp))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp < 0) {
        less = !sort_collations_->at(i).is_ascending_;
      } else if (cmp > 0) {
        less = sort_collations_->at(i).is_ascending_;
      }
    }
  }
  return less;
}

bool ObPxMSReceiveVecOp::Compare::operator()(const common::ObIArray<ObIVector *> &vectors,
                                             int64_t l_idx,
                                             int64_t r_idx,
                                             ObEvalCtx &eval_ctx)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_UNLIKELY(0 == vectors.count())) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), K(vectors.count()));
  } else {
    int cmp = 0;
    const char *l_data = NULL;
    const char *r_data = NULL;
    ObLength l_len = 0;
    ObLength r_len = 0;
    for (int64_t i = 0; 0 == cmp && i < sort_cmp_funs_->count() && OB_SUCC(ret); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      const ObExpr *expr = exprs_.at(idx);

      NullSafeRowCmpFunc &cmp_func = NULL_FIRST == sort_collations_->at(i).null_pos_ ?
                             expr->basic_funcs_->row_null_first_cmp_ :
                             expr->basic_funcs_->row_null_last_cmp_;
     const ObObjMeta &obj_meta = expr->obj_meta_;
      const ObIVector *vec = vectors.at(idx);
      vec->get_payload(l_idx, l_data, l_len);
      vec->get_payload(r_idx, r_data, r_len);
      if (OB_FAIL(cmp_func(obj_meta, obj_meta, l_data, l_len, vec->is_null(l_idx),
                           r_data, r_len, vec->is_null(r_idx), cmp))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp < 0) {
        less = !sort_collations_->at(i).is_ascending_;
      } else if (cmp > 0) {
        less = sort_collations_->at(i).is_ascending_;
      }
    }
  }
  return less;
}

int ObPxMSReceiveVecOp::MergeSortInput::need_dump(ObSqlMemMgrProcessor &sql_mem_processor,
                                               ObIAllocator &alloc,
                                               bool &need_dump)
{
  int ret = OB_SUCCESS;
  need_dump = false;
  if (sql_mem_processor.get_data_size() > sql_mem_processor.get_mem_bound()
          && GCONF.is_sql_operator_dump_enabled()
          && OB_FAIL(sql_mem_processor.extend_max_memory_size(
            &alloc,
            [&](int64_t max_memory_size) {
              return sql_mem_processor.get_data_size() > max_memory_size;
            },
            need_dump, sql_mem_processor.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
