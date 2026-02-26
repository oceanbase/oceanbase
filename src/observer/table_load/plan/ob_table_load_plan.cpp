/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_plan.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_data_channel.h"
#include "observer/table_load/plan/ob_table_load_full_plan.h"
#include "observer/table_load/plan/ob_table_load_inc_plan.h"
#include "observer/table_load/plan/ob_table_load_op.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
ObTableLoadPlan::ObTableLoadPlan(ObTableLoadStoreCtx *store_ctx)
  : ctx_(store_ctx->ctx_),
    store_ctx_(store_ctx),
    allocator_("TLD_Plan"),
    first_table_op_(nullptr),
    finish_op_(nullptr)
{
  allocator_.set_tenant_id(MTL_ID());
  ops_.set_block_allocator(ModulePageAllocator(allocator_));
  table_ops_.set_block_allocator(ModulePageAllocator(allocator_));
  channels_.set_block_allocator(ModulePageAllocator(allocator_));
}

ObTableLoadPlan::~ObTableLoadPlan()
{
  for (int64_t i = 0; i < ops_.count(); ++i) {
    ObTableLoadOp *op = ops_.at(i);
    op->~ObTableLoadOp();
    allocator_.free(op);
  }
  ops_.reset();
  for (int64_t i = 0; i < table_ops_.count(); ++i) {
    ObTableLoadTableOp *table_op = table_ops_.at(i);
    table_op->~ObTableLoadTableOp();
    allocator_.free(table_op);
  }
  table_ops_.reset();
  for (int64_t i = 0; i < channels_.count(); ++i) {
    ObTableLoadTableChannel *channel = channels_.at(i);
    channel->~ObTableLoadTableChannel();
    allocator_.free(channel);
  }
  channels_.reset();
}

ObTableLoadWriteType::Type ObTableLoadPlan::get_write_type()
{
  if (store_ctx_->write_ctx_.is_fast_heap_table_) {
    return ObTableLoadWriteType::DIRECT_WRITE;
  } else if (store_ctx_->write_ctx_.enable_pre_sort_) {
    return ObTableLoadWriteType::PRE_SORT_WRITE;
  } else {
    return ObTableLoadWriteType::STORE_WRITE;
  }
}

int ObTableLoadPlan::finish_generate(ObTableLoadTableOp *first_table_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == first_table_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected first table op is null", KR(ret), KP(first_table_op));
  } else {
    // 检查first_table_op是否有上游依赖
    if (OB_UNLIKELY(!first_table_op->get_dependencies().empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected first table op has dependencies", KR(ret), KPC(first_table_op));
    }
    // 检查first_table_op的输入是不是write_input
    else if (OB_UNLIKELY(ObTableLoadInputType::WRITE_INPUT != first_table_op->get_input_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected root table op input type", KR(ret), KPC(first_table_op));
    } else {
      first_table_op_ = first_table_op;
    }
    // 创建finish_op
    if (OB_SUCC(ret)) {
      ObTableLoadFinishOp *finish_op = nullptr;
      if (OB_FAIL(alloc_op(finish_op, this))) {
        LOG_WARN("fail to alloc op", KR(ret));
      } else {
        finish_op_ = finish_op;
        FLOG_INFO("[DIRECT_LOAD_OP] generate plan success", "plan", *this);
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObTableLoadPlan)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(ctx), KP_(store_ctx));
  J_COMMA();
  J_NAME("table_ops");
  J_COLON();
  J_ARRAY_START();
  if (!table_ops_.empty()) {
    databuff_printf(buf, buf_len, pos, "cnt:%ld, ", table_ops_.count());
    for (int64_t i = 0; i < table_ops_.count(); ++i) {
      ObTableLoadTableOp *table_op = table_ops_.at(i);
      if (0 == i) {
        databuff_printf(buf, buf_len, pos, "%ld:", i);
      } else {
        databuff_printf(buf, buf_len, pos, ", %ld:", i);
      }
      pos += table_op->simple_to_string(buf + pos, buf_len - pos);
    }
  }
  J_ARRAY_END();
  J_COMMA();
  J_NAME("channels");
  J_COLON();
  J_ARRAY_START();
  if (!channels_.empty()) {
    databuff_printf(buf, buf_len, pos, "cnt:%ld, ", channels_.count());
    for (int64_t i = 0; i < channels_.count(); ++i) {
      ObTableLoadTableChannel *channel = channels_.at(i);
      if (0 == i) {
        databuff_printf(buf, buf_len, pos, "%ld:", i);
      } else {
        databuff_printf(buf, buf_len, pos, ", %ld:", i);
      }
      pos += channel->simple_to_string(buf + pos, buf_len - pos);
    }
  }
  J_ARRAY_END();
  J_COMMA();
  J_KV(KP_(first_table_op), KP_(finish_op));
  J_OBJ_END();
  return pos;
}

int ObTableLoadPlan::create_plan(ObTableLoadStoreCtx *store_ctx, ObIAllocator &allocator,
                                 ObTableLoadPlan *&plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx));
  } else {
    if (ObDirectLoadMethod::is_incremental(store_ctx->ctx_->param_.method_)) {
      if (OB_FAIL(ObTableLoadIncPlan::create_plan(store_ctx, allocator, plan))) {
        LOG_WARN("fail to create inc plan", KR(ret));
      }
    } else {
      if (OB_FAIL(ObTableLoadFullPlan::create_plan(store_ctx, allocator, plan))) {
        LOG_WARN("fail to create full plan", KR(ret));
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
