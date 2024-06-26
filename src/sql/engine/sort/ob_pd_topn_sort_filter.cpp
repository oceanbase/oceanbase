/**
 * Copyright (c) 2024 OceanBase
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

#include "sql/engine/sort/ob_pd_topn_sort_filter.h"
#include "sql/engine/sort/ob_sort_vec_op.h"
#include "sql/engine/sort/ob_sort_vec_op_context.h"
#include "sql/engine/px/p2p_datahub/ob_pushdown_topn_filter_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "share/rc/ob_context.h"
#include "sql/engine/expr/ob_expr_topn_filter.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"
#include "src/observer/ob_server.h"

namespace oceanbase
{
namespace sql
{

ObPushDownTopNFilter::~ObPushDownTopNFilter()
{
  destroy();
}

void ObPushDownTopNFilter::destroy()
{
  if (pd_topn_filter_msg_ != nullptr) {
    ObP2PDhKey dh_key(pd_topn_filter_msg_->get_p2p_datahub_id(),
                      pd_topn_filter_msg_->get_px_seq_id(),
                      pd_topn_filter_msg_->get_task_id());
    ObP2PDatahubMsgBase *msg = nullptr;
    PX_P2P_DH.erase_msg(dh_key, msg);
    pd_topn_filter_msg_->destroy();
    mem_context_->get_malloc_allocator().free(pd_topn_filter_msg_);
    pd_topn_filter_msg_ = nullptr;
  }
  enabled_ = false;
  need_update_ = false;
  msg_set_ = false;
  pd_topn_filter_info_ = nullptr;
  topn_filter_ctx_ = nullptr;
}

inline int ObPushDownTopNFilter::init(const ObSortVecOpContext &ctx,
                                      lib::MemoryContext &mem_context)
{
  return init(ctx.pd_topn_filter_info_, ctx.tenant_id_, ctx.sk_collations_, ctx.exec_ctx_,
              mem_context, true /*use_rich_format*/);
}

int ObPushDownTopNFilter::init(const ObPushDownTopNFilterInfo *pd_topn_filter_info,
                               uint64_t tenant_id,
                               const ObIArray<ObSortFieldCollation> *sort_collations,
                               ObExecContext *exec_ctx, lib::MemoryContext &mem_context,
                               bool use_rich_format /*=false*/)
{
  int ret = OB_SUCCESS;
  ObP2PDatahubMsgBase *p2p_msg = nullptr;
  ObPushDownTopNFilterMsg *pd_topn_filter_msg = nullptr;
  mem_context_ = mem_context;
  pd_topn_filter_info_ = pd_topn_filter_info;

  ObPxSqcHandler *sqc_handler = exec_ctx->get_sqc_handler();
  int64_t px_seq_id = 0;
  if (nullptr == sqc_handler) {
    // For local plans that do not involve a PX coordinator, it's necessary to simulate a px seq id.
    // Failing to do so means that different SQL queries could access the same plan cache, resulting
    // in identical ObP2PDhKey values. Consequently, these queries would receive the same TOP-N
    // pushdown runtime filter message from a single SQL query, which is a hazardous behavior.
    px_seq_id = GCTX.sql_engine_->get_px_sequence_id();
  } else {
    ObPxSqcMeta &sqc = sqc_handler->get_sqc_init_arg().sqc_;
    px_seq_id = sqc.get_interrupt_id().px_interrupt_id_.first_;
  }

  if (OB_FAIL(PX_P2P_DH.alloc_msg(mem_context_->get_malloc_allocator(),
                                  pd_topn_filter_info_->dh_msg_type_, p2p_msg))) {
    LOG_WARN("fail to alloc msg", K(ret));
  } else if (FALSE_IT(pd_topn_filter_msg = static_cast<ObPushDownTopNFilterMsg *>(p2p_msg))) {
  } else if (OB_FAIL(pd_topn_filter_msg->init(pd_topn_filter_info, tenant_id, sort_collations,
                                              exec_ctx, px_seq_id))) {
    LOG_WARN("failed to init pushdown topn filter msg");
  } else if (!pd_topn_filter_info_->is_shuffle_
             && OB_FAIL(create_pd_topn_filter_ctx(pd_topn_filter_info, exec_ctx, use_rich_format,
                                                  px_seq_id))) {
    // for local topn filter pushdown, we directly create filter_ctx here;
    // for global topn filter pushdown, we need add a topn filter use operator and
    // create filter_ctx in topn filter use operator;
    LOG_WARN("failed to create_pd_topn_filter_ctx");
  } else {
    pd_topn_filter_msg_ = pd_topn_filter_msg;
    enabled_ = true;
  }

  LOG_TRACE("[TopN Filter] init topn filter msg");
  return ret;
}

int ObPushDownTopNFilter::update_filter_data(ObCompactRow *compact_row, const RowMeta *row_meta_)
{
  int ret = OB_SUCCESS;
  bool is_updated = false;
  if (OB_FAIL(pd_topn_filter_msg_->update_filter_data(compact_row, row_meta_, is_updated))) {
    LOG_WARN("failed to update filter data", K(ret));
  } else if (FALSE_IT(set_need_update(false))) {
  } else if (is_updated && OB_FAIL(publish_topn_msg())) {
    LOG_WARN("failed to publish topn msg");
  }
  return ret;
}

int ObPushDownTopNFilter::update_filter_data(ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  bool is_updated = false;
  if (OB_FAIL(pd_topn_filter_msg_->update_filter_data(store_row, is_updated))) {
    LOG_WARN("failed to update filter data", K(ret));
  } else if (FALSE_IT(set_need_update(false))) {
  } else if (is_updated && OB_FAIL(publish_topn_msg())) {
    LOG_WARN("failed to publish topn msg");
  }
  return ret;
}

int ObPushDownTopNFilter::create_pd_topn_filter_ctx(
    const ObPushDownTopNFilterInfo *pd_topn_filter_info, ObExecContext *exec_ctx,
    bool use_rich_format, int64_t px_seq_id)
{
  int ret = OB_SUCCESS;
  // TODO XUNSI: if pushdown to the right table with join key but not sort key not from right table,
  // prepare the compare info for it
  uint32_t expr_ctx_id = pd_topn_filter_info->expr_ctx_id_;
  ObExprTopNFilterContext *topn_filter_ctx =
      static_cast<ObExprTopNFilterContext *>(exec_ctx->get_expr_op_ctx(expr_ctx_id));
  ObPxSqcHandler *sqc_handler = exec_ctx->get_sqc_handler();
  int64_t task_id = 0;
  if (nullptr == sqc_handler) {
  } else {
    // PX plan, but non shared topn filter, each thread has its own id
    task_id = exec_ctx->get_px_task_id();
  }

  if (nullptr == topn_filter_ctx) {
    if (OB_FAIL(exec_ctx->create_expr_op_ctx(expr_ctx_id, topn_filter_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(expr_ctx_id));
    } else {
      topn_filter_ctx_ = topn_filter_ctx;
      ObP2PDhKey dh_key(pd_topn_filter_info->p2p_dh_id_, px_seq_id, task_id);
      topn_filter_ctx->topn_filter_key_ = dh_key;
      topn_filter_ctx->slide_window_.set_adptive_ratio_thresheld(
          pd_topn_filter_info->adaptive_filter_ratio_);
      if (use_rich_format) {
        int64_t max_batch_size = pd_topn_filter_info->max_batch_size_;
        void *buf = nullptr;
        if (OB_ISNULL(buf = exec_ctx->get_allocator().alloc((sizeof(uint16_t) * max_batch_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate selector_", K(max_batch_size));
        } else {
          topn_filter_ctx->row_selector_ = static_cast<uint16_t *>(buf);
        }
      }
    }
  } else {
    // topn_filter_ctx already exists means it is in rescan process
    topn_filter_ctx_ = topn_filter_ctx;
    if (nullptr == sqc_handler) {
      // for none px plan rescan, the px_seq_id will be regenerated
      // so the dh_key should be updated
      ObP2PDhKey dh_key(pd_topn_filter_info->p2p_dh_id_, px_seq_id, task_id);
      topn_filter_ctx_->topn_filter_key_ = dh_key;
    }
  }
  return ret;
}

int ObPushDownTopNFilter::publish_topn_msg()
{
  int ret = OB_SUCCESS;
  if (pd_topn_filter_info_->is_shuffle_) {
    // shuffled, not need set to local p2pmap.
    // TODO XUNSI: impl global topn filter
  } else if (!msg_set_) {
    pd_topn_filter_msg_->set_is_ready(true);
    if (OB_FAIL(PX_P2P_DH.send_local_p2p_msg(*pd_topn_filter_msg_))) {
      LOG_WARN("fail to send local p2p msg", K(ret));
    } else {
      msg_set_ = true;
      if (topn_filter_ctx_) {
        LOG_TRACE("[TopN Filter] success to set msg to local p2p datahub", K(pd_topn_filter_msg_),
                  K(topn_filter_ctx_->topn_filter_key_), K(topn_filter_ctx_->total_count_),
                  K(topn_filter_ctx_->check_count_), K(topn_filter_ctx_->filter_count_));
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
