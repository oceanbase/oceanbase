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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_merge_del_lob_op.h"
#include "observer/table_load/ob_table_load_lob_row_delete_handler.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

ObTableLoadMergeDelLobOp::ObTableLoadMergeDelLobOp(ObTableLoadMergeTableBaseOp *parent)
  : ObTableLoadMergeTableOp(parent)
{
}

int ObTableLoadMergeDelLobOp::inner_init()
{
  int ret = OB_SUCCESS;
  ObTableLoadStoreLobTableCtx *store_table_ctx = store_ctx_->data_store_table_ctx_->lob_table_ctx_;
  // 重设merge_table_ctx_
  inner_ctx_.store_table_ctx_ = store_table_ctx;
  inner_ctx_.insert_table_ctx_ = store_table_ctx->insert_table_ctx_;
  if (ObTableLoadMergerPhaseType::INSERT == merge_phase_ctx_->phase_ ||
      ObTableLoadMergerPhaseType::DELETE == merge_phase_ctx_->phase_) {
    inner_ctx_.table_store_ = &(store_table_ctx->delete_table_store_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge phase", KR(ret), K(merge_phase_ctx_->phase_));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(inner_ctx_.dml_row_handler_ =
                    OB_NEWx(ObTableLoadLobRowDeleteHandler, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadLobRowDeleteHandler", KR(ret));
    }
  }
  inner_ctx_.merge_mode_ = ObDirectLoadMergeMode::MERGE_WITH_ORIGIN_QUERY_FOR_LOB;
  inner_ctx_.use_batch_mode_ = false;
  inner_ctx_.need_calc_range_ = false;
  inner_ctx_.need_close_insert_tablet_ctx_ = true;
  inner_ctx_.is_del_lob_ = true;
  merge_table_ctx_ = &inner_ctx_;
  return ret;
}

int ObTableLoadMergeDelLobOp::inner_close()
{
  int ret = OB_SUCCESS;
  inner_ctx_.table_store_->clear();
  return ret;
}

} // namespace observer
} // namespace oceanbase
