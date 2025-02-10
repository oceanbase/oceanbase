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

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_partition_rescan_task.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadPartitionRescanTask::ObDirectLoadPartitionRescanTask()
  : merge_ctx_(nullptr), thread_cnt_(0), thread_idx_(-1), is_inited_(false)
{
}

int ObDirectLoadPartitionRescanTask::init(ObDirectLoadTabletMergeCtx *merge_ctx, int64_t thread_cnt,
                                          int64_t thread_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionRescanTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == merge_ctx || !merge_ctx->is_valid() || thread_cnt <= 0 ||
                         thread_idx < 0 || thread_idx >= thread_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KPC(merge_ctx), K(thread_cnt), K(thread_idx));
  } else {
    merge_ctx_ = merge_ctx;
    thread_cnt_ = thread_cnt;
    thread_idx_ = thread_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionRescanTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRescanTask not init", KR(ret), KP(this));
  } else {
    ObDirectLoadInsertTabletContext *insert_tablet_ctx = merge_ctx_->get_insert_tablet_ctx();
    if (OB_FAIL(insert_tablet_ctx->fill_column_group(thread_cnt_, thread_idx_))) {
      LOG_WARN("fail to fill column group", KR(ret));
    }
  }
  return ret;
}

void ObDirectLoadPartitionRescanTask::stop()
{
  if (nullptr != merge_ctx_) {
    ObDirectLoadInsertTabletContext *insert_tablet_ctx = merge_ctx_->get_insert_tablet_ctx();
    insert_tablet_ctx->cancel();
  }
}

} // namespace storage
} // namespace oceanbase
