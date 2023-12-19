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

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;


int ObDirectLoadPartitionRescanTask::init(const ObDirectLoadMergeParam &merge_param,
           ObDirectLoadTabletMergeCtx *merge_ctx,
           int64_t thread_cnt,
           int64_t thread_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionRescanTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || thread_cnt <= 0 || thread_idx < 0 || thread_idx > thread_cnt - 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(merge_param), K(thread_cnt), K(thread_idx));
  } else {
    merge_param_ = &merge_param;
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
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    const ObTabletID &tablet_id = merge_ctx_->get_tablet_id();
    if (OB_FAIL(merge_param_->insert_table_ctx_->get_tablet_context(
                  tablet_id, tablet_ctx))) {
      LOG_WARN("fail to get tablet context ", KR(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_ctx->fill_column_group(thread_cnt_, thread_idx_))) {
      LOG_WARN("fail to close writer", KR(ret));
    }
    if (OB_SUCC(ret)) {
      bool is_ready = false;
      if (OB_FAIL(merge_ctx_->inc_rescan_finish_count(is_ready))) {
        LOG_WARN("fail to inc finish count", KR(ret));
      } else if (is_ready) {
        if (OB_FAIL(tablet_ctx->close())) {
          LOG_WARN("fail to notify tablet finish", KR(ret));
        }
      }
    }
  }
  return ret;
}

void ObDirectLoadPartitionRescanTask::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRescanTask not init", KR(ret), KP(this));
  } else {
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    const ObTabletID &tablet_id = merge_ctx_->get_tablet_id();
    if (OB_FAIL(merge_param_->insert_table_ctx_->get_tablet_context(
                  tablet_id, tablet_ctx))) {
      LOG_WARN("fail to get tablet context ", KR(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_ctx->cancel())) {
      LOG_WARN("fail to cancel fill column group", K(ret));
    }
  }
  //ignore ret
}


ObDirectLoadRescanTaskIterator::ObDirectLoadRescanTaskIterator()
  : merge_ctx_(nullptr),
    tablet_merge_ctx_(nullptr),
    tablet_pos_(0),
    task_pos_(0),
    is_inited_(false)
{
}

ObDirectLoadRescanTaskIterator::~ObDirectLoadRescanTaskIterator()
{
}

int ObDirectLoadRescanTaskIterator::init(ObDirectLoadMergeCtx *merge_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadRescanTaskIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == merge_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(merge_ctx));
  } else {
    merge_ctx_ = merge_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadRescanTaskIterator::get_next_task(ObDirectLoadPartitionRescanTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadRescanTaskIterator not init", KR(ret), KP(this));
  } else {
    while (OB_SUCC(ret) && nullptr == task) {
      if (nullptr == tablet_merge_ctx_) {
        // get next partition merge ctx
        const ObIArray<ObDirectLoadTabletMergeCtx *> &tablet_merge_ctxs =
          merge_ctx_->get_tablet_merge_ctxs();
        if (tablet_pos_ >= tablet_merge_ctxs.count()) {
          ret = OB_ITER_END;
        } else {
          tablet_merge_ctx_ = tablet_merge_ctxs.at(tablet_pos_++);
          task_pos_ = 0;
        }
      }
      if (OB_SUCC(ret)) {
        const ObIArray<ObDirectLoadPartitionRescanTask *> &tasks = tablet_merge_ctx_->get_rescan_tasks();
        if (task_pos_ >= tasks.count()) {
          // try next partition
          tablet_merge_ctx_ = nullptr;
        } else {
          task = tasks.at(task_pos_++);
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
