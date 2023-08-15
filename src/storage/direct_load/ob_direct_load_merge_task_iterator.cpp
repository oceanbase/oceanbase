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

#include "storage/direct_load/ob_direct_load_merge_task_iterator.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadMergeTaskIterator::ObDirectLoadMergeTaskIterator()
  : merge_ctx_(nullptr),
    tablet_merge_ctx_(nullptr),
    tablet_pos_(0),
    task_pos_(0),
    is_inited_(false)
{
}

ObDirectLoadMergeTaskIterator::~ObDirectLoadMergeTaskIterator()
{
}

int ObDirectLoadMergeTaskIterator::init(ObDirectLoadMergeCtx *merge_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMergeTaskIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == merge_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(merge_ctx));
  } else {
    merge_ctx_ = merge_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMergeTaskIterator::get_next_task(ObDirectLoadPartitionMergeTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMergeTaskIterator not init", KR(ret), KP(this));
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
        const ObIArray<ObDirectLoadPartitionMergeTask *> &tasks = tablet_merge_ctx_->get_tasks();
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
