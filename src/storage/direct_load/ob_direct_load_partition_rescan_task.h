/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "storage/direct_load/ob_direct_load_i_merge_task.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadPartitionRescanTask : public ObDirectLoadIMergeTask
{
public:
  ObDirectLoadPartitionRescanTask();
  virtual ~ObDirectLoadPartitionRescanTask() = default;
  int init(ObDirectLoadTabletMergeCtx *merge_ctx, int64_t thread_cnt, int64_t thread_idx);
  int process() override;
  void stop() override;
  int init_iterator(ObITabletSliceRowIterator *&row_iterator) override { return OB_NOT_IMPLEMENT;}
  ObDirectLoadTabletMergeCtx *get_merge_ctx() override { return merge_ctx_; }
  TO_STRING_KV(KP_(merge_ctx), K_(thread_cnt), K_(thread_idx));

private:
  ObDirectLoadTabletMergeCtx *merge_ctx_;
  int64_t thread_cnt_;
  int64_t thread_idx_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
