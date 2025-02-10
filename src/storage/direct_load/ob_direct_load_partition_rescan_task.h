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
