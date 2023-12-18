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

#include "lib/list/ob_dlist.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadPartitionRescanTask : public common::ObDLinkBase<ObDirectLoadPartitionRescanTask>
{
public:
  ObDirectLoadPartitionRescanTask()
     : thread_cnt_(0),
       thread_idx_(-1),
       is_inited_(false)
  {
  }
  int init(const ObDirectLoadMergeParam &merge_param,
           ObDirectLoadTabletMergeCtx *merge_ctx,
           int64_t thread_cnt,
           int64_t thread_idx);
  int process();
  void stop();
  TO_STRING_KV(K_(thread_cnt), K_(thread_idx));
private:
  const ObDirectLoadMergeParam *merge_param_;
  ObDirectLoadTabletMergeCtx *merge_ctx_;
  int64_t thread_cnt_;
  int64_t thread_idx_;
  bool is_inited_;
};

class ObDirectLoadRescanTaskIterator
{
public:
  ObDirectLoadRescanTaskIterator();
  ~ObDirectLoadRescanTaskIterator();
  int init(storage::ObDirectLoadMergeCtx *merge_ctx);
  int get_next_task(ObDirectLoadPartitionRescanTask *&task);
private:
  storage::ObDirectLoadMergeCtx *merge_ctx_;
  storage::ObDirectLoadTabletMergeCtx *tablet_merge_ctx_;
  int64_t tablet_pos_;
  int64_t task_pos_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
