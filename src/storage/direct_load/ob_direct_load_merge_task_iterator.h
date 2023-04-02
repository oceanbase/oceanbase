// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_merge_ctx.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadMergeTaskIterator
{
public:
  ObDirectLoadMergeTaskIterator();
  ~ObDirectLoadMergeTaskIterator();
  int init(storage::ObDirectLoadMergeCtx *merge_ctx);
  int get_next_task(storage::ObDirectLoadPartitionMergeTask *&task);
private:
  storage::ObDirectLoadMergeCtx *merge_ctx_;
  storage::ObDirectLoadTabletMergeCtx *tablet_merge_ctx_;
  int64_t tablet_pos_;
  int64_t task_pos_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
