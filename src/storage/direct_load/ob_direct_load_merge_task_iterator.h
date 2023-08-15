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

#include "storage/blocksstable/ob_sstable.h"
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
