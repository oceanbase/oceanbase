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

#include "observer/table_load/ob_table_load_merge_insert_sstable_op.h"
#include "observer/table_load/ob_table_load_parallel_merger.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "storage/direct_load/ob_direct_load_table_store.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace storage;

ObTableLoadMergeInsertSSTableOp::ObTableLoadMergeInsertSSTableOp(
  ObTableLoadMergeTableBaseOp *parent)
  : ObTableLoadMergeTableBaseOp(parent), parallel_merger_(nullptr)
{
}

ObTableLoadMergeInsertSSTableOp::~ObTableLoadMergeInsertSSTableOp()
{
  if (nullptr != parallel_merger_) {
    parallel_merger_->~ObTableLoadParallelMerger();
    allocator_->free(parallel_merger_);
    parallel_merger_ = nullptr;
  }
}

void ObTableLoadMergeInsertSSTableOp::stop()
{
  if (nullptr != parallel_merger_) {
    parallel_merger_->stop();
  }
}

int ObTableLoadMergeInsertSSTableOp::switch_next_op(bool is_parent_called)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_parent_called)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is not parent called", KR(ret), K(lbt()));
  } else if (OB_ISNULL(merge_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge table ctx is null", KR(ret), KP(merge_table_ctx_));
  } else if (OB_ISNULL(parallel_merger_ = OB_NEWx(ObTableLoadParallelMerger, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadParallelMerger", KR(ret));
  } else if (OB_FAIL(parallel_merger_->init_merge_task(this))) {
    LOG_WARN("fail to init merge task", KR(ret));
  } else if (OB_FAIL(parallel_merger_->start())) {
    LOG_WARN("fail to start parallel merge", KR(ret));
  }
  return ret;
}

int ObTableLoadMergeInsertSSTableOp::on_success()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(switch_parent_op())) {
    LOG_WARN("fail to switch parent op", KR(ret));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
