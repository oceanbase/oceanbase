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

#include "observer/table_load/ob_table_load_merge_compact_table_op.h"
#include "observer/table_load/ob_table_load_parallel_table_compactor.h"
#include "storage/direct_load/ob_direct_load_table_store.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace storage;

ObTableLoadMergeCompactTableOp::ObTableLoadMergeCompactTableOp(ObTableLoadMergeTableBaseOp *parent)
  : ObTableLoadMergeTableBaseOp(parent), parallel_table_compactor_(nullptr)
{
}

ObTableLoadMergeCompactTableOp::~ObTableLoadMergeCompactTableOp()
{
  if (nullptr != parallel_table_compactor_) {
    parallel_table_compactor_->~ObTableLoadParallelTableCompactor();
    allocator_->free(parallel_table_compactor_);
    parallel_table_compactor_ = nullptr;
  }
}

void ObTableLoadMergeCompactTableOp::stop()
{
  if (nullptr != parallel_table_compactor_) {
    parallel_table_compactor_->stop();
  }
}

int ObTableLoadMergeCompactTableOp::switch_next_op(bool is_parent_called)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_parent_called)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is not parent called", KR(ret), K(lbt()));
  } else if (OB_ISNULL(merge_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge table ctx is null", KR(ret), KP(merge_table_ctx_));
  } else if (OB_UNLIKELY(merge_table_ctx_->table_store_->empty() ||
                         !merge_table_ctx_->table_store_->is_multiple_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table store", KR(ret), KPC(merge_table_ctx_->table_store_));
  } else {
    if (OB_ISNULL(parallel_table_compactor_ =
                    OB_NEWx(ObTableLoadParallelTableCompactor, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadParallelTableCompactor", KR(ret));
    } else if (OB_FAIL(parallel_table_compactor_->init(this))) {
      LOG_WARN("fail to init parallel table compactor", KR(ret));
    } else if (OB_FAIL(parallel_table_compactor_->start())) {
      LOG_WARN("fail to start parallel table compactor", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMergeCompactTableOp::on_success()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(switch_parent_op())) {
    LOG_WARN("fail to switch parent op", KR(ret));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
