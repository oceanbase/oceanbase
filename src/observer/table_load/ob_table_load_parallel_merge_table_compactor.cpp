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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_parallel_merge_table_compactor.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace blocksstable;

/**
 * ParallelMergeCb
 */

ObTableLoadParallelMergeTableCompactor::ParallelMergeCb::ParallelMergeCb()
  : table_compactor_(nullptr)
{
}

int ObTableLoadParallelMergeTableCompactor::ParallelMergeCb::init(
  ObTableLoadParallelMergeTableCompactor *table_compactor)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table_compactor_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ParallelMergeCb init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_compactor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_compactor));
  } else {
    table_compactor_ = table_compactor;
  }
  return ret;
}

int ObTableLoadParallelMergeTableCompactor::ParallelMergeCb::on_success()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_compactor_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ParallelMergeCb not init", KR(ret), KP(this));
  } else if (OB_FAIL(table_compactor_->handle_parallel_merge_success())) {
    LOG_WARN("fail to handle parallel merge success", KR(ret));
  }
  return ret;
}

/**
 * ObTableLoadParallelMergeTableCompactor
 */

ObTableLoadParallelMergeTableCompactor::ObTableLoadParallelMergeTableCompactor()
{
}

ObTableLoadParallelMergeTableCompactor::~ObTableLoadParallelMergeTableCompactor()
{
}

int ObTableLoadParallelMergeTableCompactor::inner_init()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(parallel_merge_ctx_.init(compact_ctx_->store_ctx_))) {
    LOG_WARN("fail to init parallel merge ctx", KR(ret));
  } else if (OB_FAIL(parallel_merge_cb_.init(this))) {
    LOG_WARN("fail to init parallel merge cb", KR(ret));
  }
  return ret;
}

int ObTableLoadParallelMergeTableCompactor::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadParallelMergeTableCompactor not init", KR(ret), KP(this));
  } else {
    ObSEArray<ObTableLoadTransStore *, 64> trans_store_array;
    if (OB_FAIL(compact_ctx_->store_ctx_->get_committed_trans_stores(trans_store_array))) {
      LOG_WARN("fail to get committed trans stores", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_store_array.count(); ++i) {
      ObTableLoadTransStore *trans_store = trans_store_array.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < trans_store->session_store_array_.count(); ++j) {
        const ObTableLoadTransStore::SessionStore *session_store =
          trans_store->session_store_array_.at(j);
        for (int64_t k = 0; OB_SUCC(ret) && k < session_store->partition_table_array_.count();
             ++k) {
          ObIDirectLoadPartitionTable *table = session_store->partition_table_array_.at(k);
          ObDirectLoadMultipleSSTable *sstable = nullptr;
          if (OB_ISNULL(sstable = dynamic_cast<ObDirectLoadMultipleSSTable *>(table))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected table", KR(ret), KPC(table));
          } else if (OB_FAIL(parallel_merge_ctx_.add_tablet_sstable(sstable))) {
            LOG_WARN("fail to add tablet sstable", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      compact_ctx_->store_ctx_->clear_committed_trans_stores();
      if (OB_FAIL(parallel_merge_ctx_.start(&parallel_merge_cb_))) {
        LOG_WARN("fail to start parallel merge", KR(ret));
      }
    }
  }
  return ret;
}

void ObTableLoadParallelMergeTableCompactor::stop()
{
  parallel_merge_ctx_.stop();
}

int ObTableLoadParallelMergeTableCompactor::handle_parallel_merge_success()
{
  int ret = OB_SUCCESS;
  if (build_result()) {
    LOG_WARN("fail to build result", KR(ret));
  } else if (OB_FAIL(compact_ctx_->handle_table_compact_success())) {
    LOG_WARN("fail to notify table compact success", KR(ret));
  }
  return ret;
}

int ObTableLoadParallelMergeTableCompactor::build_result()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCompactResult &result = compact_ctx_->result_;
  // get tables from tablet ctx
  const ObTableLoadParallelMergeCtx::TabletCtxMap &tablet_ctx_map =
    parallel_merge_ctx_.get_tablet_ctx_map();
  for (ObTableLoadParallelMergeCtx::TabletCtxIterator tablet_ctx_iter = tablet_ctx_map.begin();
       OB_SUCC(ret) && tablet_ctx_iter != tablet_ctx_map.end(); ++tablet_ctx_iter) {
    ObTableLoadParallelMergeTabletCtx *tablet_ctx = tablet_ctx_iter->second;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ctx->sstables_.size(); ++i) {
      ObDirectLoadMultipleSSTable *sstable = tablet_ctx->sstables_.at(i);
      ObDirectLoadMultipleSSTable *copied_sstable = nullptr;
      if (OB_ISNULL(copied_sstable = OB_NEWx(ObDirectLoadMultipleSSTable, (&result.allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadMultipleSSTable", KR(ret));
      } else if (OB_FAIL(copied_sstable->copy(*sstable))) {
        LOG_WARN("fail to copy multiple sstable", KR(ret));
      } else if (OB_FAIL(result.add_table(copied_sstable))) {
        LOG_WARN("fail to add table", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != copied_sstable) {
          copied_sstable->~ObDirectLoadMultipleSSTable();
          result.allocator_.free(copied_sstable);
        }
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
