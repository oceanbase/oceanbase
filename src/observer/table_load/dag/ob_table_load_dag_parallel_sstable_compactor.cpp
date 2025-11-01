/**
 * Copyright (c) 2025 OceanBase
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

#include "observer/table_load/dag/ob_table_load_dag_parallel_sstable_compactor.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "storage/direct_load/ob_direct_load_table_store.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace lib;

/**
 * ObTableLoadDagParallelCompactTabletCtx
 */

ObTableLoadDagParallelCompactTabletCtx::ObTableLoadDagParallelCompactTabletCtx()
  : merge_sstable_count_(0),
    range_count_(0),
    range_sstable_count_(0),
    range_allocator_("TLD_ParalMerge")
{
  range_allocator_.set_tenant_id(MTL_ID());
  ranges_.set_tenant_id(MTL_ID());
  range_sstables_.set_tenant_id(MTL_ID());
  old_sstables_.set_tenant_id(MTL_ID());
}

ObTableLoadDagParallelCompactTabletCtx::~ObTableLoadDagParallelCompactTabletCtx()
{
  sstables_.reset();
  range_sstables_.reset();
  old_sstables_.reset();
}

int ObTableLoadDagParallelCompactTabletCtx::set_parallel_merge_param(int64_t merge_sstable_count,
                                                                  int64_t range_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merge_sstable_count <= 0 || range_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_sstable_count), K(range_count));
  } else if (OB_UNLIKELY(merge_sstable_count_ > 0 || range_count_ > 0 || range_sstable_count_ > 0 ||
                         ranges_.empty() || ranges_.count() != range_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet ctx", KR(ret), KPC(this));
  } else {
    merge_sstable_count_ = merge_sstable_count;
    range_count_ = range_count;
    range_sstable_count_ = 0;
    range_sstables_.reset();
    if (OB_FAIL(range_sstables_.prepare_allocate(range_count))) {
      LOG_WARN("fail to prepare allocate array", KR(ret));
    }
  }
  return ret;
}

/**
 * ObTableLoadDagSSTableCompare
 */

ObTableLoadDagSSTableCompare::ObTableLoadDagSSTableCompare() : result_code_(OB_SUCCESS) {}

ObTableLoadDagSSTableCompare::~ObTableLoadDagSSTableCompare() {}

bool ObTableLoadDagSSTableCompare::operator()(const ObDirectLoadTableHandle lhs,
                                           const ObDirectLoadTableHandle rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(lhs), K(rhs));
  } else {
    ObDirectLoadMultipleSSTable *lhs_multi_sstable =
      static_cast<ObDirectLoadMultipleSSTable *>(lhs.get_table());
    ObDirectLoadMultipleSSTable *rhs_multi_sstable =
      static_cast<ObDirectLoadMultipleSSTable *>(rhs.get_table());
    cmp_ret = lhs_multi_sstable->get_meta().row_count_ - rhs_multi_sstable->get_meta().row_count_;
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  }
  return cmp_ret < 0;
}

/**
 * ObTableLoadDagParallelSSTableCompactor
 */

ObTableLoadDagParallelSSTableCompactor::ObTableLoadDagParallelSSTableCompactor()
  : store_ctx_(nullptr),
    op_ctx_(nullptr),
    allocator_("TLD_ParalSSTC"),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadDagParallelSSTableCompactor::~ObTableLoadDagParallelSSTableCompactor()
{
  FOREACH(it, tablet_ctx_map_)
  {
    ObTableLoadDagParallelCompactTabletCtx *tablet_ctx = it->second;
    tablet_ctx->~ObTableLoadDagParallelCompactTabletCtx();
    allocator_.free(tablet_ctx);
  }
  tablet_ctx_map_.destroy();
}

int ObTableLoadDagParallelSSTableCompactor::init(ObTableLoadStoreCtx *store_ctx,
                                              ObTableLoadTableOpCtx *op_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDagParallelSSTableCompactor init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(store_ctx == nullptr || op_ctx == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx), KP(op_ctx));
  } else {
    store_ctx_ = store_ctx;
    op_ctx_ = op_ctx;
    if (OB_FAIL(tablet_ctx_map_.create(1024, "TLD_CptCtxMap", "TLD_CptCtxMap", MTL_ID()))) {
      LOG_WARN("fail to create ctx map", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDagParallelSSTableCompactor::prepare_compact()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagParallelSSTableCompactor not init", KR(ret), KP(this));
  } else {
    ObDirectLoadTableStore *table_store = &op_ctx_->table_store_;
    // 根据table_store构造tablet_ctx
    FOREACH_X(it, *table_store, OB_SUCC(ret))
    {
      const ObTabletID &tablet_id = it->first;
      ObDirectLoadTableHandleArray *table_handle_array = it->second;
      ObTableLoadDagParallelCompactTabletCtx *tablet_ctx = nullptr;
      if (OB_ISNULL(tablet_ctx = OB_NEWx(ObTableLoadDagParallelCompactTabletCtx, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObTableLoadDagParallelCompactTabletCtx", KR(ret));
      } else if (OB_FAIL(tablet_ctx->sstables_.assign(*table_handle_array))) {
        LOG_WARN("fail to assign table handle array", KR(ret));
      } else if (FALSE_IT(tablet_ctx->tablet_id_ = tablet_id)) {
      } else if (OB_FAIL(tablet_ctx_map_.set_refactored(tablet_id, tablet_ctx))) {
        LOG_WARN("fail to set refactored", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != tablet_ctx) {
          tablet_ctx->~ObTableLoadDagParallelCompactTabletCtx();
          allocator_.free(tablet_ctx);
          tablet_ctx = nullptr;
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_store->clear();
    }
  }
  return ret;
}

int ObTableLoadDagParallelSSTableCompactor::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagParallelSSTableCompactor not init", KR(ret), KP(this));
  } else {
    ObDirectLoadTableStore &table_store = op_ctx_->table_store_;
    table_store.clear();
    table_store.set_multiple_sstable();
    FOREACH_X(it, tablet_ctx_map_, OB_SUCC(ret))
    {
      const ObTabletID &tablet_id = it->first;
      ObTableLoadDagParallelCompactTabletCtx *tablet_ctx = it->second;
      if (OB_FAIL(table_store.add_tablet_tables(tablet_id, tablet_ctx->sstables_))) {
        LOG_WARN("fail to add tables", KR(ret));
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
