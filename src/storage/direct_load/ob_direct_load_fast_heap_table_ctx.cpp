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

#include "storage/direct_load/ob_direct_load_fast_heap_table_ctx.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "share/ob_tablet_autoincrement_service.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace lib;
using namespace share;
using namespace table;

ObDirectLoadFastHeapTableContext::ObDirectLoadFastHeapTableContext()
  : allocator_("TLD_FHTableCtx"), is_inited_(false)
{
}

ObDirectLoadFastHeapTableContext::~ObDirectLoadFastHeapTableContext()
{
  for (TABLET_CTX_MAP::iterator iter = tablet_ctx_map_.begin(); iter != tablet_ctx_map_.end();
       ++iter) {
    ObDirectLoadFastHeapTableTabletContext *tablet_ctx = iter->second;
    tablet_ctx->~ObDirectLoadFastHeapTableTabletContext();
    allocator_.free(tablet_ctx);
  }
  tablet_ctx_map_.reuse();
}

int ObDirectLoadFastHeapTableContext::init(uint64_t tenant_id,
                                           const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
                                           const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids,
                                           int64_t reserved_parallel)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadFastHeapTableContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || ls_partition_ids.empty()
                         || (ls_partition_ids.count() != target_ls_partition_ids.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(ls_partition_ids), K(target_ls_partition_ids));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    if (OB_FAIL(create_all_tablet_contexts(tenant_id, ls_partition_ids, target_ls_partition_ids, reserved_parallel))) {
      LOG_WARN("fail to create all tablet contexts", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadFastHeapTableContext::create_all_tablet_contexts(
    uint64_t tenant_id,
    const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
    const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids,
    int64_t reserved_parallel)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ls_partition_ids.empty()
                  || target_ls_partition_ids.empty()
                  || (ls_partition_ids.count() != target_ls_partition_ids.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ls_partition_ids), K(target_ls_partition_ids));
  } else if (OB_FAIL(
               tablet_ctx_map_.create(ls_partition_ids.count(), "TLD_TabInsCtx", "TLD_TabInsCtx", MTL_ID()))) {
    LOG_WARN("fail to create tablet ctx map", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
      const ObTabletID &tablet_id = ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
      const ObTabletID &target_tablet_id = target_ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
      ObDirectLoadFastHeapTableTabletContext *tablet_ctx = nullptr;
      if (OB_ISNULL(tablet_ctx = OB_NEWx(ObDirectLoadFastHeapTableTabletContext, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadFastHeapTableTabletContext", KR(ret));
      } else if (OB_FAIL(tablet_ctx->init(tenant_id, tablet_id, target_tablet_id, reserved_parallel))) {
        LOG_WARN("fail to init fast heap table tablet ctx", KR(ret));
      } else if (OB_FAIL(tablet_ctx_map_.set_refactored(tablet_id, tablet_ctx))) {
        LOG_WARN("fail to set tablet ctx map", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != tablet_ctx) {
          tablet_ctx->~ObDirectLoadFastHeapTableTabletContext();
          allocator_.free(tablet_ctx);
          tablet_ctx = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadFastHeapTableContext::get_tablet_context(
  const ObTabletID &tablet_id, ObDirectLoadFastHeapTableTabletContext *&tablet_ctx) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadFastHeapTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_ctx_map_.get_refactored(tablet_id, tablet_ctx))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
      LOG_WARN("fail to get tablet ctx map", KR(ret), K(tablet_id));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

/**
 * ObDirectLoadFastHeapTableTabletContext
 */

ObDirectLoadFastHeapTableTabletContext::ObDirectLoadFastHeapTableTabletContext()
  : tenant_id_(OB_INVALID_ID), is_inited_(false)
{
}

int ObDirectLoadFastHeapTableTabletContext::init(uint64_t tenant_id,
    const ObTabletID &tablet_id, const ObTabletID &target_tablet_id, int64_t reserved_parallel)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadFastHeapTableTabletContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                         || !tablet_id.is_valid()
                         || !target_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(tablet_id), K(target_tablet_id));
  } else {
    tenant_id_ = tenant_id;
    tablet_id_ = tablet_id;
    target_tablet_id_ = target_tablet_id;
    start_seq_.set_parallel_degree(reserved_parallel);
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadFastHeapTableTabletContext::get_write_ctx(
  ObDirectLoadFastHeapTableTabletWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadFastHeapTableTabletContext not init", KR(ret), KP(this));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(pk_cache_.fetch(WRITE_BATCH_SIZE, write_ctx.pk_interval_))) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        LOG_WARN("fail to fetch from pk cache", KR(ret));
      } else {
        if (OB_FAIL(refresh_pk_cache())) {
          LOG_WARN("fail to refresh pk cache", KR(ret));
        } else if (OB_FAIL(pk_cache_.fetch(WRITE_BATCH_SIZE, write_ctx.pk_interval_))) {
          LOG_WARN("fail to fetch from pk cache", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      write_ctx.start_seq_.macro_data_seq_ = start_seq_.macro_data_seq_;
      start_seq_.macro_data_seq_ += WRITE_BATCH_SIZE;
    }
  }
  return ret;
}

int ObDirectLoadFastHeapTableTabletContext::refresh_pk_cache()
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, fast_heap_table_refresh_pk_cache);
  int ret = OB_SUCCESS;
  ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
  pk_cache_.tablet_id_ = tablet_id_;
  pk_cache_.cache_size_ = PK_CACHE_SIZE;
  if (OB_FAIL(auto_inc.get_tablet_cache_interval(tenant_id_, pk_cache_))) {
    LOG_WARN("get_autoinc_seq fail", K(ret), K_(tenant_id), K_(tablet_id));
  } else if (OB_UNLIKELY(PK_CACHE_SIZE > pk_cache_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected autoincrement value count", K(ret), K(pk_cache_));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
