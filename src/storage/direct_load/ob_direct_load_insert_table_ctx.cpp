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

#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace table;
using namespace observer;
using namespace blocksstable;
using namespace lib;
using namespace share;

/**
 * ObDirectLoadInsertTableParam
 */

ObDirectLoadInsertTableParam::ObDirectLoadInsertTableParam()
  : table_id_(OB_INVALID_ID),
    schema_version_(0),
    snapshot_version_(0),
    execution_id_(0),
    ddl_task_id_(0),
    data_version_(0),
    reserved_parallel_(0),
    ls_partition_ids_(),
    target_ls_partition_ids_()
{
  ls_partition_ids_.set_attr(ObMemAttr(MTL_ID(), "DLITP_ids"));
  target_ls_partition_ids_.set_attr(ObMemAttr(MTL_ID(), "DLITP_t_ids"));
}

ObDirectLoadInsertTableParam::~ObDirectLoadInsertTableParam()
{
}

bool ObDirectLoadInsertTableParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ && schema_version_ >= 0 && snapshot_version_ >= 0 &&
         execution_id_ >= 0 && ddl_task_id_ > 0 && data_version_ >= 0 && reserved_parallel_ >= 0 &&
         ls_partition_ids_.count() > 0 &&
         ls_partition_ids_.count() == target_ls_partition_ids_.count();
}

int ObDirectLoadInsertTableParam::assign(const ObDirectLoadInsertTableParam &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  schema_version_ = other.schema_version_;
  snapshot_version_ = other.snapshot_version_;
  execution_id_ = other.execution_id_;
  ddl_task_id_ = other.ddl_task_id_;
  data_version_ = other.data_version_;
  reserved_parallel_ = other.reserved_parallel_;
  if (OB_FAIL(ls_partition_ids_.assign(other.ls_partition_ids_))) {
    LOG_WARN("fail to assign ls tablet ids", KR(ret));
  } else if (OB_FAIL(target_ls_partition_ids_.assign(other.target_ls_partition_ids_))) {
    LOG_WARN("fail to assign ls tablet ids", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadInsertTabletParam
 */

ObDirectLoadInsertTabletParam::ObDirectLoadInsertTabletParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    table_id_(OB_INVALID_ID),
    schema_version_(0),
    snapshot_version_(0),
    execution_id_(0),
    ddl_task_id_(0),
    data_version_(0),
    reserved_parallel_(0),
    context_id_(0)
{
}

ObDirectLoadInsertTabletParam::~ObDirectLoadInsertTabletParam() {}

bool ObDirectLoadInsertTabletParam::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid() && OB_INVALID_ID != table_id_ &&
         tablet_id_.is_valid() && origin_tablet_id_.is_valid() && schema_version_ >= 0 &&
         snapshot_version_ >= 0 && execution_id_ >= 0 && ddl_task_id_ > 0 && data_version_ >= 0 &&
         reserved_parallel_ >= 0 && context_id_ >= 0;
}

/**
 * ObDirectLoadInsertTabletContext
 */

ObDirectLoadInsertTabletContext::ObDirectLoadInsertTabletContext()
  : is_open_(false), is_inited_(false)
{
}

ObDirectLoadInsertTabletContext::~ObDirectLoadInsertTabletContext()
{
}

int ObDirectLoadInsertTabletContext::init(const ObDirectLoadInsertTabletParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTabletContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    param_ = param;
    start_seq_.set_parallel_degree(param.reserved_parallel_);
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (is_open_) {
    // do nothing
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (!is_open_) {
      ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
      ObTabletDirectLoadInsertParam direct_load_param;
      direct_load_param.is_replay_ = false;
      direct_load_param.common_param_.direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_LOAD_DATA;
      direct_load_param.common_param_.data_format_version_ = param_.data_version_;
      direct_load_param.common_param_.read_snapshot_ = param_.snapshot_version_;
      direct_load_param.common_param_.ls_id_ = param_.ls_id_;
      direct_load_param.common_param_.tablet_id_ = param_.tablet_id_;
      direct_load_param.runtime_only_param_.exec_ctx_ = nullptr;
      direct_load_param.runtime_only_param_.task_id_ = param_.ddl_task_id_;
      direct_load_param.runtime_only_param_.table_id_ = param_.table_id_;
      direct_load_param.runtime_only_param_.schema_version_ = param_.schema_version_;
      direct_load_param.runtime_only_param_.task_cnt_ = 1; // default value.
      if (OB_FAIL(sstable_insert_mgr->create_tablet_direct_load(
            param_.context_id_, param_.execution_id_, direct_load_param))) {
        LOG_WARN("create tablet manager failed", K(ret));
      } else if (OB_FAIL(sstable_insert_mgr->open_tablet_direct_load(
                   true, param_.ls_id_, param_.tablet_id_, param_.context_id_, start_scn_,
                   handle_))) {
        LOG_WARN("fail to open tablet direct load", KR(ret), K(param_.tablet_id_));
      } else {
        is_open_ = true;
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_open_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected does not open", KR(ret));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->close_tablet_direct_load(
          param_.context_id_, true, param_.ls_id_, param_.tablet_id_, true, true))) {
      LOG_WARN("fail to close tablet direct load", KR(ret), K(param_.ls_id_),
               K(param_.tablet_id_));
    } else {
      is_open_ = false;
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::get_pk_interval(uint64_t count,
                                                     share::ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pk_cache_.fetch(count, pk_interval))) {
    if (OB_UNLIKELY(OB_EAGAIN != ret)) {
      LOG_WARN("fail to fetch from pk cache", KR(ret));
    } else {
      if (OB_FAIL(refresh_pk_cache(param_.origin_tablet_id_, pk_cache_))) {
        LOG_WARN("fail to refresh pk cache", KR(ret));
      } else if (OB_FAIL(pk_cache_.fetch(count, pk_interval))) {
        LOG_WARN("fail to fetch from pk cache", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::get_lob_pk_interval(uint64_t count,
                                                         share::ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lob_pk_cache_.fetch(count, pk_interval))) {
    if (OB_UNLIKELY(OB_EAGAIN != ret)) {
      LOG_WARN("fail to fetch from pk cache", KR(ret));
    } else {
      if (OB_FAIL(refresh_pk_cache(param_.lob_tablet_id_, lob_pk_cache_))) {
        LOG_WARN("fail to refresh pk cache", KR(ret));
      } else if (OB_FAIL(lob_pk_cache_.fetch(count, pk_interval))) {
        LOG_WARN("fail to fetch from pk cache", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::get_write_ctx(ObDirectLoadInsertTabletWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadLobTabletContext not init", KR(ret), KP(this));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(get_pk_interval(WRITE_BATCH_SIZE, write_ctx.pk_interval_))) {
      LOG_WARN("fail to get pk interval", KR(ret), KP(this));
    } else {
      write_ctx.start_seq_.macro_data_seq_ = start_seq_.macro_data_seq_;
      start_seq_.macro_data_seq_ += WRITE_BATCH_SIZE;
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::get_lob_write_ctx(ObDirectLoadInsertTabletWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadLobTabletContext not init", KR(ret), KP(this));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(
          get_lob_pk_interval(WRITE_BATCH_SIZE, write_ctx.pk_interval_))) {
      LOG_WARN("fail to get pk interval", KR(ret), KP(this));
    } else {
      write_ctx.start_seq_.macro_data_seq_ = lob_start_seq_.macro_data_seq_;
      lob_start_seq_.macro_data_seq_ += WRITE_BATCH_SIZE;
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::refresh_pk_cache(const common::ObTabletID &tablet_id, share::ObTabletCacheInterval &pk_cache)
{
  int ret = OB_SUCCESS;
  ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
  pk_cache.tablet_id_ = tablet_id;
  pk_cache.cache_size_ = PK_CACHE_SIZE;
  if (OB_FAIL(auto_inc.get_tablet_cache_interval(param_.tenant_id_, pk_cache))) {
    LOG_WARN("get_autoinc_seq fail", K(ret), K_(param_.tenant_id), K(tablet_id));
  } else if (OB_UNLIKELY(PK_CACHE_SIZE > pk_cache.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected autoincrement value count", K(ret), K(pk_cache));
  }
  return ret;

}

int ObDirectLoadInsertTabletContext::fill_sstable_slice(const int64_t &slice_id,
                                                        ObIStoreRowIterator &iter,
                                                        int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObDirectLoadSliceInfo slice_info;
    // slice_info.is_full_direct_load_ = !param_.px_mode_;
    slice_info.is_full_direct_load_ = true;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = param_.ls_id_;
    slice_info.data_tablet_id_ = param_.tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = param_.context_id_;
    if (OB_FAIL(sstable_insert_mgr->fill_sstable_slice(slice_info, &iter, affected_rows))) {
      LOG_WARN("fail to fill sstable slice", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::fill_lob_sstable_slice(ObIAllocator &allocator,
                                                            const int64_t &lob_slice_id,
                                                            share::ObTabletCacheInterval &pk_interval,
                                                            blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObDirectLoadSliceInfo slice_info;
    // slice_info.is_full_direct_load_ = !param_.px_mode_;
    slice_info.is_full_direct_load_ = true;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = param_.ls_id_;
    slice_info.data_tablet_id_ = param_.tablet_id_;
    slice_info.slice_id_ = lob_slice_id;
    slice_info.context_id_ = param_.context_id_;
    if (OB_FAIL(handle_.get_obj()->fill_lob_sstable_slice(allocator, slice_info, start_scn_, pk_interval, datum_row))) {
      LOG_WARN("fail to fill sstable slice", KR(ret), K(slice_info), K(datum_row));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::open_sstable_slice(
  const blocksstable::ObMacroDataSeq &start_seq, int64_t &slice_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObDirectLoadSliceInfo slice_info;
    // slice_info.is_full_direct_load_ = !param_.px_mode_;
    slice_info.is_full_direct_load_ = true;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = param_.ls_id_;
    slice_info.data_tablet_id_ = param_.tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = param_.context_id_;
    if (OB_FAIL(open())) {
      LOG_WARN("fail to open tablet direct load", KR(ret));
    } else if (OB_FAIL(sstable_insert_mgr->open_sstable_slice(start_seq, slice_info))) {
      LOG_WARN("fail to construct sstable slice writer", KR(ret), K(slice_info.data_tablet_id_));
    } else {
      slice_id = slice_info.slice_id_;
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::open_lob_sstable_slice(
  const blocksstable::ObMacroDataSeq &start_seq, int64_t &slice_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObDirectLoadSliceInfo slice_info;
    // slice_info.is_full_direct_load_ = !param_.px_mode_;
    slice_info.is_full_direct_load_ = true;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = param_.ls_id_;
    slice_info.data_tablet_id_ = param_.tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = param_.context_id_;
    if (OB_FAIL(open())) {
      LOG_WARN("fail to open tablet direct load", KR(ret));
    } else if (OB_FAIL(sstable_insert_mgr->open_sstable_slice(start_seq, slice_info))) {
      LOG_WARN("fail to construct sstable slice writer", KR(ret), K(slice_info.data_tablet_id_));
    } else {
      slice_id = slice_info.slice_id_;
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::close_sstable_slice(const int64_t slice_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = true;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = param_.ls_id_;
    slice_info.data_tablet_id_ = param_.tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = param_.context_id_;
    if (OB_FAIL(sstable_insert_mgr->close_sstable_slice(slice_info))) {
      LOG_WARN("fail to close tablet direct load", KR(ret), K(slice_id),
               K(param_.tablet_id_));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::close_lob_sstable_slice(const int64_t slice_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = true;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = param_.ls_id_;
    slice_info.data_tablet_id_ = param_.tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = param_.context_id_;
    if (OB_FAIL(sstable_insert_mgr->close_sstable_slice(slice_info))) {
      LOG_WARN("fail to close tablet direct load", KR(ret), K(slice_id),
                K(param_.tablet_id_));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::calc_range()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->calc_range(param_.ls_id_, param_.tablet_id_, true))) {
      LOG_WARN("fail to calc range", KR(ret), K(param_.tablet_id_));
    } else {
      LOG_INFO("success to calc range", K(param_.tablet_id_));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::fill_column_group(const int64_t thread_cnt, const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->fill_column_group(param_.ls_id_, param_.tablet_id_, true/*is direct load*/, thread_cnt, thread_id))) {
      LOG_WARN("fail to fill column group", KR(ret), K(param_.tablet_id_), K(thread_cnt), K(thread_id));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::cancel()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", K(ret), KP(this));
  } else {
    LOG_INFO("start to remove slice writers", K(param_.tablet_id_));
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->cancel(param_.ls_id_, param_.tablet_id_, true/*is direct load*/))) {
      LOG_WARN("cancel direct load fill task failed", K(ret), K(param_.tablet_id_));
    }
  }
  return ret;
}

/**
 * ObDirectLoadInsertTableContext
 */

ObDirectLoadInsertTableContext::ObDirectLoadInsertTableContext() : is_inited_(false) {}

ObDirectLoadInsertTableContext::~ObDirectLoadInsertTableContext() { destory(); }

void ObDirectLoadInsertTableContext::destory()
{
  int ret = OB_SUCCESS;
  for (TABLET_CTX_MAP::iterator iter = tablet_ctx_map_.begin(); iter != tablet_ctx_map_.end();
       ++iter) {
    ObDirectLoadInsertTabletContext *tablet_ctx = iter->second;
    tablet_ctx->~ObDirectLoadInsertTabletContext();
    allocator_.free(tablet_ctx);
  }
  tablet_ctx_map_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

int ObDirectLoadInsertTableContext::init(const ObDirectLoadInsertTableParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTableContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("fail to assign param", KR(ret));
  } else if (OB_FAIL(tablet_ctx_map_.create(1024, "TLD_InsTabCtx",
                                     "TLD_InsTabCtx", MTL_ID()))) {
    LOG_WARN("fail to create tablet ctx map", KR(ret));
  } else if (OB_FAIL(MTL(ObTenantDirectLoadMgr *)->alloc_execution_context_id(ddl_ctrl_.context_id_))) {
    LOG_WARN("alloc execution context id failed", K(ret));
  } else if (OB_FAIL(create_all_tablet_contexts())) {
    LOG_WARN("fail to create all tablet contexts", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadInsertTableContext::create_all_tablet_contexts()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  for (int64_t i = 0; OB_SUCC(ret) && i < param_.ls_partition_ids_.count(); ++i) {
    const ObLSID &target_ls_id = param_.target_ls_partition_ids_.at(i).ls_id_;
    const ObTabletID &tablet_id = param_.ls_partition_ids_.at(i).part_tablet_id_.tablet_id_;
    const ObTabletID &target_tablet_id =
      param_.target_ls_partition_ids_.at(i).part_tablet_id_.tablet_id_;
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    ObLSService *ls_service = nullptr;
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(ls_service->get_ls(target_ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(ret));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, target_tablet_id, tablet_handle,
                                                  ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get tablet handle failed", K(ret));
    } else {
      ObTabletBindingMdsUserData ddl_data;
      ObDirectLoadInsertTabletParam insert_tablet_param;
      insert_tablet_param.tenant_id_ = tenant_id;
      insert_tablet_param.ls_id_ = target_ls_id;
      insert_tablet_param.table_id_ = param_.table_id_;
      insert_tablet_param.tablet_id_ = target_tablet_id;
      insert_tablet_param.origin_tablet_id_ = tablet_id;
      insert_tablet_param.schema_version_ = param_.schema_version_;
      insert_tablet_param.snapshot_version_ = param_.snapshot_version_;
      insert_tablet_param.execution_id_ = param_.execution_id_;
      insert_tablet_param.ddl_task_id_ = param_.ddl_task_id_;
      insert_tablet_param.data_version_ = param_.data_version_;
      insert_tablet_param.reserved_parallel_ = param_.reserved_parallel_;
      insert_tablet_param.context_id_ = ddl_ctrl_.context_id_;
      if (OB_FAIL(tablet_handle.get_obj()->get_ddl_data(SCN::max_scn(), ddl_data))) {
        LOG_WARN("get ddl data failed", K(ret));
      } else if (OB_ISNULL(tablet_ctx = OB_NEWx(ObDirectLoadInsertTabletContext, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadInsertTabletContext", KR(ret));
      } else if (OB_FALSE_IT(insert_tablet_param.lob_tablet_id_ = ddl_data.lob_meta_tablet_id_)) {
      } else if (OB_FAIL(tablet_ctx->init(insert_tablet_param))) {
        LOG_WARN("fail to init fast heap table tablet ctx", KR(ret));
      } else if (OB_FAIL(tablet_ctx_map_.set_refactored(tablet_id, tablet_ctx))) {
        LOG_WARN("fail to set tablet ctx map", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != tablet_ctx) {
          tablet_ctx->~ObDirectLoadInsertTabletContext();
          allocator_.free(tablet_ctx);
          tablet_ctx = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::get_tablet_context(
  const ObTabletID &tablet_id, ObDirectLoadInsertTabletContext *&tablet_ctx) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
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

} // namespace storage
} // namespace oceanbase
