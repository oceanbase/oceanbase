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
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/table/ob_table_load_dml_stat.h"
#include "share/table/ob_table_load_sql_statistics.h"
#include "share/stat/ob_stat_item.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/lob/ob_lob_meta.h"

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
    schema_version_(OB_INVALID_VERSION),
    snapshot_version_(0),
    ddl_task_id_(0),
    data_version_(0),
    parallel_(0),
    reserved_parallel_(0),
    rowkey_column_count_(0),
    column_count_(0),
    lob_column_count_(0),
    is_partitioned_table_(false),
    is_heap_table_(false),
    is_column_store_(false),
    online_opt_stat_gather_(false),
    is_incremental_(false),
    datum_utils_(nullptr),
    col_descs_(nullptr),
    cmp_funcs_(nullptr),
    online_sample_percent_(1.)
{
}

ObDirectLoadInsertTableParam::~ObDirectLoadInsertTableParam() {}

bool ObDirectLoadInsertTableParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ &&
         OB_INVALID_VERSION != schema_version_ &&
         snapshot_version_ >= 0 &&
         ddl_task_id_ > 0 &&
         data_version_ >= 0 &&
         parallel_ > 0 &&
         reserved_parallel_ >= 0 &&
         rowkey_column_count_ > 0 &&
         column_count_ > 0 && column_count_ >= rowkey_column_count_ &&
         lob_column_count_ >= 0 && lob_column_count_ < column_count_ &&
         (!is_incremental_ || trans_param_.is_valid()) &&
         nullptr != datum_utils_ &&
         nullptr != col_descs_ && col_descs_->count() == column_count_ &&
         nullptr != cmp_funcs_;
}

/**
 * ObDirectLoadInsertTabletContext
 */

ObDirectLoadInsertTabletContext::ObDirectLoadInsertTabletContext()
  : table_ctx_(nullptr),
    param_(nullptr),
    context_id_(0),
    row_count_(0),
    open_err_(OB_SUCCESS),
    is_open_(false),
    is_create_(false),
    is_cancel_(false),
    is_inited_(false)
{
}

ObDirectLoadInsertTabletContext::~ObDirectLoadInsertTabletContext()
{
  int ret = OB_SUCCESS;
  if (is_create_) {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->close_tablet_direct_load(
          context_id_, !param_->is_incremental_, ls_id_, tablet_id_, false /*need_commit*/,
          true /*emergent_finish*/))) {
      LOG_WARN("fail to close tablet direct load", KR(ret), K(ls_id_), K(tablet_id_));
    } else {
      is_create_ = false;
      handle_.reset();
    }
  }
}

int ObDirectLoadInsertTabletContext::init(ObDirectLoadInsertTableContext *table_ctx,
                                          const ObLSID &ls_id,
                                          const ObTabletID &origin_tablet_id,
                                          const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTabletContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_ctx || !ls_id.is_valid() ||
                         !origin_tablet_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(table_ctx), K(ls_id), K(origin_tablet_id), K(tablet_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ObLSService *ls_service = nullptr;
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    ObTabletBindingMdsUserData ddl_data;
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(ret));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle,
                                                 ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get tablet handle failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_data(SCN::max_scn(), ddl_data))) {
      LOG_WARN("get ddl data failed", K(ret));
    } else {
      table_ctx_ = table_ctx;
      param_ = &table_ctx->get_param();
      context_id_ = table_ctx->get_context_id();
      ls_id_ = ls_id;
      origin_tablet_id_ = origin_tablet_id;
      tablet_id_ = tablet_id;
      lob_tablet_id_ = ddl_data.lob_meta_tablet_id_;
      start_seq_.set_parallel_degree(param_->reserved_parallel_);
      if (need_del_lob()) {
        lob_start_seq_.set_parallel_degree(param_->parallel_);
      }
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else if (is_open_) {
    // do nothing
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_FAIL(open_err_)) {
      LOG_WARN("open has error", KR(ret), K(origin_tablet_id_), K(tablet_id_));
    } else if (!is_open_) {
      ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
      ObTabletDirectLoadInsertParam direct_load_param;
      direct_load_param.is_replay_ = false;
      direct_load_param.common_param_.direct_load_type_ =
          param_->is_incremental_ ? ObDirectLoadType::DIRECT_LOAD_INCREMENTAL
                                  : ObDirectLoadType::DIRECT_LOAD_LOAD_DATA;
      direct_load_param.common_param_.data_format_version_ = param_->data_version_;
      direct_load_param.common_param_.read_snapshot_ = param_->snapshot_version_;
      direct_load_param.common_param_.ls_id_ = ls_id_;
      direct_load_param.common_param_.tablet_id_ = tablet_id_;
      direct_load_param.runtime_only_param_.exec_ctx_ = nullptr;
      direct_load_param.runtime_only_param_.task_id_ = param_->ddl_task_id_;
      direct_load_param.runtime_only_param_.table_id_ = param_->table_id_;
      direct_load_param.runtime_only_param_.schema_version_ = param_->schema_version_;
      direct_load_param.runtime_only_param_.task_cnt_ = 1; // default value.
      direct_load_param.runtime_only_param_.parallel_ = param_->parallel_;
      direct_load_param.runtime_only_param_.tx_desc_ = param_->trans_param_.tx_desc_;
      direct_load_param.runtime_only_param_.trans_id_ = param_->trans_param_.tx_id_;
      direct_load_param.runtime_only_param_.seq_no_ = param_->trans_param_.tx_seq_.cast_to_int();
      if (OB_FAIL(sstable_insert_mgr->create_tablet_direct_load(
          context_id_, context_id_ /*execution_id*/, direct_load_param))) {
        LOG_WARN("create tablet manager failed", KR(ret), K(direct_load_param));
      } else if (FALSE_IT(is_create_ = true)) {
      } else if (OB_FAIL(sstable_insert_mgr->open_tablet_direct_load(
          !param_->is_incremental_, ls_id_, tablet_id_, context_id_, start_scn_, handle_))) {
        LOG_WARN("fail to open tablet direct load", KR(ret), K(tablet_id_));
      } else {
        is_open_ = true;
      }
      if (OB_FAIL(ret) && !param_->is_incremental_) {
        open_err_ = ret; // avoid open repeatedly when failed
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
  } else if (OB_UNLIKELY(!is_open_ || is_cancel_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected to close", KR(ret), K(is_open_), K(is_cancel_));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->close_tablet_direct_load(
          context_id_, !param_->is_incremental_, ls_id_, tablet_id_, true /*need_commit*/,
          true /*emergent_finish*/))) {
      LOG_WARN("fail to close tablet direct load", KR(ret), K(ls_id_), K(tablet_id_));
    } else {
      is_open_ = false;
      handle_.reset();
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
    LOG_INFO("start to remove slice writers", K(tablet_id_));
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->cancel(context_id_, ls_id_, tablet_id_, !param_->is_incremental_))) {
      LOG_WARN("cancel direct load fill task failed", K(ret), K(context_id_), K(tablet_id_));
    } else {
      is_cancel_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::get_pk_interval(uint64_t count,
                                                     ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pk_cache_.fetch(count, pk_interval))) {
    if (OB_UNLIKELY(OB_EAGAIN != ret)) {
      LOG_WARN("fail to fetch from pk cache", KR(ret));
    } else {
      if (OB_FAIL(refresh_pk_cache(origin_tablet_id_, pk_cache_))) {
        LOG_WARN("fail to refresh pk cache", KR(ret));
      } else if (OB_FAIL(pk_cache_.fetch(count, pk_interval))) {
        LOG_WARN("fail to fetch from pk cache", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::get_lob_pk_interval(uint64_t count,
                                                         ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lob_pk_cache_.fetch(count, pk_interval))) {
    if (OB_UNLIKELY(OB_EAGAIN != ret)) {
      LOG_WARN("fail to fetch from pk cache", KR(ret));
    } else {
      if (OB_FAIL(refresh_pk_cache(lob_tablet_id_, lob_pk_cache_))) {
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
    if (OB_FAIL(get_lob_pk_interval(WRITE_BATCH_SIZE, write_ctx.pk_interval_))) {
      LOG_WARN("fail to get pk interval", KR(ret), KP(this));
    } else {
      write_ctx.start_seq_.macro_data_seq_ = lob_start_seq_.macro_data_seq_;
      lob_start_seq_.macro_data_seq_ += WRITE_BATCH_SIZE;
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::refresh_pk_cache(const ObTabletID &tablet_id,
                                                      ObTabletCacheInterval &pk_cache)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
  pk_cache.tablet_id_ = tablet_id;
  pk_cache.cache_size_ = PK_CACHE_SIZE;
  if (OB_FAIL(auto_inc.get_tablet_cache_interval(tenant_id, pk_cache))) {
    LOG_WARN("get_autoinc_seq fail", K(ret), K(tenant_id), K(tablet_id));
  } else if (OB_UNLIKELY(PK_CACHE_SIZE > pk_cache.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected autoincrement value count", K(ret), K(pk_cache));
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::init_datum_row(ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", K(ret), KP(this));
  } else {
    const int64_t real_column_count =
      param_->column_count_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    if (OB_FAIL(datum_row.init(real_column_count))) {
      LOG_WARN("fail to init datum row", KR(ret), K(real_column_count));
    } else {
      const int64_t trans_version =
        !param_->is_incremental_ ? param_->snapshot_version_ : INT64_MAX;
      datum_row.trans_id_ = param_->trans_param_.tx_id_;
      datum_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT, !param_->is_incremental_ ? DF_TYPE_NORMAL : DF_TYPE_INSERT_DELETE);
      datum_row.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row.mvcc_row_flag_.set_uncommitted_row(param_->is_incremental_);
      // fill trans_version
      datum_row.storage_datums_[param_->rowkey_column_count_].set_int(-trans_version);
      // fill sql_no
      datum_row.storage_datums_[param_->rowkey_column_count_ + 1].set_int(
        -param_->trans_param_.tx_seq_.cast_to_int());
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::init_lob_datum_row(blocksstable::ObDatumRow &datum_row, const bool is_delete)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", K(ret), KP(this));
  } else {
    const int64_t rowkey_column_count = ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT;
    const int64_t real_column_count =
      ObLobMetaUtil::LOB_META_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    if (OB_FAIL(datum_row.init(real_column_count))) {
      LOG_WARN("fail to init datum row", KR(ret), K(real_column_count));
    } else {
      const int64_t trans_version =
        !param_->is_incremental_ ? param_->snapshot_version_ : INT64_MAX;
      datum_row.trans_id_ = param_->trans_param_.tx_id_;
      datum_row.row_flag_.set_flag(is_delete ? ObDmlFlag::DF_DELETE : ObDmlFlag::DF_INSERT);
      datum_row.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row.mvcc_row_flag_.set_uncommitted_row(param_->is_incremental_);
      // fill trans_version
      datum_row.storage_datums_[rowkey_column_count].set_int(-trans_version);
      // fill sql_no
      datum_row.storage_datums_[rowkey_column_count + 1].set_int(
        -param_->trans_param_.tx_seq_.cast_to_int());
    }
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
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
    if (OB_FAIL(sstable_insert_mgr->fill_sstable_slice(slice_info, &iter, affected_rows))) {
      LOG_WARN("fail to fill sstable slice", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::fill_lob_meta_sstable_slice(const int64_t &lob_slice_id,
                                                                 ObIStoreRowIterator &iter,
                                                                 int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else if (OB_UNLIKELY(!is_open_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not open", KR(ret));
  } else {
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = lob_slice_id;
    slice_info.context_id_ = context_id_;
    if (OB_UNLIKELY(!handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid handle", KR(ret));
    } else if (OB_FAIL(handle_.get_obj()->fill_lob_meta_sstable_slice(slice_info,
                                                                      start_scn_,
                                                                      &iter,
                                                                      affected_rows))) {
      LOG_WARN("fail to fill lob meta sstable slice", KR(ret), K(slice_info));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::fill_lob_sstable_slice(ObIAllocator &allocator,
                                                            const int64_t &lob_slice_id,
                                                            ObTabletCacheInterval &pk_interval,
                                                            ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else if (OB_UNLIKELY(!is_open_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not open", KR(ret));
  } else {
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = lob_slice_id;
    slice_info.context_id_ = context_id_;
    if (OB_UNLIKELY(!handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid handle", KR(ret));
    } else if (OB_FAIL(handle_.get_obj()->fill_lob_sstable_slice(allocator, slice_info, start_scn_,
                                                          pk_interval, datum_row))) {
      LOG_WARN("fail to fill sstable slice", KR(ret), K(slice_info), K(datum_row));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::open_sstable_slice(const ObMacroDataSeq &start_seq,
                                                        int64_t &slice_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
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

int ObDirectLoadInsertTabletContext::open_lob_sstable_slice(const ObMacroDataSeq &start_seq,
                                                            int64_t &slice_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
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
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
    blocksstable::ObMacroDataSeq unused_seq;
    if (OB_FAIL(sstable_insert_mgr->close_sstable_slice(slice_info, nullptr/*insert_monitor*/, unused_seq))) {
      LOG_WARN("fail to close tablet direct load", KR(ret), K(slice_id), K(tablet_id_));
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
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
    blocksstable::ObMacroDataSeq unused_seq;
    if (OB_FAIL(sstable_insert_mgr->close_sstable_slice(slice_info, nullptr/*insert_monitor*/, unused_seq))) {
      LOG_WARN("fail to close tablet direct load", KR(ret), K(slice_id), K(tablet_id_));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::calc_range(const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->calc_range(ls_id_, tablet_id_, thread_cnt,
                                               !param_->is_incremental_))) {
      LOG_WARN("fail to calc range", KR(ret), K(tablet_id_));
    } else {
      LOG_INFO("success to calc range", K(tablet_id_));
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::fill_column_group(const int64_t thread_cnt,
                                                       const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->fill_column_group(ls_id_, tablet_id_, !param_->is_incremental_,
                                                      thread_cnt, thread_id))) {
      LOG_WARN("fail to fill column group", KR(ret), K(tablet_id_), K(thread_cnt), K(thread_id));
    }
  }
  return ret;
}

/**
 * ObDirectLoadInsertTableContext
 */

ObDirectLoadInsertTableContext::ObDirectLoadInsertTableContext()
  : allocator_("TLD_InsTblCtx"), safe_allocator_(allocator_), is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadInsertTableContext::~ObDirectLoadInsertTableContext() { destory(); }

void ObDirectLoadInsertTableContext::destory()
{
  FOREACH(iter, tablet_ctx_map_)
  {
    ObDirectLoadInsertTabletContext *tablet_ctx = iter->second;
    tablet_ctx->~ObDirectLoadInsertTabletContext();
    allocator_.free(tablet_ctx);
  }
  tablet_ctx_map_.destroy();
  FOREACH(iter, sql_stat_map_)
  {
    ObTableLoadSqlStatistics *sql_statistics = iter->second;
    sql_statistics->~ObTableLoadSqlStatistics();
    allocator_.free(sql_statistics);
  }
  sql_stat_map_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

int ObDirectLoadInsertTableContext::init(
  const ObDirectLoadInsertTableParam &param,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTableContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || ls_partition_ids.empty() ||
                         target_ls_partition_ids.empty() ||
                         ls_partition_ids.count() != target_ls_partition_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(ls_partition_ids), K(target_ls_partition_ids));
  } else {
    param_ = param;
    if (OB_FAIL(tablet_ctx_map_.create(1024, "TLD_InsTabCtx", "TLD_InsTabCtx", MTL_ID()))) {
      LOG_WARN("fail to create tablet ctx map", KR(ret));
    } else if (OB_FAIL(
                 MTL(ObTenantDirectLoadMgr *)->alloc_execution_context_id(ddl_ctrl_.context_id_))) {
      LOG_WARN("alloc execution context id failed", K(ret));
    } else if (OB_FAIL(create_all_tablet_contexts(ls_partition_ids, target_ls_partition_ids))) {
      LOG_WARN("fail to create all tablet contexts", KR(ret));
    } else if (param_.online_opt_stat_gather_ &&
               sql_stat_map_.create(1024, "TLD_SqlStatMap", "TLD_SqlStatMap", MTL_ID())) {
      LOG_WARN("fail to create sql stat map", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::create_all_tablet_contexts(
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
    const ObTabletID &origin_tablet_id = ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
    const ObLSID &ls_id = target_ls_partition_ids.at(i).ls_id_;
    const ObTabletID &tablet_id = target_ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    if (OB_ISNULL(tablet_ctx = OB_NEWx(ObDirectLoadInsertTabletContext, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadInsertTabletContext", KR(ret));
    } else if (OB_FAIL(tablet_ctx->init(this, ls_id, origin_tablet_id, tablet_id))) {
      LOG_WARN("fail to init fast heap table tablet ctx", KR(ret));
    } else if (OB_FAIL(tablet_ctx_map_.set_refactored(origin_tablet_id, tablet_ctx))) {
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

int ObDirectLoadInsertTableContext::commit(ObTableLoadDmlStat &dml_stats,
                                           ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_FAIL(collect_dml_stat(dml_stats))) {
    LOG_WARN("fail to collect dml stat", KR(ret));
  } else if (param_.online_opt_stat_gather_) {
    if (OB_FAIL(collect_sql_statistics(sql_statistics))) {
      LOG_WARN("fail to collect sql statistics", KR(ret));
    }
  }
  return ret;
}

void ObDirectLoadInsertTableContext::cancel()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    FOREACH(iter, tablet_ctx_map_)
    {
      const ObTabletID &tablet_id = iter->first;
      ObDirectLoadInsertTabletContext *tablet_ctx = iter->second;
      if (OB_ISNULL(tablet_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet ctx is NULL", KR(ret), K(tablet_id));
      } else if (OB_FAIL(tablet_ctx->cancel())) {
        LOG_WARN("fail to cancel tablet ctx", KR(ret), K(tablet_id));
      }
    }
  }
}

int64_t ObDirectLoadInsertTableContext::get_sql_stat_column_count() const
{
  int64_t column_count = 0;
  if (!param_.is_heap_table_) {
    column_count = param_.column_count_;
  } else {
    column_count = param_.column_count_ - param_.rowkey_column_count_;
  }
  return column_count;
}

int ObDirectLoadInsertTableContext::get_sql_statistics(ObTableLoadSqlStatistics *&sql_statistics)
{
  int ret = OB_SUCCESS;
  sql_statistics = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param_.online_opt_stat_gather_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no gather sql stat", KR(ret), K(param_));
  } else {
    const int64_t part_id = get_tid_cache();
    if (OB_FAIL(sql_stat_map_.get_refactored(part_id, sql_statistics))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get sql stat", KR(ret), K(part_id));
      } else {
        ret = OB_SUCCESS;
        const int64_t column_count = get_sql_stat_column_count();
        ObTableLoadSqlStatistics *new_sql_statistics = nullptr;
        if (OB_ISNULL(new_sql_statistics = OB_NEWx(ObTableLoadSqlStatistics, (&safe_allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObTableLoadSqlStatistics", KR(ret));
        } else if (OB_FAIL(new_sql_statistics->create(column_count))) {
          LOG_WARN("fail to create sql stat", KR(ret), K(column_count));
        } else if (OB_FALSE_IT(new_sql_statistics->get_sample_helper().init(param_.online_sample_percent_))) {
        } else if (OB_FAIL(sql_stat_map_.set_refactored(part_id, new_sql_statistics))) {
          LOG_WARN("fail to set sql stat back", KR(ret), K(part_id));
        } else {
          sql_statistics = new_sql_statistics;
        }
        if (OB_FAIL(ret)) {
          if (nullptr != new_sql_statistics) {
            new_sql_statistics->~ObTableLoadSqlStatistics();
            safe_allocator_.free(new_sql_statistics);
            new_sql_statistics = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::update_sql_statistics(ObTableLoadSqlStatistics &sql_statistics,
                                                          const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  bool ignore = false;
  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param_.online_opt_stat_gather_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not gather sql stat", KR(ret), K(param_));
  } else if (OB_UNLIKELY(datum_row.get_column_count() != param_.column_count_ + extra_rowkey_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum row", KR(ret), K(param_), K(datum_row));
  } else if (OB_FAIL(sql_statistics.get_sample_helper().sample_row(ignore))) {
    LOG_WARN("failed to sample row", KR(ret));
  } else if (ignore) {
    // do nothing
  } else {
    ObOptOSGColumnStat *col_stat = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_.column_count_; i++) {
      if (i < param_.rowkey_column_count_ && param_.is_heap_table_) {
        // ignore heap table hidden pk
      } else {
        const int64_t datum_idx = i < param_.rowkey_column_count_ ? i : i + extra_rowkey_cnt;
        const int64_t col_stat_idx = param_.is_heap_table_ ? i - 1 : i;
        const ObStorageDatum &datum = datum_row.storage_datums_[datum_idx];
        const ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
        const ObColDesc &col_desc = param_.col_descs_->at(i);
        const bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
        if (is_valid) {
          if (OB_FAIL(sql_statistics.get_col_stat(col_stat_idx, col_stat))) {
            LOG_WARN("fail to get col stat", KR(ret), K(col_stat_idx));
          } else if (OB_ISNULL(col_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col stat is null", KR(ret), K(col_stat_idx));
          } else if (OB_FAIL(col_stat->update_column_stat_info(&datum,
                                                               col_desc.col_type_,
                                                               cmp_func.cmp_func_))) {
            LOG_WARN("fail to merge obj", KR(ret), KP(col_stat));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::collect_dml_stat(ObTableLoadDmlStat &dml_stats)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  dml_stats.reset();
  FOREACH_X(iter, tablet_ctx_map_, OB_SUCC(ret)) {
    ObDirectLoadInsertTabletContext *tablet_ctx = iter->second;
    ObOptDmlStat *dml_stat = nullptr;
    if (OB_FAIL(dml_stats.allocate_dml_stat(dml_stat))) {
      LOG_WARN("fail to allocate table stat", KR(ret));
    } else {
      dml_stat->tenant_id_ = tenant_id;
      dml_stat->table_id_ = param_.table_id_;
      dml_stat->tablet_id_ = tablet_ctx->get_tablet_id().id();
      dml_stat->insert_row_count_ = tablet_ctx->get_row_count();
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::collect_sql_statistics(ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = get_sql_stat_column_count();
  sql_statistics.reset();
  if (OB_UNLIKELY(!param_.online_opt_stat_gather_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no gather sql stat", KR(ret), K(param_));
  } else if (OB_FAIL(sql_statistics.create(column_count))) {
    LOG_WARN("fail to create sql stat", KR(ret), K(column_count));
  } else {
    const StatLevel stat_level = TABLE_LEVEL;
    const int64_t partition_id = !param_.is_partitioned_table_ ? param_.table_id_ : -1;
    int64_t table_avg_len = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      ObOptOSGColumnStat *osg_col_stat = nullptr;
      const ObColDesc &col_desc = param_.col_descs_->at(!param_.is_heap_table_ ? i : i + 1);
      if (OB_FAIL(sql_statistics.get_col_stat(i, osg_col_stat))) {
        LOG_WARN("fail to get col stat", KR(ret), K(i));
      }
      FOREACH_X(iter, sql_stat_map_, OB_SUCC(ret)) {
        const int64_t part_id = iter->first;
        ObTableLoadSqlStatistics *part_sql_statistics = iter->second;
        ObOptOSGColumnStat *part_osg_col_stat = nullptr;
        if (OB_ISNULL(part_sql_statistics)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected sql stat is null", KR(ret), K(part_id));
        } else if (OB_FAIL(part_sql_statistics->get_col_stat(i, part_osg_col_stat))) {
          LOG_WARN("fail to get col stat", KR(ret), K(i));
        } else if (OB_FAIL(osg_col_stat->merge_column_stat(*part_osg_col_stat))) {
          LOG_WARN("fail to merge column stat", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        osg_col_stat->col_stat_->calc_avg_len();
        osg_col_stat->col_stat_->set_table_id(param_.table_id_);
        osg_col_stat->col_stat_->set_partition_id(partition_id);
        osg_col_stat->col_stat_->set_stat_level(stat_level);
        osg_col_stat->col_stat_->set_column_id(col_desc.col_id_);
        osg_col_stat->col_stat_->set_num_distinct(
          ObGlobalNdvEval::get_ndv_from_llc(osg_col_stat->col_stat_->get_llc_bitmap()));
        if (OB_FAIL(osg_col_stat->set_min_max_datum_to_obj())) {
          LOG_WARN("failed to set min max datum to obj", K(ret));
        } else {
          table_avg_len += osg_col_stat->col_stat_->get_avg_len();
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObOptTableStat *table_stat = nullptr;
      uint64_t sample_value = 0;
      if (OB_FAIL(sql_statistics.get_table_stat(0, table_stat))) {
        LOG_WARN("fail to get table stat", KR(ret));
      } else {
        int64_t row_count = 0;
        FOREACH(iter, tablet_ctx_map_)
        {
          ObDirectLoadInsertTabletContext *tablet_ctx = iter->second;
          row_count += tablet_ctx->get_row_count();
        }
        if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0) {
          FOREACH_X(iter, sql_stat_map_, OB_SUCC(ret)) {
            const int64_t part_id = iter->first;
            ObTableLoadSqlStatistics *part_sql_statistics = iter->second;
            if (OB_ISNULL(part_sql_statistics)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected sql stat is null", KR(ret), K(part_id));
            } else {
              sample_value += part_sql_statistics->get_sample_helper().sample_value_;
            }
          }
        }
        if (OB_SUCC(ret)) {
          table_stat->set_table_id(param_.table_id_);
          table_stat->set_partition_id(partition_id);
          table_stat->set_object_type(stat_level);
          table_stat->set_row_count(row_count);
          table_stat->set_avg_row_size(table_avg_len);
          table_stat->set_sample_size(sample_value);
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
