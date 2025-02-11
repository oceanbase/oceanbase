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

#include "storage/direct_load/ob_direct_load_insert_data_table_ctx.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "share/stat/ob_stat_item.h"
#include "share/table/ob_table_load_dml_stat.h"
#include "share/table/ob_table_load_sql_statistics.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace table;

/**
 * ObDirectLoadInsertDataTabletContext
 */

ObDirectLoadInsertDataTabletContext::ObDirectLoadInsertDataTabletContext()
  : context_id_(0),
    direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID),
    open_err_(OB_SUCCESS),
    is_create_(false),
    is_open_(false),
    is_closed_(false),
    is_cancel_(false)
{
}

ObDirectLoadInsertDataTabletContext::~ObDirectLoadInsertDataTabletContext()
{
  int ret = OB_SUCCESS;
  if (is_create_ && !is_closed_) {
    close_tablet_direct_load(false /*commit*/);
  }
}

int ObDirectLoadInsertDataTabletContext::init(ObDirectLoadInsertDataTableContext *table_ctx,
                                              const ObLSID &ls_id,
                                              const ObTabletID &origin_tablet_id,
                                              const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertDataTabletContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_ctx || !ls_id.is_valid() ||
                         !origin_tablet_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_ctx), K(ls_id), K(origin_tablet_id), K(tablet_id));
  } else {
    table_ctx_ = table_ctx;
    param_ = &table_ctx->param_;
    context_id_ = table_ctx->ddl_ctrl_.context_id_;
    direct_load_type_ = table_ctx->ddl_ctrl_.direct_load_type_;
    ls_id_ = ls_id;
    origin_tablet_id_ = origin_tablet_id;
    tablet_id_ = tablet_id;
    pk_tablet_id_ = origin_tablet_id_; // 从原表取, ddl会帮忙同步到隐藏表
    if (OB_FAIL(start_seq_.set_parallel_degree(param_->reserved_parallel_))) {
      LOG_WARN("fail to set parallel degree", KR(ret), K(param_->reserved_parallel_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else if (is_open_) {
    // do nothing
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_UNLIKELY(is_closed_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected already closed", KR(ret));
    } else if (OB_FAIL(open_err_)) {
      LOG_WARN("open has error", KR(ret), K(origin_tablet_id_), K(tablet_id_));
    } else if (!is_open_) {
      while (OB_SUCC(ret)) {
        if (OB_UNLIKELY(is_cancel_)) {
          ret = OB_CANCELED;
          LOG_WARN("task is cancel", KR(ret));
        } else {
          if (OB_FAIL(create_tablet_direct_load())) {
            LOG_WARN("fail to create tablet direct load", KR(ret));
          } else if (OB_FAIL(open_tablet_direct_load())) {
            LOG_WARN("fail to open tablet direct load", KR(ret));
            if (ret == OB_EAGAIN || ret == OB_MINOR_FREEZE_NOT_ALLOW) {
              LOG_WARN("retry to open tablet context");
              ret = OB_SUCCESS;
            }
          } else {
            break;
          }
          if (OB_FAIL(ret)) {
            open_err_ = ret; // avoid open repeatedly when failed
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_UNLIKELY(!is_open_ || is_closed_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ctx", KR(ret), K(is_open_), K(is_closed_));
    } else if (OB_FAIL(close_tablet_direct_load(true /*commit*/))) {
      LOG_WARN("fail to close tablet direct load", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::create_tablet_direct_load()
{
  int ret = OB_SUCCESS;
  if (is_create_) {
    // do nothing
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    ObTabletDirectLoadInsertParam direct_load_param;
    direct_load_param.is_replay_ = false;
    direct_load_param.common_param_.direct_load_type_ = direct_load_type_;
    direct_load_param.common_param_.data_format_version_ = param_->data_version_;
    direct_load_param.common_param_.read_snapshot_ = param_->snapshot_version_;
    direct_load_param.common_param_.ls_id_ = ls_id_;
    direct_load_param.common_param_.tablet_id_ = tablet_id_;
    direct_load_param.common_param_.is_no_logging_ = param_->is_no_logging_;
    direct_load_param.runtime_only_param_.exec_ctx_ = nullptr;
    direct_load_param.runtime_only_param_.task_id_ = param_->ddl_task_id_;
    direct_load_param.runtime_only_param_.table_id_ = param_->table_id_;
    direct_load_param.runtime_only_param_.schema_version_ = param_->schema_version_;
    direct_load_param.runtime_only_param_.task_cnt_ = 1; // default value.
    direct_load_param.runtime_only_param_.parallel_ = param_->parallel_;
    direct_load_param.runtime_only_param_.tx_desc_ = param_->trans_param_.tx_desc_;
    direct_load_param.runtime_only_param_.trans_id_ = param_->trans_param_.tx_id_;
    direct_load_param.runtime_only_param_.seq_no_ = param_->trans_param_.tx_seq_.cast_to_int();
    direct_load_param.runtime_only_param_.max_batch_size_ = param_->max_batch_size_;
    if (OB_FAIL(sstable_insert_mgr->create_tablet_direct_load(
          context_id_, context_id_ /*execution_id*/, direct_load_param))) {
      LOG_WARN("create tablet manager failed", KR(ret), K(direct_load_param));
    } else {
      is_create_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::open_tablet_direct_load()
{
  int ret = OB_SUCCESS;
  if (is_open_) {
    // do nothing
  } else {
    ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(sstable_insert_mgr->open_tablet_direct_load(direct_load_type_, ls_id_, tablet_id_,
                                                            context_id_))) {
      LOG_WARN("fail to open tablet direct load", KR(ret), K(tablet_id_));
    } else if (OB_FAIL(ddl_agent_.init(context_id_, ls_id_, tablet_id_, direct_load_type_))) {
      LOG_WARN("init ddl agent failed", K(ret));
    } else {
      is_open_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::close_tablet_direct_load(bool commit)
{
  int ret = OB_SUCCESS;
  ObTenantDirectLoadMgr *sstable_insert_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_FAIL(sstable_insert_mgr->close_tablet_direct_load(
        context_id_, direct_load_type_, ls_id_, tablet_id_, commit, true /*emergent_finish*/))) {
    LOG_WARN("fail to close tablet direct load", KR(ret), K(ls_id_), K(tablet_id_));
  } else {
    is_closed_ = true;
  }
  return ret;
}

void ObDirectLoadInsertDataTabletContext::cancel()
{
  is_cancel_ = true;
  LOG_INFO("start to remove slice writers", K(tablet_id_));
  ddl_agent_.cancel();
}

//////////////////////// write interface ////////////////////////

int ObDirectLoadInsertDataTabletContext::open_sstable_slice(const ObMacroDataSeq &start_seq,
                                                            const int64_t slice_idx,
                                                            int64_t &slice_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
    slice_info.total_slice_cnt_ = param_->parallel_; //mock total slice cnt
    slice_info.slice_idx_ = slice_idx;
    if (OB_FAIL(open())) {
      LOG_WARN("fail to open tablet direct load", KR(ret));
    } else if (OB_FAIL(get_prefix_merge_slice_idx(slice_info.merge_slice_idx_))) {
      LOG_WARN("get prefix merge slice idx failed", KR(ret));
    } else if (OB_FAIL(ddl_agent_.open_sstable_slice(start_seq, slice_info))) {
      LOG_WARN("fail to construct sstable slice writer", KR(ret), K(slice_info.data_tablet_id_));
    } else {
      slice_id = slice_info.slice_id_;
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::fill_sstable_slice(const int64_t &slice_id,
                                                            ObIStoreRowIterator &iter,
                                                            int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
    if (OB_FAIL(ddl_agent_.fill_sstable_slice(slice_info, &iter, affected_rows))) {
      LOG_WARN("fail to fill sstable slice", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::fill_sstable_slice(const int64_t &slice_id,
                                                            const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
    if (OB_FAIL(ddl_agent_.fill_sstable_slice(slice_info, datum_rows))) {
      LOG_WARN("fail to fill sstable slice", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::close_sstable_slice(const int64_t slice_id,
                                                             const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else {
    blocksstable::ObMacroDataSeq unused_seq;
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = false;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
    if (OB_FAIL(
          ddl_agent_.close_sstable_slice(slice_info, nullptr /*insert_monitor*/, unused_seq))) {
      LOG_WARN("fail to close tablet direct load", KR(ret), K(slice_id), K(tablet_id_));
    } else if (OB_FAIL(record_closed_slice(slice_idx))) {
      LOG_WARN("record closed slice failed", KR(ret), K(slice_idx));
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::record_closed_slice(const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_FAIL(closed_slices_.push_back(slice_idx))) {
      LOG_WARN("push back slice idx failed", KR(ret));
    } else {
      ob_sort(closed_slices_.begin(), closed_slices_.end());
    }
  }
  LOG_TRACE("push slice idx", KR(ret), K(slice_idx), K(closed_slices_));
  return ret;
}

int ObDirectLoadInsertDataTabletContext::get_prefix_merge_slice_idx(int64_t &slice_idx)
{
  int ret = OB_SUCCESS;
  int64_t max_continued_idx = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    lib::ObMutexGuard guard(mutex_);
    for (int64_t i = 0; OB_SUCC(ret) && i < closed_slices_.count(); ++i) {
      if (i != closed_slices_.at(i)) {
        max_continued_idx = 0 == i ? 0 : i - 1;
        break;
      }
    }
    slice_idx = max_continued_idx < 0 ? max(0, closed_slices_.count() - 1) : max_continued_idx;
  }
  LOG_TRACE("get merge slice idx", KR(ret), K(max_continued_idx), K(slice_idx), K(closed_slices_.count()));
  return ret;
}

int ObDirectLoadInsertDataTabletContext::open_lob_sstable_slice(const ObMacroDataSeq &start_seq,
                                                                int64_t &slice_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
    slice_info.total_slice_cnt_ = param_->parallel_; //mock total slice cnt
    if (OB_FAIL(open())) {
      LOG_WARN("fail to open tablet direct load", KR(ret));
    } else if (OB_FAIL(ddl_agent_.open_sstable_slice(start_seq, slice_info))) {
      LOG_WARN("fail to construct sstable slice writer", KR(ret), K(slice_info.data_tablet_id_));
    } else {
      slice_id = slice_info.slice_id_;
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::fill_lob_sstable_slice(ObIAllocator &allocator,
                                                                const int64_t &lob_slice_id,
                                                                ObTabletCacheInterval &pk_interval,
                                                                ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = lob_slice_id;
    slice_info.context_id_ = context_id_;
    if (OB_FAIL(ddl_agent_.fill_lob_sstable_slice(allocator, slice_info, pk_interval, datum_row))) {
      LOG_WARN("fail to fill sstable slice", KR(ret), K(slice_info), K(datum_row));
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::fill_lob_sstable_slice(ObIAllocator &allocator,
                                                                const int64_t &lob_slice_id,
                                                                ObTabletCacheInterval &pk_interval,
                                                                ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = lob_slice_id;
    slice_info.context_id_ = context_id_;
    if (OB_FAIL(
          ddl_agent_.fill_lob_sstable_slice(allocator, slice_info, pk_interval, datum_rows))) {
      LOG_WARN("fail to fill sstable slice", KR(ret), K(slice_info));
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::fill_lob_meta_sstable_slice(const int64_t &lob_slice_id,
                                                                     ObIStoreRowIterator &iter,
                                                                     int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
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
    if (OB_FAIL(ddl_agent_.fill_lob_meta_sstable_slice(slice_info, &iter, affected_rows))) {
      LOG_WARN("fail to fill lob meta sstable slice", KR(ret), K(slice_info));
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::close_lob_sstable_slice(const int64_t slice_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else {
    blocksstable::ObMacroDataSeq unused_seq;
    ObDirectLoadSliceInfo slice_info;
    slice_info.is_full_direct_load_ = !param_->is_incremental_;
    slice_info.is_lob_slice_ = true;
    slice_info.ls_id_ = ls_id_;
    slice_info.data_tablet_id_ = tablet_id_;
    slice_info.slice_id_ = slice_id;
    slice_info.context_id_ = context_id_;
    if (OB_FAIL(
          ddl_agent_.close_sstable_slice(slice_info, nullptr /*insert_monitor*/, unused_seq))) {
      LOG_WARN("fail to close tablet direct load", KR(ret), K(slice_id), K(tablet_id_));
    }
  }
  return ret;
}

//////////////////////// rescan interface ////////////////////////

int ObDirectLoadInsertDataTabletContext::calc_range(const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    if (OB_FAIL(ddl_agent_.calc_range(context_id_, thread_cnt))) {
      LOG_WARN("fail to calc range", KR(ret), K(tablet_id_), K(context_id_), K(thread_cnt));
    } else {
      LOG_INFO("success to calc range", K(tablet_id_));
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTabletContext::fill_column_group(const int64_t thread_cnt,
                                                           const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTabletContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_cancel_)) {
    ret = OB_CANCELED;
    LOG_WARN("task is cancel", KR(ret));
  } else {
    if (OB_FAIL(ddl_agent_.fill_column_group(thread_cnt, thread_id))) {
      LOG_WARN("fail to fill column group", KR(ret), K(tablet_id_), K(thread_cnt), K(thread_id));
    }
  }
  return ret;
}

/**
 * ObDirectLoadInsertDataTableContext
 */

ObDirectLoadInsertDataTableContext::ObDirectLoadInsertDataTableContext() {}

ObDirectLoadInsertDataTableContext::~ObDirectLoadInsertDataTableContext() { reset(); }

void ObDirectLoadInsertDataTableContext::reset()
{
  if (sql_stat_map_.created()) {
    FOREACH(iter, sql_stat_map_)
    {
      ObTableLoadSqlStatistics *sql_statistics = iter->second;
      sql_statistics->~ObTableLoadSqlStatistics();
      allocator_.free(sql_statistics);
    }
    sql_stat_map_.destroy();
  }
  ObDirectLoadInsertTableContext::reset();
}

int ObDirectLoadInsertDataTableContext::init(
  const ObDirectLoadInsertTableParam &param,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertDataTableContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || ls_partition_ids.empty() ||
                         target_ls_partition_ids.empty() ||
                         ls_partition_ids.count() != target_ls_partition_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(ls_partition_ids), K(target_ls_partition_ids));
  } else if (OB_FAIL(inner_init())) {
    LOG_WARN("fail to inner init", KR(ret));
  } else {
    param_ = param;
    ddl_ctrl_.direct_load_type_ = param.is_incremental_
                                    ? DIRECT_LOAD_INCREMENTAL
                                    : (ObDDLUtil::use_idempotent_mode(param_.data_version_)
                                         ? ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2
                                         : ObDirectLoadType::DIRECT_LOAD_LOAD_DATA);
    if (OB_FAIL(MTL(ObTenantDirectLoadMgr *)->alloc_execution_context_id(ddl_ctrl_.context_id_))) {
      LOG_WARN("alloc execution context id failed", K(ret));
    } else if (OB_FAIL(create_all_tablet_contexts(ls_partition_ids, target_ls_partition_ids))) {
      LOG_WARN("fail to create all tablet contexts", KR(ret), K(ls_partition_ids),
               K(target_ls_partition_ids));
    } else if (param_.online_opt_stat_gather_ &&
               sql_stat_map_.create(1024, "TLD_SqlStatMap", "TLD_SqlStatMap", MTL_ID())) {
      LOG_WARN("fail to create sql stat map", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTableContext::create_all_tablet_contexts(
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
    const ObTabletID &origin_tablet_id = ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
    const ObLSID &ls_id = target_ls_partition_ids.at(i).ls_id_;
    const ObTabletID &tablet_id = target_ls_partition_ids.at(i).part_tablet_id_.tablet_id_;
    ObDirectLoadInsertDataTabletContext *tablet_ctx = nullptr;
    if (OB_ISNULL(tablet_ctx = OB_NEWx(ObDirectLoadInsertDataTabletContext, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadInsertDataTabletContext", KR(ret));
    } else if (OB_FAIL(tablet_ctx->init(this, ls_id, origin_tablet_id, tablet_id))) {
      LOG_WARN("fail to init tablet ctx", KR(ret));
    } else if (OB_FAIL(tablet_ctx_map_.set_refactored(origin_tablet_id, tablet_ctx))) {
      LOG_WARN("fail to set tablet ctx map", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != tablet_ctx) {
        tablet_ctx->~ObDirectLoadInsertDataTabletContext();
        allocator_.free(tablet_ctx);
        tablet_ctx = nullptr;
      }
    }
  }
  return ret;
}

//////////////////////// sql stats ////////////////////////

int64_t ObDirectLoadInsertDataTableContext::get_sql_stat_column_count() const
{
  int64_t column_count = 0;
  if (!param_.is_table_without_pk_) {
    column_count = param_.column_count_;
  } else {
    column_count = param_.column_count_ - param_.rowkey_column_count_;
  }
  return column_count;
}

int ObDirectLoadInsertDataTableContext::get_sql_statistics(
  ObTableLoadSqlStatistics *&sql_statistics)
{
  int ret = OB_SUCCESS;
  sql_statistics = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTableContext not init", KR(ret), KP(this));
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
        } else if (OB_FALSE_IT(
                     new_sql_statistics->get_sample_helper().init(param_.online_sample_percent_))) {
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

int ObDirectLoadInsertDataTableContext::update_sql_statistics(
  ObTableLoadSqlStatistics &sql_statistics, const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  bool ignore = false;
  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTableContext not init", KR(ret), KP(this));
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
      if (i < param_.rowkey_column_count_ && param_.is_table_without_pk_) {
        // ignore heap table hidden pk
      } else {
        const int64_t datum_idx = i < param_.rowkey_column_count_ ? i : i + extra_rowkey_cnt;
        const int64_t col_stat_idx = param_.is_table_without_pk_ ? i - 1 : i;
        const ObStorageDatum &datum = datum_row.storage_datums_[datum_idx];
        const ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
        const ObColDesc &col_desc = param_.col_descs_->at(i);
        const bool is_valid =
          ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type(), true);
        if (is_valid) {
          if (OB_FAIL(sql_statistics.get_col_stat(col_stat_idx, col_stat))) {
            LOG_WARN("fail to get col stat", KR(ret), K(col_stat_idx));
          } else if (OB_ISNULL(col_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col stat is null", KR(ret), K(col_stat_idx));
          } else if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_,
                                                               cmp_func.cmp_func_))) {
            LOG_WARN("fail to merge obj", KR(ret), K(i), K(col_desc), K(datum), KP(col_stat));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTableContext::update_sql_statistics(
  ObTableLoadSqlStatistics &sql_statistics, const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param_.online_opt_stat_gather_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not gather sql stat", KR(ret), K(param_));
  } else if (OB_UNLIKELY(datum_rows.get_column_count() !=
                         param_.column_count_ + extra_rowkey_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum row", KR(ret), K(param_));
  } else {
    bool ignore = false;
    ObOptOSGColumnStat *col_stat = nullptr;
    ObDatum datum;
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < datum_rows.row_count_; ++row_idx) {
      if (OB_FAIL(sql_statistics.get_sample_helper().sample_row(ignore))) {
        LOG_WARN("failed to sample row", KR(ret));
      } else if (ignore) {
        // do nothing
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < param_.column_count_; ++i) {
          if (i < param_.rowkey_column_count_ && param_.is_table_without_pk_) {
            // ignore heap table hidden pk
          } else {
            const int64_t datum_idx = i < param_.rowkey_column_count_ ? i : i + extra_rowkey_cnt;
            const int64_t col_stat_idx = param_.is_table_without_pk_ ? i - 1 : i;
            const ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
            const ObColDesc &col_desc = param_.col_descs_->at(i);
            ObIVector *vector = datum_rows.vectors_.at(datum_idx);
            const bool is_valid =
              ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type(), true);
            if (is_valid) {
              if (OB_FAIL(sql_statistics.get_col_stat(col_stat_idx, col_stat))) {
                LOG_WARN("fail to get col stat", KR(ret), K(col_stat_idx));
              } else if (OB_ISNULL(col_stat)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected col stat is null", KR(ret), K(col_stat_idx));
              } else if (OB_FAIL(ObDirectLoadVectorUtils::to_datum(vector, row_idx, datum))) {
                LOG_WARN("fail to get datum", KR(ret));
              } else if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_,
                                                                   cmp_func.cmp_func_))) {
                LOG_WARN("fail to merge obj", KR(ret), KP(col_stat));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTableContext::update_sql_statistics(
  ObTableLoadSqlStatistics &sql_statistics,
  const ObDirectLoadDatumRow &datum_row,
  const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  bool ignore = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param_.online_opt_stat_gather_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not gather sql stat", KR(ret), K(param_));
  } else if (OB_UNLIKELY(row_flag.get_column_count(datum_row.get_column_count()) !=
                           param_.column_count_ ||
                         (row_flag.uncontain_hidden_pk_ && !param_.is_table_without_pk_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum row", KR(ret), K(param_), K(datum_row), K(row_flag));
  } else if (OB_FAIL(sql_statistics.get_sample_helper().sample_row(ignore))) {
    LOG_WARN("failed to sample row", KR(ret));
  } else if (ignore) {
    // do nothing
  } else {
    ObOptOSGColumnStat *col_stat = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_.column_count_; ++i) {
      if (i < param_.rowkey_column_count_ && param_.is_table_without_pk_) {
        // ignore heap table hidden pk
      } else {
        const int64_t datum_idx = row_flag.uncontain_hidden_pk_ ? i - 1 : i;
        const int64_t col_stat_idx = param_.is_table_without_pk_ ? i - 1 : i;
        const ObStorageDatum &datum = datum_row.storage_datums_[datum_idx];
        const ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
        const ObColDesc &col_desc = param_.col_descs_->at(i);
        const bool is_valid =
          ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type(), true);
        if (is_valid) {
          if (OB_FAIL(sql_statistics.get_col_stat(col_stat_idx, col_stat))) {
            LOG_WARN("fail to get col stat", KR(ret), K(col_stat_idx));
          } else if (OB_ISNULL(col_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col stat is null", KR(ret), K(col_stat_idx));
          } else if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_,
                                                               cmp_func.cmp_func_))) {
            LOG_WARN("fail to merge obj", KR(ret), K(i), K(col_desc), K(datum), KP(col_stat));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTableContext::update_sql_statistics(
  ObTableLoadSqlStatistics &sql_statistics, const IVectorPtrs &vectors, const int64_t row_idx,
  const ObDirectLoadRowFlag &row_flag)
{
  int ret = OB_SUCCESS;
  bool ignore = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param_.online_opt_stat_gather_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not gather sql stat", KR(ret), K(param_));
  } else if (OB_UNLIKELY(row_flag.get_column_count(vectors.count()) != param_.column_count_ ||
                         row_idx < 0 ||
                         (row_flag.uncontain_hidden_pk_ && !param_.is_table_without_pk_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum row", KR(ret), K(param_), K(vectors.count()), K(row_idx), K(row_flag));
  } else if (OB_FAIL(sql_statistics.get_sample_helper().sample_row(ignore))) {
    LOG_WARN("failed to sample row", KR(ret));
  } else if (ignore) {
    // do nothing
  } else {
    ObOptOSGColumnStat *col_stat = nullptr;
    ObDatum datum;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_.column_count_; ++i) {
      if (i < param_.rowkey_column_count_ && param_.is_table_without_pk_) {
        // ignore heap table hidden pk
      } else {
        const int64_t datum_idx = row_flag.uncontain_hidden_pk_ ? i - 1 : i;
        const int64_t col_stat_idx = param_.is_table_without_pk_ ? i - 1 : i;
        const ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
        const ObColDesc &col_desc = param_.col_descs_->at(i);
        ObIVector *vector = vectors.at(datum_idx);
        const bool is_valid =
          ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type(), true);
        if (is_valid) {
          if (OB_FAIL(sql_statistics.get_col_stat(col_stat_idx, col_stat))) {
            LOG_WARN("fail to get col stat", KR(ret), K(col_stat_idx));
          } else if (OB_ISNULL(col_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col stat is null", KR(ret), K(col_stat_idx));
          } else if (OB_FAIL(ObDirectLoadVectorUtils::to_datum(vector, row_idx, datum))) {
            LOG_WARN("fail to get datum", KR(ret));
          } else if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_,
                                                               cmp_func.cmp_func_))) {
            LOG_WARN("fail to merge obj", KR(ret), K(i), K(col_desc), K(datum), KP(col_stat));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertDataTableContext::collect_sql_stats(ObTableLoadDmlStat &dml_stats,
                                                          ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTableContext not init", KR(ret), KP(this));
  } else if (OB_FAIL(collect_dml_stat(dml_stats))) {
    LOG_WARN("fail to collect dml stat", KR(ret));
  } else if (param_.online_opt_stat_gather_ && OB_FAIL(collect_sql_statistics(sql_statistics))) {
    LOG_WARN("fail to collect sql statistics", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertDataTableContext::collect_dml_stat(ObTableLoadDmlStat &dml_stats)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  dml_stats.reset();
  FOREACH_X(iter, tablet_ctx_map_, OB_SUCC(ret))
  {
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

int ObDirectLoadInsertDataTableContext::collect_sql_statistics(
  ObTableLoadSqlStatistics &sql_statistics)
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
      const ObColDesc &col_desc = param_.col_descs_->at(!param_.is_table_without_pk_ ? i : i + 1);
      if (OB_FAIL(sql_statistics.get_col_stat(i, osg_col_stat))) {
        LOG_WARN("fail to get col stat", KR(ret), K(i));
      }
      FOREACH_X(iter, sql_stat_map_, OB_SUCC(ret))
      {
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
          FOREACH_X(iter, sql_stat_map_, OB_SUCC(ret))
          {
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
