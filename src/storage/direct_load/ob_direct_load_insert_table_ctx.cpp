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
#include "share/stat/ob_stat_item.h"
#include "share/table/ob_table_load_dml_stat.h"
#include "share/table/ob_table_load_sql_statistics.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"

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
    lob_inrow_threshold_(-1),
    is_partitioned_table_(false),
    is_table_without_pk_(false),
    is_table_with_hidden_pk_column_(false),
    is_index_table_(false),
    online_opt_stat_gather_(false),
    is_insert_lob_(false),
    is_incremental_(false),
    reuse_pk_(true),
    datum_utils_(nullptr),
    col_descs_(nullptr),
    cmp_funcs_(nullptr),
    lob_column_idxs_(nullptr),
    online_sample_percent_(1.),
    is_no_logging_(false),
    max_batch_size_(0)
{
}

ObDirectLoadInsertTableParam::~ObDirectLoadInsertTableParam() {}

bool ObDirectLoadInsertTableParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_VERSION != schema_version_ &&
         snapshot_version_ >= 0 && ddl_task_id_ > 0 && data_version_ >= 0 && parallel_ > 0 &&
         reserved_parallel_ >= 0 && rowkey_column_count_ > 0 && column_count_ > 0 &&
         column_count_ >= rowkey_column_count_ && lob_inrow_threshold_ >= 0 &&
         (!is_incremental_ || trans_param_.is_valid()) && nullptr != datum_utils_ &&
         nullptr != col_descs_ && col_descs_->count() == column_count_ && nullptr != cmp_funcs_ &&
         nullptr != lob_column_idxs_ && max_batch_size_ > 0;
}

/**
 * ObDirectLoadInsertTabletContext
 */

ObDirectLoadInsertTabletContext::ObDirectLoadInsertTabletContext()
  : table_ctx_(nullptr),
    param_(nullptr),
    ls_id_(),
    origin_tablet_id_(),
    tablet_id_(),
    pk_tablet_id_(),
    lob_tablet_ctx_(nullptr),
    slice_idx_(0),
    start_seq_(),
    pk_cache_(),
    row_count_(0),
    is_inited_(false)
{
  closed_slices_.set_tenant_id(MTL_ID());
}

ObDirectLoadInsertTabletContext::~ObDirectLoadInsertTabletContext() {}

//////////////////////// write interface ////////////////////////

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

int ObDirectLoadInsertTabletContext::get_pk_interval(uint64_t count,
                                                     ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pk_cache_.fetch(count, pk_interval))) {
    if (OB_UNLIKELY(OB_EAGAIN != ret)) {
      LOG_WARN("fail to fetch from pk cache", KR(ret));
    } else {
      if (OB_FAIL(refresh_pk_cache((param_->reuse_pk_ ? pk_tablet_id_ : tablet_id_), pk_cache_))) {
        LOG_WARN("fail to refresh pk cache", KR(ret), K(pk_tablet_id_));
      } else if (OB_FAIL(pk_cache_.fetch(count, pk_interval))) {
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
    LOG_WARN("ObDirectLoadInsertTabletContext not init", KR(ret), KP(this));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(get_pk_interval(WRITE_BATCH_SIZE, write_ctx.pk_interval_))) {
      LOG_WARN("fail to get pk interval", KR(ret), KP(this));
    } else {
      write_ctx.start_seq_.macro_data_seq_ = start_seq_.macro_data_seq_;
      write_ctx.slice_idx_ = slice_idx_;
      // the macro block may not be recycled when load data failed in shared storage mode, TODO(jianming)
      start_seq_.macro_data_seq_ += WRITE_BATCH_SIZE;
      ++slice_idx_;
    }
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::get_row_info(ObDirectLoadInsertTableRowInfo &row_info,
                                                  const bool is_delete)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTabletContext not init", K(ret), KP(this));
  } else {
    row_info.row_flag_.set_flag(
      is_delete ? ObDmlFlag::DF_DELETE : ObDmlFlag::DF_INSERT,
      // 只有增量inc_replace模式下的主表insert行需要DF_TYPE_INSERT_DELETE
      // * 目前这里没有细分增量inc和增量inc_replace
      // * 增量inc_replace带索引或lob会退化成增量inc
      (!param_->is_incremental_ || is_delete) ? DF_TYPE_NORMAL : DF_TYPE_INSERT_DELETE);
    row_info.mvcc_row_flag_.set_compacted_multi_version_row(true);
    row_info.mvcc_row_flag_.set_first_multi_version_row(true);
    row_info.mvcc_row_flag_.set_last_multi_version_row(true);
    row_info.mvcc_row_flag_.set_uncommitted_row(param_->is_incremental_);
    row_info.trans_version_ = !param_->is_incremental_ ? param_->snapshot_version_ : INT64_MAX;
    row_info.trans_id_ = param_->trans_param_.tx_id_;
    row_info.seq_no_ = param_->trans_param_.tx_seq_.cast_to_int();
  }
  return ret;
}

int ObDirectLoadInsertTabletContext::init_datum_row(ObDatumRow &datum_row, const bool is_delete)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTabletContext not init", K(ret), KP(this));
  } else {
    const int64_t real_column_count =
      param_->column_count_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    if (OB_FAIL(datum_row.init(real_column_count))) {
      LOG_WARN("fail to init datum row", KR(ret), K(real_column_count));
    } else {
      const int64_t trans_version =
        !param_->is_incremental_ ? param_->snapshot_version_ : INT64_MAX;
      datum_row.trans_id_ = param_->trans_param_.tx_id_;
      datum_row.row_flag_.set_flag(
        is_delete ? ObDmlFlag::DF_DELETE : ObDmlFlag::DF_INSERT,
        // 只有增量inc_replace模式下的主表insert行需要DF_TYPE_INSERT_DELETE
        // * 目前这里没有细分增量inc和增量inc_replace
        // * 增量inc_replace带索引或lob会退化成增量inc
        (!param_->is_incremental_ || is_delete) ? DF_TYPE_NORMAL : DF_TYPE_INSERT_DELETE);
      datum_row.mvcc_row_flag_.set_compacted_multi_version_row(true);
      datum_row.mvcc_row_flag_.set_first_multi_version_row(true);
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

/**
 * ObDirectLoadInsertTableContext
 */

ObDirectLoadInsertTableContext::ObDirectLoadInsertTableContext()
  : allocator_("TLD_InsTblCtx"), safe_allocator_(allocator_), is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadInsertTableContext::~ObDirectLoadInsertTableContext() { reset(); }

void ObDirectLoadInsertTableContext::reset()
{
  if (tablet_ctx_map_.created()) {
    FOREACH(iter, tablet_ctx_map_)
    {
      ObDirectLoadInsertTabletContext *tablet_ctx = iter->second;
      tablet_ctx->~ObDirectLoadInsertTabletContext();
      allocator_.free(tablet_ctx);
    }
    tablet_ctx_map_.destroy();
  }
  allocator_.reset();
}

int ObDirectLoadInsertTableContext::inner_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_ctx_map_.create(1024, "TLD_InsTabCtx", "TLD_InsTabCtx", MTL_ID()))) {
    LOG_WARN("fail to create tablet ctx map", KR(ret));
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

void ObDirectLoadInsertTableContext::cancel()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    FOREACH(iter, tablet_ctx_map_)
    {
      ObDirectLoadInsertTabletContext *tablet_ctx = iter->second;
      tablet_ctx->cancel();
    }
  }
}

} // namespace storage
} // namespace oceanbase
