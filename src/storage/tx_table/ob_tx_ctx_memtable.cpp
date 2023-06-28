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

#include "storage/tx_table/ob_tx_ctx_memtable.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_tablet_merge_task.h"

namespace oceanbase
{
using namespace share;
using namespace palf;

namespace storage
{

ObTxCtxMemtable::ObTxCtxMemtable()
  : ObIMemtable(),
    is_inited_(false),
    is_frozen_(false),
    ls_ctx_mgr_guard_(),
    flush_lock_()
{
}

ObTxCtxMemtable::~ObTxCtxMemtable()
{
  reset();
}

void ObTxCtxMemtable::reset()
{
  ls_ctx_mgr_guard_.reset();
  ls_id_.reset();
  ObITable::reset();
  is_frozen_ = false;
  is_inited_ = false;
}

int ObTxCtxMemtable::init(const ObITable::TableKey &table_key,
                          const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init tx ctx memtable twice", KR(ret));
  } else if (OB_FAIL(ObITable::init(table_key))) {
    STORAGE_LOG(WARN, "ObITable::init fail");
  } else if (OB_FAIL(ls_ctx_mgr_guard_.init(ls_id))) {
    STORAGE_LOG(WARN, "ls ctx mgr guard acquire ref failed", K(ret), K(ls_id));
  } else {
    ls_id_ = ls_id;
    is_inited_ = true;
    TRANS_LOG(INFO, "ob tx ctx memtable init successfully", K(ls_id), K(table_key));
  }

  return ret;
}

int ObTxCtxMemtable::scan(const ObTableIterParam &param,
                          ObTableAccessContext &context,
                          const blocksstable::ObDatumRange &key_range,
                          ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  ObTxCtxMemtableScanIterator *scan_iter_ptr = nullptr;
  void *scan_iter_buff = nullptr;
  UNUSED(key_range);
  UNUSED(context);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ob tx ctx memtable is not inited.", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid param", KR(ret), K(param), K(context));
  } else if (OB_UNLIKELY(!param.is_multi_version_minor_merge_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ObTxCtxMemtable only support scan for minor merge", KR(ret), K(param));
  } else if (OB_ISNULL(scan_iter_buff
                       = context.stmt_allocator_->alloc(sizeof(ObTxCtxMemtableScanIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "construct ObTxCtxMemtableScanIterator fail", "scan_iter_buffer",
                scan_iter_buff, "scan_iter_ptr", scan_iter_ptr, KR(ret));
  } else if (FALSE_IT(scan_iter_ptr = new (scan_iter_buff) ObTxCtxMemtableScanIterator())) {
  } else if (OB_FAIL(scan_iter_ptr->init(this))) {
    STORAGE_LOG(WARN, "init scan_iter_ptr fail.", KR(ret), K(context));
  } else {
    // tx ctx memtable scan iterator init success
    row_iter = scan_iter_ptr;
    TRANS_LOG(INFO, "ob tx ctx memtable scan successfully", KPC(this));
  }

  return ret;
}

int ObTxCtxMemtable::get(const storage::ObTableIterParam &param,
                         storage::ObTableAccessContext &context,
                         const blocksstable::ObDatumRowkey &rowkey,
                         blocksstable::ObDatumRow &row)
{
  UNUSED(param);
  UNUSED(context);
  UNUSED(rowkey);
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}

int ObTxCtxMemtable::get(const storage::ObTableIterParam &param,
                         storage::ObTableAccessContext &context,
                         const blocksstable::ObDatumRowkey &rowkey,
                         ObStoreRowIterator *&row_iter)
{
  UNUSED(param);
  UNUSED(context);
  UNUSED(rowkey);
  UNUSED(row_iter);
  return OB_NOT_SUPPORTED;
}

int ObTxCtxMemtable::multi_get(const ObTableIterParam &param,
                               ObTableAccessContext &context,
                               const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
                               ObStoreRowIterator *&row_iter)
{
  UNUSED(param);
  UNUSED(context);
  UNUSED(rowkeys);
  UNUSED(row_iter);
  return OB_NOT_SUPPORTED;
}

int ObTxCtxMemtable::multi_scan(const ObTableIterParam &param,
                                ObTableAccessContext &context,
                                const common::ObIArray<blocksstable::ObDatumRange> &ranges,
                                ObStoreRowIterator *&row_iter)
{
  UNUSED(param);
  UNUSED(context);
  UNUSED(ranges);
  UNUSED(row_iter);
  return OB_NOT_SUPPORTED;
}

int ObTxCtxMemtable::get_frozen_schema_version(int64_t &schema_version) const
{
  UNUSED(schema_version);
  return OB_NOT_SUPPORTED;
}

bool ObTxCtxMemtable::can_be_minor_merged()
{
  return true;
}

transaction::ObLSTxCtxMgr *ObTxCtxMemtable::get_ls_tx_ctx_mgr()
{
  return ls_ctx_mgr_guard_.get_ls_tx_ctx_mgr();
}

SCN ObTxCtxMemtable::get_rec_scn()
{
  int ret = OB_SUCCESS;
  SCN rec_scn;

  if (OB_FAIL(get_ls_tx_ctx_mgr()->get_rec_scn(rec_scn))) {
    TRANS_LOG(WARN, "get rec scn failed", K(ret));
  } else {
    TRANS_LOG(INFO, "tx ctx memtable get rec scn", KPC(this), K(rec_scn));
  }

  return rec_scn;
}

ObTabletID ObTxCtxMemtable::get_tablet_id() const
{
  return LS_TX_CTX_TABLET;
}

bool ObTxCtxMemtable::is_flushing() const
{
  return ATOMIC_LOAD(&is_frozen_);
}

int ObTxCtxMemtable::on_memtable_flushed()
{
  ATOMIC_STORE(&is_frozen_, false);
  return get_ls_tx_ctx_mgr()->on_tx_ctx_table_flushed();
}

bool ObTxCtxMemtable::is_frozen_memtable() const
{
  return ATOMIC_LOAD(&is_frozen_);
}

bool ObTxCtxMemtable::is_active_memtable() const
{
  return !ATOMIC_LOAD(&is_frozen_);
}

int ObTxCtxMemtable::flush(SCN recycle_scn, bool need_freeze)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(flush_lock_);

  if (need_freeze) {
    SCN rec_scn = get_rec_scn();
    if (rec_scn >= recycle_scn) {
      TRANS_LOG(INFO, "no need to freeze", K(rec_scn), K(recycle_scn));
    } else if (is_active_memtable()) {
      int64_t cur_time_us = ObTimeUtility::current_time();
      ObScnRange scn_range;
      scn_range.start_scn_.set_base();
      if (OB_FAIL(scn_range.end_scn_.convert_for_tx(cur_time_us))) {
        TRANS_LOG(WARN, "failed to convert_from_ts", K(ret), K(cur_time_us));
      } else {
        set_scn_range(scn_range);
        set_snapshot_version(scn_range.end_scn_);
        ATOMIC_STORE(&is_frozen_, true);
      }
    }
  }

  if (OB_SUCC(ret) && is_frozen_memtable()) {
    compaction::ObTabletMergeDagParam param;
    param.ls_id_ = ls_id_;
    param.tablet_id_ = LS_TX_CTX_TABLET;
    param.merge_type_ = MINI_MERGE;
    param.merge_version_ = ObVersionRange::MIN_VERSION;
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tx_table_merge_dag(param))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
          TRANS_LOG(WARN, "failed to schedule tablet merge dag", K(ret));
      }
    } else {
      TRANS_LOG(INFO, "tx ctx memtable flush successfully", KPC(this), K(ls_id_));
    }
  }
  
  return ret;
}

} // namespace storage
} // namespace oceanbase
