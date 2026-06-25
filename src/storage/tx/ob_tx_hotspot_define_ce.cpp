/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OB_HOTSPOT_GROUP_COMMIT

#include "storage/tx/ob_tx_hotspot_define.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace transaction
{

const char *to_cstr(const TxRedoFlushStatus &status)
{
  const char *cstr = nullptr;
  switch (status) {
  case TxRedoFlushStatus::NORMAL_START:
    cstr = "NORMAL_START";
    break;
  case TxRedoFlushStatus::NORMAL_FLUSHED:
    cstr = "NORMAL_FLUSHED";
    break;
  case TxRedoFlushStatus::PRIMARY_PREPARING:
    cstr = "PRIMARY_PREPARING";
    break;
  case TxRedoFlushStatus::PRIMARY_COLLECTING:
    cstr = "PRIMARY_COLLECTING";
    break;
  case TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED:
    cstr = "PRIMARY_AGGR_SUCCEEDED";
    break;
  case TxRedoFlushStatus::PRIMARY_AGGR_FAILED:
    cstr = "PRIMARY_AGGR_FAILED";
    break;
  case TxRedoFlushStatus::PRIMARY_COMPLETED:
    cstr = "PRIMARY_COMPLETED";
    break;
  case TxRedoFlushStatus::SECONDARY_PREPARING:
    cstr = "SECONDARY_PREPARING";
    break;
  case TxRedoFlushStatus::SECONDARY_MIGRATING:
    cstr = "SECONDARY_MIGRATING";
    break;
  case TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED:
    cstr = "SECONDARY_MIGRATE_SYNCED";
    break;
  case TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED:
    cstr = "SECONDARY_MIGRATE_FAILED";
    break;
  case TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED:
    cstr = "SECONDARY_MIGRATE_SUCCEEDED";
    break;
  default:
    cstr = "INVALID_STATUS";
    break;
  }
  return cstr;
}

const char *to_cstr(const TxSecondaryRespStatus &status)
{
  const char *cstr = nullptr;
  switch (status) {
  case TxSecondaryRespStatus::RESP_NOT_SENT:
    cstr = "RESP_NOT_SENT";
    break;
  case TxSecondaryRespStatus::RESP_COMMITTED:
    cstr = "RESP_COMMITTED";
    break;
  case TxSecondaryRespStatus::RESP_ABORTED_BY_SESSION:
    cstr = "RESP_ABORTED_BY_SESSION";
    break;
  case TxSecondaryRespStatus::RESP_SKIPPED_BY_PRIMARY:
    cstr = "RESP_SKIPPED_BY_PRIMARY";
    break;
  default:
    cstr = "INVALID_RESP_STATUS";
    break;
  }
  return cstr;
}

ObHotspotSchedulerResponseTask::~ObHotspotSchedulerResponseTask()
{
  destroy();
}

void ObHotspotSchedulerResponseTask::reset()
{
  primary_tx_ctx_ = nullptr;
  tx_result_ = OB_INVALID_ARGUMENT;
  commit_version_.reset();
  retry_cnt_ = 0;
  ATOMIC_STORE(&in_queue_, 0);
}

void ObHotspotSchedulerResponseTask::destroy()
{
  reset();
}

int ObHotspotSchedulerResponseTask::init(ObPartTransCtx *)
{
  return OB_NOT_SUPPORTED;
}

int ObHotspotSchedulerResponseTask::enable(const int, const share::SCN)
{
  return OB_NOT_SUPPORTED;
}

int ObHotspotSchedulerResponseTask::handle()
{
  return OB_NOT_SUPPORTED;
}

bool ObHotspotSchedulerResponseTask::is_in_queue() const
{
  return ATOMIC_LOAD(&in_queue_) != 0;
}

int ObHotspotSchedulerResponseTask::set_in_queue(const bool in_queue_status)
{
  ATOMIC_STORE(&in_queue_, in_queue_status ? 1 : 0);
  return OB_SUCCESS;
}

int ObTxHotspotRedoCacheHandle::insert_into(ObPartTransCtx *)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::append_hotspot_idle_cb(ObTxLogCb *, const int64_t)
{
  return OB_NOT_SUPPORTED;
}

bool ObTxHotspotRedoCacheHandle::need_rearrange() const
{
  return false;
}

int ObTxHotspotRedoCacheHandle::rearrange_hotspot_task()
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::extract_hotspot_redo(const int64_t)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::assign_remapped_seq_ranges(const ObTxSEQ, const ObTxSEQ)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::get_secondary_tx_redo_range(
    const int64_t, ObSecondaryTxRedoRange &, int64_t &)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::try_flush_hotspot_redo(const int64_t, ObTxRedoLog &, ObTxSEQ &)
{
  return OB_NOT_SUPPORTED;
}

void ObTxHotspotRedoCacheHandle::need_increase_logging_concurrency(int64_t &) const
{
}

ObTxSEQ ObTxHotspotRedoCacheHandle::get_max_sub_tx_seq_no() const
{
  return ObTxSEQ();
}

int64_t ObTxHotspotRedoCacheHandle::get_hotspot_cache_count() const
{
  return 0;
}

int64_t ObTxHotspotRedoCacheHandle::get_free_cb_count() const
{
  return 0;
}

int64_t ObTxHotspotRedoCacheHandle::get_busy_cb_count() const
{
  return 0;
}

void ObTxHotspotRedoCacheHandle::get_cb_list_count(
    int64_t &free_cb_cnt, int64_t &busy_cb_cnt, int64_t &idle_cb_cnt) const
{
  free_cb_cnt = 0;
  busy_cb_cnt = 0;
  idle_cb_cnt = 0;
}

ObTxHotspotRedoCacheHandle::HotspotLogCbView ObTxHotspotRedoCacheHandle::get_cb_list_snapshot() const
{
  HotspotLogCbView view = {0, 0, 0, true};
  return view;
}

share::SCN ObTxHotspotRedoCacheHandle::get_min_busy_log_ts() const
{
  return share::SCN();
}

share::SCN ObTxHotspotRedoCacheHandle::get_max_busy_log_ts() const
{
  return share::SCN();
}

palf::LSN ObTxHotspotRedoCacheHandle::get_max_busy_lsn() const
{
  return palf::LSN();
}

void ObTxHotspotRedoCacheHandle::get_max_busy_log_ts_and_lsn(share::SCN &log_ts, palf::LSN &lsn) const
{
  log_ts.reset();
  lsn.reset();
}

int ObTxHotspotRedoCacheHandle::get_free_cb(ObTxLogCb *&)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::add_busy_cb(ObTxLogCb *)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::return_hotspot_cb(ObTxLogCb *, const bool, const bool)
{
  return OB_NOT_SUPPORTED;
}

void ObTxHotspotRedoCacheHandle::compare_hotspot_cb(
    ObTxLogCb *&normal_log_cb, ObTxLogCb *&hotspot_log_cb, bool &is_hotspot_larger)
{
  UNUSED(normal_log_cb);
  UNUSED(hotspot_log_cb);
  is_hotspot_larger = true;
}

int ObTxHotspotRedoCacheHandle::check_status(const int64_t, bool &, bool &)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::after_flush_hotspot_redo(const int64_t, const share::SCN, const int)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::after_sync_hotspot_redo(const int64_t, const bool, const share::SCN)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::remove_synced_hotspot_redo(const int64_t, int64_t &)
{
  return OB_NOT_SUPPORTED;
}

void ObTxHotspotRedoCacheHandle::push_response_task(const int, const share::SCN)
{
}

bool ObTxHotspotRedoCacheHandle::all_redo_flushed() const
{
  return true;
}

bool ObTxHotspotRedoCacheHandle::all_redo_synced() const
{
  return true;
}

bool ObTxHotspotRedoCacheHandle::all_redo_frozen_flushed() const
{
  return true;
}

void ObTxHotspotRedoCacheHandle::clear_all_last_submit_ret_code()
{
}

void ObTxHotspotRedoCacheHandle::record_all_secondary_finished()
{
}

void ObTxHotspotRedoCacheHandle::inc_dispatch_msg_sent()
{
}

int64_t ObTxHotspotRedoCacheHandle::get_pending_dispatch_msg_cnt() const
{
  return 0;
}

int64_t ObTxHotspotRedoCacheHandle::get_pending_submit_other_redo_msg_cnt() const
{
  return 0;
}

void ObTxHotspotRedoCacheHandle::inc_pending_dispatch_msg_cnt()
{
}

void ObTxHotspotRedoCacheHandle::dec_pending_dispatch_msg_cnt()
{
}

void ObTxHotspotRedoCacheHandle::inc_pending_submit_other_redo_msg_cnt()
{
}

void ObTxHotspotRedoCacheHandle::dec_pending_submit_other_redo_msg_cnt()
{
}

int ObTxHotspotRedoCacheHandle::get_rollback_range(const ObTransID *, ObTxSEQ &, ObTxSEQ &) const
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::rollback_secondary_memtable_and_mark(const ObTransID &)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::try_release_idle_log_cb(ObPartTransCtx *)
{
  return OB_SUCCESS;
}

bool ObTxHotspotRedoCacheHandle::need_rollback_primary_tx() const
{
  return false;
}

int ObTxHotspotRedoCacheHandle::abort_secondary_txs(const int)
{
  return OB_NOT_SUPPORTED;
}

int ObTxHotspotRedoCacheHandle::cleanup_hotspot_cache_and_revert_secondary_ctxs()
{
  return OB_SUCCESS;
}

int ObTxHotspotRedoCacheHandle::response_scheduler(const int, const share::SCN)
{
  return OB_NOT_SUPPORTED;
}

void ObTxHotspotRedoCacheHandle::record_aggr_start_ts()
{
}

void ObTxHotspotRedoCacheHandle::record_dispatch_start_ts()
{
}

void ObTxHotspotRedoCacheHandle::record_aggr_end_ts()
{
}

int64_t ObTxHotspotRedoCacheHandle::get_aggr_end_ts() const
{
  return OB_INVALID_TIMESTAMP;
}

void ObTxHotspotRedoCacheHandle::set_terminal_ret(const int)
{
}

int ObTxHotspotRedoCacheHandle::get_terminal_ret() const
{
  return OB_SUCCESS;
}

void ObTxHotspotRedoCacheHandle::set_terminal_ret_if_success(const int)
{
}

void ObTxHotspotRedoCacheHandle::inc_freeze_accel()
{
}

void ObTxHotspotRedoCacheHandle::inc_dispatch_msg_skipped()
{
}

void ObTxHotspotRedoCacheHandle::inc_submit_block_frozen()
{
}

void ObTxHotspotRedoCacheHandle::inc_cb_alloc_fail()
{
}

int ObTxHotspotRedoCacheHandle::reuse()
{
  cache_ = nullptr;
  primary_last_seq_no_ = ObTxSEQ(1, 0);
  if (!resp_task_.is_in_queue()) {
    resp_task_.reset();
  }
  return OB_SUCCESS;
}

void ObTxHotspotRedoCacheHandle::reset()
{
  (void)reuse();
}

void ObTxHotspotRedoCacheHandle::set_primary_last_seq_no(const ObTxSEQ &seq)
{
  primary_last_seq_no_.inc_update(seq);
}

ObTxSEQ ObTxHotspotRedoCacheHandle::get_primary_last_seq_no() const
{
  return primary_last_seq_no_.atomic_load();
}

} // namespace transaction
} // namespace oceanbase

#endif // OB_HOTSPOT_GROUP_COMMIT
