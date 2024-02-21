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

#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_service.h"
#include "src/storage/tx/ob_ts_mgr.h"
#include "storage/tx/wrs/ob_weak_read_util.h"

namespace oceanbase
{
namespace concurrency_control
{

int64_t ObMultiVersionGarbageCollector::GARBAGE_COLLECT_PRECISION = 1_s;
int64_t ObMultiVersionGarbageCollector::GARBAGE_COLLECT_RETRY_INTERVAL = 1_min;
int64_t ObMultiVersionGarbageCollector::GARBAGE_COLLECT_EXEC_INTERVAL = 10 * GARBAGE_COLLECT_RETRY_INTERVAL;
int64_t ObMultiVersionGarbageCollector::GARBAGE_COLLECT_RECLAIM_DURATION = 3 * GARBAGE_COLLECT_EXEC_INTERVAL;

ObMultiVersionGarbageCollector::ObMultiVersionGarbageCollector()
  : timer_(),
    timer_handle_(),
    last_study_timestamp_(0),
    last_refresh_timestamp_(0),
    last_reclaim_timestamp_(0),
    last_sstable_overflow_timestamp_(0),
    has_error_when_study_(false),
    refresh_error_too_long_(false),
    has_error_when_reclaim_(false),
    gc_is_disabled_(false),
    global_reserved_snapshot_(share::SCN::min_scn()),
    is_inited_(false) {}

ObMultiVersionGarbageCollector::~ObMultiVersionGarbageCollector() {}

int ObMultiVersionGarbageCollector::mtl_init(ObMultiVersionGarbageCollector *&m)
{
  return m->init();
}

int ObMultiVersionGarbageCollector::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    MVCC_LOG(WARN, "ObMultiVersionGarbageCollector init twice", K(ret), KP(this));
  } else {
    last_study_timestamp_ = 0;
    last_refresh_timestamp_ = 0;
    last_reclaim_timestamp_ = 0;
    last_sstable_overflow_timestamp_ = 0;
    has_error_when_study_ = false;
    refresh_error_too_long_ = false;
    has_error_when_reclaim_ = false;
    gc_is_disabled_ = false;
    global_reserved_snapshot_ = share::SCN::min_scn();
    is_inited_ = true;
    MVCC_LOG(INFO, "multi version garbage collector init", KP(this));
  }
  return ret;
}

void ObMultiVersionGarbageCollector::cure()
{
  last_study_timestamp_ = 0;
  last_refresh_timestamp_ = 0;
  last_reclaim_timestamp_ = 0;
  last_sstable_overflow_timestamp_ = 0;
  has_error_when_study_ = false;
  refresh_error_too_long_ = false;
  has_error_when_reclaim_ = false;
  gc_is_disabled_ = false;
  global_reserved_snapshot_ = share::SCN::min_scn();
}

int ObMultiVersionGarbageCollector::start()
{
  int ret = OB_SUCCESS;

  if(!is_inited_) {
    ret = OB_NOT_INIT;
    MVCC_LOG(ERROR, "has not been inited", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(timer_.init_and_start(1                         /*worker_num*/,
                                           GARBAGE_COLLECT_PRECISION /*precision*/,
                                           "MultiVersionGC"          /*label*/))) {
    MVCC_LOG(ERROR, "fail to init and start timer", KR(ret), KPC(this));
  } else if (OB_FAIL(timer_.schedule_task_repeat(
                       timer_handle_,
                       GARBAGE_COLLECT_RETRY_INTERVAL, /*interval*/
                       [this]() { /*task*/
                         int ret = OB_SUCCESS;
                         uint64_t data_version = 0;
                         omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
                         if (!tenant_config->_mvcc_gc_using_min_txn_snapshot) {
                           cure();
                         } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(MTL_ID()),
                                                                 data_version))) {
                           MVCC_LOG(WARN, "get min data version failed", KR(ret),
                                    K(gen_meta_tenant_id(MTL_ID())));
                         } else if (data_version >= DATA_VERSION_4_1_0_0) {
                           // compatibility is important
                           (void)repeat_study();
                           (void)repeat_refresh();
                           (void)repeat_reclaim();
                         }
                         return false; }))) {
    MVCC_LOG(ERROR, "schedule repeat task failed", KR(ret), KPC(this));
  } else {
    MVCC_LOG(INFO, "multi version garbage collector start", KPC(this),
             K(GARBAGE_COLLECT_RETRY_INTERVAL), K(GARBAGE_COLLECT_EXEC_INTERVAL),
             K(GARBAGE_COLLECT_PRECISION), K(GARBAGE_COLLECT_RECLAIM_DURATION));
  }

  return ret;
}

int ObMultiVersionGarbageCollector::stop()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    MVCC_LOG(WARN, "ObCheckPointService is not initialized", K(ret));
  } else {
    ObTimeGuard timeguard(__func__, 1 * 1000 * 1000);
    (void)timer_handle_.stop_and_wait();
    timer_.stop();
    last_study_timestamp_ = 0;
    last_refresh_timestamp_ = 0;
    last_reclaim_timestamp_ = 0;
    last_sstable_overflow_timestamp_ = 0;
    has_error_when_study_ = false;
    refresh_error_too_long_ = false;
    has_error_when_reclaim_ = false;
    gc_is_disabled_ = false;
    global_reserved_snapshot_ = share::SCN::min_scn();
    is_inited_ = false;
    MVCC_LOG(INFO, "multi version garbage collector stop", KPC(this));
  }

  return ret;
}

void ObMultiVersionGarbageCollector::wait()
{
  timer_.wait();
  MVCC_LOG(INFO, "multi version garbage collector wait", KPC(this));
}

void ObMultiVersionGarbageCollector::destroy()
{
  timer_.destroy();
  MVCC_LOG(INFO, "multi version garbage collector destroy", KPC(this));
}

// study means learning for the different ObMultiVersionSnapshotType and
// reporting to the inner table. And the repeat_study will study each time when
// meeting the error or meeting the time requirement.
void ObMultiVersionGarbageCollector::repeat_study()
{
  int ret = OB_SUCCESS;
  const int64_t current_timestamp = ObClockGenerator::getRealClock();

  if (has_error_when_study_  // enconter error during last study
      // study every 10 min(default of GARBAGE_COLLECT_EXEC_INTERVAL)
      || current_timestamp - last_study_timestamp_ > GARBAGE_COLLECT_EXEC_INTERVAL) {
    if (OB_FAIL(study())) {
      has_error_when_study_ = true;
      if (current_timestamp - last_study_timestamp_ > 10 * GARBAGE_COLLECT_EXEC_INTERVAL
          && 0 != last_study_timestamp_
          // for mock or test that change GARBAGE_COLLECT_EXEC_INTERVAL to a small value
          && current_timestamp - last_study_timestamp_ > 10 * 10_min) {
        MVCC_LOG(ERROR, "repeat study failed too much time", K(ret),
                 KPC(this), K(current_timestamp));
      } else {
        MVCC_LOG(WARN, "repeat study failed, we will retry immediately", K(ret),
                 KPC(this), K(current_timestamp));
      }
    } else {
      has_error_when_study_ = false;
      last_study_timestamp_ = common::ObTimeUtility::current_time();
      MVCC_LOG(INFO, "repeat study successfully", K(ret), KPC(this),
               K(current_timestamp), K(GARBAGE_COLLECT_EXEC_INTERVAL));
    }
  } else {
    MVCC_LOG(INFO, "skip repeat study", K(ret), KPC(this),
             K(current_timestamp), K(GARBAGE_COLLECT_EXEC_INTERVAL));
  }
}

// collect means collection for the different ObMultiVersionSnapshotType from
// the inner table. And the repeat_collect will study each time when meeting the
// time requirement.
void ObMultiVersionGarbageCollector::repeat_refresh()
{
  int ret = OB_SUCCESS;
  const int64_t current_timestamp = ObClockGenerator::getRealClock();

  // collect every 1 min(default of GARBAGE_COLLECT_RETRY_INTERVAL)
  if (OB_FAIL(refresh_())) {
    if (is_refresh_fail() ||
        (current_timestamp - last_refresh_timestamp_ > 30 * GARBAGE_COLLECT_RETRY_INTERVAL
         && 0 != last_refresh_timestamp_
         // for mock or test that change GARBAGE_COLLECT_RETRY_INTERVAL to a small value
         && current_timestamp - last_refresh_timestamp_ > 30 * 1_min)) {
      // the server may cannot contact to the inner table and prevent reserved
      // snapshot from advancing. We think the multi-version data on this server
      // is not reachable for all active txns, so we gives up to use follow the
      // rules of multi-version garbage collector and use the undo_retention and
      // snapshot_gc_ts as the new mechanism to guarantee the recycle of data.
      refresh_error_too_long_ = true;
      MVCC_LOG(ERROR, "repeat refresh failed too much time", K(ret),
               KPC(this), K(current_timestamp));
    } else {
      MVCC_LOG(WARN, "repeat refresh failed, we will retry immediately", K(ret),
               KPC(this), K(current_timestamp));
    }
  } else {
    refresh_error_too_long_ = false;
    last_refresh_timestamp_ = common::ObTimeUtility::current_time();
    MVCC_LOG(INFO, "repeat refresh successfully", K(ret), KPC(this),
             K(current_timestamp), K(GARBAGE_COLLECT_RETRY_INTERVAL));
  }
}

// reclaim means collecting and reclaiming the expired entries in the inner
// table. And the repeat_reclaim will relaim each time we meeting the error or
// meeting the time requirement.
void ObMultiVersionGarbageCollector::repeat_reclaim()
{
  int ret = OB_SUCCESS;
  const int64_t current_timestamp = ObClockGenerator::getRealClock();

  if (has_error_when_reclaim_  // enconter error during last reclaim
      // reclaim every 10 min(default of GARBAGE_COLLECT_EXEC_INTERVAL)
      || current_timestamp - last_reclaim_timestamp_ > GARBAGE_COLLECT_EXEC_INTERVAL) {
    if (OB_FAIL(reclaim())) {
      has_error_when_reclaim_ = true;
      if (current_timestamp - last_reclaim_timestamp_ > 10 * GARBAGE_COLLECT_EXEC_INTERVAL
          && 0 != last_reclaim_timestamp_
          // for mock or test that change GARBAGE_COLLECT_EXEC_INTERVAL to a small value
          && current_timestamp - last_reclaim_timestamp_ > 10 * 10_min) {
        MVCC_LOG(ERROR, "repeat reclaim failed too much time", K(ret),
                 KPC(this), K(current_timestamp));
      } else {
        MVCC_LOG(WARN, "repeat reclaim failed, we will retry immediately", K(ret),
                 KPC(this), K(current_timestamp));
      }
    } else {
      has_error_when_reclaim_ = false;
      last_reclaim_timestamp_ = common::ObTimeUtility::current_time();
      MVCC_LOG(INFO, "repeat reclaim successfully", K(ret), KPC(this),
               K(current_timestamp), K(GARBAGE_COLLECT_EXEC_INTERVAL));
    }
  } else {
    MVCC_LOG(INFO, "skip repeat reclaim", K(ret), KPC(this),
             K(current_timestamp), K(GARBAGE_COLLECT_EXEC_INTERVAL));
  }
}

// According to the requirement of the multi-version garbage collector and the
// document
// four timestamp from each OceanBase node:
// 1. The timestamp of the minimum unallocation GTS
// 2. The timestamp of the minimum unallocation WRS
// 3. The maximum commit timestamp of each node
// 4. The minimum snapshot of the active txns of each node
//
//   |                                           |
//   +--t1----C----------------------t2--------->| Machine1
//   |                                           |
//   |                                           |
//   +-------------------C---------------------->| Machine2
//   |                                           |
//   |                                           |
//   +----------------------------------C------->| Machine3
//   |                                           |
//           GARBAGE_COLLECT_EXEC_INTERVAL
//
// Let's watch for the above example, three machines may each report(the action
// "C"" in the above picture) its timestamps to inner table at different times
// during the GARBAGE_COLLECT_EXEC_INTERVAL. We can think the questions basing on
// one of the instance. Let's go for the machine1:
// (Let's think that a txn must only starts on one of the machine(called TxDesc))
// 1. If the txn starts before the report started:
//   a. If the txn has finished before C, we need not take it into consideration.
//   b. If the txn has not finished, we will consider it with the above timestamp 4.
// 2. If the txn starts after the report started:
//   a. The txn using GTS as snapshot, we will consider it with the above timestamp 1.
//   b. The txn using WRS as snapshot, we will consider it with the above timestamp 2.
//   c. The txn using max committed version as snapshot, we will consider it with the
//      above timestamp 3.
//
// So if we generalize all machines using the above rules, all txns started on known
// machine will be taken into condsideration based on our alogorithm
//
// NB: So we must insert or update the 4 entries atomically for the correctness.
int ObMultiVersionGarbageCollector::study()
{
  int ret = OB_SUCCESS;
  share::SCN min_unallocated_GTS(share::SCN::max_scn());
  share::SCN min_unallocated_WRS(share::SCN::max_scn());
  share::SCN max_committed_txn_version(share::SCN::max_scn());
  share::SCN min_active_txn_version(share::SCN::max_scn());

  ObTimeGuard timeguard(__func__, 1 * 1000 * 1000);

  // standby cluster uses the same interface for GTS
  if (OB_FAIL(study_min_unallocated_GTS(min_unallocated_GTS))) {
    MVCC_LOG(WARN, "study min unallocated GTS failed", K(ret));
  } else if (!min_unallocated_GTS.is_valid()
             || min_unallocated_GTS.is_min()
             || min_unallocated_GTS.is_max()) {
    ret = OB_ERR_UNEXPECTED;
    MVCC_LOG(ERROR, "wrong min unallocated GTS",
             K(ret), K(min_unallocated_GTS), KPC(this));
  } else {
    MVCC_LOG(INFO, "study min unallocated gts succeed",
             K(ret), K(min_unallocated_GTS), KPC(this));
  }

  timeguard.click("study_min_unallocated_GTS");

  if (OB_SUCC(ret)) {
    if (!GCTX.is_standby_cluster() && // standby cluster does not support WRS
        OB_FAIL(study_min_unallocated_WRS(min_unallocated_WRS))) {
      MVCC_LOG(WARN, "study min unallocated GTS failed", K(ret));
    } else if (!min_unallocated_WRS.is_valid()
               || min_unallocated_WRS.is_min()
               || min_unallocated_WRS.is_max()) {
      ret = OB_ERR_UNEXPECTED;
      MVCC_LOG(ERROR, "wrong min unallocated WRS",
               K(ret), K(min_unallocated_WRS), KPC(this));
    } else {
      MVCC_LOG(INFO, "study min unallocated wrs succeed",
               K(ret), K(min_unallocated_WRS), KPC(this));
    }
  }

  timeguard.click("study_min_unallocated_WRS");

  if (OB_SUCC(ret)) {
    if (OB_FAIL(study_max_committed_txn_version(max_committed_txn_version))) {
      MVCC_LOG(WARN, "study max committed txn version failed", K(ret));
    } else if (!max_committed_txn_version.is_valid()
               || max_committed_txn_version.is_max()) {
      ret = OB_ERR_UNEXPECTED;
      MVCC_LOG(ERROR, "wrong max committed txn version",
               K(ret), K(max_committed_txn_version), KPC(this));
    } else {
      MVCC_LOG(INFO, "study max committed txn version succeed",
               K(ret), K(max_committed_txn_version), KPC(this));
    }
  }

  timeguard.click("study_max_commited_txn_version");

  if (OB_SUCC(ret)) {
    if (OB_FAIL(study_min_active_txn_version(min_active_txn_version))) {
      MVCC_LOG(WARN, "study min active txn version failed", K(ret));
    } else {
      MVCC_LOG(INFO, "study min active txn version succeed",
               K(ret), K(min_active_txn_version), KPC(this));
    }
  }

  timeguard.click("study_min_active_txn_version");

  if (OB_SUCC(ret) &&
      can_report() &&
      OB_FAIL(report(min_unallocated_GTS,
                     min_unallocated_WRS,
                     max_committed_txn_version,
                     min_active_txn_version))) {
    MVCC_LOG(WARN, "report garbage collect info failed", K(ret));
  }

  timeguard.click("report");

  MVCC_LOG(INFO, "study multi version garabage collector end",
           K(ret), KPC(this), K(min_unallocated_GTS), K(min_unallocated_GTS),
           K(max_committed_txn_version), K(min_active_txn_version));

  return ret;
}

// The read snapshot version may base on GTS for most txns, so we need study it on each machine.
int ObMultiVersionGarbageCollector::study_min_unallocated_GTS(share::SCN &min_unallocated_GTS)
{
  int ret = OB_SUCCESS;

  const transaction::MonotonicTs stc_ahead = transaction::MonotonicTs::current_time() ;
  transaction::MonotonicTs unused_receive_gts_ts(0);
  const int64_t timeout_us = 1 * 1000 * 1000; // 1s
  const int64_t expire_time_us = common::ObTimeUtility::current_time() + timeout_us;
  share::SCN gts_scn;

  do {
    // We get the gts each 10ms in order not to report too much error during reboot
    // and continue to fetch under the failure among 1s.
    ret = OB_TS_MGR.get_gts(MTL_ID(),
                            stc_ahead,
                            NULL, // gts task
                            gts_scn,
                            unused_receive_gts_ts);
    if (ret == OB_EAGAIN) {
      if (common::ObTimeUtility::current_time() > expire_time_us) {
        ret = OB_TIMEOUT;
      } else {
        ob_usleep(10 * 1000/*10ms*/);
      }
    } else if (OB_FAIL(ret)) {
      MVCC_LOG(WARN, "get gts fail", KR(ret));
    } else if (!gts_scn.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MVCC_LOG(ERROR, "get gts fail", K(gts_scn), K(ret));
    } else {
      min_unallocated_GTS = gts_scn;
    }
  } while (ret == OB_EAGAIN);

  return ret;
}

// The read snapshot version may base on WRS for the boundary weak read txn, so we
// need study it on each machine.
int ObMultiVersionGarbageCollector::study_min_unallocated_WRS(
  share::SCN &min_unallocated_WRS)
{
  int ret = OB_SUCCESS;

  const int64_t current_time = ObTimeUtility::current_time();
  const int64_t max_read_stale_time =
    transaction::ObWeakReadUtil::max_stale_time_for_weak_consistency(MTL_ID());

  if (OB_FAIL(MTL(transaction::ObTransService*)->get_weak_read_snapshot_version(
                -1, // system variable : max read stale time for user
                false,
                min_unallocated_WRS))) {
    MVCC_LOG(WARN, "fail to get weak read snapshot", K(ret));
    if (OB_REPLICA_NOT_READABLE == ret) {
      // The global weak read service cannot provide services in some cases(for
      // example backup cluster's weak read service may hung during recovery).
      // So instead of report the error, we decide to use the max allowed stale
      // time for garbage collector.
      min_unallocated_WRS.convert_from_ts(current_time - max_read_stale_time);
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

// The read snapshot version may base on max committed txn version for the
// single ls txn, so we need study it on each machine.
int ObMultiVersionGarbageCollector::study_max_committed_txn_version(
  share::SCN &max_committed_txn_version)
{
  int ret = OB_SUCCESS;

  max_committed_txn_version = MTL(transaction::ObTransService*)->
    get_tx_version_mgr().get_max_commit_ts(false/*elr*/);

  if (max_committed_txn_version.is_base_scn()) {
    // if the max committed txn version is base_scn(not updated by any txns and
    // async loop worker), we need ignore it and retry the next time
    ret = OB_EAGAIN;
    MVCC_LOG(WARN, "get max committed txn version is base version",
             K(ret), K(max_committed_txn_version));
  }

  return ret;
}

// We need collect all active txns, so decide to collect all snapshot version on
// one machine through tranversing the sessions. Lets' show all possibilities of
// the txns:
// 1. RR/SI, AC=0 txn: it will create the session with tx_desc on the scheduler
//      and record the snapshot_version on it. We can directly use it.
// 2. RC, AC=0 txn: it will create the session with tx_desc on the scheduler
//      while not recording the snapshot_version. We currently use session_state
//      and query_start_ts to act as the alive stmt snapshot.
//      TODO(handora.qc): record the snapshot version to tx_desc in the feture.
// 3. AC=1 txn: it may contain no tx_desc on session, while it must create session.
//      Even for remote execution, it will create session on the execution machine.
int ObMultiVersionGarbageCollector::study_min_active_txn_version(
  share::SCN &min_active_txn_version)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    MVCC_LOG(WARN, "session mgr is nullptr");
  } else if (OB_FAIL(GCTX.session_mgr_->
                     get_min_active_snapshot_version(min_active_txn_version))) {
    MVCC_LOG(WARN, "get min active snaphot version failed", K(ret));
  }

  return ret;
}

// refresh_ will timely collect the multi-version garbage collection info from
// inner table. We need take three factors into consideration. 1, GC may be
// disabled by disk monitor; 2, gc info may go back, and we should ignore these
// value; 3, gc may use INT64_MAX under exceptional condition, and we also need
// ignore it.
int ObMultiVersionGarbageCollector::refresh_()
{
  int ret = OB_SUCCESS;
  concurrency_control::ObMultiVersionGCSnapshotCalculator collector;

  ObTimeGuard timeguard(__func__, 1 * 1000 * 1000);

  if (is_refresh_fail()) {
    ret = OB_EAGAIN;
    MVCC_LOG(WARN, "mock refresh failed", K(ret), KPC(this), K(collector));
  } else if (OB_FAIL(MTL(concurrency_control::ObMultiVersionGarbageCollector *)->collect(collector))) {
    MVCC_LOG(WARN, "collect snapshot info sql failed", K(ret), KPC(this), K(collector));
  } else {
    // Step1: check whether gc status is disabled, then set or reset the gc
    // status based on the collector's result;
    decide_gc_status_(collector.get_status());
    timeguard.click("decide_gc_status_");

    // Step2: whether gc status is wrong or not on the server, we need refresh
    // it continuously. We will ignore the return code because it will not
    // effect the refresh result.
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(disk_monitor_(collector.is_this_server_disabled()))) {
      MVCC_LOG(WARN, "disk mintor failed", KPC(this), K(collector), K(tmp_ret));
    }

    timeguard.click("disk_mointor_");

    // Step3: cache the reserved snapshot of active txn for future use.
    // NB: be care of the lower value and maximum value which is not reasonable
    decide_reserved_snapshot_version_(collector.get_reserved_snapshot_version(),
                                      collector.get_reserved_snapshot_type());

    timeguard.click("decide_reserved_snapshot_");

    MVCC_LOG(INFO, "multi-version garbage collector refresh successfully",
             KPC(this), K(collector));
  }

  return ret;
}

void ObMultiVersionGarbageCollector::decide_gc_status_(const ObMultiVersionGCStatus gc_status)
{
  if (gc_status & ObMultiVersionGCStatus::DISABLED_GC_STATUS) {
    MVCC_LOG_RET(WARN, OB_ERR_UNEXPECTED, "gc status is disabled", KPC(this),
             K(global_reserved_snapshot_), K(gc_status));
    gc_is_disabled_ = true;
  } else if (gc_is_disabled_) {
    MVCC_LOG(INFO, "gc status is enabled", KPC(this),
             K(global_reserved_snapshot_), K(gc_status));
    gc_is_disabled_ = false;
  }
}

void ObMultiVersionGarbageCollector::decide_reserved_snapshot_version_(
  const share::SCN reserved_snapshot,
  const ObMultiVersionSnapshotType reserved_type)
{
  int ret = OB_SUCCESS;

  if (!reserved_snapshot.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MVCC_LOG(ERROR, "reserved is not valid", K(ret), KPC(this),
             K(global_reserved_snapshot_), K(reserved_snapshot));
  } else if (reserved_snapshot < global_reserved_snapshot_) {
    if ((global_reserved_snapshot_.get_val_for_tx() -
         reserved_snapshot.get_val_for_tx()) / 1000 > 30 * 1_min) {
      // We ignore the reserved snapshot with too late snapshot and report WARN
      // because there may be servers offline and online suddenly and report a
      // stale txn version. And we report error for a too too old snapshot.
      // NB: There may be WRS service which disables the monotonic weak read and
      // finally causes the timestamp to go back, so we should ignore it.
      if (ObMultiVersionSnapshotType::MIN_UNALLOCATED_WRS == reserved_type
          && !transaction::ObWeakReadUtil::enable_monotonic_weak_read(MTL_ID())) {
        MVCC_LOG(WARN, "update a smaller reserved snapshot with wrs disable monotonic weak read",
                 K(ret), KPC(this), K(global_reserved_snapshot_), K(reserved_snapshot));
      } else if (ObMultiVersionSnapshotType::MIN_UNALLOCATED_WRS == reserved_type
                 && ((global_reserved_snapshot_.get_val_for_tx() -
                      reserved_snapshot.get_val_for_tx()) / 1000 >
                     MAX(transaction::ObWeakReadUtil::max_stale_time_for_weak_consistency(MTL_ID()),
                         100 * 1_min))) {
        MVCC_LOG(ERROR, "update a too too smaller reserved snapshot with wrs!!!",
                 K(ret), KPC(this), K(global_reserved_snapshot_), K(reserved_snapshot),
                 K(transaction::ObWeakReadUtil::max_stale_time_for_weak_consistency(MTL_ID())));
      } else if ((global_reserved_snapshot_.get_val_for_tx() -
                  reserved_snapshot.get_val_for_tx()) / 1000 > 100 * 1_min) {
        MVCC_LOG(WARN, "update a too too smaller reserved snapshot!!!", K(ret), KPC(this),
                 K(global_reserved_snapshot_), K(reserved_snapshot));
      } else {
        MVCC_LOG(WARN, "update a too smaller reserved snapshot!", K(ret), KPC(this),
                 K(global_reserved_snapshot_), K(reserved_snapshot));
      }
    } else {
      MVCC_LOG(WARN, "update a smaller reserved snapshot", K(ret), KPC(this),
               K(global_reserved_snapshot_), K(reserved_snapshot));
    }
  } else if (reserved_snapshot.is_max()) {
    MVCC_LOG(WARN, "reserved snapshot is max value", K(ret), KPC(this),
             K(global_reserved_snapshot_), K(reserved_snapshot));
  } else {
    MVCC_LOG(INFO, "succeed to update global reserved snapshot", K(ret), KPC(this),
             K(global_reserved_snapshot_), K(reserved_snapshot));
    global_reserved_snapshot_.atomic_set(reserved_snapshot);
  }
}

share::SCN ObMultiVersionGarbageCollector::get_reserved_snapshot_for_active_txn() const
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));

  if (!tenant_config->_mvcc_gc_using_min_txn_snapshot) {
    return share::SCN::max_scn();
  } else if (refresh_error_too_long_) {
    if (REACH_TENANT_TIME_INTERVAL(1_s)) {
      MVCC_LOG_RET(WARN, OB_ERR_UNEXPECTED, "get reserved snapshot for active txn with long not updated", KPC(this));
    }
    return share::SCN::max_scn();
  } else if (gc_is_disabled_) {
    if (REACH_TENANT_TIME_INTERVAL(1_s)) {
      MVCC_LOG_RET(WARN, OB_ERR_UNEXPECTED, "get reserved snapshot for active txn with gc is disabled", KPC(this));
    }
    return share::SCN::max_scn();
  } else {
    return global_reserved_snapshot_.atomic_load();
  }
}

bool ObMultiVersionGarbageCollector::is_gc_disabled() const
{
  return gc_is_disabled_        // gc status is not allowed
    || refresh_error_too_long_; // refresh inner table failed
}

// collect reads all entries from inner-table, and apply functor to all entries
// to let users use it freely.
// NB: it will stop if functor report error, so use it carefully
int ObMultiVersionGarbageCollector::collect(ObMultiVersionGCSnapshotFunctor& calculator)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    const uint64_t tenant_id = MTL_ID();
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);

    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_NOT_INIT;
      MVCC_LOG(WARN, "sql_proxy_ not init yet, collect abort", KR(ret));
    } else if (OB_FAIL(sql.assign_fmt(QUERY_ALL_RESERVED_SNAPSHOT_SQL,
                                      share::OB_ALL_RESERVED_SNAPSHOT_TNAME,
                                      tenant_id))) {
      MVCC_LOG(WARN, "generate QUERY_ALL_SNAPSHOT_SQL fail", KR(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, sql.ptr()))) {
      MVCC_LOG(WARN, "execute sql read fail", KR(ret), K(meta_tenant_id), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      MVCC_LOG(ERROR, "execute sql fail", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
    } else {
      int64_t snapshot_version = 0;
      share::SCN snapshot_version_scn;
      ObMultiVersionSnapshotType snapshot_type = ObMultiVersionSnapshotType::MIN_SNAPSHOT_TYPE;
      ObMultiVersionGCStatus gc_status = ObMultiVersionGCStatus::INVALID_GC_STATUS;
      int64_t create_time = 0;
      char svr_ip_buf[MAX_IP_ADDR_LENGTH + 1] = {0};
      int64_t svr_ip_len = 0;
      uint64_t svr_port = 0;
      ObAddr addr;

      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        EXTRACT_UINT_FIELD_MYSQL(*result, "snapshot_version", snapshot_version, int64_t);
        EXTRACT_UINT_FIELD_MYSQL(*result, "snapshot_type", snapshot_type, concurrency_control::ObMultiVersionSnapshotType);
        EXTRACT_UINT_FIELD_MYSQL(*result, "create_time", create_time, int64_t);
        EXTRACT_UINT_FIELD_MYSQL(*result, "status", gc_status, concurrency_control::ObMultiVersionGCStatus);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", svr_ip_buf, 128, svr_ip_len);
        EXTRACT_UINT_FIELD_MYSQL(*result, "svr_port", svr_port, uint64_t);

        if (!addr.set_ip_addr(svr_ip_buf, svr_port)) {
          ret = OB_ERR_UNEXPECTED;
          MVCC_LOG(WARN, "set svr addr failed", K(svr_ip_buf), K(svr_port));
        } else if (OB_FAIL(snapshot_version_scn.convert_for_inner_table_field(snapshot_version))) {
          MVCC_LOG(WARN, "set min snapshot version scn failed", K(ret), K(snapshot_version));
        } else if (OB_FAIL(calculator(snapshot_version_scn,
                                      snapshot_type,
                                      gc_status,
                                      create_time,
                                      addr))) {
          MVCC_LOG(WARN, "calculate snapshot version failed", K(ret));
        } else {
          MVCC_LOG(INFO, "multi version garbage colloector collects successfully", K(sql),
                   K(snapshot_version), K(snapshot_type), K(create_time), K(addr), K(gc_status));
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

// mock for disabling report
bool ObMultiVersionGarbageCollector::can_report()
{
  return true;
}

// mock for disabling refresh
bool ObMultiVersionGarbageCollector::is_refresh_fail()
{
  return false;
}

// report will report the four entries into the inner table.
// NB: the 4 entries must be inserted atomically as the reason has been talked
//     about in the function 'study' for the rule 2.
int ObMultiVersionGarbageCollector::report(const share::SCN min_unallocated_GTS,
                                           const share::SCN min_unallocated_WRS,
                                           const share::SCN max_committed_txn_version,
                                           const share::SCN min_active_txn_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObAddr &self_addr = GCTX.self_addr();
  char ip_buffer[MAX_IP_ADDR_LENGTH + 1] = {0};
  const uint64_t tenant_id = MTL_ID();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  const int64_t current_ts = ObClockGenerator::getRealClock();

  if (OB_UNLIKELY(!self_addr.ip_to_string(ip_buffer, MAX_IP_ADDR_LENGTH))) {
    ret = OB_INVALID_ARGUMENT;
    MVCC_LOG(WARN, "ip to string failed", K(self_addr));
  } else if (OB_FAIL(sql.assign_fmt(INSERT_ON_UPDATE_ALL_RESERVED_SNAPSHOT_SQL,
                                    share::OB_ALL_RESERVED_SNAPSHOT_TNAME,
                                    // entries of the MIN_UNALLOCATED_GTS
                                    tenant_id,
                                    (uint64_t)(ObMultiVersionSnapshotType::MIN_UNALLOCATED_GTS),
                                    int(MAX_IP_ADDR_LENGTH), ip_buffer,
                                    self_addr.get_port(),
                                    current_ts,
                                    (uint64_t)(ObMultiVersionGCStatus::NORMAL_GC_STATUS),
                                    min_unallocated_GTS.get_val_for_inner_table_field(),
                                    // entries of the MIN_UNALLOCATED_WRS
                                    tenant_id,
                                    (uint64_t)(ObMultiVersionSnapshotType::MIN_UNALLOCATED_WRS),
                                    int(MAX_IP_ADDR_LENGTH), ip_buffer,
                                    self_addr.get_port(),
                                    current_ts,
                                    (uint64_t)(ObMultiVersionGCStatus::NORMAL_GC_STATUS),
                                    min_unallocated_WRS.get_val_for_inner_table_field(),
                                    // entries of the MAX_COMMITTED_TXN_VERSION
                                    tenant_id,
                                    (uint64_t)(ObMultiVersionSnapshotType::MAX_COMMITTED_TXN_VERSION),
                                    int(MAX_IP_ADDR_LENGTH), ip_buffer,
                                    self_addr.get_port(),
                                    current_ts,
                                    (uint64_t)(ObMultiVersionGCStatus::NORMAL_GC_STATUS),
                                    max_committed_txn_version.get_val_for_inner_table_field(),
                                    // entries of the ACTIVE_TXN_SNAPSHOT
                                    tenant_id,
                                    (uint64_t)(ObMultiVersionSnapshotType::ACTIVE_TXN_SNAPSHOT),
                                    int(MAX_IP_ADDR_LENGTH), ip_buffer,
                                    self_addr.get_port(),
                                    current_ts,
                                    (uint64_t)(ObMultiVersionGCStatus::NORMAL_GC_STATUS),
                                    min_active_txn_version.get_val_for_inner_table_field()))) {
    MVCC_LOG(WARN, "format sql fail", KR(ret), K(sql));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
    MVCC_LOG(WARN, "sql_proxy_ not init yet, report abort", KR(ret), K(sql));
  } else if (OB_FAIL(GCTX.sql_proxy_->write(meta_tenant_id, sql.ptr(), affected_rows))) {
    MVCC_LOG(WARN, "execute sql fail", KR(ret), K(sql));
  } else if (8 != affected_rows && // for on duplicate update
             4 != affected_rows) { // for first insert
    ret = OB_ERR_UNEXPECTED;
    MVCC_LOG(ERROR, "report multi version snapshot failed", KR(ret), K(sql), K(affected_rows));
  } else {
    MVCC_LOG(INFO, "report multi version snapshot success", KR(ret), K(sql), K(affected_rows));
  }

  return ret;
}

// update_status will update the four entries' status into the inner table.
// NB: the 4 entries must be updated atomically as the reason has been talked
//     about in the function 'study' for the rule 2.
int ObMultiVersionGarbageCollector::update_status(const ObMultiVersionGCStatus status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const ObAddr &self_addr = GCTX.self_addr();
  char ip_buffer[MAX_IP_ADDR_LENGTH + 1] = {0};
  const uint64_t tenant_id = MTL_ID();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);

  if (OB_UNLIKELY(!self_addr.ip_to_string(ip_buffer, MAX_IP_ADDR_LENGTH))) {
    ret = OB_INVALID_ARGUMENT;
    MVCC_LOG(WARN, "ip to string failed", K(self_addr));
  } else if (OB_FAIL(sql.assign_fmt(UPDATE_RESERVED_SNAPSHOT_STATUS,
                                    share::OB_ALL_RESERVED_SNAPSHOT_TNAME,
                                    (uint64_t)(status),
                                    tenant_id,
                                    int(MAX_IP_ADDR_LENGTH), ip_buffer,
                                    self_addr.get_port()))) {
    MVCC_LOG(WARN, "format sql fail", KR(ret), K(sql));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
    MVCC_LOG(WARN, "sql_proxy_ not init yet, report abort", KR(ret), K(sql));
  } else if (OB_FAIL(GCTX.sql_proxy_->write(meta_tenant_id, sql.ptr(), affected_rows))) {
    MVCC_LOG(WARN, "execute sql fail", KR(ret), K(sql));
  } else if (0 != affected_rows && // update with the same row
             4 != affected_rows) { // normal update succeed
    ret = OB_ERR_UNEXPECTED;
    MVCC_LOG(ERROR, "report multi version snapshot failed", KR(ret), K(sql), K(affected_rows));
  } else {
    MVCC_LOG(INFO, "update multi version snapshot success", KR(ret), K(sql), K(affected_rows));
  }

  return ret;
}

// We need reclaim expired entries in the inner table under exceptional
// conditions. For example, when the node where the active txn is located
// cannot update the inner table in time due to abnormal reasons, the snapshot
// value cannot be advanced and a large number of versions cannot be recycled.
// So we need to handle snapshot processing in abnormal situations.
//
// Firstly, we rely on the sys ls's ability to manage the ls. We hope that if
// the node cannot contact to the inner table due to abnormalities, then
// eventually the entries of this node will be removed from the innner table, so
// that we no longer rely on this Timestamps provided by the node as recycling
// snapshots.(implemented in the reclaim)
//
// Secondly, we rely on the each node's ability to ignore faulty timestamps. We
// hope that if the sys ls cannot contact to the inner table and remove the
// entries in time, each node will ignore the timestamps that has not been
// updated for a long time.(implemented in the ObMultiVersionGCSnapshotCalculator).
//
// Finally, in the worst case, the node cannot contact to the inner table and
// fail to advance the snapshot it maintained. We think the multi-version data
// on the node is not reachable for active txns, so we ignore the value of the
// inner table and use the undo_retention and the snapshot_gc_ts as the new
// mechanism.(implemented in the repeat_refresh)
//
// All in all, our purpose is that no matter what the exception is, we must be
// able to provide a user-reasonable(may be value of the customer) recycling
// snapshot that all users can understand.
int ObMultiVersionGarbageCollector::reclaim()
{
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  storage::ObLSHandle ls_handle;
  logservice::ObLogHandler *handler = nullptr;
  int64_t old_proposal_id = 0;
  ObArray<ObAddr> reclaimable_servers;
  ObArray<ObAddr> snapshot_servers;
  bool is_this_server_disabled = false;
  common::ObRole role;

  ObTimeGuard timeguard(__func__, 1 * 1000 * 1000);

  if (OB_FAIL(MTL(storage::ObLSService*)->
              get_ls(share::SYS_LS, ls_handle, ObLSGetMod::MULTI_VERSION_GARBAGE_COLLOECTOR_MOD))) {
    MVCC_LOG(WARN, "get sys ls failed", K(ret));
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(ls = ls_handle.get_ls())
             || OB_ISNULL(handler = ls_handle.get_ls()->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    MVCC_LOG(ERROR, "log stream is NULL", K(ret), K(ls));
  } else if (OB_FAIL(handler->get_role(role, old_proposal_id))) {
    MVCC_LOG(WARN, "fail to get role", KR(ret));
  } else if (common::is_leader_like(role)) {
    timeguard.click("get_leader");

    // TODO(handora.qc): use nicer timer
    const int64_t current_timestamp = ObClockGenerator::getRealClock();

    ObMultiVersionGCSnapshotOperator collector(
      [current_timestamp,
       &reclaimable_servers,
       &snapshot_servers,
       &is_this_server_disabled](const share::SCN snapshot_version,
                                 const ObMultiVersionSnapshotType snapshot_type,
                                 const ObMultiVersionGCStatus status,
                                 const int64_t create_time,
                                 const ObAddr addr) -> int {
        int ret = OB_SUCCESS;
        int tmp_ret = OB_SUCCESS;
        bool need_reclaim = false;

        if (OB_FAIL(snapshot_servers.push_back(addr))) {
          MVCC_LOG(WARN, "push array failed", K(ret));
        } else {
          // TODO(handora.qc): use a better time monitor for the node lost for a long time
          if (current_timestamp > create_time
              && current_timestamp - create_time > GARBAGE_COLLECT_RECLAIM_DURATION) {
            bool is_exist = true;
            if (OB_TMP_FAIL(share::ObAllServerTracer::get_instance().is_server_exist(addr, is_exist))) {
              MVCC_LOG(WARN, "check all server tracer failed", K(tmp_ret));
            } else if (is_exist) {
              // Case 1: server exists, while not renew snapshot for a long time
              bool is_alive = false;
              if (OB_TMP_FAIL(share::ObAllServerTracer::get_instance().check_server_alive(addr, is_alive))) {
                MVCC_LOG(WARN, "check all server tracer failed", K(tmp_ret));
              } else if (is_alive) {
                // Case 1.1: server is alive, we report the WARN for not
                //           renewing. because there may be tenant transfer out
                //           which cause it will not be reclaimed forever
                MVCC_LOG(WARN, "server alives while not renew for a long time", K(create_time),
                         K(current_timestamp), K(addr), K(snapshot_type), K(snapshot_version));
                need_reclaim = true;
              } else {
                // Case 1.2: server is not alive, we report the WARN and reclaim
                //           it immediately
                MVCC_LOG(WARN, "server not alives while not renew for a long time", K(create_time),
                         K(current_timestamp), K(addr), K(snapshot_type), K(snapshot_version));
                need_reclaim = true;
              }
            } else {
              // Case 2: server doesnot exits,  we report the WARN and reclaim
              //         it immediately
              MVCC_LOG(WARN, "server doesnot exists so we should remove it", K(create_time),
                       K(current_timestamp), K(addr), K(snapshot_type), K(snapshot_version));
              need_reclaim = true;
            }
          }

          if (need_reclaim) {
            bool exist = false;
            for (int64_t i = 0; !exist && i < reclaimable_servers.count(); i++) {
              if (addr == reclaimable_servers[i]) {
                exist = true;
              }
            }
            if (!exist && OB_FAIL(reclaimable_servers.push_back(addr))) {
              MVCC_LOG(WARN, "push back array failed", K(ret));
            }
          }
        }

        if (!is_this_server_disabled &&
            addr == GCTX.self_addr() &&
            status != ObMultiVersionGCStatus::NORMAL_GC_STATUS) {
          is_this_server_disabled = true;
        }

        return ret;
      });

    // collect all info for reclaimable servers and all reported servers
    if (OB_FAIL(collect(collector))) {
      MVCC_LOG(WARN, "collect snapshot info failed", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      timeguard.click("collect");

      if (0 == reclaimable_servers.count()) {
        // all snapshot info is not reclaimable
        MVCC_LOG(INFO, "skip all alivavle snapshots info");
        // reclaim all uncessary servers
      } else if (OB_TMP_FAIL((reclaim_(reclaimable_servers)))) {
        MVCC_LOG(WARN, "reclaim snapshot info failed", K(tmp_ret),
                 K(reclaimable_servers));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }

      timeguard.click("reclaim_");

      if (0 == snapshot_servers.count()) {
        MVCC_LOG(WARN, "no alive servers now, please check it clearly",
                 KPC(this), K(snapshot_servers), K(reclaimable_servers));
        // monitor all servers
      } else if (OB_TMP_FAIL(monitor_(snapshot_servers))) {
        MVCC_LOG(WARN, "snapshots servers are mintor failed",
                 KPC(this), K(snapshot_servers), K(tmp_ret));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }

      timeguard.click("monitor_");
    }
  }

  MVCC_LOG(INFO, "reclaim multi version garabage collector end", KPC(this),
           K(ret), K(role), K(reclaimable_servers), K(snapshot_servers),
           K(is_this_server_disabled));

  return ret;
}

// mointor checks all servers in the inner table and server manager and check
// whether there exists a server in the server manager and does not report its
// timestamps from beginning to the end.
int ObMultiVersionGarbageCollector::monitor_(const ObArray<ObAddr> &snapshot_servers)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> lost_servers;

  if (OB_FAIL(share::ObAllServerTracer::get_instance().for_each_server_info(
                [&snapshot_servers,
                 &lost_servers](const share::ObServerInfoInTable &server_info) -> int {
                  int ret = OB_SUCCESS;
                  bool found = false;

                  // find servers that recorded in __all_server table while has
                  // not reported its timestamp.
                  for (int64_t i = 0; !found && i < snapshot_servers.count(); ++i) {
                    if (server_info.get_server() == snapshot_servers[i]) {
                      found = true;
                    }
                  }

                  if (!found) {// not found in __all_reserved_snapshot inner table
                    if (OB_FAIL(lost_servers.push_back(server_info.get_server()))) {
                      MVCC_LOG(WARN, "lost servers push back failed", K(ret));
                    } else if (!server_info.is_valid()) {
                      MVCC_LOG(ERROR, "invalid server info", K(ret), K(server_info));
                      // if not in service, we ignore it and report the warning
                    } else if (!server_info.in_service() || server_info.is_stopped()) {
                      MVCC_LOG(WARN, "server is not alive, we will remove soon", K(ret), K(server_info));
                      // if not alive, we ignore it and report the warning
                    } else if (!server_info.is_alive()) {
                      MVCC_LOG(WARN, "server is not alive, please pay attention", K(ret), K(server_info));
                    } else {
                      // may be lost or do not contain the tenant
                      // TODO(handora.qc): make it better and more clear
                      MVCC_LOG(INFO, "server is alive when mointor", K(ret), K(server_info));
                    }
                  }

                  return ret;
                }))) {
    MVCC_LOG(WARN, "for each server status failed", K(ret));
  } else {
    MVCC_LOG(INFO, "garbage collector monitor server status monitor",
             K(snapshot_servers), K(lost_servers));
  }

  return ret;
}

// disk monitor will monitor the current disk status and report to inner table
// in time when finding the status of the two parties does not match.
//
// The demand comes from the following story:
// Some scenes often appear in the online environment that long-running
// snapshots prevent data from recycling, so we must take disk usage into
// consideration. So we timely check the disk usage and report it to the
// inner_table(called gc status). And all server will check the gc status
// before using it.
int ObMultiVersionGarbageCollector::disk_monitor_(const bool is_this_server_alomost_full)
{
  int ret = OB_SUCCESS;
  bool is_almost_full = false;
  bool need_report = false;

  if (OB_FAIL(is_disk_almost_full_(is_almost_full))) {
    MVCC_LOG(WARN, "check disk almost full failed", K(ret), KPC(this));
  } else if (is_this_server_alomost_full && is_almost_full) {
    need_report = false;
    MVCC_LOG(WARN, "the disk still be full of the disk", K(ret), KPC(this));
  } else if (!is_this_server_alomost_full && is_almost_full) {
    MVCC_LOG(WARN, "the disk becoming full of the disk", K(ret), KPC(this));
    need_report = true;
  } else if (!is_this_server_alomost_full && !is_almost_full) {
    // normal scense
    need_report = false;
  } else if (is_this_server_alomost_full && !is_almost_full) {
    MVCC_LOG(INFO, "the disk becoming not full of the disk", K(ret), KPC(this));
    need_report = true;
  }

  if (need_report) {
    ObMultiVersionGCStatus status = is_almost_full ?
      ObMultiVersionGCStatus::DISABLED_GC_STATUS :
      ObMultiVersionGCStatus::NORMAL_GC_STATUS;
    if (OB_FAIL(update_status(status))) {
      MVCC_LOG(WARN, "disk monitor failed", K(ret), K(status));
    } else {
      MVCC_LOG(INFO, "report disk monitor succeed", K(ret), K(status),
               K(is_this_server_alomost_full), K(is_almost_full));
    }
  }

  return ret;
}

// reclaim remove entries according to reclaimable_snapshots_info
int ObMultiVersionGarbageCollector::reclaim_(const ObArray<ObAddr> &reclaimable_servers)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char ip_buffer[MAX_IP_ADDR_LENGTH + 1] = {0};
  const uint64_t tenant_id = MTL_ID();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);

  for (int64_t i = 0; i < reclaimable_servers.count(); ++i) {
    ObAddr addr = reclaimable_servers[i];
    if (OB_UNLIKELY(!addr.ip_to_string(ip_buffer, MAX_IP_ADDR_LENGTH))) {
      ret = OB_INVALID_ARGUMENT;
      MVCC_LOG(WARN, "ip to string failed", K(addr));
    } else if (OB_FAIL(sql.assign_fmt(DELETE_EXPIRED_RESERVED_SNAPSHOT,
                                      share::OB_ALL_RESERVED_SNAPSHOT_TNAME,
                                      tenant_id,
                                      int(MAX_IP_ADDR_LENGTH), ip_buffer,
                                      addr.get_port()))) {
      MVCC_LOG(WARN, "format sql fail", KR(ret), K(sql));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_NOT_INIT;
      MVCC_LOG(WARN, "sql_proxy_ not init yet, report abort", KR(ret), K(sql));
    } else if (OB_FAIL(GCTX.sql_proxy_->write(meta_tenant_id, sql.ptr(), affected_rows))) {
      MVCC_LOG(WARN, "execute sql fail", KR(ret), K(sql));
    } else if (OB_UNLIKELY(0 != affected_rows && 4 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      MVCC_LOG(WARN, "affected rows is wrong", KR(ret), K(sql), K(affected_rows));
    } else {
      MVCC_LOG(INFO, "reclaim expired multi version snapshot success",
               KR(ret), K(sql), K(addr));
    }
  }

  return ret;
}

// Some scenes often appear in the online environment that long-running
// snapshots prevent data from recycling, so we must take disk usage into
// consideration. So we timely check the disk usage and report it to the
// inner_table(called gc status). And all server will check the gc status
// before using it.
int ObMultiVersionGarbageCollector::is_disk_almost_full_(bool &is_almost_full)
{
  int ret = OB_SUCCESS;
  is_almost_full = false;
  const int64_t required_size = 0;

  // Case1: io device is almost full
  if (!is_almost_full
      && OB_FAIL(THE_IO_DEVICE->check_space_full(required_size))) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
      ret = OB_SUCCESS;
      is_almost_full = true;
      MVCC_LOG(WARN, "disk is almost full, we should give up", KPC(this));
    } else {
      MVCC_LOG(WARN, "failed to check space full", K(ret));
    }
  }

  // Case2: sstable is overflow during merge
  if (!is_almost_full
      && is_sstable_overflow_()) {
    is_almost_full = true;
    MVCC_LOG(WARN, "disk is almost full, we should give up", KPC(this));
  }

  return ret;
}

void ObMultiVersionGarbageCollector::report_sstable_overflow()
{
  const int64_t current_timestamp = common::ObTimeUtility::current_time();
  ATOMIC_STORE(&last_sstable_overflow_timestamp_, current_timestamp);
  MVCC_LOG_RET(WARN, OB_SIZE_OVERFLOW, "sstable is alomost overflow, we should give up", KPC(this));
}

bool ObMultiVersionGarbageCollector::is_sstable_overflow_()
{
  bool b_ret = false;
  const int64_t current_timestamp = common::ObTimeUtility::current_time();
  const int64_t last_sstable_overflow_timestamp = ATOMIC_LOAD(&last_sstable_overflow_timestamp_);
  if (0 != last_sstable_overflow_timestamp
      && current_timestamp >= last_sstable_overflow_timestamp
      // We currenly think that there may be a disk full problem if there exists
      // an sstable overflow error within 5 minutes
      && current_timestamp - last_sstable_overflow_timestamp <= 5 * 1_min) {
    b_ret = true;
  }
  return b_ret;
}

ObMultiVersionGCSnapshotCalculator::ObMultiVersionGCSnapshotCalculator()
  : reserved_snapshot_version_(share::SCN::max_scn()),
    reserved_snapshot_type_(ObMultiVersionSnapshotType::MIN_SNAPSHOT_TYPE),
    reserved_status_(ObMultiVersionGCStatus::INVALID_GC_STATUS),
    reserved_create_time_(0),
    reserved_addr_(),
    is_this_server_disabled_(false),
    status_(ObMultiVersionGCStatus::NORMAL_GC_STATUS) {}

ObMultiVersionGCSnapshotCalculator::~ObMultiVersionGCSnapshotCalculator()
{
  reserved_snapshot_version_ = share::SCN::max_scn();
  reserved_snapshot_type_ = ObMultiVersionSnapshotType::MIN_SNAPSHOT_TYPE;
  reserved_status_ = ObMultiVersionGCStatus::INVALID_GC_STATUS;
  reserved_create_time_ = 0;
  reserved_addr_.reset();
  is_this_server_disabled_ = false;
  status_ = ObMultiVersionGCStatus::NORMAL_GC_STATUS;
}

int ObMultiVersionGCSnapshotCalculator::operator()(const share::SCN snapshot_version,
                                                   const ObMultiVersionSnapshotType snapshot_type,
                                                   const ObMultiVersionGCStatus status,
                                                   const int64_t create_time,
                                                   const ObAddr addr)
{
  int ret = OB_SUCCESS;
  // TODO(handora.qc): change machine time to nicer time
  const int64_t current_ts = ObClockGenerator::getRealClock();

  // Step1: calculate the minumium reserved version and record it
  if (snapshot_version < reserved_snapshot_version_) {
    if (current_ts > create_time &&
        current_ts - create_time > 2 * ObMultiVersionGarbageCollector::GARBAGE_COLLECT_RECLAIM_DURATION &&
        // for mock or test that change GARBAGE_COLLECT_EXEC_INTERVAL to a small value
        current_ts - create_time > 2 * 3 * 10_min) {
      // we report WARN here because there may be servers offline and online
      // suddenly and report a stale txn or there may be tenant being dropped
      // and alived server may fetch the tenant info
      MVCC_LOG(WARN, "ignore too old version", K(snapshot_version),
               K(snapshot_type), K(current_ts), K(create_time), K(addr));
    } else {
      reserved_snapshot_version_ = snapshot_version;
      reserved_snapshot_type_ = snapshot_type;
      reserved_create_time_ = create_time;
      reserved_addr_ = addr;
    }
  }

  // Step2: ensure the gc status of the current server
  if (!is_this_server_disabled_ &&
      addr == GCTX.self_addr() &&
      status != ObMultiVersionGCStatus::NORMAL_GC_STATUS) {
    is_this_server_disabled_ = true;
  }

  // Step3: merge the status of all multi-version gc status
  status_ = status_ | status;

  return ret;
}

share::SCN ObMultiVersionGCSnapshotCalculator::get_reserved_snapshot_version() const
{
  return reserved_snapshot_version_;
}

ObMultiVersionSnapshotType ObMultiVersionGCSnapshotCalculator::get_reserved_snapshot_type() const
{
  return reserved_snapshot_type_;
}

ObMultiVersionGCStatus ObMultiVersionGCSnapshotCalculator::get_status() const
{
  return status_;
}

ObMultiVersionSnapshotInfo::ObMultiVersionSnapshotInfo()
  : snapshot_version_(share::SCN::min_scn()),
    snapshot_type_(ObMultiVersionSnapshotType::MIN_SNAPSHOT_TYPE),
    status_(ObMultiVersionGCStatus::INVALID_GC_STATUS),
    create_time_(0),
    addr_() {}

ObMultiVersionSnapshotInfo::ObMultiVersionSnapshotInfo(const share::SCN snapshot_version,
                                                       const ObMultiVersionSnapshotType snapshot_type,
                                                       const ObMultiVersionGCStatus status,
                                                       const int64_t create_time,
                                                       const ObAddr addr)
  : snapshot_version_(snapshot_version),
    snapshot_type_(snapshot_type),
    status_(status),
    create_time_(create_time),
    addr_(addr) {}

ObMultiVersionGCSnapshotCollector::ObMultiVersionGCSnapshotCollector(
  ObIArray<ObMultiVersionSnapshotInfo> &snapshot_info)
  : snapshots_info_(snapshot_info) {}

ObMultiVersionGCSnapshotCollector::~ObMultiVersionGCSnapshotCollector() {}

int ObMultiVersionGCSnapshotCollector::operator()(const share::SCN snapshot_version,
                                                  const ObMultiVersionSnapshotType snapshot_type,
                                                  const ObMultiVersionGCStatus status,
                                                  const int64_t create_time,
                                                  const ObAddr addr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(snapshots_info_.push_back(ObMultiVersionSnapshotInfo(snapshot_version,
                                                                   snapshot_type,
                                                                   status,
                                                                   create_time,
                                                                   addr)))) {
    MVCC_LOG(WARN, "push back to snapshots info failed", K(ret));
  }

  return ret;
}

ObMultiVersionGCSnapshotOperator::ObMultiVersionGCSnapshotOperator(
  const ObMultiVersionGCSnapshotFunction &func)
  : func_(func) {}

int ObMultiVersionGCSnapshotOperator::operator()(const share::SCN snapshot_version,
                                                 const ObMultiVersionSnapshotType snapshot_type,
                                                 const ObMultiVersionGCStatus status,
                                                 const int64_t create_time,
                                                 const ObAddr addr)
{
  return func_(snapshot_version, snapshot_type, status, create_time, addr);
}

// Functor to fetch the status of all alived session
// TODO(handora.qc): using better timestamp instead of cur state start time
bool GetMinActiveSnapshotVersionFunctor::operator()(sql::ObSQLSessionMgr::Key key,
                                                    sql::ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  UNUSED(key);

  if (OB_ISNULL(sess_info)) {
    ret = OB_NOT_INIT;
    MVCC_LOG(WARN, "session info is NULL");
  } else if (false == sess_info->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MVCC_LOG(WARN, "session info is not valid", K(ret));
  } else if (sess_info->get_is_deserialized()) {
    // skip deserialized session, only visit the original
  } else if (MTL_ID() == sess_info->get_effective_tenant_id()) {
    sql::ObSQLSessionInfo::LockGuard data_lock_guard(sess_info->get_thread_data_lock());
    share::SCN snapshot_version(share::SCN::max_scn());

    if (sess_info->is_in_transaction()) {
      share::SCN desc_snapshot;
      transaction::ObTxDesc *tx_desc = nullptr;
      share::SCN sess_snapshot = sess_info->get_reserved_snapshot_version();
      if (OB_ISNULL(tx_desc = sess_info->get_tx_desc())) {
        ret = OB_ERR_UNEXPECTED;
        MVCC_LOG(ERROR, "tx desc is nullptr", K(ret), KPC(sess_info));
      } else if (FALSE_IT(desc_snapshot = tx_desc->get_snapshot_version())) {
      } else if (transaction::ObTxIsolationLevel::SERIAL == tx_desc->get_isolation_level() ||
                 transaction::ObTxIsolationLevel::RR == tx_desc->get_isolation_level()) {
        // Case 1: RR/SI with tx desc exists, it means the snapshot is get from
        // scheduler and must maintained in the session and tx desc
        if (desc_snapshot.is_valid()) {
          snapshot_version = desc_snapshot;
        }
        MVCC_LOG(DEBUG, "RR/SI txn with tx_desc", K(MTL_ID()), KPC(sess_info),
                 K(snapshot_version), K(min_active_snapshot_version_), K(desc_snapshot),
                 K(sess_snapshot), K(desc_snapshot));
      } else if (transaction::ObTxIsolationLevel::RC == tx_desc->get_isolation_level()) {
        // Case 2: RC with tx desc exists, it may exists that snapshot is get from
        // the executor and not maintained in the session and tx desc. So we need
        // use session query start time carefully
        if (sql::ObSQLSessionState::QUERY_ACTIVE == sess_info->get_session_state()) {
          if (desc_snapshot.is_valid()) {
            snapshot_version = desc_snapshot;
          } else if (sess_snapshot.is_valid()) {
            snapshot_version = sess_snapshot;
          } else {
            // We gave a 5 minutes redundancy when get from session query start
            // time under the case that local snapshot from tx_desc and session
            // is unusable
            snapshot_version.convert_from_ts(sess_info->get_cur_state_start_time()
                                             - 5L * 1000L * 1000L * 60L);
            MVCC_LOG(INFO, "RC txn with tx_desc while from session start time",
                     K(MTL_ID()), KPC(sess_info), K(snapshot_version),
                     K(min_active_snapshot_version_),
                     K(sess_info->get_cur_state_start_time()));
          }
        }
        MVCC_LOG(DEBUG, "RC txn with tx_desc", K(MTL_ID()), KPC(sess_info),
                 K(snapshot_version), K(min_active_snapshot_version_), K(desc_snapshot),
                 K(sess_snapshot), K(desc_snapshot));
      } else {
        MVCC_LOG(INFO, "unknown txn with tx_desc", K(MTL_ID()), KPC(sess_info),
                 K(snapshot_version), K(min_active_snapshot_version_), K(desc_snapshot));
      }
    } else {
      share::SCN sess_snapshot = sess_info->get_reserved_snapshot_version();
      if (transaction::ObTxIsolationLevel::SERIAL == sess_info->get_tx_isolation() ||
          transaction::ObTxIsolationLevel::RR == sess_info->get_tx_isolation()) {
        // Case 3: RR/SI with tx desc does not exist or not in tx, it is not for
        // the current running scheduler
        if (sql::ObSQLSessionState::QUERY_ACTIVE == sess_info->get_session_state()) {
          if (sess_snapshot.is_valid()) {
            snapshot_version = sess_snapshot;
          } else {
            // We gave a 5 minutes redundancy when get from session query start
            // time under the case that local snapshot from tx_desc and session
            // is unusable
            snapshot_version.convert_from_ts(sess_info->get_cur_state_start_time()
                                             - 5L * 1000L * 1000L * 60L);
            MVCC_LOG(INFO, "RR/SI txn with non tx_desc while from session start time",
                     K(MTL_ID()), KPC(sess_info), K(snapshot_version), K(sess_snapshot),
                     K(min_active_snapshot_version_), K(sess_info->get_cur_state_start_time()));
          }
        }
        MVCC_LOG(DEBUG, "RR/SI txn with non tx_desc", K(MTL_ID()), KPC(sess_info),
                 K(snapshot_version), K(min_active_snapshot_version_), K(sess_snapshot));
      } else if (transaction::ObTxIsolationLevel::RC == sess_info->get_tx_isolation()) {
        // Case 4: RC with tx desc does not exist, and the snapshot version may not
        // maintained, so we use query start time instead
        if (sql::ObSQLSessionState::QUERY_ACTIVE == sess_info->get_session_state()) {
          if (sess_snapshot.is_valid()) {
            snapshot_version = sess_snapshot;
          } else {
            // We gave a 5 minutes redundancy when get from session query start
            // time under the case that local snapshot from tx_desc and session
            // is unusable
            snapshot_version.convert_from_ts(sess_info->get_cur_state_start_time()
                                             - 5L * 1000L * 1000L * 60L);
            MVCC_LOG(INFO, "RC txn with non tx_desc while from session start time",
                     K(MTL_ID()), KPC(sess_info), K(snapshot_version), K(sess_snapshot),
                     K(min_active_snapshot_version_), K(sess_info->get_cur_state_start_time()));
          }
        }
        MVCC_LOG(DEBUG, "RC txn with non tx_desc", K(MTL_ID()), KPC(sess_info),
                 K(snapshot_version), K(min_active_snapshot_version_), K(sess_snapshot));
      } else {
        MVCC_LOG(INFO, "unknown txn with non tx_desc", K(MTL_ID()), KPC(sess_info),
                 K(snapshot_version), K(min_active_snapshot_version_));
      }
    }

    if (OB_SUCC(ret)
        && share::SCN::min_scn() != snapshot_version
        && snapshot_version < min_active_snapshot_version_) {
      const int64_t current_timestamp = ObClockGenerator::getRealClock();
      const int64_t snapshot_version_ts = snapshot_version.get_val_for_tx() / 1000;
      if (snapshot_version_ts < current_timestamp
          && current_timestamp - snapshot_version_ts > 100 * 1_min) {
        MVCC_LOG(INFO, "GetMinActiveSnapshotVersionFunctor find a small snapshot txn",
                 K(MTL_ID()), KPC(sess_info), K(snapshot_version),
                 K(current_timestamp), K(min_active_snapshot_version_));
      }
      min_active_snapshot_version_ = snapshot_version;
    }
  }

  return OB_SUCCESS == ret;
}

} // namespace concurrency_control
} // namespace oceanbase
