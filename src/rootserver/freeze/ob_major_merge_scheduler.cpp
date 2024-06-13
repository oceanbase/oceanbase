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

#define USING_LOG_PREFIX RS_COMPACTION

#include "rootserver/freeze/ob_major_merge_scheduler.h"

#include "rootserver/ob_root_service.h"
#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/freeze/ob_major_merge_info_manager.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/freeze/ob_tenant_all_zone_merge_strategy.h"
#include "rootserver/freeze/ob_major_freeze_util.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/allocator/page_arena.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "share/tablet/ob_tablet_table_iterator.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_column_checksum_error_operator.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "share/ob_server_table_operator.h"
#include "share/ob_global_merge_table_operator.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;

///////////////////////////////////////////////////////////////////////////////

int ObMajorMergeIdling::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}

int64_t ObMajorMergeIdling::get_idle_interval_us()
{
  int64_t interval_us = DEFAULT_SCHEDULE_IDLE_US;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_LIKELY(tenant_config.is_valid())) {
    interval_us = tenant_config->merger_check_interval;
  }
  return interval_us;
}

///////////////////////////////////////////////////////////////////////////////

ObMajorMergeScheduler::ObMajorMergeScheduler(const uint64_t tenant_id)
  : ObFreezeReentrantThread(tenant_id), is_inited_(false), is_primary_service_(true), fail_count_(0),
    first_check_merge_us_(0), idling_(stop_), merge_info_mgr_(nullptr),
    config_(nullptr), merge_strategy_(), progress_checker_(tenant_id, stop_)
{
}

int ObMajorMergeScheduler::init(
    const bool is_primary_service,
    ObMajorMergeInfoManager &merge_info_mgr,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObIServerTrace &server_trace,
    common::ObServerConfig &config,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(tenant_id));
  } else if (OB_INVALID_ID == tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(merge_strategy_.init(tenant_id_, &merge_info_mgr.get_zone_merge_mgr()))) {
    LOG_WARN("fail to init tenant zone merge strategy", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(progress_checker_.init(is_primary_service, sql_proxy,
                schema_service, server_trace, merge_info_mgr))) {
    LOG_WARN("fail to init progress_checker", KR(ret));
  } else if (OB_FAIL(idling_.init(tenant_id_))) {
    LOG_WARN("fail to init idling", KR(ret), K_(tenant_id));
  } else {
    first_check_merge_us_ = 0;
    is_primary_service_ = is_primary_service;
    merge_info_mgr_ = &merge_info_mgr;
    config_ = &config;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObMajorMergeScheduler::start()
{
  int ret = OB_SUCCESS;
  lib::Threads::set_run_wrapper(MTL_CTX());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMajorMergeScheduler not init", KR(ret));
  } else if (OB_FAIL(create(MAJOR_MERGE_SCHEDULER_THREAD_CNT, "MergeScheduler"))) {
    LOG_WARN("fail to create thread", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObRsReentrantThread::start())) {
    LOG_WARN("fail to start thread", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("succ to start ObMajorMergeScheduler", K_(tenant_id));
  }
  return ret;
}

void ObMajorMergeScheduler::run3()
{
  LOG_INFO("major merge scheduler will run", K_(tenant_id));
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  }

  if (OB_SUCC(ret)) {
    while (!stop_) {
      update_last_run_timestamp();
      // record if try_update_epoch_and_reload() is failed.
      bool is_update_epoch_and_reload_failed = false;
      if (OB_FAIL(try_update_epoch_and_reload())) {
        is_update_epoch_and_reload_failed = true;
        LOG_WARN("fail to try_update_epoch_and_reload", KR(ret), "cur_epoch", get_epoch());
      } else if (OB_FAIL(do_work())) {
        LOG_WARN("fail to do major scheduler work", KR(ret), K_(tenant_id), "cur_epoch", get_epoch());
      }
      // out of do_work, there must be no major merge on this server. therefore, here, clear
      // compcation diagnose infos that stored in memory of this server.
      progress_checker_.reset_uncompacted_tablets();

      int tmp_ret = OB_SUCCESS;
      // If try_update_epoch_and_reload() is failed, idle only DEFAULT_IDLE_US. So as to
      // succeed to try_update_epoch_and_reload() as soon as possible.
      if (OB_TMP_FAIL(try_idle(DEFAULT_IDLE_US, is_update_epoch_and_reload_failed ? OB_SUCCESS : ret))) {
        LOG_WARN("fail to try_idle", KR(ret), KR(tmp_ret));
      }
      ret = OB_SUCCESS; // ignore ret, continue
    }
  }

  LOG_INFO("major merge scheduler will exit", K_(tenant_id));
}

int ObMajorMergeScheduler::try_idle(
    const int64_t ori_idle_time_us,
    const int work_ret)
{
  int ret = OB_SUCCESS;
  const int64_t IMMEDIATE_RETRY_CNT = 3;
  int64_t idle_time_us = ori_idle_time_us;
  int64_t merger_check_interval = idling_.get_idle_interval_us();
  const int64_t start_time_us = ObTimeUtil::current_time();
  bool clear_cached_info = false;

  if (OB_SUCCESS == work_ret) {
    fail_count_ = 0;
  } else {
    ++fail_count_;
  }

  if (0 == fail_count_) {
    // default idle
  } else if (fail_count_ < IMMEDIATE_RETRY_CNT) {
    idle_time_us = fail_count_ * DEFAULT_IDLE_US;
    if (idle_time_us > merger_check_interval) {
      idle_time_us = merger_check_interval;
    }
    LOG_WARN("fail to major merge, will immediate retry", K_(tenant_id),
             K_(fail_count), LITERAL_K(IMMEDIATE_RETRY_CNT), K(idle_time_us));
  } else {
    idle_time_us = merger_check_interval;
    LOG_WARN("major merge failed more than immediate cnt, turn to idle status",
             K_(fail_count), LITERAL_K(IMMEDIATE_RETRY_CNT), K(idle_time_us));
  }

  if (is_paused()) {
    const uint64_t start_ts = ObTimeUtility::fast_current_time();
    while (OB_SUCC(ret) && is_paused() && !stop_) {
      LOG_INFO("major_merge_scheduler is paused", K_(tenant_id), K(idle_time_us),
               "epoch", get_epoch());
      if (OB_FAIL(idling_.idle(idle_time_us))) {
        LOG_WARN("fail to idle", KR(ret), K(idle_time_us));
      } else if (ObTimeUtility::fast_current_time() > start_ts + PAUSED_WAITING_CLEAR_MEMORY_THRESHOLD) {
        (void) progress_checker_.clear_cached_info();
        clear_cached_info = true;
        LOG_INFO("clear cached info when idling", KR(ret), K(start_ts));
      }
    }
    LOG_INFO("major_merge_scheduler is not idling", KR(ret), K_(tenant_id), K(idle_time_us),
             K_(stop), "epoch", get_epoch());
  } else if (OB_FAIL(idling_.idle(idle_time_us))) {
    LOG_WARN("fail to idle", KR(ret), K(idle_time_us));
  }
  if (OB_SUCC(ret) && clear_cached_info) {
    const int64_t expected_epoch = get_epoch();
    if (OB_FAIL(do_before_major_merge(expected_epoch, false/*start_merge*/))) {
      LOG_WARN("failed to set basic info again", KR(ret), K(expected_epoch));
    }
  }
  const int64_t cost_time_us = ObTimeUtil::current_time() - start_time_us;

  return ret;
}

int ObMajorMergeScheduler::get_uncompacted_tablets(
    ObArray<ObTabletReplica> &uncompacted_tablets,
    ObArray<uint64_t> &uncompacted_table_ids) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    if (OB_FAIL(progress_checker_.get_uncompacted_tablets(uncompacted_tablets, uncompacted_table_ids))) {
      LOG_WARN("fail to get uncompacted tablets", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObMajorMergeScheduler::do_work()
{
  int ret = OB_SUCCESS;

  HEAP_VARS_2((ObZoneMergeInfoArray, info_array), (ObGlobalMergeInfo, global_info)) {
    const int64_t curr_round_epoch = get_epoch();
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", KR(ret), K_(tenant_id));
    } else {
      FREEZE_TIME_GUARD;
      if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().try_reload())) {
        LOG_WARN("fail to try reload", KR(ret), K_(tenant_id));
      }
    }
    if (FAILEDx(merge_info_mgr_->get_zone_merge_mgr().get_snapshot(global_info, info_array))) {
      LOG_WARN("fail to get merge info", KR(ret), K_(tenant_id));
    } else {
      bool need_merge = true;
      if (global_info.is_merge_error()) {
        need_merge = false;
        LOG_WARN("cannot do this round major merge cuz is_merge_error", K(need_merge), K(global_info));
      } else if (global_info.is_last_merge_complete()) {
        if (global_info.global_broadcast_scn() == global_info.frozen_scn()) {
          need_merge = false;
        } else if (global_info.global_broadcast_scn() < global_info.frozen_scn()) {
          // should do next round merge with higher broadcast_scn
          if (OB_FAIL(generate_next_global_broadcast_scn(curr_round_epoch))) {
            LOG_WARN("fail to generate next broadcast scn", KR(ret), K(global_info), K(curr_round_epoch));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid frozen_scn", KR(ret), K(global_info));
        }
      } else {
        // do major freeze with current broadcast_scn
      }

      if (OB_SUCC(ret) && need_merge) {
        if (OB_FAIL(do_before_major_merge(curr_round_epoch, true/*start_merge*/))) {
          LOG_WARN("fail to do before major merge", KR(ret), K(curr_round_epoch));
        } else if (OB_FAIL(do_one_round_major_merge(curr_round_epoch))) {
          LOG_WARN("fail to do major merge", KR(ret), K(curr_round_epoch));
        }
      }
    }

    LOG_TRACE("finish do merge scheduler work", KR(ret), K(curr_round_epoch), K(global_info));
    // is_merging = false, except for switchover
    check_merge_interval_time(false);
  }
  return ret;
}

int ObMajorMergeScheduler::do_before_major_merge(const int64_t expected_epoch, const bool start_merge)
{
  int ret = OB_SUCCESS;
  share::SCN global_broadcast_scn;
  global_broadcast_scn.set_min();
  FREEZE_TIME_GUARD;
  if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_global_broadcast_scn(global_broadcast_scn))) {
    LOG_WARN("fail to get global broadcast scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(progress_checker_.set_basic_info(global_broadcast_scn, expected_epoch))) {
    LOG_WARN("failed to set basic info of progress checker", KR(ret), K(global_broadcast_scn), K(expected_epoch));
  } else if (start_merge && OB_FAIL(ObColumnChecksumErrorOperator::delete_column_checksum_err_info(
      *sql_proxy_, tenant_id_, global_broadcast_scn))) {
    LOG_WARN("fail to delete column checksum error info", KR(ret), K(global_broadcast_scn));
  }
  return ret;
}

int ObMajorMergeScheduler::do_one_round_major_merge(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  HEAP_VAR(ObGlobalMergeInfo, global_info) {
    LOG_INFO("start to do one round major_merge", K(expected_epoch));
    // loop until 'this round major merge finished' or 'epoch changed'
    while (!stop_ && !is_paused()) {
      update_last_run_timestamp();
      ObCurTraceId::init(GCONF.self_addr_);
      ObZoneArray to_merge_zone;
      // Place is_last_merge_complete() to the head of this while loop.
      // So as to break this loop at once, when the last merge is complete.
      // Otherwise, may run one extra loop that should not run, and thus incur error.
      //
      if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_snapshot(global_info))) {
        LOG_WARN("fail to get zone global merge info", KR(ret));
      } else if (global_info.is_last_merge_complete()) {
        // this round major merge is complete
        break;
      } else if (OB_FAIL(get_next_merge_zones(to_merge_zone))) {  // get zones to schedule merge
        LOG_WARN("fail to get next merge zones", KR(ret));
      } else if (to_merge_zone.empty()) {
        // no new zone to merge
        LOG_INFO("no more zone need to merge", K_(tenant_id), K(global_info));
      } else if (OB_FAIL(schedule_zones_to_merge(to_merge_zone, expected_epoch))) {
        LOG_WARN("fail to schedule zones to merge", KR(ret), K(to_merge_zone), K(expected_epoch));
      }
      // Need to update_merge_status, even though to_merge_zone is empty.
      // E.g., in the 1st loop, already schedule all zones to merge, but not finish major merge.
      // In the 2nd loop, though to_merge_zone is empty, need continue to update_merge_status.
      if (FAILEDx(update_merge_status(global_info.global_broadcast_scn(), expected_epoch))) {
        LOG_WARN("fail to update merge status", KR(ret), K(expected_epoch));
        if (TC_REACH_TIME_INTERVAL(ADD_EVENT_INTERVAL)) {
          ROOTSERVICE_EVENT_ADD("daily_merge", "merge_process", K_(tenant_id),
                            "check merge progress fail", ret,
                            "global_broadcast_scn", global_info.global_broadcast_scn_,
                            "service_addr", GCONF.self_addr_);
        }
      }

      if (OB_FREEZE_SERVICE_EPOCH_MISMATCH == ret) {
        // if freeze_service_epoch changed, finish current loop. After get new epoch, re-execute
        break;
      }

      // wait some time to merge
      if (OB_SUCCESS != (tmp_ret = try_idle(DEFAULT_IDLE_US, ret))) {
        LOG_WARN("fail to idle", KR(ret));
      }

      ret = OB_SUCCESS;
      // treat as is_merging = true, even though last merge complete
      check_merge_interval_time(true);
      LOG_INFO("finish one round of loop in do_one_round_major_merge", K(expected_epoch), K(global_info));
    }
  }
  LOG_INFO("finish do_one_round_major_merge", K(expected_epoch));

  return ret;
}

int ObMajorMergeScheduler::generate_next_global_broadcast_scn(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;

  SCN new_global_broadcast_scn;
  // MERGE_STATUS: IDLE -> MERGING
  if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().generate_next_global_broadcast_scn(expected_epoch, new_global_broadcast_scn))) {
    LOG_WARN("fail to generate next broadcast scn", KR(ret), K(expected_epoch));
  } else {
    LOG_INFO("start to schedule new round merge", K(new_global_broadcast_scn), K_(tenant_id));
    ROOTSERVICE_EVENT_ADD("daily_merge", "merging", K_(tenant_id), "global_broadcast_scn",
                          new_global_broadcast_scn.get_val_for_inner_table_field(),
                          "zone", "global_zone", K(expected_epoch));
  }

  return ret;
}

int ObMajorMergeScheduler::get_next_merge_zones(ObZoneArray &to_merge)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(merge_strategy_.get_next_zone(to_merge))) {
    LOG_WARN("fail to get mext merge zones", KR(ret), K_(tenant_id));
  }

  return ret;
}

int ObMajorMergeScheduler::schedule_zones_to_merge(
    const ObZoneArray &to_merge,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;

  HEAP_VARS_2((ObZoneMergeInfoArray, info_array), (ObGlobalMergeInfo, global_info)) {
    if (to_merge.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(to_merge));
    } else if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_snapshot(global_info, info_array))) {
      LOG_WARN("get zone info snapshot failed", KR(ret));
    }

    // set zone merging flag
    if (OB_SUCC(ret)) {
      HEAP_VAR(ObZoneMergeInfo, tmp_info) {
        FOREACH_CNT_X(zone, to_merge, (OB_SUCCESS == ret) && !stop_) {
          tmp_info.reset();
          tmp_info.tenant_id_ = tenant_id_;
          tmp_info.zone_ = *zone;
          if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_zone_merge_info(tmp_info))) {
            LOG_WARN("fail to get zone", KR(ret), K(*zone));
          } else if (0 == tmp_info.is_merging_.get_value()) {
            if (OB_FAIL(set_zone_merging(*zone, expected_epoch))) {
              LOG_WARN("fail to set zone merging", KR(ret), K(*zone), K(expected_epoch));
            }
          }
        }
      }
    }

    // start merge
    if (OB_SUCC(ret)) {
      if (OB_FAIL(start_zones_merge(to_merge, expected_epoch))) {
        LOG_WARN("fail to start zone merge", KR(ret), K(to_merge), K(expected_epoch));
      } else {
        LOG_INFO("start to schedule zone merge", K(to_merge), K(expected_epoch));
      }
    }
  }

  return ret;
}

int ObMajorMergeScheduler::start_zones_merge(const ObZoneArray &to_merge, const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  SCN global_broadcast_scn;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_global_broadcast_scn(global_broadcast_scn))) {
    LOG_WARN("fail to get global broadcast scn", KR(ret));
  } else {
    for (int64_t i = 0; !stop_ && OB_SUCC(ret) && (i < to_merge.count()); ++i) {
      HEAP_VAR(ObZoneMergeInfo, tmp_info) {
        tmp_info.tenant_id_ = tenant_id_;
        tmp_info.zone_ = to_merge.at(i);
        if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_zone_merge_info(tmp_info))) {
          LOG_WARN("fail to get zone", KR(ret), "zone", tmp_info.zone_);
        } else if (0 == tmp_info.is_merging_.get_value()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_INFO("zone is not merging, can not start merge", KR(ret),
                   "zone", tmp_info.zone_, K(tmp_info));
        } else if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().start_zone_merge(to_merge.at(i), expected_epoch))) {
          LOG_WARN("fail to start zone merge", KR(ret), "zone", to_merge.at(i), K(expected_epoch));
        } else {
          ROOTSERVICE_EVENT_ADD("daily_merge", "start_merge", K_(tenant_id), "zone", to_merge.at(i),
              "global_broadcast_scn", global_broadcast_scn.get_val_for_inner_table_field()) ;
        }
      }
    }
  }
  return ret;
}

int ObMajorMergeScheduler::update_merge_status(
  const SCN &global_broadcast_scn,
  const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  compaction::ObMergeProgress progress;
  DEBUG_SYNC(RS_VALIDATE_CHECKSUM);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(progress_checker_.check_progress(progress))) {
    LOG_WARN("fail to check merge status", KR(ret), K_(tenant_id), K(expected_epoch));
    if (OB_CHECKSUM_ERROR == ret) {
      if (OB_TMP_FAIL(merge_info_mgr_->get_zone_merge_mgr().set_merge_error(ObZoneMergeInfo::ObMergeErrorType::CHECKSUM_ERROR,
          expected_epoch))) {
        LOG_WARN("fail to set merge error", KR(ret), KR(tmp_ret), K_(tenant_id), K(expected_epoch));
      }
    }
  } else {
    LOG_INFO("succcess to update merge status", K(ret), K(global_broadcast_scn), K(progress), K(expected_epoch));
    if (OB_FAIL(handle_merge_progress(progress, global_broadcast_scn, expected_epoch))) {
      LOG_WARN("fail to handle all zone merge", KR(ret), K(global_broadcast_scn), K(expected_epoch));
    }
  }

  return ret;
}

int ObMajorMergeScheduler::handle_merge_progress(
    const compaction::ObMergeProgress &progress,
    const share::SCN &global_broadcast_scn,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (progress.is_merge_finished() || progress.is_merge_abnomal()) {
    if (progress.is_merge_abnomal()) {
      LOG_WARN("merge progress is abnomal, finish progress anyway", K(global_broadcast_scn), K(progress));
    } else {
      LOG_INFO("merge completed", K(global_broadcast_scn), K(progress));
    }
    if (OB_FAIL(try_update_global_merged_scn(expected_epoch))) { // MERGE_STATUS: change to IDLE
      LOG_WARN("fail to update global_merged_scn", KR(ret), K_(tenant_id), K(expected_epoch));
    }
  } else {
    LOG_INFO("this round of traversal is completed, but there are still tablets/tables that have not been merged",
      K(ret), K(global_broadcast_scn), K(progress));
  }
  return ret;
}

int ObMajorMergeScheduler::try_update_global_merged_scn(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObGlobalMergeInfo, global_info) {
    uint64_t global_broadcast_scn_val = UINT64_MAX;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", KR(ret));
    } else if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_snapshot(global_info))) {
      LOG_WARN("fail to get global merge info", KR(ret));
    } else if (global_info.is_merge_error()) {
      LOG_WARN("should not update global merged scn, cuz is_merge_error is true", K(global_info));
    } else if (global_info.last_merged_scn() != global_info.global_broadcast_scn()) {
      if (FALSE_IT(global_broadcast_scn_val = global_info.global_broadcast_scn_.get_scn_val())) {
      } else if (OB_FAIL(update_all_tablets_report_scn(global_broadcast_scn_val, expected_epoch))) {
        LOG_WARN("fail to update all tablets report_scn", KR(ret), K(expected_epoch),
                  K(global_broadcast_scn_val));
      } else if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().try_update_global_last_merged_scn(expected_epoch))) {
        LOG_WARN("try update global last_merged_scn failed", KR(ret), K(expected_epoch));
      } else if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().finish_all_zone_merge(expected_epoch, global_broadcast_scn_val))) {
        LOG_WARN("failed to finish all zone merge", KR(ret));
      } else if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(
            *GCTX.sql_proxy_, tenant_id_, global_info, true/*print_sql*/))) {
        LOG_WARN("failed to load global merge info", KR(ret), K(global_info));
      } else if (global_info.is_last_merge_complete() && OB_FAIL(progress_checker_.clear_cached_info())) { // clear only when merge finished
        LOG_WARN("fail to do prepare handle of progress checker", KR(ret), K(expected_epoch));
      } else {
        ROOTSERVICE_EVENT_ADD("daily_merge", "global_merged", K_(tenant_id),
                              "global_broadcast_scn", global_broadcast_scn_val);
      }
    }
  }
  return ret;
}

int ObMajorMergeScheduler::set_zone_merging(const ObZone &zone, const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().set_zone_merging(zone, expected_epoch))) {
    LOG_WARN("fail to set zone merging flag", KR(ret), K_(tenant_id), K(zone), K(expected_epoch));
  } else {
    ROOTSERVICE_EVENT_ADD("daily_merge", "set_zone_merging", K_(tenant_id), K(zone));
    LOG_INFO("set zone merging success", K_(tenant_id), K(zone));
  }

  return ret;
}

int ObMajorMergeScheduler::try_update_epoch_and_reload()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(epoch_update_lock_);
  int64_t latest_epoch = -1;
  ObRole role = ObRole::INVALID_ROLE;
  if (OB_FAIL(obtain_proposal_id_from_ls(is_primary_service_, latest_epoch, role))) {
    LOG_WARN("fail to obtain latest epoch", KR(ret));
  } else if (ObRole::LEADER == role) {
    if (latest_epoch > get_epoch()) {
      const int64_t ori_epoch = get_epoch();
      // update freeze_service_epoch before reload to ensure loading latest merge_info and freeze_info
      if (OB_FAIL(do_update_freeze_service_epoch(latest_epoch))) {
        LOG_WARN("fail to update freeze_service_epoch", KR(ret), K(ori_epoch), K(latest_epoch));
        // Here, we do not know if freeze_service_epoch in __all_service_epoch has been updated to
        // latest_epoch. If it has been updated to latest_epoch, freeze_service_epoch in memory
        // must also be updated to latest_epoch. So as to keep freeze_service_epoch consistent in
        // memory and table.
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(update_epoch_in_memory_and_reload())) {
          if (OB_EAGAIN == tmp_ret) {
            LOG_WARN("fail to update epoch in memory and reload, will retry", KR(ret), KR(tmp_ret),
                     K(ori_epoch), K(latest_epoch));
            // in order to retry do_one_tenant_major_freeze, set ret to OB_EAGAIN here
            ret = tmp_ret;
          } else {
            LOG_WARN("fail to update epoch in memory and reload", KR(ret), KR(tmp_ret),
                     K(ori_epoch), K(latest_epoch));
          }
        }
      } else {
        if (OB_FAIL(do_update_and_reload(latest_epoch))) {
          LOG_WARN("fail to do update and reload", KR(ret), K(ori_epoch), K(latest_epoch));
        }
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("fail to try_update_epoch_and_reload", KR(ret), K(ori_epoch), K(latest_epoch));
      } else {
        LOG_INFO("succ to try_update_epoch_and_reload", K(ori_epoch), K(latest_epoch));
      }
    } else if (latest_epoch < get_epoch()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("latest epoch should not less than cached epoch", KR(ret), K(latest_epoch), "cached_epoch", get_epoch());
    }
  } else { // ObRole::LEADER != role
    ret = OB_NOT_MASTER;
    LOG_INFO("not master ls", KR(ret), K(role), "cur_epoch", get_epoch(), K(latest_epoch));
  }
  return ret;
}

int ObMajorMergeScheduler::do_update_freeze_service_epoch(
    const int64_t latest_epoch)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  FREEZE_TIME_GUARD;
  if (OB_FAIL(ObServiceEpochProxy::update_service_epoch(*sql_proxy_, tenant_id_,
      ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, latest_epoch, affected_rows))) {
    LOG_WARN("fail to update freeze_service_epoch", KR(ret), K(latest_epoch), K_(tenant_id));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not indeed update freeze_service_epoch", KR(ret), K(latest_epoch), K_(tenant_id), K(affected_rows));
  }
  LOG_INFO("finish to update freeze_service_epoch", KR(ret), K(latest_epoch), K_(tenant_id));
  return ret;
}

int ObMajorMergeScheduler::update_epoch_in_memory_and_reload()
{
  int ret = OB_SUCCESS;
  int64_t freeze_service_epoch = -1;
  // 1. get freeze_service_epoch from __all_service_epoch with retry
  if (OB_FAIL(get_epoch_with_retry(freeze_service_epoch))) {
    LOG_WARN("fail to get epoch with retry", KR(ret));
  } else {
    // 2. if role = LEADER, update freeze_service_epoch in memory and reload merge_info/freeze_info
    int64_t latest_epoch = -1;
    ObRole role = ObRole::INVALID_ROLE;
    if (OB_FAIL(obtain_proposal_id_from_ls(is_primary_service_, latest_epoch, role))) {
      LOG_WARN("fail to obtain latest epoch", KR(ret));
    } else if (ObRole::LEADER == role) {
      int64_t cur_epoch = get_epoch();
      if (freeze_service_epoch < cur_epoch) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("freeze_service_epoch in table should not be less than freeze_service_epoch in "
                 "memory", KR(ret), K(freeze_service_epoch), K(cur_epoch));
      } else if (latest_epoch < freeze_service_epoch) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("latest_epoch should not be less than freeze_service_epoch in table", KR(ret),
                 K(latest_epoch), K(freeze_service_epoch));
      } else if (latest_epoch > freeze_service_epoch) {
        // 1. freeze_service_epoch in __all_service_epoch has not been successfully updated, when
        // executing do_update_service_and_epoch.
        // 2. freeze_service_epoch in __all_service_epoch has [not] been successfully updated, when
        // executing do_update_service_and_epoch. After this, switch leader to other observer and
        // switch leader back to this observer.
        ret = OB_EAGAIN;
        LOG_WARN("latest_epoch is larger than freeze_service_epoch in table, will retry", KR(ret),
                 K(latest_epoch), K(freeze_service_epoch));
      } else if (latest_epoch == freeze_service_epoch) {
        // freeze_service_epoch in __all_service_epoch has been successfully updated, when executing
        // do_update_service_and_epoch. moreover, leader does not switch later.
        if (OB_FAIL(do_update_and_reload(freeze_service_epoch))) {
          LOG_WARN("fail to do update and reload", KR(ret), K(freeze_service_epoch));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to update epoch in memory and reload", K(freeze_service_epoch));
  }
  return ret;
}

int ObMajorMergeScheduler::get_epoch_with_retry(int64_t &freeze_service_epoch)
{
  int ret = OB_SUCCESS;
  int final_ret = OB_SUCCESS;
  bool get_service_epoch_succ = false;
  const int64_t MAX_RETRY_COUNT = 5;
  for (int64_t i = 0; !stop_ && OB_SUCC(ret) && (!get_service_epoch_succ) && (i < MAX_RETRY_COUNT); ++i) {
    FREEZE_TIME_GUARD;
    if (OB_FAIL(ObServiceEpochProxy::get_service_epoch(*sql_proxy_, tenant_id_,
                ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, freeze_service_epoch))) {
      const int64_t idle_time_us = 100 * 1000 * (i + 1);
      LOG_WARN("fail to get freeze_service_epoch, will retry", KR(ret), K_(tenant_id),
               K(idle_time_us), "cur_retry_count", i + 1, K(MAX_RETRY_COUNT));
      USLEEP(idle_time_us);
      final_ret = ret;
      ret = OB_SUCCESS;
    } else {
      get_service_epoch_succ = true;
    }
  }
  if (OB_SUCC(ret) && !get_service_epoch_succ) {
    ret = final_ret;
    LOG_WARN("fail to get freeze_service_epoch", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObMajorMergeScheduler::do_update_and_reload(const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_info_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge info mgr is null", KR(ret));
  } else {
    FREEZE_TIME_GUARD;
    share::SCN global_broadcast_scn;
    if (OB_FAIL(set_epoch(epoch))) {
      LOG_WARN("fail to set epoch", KR(ret), K(epoch));
    } else if (OB_FAIL(merge_info_mgr_->reload(true/*reload_zone_mereg_info*/))) {
      LOG_WARN("fail to reload merge info mgr", KR(ret));
    }
    if (OB_FAIL(ret)) {
      merge_info_mgr_->reset_info();
      LOG_WARN("fail to reload", KR(ret));
    }
  }
  return ret;
}

int ObMajorMergeScheduler::update_all_tablets_report_scn(
    const uint64_t global_braodcast_scn_val,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  FREEZE_TIME_GUARD;
  if (OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_report_scn(
          tenant_id_,
          global_braodcast_scn_val,
          ObTabletReplica::ScnStatus::SCN_STATUS_ERROR,
          stop_,
          expected_epoch))) {
    LOG_WARN("fail to batch update report_scn", KR(ret), K_(tenant_id), K(global_braodcast_scn_val));
  }
  return ret;
}

void ObMajorMergeScheduler::check_merge_interval_time(const bool is_merging)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  int64_t global_last_merged_time = -1;
  int64_t global_merge_start_time = -1;
  int64_t max_merge_time = -1;
  int64_t start_service_time = -1;
  int64_t total_service_time = -1;
  if (OB_ISNULL(merge_info_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge info mgr is unexpected nullptr", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_global_last_merged_time(global_last_merged_time))) {
    LOG_WARN("fail to get global last merged time", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_global_merge_start_time(global_merge_start_time))) {
    LOG_WARN("fail to get global merge start time", KR(ret), K_(tenant_id));
  } else {
    const int64_t MAX_NO_MERGE_INTERVAL = 36 * 3600 * 1000 * 1000L;  // 36 hours
    if ((global_last_merged_time < 0) || (global_merge_start_time < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected global_last_merged_time and global_merge_start_time", KR(ret),
               K(global_last_merged_time), K(global_merge_start_time), K_(tenant_id));
    } else if ((0 == global_last_merged_time) && (0 == global_merge_start_time)) {
      if (0 == first_check_merge_us_) {
        first_check_merge_us_ = now;
      }
      max_merge_time = first_check_merge_us_;
    } else {
      max_merge_time = MAX(global_last_merged_time, global_merge_start_time);
    }
    if (OB_SUCC(ret)) {
      ObServerTableOperator st_operator;
      if (OB_FAIL(st_operator.init(sql_proxy_))) {
        LOG_WARN("fail to init server table operator", K(ret), K_(tenant_id));
      } else {
        FREEZE_TIME_GUARD;
        if (OB_FAIL(st_operator.get_start_service_time(GCONF.self_addr_, start_service_time))) {
          LOG_WARN("fail to get start service time", KR(ret), K_(tenant_id));
        } else {
          total_service_time = now - start_service_time;
        }
      }
    }
    // In order to avoid LOG_ERROR when the tenant miss daily merge due to the cluster restarted.
    // LOG_ERROR should satisfy two additional condition:
    // 1. start_service_time > 0. start_service_time is initialized to 0 when observer starts.
    // Then it will be updated to the time when observer starts through heartbeat, which is
    // scheduled every 2 seconds.
    // 2. total_service_time > MAX_NO_MERGE_INTERVAL.
    if (OB_SUCC(ret) && !is_paused() && (start_service_time > 0)
        && (total_service_time > MAX_NO_MERGE_INTERVAL)) {
      if (is_merging) {
        if ((now - max_merge_time) > MAX_NO_MERGE_INTERVAL) {
          if (TC_REACH_TIME_INTERVAL(30 * 60 * 1000 * 1000)) {
            LOG_ERROR("long time major freeze not finish, please check it", KR(ret),
              K(global_last_merged_time), K(global_merge_start_time), K(max_merge_time),
              K(now), K_(tenant_id), K(is_merging), K(start_service_time), K(total_service_time));
          }
        }
      } else {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
        if (OB_UNLIKELY(!tenant_config.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant config is not valid", KR(ret), K_(tenant_id));
        } else if (((now - max_merge_time) > MAX_NO_MERGE_INTERVAL) &&
                   (GCONF.enable_major_freeze) &&
                   (!tenant_config->major_freeze_duty_time.disable())) {
          if (TC_REACH_TIME_INTERVAL(30 * 60 * 1000 * 1000)) {
            // standby tenant cannot launch major freeze itself, it perform major freeze according
            // to freeze info synchronized from primary tenant. therefore, standby tenants that
            // stop sync from primary tenant will not perform major freeze any more. do not print
            // error log for this case. issue-id: 56800988
            ObAllTenantInfo tenant_info;
            if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
                                                               false, tenant_info))) {
              LOG_WARN("fail to load tenant info", KR(ret), K_(tenant_id));
            } else if (tenant_info.is_standby()
                       && (tenant_info.get_standby_scn() >= tenant_info.get_recovery_until_scn())) {
              LOG_INFO("standby tenant do not sync from primary tenant any more, and do not"
                       " major freeze any more");
            } else {
              LOG_ERROR("long time no major freeze, please check it", KR(ret),
                K(global_last_merged_time), K(global_merge_start_time), K(max_merge_time),
                K(now), K_(tenant_id), K(is_merging), K(start_service_time), K(total_service_time));
            }
          }
        }
      }
    }
  }
}

} // end namespace rootserver
} // end namespace oceanbase
