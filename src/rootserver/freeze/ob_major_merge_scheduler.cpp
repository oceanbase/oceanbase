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

#define USING_LOG_PREFIX RS

#include "rootserver/freeze/ob_major_merge_scheduler.h"

#include "rootserver/ob_root_service.h"
#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "rootserver/freeze/ob_freeze_info_manager.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/freeze/ob_tenant_all_zone_merge_strategy.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "share/tablet/ob_tablet_table_iterator.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_column_checksum_error_operator.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObMergeErrorCallback::init(
    const uint64_t tenant_id,
    ObZoneMergeManager &zone_merge_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    zone_merge_mgr_ = &zone_merge_mgr;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObMergeErrorCallback::handle_merge_error(
    const int64_t error_type,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    if (OB_FAIL(zone_merge_mgr_->set_merge_error(error_type, expected_epoch))) {
      LOG_WARN("fail to set merge error", KR(ret), K_(tenant_id), K(error_type), K(expected_epoch));
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////

int ObFullChecksumValidator::init(
    const uint64_t tenant_id,
    ObMySQLProxy &sql_proxy,
    ObZoneMergeManager &zone_merge_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_validator_.init(tenant_id, &sql_proxy))) {
    LOG_WARN("fail to init tablet checksum validator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(cross_cluster_validator_.init(tenant_id, &sql_proxy))) {
    LOG_WARN("fail to init cross cluster checksum validator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(index_validator_.init(tenant_id, &sql_proxy))) {
    LOG_WARN("fail to init index checksum validator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(merge_err_cb_.init(tenant_id, zone_merge_mgr))) {
    LOG_WARN("fail to init merge error callback", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObFullChecksumValidator::execute_check(
    const ObSimpleFrozenStatus &frozen_status,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else {
    // Set check condition for each validator here.
    if (PRIMARY_CLUSTER == ObClusterInfoGetter::get_cluster_role_v2()) {
      // no need to check cross-cluster checksum in primary cluster
      cross_cluster_validator_.set_need_check(false);
    }

    if (OB_FAIL(tablet_validator_.check(frozen_status))) {
      LOG_WARN("fail to do check of tablet validator", KR(ret));
    } else if (/*OB_FAIL(cross_cluster_validator_.check(frozen_status))*/ false) {
      LOG_WARN("fail to do check of cross_cluster validator", KR(ret));
    } else if (OB_FAIL(index_validator_.check(frozen_status))) {
      LOG_WARN("fail to do check of index validator", KR(ret));
    }
    last_check_time_ = ObTimeUtility::current_time();

    if (OB_CHECKSUM_ERROR == ret) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(merge_err_cb_.handle_merge_error(ObZoneMergeInfo::CHECKSUM_ERROR, expected_epoch))) {
        LOG_WARN("fail to handle merge error", K(tmp_ret), K_(tenant_id), K(expected_epoch));
        ret = (OB_SUCC(ret) ? tmp_ret : ret);
      }
    }
  }
  return ret;
}

int ObFullChecksumValidator::sync_tablet_checksum()
{
  int ret = OB_SUCCESS;
  if (PRIMARY_CLUSTER == ObClusterInfoGetter::get_cluster_role_v2()) {
    if (OB_FAIL(cross_cluster_validator_.write_tablet_checksum_item())) {
      LOG_WARN("fail to sync tablet", KR(ret), K_(tenant_id));
    } else {
      // no need to check cross-cluster checksum if sync checksum failed.
      cross_cluster_validator_.set_need_check(false);
    }
  } else {
    LOG_TRACE("no need to sync tablet checksum in current cluster", K_(tenant_id));
  }
  return ret;
}

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

ObMajorMergeScheduler::ObMajorMergeScheduler()
  : ObFreezeReentrantThread(), is_inited_(false), fail_count_(0), first_check_merge_us_(0), 
    idling_(stop_), zone_merge_mgr_(nullptr), freeze_info_mgr_(nullptr), config_(nullptr), 
    merge_strategy_(), progress_checker_(), checksum_validator_()
{
}

int ObMajorMergeScheduler::init(
    const uint64_t tenant_id,
    ObZoneMergeManager &zone_merge_mgr,
    ObFreezeInfoManager &freeze_info_mgr,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObIServerTrace &server_trace,
    common::ObServerConfig &config,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(tenant_id));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id));
  } else if (OB_FAIL(merge_strategy_.init(tenant_id, &zone_merge_mgr))) {
    LOG_WARN("fail to init tenant zone merge strategy", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(progress_checker_.init(tenant_id, sql_proxy, schema_service,
          zone_merge_mgr, *GCTX.lst_operator_, server_trace))) {
    LOG_WARN("fail to init progress_checker", KR(ret));
  } else if (OB_FAIL(checksum_validator_.init(tenant_id, sql_proxy, zone_merge_mgr))) {
    LOG_WARN("fail to init checksum checker", KR(ret), K(tenant_id));
  } else if (OB_FAIL(idling_.init(tenant_id))) {
    LOG_WARN("fail to init idling", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    first_check_merge_us_ = 0;
    zone_merge_mgr_ = &zone_merge_mgr;
    freeze_info_mgr_ = &freeze_info_mgr;
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
      if (OB_FAIL(try_update_epoch_and_reload())) {
        LOG_WARN("fail to try_update_epoch_and_reload", KR(ret), "cur_epoch", get_epoch());
      } else if (OB_FAIL(do_work())) {
        LOG_WARN("fail to do major scheduler work", KR(ret), K_(tenant_id), "cur_epoch", get_epoch());
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(try_idle(DEFAULT_IDLE_US, ret))) {
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

  if (OB_SUCC(work_ret)) {
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
             K_(fail_count), LITERAL_K(IMMEDIATE_RETRY_CNT));
  }

  if (is_paused()) {
    while (OB_SUCC(ret) && is_paused() && !stop_) {
      LOG_INFO("major_merge_scheduler is paused", K_(tenant_id), K(idle_time_us));
      if (OB_FAIL(idling_.idle(idle_time_us))) {
        LOG_WARN("fail to idle", KR(ret));
      }
    }
    LOG_INFO("major_merge_scheduler is not paused", K_(tenant_id), K(idle_time_us));
  } else if (OB_FAIL(idling_.idle(idle_time_us))) {
    LOG_WARN("fail to idle", KR(ret), K(idle_time_us));
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
    } else if (OB_FAIL(zone_merge_mgr_->try_reload())) {
      LOG_WARN("fail to try reload", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(zone_merge_mgr_->get_snapshot(global_info, info_array))) {
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
        if (OB_FAIL(do_one_round_major_merge(curr_round_epoch))) {
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

int ObMajorMergeScheduler::do_one_round_major_merge(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  HEAP_VARS_2((ObZoneMergeInfoArray, info_array), (ObGlobalMergeInfo, global_info)) {
    LOG_INFO("start to do one round major_merge", K(expected_epoch));
    // loop until 'this round major merge finished' or 'epoch changed'
    while (!stop_ && !is_paused()) {
      update_last_run_timestamp();

      // get zones to schedule merge
      ObZoneArray to_merge_zone;
      if (OB_FAIL(get_next_merge_zones(to_merge_zone))) {
        LOG_WARN("fail to get next merge zones", KR(ret));
      } else if (to_merge_zone.empty()) {
        // no new zone to merge
        LOG_INFO("no more zone need to merge", K_(tenant_id));
      } else if (OB_FAIL(schedule_zones_to_merge(to_merge_zone, expected_epoch))) {
        LOG_WARN("fail to get next merge zones", KR(ret), K(to_merge_zone), K(expected_epoch));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(zone_merge_mgr_->get_snapshot(global_info, info_array))) {
          LOG_WARN("fail to get zone global merge info", KR(ret));
        } else if (global_info.is_last_merge_complete()) {
          // this round major merge is complete
          break;
        } else if (OB_FAIL(update_merge_status(expected_epoch))) {
          LOG_WARN("fail to update merge status", KR(ret), K(expected_epoch));
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
      tmp_ret = OB_SUCCESS;
      // treat as is_merging = true, even though last merge complete
      check_merge_interval_time(true);
    }
  }

  return ret;
}

int ObMajorMergeScheduler::generate_next_global_broadcast_scn(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;

  palf::SCN new_global_broadcast_scn;
  // MERGE_STATUS: IDLE -> MERGING
  if (OB_FAIL(zone_merge_mgr_->generate_next_global_broadcast_scn(expected_epoch, new_global_broadcast_scn))) {
    LOG_WARN("fail to generate next broadcast scn", KR(ret), K(expected_epoch));
  } else {
    LOG_INFO("start to schedule new round merge", K(new_global_broadcast_scn), K_(tenant_id));
    ROOTSERVICE_EVENT_ADD("daily_merge", "merging", K_(tenant_id), "global_broadcast_scn",
                          new_global_broadcast_scn.get_val_for_inner_table_field(),
                          "zone", "global_zone");
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
    } else if (OB_FAIL(zone_merge_mgr_->get_snapshot(global_info, info_array))) {
      LOG_WARN("get zone info snapshot failed", KR(ret));
    }

    // set zone merging flag
    if (OB_SUCC(ret)) {
      HEAP_VAR(ObZoneMergeInfo, tmp_info) {
        FOREACH_CNT_X(zone, to_merge, OB_SUCCESS == ret) {
          tmp_info.reset();
          tmp_info.tenant_id_ = tenant_id_;
          tmp_info.zone_ = *zone;
          if (OB_FAIL(zone_merge_mgr_->get_zone_merge_info(tmp_info))) {
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
  palf::SCN global_broadcast_scn;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_->get_global_broadcast_scn(global_broadcast_scn))) {
    LOG_WARN("fail to get_global_broadcast_scn", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < to_merge.count()); ++i) {
      HEAP_VAR(ObZoneMergeInfo, tmp_info) {
        tmp_info.tenant_id_ = tenant_id_;
        tmp_info.zone_ = to_merge.at(i);
        if (OB_FAIL(zone_merge_mgr_->get_zone_merge_info(tmp_info))) {
          LOG_WARN("fail to get zone", KR(ret), "zone", tmp_info.zone_);
        } else if (0 == tmp_info.is_merging_.get_value()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_INFO("zone is not merging, can not start merge", KR(ret), 
                   "zone", tmp_info.zone_, K(tmp_info));
        } else if (OB_FAIL(zone_merge_mgr_->start_zone_merge(to_merge.at(i), expected_epoch))) {
          LOG_WARN("fail to start zone merge", KR(ret), "zone", to_merge.at(i), K(expected_epoch));
        } else {
          ROOTSERVICE_EVENT_ADD("daily_merge", "start_merge", K_(tenant_id),
                                "zone", to_merge.at(i), K(global_broadcast_scn)) ;
        }
      }
    }
  }
  return ret;
}

int ObMajorMergeScheduler::update_merge_status(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObAllZoneMergeProgress all_progress;
  palf::SCN global_broadcast_scn;
  bool all_merged = true;
  ObSimpleFrozenStatus frozen_status;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_->get_global_broadcast_scn(global_broadcast_scn))) {
    LOG_WARN("fail to get_global_broadcast_scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_->get_freeze_info(global_broadcast_scn, frozen_status))) {
    LOG_WARN("fail to get freeze info", KR(ret), K_(tenant_id), K(global_broadcast_scn));
  } else if (OB_FAIL(progress_checker_.check_merge_progress(stop_, global_broadcast_scn, all_progress))) {
    LOG_WARN("fail to check merge status", KR(ret), K_(tenant_id), K(global_broadcast_scn));
    int64_t time_interval = 10L * 60 * 1000 * 1000;  // record every 10 minutes
    if (TC_REACH_TIME_INTERVAL(time_interval)) {
      ROOTSERVICE_EVENT_ADD("daily_merge", "merge_process", K_(tenant_id), 
                            "check merge progress fail", ret,
                            "global_broadcast_scn", global_broadcast_scn.get_val_for_inner_table_field(),
                            "service_addr", GCONF.self_addr_);
    }
  } else {
    HEAP_VAR(ObZoneMergeInfo, info) {
      FOREACH_X(progress, all_progress, OB_SUCC(ret)) {
        const ObZone &zone = progress->zone_;
        bool merged = false;
        info.reset();
        info.zone_ = progress->zone_;
        info.tenant_id_ = tenant_id_;

        if (OB_FAIL(zone_merge_mgr_->get_zone_merge_info(info))) {
          LOG_WARN("fail to find zone", KR(ret), K_(tenant_id), K(zone));
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          }
        } else {
          merged = (0 == progress->unmerged_tablet_cnt_);
          if (!merged) {
            all_merged = false;
            LOG_INFO("zone merge not finish", "zone", progress->zone_, "unmerged_cnt", progress->unmerged_tablet_cnt_);
          }

          palf::SCN cur_all_merged_scn;
          const palf::SCN &ori_all_merged_scn = info.all_merged_scn();
          palf::SCN last_merged_scn = (merged ? info.broadcast_scn() : info.last_merged_scn());

          if (progress->smallest_snapshot_scn_ <= palf::SCN::min_scn()) {
            cur_all_merged_scn = info.broadcast_scn();
          } else {
            cur_all_merged_scn = progress->smallest_snapshot_scn_;
          }

          if ((last_merged_scn < cur_all_merged_scn) || (ori_all_merged_scn > cur_all_merged_scn)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect version", KR(ret), K(last_merged_scn), K(cur_all_merged_scn), K(ori_all_merged_scn));
          } else {
            // TODO 'zone merge finish' & 'zone all tablet merge finish' should be handled in same procedure.
            if (info.last_merged_scn_.get_scn() != last_merged_scn) {
              LOG_INFO("this zone merge finished", K(zone), K_(tenant_id), K(cur_all_merged_scn),
                K(ori_all_merged_scn), K(last_merged_scn));
              // update last_merged_scn in all_merge_info table
              if (OB_FAIL(zone_merge_mgr_->finish_zone_merge(zone, expected_epoch, last_merged_scn,
                  ori_all_merged_scn))) {
                LOG_WARN("fail to finish zone merge", KR(ret), K(zone), K(expected_epoch));
              } else {
                ROOTSERVICE_EVENT_ADD("daily_merge", "zone_merge_finish", K_(tenant_id),
                                      "last_merged_scn", info.last_merged_scn_, K(zone));
              }
            }

            if (OB_SUCC(ret) && (ori_all_merged_scn != cur_all_merged_scn)) {
              LOG_INFO("this zone all tablet merge finished", K(zone), K_(tenant_id), K(cur_all_merged_scn),
                K(ori_all_merged_scn), K(last_merged_scn));
              // update all_merged_scn in all_merge_info table
              if (OB_FAIL(zone_merge_mgr_->finish_zone_merge(zone, expected_epoch, last_merged_scn,
                  cur_all_merged_scn))) {
                LOG_WARN("fail to finish zone merge", KR(ret), K(zone), K(expected_epoch));
              } else {
                ROOTSERVICE_EVENT_ADD("daily_merge", "zone_all_tablet_merged", K_(tenant_id),
                                      "all_merged_scn", cur_all_merged_scn, K(zone));
              }
            }
          } // end do check
        }
      }
    }

    if (OB_SUCC(ret) && all_merged) {
      // MERGE_STATUS: MERGING -> VERIFYING CHECKSUM
      if (OB_FAIL(update_global_merge_info_after_merge(expected_epoch))) {
        LOG_WARN("fail to update global merge info after merge", KR(ret), K_(tenant_id), K(expected_epoch));
      } else if (OB_FAIL(ObColumnChecksumErrorOperator::delete_column_checksum_err_info(*sql_proxy_,  
          tenant_id_, global_broadcast_scn))) {
        LOG_WARN("fail to delete column checksum error info", KR(ret), K_(tenant_id), K(global_broadcast_scn));
      } else if (OB_FAIL(checksum_validator_.execute_check(frozen_status, expected_epoch))) { 
        LOG_WARN("fail to execute checking checksum", KR(ret), K_(tenant_id), K(expected_epoch));
      // TODO NOT support cross-cluster checksum verify
      } else if (/*OB_FAIL(checksum_validator_.sync_tablet_checksum())*/ false) {
        LOG_WARN("fail to sync tablet checksum item", KR(ret), K_(tenant_id));
      }

      // MERGE_STATUS: VERIFYING CHECKSUM -> IDLE
      if (FAILEDx(try_update_global_merged_scn(expected_epoch))) {
        LOG_WARN("fail to update global_merged_scn", KR(ret), K_(tenant_id), K(expected_epoch));
      }
    }
  }

  return ret;
}

int ObMajorMergeScheduler::update_global_merge_info_after_merge(const int64_t expected_epaoch)
{
  int ret = OB_SUCCESS;
  // need update global merge_status
  if (OB_FAIL(zone_merge_mgr_->update_global_merge_info_after_merge(expected_epaoch))) {
    LOG_WARN("fail to update global merge info after merge", KR(ret), K_(tenant_id), K(expected_epaoch));
  }
  return ret;
}

int ObMajorMergeScheduler::try_update_global_merged_scn(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  HEAP_VARS_2((ObZoneMergeInfoArray, infos), (ObGlobalMergeInfo, global_info)) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", KR(ret));
    } else if (OB_FAIL(zone_merge_mgr_->get_snapshot(global_info, infos))) {
      LOG_WARN("fail to get zone info", KR(ret));
    } else {
      if (global_info.last_merged_scn() != global_info.global_broadcast_scn()) {
        bool merged = true;
        FOREACH_X(info, infos, merged) {
          if (info->last_merged_scn() != global_info.global_broadcast_scn()) {
            merged = false;
          }
        }
        if (merged) {
          if (OB_FAIL(zone_merge_mgr_->try_update_global_last_merged_scn(expected_epoch))) {
            LOG_WARN("try update global last_merged_scn failed", KR(ret), K(expected_epoch));
          } else {
            ROOTSERVICE_EVENT_ADD("daily_merge", "global_merged", K_(tenant_id),
                                  "global_broadcast_scn", global_info.global_broadcast_scn_.get_scn_val());
          }
        }
      }
    }
  }
  return ret;
}

int ObMajorMergeScheduler::set_zone_merging(const ObZone &zone, const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_merge_mgr_->set_zone_merging(zone, expected_epoch))) {
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
  if (OB_FAIL(obtain_proposal_id_from_ls(latest_epoch, role))) {
    LOG_WARN("fail to obtain latest epoch", KR(ret));
  } else if (ObRole::LEADER == role) {
    if (latest_epoch > get_epoch()) {
      const int64_t ori_epoch = get_epoch();
      // update freeze_service_epoch before reload to ensure loading latest merge_info and freeze_info
      if (OB_FAIL(do_update_freeze_service_epoch(latest_epoch))) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(set_epoch(ori_epoch))) {
          LOG_WARN("fail to set epoch", KR(ret), KR(tmp_ret), K(ori_epoch));
        }
        LOG_WARN("fail to update freeze_service_epoch", KR(ret), K(ori_epoch), K(latest_epoch));
      } else {
        if (OB_FAIL(set_epoch(latest_epoch))) {
          LOG_WARN("fail to set epoch", KR(ret), K(latest_epoch));
        } else if (OB_FAIL(zone_merge_mgr_->reload())) {
          LOG_WARN("fail to reload zone_merge_mgr", KR(ret));
        } else if (OB_FAIL(freeze_info_mgr_->reload())) {
          LOG_WARN("fail to reload freeze_info_mgr", KR(ret));
        }
        if (OB_FAIL(ret)) {
          zone_merge_mgr_->reset_merge_info();
          freeze_info_mgr_->reset_freeze_info();
          LOG_WARN("fail to reload", KR(ret));
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
  }
  return ret;
}

int ObMajorMergeScheduler::do_update_freeze_service_epoch(
    const int64_t latest_epoch)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
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

void ObMajorMergeScheduler::check_merge_interval_time(const bool is_merging)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  int64_t global_last_merged_time = -1;
  int64_t global_merge_start_time = -1;
  int64_t max_merge_time = -1;
  if (OB_ISNULL(zone_merge_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_merge_mgr_ is unexpected nullptr", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(zone_merge_mgr_->get_global_last_merged_time(global_last_merged_time))) {
    LOG_WARN("fail to get global last merged time", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(zone_merge_mgr_->get_global_merge_start_time(global_merge_start_time))) {
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
    if (OB_SUCC(ret) && !is_paused()) {
      if (is_merging) {
        if ((now - max_merge_time) > MAX_NO_MERGE_INTERVAL) {
          if (TC_REACH_TIME_INTERVAL(30 * 60 * 1000 * 1000)) {
            LOG_ERROR("long time major freeze not finish, please check it", KR(ret), K(global_last_merged_time),
              K(global_merge_start_time), K(max_merge_time), K(now), K_(tenant_id), K(is_merging));
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
            LOG_ERROR("long time no major freeze, please check it", KR(ret), K(global_last_merged_time),
              K(global_merge_start_time), K(max_merge_time), K(now), K_(tenant_id), K(is_merging));
          }
        }
      }
    }
  }
}

} // end namespace rootserver
} // end namespace oceanbase
