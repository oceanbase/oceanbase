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

#include "ob_daily_merge_scheduler.h"

#include "lib/container/ob_se_array_iterator.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/lock/ob_latch.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/utility/utility.h"
#include "share/ob_debug_sync.h"
#include "share/config/ob_server_config.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_sstable_checksum_operator.h"
#include "share/ob_schema_status_proxy.h"
#include "observer/ob_server_struct.h"
#include "ob_rs_event_history_table_operator.h"
#include "ob_index_builder.h"
#include "ob_zone_manager.h"
#include "ob_freeze_info_manager.h"

namespace oceanbase {
namespace rootserver {
using namespace common;
using namespace share;
using namespace share::schema;

int64_t ObDailyMergeIdling::get_idle_interval_us()
{
  return GCONF.merger_check_interval;
}

ObDailyMergeScheduler::ObDailyMergeScheduler()
    : ObRsReentrantThread(true),
      inited_(false),
      zone_mgr_(NULL),
      config_(NULL),
      self_(),
      pt_util_(NULL),
      idling_(stop_),
      ddl_service_(NULL),
      switch_leader_mgr_(),
      merger_warm_up_duration_time_(0),
      leader_info_array_(),
      checksum_checker_(NULL),
      execute_order_generator_(),
      sql_proxy_(NULL),
      schema_service_(NULL),
      pt_operator_(NULL),
      remote_pt_operator_(NULL),
      switch_leader_check_interval_(0),
      last_start_merge_time_(0),
      freeze_info_manager_(NULL)
{}

ObDailyMergeScheduler::~ObDailyMergeScheduler()
{
  if (!stop_) {
    stop();
    wait();
  }
}

int ObDailyMergeScheduler::init(ObZoneManager& zone_mgr, common::ObServerConfig& config, const ObZone& self_zone,
    ObILeaderCoordinator& leader_coordinator, ObPartitionTableUtil& pt_util, ObDDLService& ddl_service,
    ObServerManager& server_mgr, ObPartitionTableOperator& pt, ObRemotePartitionTableOperator& remote_pt,
    ObMultiVersionSchemaService& schema_service, ObMySQLProxy& sql_proxy, ObFreezeInfoManager& freeze_info_manager,
    ObRootBalancer& root_balancer)
{
  int ret = OB_SUCCESS;
  const int daily_merge_scheduler_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (self_zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(self_zone));
  } else if (OB_FAIL(create(daily_merge_scheduler_thread_cnt, "DayMergeSche"))) {
    LOG_WARN("create thread failed", K(ret), K(daily_merge_scheduler_thread_cnt));
  } else if (OB_FAIL(switch_leader_mgr_.init(leader_coordinator, zone_mgr, config))) {
    LOG_WARN("failed to init switch leader mgr", K(ret));
  } else {
    execute_order_generator_.init(zone_mgr, server_mgr, pt, schema_service);
    zone_mgr_ = &zone_mgr;
    config_ = &config;
    self_ = self_zone;
    pt_util_ = &pt_util;
    ddl_service_ = &ddl_service;
    // leader_pos_backuped_ = false;
    merger_warm_up_duration_time_ = 0;  // forbid config.merger_warm_up_duration_time;
    sql_proxy_ = &sql_proxy;
    schema_service_ = &schema_service;
    pt_operator_ = &pt;
    remote_pt_operator_ = &remote_pt;
    freeze_info_manager_ = &freeze_info_manager;
    last_start_merge_time_ = ObTimeUtility::current_time();
    root_balancer_ = &root_balancer;
    inited_ = true;
  }
  return ret;
}

void ObDailyMergeScheduler::run3()
{
  LOG_INFO("daily merge scheduler start");
  int ret = OB_SUCCESS;
  const int64_t immediate_retry_cnt = 3;
  int64_t fail_count = 0;
  // when rs start, try clear backup leader pos
  // leader_pos_backuped_ = true;
  leader_info_array_.reset();
  int64_t last_error_log_time = 0;
  const int64_t PRINT_LOG_INTERVAL = 300 * 1000L * 1000L;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(try_start_smooth_coordinate())) {
    LOG_WARN("fail to restore smooth coordinate", K(ret));
  }

  if (OB_SUCC(ret)) {
    while (!stop_) {
      update_last_run_timestamp();
      DEBUG_SYNC(BEFORE_DAILY_MERGE_RUN);
      if (OB_ISNULL(checksum_checker_)) {
        LOG_ERROR("invalid checksum checker", K(checksum_checker_));
      } else {
        checksum_checker_->set_merging_status(false);
        checksum_checker_->check();
      }
      if (fail_count > immediate_retry_cnt) {
        LOG_WARN("daily merge failed more than immediate cnt, turn to idle status",
            K(fail_count),
            LITERAL_K(immediate_retry_cnt));
        if (OB_FAIL(idle())) {
          LOG_WARN("idle failed", K(ret));
        }
      } else if (fail_count > 0) {
        LOG_WARN("daily merge failed, will immediate retry", K(fail_count));
        int64_t short_sleep_time_us = fail_count * 10 * 1000 * 1000L;
        if (short_sleep_time_us > GCONF.merger_check_interval) {
          short_sleep_time_us = GCONF.merger_check_interval;
        }
        if (OB_FAIL(idling_.idle(short_sleep_time_us))) {
          LOG_WARN("idle failed", K(ret), K(short_sleep_time_us));
        }
      }
      // can not continue when merge error
      if (OB_SUCC(ret)) {
        bool is_merge_error = false;
        if (OB_FAIL(zone_mgr_->is_merge_error(is_merge_error))) {
          LOG_WARN("fail to check is merge error", K(ret));
        } else if (is_merge_error) {
          if (ObTimeUtility::current_time() - last_error_log_time > PRINT_LOG_INTERVAL) {
            last_error_log_time = ObTimeUtility::current_time();
            LOG_ERROR("merge error, refuse to start merge", K(ret), K(is_merge_error));
          }
          if (OB_FAIL(idle())) {
            LOG_WARN("idle failed", K(ret));
          }
        } else {
          // if (OB_SUCC(ret)) {
          //  if (!zone_mgr_->is_stagger_merge() && leader_pos_backuped_) {
          //    if (OB_FAIL(try_restore_leader_pos())) {
          //      LOG_WARN("failed to do try restore leader pos", K(ret));
          //    }
          //  }
          //}

          if (OB_FAIL(ret)) {
          } else {
            ret = main_round();
            if (OB_SUCC(ret)) {
              LOG_DEBUG("daily merge schedule round success");
              fail_count = 0;
            } else {
              LOG_WARN("daily merge schedule round failed", K(ret));
              fail_count++;
            }
          }
        }
      }
      // retry until stopped, reset ret to OB_SUCCESS
      ret = OB_SUCCESS;
    }
  }
  LOG_INFO("daily merge scheduler quit, reset_op_array");
  switch_leader_mgr_.reset_op_array();
  leader_info_array_.reset();
  return;
}

// int ObDailyMergeScheduler::try_restore_leader_pos()
//{
//  int ret = OB_SUCCESS;
//  bool is_finish = false;
//  bool is_succ = false;
//  const int64_t warm_up_duration_time = merger_warm_up_duration_time_;
//  ObPartitionTableUtil::ObLeaderInfoArray *skip_leader_info_array = NULL;
//  if (OB_FAIL(switch_leader_mgr_.check_all_op_finish(is_finish))) {
//    LOG_WARN("failed to check is all op finish", K(ret));
//  } else if (!is_finish) {
//    // do nothing
//  } else if (OB_FAIL(switch_leader_mgr_.is_last_switch_turn_succ(is_succ))) {
//    LOG_WARN("failed to check is last switch leader succ", K(ret));
//  } else if (is_succ) {
//    if (warm_up_duration_time > 0) {
//      const bool backup_flag = false;
//      DEBUG_SYNC(DAILY_MERGE_BEFORE_RESTORE_LEADER_POS);
//      LOG_INFO("start set_leader_backup_flag", K(backup_flag));
//      if (OB_FAIL(pt_util_->set_leader_backup_flag(stop_, backup_flag, skip_leader_info_array))) {
//        LOG_WARN("clear leader backup flag failed", K(ret));
//      } else {
//        leader_pos_backuped_ = false;
//      }
//      LOG_INFO("finish set_leader_backup_flag", K(backup_flag));
//    } else {
//      leader_pos_backuped_ = false;
//      LOG_INFO("finish set_leader_backup_flag, nothing todo");
//    }
//  } else if (OB_FAIL(switch_leader_mgr_.start_switch_leader_without_warm_up())) {
//    LOG_WARN("failed to start_switch_leader_without_warm_up", K(ret));
//  }
//  return ret;
//}

int ObDailyMergeScheduler::main_round()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  ObZoneInfoArray infos;
  HEAP_VAR(ObGlobalInfo, global)
  {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(zone_mgr_->get_snapshot(global, infos))) {
      LOG_WARN("get zone status snapshot failed", K(ret));
    } else {
      if (global.last_merged_version_ == global.global_broadcast_version_ &&
          global.global_broadcast_version_ >= global.frozen_version_) {
        // check merge status if not all partition updated
        bool all_merged = false;
        if (OB_FAIL(is_all_partition_merged(global.global_broadcast_version_, infos, all_merged))) {
          LOG_WARN("check is all partition merged failed",
              K(ret),
              "global_broad_cast_version",
              global.global_broadcast_version_.value_);
        } else {
          // not all partitions merged, try to update merge status
          if (!all_merged) {
            if (OB_FAIL(update_merge_status())) {
              LOG_WARN("update merge status failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          // wait major freeze
          if (OB_FAIL(idle())) {
            LOG_WARN("idle failed", K(ret));
          }
        }
      } else {
        if (need_switch_leader()) {
          switch_leader_mgr_.set_merge_status(true);
        }
        if (OB_FAIL(schedule_daily_merge())) {
          LOG_WARN("schedule daily merge failed", K(ret));
        }
        switch_leader_mgr_.set_merge_status(false);
        switch_leader_mgr_.wake_up_leader_coordinator();
      }
    }
    check_merge_timeout();
  }
  return ret;
}

int ObDailyMergeScheduler::is_all_partition_merged(
    const int64_t global_broadcast_version, const ObZoneInfoArray& infos, bool& merged)
{
  int ret = OB_SUCCESS;
  merged = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (global_broadcast_version <= 0 || infos.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(global_broadcast_version), K(infos));
  } else {
    FOREACH_CNT_X(zone, infos, OB_SUCCESS == ret && merged)
    {
      if (zone->all_merged_version_ != global_broadcast_version) {
        merged = false;
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::schedule_daily_merge()
{
  int ret = OB_SUCCESS;
  ObZoneInfoArray infos;
  // const int64_t warm_up_duration_time = merger_warm_up_duration_time_;
  HEAP_VAR(ObGlobalInfo, global)
  {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(zone_mgr_->get_snapshot(global, infos))) {
      LOG_WARN("get zone status snapshot failed", K(ret));
    } else {
      // update global_broadcast_version if needed
      if (global.last_merged_version_ == global.global_broadcast_version_ &&
          global.global_broadcast_version_ < global.frozen_version_) {
        // if (zone_mgr_->is_stagger_merge() && warm_up_duration_time > 0) {
        //  if (OB_FAIL(backup_leader_pos())) {
        //    LOG_WARN("backup leader position failed", K(ret));
        //  }
        //}
        ObSimpleFrozenStatus frozen_info;
        int64_t now = ObTimeUtility::current_time();
        if (OB_FAIL(ret)) {
        } else if (PRIMARY_CLUSTER == ObClusterInfoGetter::get_cluster_type_v2()) {
          if (OB_FAIL(freeze_info_manager_->get_max_frozen_status_for_daily_merge(frozen_info))) {
            LOG_WARN("fail to get max frozen status with schema version", K(ret));
          } else if (frozen_info.frozen_version_ < global.global_broadcast_version_ + 1) {
            ret = OB_EAGAIN;
            LOG_WARN("wait for frozen_timestamp and schema_version", K(ret), K(now), K(frozen_info));
          }
        } else {
          // nothing to do
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(zone_mgr_->generate_next_global_broadcast_version())) {
          LOG_WARN("generate next broadcast version failed", K(ret));
        } else {
          int64_t global_broadcast_version = 0;
          if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
            LOG_WARN("get_global_broadcast_version failed", K(ret));
          } else {
            LOG_INFO("start to schdule new round merge", "version", global_broadcast_version);
            ROOTSERVICE_EVENT_ADD(
                "daily_merge", "merging", "merge_version", global_broadcast_version, "zone", "global_zone");
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schedule_stagger_merge())) {
        LOG_WARN("schedule stagger merge failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::schedule_stagger_merge()
{
  int ret = OB_SUCCESS;
  // ObPartitionTableUtil::ObLeaderInfoArray *skip_leader_info_array = NULL;
  int64_t global_broadcast_version = 0;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
    LOG_WARN("fail to get global broadcast version", KR(ret));
  } else {
    bool checked = false;
    bool in_merge = true;
    bool merge_error = false;
    ObZoneArray to_merge;
    LOG_INFO("start to schedule_stagger_merge", K(global_broadcast_version));
    while (!stop_) {
      update_last_run_timestamp();
      LOG_INFO("stagger merge is in progress", K(global_broadcast_version));
      ret = OB_SUCCESS;
      to_merge.reuse();
      DEBUG_SYNC(HANG_MERGE);
      bool is_merge_error = false;
      if (OB_ISNULL(checksum_checker_)) {
        LOG_ERROR("invalid checksum checker", K(checksum_checker_));
      } else if (OB_FAIL(zone_mgr_->is_merge_error(is_merge_error))) {
        LOG_WARN("fail to check is merge error", K(ret));
      } else if (is_merge_error) {
        LOG_ERROR("merge error; please check it", K(ret), K(is_merge_error));
      } else {
        checksum_checker_->set_merging_status(true);
        checksum_checker_->check();
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(next_merge_zones(to_merge))) {
          LOG_WARN("fail to get next merge zones", K(ret));
        } else if (OB_FAIL(zone_mgr_->is_in_merge(in_merge))) {
          LOG_WARN("check is in merge failed", K(ret));
        } else if (!in_merge) {
          // const bool backup_flag = false;
          bool is_finish = false;
          if (OB_FAIL(switch_leader_mgr_.check_all_op_finish(is_finish))) {
            LOG_WARN("failed to check is all switch leader op finish", K(ret));
          } else if (is_finish) {
            // const int64_t warm_up_duration_time = merger_warm_up_duration_time_;
            // if (leader_pos_backuped_ && warm_up_duration_time > 0) {
            //  bool is_succ = false;
            //  if (OB_FAIL(switch_leader_mgr_.is_last_switch_turn_succ(is_succ))) {
            //    LOG_WARN("failed to check is last switch leader succ", K(ret));
            //  } else if (is_succ) {
            //    DEBUG_SYNC(DAILY_MERGE_BEFORE_RESTORE_LEADER_POS);
            //    LOG_INFO("start set_leader_backup_flag", K(backup_flag));
            //    if (OB_FAIL(pt_util_->set_leader_backup_flag(stop_, backup_flag, skip_leader_info_array))) {
            //      LOG_WARN("clear leader backup flag failed", K(ret));
            //    } else {
            //      leader_pos_backuped_ = false;
            //    }
            //    LOG_INFO("finish set_leader_backup_flag", K(backup_flag));
            //  } else {
            //    LOG_WARN("last switch leader loop is failed, cannot clear leader pos backup flag");
            //  }
            //}

            if (OB_SUCC(ret)) {
              LOG_INFO("all zone stagger merge finished", K(global_broadcast_version));
              break;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (!to_merge.empty()) {
          if (OB_FAIL(zone_mgr_->is_merge_error(merge_error))) {
            LOG_WARN("fail to check is_merge_error failed", K(ret));
          } else if (!merge_error && !config_->is_manual_merge_enabled() &&
                     GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_1440) {
            last_start_merge_time_ = ObTimeUtility::current_time();
            if (OB_FAIL(process_zones(to_merge))) {
              LOG_WARN("start merge zones failed", K(ret), K(to_merge));
            }
          } else {
            LOG_WARN("fail to start zone merge",
                K(merge_error),
                "enable_manual_merge",
                config_->is_manual_merge_enabled(),
                "min_cluster_version",
                GET_MIN_CLUSTER_VERSION());
            if (merge_error) {
              LOG_ERROR("merge error!! refuse to schedule merge any more", K(ret), K(merge_error));
            } else if (config_->is_manual_merge_enabled()) {
              int64_t now = ObTimeUtility::current_time();
              if (now - last_start_merge_time_ > config_->zone_merge_timeout) {
                LOG_ERROR(
                    "long time no zone start to merge, please check it", K(ret), K(now), K(last_start_merge_time_));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = switch_leader_mgr_.do_work();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("failed to do switch leader mgr work", K(tmp_ret));
          if (OB_SUCCESS == ret) {
            ret = tmp_ret;
          }
        }
      }

      if (OB_SUCC(ret)) {
        // No need to idle if no new merge started or in first loop.
        if (checked || to_merge.count() > 0) {
          if (OB_FAIL(idle())) {
            LOG_WARN("idle failed", K(ret));
          }
        }
      }

      // always try to update merge status
      if (!stop_) {
        int tmp_ret = update_merge_status();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("update merge status failed", K(tmp_ret));
        }
        checked = true;
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }  // end while
  }

  LOG_INFO("finish schedule_stagger_merge", K(ret), K(global_broadcast_version));
  return ret;
}

int ObDailyMergeScheduler::manual_start_merge(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  ObZoneArray to_merge;
  bool in_merge = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty zone", K(ret));
  } else if (OB_FAIL(to_merge.push_back(zone))) {
    LOG_WARN("add to merge zone failed", K(ret));
  } else if (OB_FAIL(zone_mgr_->is_in_merge(in_merge))) {
    LOG_WARN("check is in merge failed", K(ret));
  } else if (!in_merge) {
    ret = OB_MERGE_NOT_STARTED;
    LOG_WARN("not in merging status, please try again later after global merge started", K(ret), K(zone));
  } else {
    HEAP_VAR(ObGlobalInfo, global)
    {
      ObZoneInfoArray infos;
      if (OB_FAIL(zone_mgr_->get_snapshot(global, infos))) {
        LOG_WARN("get zone info snapshot failed", K(ret));
      } else {
        bool found = false;
        FOREACH_CNT_X(info, infos, OB_SUCCESS == ret && !found)
        {
          if (info->zone_ == zone) {
            found = true;
            if (info->broadcast_version_ == global.global_broadcast_version_) {
              ret = OB_MERGE_ALREADY_STARTED;
              LOG_WARN("merge already started", K(ret), K(zone), "zone_info", *info);
            } else if (OB_FAIL(process_zones(to_merge))) {
              LOG_WARN("start merge zone failed", K(ret), K(zone));
            }
          }
        }
        if (!found) {
          ret = OB_ZONE_INFO_NOT_EXIST;
          LOG_WARN("zone not found", K(ret), K(zone));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = switch_leader_mgr_.wake_up_leader_coordinator();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to wake up leader coordinator", K(ret));
    }
  }
  return ret;
}

int ObDailyMergeScheduler::process_zones(const ObZoneArray& to_merge)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObGlobalInfo, global)
  {
    ObZoneInfoArray infos;
    lib::ObMutexGuard guard(mutex_);
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (to_merge.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(to_merge));
    } else if (OB_FAIL(zone_mgr_->get_snapshot(global, infos))) {
      LOG_WARN("get zone info snapshot failed", K(ret));
    }
    // set zone merging flag
    if (OB_SUCC(ret)) {
      HEAP_VAR(ObZoneInfo, tmp_info)
      {
        FOREACH_CNT_X(zone, to_merge, OB_SUCCESS == ret)
        {
          tmp_info.reset();
          tmp_info.zone_ = *zone;
          if (OB_FAIL(zone_mgr_->get_zone(tmp_info))) {
            LOG_WARN("failed to get zone", K(ret), K(*zone));
          } else if (!tmp_info.is_merging_) {
            if (OB_FAIL(set_zone_merging(*zone))) {
              LOG_WARN("fail to set zone merging", K(ret), K(zone));
            } else {
              LOG_INFO("set zone merging", K(*zone));
            }
          }  // end !tmp_info.is_merging_
        }    // end FOREACH_CNT_X(zone,
      }
    }
    // start merge
    if (OB_SUCC(ret)) {
      if (OB_FAIL(start_zones_merge(to_merge))) {
        LOG_WARN("start zone merge failed", K(ret), K(to_merge));
      } else {
        LOG_INFO("start to schedule zone merge", K(to_merge));
      }
    }
  }
  return ret;
}

bool ObDailyMergeScheduler::need_switch_leader()
{
  bool bret = false;
  if (!GCONF.enable_auto_leader_switch || GCONF.enable_manual_merge || !GCONF.enable_merge_by_turn) {
    bret = false;
  } else {
    bret = true;
  }

  LOG_DEBUG("check need swtich leader", K(bret));
  return bret;
}

int ObDailyMergeScheduler::start_zones_merge(const ObZoneArray& to_merge)
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  int64_t global_broadcast_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(switch_leader_mgr_.check_all_op_finish(is_finish))) {
    LOG_WARN("failed to check is all op finish", K(ret));
  } else if (!is_finish) {
    // nothing todo
  } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
    LOG_WARN("get_global_broadcast_version failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < to_merge.count(); ++i) {
      bool is_succ = true;
      HEAP_VAR(ObZoneInfo, tmp_info)
      {
        tmp_info.zone_ = to_merge.at(i);
        if (OB_FAIL(zone_mgr_->get_zone(tmp_info))) {
          LOG_WARN("failed to get zone", K(ret), "zone", tmp_info.zone_);
        } else if (!tmp_info.is_merging_) {
          // nothing todo
        } else if (OB_FAIL(check_switch_result(to_merge.at(i), is_succ))) {
          LOG_WARN("fail to check switch leader", K(ret), "zone", to_merge.at(i));
        } else if (is_succ) {
          if (OB_FAIL(zone_mgr_->start_zone_merge(to_merge.at(i)))) {
            LOG_WARN("start zone merge failed", K(ret), "zone", to_merge.at(i));
          } else {
            ROOTSERVICE_EVENT_ADD("daily_merge", "start_merge", "zone", to_merge.at(i), K(global_broadcast_version));
          }
        } else {
          switch_leader_check_interval_ = SWITCH_LEADER_CHECK_INTERVAL;
          LOG_ERROR("fail to switch leader, can't start merge now", K(ret), "zone", to_merge.at(i));
          ROOTSERVICE_EVENT_ADD("daily_merge", "switch_leader_fail", "zone", to_merge.at(i));
          int64_t revert_ret = OB_SUCCESS;
          if (OB_SUCCESS != (revert_ret = cancel_zone_merging(to_merge.at(i)))) {
            LOG_WARN("fail to clear merging flag", K(ret), "zone", to_merge.at(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::set_zone_merging(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, info)
  {
    info.zone_ = zone;
    bool need_switch = need_switch_leader();
    if (OB_FAIL(zone_mgr_->get_zone(info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find zone failed", K(ret), K(zone));
      } else {
        ret = OB_SUCCESS;
        LOG_INFO("ignore not exist zones", "zone", info.zone_);
      }
    } else if (need_switch) {
      if (OB_FAIL(set_zone_merging_while_need_switch_leader(zone))) {
        LOG_WARN("fail to set zone merging", KR(ret), K(zone));
      }
    } else {
      if (OB_FAIL(zone_mgr_->set_zone_merging(zone))) {
        LOG_WARN("set zone merging flag failed", K(ret), "zone", zone);
      } else {
        ROOTSERVICE_EVENT_ADD("daily_merge", "set_zone_merging", K(zone));
        LOG_INFO("set zone merging success", K(ret), K(zone));
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::set_zone_merging_while_need_switch_leader(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  bool switch_leader_succ = true;
  HEAP_VAR(ObZoneInfo, info)
  {
    info.zone_ = zone;
    bool need_switch = need_switch_leader();
    if (!need_switch) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be here", KR(ret), K(zone), K(need_switch));
    } else if (OB_FAIL(zone_mgr_->get_zone(info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find zone failed", K(ret), K(zone));
      } else {
        ret = OB_SUCCESS;
        LOG_INFO("ignore not exist zones", "zone", info.zone_);
      }
    } else if (need_switch && info.start_merge_fail_times_ > 0) {
      ObZoneArray zone_list;
      common::ObSEArray<bool, 1> results;
      if (OB_FAIL(zone_list.push_back(zone))) {
        LOG_WARN("fail to push back", K(ret), K(zone));
      } else if (OB_FAIL(switch_leader_mgr_.check_switch_leader(zone_list, results))) {
        switch_leader_check_interval_ = SWITCH_LEADER_CHECK_INTERVAL;
        if (OB_EAGAIN == ret) {
          // try again
        } else {
          LOG_WARN("fail to check switch leader", K(ret));
        }
      } else if (results.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid result count", K(ret));
      } else {
        switch_leader_succ = results.at(0);
      }
    }
    if ((OB_SUCC(ret) && !switch_leader_succ) || OB_EAGAIN == ret) {
      switch_leader_check_interval_ = SWITCH_LEADER_CHECK_INTERVAL;
      info.start_merge_fail_times_++;
      if (info.start_merge_fail_times_ % 2 == 0) {
        LOG_ERROR(
            "can't switch leader for daily merge, please check it", K(zone), "fail_time", info.start_merge_fail_times_);
        ROOTSERVICE_EVENT_ADD("daily_merge", "switch_leader_fail", K(zone));
      }
      if (OB_FAIL(zone_mgr_->inc_start_merge_fail_times(info.zone_))) {
        LOG_WARN("fail to inc start merge fail times", K(ret), K(info));
      }
    }
    if (OB_SUCC(ret) && !need_switch) {
      // nothing todo
    } else if ((OB_SUCC(ret) && switch_leader_succ) || (OB_EAGAIN == ret && info.start_merge_fail_times_ % 5 == 0)) {
      if (OB_FAIL(switch_leader_mgr_.add_set_zone_merging_op(zone, leader_info_array_))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to add set zone merging op", K(ret), K(zone));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        ROOTSERVICE_EVENT_ADD("daily_merge", "add_set_zone_merging_op", K(zone));
        LOG_INFO("add set zone merging op success", K(ret), K(zone));
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::cancel_zone_merging(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (OB_FAIL(zone_mgr_->clear_zone_merging(zone))) {
    LOG_WARN("fail to clear merging flag", K(ret), K(zone));
  } else {
    ROOTSERVICE_EVENT_ADD("daily_merge", "clear_zone_merging", K(zone));
    LOG_WARN("clear zone merging flag", K(ret), K(zone));
  }
  return ret;
}

bool ObDailyMergeScheduler::need_check_switch_leader()
{
  bool bret = false;
  if (need_switch_leader()) {
    bret = true;
  } else {
    bret = false;
  }
  LOG_DEBUG("need check swtich leader", K(bret));
  return bret;
}

int ObDailyMergeScheduler::check_switch_result(const ObZone& zone, bool& is_succ)
{
  int ret = OB_SUCCESS;
  ObPartitionTableIterator iter;
  is_succ = true;
  bool ignore_row_checksum = true;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service not init", K(ret));
  } else if (!need_check_switch_leader()) {
    is_succ = true;
  } else if (OB_FAIL(iter.init(*pt_operator_, *schema_service_, ignore_row_checksum))) {
    LOG_WARN("partition table iterator init failed", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_only_user_table())) {
    LOG_WARN("fail to set filter", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_only_user_tenant())) {
    LOG_WARN("fail to set filter", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_only_full_replica())) {
    LOG_WARN("fail to set filter", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_replica_status(REPLICA_STATUS_NORMAL))) {
    LOG_WARN("set filter failed", K(ret));
  } else if (OB_FAIL(iter.get_partition_entity_filters().filter_single_primary_zone(*zone_mgr_))) {
    LOG_WARN("fail to set filter", K(ret));
  } else if (OB_FAIL(iter.get_filters().set_zone(zone))) {
    LOG_WARN("fail to set zone trace", K(ret), K(zone));
  } else {
    ObPartitionInfo partition_info;
    while (OB_SUCC(ret)) {
      partition_info.reuse();
      if (OB_FAIL(iter.next(partition_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("iterator partition table failed", K(ret));
        }
        break;
      } else {
        FOREACH_CNT_X(replica, partition_info.get_replicas_v2(), OB_SUCC(ret))
        {
          if (OB_ISNULL(replica)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid replica", K(ret), K(replica));
          } else if (REPLICA_TYPE_FULL != replica->replica_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect replica type, should be full replica", K(ret), "replica", *replica);
          } else if (replica->is_leader_by_election()) {
            is_succ = false;
            LOG_WARN("fail to switch leader while daily merge", K(partition_info), K(zone));
            ROOTSERVICE_EVENT_ADD("daily_merge",
                "partition switch leader fail",
                "tenant_id",
                partition_info.get_tenant_id(),
                "table_id",
                partition_info.get_table_id(),
                "partition_id",
                partition_info.get_partition_id(),
                "leader",
                replica->server_);
          }
        }  // end FOREACH_CNT_X
      }
    }  // end while
  }
  return ret;
}

int ObDailyMergeScheduler::calc_suspend_zone_count(int64_t& suspend_zone_count)
{
  int ret = OB_SUCCESS;
  ObZoneInfoArray all_zone;
  suspend_zone_count = 0;
  int64_t global_broadcast_version = 0;
  bool is_merged_or_merge_timeout = false;
  if (OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
  } else if (OB_FAIL(zone_mgr_->get_zone(all_zone))) {
    LOG_WARN("fail to get zone", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
    LOG_WARN("get_global_broadcast_version failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_zone.count(); ++i) {
    if (OB_FAIL(switch_leader_mgr_.is_zone_merged_or_timeout(all_zone.at(i).zone_, is_merged_or_merge_timeout))) {
      LOG_WARN("failed to check is zone merged or timeout", K(ret), K(all_zone.at(i).zone_));
    } else if (all_zone.at(i).last_merged_version_ == global_broadcast_version ||
               all_zone.at(i).is_merge_timeout_) {  // merge finished
    } else if (all_zone.at(i).suspend_merging_) {
      suspend_zone_count++;
    }
  }
  return ret;
}

int ObDailyMergeScheduler::calc_merge_info(int64_t& in_merging_count, int64_t& merged_count)
{
  int ret = OB_SUCCESS;
  ObZoneInfoArray all_zone;
  in_merging_count = 0;
  merged_count = 0;
  int64_t global_broadcast_version = 0;
  if (OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
  } else if (OB_FAIL(zone_mgr_->get_zone(all_zone))) {
    LOG_WARN("fail to get zone", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
    LOG_WARN("get_global_broadcast_version failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_zone.count(); ++i) {
    if (all_zone.at(i).last_merged_version_ == global_broadcast_version ||
        all_zone.at(i).is_merge_timeout_) {  // merge finished
      merged_count++;
    } else if (all_zone.at(i).broadcast_version_ == global_broadcast_version) {  // in merge
      if (all_zone.at(i).suspend_merging_) {
      } else {
        in_merging_count++;
      }
    } else {  // merge not started
      // nothing to do
    }
  }
  return ret;
}

int ObDailyMergeScheduler::next_merge_zones(ObZoneArray& to_merge)
{
  int ret = OB_SUCCESS;
  ObZoneArray merge_list;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_merge_list(merge_list))) {
    LOG_ERROR("fail to get merge list, schedule daily merge by conflict pair", K(ret));
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(generate_next_zones_by_conflict_pair(to_merge))) {
        LOG_WARN("fail to generate next zone", K(ret));
      }
    }
  } else if (merge_list.count() > 0) {
    if (OB_FAIL(generate_next_zones_by_merge_list(merge_list, to_merge))) {
      LOG_WARN("fail to generate merge zones", K(ret));
    } else {
      LOG_WARN("get merge list", K(ret), K(merge_list));
    }
  } else {
    if (OB_FAIL(generate_next_zones_by_conflict_pair(to_merge))) {
      LOG_WARN("fail to generate next zone", K(ret));
    }
  }
  return ret;
}

int ObDailyMergeScheduler::check_merge_list(ObZoneArray& merge_list)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(zone_mgr_->check_merge_order(merge_list))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ObZoneArray zone_array;
      if (OB_FAIL(zone_mgr_->get_zone(zone_array))) {
        LOG_WARN("fail to get zone", K(ret));
      } else {
        for (int i = 0; i < zone_array.count() && OB_SUCC(ret); i++) {
          if (!has_exist_in_array(merge_list, zone_array.at(i))) {
            LOG_ERROR("zone not exist in zone_merge_order", "zone", zone_array.at(i));
            if (OB_FAIL(merge_list.push_back(zone_array.at(i)))) {
              LOG_WARN("fail to push back", K(ret), "zone", zone_array.at(i));
            } else {
              LOG_INFO("add zone to merge list", "zone", zone_array.at(i));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::generate_next_zones_by_merge_list(const ObZoneArray& merge_order_list, ObZoneArray& to_merge)
{
  int ret = OB_SUCCESS;
  int in_merge_zone_count = 0;
  to_merge.reuse();
  ObZoneArray merge_list;
  if (OB_FAIL(merge_list.assign(merge_order_list))) {
    LOG_WARN("fail to assign merge list", K(ret));
  } else if (OB_FAIL(check_merge_list(merge_list))) {
    LOG_WARN("fail to check merge list", K(ret), K(merge_list));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_list.count(); ++i) {
    HEAP_VAR(ObZoneInfo, info)
    {
      info.zone_ = merge_list.at(i);
      if (OB_FAIL(zone_mgr_->get_zone(info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("find zone failed", K(ret), "zone", merge_list.at(i));
        } else {
          ret = OB_SUCCESS;
          LOG_INFO("ignore not exist zones", "zone", info.zone_);
        }
      } else {
        int64_t global_broadcast_version = 0;
        bool is_merged_or_merge_timeout = false;
        if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
          LOG_WARN("get_global_broadcast_version failed", K(ret));
        } else if (OB_FAIL(switch_leader_mgr_.is_zone_merged_or_timeout(info.zone_, is_merged_or_merge_timeout))) {
          LOG_WARN("failed to check is zone merged or timeout", K(ret), K(info));
        } else if (info.last_merged_version_ == global_broadcast_version || info.is_merge_timeout_) {  // merge finished
          // nothing todo
        } else if (info.broadcast_version_ == global_broadcast_version) {  // in merge
          if (is_merged_or_merge_timeout) {
            // this zone is merged or timeout, as a pending op of switch_leader_mgr_
          } else {
            in_merge_zone_count++;
          }
        } else {  // merge not started
          if (OB_FAIL(to_merge.push_back(merge_list.at(i)))) {
            LOG_WARN("push zone to array failed", K(ret));
          }
        }
        LOG_WARN("check zone", K(info), K(merge_list), K(in_merge_zone_count));
      }
    }

    if (OB_SUCC(ret)) {
      if (GCONF.enable_merge_by_turn && to_merge.count() + in_merge_zone_count >= config_->zone_merge_concurrency) {
        break;
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::generate_next_zones_by_conflict_pair(ObZoneArray& merge_list)
{
  int ret = OB_SUCCESS;
  merge_list.reset();
  bool enable_merge_by_turn = GCONF.enable_merge_by_turn;
  int64_t merge_concurrency = config_->zone_merge_concurrency;
  if (OB_FAIL(execute_order_generator_.get_next_zone(enable_merge_by_turn, merge_concurrency, merge_list))) {
    LOG_WARN("fail to get next zone", K(ret));
  }
  if (merge_list.count() <= 0 && !stop_ && OB_SUCC(ret)) {
    int tmp_ret = double_check(merge_list);
    if (merge_list.count() > 0 && OB_SUCCESS == tmp_ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObDailyMergeScheduler::double_check(ObZoneArray& to_merge_zone)
{
  int ret = OB_SUCCESS;
  int64_t zone_count = 0;
  int64_t in_merging_count = 0;
  int64_t merged_count = 0;
  int64_t suspend_count = 0;
  if (OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
  } else if (to_merge_zone.count() > 0) {
    // nothing to do
  } else {
    if (OB_FAIL(zone_mgr_->get_zone_count(zone_count))) {
      LOG_WARN("fail to get zone count", K(ret));
    } else if (OB_FAIL(calc_merge_info(in_merging_count, merged_count))) {
      LOG_WARN("fail to get in merging zone cuont", K(ret));
    } else if (OB_FAIL(calc_suspend_zone_count(suspend_count))) {
      LOG_WARN("fail to get suspend zone count", K(ret));
    } else if (0 == in_merging_count && merged_count + suspend_count < zone_count) {
      LOG_ERROR("daily merge scheduler error", K(zone_count), K(merged_count), K(in_merging_count), K(suspend_count));
      if (OB_FAIL(force_schedule_zone(to_merge_zone))) {
        LOG_WARN("fail to start one zone", K(ret));
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::force_schedule_zone(ObZoneArray& to_merge)
{
  int ret = OB_SUCCESS;
  ObZoneInfoArray zone_info;
  int64_t global_broadcast_version = 0;
  if (OB_ISNULL(zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid zone mgr", K(ret), K(zone_mgr_));
  } else if (0 != to_merge.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(to_merge.count()));
  } else if (OB_FAIL(zone_mgr_->get_zone(zone_info))) {
    LOG_WARN("fail to get zone info", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
    LOG_WARN("get_global_broadcast_version failed", K(ret));
  } else {
    for (int64_t i = 0; i < zone_info.count() && OB_SUCC(ret); i++) {
      if (zone_info.at(i).broadcast_version_ < global_broadcast_version) {
        if (OB_FAIL(to_merge.push_back(zone_info.at(i).zone_))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          LOG_INFO("start one zone merge", "zone", zone_info.at(i).zone_);
          break;
        }
      } else {
      }
    }  // end for
  }
  return ret;
}

int ObDailyMergeScheduler::limit_merge_concurrency(ObZoneArray& ready_zone, ObZoneArray& to_merge)
{
  int ret = OB_SUCCESS;
  to_merge.reset();
  std::sort(ready_zone.begin(), ready_zone.end());
  int64_t global_broadcast_version = 0;
  int64_t in_merge_zone_count = 0;
  int64_t merged_count = 0;
  HEAP_VAR(ObZoneInfo, info)
  {
    if (OB_ISNULL(zone_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
    } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(global_broadcast_version))) {
      LOG_WARN("get_global_broadcast_version failed", K(ret));
    } else if (OB_FAIL(calc_merge_info(in_merge_zone_count, merged_count))) {
      LOG_WARN("fail to get in merging zone count", K(ret));
    } else {
      for (int64_t i = 0; i < ready_zone.count() && OB_SUCC(ret); i++) {
        if (GCONF.enable_merge_by_turn && to_merge.count() + in_merge_zone_count >= config_->zone_merge_concurrency) {
          break;
        }
        info.zone_ = ready_zone.at(i);
        if (OB_FAIL(zone_mgr_->get_zone(info))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("find zone failed", K(ret), "zone", info.zone_);
          } else {
            ret = OB_SUCCESS;
            LOG_INFO("ignore not exist zones");
          }
        } else if (info.broadcast_version_ == global_broadcast_version) {
          // nothing to do
          LOG_WARN("get zone alreay start merge", K(info), K(global_broadcast_version));
        } else if (info.suspend_merging_) {
          // nothing to do
        } else if (OB_FAIL(to_merge.push_back(ready_zone.at(i)))) {
          LOG_WARN("fail to push back", K(ret), K(i), "zone", ready_zone.at(i));
        }
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::arrange_execute_order(ObZoneArray& order)
{
  int ret = OB_SUCCESS;
  ObZoneArray tmp_order;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_zone(tmp_order))) {
    LOG_INFO("get all zones failed", K(ret));
  }

  // get merge list and check if need rearranging.
  bool need_rearranging = false;
  if (OB_SUCC(ret)) {
    std::sort(tmp_order.begin(), tmp_order.end());
    if (OB_FAIL(get_merge_list(order))) {
      LOG_WARN("get merge list failed", K(ret));
    } else {
      if (tmp_order.count() != order.count()) {
        need_rearranging = true;
      }
      FOREACH_CNT_X(zone, order, !need_rearranging)
      {
        bool found = false;
        FOREACH_CNT_X(tzone, tmp_order, !found)
        {
          if (*tzone == *zone) {
            found = true;
          }
        }
        if (!found) {
          need_rearranging = true;
        }
      }
    }
  }

  if (OB_SUCCESS == ret && need_rearranging) {
    order.reuse();
    int64_t idx = 0;
    for (; idx < tmp_order.count(); ++idx) {
      if (self_ == tmp_order.at(idx)) {
        break;
      }
    }
    if (idx == tmp_order.count()) {
      LOG_WARN("self not found in all zones", K_(self));
      idx = tmp_order.count() - 1;
    }
    for (int i = 0; OB_SUCC(ret) && i < tmp_order.count(); ++i) {
      if (OB_FAIL(order.push_back(tmp_order.at((i + idx + 1) % tmp_order.count())))) {
        LOG_WARN("push zone to array failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("new merge order generated", K(order));
      ROOTSERVICE_EVENT_ADD("daily_merge", "arrange merge order", K(order));
    }
  }
  return ret;
}

int ObDailyMergeScheduler::update_merge_status()
{
  int ret = OB_SUCCESS;
  ObPartitionTableUtil::ObZoneMergeProgress all_progress;
  bool all_majority_merged = false;
  int64_t version = 0;
  int64_t last_merged_version = 0;
  int64_t all_merged_version = 0;
  bool merged = false;
  static const int64_t FULL_PERCENTAGE = 100;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(version))) {
    LOG_WARN("get_global_broadcast_version failed", K(ret));
  } else if (OB_FAIL(pt_util_->check_merge_progress(stop_, version, all_progress, all_majority_merged))) {
    LOG_WARN("check merge status failed", K(ret), K(version));
    int64_t time_interval = 10L * 60 * 1000 * 1000;  // print every 10 minutes
    if (TC_REACH_TIME_INTERVAL(time_interval)) {
      ROOTSERVICE_EVENT_ADD("daily_merge",
          "merge_process",
          "check merge progress fail",
          ret,
          "merge version",
          version,
          "return code",
          ret,
          "rootserver",
          GCONF.self_addr_);
    }
  } else {
    HEAP_VAR(ObZoneInfo, info)
    {
      FOREACH_X(progress, all_progress, OB_SUCCESS == ret)
      {
        const ObZone& zone = progress->zone_;
        merged = false;
        info.reset();
        info.zone_ = progress->zone_;
        if (OB_FAIL(zone_mgr_->get_zone(info))) {
          LOG_WARN("find zone failed", K(ret), K(zone));
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          }
        } else {
          int64_t merger_completion_percentage = config_->merger_completion_percentage;
          if (merger_completion_percentage >= FULL_PERCENTAGE) {
            merged = (0 == progress->unmerged_partition_cnt_);
          } else {
            if (progress->get_merged_partition_percentage() >= merger_completion_percentage &&
                progress->get_merged_data_percentage() >= merger_completion_percentage) {
              merged = true;
              LOG_INFO("check merge finished by percentage",
                  "zone",
                  progress->zone_,
                  "threadhold",
                  merger_completion_percentage,
                  "finished_cnt_percentage",
                  progress->get_merged_partition_percentage(),
                  "finished_size_percentage",
                  progress->get_merged_data_percentage(),
                  "progress",
                  *progress);
            }
          }
          last_merged_version = merged ? info.broadcast_version_ : info.last_merged_version_;
          all_merged_version =
              progress->smallest_data_version_ <= 0 ? info.broadcast_version_ : progress->smallest_data_version_;
          all_merged_version = std::min(all_merged_version, last_merged_version);
          all_merged_version = std::max(all_merged_version, static_cast<int64_t>(info.all_merged_version_));

          if (info.last_merged_version_ != last_merged_version) {
            if (OB_FAIL(switch_leader_mgr_.add_finish_zone_merge_op(
                    zone, last_merged_version, all_merged_version, leader_info_array_))) {
              if (OB_EAGAIN != ret) {
                LOG_WARN("failed to add finish zone merge op", K(ret), K(zone));
              } else {
                ret = OB_SUCCESS;
              }
            } else {
              root_balancer_->wakeup();
            }
          } else {
            // When info.last_merged_version_ == last_merged_version, is_merging flag will not
            // changed. So need not finish_zone_merge
            if (info.all_merged_version_ != all_merged_version) {
              LOG_INFO("zone all partition finished",
                  K(zone),
                  K(all_merged_version),
                  "zone_merged_version",
                  info.all_merged_version_.value_);
              if (OB_FAIL(zone_mgr_->finish_zone_merge(progress->zone_, last_merged_version, all_merged_version))) {
                LOG_WARN("finish zone merge failed",
                    K(ret),
                    K(version),
                    K(last_merged_version),
                    K(all_merged_version),
                    "progress",
                    *progress,
                    "zone_info",
                    info);
              } else {
                ROOTSERVICE_EVENT_ADD(
                    "daily_merge", "all_partition_merged", "merge_version", all_merged_version, K(zone));
              }
            }
          }
        }
      }  // end for
    }

    if (OB_SUCC(ret)) {
      if (all_majority_merged) {
        DEBUG_SYNC(BEFORE_MERGE_FINISH);
        // check checksum before update global merged version, otherwise we might miss checksum error
        int tmp_ret = checksum_checker_->check();
        if (OB_EAGAIN != tmp_ret) {
          if (OB_FAIL(try_update_global_merged_version())) {
            LOG_WARN("update global merged version failed", K(ret));
          }
        }
      }
    }
  }
  check_merge_timeout();
  return ret;
}

void ObDailyMergeScheduler::check_merge_timeout()
{
  int ret = OB_SUCCESS;
  ObZoneArray zones;
  int64_t version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_zone(zones))) {
    LOG_INFO("get all zones failed", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_global_broadcast_version(version))) {
    LOG_WARN("get_global_broadcast_version failed", K(ret));
  } else {
    HEAP_VAR(ObZoneInfo, info)
    {
      int64_t now = ObTimeUtility::current_time();
      int64_t max_last_merge_merged_time = 0;
      int64_t max_start_merge_time = 0;
      FOREACH(zone, zones)
      {
        info.zone_ = *zone;
        // check merge timeout, zone info may changed, fetch again.
        if (OB_FAIL(zone_mgr_->get_zone(info))) {
          LOG_WARN("get zone failed", K(ret), K(*zone));
        } else {
          if (max_last_merge_merged_time < info.last_merged_time_) {
            max_last_merge_merged_time = info.last_merged_time_;
          }
          if (max_start_merge_time < info.merge_start_time_) {
            max_start_merge_time = info.merge_start_time_;
          }
          if (info.broadcast_version_ == version && info.last_merged_version_ != info.broadcast_version_ &&
              now - info.merge_start_time_ > config_->zone_merge_timeout && !info.is_merge_timeout_) {
            LOG_INFO("set zone merge timeout", K(*zone));
            if (OB_FAIL(switch_leader_mgr_.add_set_zone_merge_time_out_op(info.zone_, version, leader_info_array_))) {
              if (OB_EAGAIN != ret) {
                LOG_WARN("failed to add set zone merge time out op", K(ret), K(*zone));
              } else {
                ret = OB_SUCCESS;
              }
            }
            ROOTSERVICE_EVENT_ADD("daily_merge",
                "merge_process",
                "merge timeout",
                *zone,
                "start time",
                info.merge_start_time_,
                "current time",
                now,
                "result",
                ret);
          }
        }
      }                                                                // end FOREACH(zone, zones)
      const int64_t MAX_NO_MERGE_INTERVAL = 36 * 3600 * 1000 * 1000L;  // 36 hours
      if (now - max_last_merge_merged_time > MAX_NO_MERGE_INTERVAL &&
          now - max_start_merge_time > MAX_NO_MERGE_INTERVAL) {
        LOG_ERROR("long time no daily merge, please check it", K(ret));
      }
    }
  }
}

int ObDailyMergeScheduler::try_update_global_merged_version()
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObGlobalInfo, global)
  {
    ObZoneInfoArray infos;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret));
    } else if (OB_FAIL(zone_mgr_->get_snapshot(global, infos))) {
      LOG_WARN("get zone status snapshot failed", K(ret));
    } else {
      if (global.last_merged_version_ != global.global_broadcast_version_) {
        bool merged = true;
        FOREACH_X(info, infos, merged)
        {
          if (info->last_merged_version_ != global.global_broadcast_version_) {
            merged = false;
          }
        }
        if (merged) {
          if (OB_FAIL(zone_mgr_->try_update_global_last_merged_version())) {
            LOG_WARN("try update global last merged version failed", K(ret));
          } else {
            ROOTSERVICE_EVENT_ADD("daily_merge", "global_merged", "version", global.global_broadcast_version_.value_);
          }
        }
      }
    }
  }
  return ret;
}

int ObDailyMergeScheduler::idle()
{
  int ret = OB_SUCCESS;
  int64_t warm_up_start_time = 0;
  const int64_t warm_up_duration_time = merger_warm_up_duration_time_;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_warm_up_start_time(warm_up_start_time))) {
    LOG_WARN("failed to get warm up start time", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    if (warm_up_duration_time + warm_up_start_time <= now) {
      if (switch_leader_check_interval_ > 0 && switch_leader_check_interval_ < GCONF.merger_check_interval) {
        LOG_INFO("switch leader failed, sleep shortly", K(switch_leader_check_interval_));
        if (OB_FAIL(idling_.idle(switch_leader_check_interval_))) {
          LOG_WARN("idle failed", K(ret));
        }
      } else if (OB_FAIL(idling_.idle(DEFAULT_IDLE_DURATION))) {
        LOG_WARN("idle failed", K(ret));
      }
    } else {
      const int64_t idle_time = warm_up_start_time + warm_up_duration_time - now;
      LOG_INFO("is doing warm up, only sleep warm up left time",
          K(idle_time),
          K(warm_up_start_time),
          K(warm_up_duration_time));
      if (OB_FAIL(idling_.idle(idle_time))) {
        LOG_WARN("idle failed", K(ret));
      }
    }
  }
  switch_leader_check_interval_ = 0;
  DEBUG_SYNC(DAILY_MERGE_SCHEDULER_IDLE);
  return ret;
}

void ObDailyMergeScheduler::wakeup()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    idling_.wakeup();
  }
}

int ObDailyMergeScheduler::reset_merger_warm_up_duration_time(const int64_t merger_warm_up_duration_time)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (merger_warm_up_duration_time != merger_warm_up_duration_time_) {
    // forbid
    //    LOG_INFO("reset_merger_warm_up_duration_time",
    //             "old", merger_warm_up_duration_time_,
    //             "new", merger_warm_up_duration_time);
    //    merger_warm_up_duration_time_ = merger_warm_up_duration_time;
    merger_warm_up_duration_time_ = 0;
    switch_leader_mgr_.set_merger_warm_up_duration_time(merger_warm_up_duration_time);
    if (!stop_) {
      idling_.wakeup();
    }
  }
  return ret;
}

void ObDailyMergeScheduler::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
}

// int ObDailyMergeScheduler::backup_leader_pos()
//{
//  const bool backup_flag = true;
//  int ret = OB_SUCCESS;
//  const int64_t start_time = ObTimeUtility::current_time();
//
//  LOG_INFO("start set_leader_backup_flag", K(backup_flag));
//  if (!inited_) {
//    ret = OB_NOT_INIT;
//    LOG_WARN("not init", K(ret));
//  } else if (OB_FAIL(pt_util_->set_leader_backup_flag(stop_, backup_flag, &leader_info_array_))) {
//    LOG_WARN("backup leader position failed", K(ret));
//  } else {
//    leader_pos_backuped_ = true;
//  }
//  const int64_t cost_time = ObTimeUtility::current_time() - start_time;
//  LOG_INFO("end set_leader_backup_flag", K(ret), K(backup_flag), K(cost_time), K(leader_info_array_));
//  return ret;
//}
//
int ObDailyMergeScheduler::get_merge_list(ObZoneArray& zone_array)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObString merge_order_str = config_->zone_merge_order.str();
    ObString zone_str;
    ObString zone;
    bool split_end = false;
    while (!split_end && OB_SUCCESS == ret) {
      zone_str = merge_order_str.split_on(',');
      if (zone_str.empty() && NULL == zone_str.ptr()) {
        split_end = true;
        zone_str = merge_order_str;
      }
      zone = zone_str.trim();
      if (!zone.empty()) {
        if (OB_FAIL(zone_array.push_back(zone))) {
          LOG_WARN("push back to array failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(zone_mgr_->check_merge_order(zone_array))) {
        LOG_WARN("fail to check merge order", K(ret), K(zone_array));
      }
    }
  }
  return ret;
}

// check zone info. need start smooth coordinate when zone is merging.
int ObDailyMergeScheduler::try_start_smooth_coordinate()
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObGlobalInfo, global)
  {
    ObZoneInfoArray all_zone;
    if (OB_ISNULL(zone_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid zone mgr", K(ret), K(zone_mgr_));
    } else if (OB_FAIL(zone_mgr_->get_snapshot(global, all_zone))) {
      LOG_WARN("get zone status snapshot failed", K(ret));
    }
    bool need_start = false;
    for (int64_t i = 0; i < all_zone.count() && OB_SUCC(ret); i++) {
      const ObZoneInfo& info = all_zone.at(i);
      if (info.is_merging_ && info.broadcast_version_ < global.global_broadcast_version_) {
        need_start = true;
        break;
      }
    }
    if (OB_SUCC(ret) && need_start) {
      if (OB_FAIL(switch_leader_mgr_.start_smooth_coordinate())) {
        LOG_WARN("fail to start smooth coordinate", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObDailyMergeScheduler::get_schedule_interval() const
{
  return idling_.get_idle_interval_us();
}

//////////////////
ObSwitchLeaderMgr::ObSwitchLeaderMgr()
    : is_inited_(false),
      leader_coordinator_(NULL),
      zone_mgr_(NULL),
      config_(NULL),
      op_array_(),
      allocator_(),
      merger_warm_up_duration_time_(0)
{}

ObSwitchLeaderMgr::~ObSwitchLeaderMgr()
{
  reset();
}

void ObSwitchLeaderMgr::reset()
{
  is_inited_ = false;
  leader_coordinator_ = NULL;
  zone_mgr_ = NULL;
  config_ = NULL;
  reset_op_array();
}

void ObSwitchLeaderMgr::reset_op_array()
{
  for (int64_t i = 0; i < op_array_.count(); ++i) {
    op_array_[i]->~ObIZoneOp();
  }
  op_array_.reset();
  allocator_.reset();
}

int ObSwitchLeaderMgr::init(
    ObILeaderCoordinator& leader_coordinator, ObZoneManager& zone_mgr, common::ObServerConfig& config)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    leader_coordinator_ = &leader_coordinator;
    zone_mgr_ = &zone_mgr;
    config_ = &config;
    is_inited_ = true;
  }
  return ret;
}

int ObSwitchLeaderMgr::start_smooth_coordinate()
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(leader_coordinator_->get_lock(), ObLatchIds::LEADER_COORDINATOR_LOCK);
  if (OB_FAIL(leader_coordinator_->start_smooth_coordinate())) {
    LOG_WARN("failed to start smooth coordinate", K(ret));
  }
  return ret;
}

int ObSwitchLeaderMgr::do_work()
{
  int ret = OB_SUCCESS;
  bool is_doing_leader_coordinate = false;
  bool is_doing_warm_up = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (op_array_.count() > 0) {
    if (OB_FAIL(check_doing_warm_up(is_doing_warm_up))) {
      LOG_WARN("failed to check is doing warm up", K(ret));
    } else if (is_doing_warm_up) {
      LOG_INFO("is doing warm up, skip do work");
    } else if (OB_FAIL(leader_coordinator_->is_doing_smooth_coordinate(is_doing_leader_coordinate))) {
      LOG_WARN("failed to check is doing smooth coordinate", K(ret));
    } else if (is_doing_leader_coordinate) {
      LOG_INFO("is doing leader coordinate, skip do work");
    } else {
      bool need_coordinate = false;
      ObLatchWGuard guard(leader_coordinator_->get_lock(), ObLatchIds::LEADER_COORDINATOR_LOCK);
      for (int64_t i = 0; OB_SUCC(ret) && i < op_array_.count(); ++i) {
        if (NULL == op_array_[i]) {
          ret = OB_ERR_SYS;
          LOG_ERROR("op_array_[i] must not null", K(ret), K(i));
        } else if (!op_array_[i]->is_done()) {
          if (OB_FAIL(op_array_[i]->do_op())) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              LOG_INFO("fail to do op, zone not exist, ignore it", K(ret), K(i));
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to do op", K(ret), K(i));
            }
          } else if (need_smooth_coordinate(*op_array_[i])) {
            need_coordinate = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (need_coordinate && OB_FAIL(leader_coordinator_->start_smooth_coordinate())) {
          LOG_WARN("failed to start smooth coordinate", K(ret));
        }
        reset_op_array();
      }
    }
  }

  return ret;
}

// check need smooth coordinate
// 1. when enable merger warm up, return true
// 2. when zone is merging, return true
bool ObSwitchLeaderMgr::need_smooth_coordinate(const ObIZoneOp& zone_op)
{
  bool bret = false;
  if (zone_op.get_type() == ObIZoneOp::SetZoneMergingType) {
    bret = true;
  } else if (merger_warm_up_duration_time_ > 0) {
    bret = true;
  }
  return bret;
}

int ObSwitchLeaderMgr::check_all_op_finish(bool& is_finish)
{
  int ret = OB_SUCCESS;
  bool is_doing_leader_coordinate = false;
  // const int64_t now = ObTimeUtility::current_time();
  // int64_t warm_up_start_time = 0;
  is_finish = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
    //} else if (OB_FAIL(zone_mgr_->get_warm_up_start_time(warm_up_start_time))) {
    //  LOG_WARN("failed to get warm up start time", K(ret));
    //} else if (warm_up_start_time <= now && warm_up_start_time + merger_warm_up_duration_time_ >= now) {
    //  is_finish = false;
    //  LOG_INFO("is doing warm up, not finish");
  } else if (op_array_.count() > 0) {
    is_finish = false;
    LOG_INFO("op is not empty, not finish", K(op_array_));
  } else if (OB_FAIL(leader_coordinator_->is_doing_smooth_coordinate(is_doing_leader_coordinate))) {
    LOG_WARN("failed to check is doing smooth coordinate", K(ret));
  } else if (is_doing_leader_coordinate) {
    is_finish = false;
    LOG_INFO("is doing leader coordinate, not finish");
  }

  if (OB_FAIL(ret)) {
    is_finish = false;
  }

  // if (OB_SUCC(ret) && is_finish) {
  //  if (0 != warm_up_start_time) {
  //    LOG_INFO("clear warm up");
  //    if (OB_FAIL(zone_mgr_->set_warm_up_start_time(0 /* 0 means stop warm up */))) {
  //      LOG_WARN("failed to clear warm up start time", K(ret));
  //    }
  //  }
  //}
  return ret;
}

int ObSwitchLeaderMgr::can_add_new_op(const common::ObZone& zone, bool& can)
{
  int ret = OB_SUCCESS;
  bool is_doing_leader_coordinate = false;
  can = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(leader_coordinator_->is_doing_smooth_coordinate(is_doing_leader_coordinate))) {
    LOG_WARN("failed to check is doing smooth coordinate", K(ret));
  } else if (is_doing_leader_coordinate) {
    can = false;
    LOG_INFO("is doing leader coordinate, cannot add new op");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < op_array_.count() && can; ++i) {
      if (NULL == op_array_[i]) {
        ret = OB_ERR_SYS;
        LOG_ERROR("op_array_[i] must not null", K(ret), K(i));
      } else if (op_array_[i]->get_zone() == zone) {
        can = false;
      }
    }
  }

  if (OB_FAIL(ret)) {
    can = false;
  }
  return ret;
}

int ObSwitchLeaderMgr::check_doing_warm_up(bool& is_doing)
{
  int ret = OB_SUCCESS;
  int64_t warm_up_start_time = 0;
  const int64_t now = ObTimeUtility::current_time();
  is_doing = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_warm_up_start_time(warm_up_start_time))) {
    LOG_WARN("failed to get warm up start time", K(ret));
  } else if (warm_up_start_time <= now && warm_up_start_time + merger_warm_up_duration_time_ >= now) {
    is_doing = true;
  }
  return ret;
}

void ObSwitchLeaderMgr::set_merge_status(bool is_in_merging)
{
  if (OB_ISNULL(leader_coordinator_)) {
    LOG_WARN("get invalid leader_coordinator");
  } else {
    leader_coordinator_->set_merge_status(is_in_merging);
  }
}

// int ObSwitchLeaderMgr::is_last_switch_turn_succ(bool &is_succ)
//{
//  int ret = OB_SUCCESS;
//
//  if (!is_inited_) {
//    ret = OB_NOT_INIT;
//    LOG_WARN("not inited", K(ret));
//  } else if (OB_FAIL(leader_coordinator_->is_last_switch_turn_succ(is_succ))) {
//    LOG_WARN("failed to get is last switch leader succ", K(ret));
//  }
//  return ret;
//}

int ObSwitchLeaderMgr::start_switch_leader_without_warm_up()
{
  int ret = OB_SUCCESS;
  bool is_finish = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_all_op_finish(is_finish))) {
    LOG_WARN("failed to check is all op finish", K(ret));
  } else if (!is_finish) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cannot start switch leader without warm up when not all op finish", K(ret));
  } else if (OB_FAIL(leader_coordinator_->start_smooth_coordinate())) {
    LOG_WARN("failed to start smooth coordinate", K(ret));
  }
  return ret;
}

bool ObSwitchLeaderMgr::need_warm_up()
{
  bool bret = false;
  int ret = OB_SUCCESS;
  bool need_warm_up = true;
  int64_t active_zone_count = 0;
  int64_t zone_merge_concurrency = 0;
  int64_t zone_count = 0;

  if (!GCONF.enable_auto_leader_switch || GCONF.enable_manual_merge || !GCONF.enable_merge_by_turn ||
      0 == merger_warm_up_duration_time_) {
    need_warm_up = false;
  } else {
    HEAP_VAR(ObGlobalInfo, global)
    {
      ObDailyMergeScheduler::ObZoneInfoArray infos;
      if (OB_FAIL(zone_mgr_->get_snapshot(global, infos))) {
        LOG_WARN("get zone status snapshot failed", K(ret));
      } else {
        zone_merge_concurrency = config_->zone_merge_concurrency;
        zone_count = infos.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
          if (ObZoneStatus::ACTIVE == infos.at(i).status_) {
            ++active_zone_count;
          }
        }

        if (OB_SUCC(ret)) {
          if (zone_merge_concurrency >= zone_count || 1 == active_zone_count) {
            need_warm_up = false;
            LOG_INFO("no need to warm up before do operation on this zone",
                K(active_zone_count),
                K(zone_count),
                K(zone_merge_concurrency));
          }
        }
      }
    }
  }
  bret = need_warm_up;
  return bret;
}

int ObSwitchLeaderMgr::start_warm_up(
    const ObPartitionTableUtil::ObLeaderInfoArray& leader_info_array, const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  bool zone_need_warm_up = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!need_warm_up()) {
    // nothing todo
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < leader_info_array.count() && !zone_need_warm_up; ++i) {
      if (leader_info_array.at(i).zone_ == zone && 0 != leader_info_array.at(i).leader_count_) {
        zone_need_warm_up = true;
        LOG_INFO("zone has leader, need warm up", K(zone), "leader count", leader_info_array.at(i).leader_count_);
      }
    }
    if (!zone_need_warm_up) {
      LOG_INFO("no leader on this zone before schedule daily merge, no need warm up", K(zone));
    }
  }

  if (OB_SUCC(ret) && zone_need_warm_up) {
    if (OB_FAIL(zone_mgr_->set_warm_up_start_time(now))) {
      LOG_WARN("failed to start warm up", K(ret));
    } else {
      HEAP_VAR(ObGlobalInfo, global)
      {
        ObDailyMergeScheduler::ObZoneInfoArray infos;
        if (OB_FAIL(zone_mgr_->get_snapshot(global, infos))) {
          LOG_WARN("get zone status snapshot failed", K(ret));
        } else {
          int64_t global_broadcast_version = global.global_broadcast_version_;
          LOG_INFO("start warm up", K(ret), K(now), K(zone), K(global_broadcast_version));
          ROOTSERVICE_EVENT_ADD("daily_merge",
              "start_warm_up",
              "merge_version",
              global_broadcast_version,
              K(zone),
              "rootserver",
              GCONF.self_addr_);
        }
      }
    }
  }
  return ret;
}

int ObSwitchLeaderMgr::add_set_zone_merging_op(
    const common::ObZone& zone, const ObPartitionTableUtil::ObLeaderInfoArray& leader_info_array)
{
  int ret = OB_SUCCESS;
  void* tmp_buf = NULL;
  ObIZoneOp* op = NULL;
  bool can_add = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(can_add_new_op(zone, can_add))) {
    LOG_WARN("failed to check can add new op", K(ret));
  } else if (!can_add) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(start_warm_up(leader_info_array, zone))) {
    LOG_WARN("failed to start warm up", K(ret));
  } else if (NULL == (tmp_buf = allocator_.alloc(sizeof(ObSetZoneMergingOp)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("cannot alloc op", K(ret));
  } else {
    op = new (tmp_buf) ObSetZoneMergingOp(*zone_mgr_, zone);
    if (OB_FAIL(op_array_.push_back(op))) {
      LOG_WARN("failed to add op", K(ret));
      op->~ObIZoneOp();
      allocator_.free(tmp_buf);
      tmp_buf = NULL;
      op = NULL;
    } else {
      LOG_INFO("succeed to add_set_zone_merging_op", K(zone));
    }
  }
  return ret;
}

int ObSwitchLeaderMgr::add_finish_zone_merge_op(const common::ObZone& zone, const int64_t last_merged_version,
    const int64_t all_merged_version, const ObPartitionTableUtil::ObLeaderInfoArray& leader_info_array)
{
  int ret = OB_SUCCESS;
  void* tmp_buf = NULL;
  ObIZoneOp* op = NULL;
  bool can_add = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(can_add_new_op(zone, can_add))) {
    LOG_WARN("failed to check can add new op", K(ret));
  } else if (!can_add) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(start_warm_up(leader_info_array, zone))) {
    LOG_WARN("failed to start warm up", K(ret));
  } else if (NULL == (tmp_buf = allocator_.alloc(sizeof(ObSetFinishZoneMergeOp)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("cannot alloc op", K(ret));
  } else {
    op = new (tmp_buf) ObSetFinishZoneMergeOp(*zone_mgr_, zone, last_merged_version, all_merged_version);
    if (OB_FAIL(op_array_.push_back(op))) {
      LOG_WARN("failed to add op", K(ret));
      op->~ObIZoneOp();
      allocator_.free(tmp_buf);
      tmp_buf = NULL;
      op = NULL;
    } else {
      LOG_INFO("succeed to add_finish_zone_merge_op", K(zone), K(last_merged_version), K(all_merged_version));
    }
  }
  return ret;
}

int ObSwitchLeaderMgr::add_set_zone_merge_time_out_op(
    const common::ObZone& zone, const int64_t version, const ObPartitionTableUtil::ObLeaderInfoArray& leader_info_array)
{
  int ret = OB_SUCCESS;
  void* tmp_buf = NULL;
  ObIZoneOp* op = NULL;
  bool can_add = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (zone.is_empty() || OB_INVALID_VERSION == version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(zone), K(version));
  } else if (OB_FAIL(can_add_new_op(zone, can_add))) {
    LOG_WARN("failed to check can add new op", K(ret));
  } else if (!can_add) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(start_warm_up(leader_info_array, zone))) {
    LOG_WARN("failed to start warm up", K(ret));
  } else if (NULL == (tmp_buf = allocator_.alloc(sizeof(ObSetZoneMergeTimeOutOp)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("cannot alloc op", K(ret));
  } else {
    op = new (tmp_buf) ObSetZoneMergeTimeOutOp(*zone_mgr_, zone, version);
    if (OB_FAIL(op_array_.push_back(op))) {
      LOG_WARN("failed to add op", K(ret));
      op->~ObIZoneOp();
      allocator_.free(tmp_buf);
      tmp_buf = NULL;
      op = NULL;
    } else {
      LOG_INFO("succeed to add_set_zone_merge_time_out_op", K(zone), K(version));
    }
  }
  return ret;
}

int ObSwitchLeaderMgr::is_zone_merged_or_timeout(const common::ObZone& zone, bool& result)
{
  int ret = OB_SUCCESS;
  result = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !result && i < op_array_.count(); ++i) {
      if (NULL == op_array_[i]) {
        ret = OB_ERR_SYS;
        LOG_ERROR("op_array_[i] must not null", K(ret), K(i));
      } else if (op_array_[i]->get_zone() == zone &&
                 (ObIZoneOp::SetFinishZoneMergeType == op_array_[i]->get_type() ||
                     ObIZoneOp::SetZoneMergeTimeOutType == op_array_[i]->get_type())) {
        result = true;
      }
    }
  }
  return ret;
}

int ObSwitchLeaderMgr::wake_up_leader_coordinator()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    leader_coordinator_->signal();
  }
  return ret;
}

void ObSwitchLeaderMgr::set_merger_warm_up_duration_time(const int64_t merger_warm_up_duration_time)
{
  merger_warm_up_duration_time_ = merger_warm_up_duration_time;
}

int ObSwitchLeaderMgr::check_switch_leader(const ObIArray<ObZone>& zone, ObIArray<bool>& is_succ)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(leader_coordinator_->check_daily_merge_switch_leader(zone, is_succ))) {
    LOG_WARN("fail to check switch leader", K(ret), K(zone));
  }
  return ret;
}

////////////////////////
ObSetZoneMergingOp::ObSetZoneMergingOp(ObZoneManager& zone_mgr, const common::ObZone& zone)
    : zone_mgr_(zone_mgr), zone_(zone), is_done_(false)
{}

int ObSetZoneMergingOp::do_op()
{
  int ret = OB_SUCCESS;

  if (is_done_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("op is done, cannot do twice");
  } else if (OB_FAIL(zone_mgr_.set_zone_merging(zone_))) {
    LOG_WARN("failed to set zone merging", K(ret), K_(zone));
  } else {
    is_done_ = true;
    LOG_INFO("succ to set zone merging", K_(zone));
  }

  return ret;
}

ObSetFinishZoneMergeOp::ObSetFinishZoneMergeOp(ObZoneManager& zone_mgr, const common::ObZone& zone,
    const int64_t last_merged_version, const int64_t all_merged_version)
    : zone_mgr_(zone_mgr),
      zone_(zone),
      last_merged_version_(last_merged_version),
      all_merged_version_(all_merged_version),
      is_done_(false)
{}

int ObSetFinishZoneMergeOp::do_op()
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, info)
  {
    info.zone_ = zone_;
    if (is_done_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("op is done, cannot do twice");
    } else if (OB_FAIL(zone_mgr_.get_zone(info))) {
      LOG_WARN("failed to get zone", K(ret), K(zone_));
    } else if (OB_FAIL(zone_mgr_.finish_zone_merge(zone_, last_merged_version_, all_merged_version_))) {
      LOG_WARN("failed to finish zone merge", K(ret), K_(zone), K_(last_merged_version), K_(all_merged_version));
    } else {
      is_done_ = true;
      LOG_INFO("succ to finish zone merge", K_(zone), K_(last_merged_version), K_(all_merged_version));
      if (info.last_merged_version_ != last_merged_version_) {
        ROOTSERVICE_EVENT_ADD("daily_merge", "merge_succeed", "merge_version", last_merged_version_, K_(zone));
      }
      if (info.all_merged_version_ != all_merged_version_) {
        ROOTSERVICE_EVENT_ADD("daily_merge", "all_partition_merged", "merge_version", all_merged_version_, K_(zone));
      }
    }
  }
  return ret;
}

ObSetZoneMergeTimeOutOp::ObSetZoneMergeTimeOutOp(
    ObZoneManager& zone_mgr, const common::ObZone& zone, const int64_t version)
    : zone_mgr_(zone_mgr), zone_(zone), is_done_(false), version_(version)
{}

int ObSetZoneMergeTimeOutOp::do_op()
{
  int ret = OB_SUCCESS;
  if (is_done_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("op is done, cannot do twice");
  } else if (OB_FAIL(zone_mgr_.set_zone_merge_timeout(zone_))) {
    LOG_WARN("failed to set zone merge timeout", K(ret), K_(zone));
  } else {
    is_done_ = true;
    LOG_INFO("succ to set zone merge timeout", K_(zone));
    ROOTSERVICE_EVENT_ADD("daily_merge", "merge_timeout", "merge_version", version_, K_(zone));
  }
  return ret;
}

/////////////////////////
int ObPartitionChecksumChecker::init(common::ObMySQLProxy* sql_proxy, common::ObISQLClient* remote_sql_proxy,
    ObZoneManager& zone_manager, ObFreezeInfoManager& freeze_info_manager, share::ObIMergeErrorCb* merge_error_callback)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(remote_sql_proxy) || OB_ISNULL(sql_proxy) || OB_ISNULL(merge_error_callback)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(remote_sql_proxy), K(sql_proxy), K(merge_error_callback));
  } else {
    sql_proxy_ = sql_proxy;
    remote_sql_proxy_ = remote_sql_proxy;
    zone_manager_ = &zone_manager;
    freeze_info_manager_ = &freeze_info_manager;
    merge_error_cb_ = merge_error_callback;
  }
  return ret;
}

int ObPartitionChecksumChecker::check()
{
  int ret = OB_SUCCESS;
  bool do_check = false;
  const int64_t MAX_CHECK_INTERVAL = GCONF.merger_check_interval;
  if (in_merging_ && ObTimeUtility::current_time() - last_check_time_ > MIN_CHECK_INTERVAL) {
    do_check = true;
  } else if (!in_merging_ && ObTimeUtility::current_time() - last_check_time_ > MAX_CHECK_INTERVAL) {
    do_check = true;
  }
  if (do_check) {
    int tmp_ret = OB_SUCCESS;
    check_partition_checksum();
    if (OB_SUCCESS != (tmp_ret = check_global_index_column_checksum())) {
      ret = tmp_ret;
      LOG_WARN("fail to check global index column checksum", K(tmp_ret));
    }
    last_check_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

void ObPartitionChecksumChecker::check_partition_checksum()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(local_data_checksum_iter_.init(sql_proxy_, merge_error_cb_))) {
    LOG_WARN("failed to init data_checksum_iter", K(ret));
  } else {
    ObSSTableDataChecksumInfo info;
    while (OB_SUCC(local_data_checksum_iter_.next(info))) {}
  }
}

int ObPartitionChecksumChecker::check_global_index_column_checksum()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  int64_t version = 0;
  share::ObSimpleFrozenStatus frozen_status;
  ObBackupInfoManager backup_info_mgr;
  if (OB_FAIL(zone_manager_->get_global_broadcast_version(version))) {
    LOG_WARN("get_global_broadcast_version failed", K(ret));
  } else if (OB_FAIL(freeze_info_manager_->get_freeze_info(version, frozen_status))) {
    LOG_WARN("fail to get freeze info", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
                 OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else if (OB_FAIL(backup_info_mgr.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("fail to init back up info mgr", K(ret));
  } else {
    ObArray<const ObSimpleTableSchemaV2*> table_schemas;
    int64_t backup_base_version = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      bool is_restore = false;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.check_tenant_is_restore(tenant_id, is_restore))) {
        LOG_WARN("fail to check tenant is restore", KR(ret), K(tenant_id));
      } else if (is_restore) {
        // skip check restoring tenant global index column checksum
        // do nothing
      } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
        LOG_WARN("fail to get table schemas in tenant", K(ret));
      } else if (OB_FAIL(backup_info_mgr.get_base_backup_version(tenant_id, *sql_proxy_, backup_base_version))) {
        LOG_WARN("fail to get base backup version", K(ret));
      } else if (backup_base_version > 0 && frozen_status.frozen_version_ < backup_base_version) {
        LOG_INFO("skip global index checking when backup tenant has not major merged",
            K(backup_base_version),
            K(frozen_status));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < table_schemas.count(); j++) {
          const ObSimpleTableSchemaV2* simple_schema = table_schemas.at(j);
          if (simple_schema->is_global_index_table() && simple_schema->can_read_index()) {
            const ObTableSchema* data_table_schema = nullptr;
            const ObTableSchema* index_table_schema = nullptr;
            if (OB_FAIL(schema_guard.get_table_schema(simple_schema->get_table_id(), index_table_schema))) {
              LOG_WARN("fail to get table schema", K(ret));
            } else if (nullptr == index_table_schema) {
              // index table is deleted do nothing
            } else if (OB_FAIL(schema_guard.get_table_schema(simple_schema->get_data_table_id(), data_table_schema))) {
              LOG_WARN("fail to get table schema", K(ret));
            } else if (nullptr == data_table_schema) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_WARN("fail to get data table schema", K(ret));
            } else if (data_table_schema->is_in_splitting()) {
              // skip column checksum checking when table is in splitting partition
            } else if (OB_FAIL(ObSSTableColumnChecksumOperator::check_column_checksum(
                           *data_table_schema, *index_table_schema, frozen_status.frozen_timestamp_, *sql_proxy_))) {
              if (OB_CHECKSUM_ERROR == ret) {
                LOG_ERROR("global index checksum error", K(ret), K(*data_table_schema), K(*index_table_schema));
                int tmp_ret = OB_SUCCESS;
                if (OB_SUCCESS != (tmp_ret = merge_error_cb_->submit_merge_error_task())) {
                  LOG_WARN("fail to submit merge error task", K(tmp_ret));
                }
              } else if (OB_EAGAIN != ret) {
                LOG_WARN("fail to check column checksum", K(ret));
              } else {
                if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
                  LOG_WARN("fail to check column checksum", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
