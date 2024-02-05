//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_tenant_tablet_scheduler_task_mgr.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "observer/report/ob_tablet_table_updater.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share;
namespace compaction
{
ObTenantTabletSchedulerTaskMgr::ObTenantTabletSchedulerTaskMgr()
  : merge_loop_tg_id_(-1),
    medium_loop_tg_id_(-1),
    sstable_gc_tg_id_(-1),
    compaction_refresh_tg_id_(-1),
    schedule_interval_(-1),
    merge_loop_task_(),
    medium_loop_task_(),
    sstable_gc_task_(),
    info_pool_resize_task_(),
    tablet_updater_refresh_task_(),
    medium_check_task_()
{}

ObTenantTabletSchedulerTaskMgr::~ObTenantTabletSchedulerTaskMgr()
{
  destroy();
}

void ObTenantTabletSchedulerTaskMgr::destroy()
{
  TG_DESTROY(merge_loop_tg_id_);
  TG_DESTROY(medium_loop_tg_id_);
  TG_DESTROY(sstable_gc_tg_id_);
  TG_DESTROY(compaction_refresh_tg_id_);
  merge_loop_tg_id_ = -1;
  medium_loop_tg_id_ = -1;
  sstable_gc_tg_id_ = -1;
  compaction_refresh_tg_id_ = -1;
  schedule_interval_ = -1;
}

void ObTenantTabletSchedulerTaskMgr::stop()
{
  TG_STOP(merge_loop_tg_id_);
  TG_STOP(medium_loop_tg_id_);
  TG_STOP(sstable_gc_tg_id_);
  TG_STOP(compaction_refresh_tg_id_);
}

void ObTenantTabletSchedulerTaskMgr::wait()
{
  TG_WAIT(merge_loop_tg_id_);
  TG_WAIT(medium_loop_tg_id_);
  TG_WAIT(sstable_gc_tg_id_);
  TG_WAIT(compaction_refresh_tg_id_);
}

void ObTenantTabletSchedulerTaskMgr::MergeLoopTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_SLOG_REPLAY_INVERVAL)) {
      LOG_WARN("slog replay hasn't finished, this task can't start", K(ret));
    }
  } else {
    if (OB_FAIL(MTL(ObTenantTabletScheduler *)->schedule_all_tablets_minor())) {
      LOG_WARN("Fail to merge all partition", K(ret));
    }
    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("MergeLoopTask", K(cost_ts));
  }
}

void ObTenantTabletSchedulerTaskMgr::MediumLoopTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_SLOG_REPLAY_INVERVAL)) {
      LOG_WARN("slog replay hasn't finished, this task can't start", K(ret));
    }
  } else {
    if (OB_FAIL(MTL(ObTenantTabletScheduler *)->schedule_all_tablets_medium())) {
      LOG_WARN("Fail to merge all partition", K(ret));
    }
    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("MediumLoopTask", K(cost_ts));
  }
}

void ObTenantTabletSchedulerTaskMgr::SSTableGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_SLOG_REPLAY_INVERVAL)) {
      LOG_WARN("slog replay hasn't finished, this task can't start", K(ret));
    }
  } else {
    // use tenant config to loop minor && medium task
    MTL(ObTenantTabletScheduler *)->reload_tenant_config();

    int64_t cost_ts = ObTimeUtility::fast_current_time();
    ObCurTraceId::init(GCONF.self_addr_);
    if (OB_FAIL(MTL(ObTenantTabletScheduler *)->update_upper_trans_version_and_gc_sstable())) {
      LOG_WARN("Fail to update upper_trans_version and gc sstable", K(ret));
    }
    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("SSTableGCTask", K(cost_ts));
  }
}

void ObTenantTabletSchedulerTaskMgr::InfoPoolResizeTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->set_max())) {
    LOG_WARN("Fail to resize info pool", K(ret));
  }
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->gc_info())) {
    LOG_WARN("Fail to gc info", K(ret));
  }
  if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->gc_cg_info_array())) {
    LOG_WARN("Fail to gc info", K(ret));
  }
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->refresh_tenant_status())) {
    LOG_WARN("Fail to refresh tenant status", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("InfoPoolResizeTask", K(cost_ts));
}

void ObTenantTabletSchedulerTaskMgr::TabletUpdaterRefreshTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(MTL(observer::ObTabletTableUpdater *)->set_thread_count())) {
    LOG_WARN("Fail to reset thread count", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("TabletUpdaterRefreshTask", K(cost_ts));
}

void ObTenantTabletSchedulerTaskMgr::MediumCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(MTL(ObTenantMediumChecker *)->check_medium_finish_schedule())) {
    LOG_WARN("Fail to check_medium_finish and schedule", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("MediumCheckTask", K(cost_ts));
}

int ObTenantTabletSchedulerTaskMgr::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MergeLoop, merge_loop_tg_id_))) {
    LOG_WARN("failed to create merge loop thread", K(ret));
  } else if (OB_FAIL(TG_START(merge_loop_tg_id_))) {
    LOG_WARN("failed to start minor merge scan thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(merge_loop_tg_id_, merge_loop_task_, schedule_interval_, repeat))) {
    LOG_WARN("Fail to schedule minor merge scan task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MediumLoop, medium_loop_tg_id_))) {
    LOG_WARN("failed to create medium loop thread", K(ret));
  } else if (OB_FAIL(TG_START(medium_loop_tg_id_))) {
    LOG_WARN("failed to start medium merge scan thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(medium_loop_tg_id_, medium_loop_task_, schedule_interval_, repeat))) {
    LOG_WARN("Fail to schedule medium merge scan task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(medium_loop_tg_id_, medium_check_task_, MEDIUM_CHECK_INTERVAL, repeat))) {
    LOG_WARN("Fail to schedule medium merge check task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTableGC, sstable_gc_tg_id_))) {
    LOG_WARN("failed to create merge loop thread", K(ret));
  } else if (OB_FAIL(TG_START(sstable_gc_tg_id_))) {
    LOG_WARN("failed to start sstable gc thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(sstable_gc_tg_id_, sstable_gc_task_, SSTABLE_GC_INTERVAL, repeat))) {
    LOG_WARN("Fail to schedule sstable gc task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::CompactionRefresh, compaction_refresh_tg_id_))) {
    LOG_WARN("failed to create compaction refresh thread", K(ret));
  } else if (OB_FAIL(TG_START(compaction_refresh_tg_id_))) {
    LOG_WARN("failed to start compaction refresh thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(compaction_refresh_tg_id_, info_pool_resize_task_, INFO_POOL_RESIZE_INTERVAL, repeat))) {
    LOG_WARN("Fail to schedule info pool resize task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(compaction_refresh_tg_id_, tablet_updater_refresh_task_, TABLET_UPDATER_REFRESH_INTERVAL, repeat))) {
    LOG_WARN("Fail to schedule tablet updater refresh task", K(ret));
  }
  return ret;
}

int ObTenantTabletSchedulerTaskMgr::restart_scheduler_timer_task(
  const int64_t merge_schedule_interval)
{
  int ret = OB_SUCCESS;
  if (schedule_interval_ == merge_schedule_interval) {
  } else if (OB_FAIL(restart_schedule_timer_task(merge_schedule_interval, medium_loop_tg_id_, medium_loop_task_))) {
    LOG_WARN("failed to reload new merge schedule interval", K(merge_schedule_interval));
  } else if (OB_FAIL(restart_schedule_timer_task(merge_schedule_interval, merge_loop_tg_id_, merge_loop_task_))) {
    LOG_WARN("failed to reload new merge schedule interval", K(merge_schedule_interval));
  } else {
    schedule_interval_ = merge_schedule_interval;
    LOG_INFO("succeeded to reload new merge schedule interval", K(merge_schedule_interval));
  }
  return ret;
}

int ObTenantTabletSchedulerTaskMgr::restart_schedule_timer_task(
  const int64_t schedule_interval,
  const int64_t tg_id,
  common::ObTimerTask &timer_task)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_FAIL(TG_TASK_EXIST(tg_id, timer_task, is_exist))) {
    LOG_ERROR("failed to check merge schedule task exist", K(ret));
  } else if (is_exist && OB_FAIL(TG_CANCEL_R(tg_id, timer_task))) {
    LOG_WARN("failed to cancel task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, timer_task, schedule_interval, true/*repeat*/))) {
    LOG_WARN("Fail to schedule timer task", K(ret));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
