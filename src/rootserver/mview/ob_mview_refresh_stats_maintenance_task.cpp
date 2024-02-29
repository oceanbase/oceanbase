/**
 * Copyright (c) 2023 OceanBase
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

#include "rootserver/mview/ob_mview_refresh_stats_maintenance_task.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_errno.h"
#include "share/schema/ob_mview_info.h"
#include "share/schema/ob_mview_refresh_stats.h"
#include "share/schema/ob_mview_refresh_stats_params.h"
#include "storage/mview/ob_mview_refresh_stats_purge.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share::schema;

/**
 * ObMViewRefreshStatsMaintenanceTask
 */

ObMViewRefreshStatsMaintenanceTask::ObMViewRefreshStatsMaintenanceTask()
  : tenant_id_(OB_INVALID_TENANT_ID),
    round_(0),
    status_(StatusType::PREPARE),
    error_code_(OB_SUCCESS),
    last_fetch_mview_id_(OB_INVALID_ID),
    mview_idx_(0),
    fetch_mview_num_(0),
    purge_mview_num_(0),
    purge_stats_num_(0),
    start_time_(-1),
    start_purge_time_(-1),
    cost_us_(-1),
    prepare_cost_us_(-1),
    purge_cost_us_(-1),
    fetch_finish_(false),
    in_sched_(false),
    is_stop_(true),
    is_inited_(false)
{
}

ObMViewRefreshStatsMaintenanceTask::~ObMViewRefreshStatsMaintenanceTask() {}

int ObMViewRefreshStatsMaintenanceTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewRefreshStatsMaintenanceTask init twice", KR(ret), KP(this));
  } else {
    const uint64_t tenant_id = MTL_ID();
    tenant_id_ = tenant_id;
    mview_ids_.set_attr(ObMemAttr(tenant_id, "MVIds"));
    is_inited_ = true;
  }
  return ret;
}

int ObMViewRefreshStatsMaintenanceTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefreshStatsMaintenanceTask not init", KR(ret), KP(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ &&
        OB_FAIL(schedule_task(MVREF_STATS_MAINTENANCE_SCHED_INTERVAL, false /*repeat*/))) {
      LOG_WARN("fail to schedule mvref stats maintenance task", KR(ret));
    } else {
      in_sched_ = true;
    }
  }
  return ret;
}

void ObMViewRefreshStatsMaintenanceTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObMViewRefreshStatsMaintenanceTask::wait() { wait_task(); }

void ObMViewRefreshStatsMaintenanceTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
  wait_task();
  cleanup();
  tenant_id_ = OB_INVALID_TENANT_ID;
  mview_ids_.destroy();
}

void ObMViewRefreshStatsMaintenanceTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefreshStatsMaintenanceTask not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_stop_)) {
    // do nothing
  } else {
    switch (status_) {
      case StatusType::PREPARE:
        if (OB_FAIL(prepare())) {
          LOG_WARN("fail to prepare", KR(ret));
        }
        break;
      case StatusType::PURGE:
        if (OB_FAIL(purge())) {
          LOG_WARN("fail to purge", KR(ret));
        }
        break;
      case StatusType::SUCCESS:
      case StatusType::FAIL:
        if (OB_FAIL(finish())) {
          LOG_WARN("fail to finish", KR(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(status_));
        break;
    }
  }
}

bool ObMViewRefreshStatsMaintenanceTask::is_retry_ret_code(int ret_code)
{
  return OB_EAGAIN == ret_code;
}

void ObMViewRefreshStatsMaintenanceTask::switch_status(StatusType new_status, int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(OB_SUCCESS == ret_code)) {
    status_ = new_status;
  } else if (is_retry_ret_code(ret_code)) {
    // do nothing
  } else {
    status_ = StatusType::FAIL;
    error_code_ = ret_code;
  }
  if (in_sched_ &&
      OB_FAIL(schedule_task(MVREF_STATS_MAINTENANCE_SCHED_INTERVAL, false /*repeat*/))) {
    LOG_WARN("fail to schedule mvref stats maintenance task", KR(ret));
  }
}

int ObMViewRefreshStatsMaintenanceTask::prepare()
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (start_time_ == -1) {
    start_time_ = ObTimeUtil::current_time();
  }
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_EAGAIN;
    LOG_WARN("version lower than 4.3, try again", KR(ret), K_(tenant_id), K(compat_version));
  } else {
    ++round_;
    prepare_cost_us_ = ObTimeUtil::current_time() - start_time_;
    LOG_INFO("mvref stats maintenance task prepare success", K(tenant_id_), K(round_),
             K(prepare_cost_us_));
  }
  switch_status(StatusType::PURGE, ret);
  return ret;
}

int ObMViewRefreshStatsMaintenanceTask::purge()
{
  int ret = OB_SUCCESS;
  StatusType new_status = StatusType::PURGE;
  if (start_purge_time_ == -1) {
    start_purge_time_ = ObTimeUtil::current_time();
  }
  if (mview_idx_ >= mview_ids_.count()) { // fetch next batch
    mview_ids_.reset();
    mview_idx_ = 0;
    if (OB_FAIL(ObMViewInfo::batch_fetch_mview_ids(*GCTX.sql_proxy_, tenant_id_,
                                                   last_fetch_mview_id_, mview_ids_,
                                                   MVIEW_NUM_FETCH_PER_SCHED))) {
      LOG_WARN("fail to batch fetch mview ids", KR(ret), K(tenant_id_), K(last_fetch_mview_id_));
    } else {
      fetch_mview_num_ += mview_ids_.count();
      fetch_finish_ = mview_ids_.count() < MVIEW_NUM_FETCH_PER_SCHED;
      if (!mview_ids_.empty()) {
        last_fetch_mview_id_ = mview_ids_.at(mview_ids_.count() - 1);
      }
    }
  } else { // purge current batch
    int64_t purge_mview_num = 0;
    int64_t purge_stats_num = 0;
    int64_t affected_rows = 0;
    while (OB_SUCC(ret) && mview_idx_ < mview_ids_.count() &&
           purge_stats_num < MVREF_STATS_NUM_PURGE_PER_SCHED) {
      const uint64_t mview_id = mview_ids_.at(mview_idx_);
      const int64_t limit = MVREF_STATS_NUM_PURGE_PER_SCHED - purge_stats_num;
      ObMViewRefreshStatsParams refresh_stats_params;
      ObMViewRefreshStats::FilterParam filter_param;
      if (OB_FAIL(ObMViewRefreshStatsParams::fetch_mview_refresh_stats_params(
            *GCTX.sql_proxy_, tenant_id_, mview_id, refresh_stats_params,
            true /*with_sys_defaults*/))) {
        LOG_WARN("fail to fetch mview refresh stats params", KR(ret), K(tenant_id_), K(mview_id));
      } else if (refresh_stats_params.get_retention_period() == -1) {
        // never be purged, skip
        affected_rows = 0;
      } else {
        filter_param.set_mview_id(mview_id);
        filter_param.set_retention_period(refresh_stats_params.get_retention_period());
        if (OB_FAIL(ObMViewRefreshStatsPurgeUtil::purge_refresh_stats(
              *GCTX.sql_proxy_, tenant_id_, filter_param, affected_rows, limit))) {
          LOG_WARN("fail to purge refresh stats", KR(ret), K(tenant_id_), K(filter_param),
                   K(limit));
        }
      }
      if (OB_SUCC(ret)) {
        purge_stats_num += affected_rows;
        if (affected_rows < limit) {
          ++purge_mview_num;
          ++mview_idx_;
        }
      }
    }
    purge_mview_num_ += purge_mview_num;
    purge_stats_num_ += purge_stats_num;
  }
  if (OB_SUCC(ret) && fetch_finish_ && mview_idx_ >= mview_ids_.count()) { // goto next status
    purge_cost_us_ = ObTimeUtility::current_time() - start_purge_time_;
    LOG_INFO("mvref stats maintenance task purge success", K(tenant_id_), K(round_),
             K(purge_cost_us_), K(fetch_mview_num_), K(purge_mview_num_), K(purge_stats_num_));
    new_status = StatusType::SUCCESS;
  }
  switch_status(new_status, ret);
  return ret;
}

int ObMViewRefreshStatsMaintenanceTask::finish()
{
  int ret = OB_SUCCESS;
  cost_us_ = ObTimeUtility::current_time() - start_time_;
  LOG_INFO("mvref stats maintenace task finish", K(tenant_id_), K(round_), K(status_),
           K(error_code_), K(cost_us_), K(prepare_cost_us_), K(purge_cost_us_), K(fetch_mview_num_),
           K(purge_mview_num_), K(purge_stats_num_));
  // cleanup
  cleanup();
  // schedule next round
  if (in_sched_ && OB_FAIL(schedule_task(MVREF_STATS_MAINTENANCE_INTERVAL, false /*repeat*/))) {
    LOG_WARN("fail to schedule mvref stats maintenance task", KR(ret));
  }
  return ret;
}

void ObMViewRefreshStatsMaintenanceTask::cleanup()
{
  status_ = StatusType::PREPARE;
  error_code_ = OB_SUCCESS;
  last_fetch_mview_id_ = OB_INVALID_ID;
  mview_ids_.reset();
  mview_idx_ = 0;
  fetch_mview_num_ = 0;
  purge_mview_num_ = 0;
  purge_stats_num_ = 0;
  start_time_ = -1;
  start_purge_time_ = -1;
  cost_us_ = -1;
  prepare_cost_us_ = -1;
  purge_cost_us_ = -1;
  fetch_finish_ = false;
}

} // namespace rootserver
} // namespace oceanbase
