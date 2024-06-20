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

#include "rootserver/mview/ob_mview_maintenance_task.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_errno.h"
#include "share/schema/ob_mview_info.h"
#include "share/schema/ob_mview_refresh_stats_params.h"
#include "storage/mview/ob_mview_refresh_stats_purge.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share::schema;
using namespace dbms_scheduler;

ObMViewMaintenanceTask::ObMViewMaintenanceTask()
  : tenant_id_(OB_INVALID_TENANT_ID),
    round_(0),
    status_(StatusType::PREPARE),
    error_code_(OB_SUCCESS),
    last_fetch_mview_id_(OB_INVALID_ID),
    mview_idx_(0),
    gc_mview_id_(OB_INVALID_ID),
    fetch_mview_num_(0),
    gc_mview_num_(0),
    gc_stats_num_(0),
    start_time_(-1),
    start_gc_mview_time_(-1),
    cost_us_(-1),
    prepare_cost_us_(-1),
    gc_mview_cost_us_(-1),
    fetch_finish_(false),
    in_sched_(false),
    is_stop_(true),
    is_inited_(false)
{
}

ObMViewMaintenanceTask::~ObMViewMaintenanceTask() {}

int ObMViewMaintenanceTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewMaintenanceTask init twice", KR(ret), KP(this));
  } else {
    const uint64_t tenant_id = MTL_ID();
    tenant_id_ = tenant_id;
    mview_ids_.set_attr(ObMemAttr(tenant_id, "MVIds"));
    is_inited_ = true;
  }
  return ret;
}

int ObMViewMaintenanceTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceTask not init", KR(ret), KP(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ && OB_FAIL(schedule_task(MVIEW_MAINTENANCE_SCHED_INTERVAL, false /*repeat*/))) {
      LOG_WARN("fail to schedule mview maintenance task", KR(ret));
    } else {
      in_sched_ = true;
    }
  }
  return ret;
}

void ObMViewMaintenanceTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObMViewMaintenanceTask::wait() { wait_task(); }

void ObMViewMaintenanceTask::destroy()
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

void ObMViewMaintenanceTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewMaintenanceTask not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_stop_)) {
    // do nothing
  } else {
    switch (status_) {
      case StatusType::PREPARE:
        if (OB_FAIL(prepare())) {
          LOG_WARN("fail to prepare", KR(ret));
        }
        break;
      case StatusType::GC_MVIEW:
        if (OB_FAIL(gc_mview())) {
          LOG_WARN("fail to gc mview", KR(ret));
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

bool ObMViewMaintenanceTask::is_retry_ret_code(int ret_code) { return OB_EAGAIN == ret_code; }

void ObMViewMaintenanceTask::switch_status(StatusType new_status, int ret_code)
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
  if (in_sched_ && OB_FAIL(schedule_task(MVIEW_MAINTENANCE_SCHED_INTERVAL, false /*repeat*/))) {
    LOG_WARN("fail to schedule mview maintenance task", KR(ret));
  }
}

int ObMViewMaintenanceTask::prepare()
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
    LOG_INFO("mview maintenance task prepare success", K(tenant_id_), K(round_),
             K(prepare_cost_us_));
  }
  switch_status(StatusType::GC_MVIEW, ret);
  return ret;
}

int ObMViewMaintenanceTask::gc_mview()
{
  int ret = OB_SUCCESS;
  StatusType new_status = StatusType::GC_MVIEW;
  if (start_gc_mview_time_ == -1) {
    start_gc_mview_time_ = ObTimeUtil::current_time();
  }
  if (mview_idx_ >= mview_ids_.count() && OB_INVALID_ID == gc_mview_id_) { // fetch next batch
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
  } else { // gc current batch
    ObSchemaGetterGuard schema_guard;
    int64_t tenant_schema_version = OB_INVALID_VERSION;
    int64_t gc_mview_num = 0;
    int64_t gc_stats_num = 0;
    int64_t affected_rows = 0;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_schema_version))) {
      LOG_WARN("fail to get schema version", KR(ret), K(tenant_id_));
    }
    while (OB_SUCC(ret) && (mview_idx_ < mview_ids_.count() || OB_INVALID_ID != gc_mview_id_) &&
           gc_stats_num < MVREF_STATS_NUM_PURGE_PER_SCHED) {
      const int64_t limit = MVREF_STATS_NUM_PURGE_PER_SCHED - gc_stats_num;
      if (OB_INVALID_ID == gc_mview_id_) { // check next mview id
        const uint64_t mview_id = mview_ids_.at(mview_idx_);
        ObMViewInfo mview_info;
        const ObTableSchema *table_schema = nullptr;
        bool is_exist = false;
        if (OB_FAIL(
              ObMViewInfo::fetch_mview_info(*GCTX.sql_proxy_, tenant_id_, mview_id, mview_info))) {
          LOG_WARN("fail to fetch mview info", KR(ret), K(tenant_id_), K(mview_id));
        } else if (mview_info.get_schema_version() > tenant_schema_version) {
          is_exist = true; // skip, wait next round
        } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, mview_id, table_schema))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id_), K(mview_id));
        } else {
          is_exist = (nullptr != table_schema);
        }
        if (OB_SUCC(ret)) {
          ++mview_idx_;
          if (!is_exist) {
            gc_mview_id_ = mview_id;
            LOG_INFO("gc one mview", K_(tenant_id), K(mview_id));
          }
        }
      } else {
        const uint64_t mview_id = gc_mview_id_;
        ObMViewRefreshStats::FilterParam filter_param;
        filter_param.set_mview_id(mview_id);
        if (OB_FAIL(ObMViewRefreshStatsPurgeUtil::purge_refresh_stats(
              *GCTX.sql_proxy_, tenant_id_, filter_param, affected_rows, limit))) {
          LOG_WARN("fail to purge refresh stats", KR(ret), K(tenant_id_), K(filter_param),
                   K(limit));
        } else {
          gc_stats_num += affected_rows;
        }
        if (OB_SUCC(ret) && affected_rows < limit) {
          if (OB_FAIL(drop_mview(mview_id))) {
            LOG_WARN("fail to drop mview", KR(ret), K(mview_id));
          } else {
            gc_mview_id_ = OB_INVALID_ID;
            ++gc_mview_num;
          }
        }
      }
    }
    gc_mview_num_ += gc_mview_num;
    gc_stats_num_ += gc_stats_num;
  }
  if (OB_SUCC(ret) && fetch_finish_ && mview_idx_ >= mview_ids_.count() &&
      OB_INVALID_ID == gc_mview_id_) { // goto next status
    gc_mview_cost_us_ = ObTimeUtility::current_time() - start_gc_mview_time_;
    LOG_INFO("mview maintenance task gc mview success", K(tenant_id_), K(round_),
             K(gc_mview_cost_us_), K(fetch_mview_num_), K(gc_mview_num_), K(gc_stats_num_));
    new_status = StatusType::SUCCESS;
  }
  switch_status(new_status, ret);
  return ret;
}

int ObMViewMaintenanceTask::finish()
{
  int ret = OB_SUCCESS;
  cost_us_ = ObTimeUtility::current_time() - start_time_;
  LOG_INFO("mview maintenace task finish", K(tenant_id_), K(round_), K(status_), K(error_code_),
           K(cost_us_), K(prepare_cost_us_), K(gc_mview_cost_us_), K(fetch_mview_num_),
           K(gc_mview_num_), K(gc_stats_num_));
  // cleanup
  cleanup();
  // schedule next round
  if (in_sched_ && OB_FAIL(schedule_task(MVIEW_MAINTENANCE_INTERVAL, false /*repeat*/))) {
    LOG_WARN("fail to schedule mview maintenance task", KR(ret));
  }
  return ret;
}

void ObMViewMaintenanceTask::cleanup()
{
  status_ = StatusType::PREPARE;
  error_code_ = OB_SUCCESS;
  last_fetch_mview_id_ = OB_INVALID_ID;
  mview_ids_.reset();
  mview_idx_ = 0;
  gc_mview_id_ = OB_INVALID_ID;
  fetch_mview_num_ = 0;
  gc_mview_num_ = 0;
  gc_stats_num_ = 0;
  start_time_ = -1;
  start_gc_mview_time_ = -1;
  cost_us_ = -1;
  prepare_cost_us_ = -1;
  gc_mview_cost_us_ = -1;
  fetch_finish_ = false;
}

int ObMViewMaintenanceTask::drop_mview(uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mview_id));
  } else {
    ObMySQLTransaction trans;
    ObMViewInfo mview_info;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(ObMViewInfo::fetch_mview_info(trans, tenant_id_, mview_id, mview_info,
                                                     true /*for_update*/, true /*nowait*/))) {
      if (OB_LIKELY(OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT == ret)) {
        LOG_WARN("can not lock mview info", KR(ret), K_(tenant_id), K(mview_id));
        ret = OB_SUCCESS; // skip, wait next round
      } else if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
        LOG_WARN("mview info not exist", KR(ret), K_(tenant_id), K(mview_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to fetch mview info", KR(ret), K(tenant_id_), K(mview_id));
      }
    } else if (OB_FAIL(ObMViewRefreshStatsParams::drop_mview_refresh_stats_params(
                 trans, tenant_id_, mview_id, true /*if_exists*/))) {
      LOG_WARN("fail to drop mview refresh stats params", KR(ret), K(tenant_id_), K(mview_id));
    } else if (!mview_info.get_refresh_job().empty() &&
               OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
                 trans, tenant_id_, mview_info.get_refresh_job(), true /*if_exists*/))) {
      LOG_WARN("fail to remove dbms sched job", KR(ret), K(tenant_id_), "job_name",
               mview_info.get_refresh_job());
    } else if (OB_FAIL(ObMViewInfo::drop_mview_info(trans, mview_info))) {
      LOG_WARN("fail to drop mview info", KR(ret), K(mview_info));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
