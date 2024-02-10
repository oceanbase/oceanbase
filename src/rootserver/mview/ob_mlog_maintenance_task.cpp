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

#include "rootserver/mview/ob_mlog_maintenance_task.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_errno.h"
#include "share/schema/ob_mlog_info.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share::schema;
using namespace dbms_scheduler;

ObMLogMaintenanceTask::ObMLogMaintenanceTask()
  : tenant_id_(OB_INVALID_TENANT_ID),
    round_(0),
    status_(StatusType::PREPARE),
    error_code_(OB_SUCCESS),
    last_fetch_mlog_id_(OB_INVALID_ID),
    fetch_mlog_num_(0),
    gc_mlog_num_(0),
    start_time_(-1),
    start_gc_mlog_time_(-1),
    cost_us_(-1),
    prepare_cost_us_(-1),
    gc_mlog_cost_us_(-1),
    fetch_finish_(false),
    in_sched_(false),
    is_stop_(true),
    is_inited_(false)
{
}

ObMLogMaintenanceTask::~ObMLogMaintenanceTask() {}

int ObMLogMaintenanceTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMLogMaintenanceTask init twice", KR(ret), KP(this));
  } else {
    const uint64_t tenant_id = MTL_ID();
    tenant_id_ = tenant_id;
    mlog_ids_.set_attr(ObMemAttr(tenant_id, "MLogIds"));
    is_inited_ = true;
  }
  return ret;
}

int ObMLogMaintenanceTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMLogMaintenanceTask not init", KR(ret), KP(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ && OB_FAIL(schedule_task(MLOG_MAINTENANCE_SCHED_INTERVAL, false /*repeat*/))) {
      LOG_WARN("fail to schedule mlog maintenance task", KR(ret));
    } else {
      in_sched_ = true;
    }
  }
  return ret;
}

void ObMLogMaintenanceTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObMLogMaintenanceTask::wait() { wait_task(); }
void ObMLogMaintenanceTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
  wait_task();
  cleanup();
  tenant_id_ = OB_INVALID_TENANT_ID;
  mlog_ids_.destroy();
}

void ObMLogMaintenanceTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMLogMaintenanceTask not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_stop_)) {
    // do nothing
  } else {
    switch (status_) {
      case StatusType::PREPARE:
        if (OB_FAIL(prepare())) {
          LOG_WARN("fail to prepare", KR(ret));
        }
        break;
      case StatusType::GC_MLOG:
        if (OB_FAIL(gc_mlog())) {
          LOG_WARN("fail to gc mlog", KR(ret));
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

bool ObMLogMaintenanceTask::is_retry_ret_code(int ret_code) { return OB_EAGAIN == ret_code; }

void ObMLogMaintenanceTask::switch_status(StatusType new_status, int ret_code)
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
  if (in_sched_ && OB_FAIL(schedule_task(MLOG_MAINTENANCE_SCHED_INTERVAL, false /*repeat*/))) {
    LOG_WARN("fail to schedule mlog maintenance task", KR(ret));
  }
}

int ObMLogMaintenanceTask::prepare()
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
    LOG_INFO("mlog maintenance task prepare success", K(tenant_id_), K(round_),
             K(prepare_cost_us_));
  }
  switch_status(StatusType::GC_MLOG, ret);
  return ret;
}

int ObMLogMaintenanceTask::gc_mlog()
{
  int ret = OB_SUCCESS;
  StatusType new_status = StatusType::GC_MLOG;
  if (start_gc_mlog_time_ == -1) {
    start_gc_mlog_time_ = ObTimeUtil::current_time();
  }
  if (mlog_ids_.empty()) { // fetch next batch
    if (OB_FAIL(ObMLogInfo::batch_fetch_mlog_ids(*GCTX.sql_proxy_, tenant_id_, last_fetch_mlog_id_,
                                                 mlog_ids_, MLOG_NUM_FETCH_PER_SCHED))) {
      LOG_WARN("fail to batch fetch mlog ids", KR(ret), K(tenant_id_), K(last_fetch_mlog_id_));
    } else {
      fetch_mlog_num_ += mlog_ids_.count();
      fetch_finish_ = mlog_ids_.count() < MLOG_NUM_FETCH_PER_SCHED;
      if (!mlog_ids_.empty()) {
        last_fetch_mlog_id_ = mlog_ids_.at(mlog_ids_.count() - 1);
      }
    }
  } else { // gc current batch
    ObSchemaGetterGuard schema_guard;
    int64_t tenant_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_schema_version))) {
      LOG_WARN("fail to get schema version", KR(ret), K(tenant_id_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < mlog_ids_.count(); ++i) {
      const uint64_t mlog_id = mlog_ids_.at(i);
      ObMLogInfo mlog_info;
      const ObTableSchema *table_schema = nullptr;
      bool is_exist = false;
      if (OB_FAIL(ObMLogInfo::fetch_mlog_info(*GCTX.sql_proxy_, tenant_id_, mlog_id, mlog_info))) {
        LOG_WARN("fail to fetch mlog info", KR(ret), K(tenant_id_), K(mlog_id));
      } else if (mlog_info.get_schema_version() > tenant_schema_version) {
        is_exist = true; // skip, wait next round
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, mlog_id, table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(tenant_id_), K(mlog_id));
      } else {
        is_exist = (nullptr != table_schema);
      }
      if (OB_SUCC(ret) && !is_exist) {
        LOG_INFO("gc one mlog", K_(tenant_id), K(mlog_id));
        if (OB_FAIL(drop_mlog(mlog_id))) {
          LOG_WARN("fail to drop mlog", KR(ret), K(mlog_id));
        } else {
          ++gc_mlog_num_;
        }
      }
    }
    mlog_ids_.reset();
  }
  if (OB_SUCC(ret) && fetch_finish_ && mlog_ids_.empty()) { // goto next status
    gc_mlog_cost_us_ = ObTimeUtility::current_time() - start_gc_mlog_time_;
    LOG_INFO("mlog maintenance task gc mlog success", K(tenant_id_), K(round_), K(gc_mlog_cost_us_),
             K(fetch_mlog_num_), K(gc_mlog_num_));
    new_status = StatusType::SUCCESS;
  }
  switch_status(new_status, ret);
  return ret;
}

int ObMLogMaintenanceTask::finish()
{
  int ret = OB_SUCCESS;
  cost_us_ = ObTimeUtility::current_time() - start_time_;
  LOG_INFO("mlog maintenace task finish", K(tenant_id_), K(round_), K(status_), K(error_code_),
           K(cost_us_), K(prepare_cost_us_), K(gc_mlog_cost_us_), K(fetch_mlog_num_),
           K(gc_mlog_num_));
  // cleanup
  cleanup();
  // schedule next round
  if (in_sched_ && OB_FAIL(schedule_task(MLOG_MAINTENANCE_INTERVAL, false /*repeat*/))) {
    LOG_WARN("fail to schedule mlog maintenance task", KR(ret));
  }
  return ret;
}

void ObMLogMaintenanceTask::cleanup()
{
  status_ = StatusType::PREPARE;
  error_code_ = OB_SUCCESS;
  last_fetch_mlog_id_ = OB_INVALID_ID;
  mlog_ids_.reset();
  fetch_mlog_num_ = 0;
  gc_mlog_num_ = 0;
  start_time_ = -1;
  start_gc_mlog_time_ = -1;
  cost_us_ = -1;
  prepare_cost_us_ = -1;
  gc_mlog_cost_us_ = -1;
  fetch_finish_ = false;
}

int ObMLogMaintenanceTask::drop_mlog(uint64_t mlog_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == mlog_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mlog_id));
  } else {
    ObMySQLTransaction trans;
    ObMLogInfo mlog_info;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(ObMLogInfo::fetch_mlog_info(trans, tenant_id_, mlog_id, mlog_info,
                                                   true /*for_update*/, true /*nowait*/))) {
      if (OB_LIKELY(OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT == ret)) {
        LOG_WARN("can not lock mlog info", KR(ret), K_(tenant_id), K(mlog_id));
        ret = OB_SUCCESS; // skip, wait next round
      } else if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
        LOG_WARN("mlog info not exist", KR(ret), K_(tenant_id), K(mlog_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to fetch mlog info", KR(ret), K(tenant_id_), K(mlog_id));
      }
    } else if (!mlog_info.get_purge_job().empty() &&
               OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
                 trans, tenant_id_, mlog_info.get_purge_job(), true /*if_exists*/))) {
      LOG_WARN("fail to remove dbms sched job", KR(ret), K(tenant_id_), "job_name",
               mlog_info.get_purge_job());
    } else if (OB_FAIL(ObMLogInfo::drop_mlog_info(trans, mlog_info))) {
      LOG_WARN("fail to drop mlog info", KR(ret), K(mlog_info));
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
