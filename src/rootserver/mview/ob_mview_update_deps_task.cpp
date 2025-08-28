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

#include "rootserver/mview/ob_mview_update_deps_task.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"

namespace oceanbase {
namespace rootserver {

ObMViewUpdateDepsTask::ObMViewUpdateDepsTask()
  : is_inited_(false),
    in_sched_(false),
    is_stop_(true),
    tenant_id_(OB_INVALID_TENANT_ID),
    last_sched_ts_(0)
{
}

ObMViewUpdateDepsTask::~ObMViewUpdateDepsTask() {}

int ObMViewUpdateDepsTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewUpdateDepsTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = MTL_ID();
    is_inited_ = true;
  }
  return ret;
}

int ObMViewUpdateDepsTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewUpdateDepsTask not init", KR(ret), KP(this));
  } else {
    is_stop_ = false;
    if (!in_sched_ && OB_FAIL(schedule_task(MVIEW_UPDATE_DEPS_INTERVAL, true /*repeat*/))) {
      LOG_WARN("fail to schedule ObMViewUpdateDepsTask", KR(ret));
    } else {
      in_sched_ = true;
    }
  }
  return ret;
}

void ObMViewUpdateDepsTask::stop()
{
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
}

void ObMViewUpdateDepsTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  in_sched_ = false;
  cancel_task();
  wait_task();
  tenant_id_ = OB_INVALID_TENANT_ID;
  last_sched_ts_ = 0;
}

void ObMViewUpdateDepsTask::wait() { wait_task(); }

int ObMViewUpdateDepsTask::need_schedule(bool &need_sche)
{
  int ret = OB_SUCCESS;
  need_sche = false;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = nullptr;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id_);
    if (OB_FAIL(sql.assign_fmt("SELECT EXISTS ("
                               "SELECT 1 FROM `%s`.`%s` t1, `%s`.`%s` t2"
                               " WHERE t1.mview_id = t2.p_obj AND t1.tenant_id = %lu) as exist_nested",
                               OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME, OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
                               ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id_)))) {
      LOG_WARN("failed to assign sql", KR(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql), K(tenant_id_));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is empty", KR(ret));
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
    } else {
      EXTRACT_BOOL_FIELD_MYSQL(*result, "exist_nested", need_sche);
    }
  }
  LOG_INFO("check mview update deps task need schedule", K(ret), K(need_sche), K(tenant_id_));
  return ret; 
}

void ObMViewUpdateDepsTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool need_sched = false;
  int64_t curr_ts = ObTimeUtil::current_time();
  if (curr_ts - last_sched_ts_ > 20 * 1000 * 1000) {
    if (OB_FAIL(need_schedule(need_sched))) {
      LOG_WARN("fail to check need schedule", K(ret), K(need_sched));
    }
  }
  if (OB_FAIL(ret) || !need_sched) {
  } else if (OB_FAIL(MTL(ObMViewMaintenanceService*)->get_all_mview_deps())) {
    LOG_WARN("update_mview_mds_op failed", KR(ret));
  } else {
    last_sched_ts_ = curr_ts;
  }
}

} // namespace rootserver
} // namespace oceanbase
