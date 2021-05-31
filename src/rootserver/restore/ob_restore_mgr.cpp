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

#include "ob_restore_mgr.h"
#include "common/ob_partition_key.h"
#include "share/restore/ob_restore_args.h"
#include "share/restore/ob_restore_uri_parser.h"
#include "observer/ob_restore_ctx.h"
#include "rootserver/ob_balance_info.h"
#include "rootserver/ob_rebalance_task_mgr.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/restore/ob_restore_meta.h"
#include "rootserver/restore/ob_restore_replica.h"
#include "rootserver/restore/ob_restore_util.h"
#include "rootserver/ob_root_balancer.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

ObRestoreMgr::ObRestoreMgr(const volatile bool& is_stop) : inited_(false), is_stop_(is_stop), ctx_(NULL)
{}

ObRestoreMgr::~ObRestoreMgr()
{
  inited_ = false;
}

int ObRestoreMgr::init(ObRestoreMgrCtx* restore_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(restore_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret));
  } else if (!restore_ctx->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid restore ctx", K(ret));
  } else {
    ctx_ = restore_ctx;
    inited_ = true;
  }
  return ret;
}

// restore by steps
int ObRestoreMgr::restore()
{
  int ret = OB_SUCCESS;
  ObSEArray<RestoreJob, 10> job_infos;
  LOG_INFO("restore start");
  if (!inited_ || NULL == ctx_ || !ctx_->is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid status", K_(inited), KP_(ctx), K(ret));
  } else if (OB_FAIL(get_jobs(job_infos))) {
    LOG_WARN("fail get job", K(ret));
  } else {
    if (job_infos.count() > 0) {
      ctx_->root_balancer_->wakeup();
    }
    int tmp_ret = OB_SUCCESS;
    // Logical restore jobs are executed asynchronously.
    FOREACH_CNT(job_info, job_infos)
    {
      int64_t job_status = job_info->status_;
      switch (job_status) {
        case RESTORE_INIT:
          tmp_ret = restore_meta(*job_info);
          break;
        case RESTORE_DOING:
          tmp_ret = restore_replica(*job_info);
          break;
        case RESTORE_DONE:
          tmp_ret = restore_success(*job_info);
          break;
        case RESTORE_STOP:
          // not implemnt
          tmp_ret = restore_stop(*job_info);
          break;
        case RESTORE_FAIL:
          tmp_ret = restore_fail(*job_info);
          break;
      }
      LOG_INFO("doing restore", K(job_status), K(job_info), K(tmp_ret), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_update_job_status())) {
        LOG_WARN("fail to update job status", K(ret));
      }
    }
  }
  return ret;
}

// To reduce schedule time of logical restore,
// wake up root_balancer while job_status is changed successfully.
int ObRestoreMgr::try_update_job_status()
{
  int ret = OB_SUCCESS;
  ObSEArray<RestoreJob, 10> job_infos;
  LOG_INFO("try update job status");
  if (!inited_ || NULL == ctx_ || !ctx_->is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid status", K_(inited), KP_(ctx), K(ret));
  } else if (OB_FAIL(get_jobs(job_infos))) {
    LOG_WARN("fail get job", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    FOREACH_CNT(job_info, job_infos)
    {
      int64_t job_status = job_info->status_;
      if (RESTORE_DONE == job_status) {
        tmp_ret = restore_success(*job_info);
        LOG_INFO("doing restore", K(job_status), K(job_info), K(tmp_ret), K(ret));
      } else if (RESTORE_FAIL == job_status) {
        tmp_ret = restore_fail(*job_info);
        LOG_INFO("doing restore", K(job_status), K(job_info), K(tmp_ret), K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}
int ObRestoreMgr::restore_meta(RestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  if (!inited_ || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->conn_env_)) {
    ret = OB_NOT_INIT;
  } else {
    ObRestoreMeta restore_meta(*ctx_->conn_env_, job_info, is_stop_);
    if (OB_FAIL(restore_meta.execute())) {
      LOG_WARN("fail execute meta restore", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // mark job as fail
    int tmp_ret = update_job_status(job_info.job_id_, RESTORE_FAIL);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail update job status. may cause restore job status unmatch", K(tmp_ret));
    }
    ROOTSERVICE_EVENT_ADD("balancer", "fail_restore_tenant_meta", "tenant", job_info.tenant_name_, "result", ret);
  } else {
    int tmp_ret = update_job_status(job_info.job_id_, RESTORE_DOING);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail update job status. may cause restore job status unmatch", K(tmp_ret));
    }
    ROOTSERVICE_EVENT_ADD("balancer", "finish_restore_tenant_meta", "tenant", job_info.tenant_name_);
  }
  return ret;
}

int ObRestoreMgr::restore_replica(RestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  if (!inited_ || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->conn_env_)) {
    ret = OB_NOT_INIT;
  } else {
    ObRestoreReplica restore_replica(*ctx_, job_info, is_stop_);
    if (OB_FAIL(restore_replica.restore())) {
      LOG_WARN("fail execute replica restore", K(job_info), K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // mark job as fail
    int tmp_ret = update_job_status(job_info.job_id_, RESTORE_FAIL);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail update job status. may cause restore job status unmatch", K(tmp_ret));
    }
    ROOTSERVICE_EVENT_ADD("balancer", "fail_restore_tenant_replica", "tenant", job_info.tenant_name_, "result", ret);
  } else {
    // Restore replica stage will finish when all logical restore tasks were executed successfully.
  }
  return ret;
}

int ObRestoreMgr::restore_success(RestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  ObRestoreTableOperator restore_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(restore_op.init(ctx_->sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(restore_op.recycle_job(job_info.job_id_, RESTORE_DONE))) {
    LOG_WARN("fail recycle job tasks", K(job_info), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("balancer", "finish_restore_tenant", "tenant", job_info.tenant_name_);
  return ret;
}

int ObRestoreMgr::restore_stop(RestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  // just nop! schedule no task
  UNUSED(job_info);
  return ret;
}

int ObRestoreMgr::restore_fail(RestoreJob& job_info)
{
  int ret = OB_SUCCESS;
  ObRestoreTableOperator restore_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(restore_op.init(ctx_->sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(restore_op.recycle_job(job_info.job_id_, RESTORE_FAIL))) {
    LOG_WARN("fail recycle job tasks", K(job_info), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("balancer", "fail_restore_tenant", "tenant", job_info.tenant_name_, "recycle_ret", ret);
  return ret;
}

int ObRestoreMgr::update_job_status(int64_t job_id, RestoreTaskStatus status)
{
  int ret = OB_SUCCESS;
  ObRestoreTableOperator restore_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(restore_op.init(ctx_->sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(restore_op.update_job_status(job_id, status))) {
    LOG_WARN("update job status fail", K(job_id), K(status), K(ret));
  }
  return ret;
}

int ObRestoreMgr::get_jobs(common::ObIArray<RestoreJob>& jobs)
{
  int ret = OB_SUCCESS;
  ObRestoreTableOperator restore_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(restore_op.init(ctx_->sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(restore_op.get_jobs(jobs))) {
    LOG_WARN("get job fail", K(ret));
  }
  return ret;
}
