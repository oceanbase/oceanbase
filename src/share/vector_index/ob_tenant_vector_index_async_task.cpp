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

#define USING_LOG_PREFIX SERVER

#include "share/vector_index/ob_tenant_vector_index_async_task.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/table/ob_ttl_util.h"
#include "share/ob_max_id_fetcher.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{

namespace share
{

// ---------------------------------- ObVectorIndexHistoryTask -----------------------------------------//

void ObVectorIndexHistoryTask::runTimerTask()
{
  ObCurTraceId::init(GCONF.self_addr_);
  do_work(); // ignore error
}

void ObVectorIndexHistoryTask::do_work()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index history task is not init", KR(ret));
  } else if (is_paused_) {
    // timer paused or not leader, do nothing
  } else if (!ObVecIndexAsyncTaskUtil::check_can_do_work()) { // skip
  } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) { // skip
  } else if (OB_FAIL(move_task_to_history_table())) {
    LOG_WARN("fail to move task to history table", K(ret));
  } else if (OB_FAIL(clear_history_task())) {
    LOG_WARN("fail to clear history task", K(ret));
  }
}

int ObVectorIndexHistoryTask::clear_history_task()
{
  int ret = OB_SUCCESS;

  const int64_t batch_size = OB_VEC_INDEX_TASK_DEL_COUNT_PER_TASK; // 4096
  const int64_t now = ObTimeUtility::current_time();
  int64_t delete_timestamp = now - OB_VEC_INDEX_TASK_HISTORY_SAVE_TIME_US;
  int64_t affect_rows = 0;
  ObSqlString sql;
  ObMySQLTransaction trans;
  if (OB_ISNULL(sql_proxy_) || tenant_id_ == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), KP(sql_proxy_));
  } else if (is_paused_) {
    ret = OB_EAGAIN;
    FLOG_INFO("exit timer task once cuz leader switch", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
    LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::clear_history_expire_task_record(
      tenant_id_, batch_size, trans, affect_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else {
    LOG_DEBUG("success to clear_history_task", K(ret), K(tenant_id_), K(sql), K(affect_rows));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}


// batch move all
int ObVectorIndexHistoryTask::move_task_to_history_table()
{
  int ret = OB_SUCCESS;
  int64_t batch_size = OB_VEC_INDEX_TASK_MOVE_BATCH_SIZE;
  int64_t move_rows = batch_size;
  if (OB_ISNULL(sql_proxy_) || tenant_id_ == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_), KP(sql_proxy_));
  } else {
    while (OB_SUCC(ret) && move_rows != 0) {
      ObMySQLTransaction trans;
      if (is_paused_) {
        ret = OB_EAGAIN;
        FLOG_INFO("exit timer task once cuz leader switch", K(ret), K(tenant_id_));
      } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
        LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::move_task_to_history_table(tenant_id_, batch_size, trans, move_rows))) {
        LOG_WARN("fail to move task to history table", KR(ret), K(tenant_id_));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }
  LOG_DEBUG("do move task to history table", K(ret), K(tenant_id_));
  return ret;
}

int ObVectorIndexHistoryTask::init(const uint64_t tenant_id, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ttl history task init twice", KR(ret));
  } else if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    sql_proxy_ = &sql_proxy;
    tenant_id_ = tenant_id;
    disable_timeout_check();
    is_inited_ = true;
  }
  return ret;
}

void ObVectorIndexHistoryTask::resume()
{
  is_paused_ = false;
}

void ObVectorIndexHistoryTask::pause()
{
  is_paused_ = true;
}

// ---------------------------------- ObTenantVecAsyncTaskScheduler -----------------------------------------//

int ObTenantVecAsyncTaskScheduler::init(const uint64_t tenant_id, ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant ttl mgr init twice", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TenantTTLManager, tg_id_))) {    // 生成新的timer
    LOG_WARN("fail to init timer", KR(ret));
  } else if (OB_FAIL(vec_history_task_.init(tenant_id, sql_proxy))) { // 历史表清理
    LOG_WARN("fail to init clear history task", K(tenant_id));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    LOG_INFO("tenant vector index mgr is inited", K_(tenant_id));
  }
  return ret;
}

int ObTenantVecAsyncTaskScheduler::start()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("tenant vector manager begin to start", K_(tenant_id));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("init ttl scheduler fail", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, vec_history_task_, VEC_INDEX_CLEAR_TASK_PERIOD, true))) {
    LOG_WARN("fail to start vector index clear history task", KR(ret));
  }
  FLOG_INFO("start tenant vector index manager", KR(ret), K_(tenant_id));

  return ret;
}

void ObTenantVecAsyncTaskScheduler::wait()
{
  FLOG_INFO("wait tenant vector index async task manager", K_(tenant_id));
  TG_WAIT(tg_id_);
  FLOG_INFO("finish to wait tenant vector index async task manager", K_(tenant_id));
}

void ObTenantVecAsyncTaskScheduler::stop()
{
  FLOG_INFO("stop tenant vector index async task manager", K_(tenant_id));
  TG_STOP(tg_id_);
  FLOG_INFO("finish to stop tenant vector index async task manager", K_(tenant_id));
}

void ObTenantVecAsyncTaskScheduler::destroy()
{
  FLOG_INFO("destroy tenant vector index async task manager", K_(tenant_id));
  TG_DESTROY(tg_id_);
  tg_id_ = -1;
  FLOG_INFO("finish to destroy tenant vector index async task manager", K_(tenant_id));
}

void ObTenantVecAsyncTaskScheduler::resume()
{
  vec_history_task_.resume();
}

void ObTenantVecAsyncTaskScheduler::pause()
{
  vec_history_task_.pause();
}


} // end namespace share
} // end namespace oceanbase
