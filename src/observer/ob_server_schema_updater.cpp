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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_server_schema_updater.h"
#include "lib/thread/thread_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace observer
{
ObServerSchemaTask::ObServerSchemaTask()
  : type_(INVALID), did_retry_(false), schema_info_()
{
}

ObServerSchemaTask::ObServerSchemaTask(
  TYPE type,
  bool did_retry)
  : type_(type), did_retry_(did_retry), schema_info_()
{
}

ObServerSchemaTask::ObServerSchemaTask(
  TYPE type,
  bool did_retry,
  const ObRefreshSchemaInfo &schema_info)
  : type_(type), did_retry_(did_retry), schema_info_(schema_info)
{
}

ObServerSchemaTask::ObServerSchemaTask(TYPE type)
  : type_(type), did_retry_(false), schema_info_()
{
}

ObServerSchemaTask::ObServerSchemaTask(
  TYPE type,
  const uint64_t tenant_id,
  const int64_t schema_version)
  : type_(type), did_retry_(false), schema_info_()
{
  schema_info_.set_tenant_id(tenant_id);
  schema_info_.set_schema_version(schema_version);
}

bool ObServerSchemaTask::need_process_alone() const
{
  return REFRESH == type_ || RELEASE == type_;
}

bool ObServerSchemaTask::is_valid() const
{
  return INVALID != type_;
}

void ObServerSchemaTask::reset()
{
  type_ = INVALID;
  did_retry_ = false;
  schema_info_.reset();
}

int64_t ObServerSchemaTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&type_, sizeof(type_), hash_val);
  if (ASYNC_REFRESH == type_) {
    const uint64_t tenant_id = get_tenant_id();
    const int64_t schema_version = get_schema_version();
    hash_val = murmurhash(&tenant_id, sizeof(tenant_id), hash_val);
    hash_val = murmurhash(&schema_version, sizeof(schema_version), hash_val);
  }
  return static_cast<int64_t>(hash_val);
}

bool ObServerSchemaTask::operator ==(const ObServerSchemaTask &other) const
{
  bool bret = (type_ == other.type_);
  if (bret && ASYNC_REFRESH == type_) {
    bret = (get_tenant_id() == other.get_tenant_id())
            && (get_schema_version() == other.get_schema_version());
  }
  return bret;
}

bool ObServerSchemaTask::operator <(const ObServerSchemaTask &other) const
{
  bool bret = (type_ < other.type_);
  if (!bret && ASYNC_REFRESH == type_ && ASYNC_REFRESH == other.type_) {
    if (get_tenant_id() < other.get_tenant_id()) {
      bret = true;
    } else if (get_tenant_id() == other.get_tenant_id()
               && get_schema_version() < other.get_schema_version()) {
      bret = true;
    } else {
      bret = false;
    }
  }
  return bret;
}

bool ObServerSchemaTask::greator_than(
     const ObServerSchemaTask &lt,
     const ObServerSchemaTask &rt)
{
  bool bret = (lt.type_ > rt.type_);
  if (!bret && ASYNC_REFRESH == lt.type_ && ASYNC_REFRESH == rt.type_) {
    if (lt.get_tenant_id() > rt.get_tenant_id()) {
      bret = true;
    } else if (lt.get_tenant_id() == rt.get_tenant_id()
               && lt.get_schema_version() > rt.get_schema_version()) {
      bret = true;
    } else {
      bret = false;
    }
  }
  return bret;
}

bool ObServerSchemaTask::compare_without_version(const ObServerSchemaTask &other) const
{
  return (*this == other);
}

uint64_t ObServerSchemaTask::get_group_id() const
{
  return static_cast<uint64_t>(type_);
}

bool ObServerSchemaTask::is_barrier() const
{
  return false;
}

int ObServerSchemaUpdater::init(const common::ObAddr &host, ObMultiVersionSchemaService *schema_mgr)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_THREAD_CNT = 2;
  const int64_t MAX_TENANT_CNT = 1024;
  if (NULL == schema_mgr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_mgr must not null");
  } else if (OB_FAIL(task_queue_.init(this,
                                      MAX_THREAD_CNT,
                                      MAX_TENANT_CNT + 2,
                                      "SerScheQueue"))) {
    LOG_WARN("init task queue failed", KR(ret), LITERAL_K(MAX_THREAD_CNT), K(MAX_TENANT_CNT));
  } else {
    host_ = host;
    schema_mgr_ = schema_mgr;
    inited_ = true;
  }
  return ret;
}

void ObServerSchemaUpdater::stop()
{
  if (!inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "not init");
  } else {
    task_queue_.stop();
  }
}

void ObServerSchemaUpdater::wait()
{
  if (!inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "not init");
  } else {
    task_queue_.wait();
  }
}

void ObServerSchemaUpdater::destroy()
{
 if (inited_) {
   stop();
   wait();
   host_.reset();
   schema_mgr_ = NULL;
   inited_ = false;
 }
}

int ObServerSchemaUpdater::process_barrier(const ObServerSchemaTask &task, bool &stopped)
{
  UNUSEDx(task, stopped);
  return OB_NOT_SUPPORTED;
}

int ObServerSchemaUpdater::batch_process_tasks(
    const ObIArray<ObServerSchemaTask> &batch_tasks, bool &stopped)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(host_);
  ObArray<ObServerSchemaTask> tasks;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob_server_schema_updeter is not inited.", KR(ret));
  } else if (stopped) {
    ret = OB_CANCELED;
    LOG_WARN("ob_server_schema_updeter is stopped.", KR(ret));
  } else if (batch_tasks.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_tasks cnt is 0", KR(ret));
  } else if (OB_FAIL(tasks.assign(batch_tasks))) {
    LOG_WARN("fail to assign task", KR(ret), "task_cnt", batch_tasks.count());
  } else {
    DEBUG_SYNC(BEFORE_SET_NEW_SCHEMA_VERSION);
    lib::ob_sort(tasks.begin(), tasks.end(), ObServerSchemaTask::greator_than);
    ObServerSchemaTask::TYPE type = tasks.at(0).type_;
    if ((ObServerSchemaTask::REFRESH == type || ObServerSchemaTask::RELEASE == type)
        && (1 != tasks.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("refresh/release schema task should process alone",
               KR(ret), "task_cnt", tasks.count());
    } else if (ObServerSchemaTask::REFRESH == type) {
      if (OB_FAIL(process_refresh_task(tasks.at(0)))) {
        LOG_WARN("fail to process refresh task", KR(ret), K(tasks.at(0)));
      }
    } else if (ObServerSchemaTask::RELEASE == type) {
      if (OB_FAIL(process_release_task())) {
        LOG_WARN("fail to process release task", KR(ret), K(tasks.at(0)));
      }
    } else if (ObServerSchemaTask::ASYNC_REFRESH == type) {
      if (OB_FAIL(process_async_refresh_tasks(tasks))) {
        LOG_WARN("fail to process async refresh tasks", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", KR(ret), K(type));
    }
  }
  ObCurTraceId::reset();
  return ret;
}

int ObServerSchemaUpdater::process_refresh_task(const ObServerSchemaTask &task)
{
  int ret = OB_SUCCESS;
  const ObRefreshSchemaInfo &schema_info = task.schema_info_;
  ObTaskController::get().switch_task(share::ObTaskType::SCHEMA);
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  LOG_INFO("[REFRESH_SCHEMA] start to process schema refresh task", KR(ret), K(schema_info));
  if (OB_ISNULL(schema_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_ is NULL", KR(ret));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", KR(ret));
  } else if (GCTX.root_service_->in_service()
             && !GCTX.root_service_->is_full_service()) {
    // Rootservice will refresh schema(holding mutex lock) when it restarts,
    // so refresh schema task triggered by heartbeat should be avoided in such a situation.
    ret = OB_EAGAIN;
    LOG_WARN("rootservice is not in full service, try again", KR(ret),
             K(GCTX.root_service_->in_service()), K(GCTX.root_service_->is_full_service()));
  } else {
    ObRefreshSchemaInfo local_schema_info;
    const uint64_t new_sequence_id = schema_info.get_sequence_id();
    uint64_t last_sequence_id = OB_INVALID_ID;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_mgr_->get_last_refreshed_schema_info(local_schema_info))) {
      LOG_WARN("fail to get local schema info", KR(ret));
    } else if (FALSE_IT(last_sequence_id = local_schema_info.get_sequence_id())) {
    } else if (OB_INVALID_ID != last_sequence_id && last_sequence_id >= new_sequence_id) {
      // skip
      LOG_INFO("[REFRESH_SCHEMA] local schema info is newer, no need to refresh schema",
               KR(ret), K(local_schema_info), K(schema_info));
    } else {
      // empty tenant_ids means refresh all tenants' schema.
      ObSEArray<uint64_t, 1> tenant_ids;
      uint64_t local_sequence_id = local_schema_info.get_sequence_id();
      uint64_t new_sequence_id = schema_info.get_sequence_id();
      // It means observer don't lost heartbeat if sequence_id is consistent, so observer can only refresh specific tenant's schema.
      if (local_schema_info.is_valid()
          && OB_INVALID_VERSION != local_sequence_id
          && local_sequence_id + 1 == new_sequence_id) {
        uint64_t refresh_tenant_id = schema_info.get_tenant_id();
        if (OB_FAIL(tenant_ids.push_back(refresh_tenant_id))) {
          LOG_WARN("fail to push back tenant_id", KR(ret), K(refresh_tenant_id));
        } else {
          LOG_INFO("[REFRESH_SCHEMA] refresh schema by tenant",
                   KR(ret), K(refresh_tenant_id), K(local_schema_info), K(schema_info));
        }
      }
      int64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
      LOG_INFO("[REFRESH_SCHEMA] begin refresh schema, ", K(begin_time));
      bool check_bootstrap = (OB_INVALID_ID == new_sequence_id);
      if (FAILEDx(schema_mgr_->refresh_and_add_schema(tenant_ids, check_bootstrap))) {
        LOG_WARN("fail to refresh and add schema", KR(ret));
      } else if (OB_FAIL(schema_mgr_->set_last_refreshed_schema_info(schema_info))) {
        LOG_WARN("fail to set last_refreshed_schema_info", KR(ret));
      } else {
      }
      LOG_INFO("[REFRESH_SCHEMA] end refresh schema with new mode, ",
               KR(ret), K(tenant_ids), "used time", ObTimeUtility::current_time() - begin_time);
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(try_load_baseline_schema_version_())) { // ignore ret
    LOG_WARN("fail to load tenant baseline schema version", KR(tmp_ret));
  }

  // For performance, schema_guard will be cached in one session instead of each SQL statement constructs its own new schema_guard,
  // which may lead to lack of schema slots since schema guard will hold reference of schema mgr in long time.
  // To avoid -4023 error caused by lack of schema slots while refresh schema, observer should try to release cached schema_guard in different sessions in such situation.
  if (OB_EAGAIN == ret) {
    OBSERVER.get_sql_session_mgr().try_check_session();
  }

  // dump schema statistics info
  if (REACH_TIME_INTERVAL(10 * 60 * 1000 * 1000)) { // 10min
    if (OB_NOT_NULL(schema_mgr_)) {
      schema_mgr_->dump_schema_statistics();
    }
  }
  return ret;
}

int ObServerSchemaUpdater::process_release_task()
{
  int ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::SCHEMA);
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  if (OB_ISNULL(schema_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_ is NULL", KR(ret));
  } else if (!schema_mgr_->is_sys_full_schema()) {
    // auto release unused memory of schema after schema split
  } else if (OB_FAIL(schema_mgr_->try_eliminate_schema_mgr())) {
    LOG_WARN("fail to eliminate schema mgr", KR(ret));
  }
  LOG_INFO("try to release schema", KR(ret));
  return ret;
}

int ObServerSchemaUpdater::process_async_refresh_tasks(
    const ObIArray<ObServerSchemaTask> &tasks)
{
  int ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::SCHEMA);
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  if (OB_ISNULL(schema_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_ is NULL", KR(ret));
  } else {
    // For each tenant, we can only execute the async refresh schema task which has maximum schema_version.
    ObSEArray<uint64_t, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> tenant_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); i++) {
      const ObServerSchemaTask &cur_task = tasks.at(i);
      if (ObServerSchemaTask::ASYNC_REFRESH != cur_task.type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur task type should be ASYNC_REFRESH", KR(ret), K(cur_task));
      } else if (i > 0) {
        const ObServerSchemaTask &last_task = tasks.at(i - 1);
        if (last_task.get_tenant_id() < cur_task.get_tenant_id()
            || (last_task.get_tenant_id() == cur_task.get_tenant_id()
                && last_task.get_schema_version() < cur_task.get_schema_version())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur task should be less than last task",
                   KR(ret), K(last_task), K(cur_task));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t cnt = tenant_ids.count();
        if (0 == cnt || tenant_ids.at(cnt - 1) != cur_task.get_tenant_id()) {
          const uint64_t tenant_id = cur_task.get_tenant_id();
          int64_t local_version = OB_INVALID_VERSION;
          int tmp_ret = OB_SUCCESS;
          if (i > 0 && tasks.at(i - 1).get_tenant_id() == cur_task.get_tenant_id()) {
            // Tasks have been sorted by (tenant_id, schema_version) in desc order, so we just get first task by tenant.
          } else if (OB_SUCCESS != (tmp_ret = schema_mgr_->get_tenant_refreshed_schema_version(
                     tenant_id, local_version))) { // ignore ret
            if (OB_ENTRY_NOT_EXIST != tmp_ret) {
              LOG_WARN("failed to get tenant refreshed schema version", KR(tmp_ret), K(tenant_id));
            }
          } else if (cur_task.get_schema_version() > local_version) {
            if (OB_FAIL(tenant_ids.push_back(cur_task.get_tenant_id()))) {
              LOG_WARN("fail to push back task", KR(ret), K(cur_task));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && tenant_ids.count() > 0) {
      if (OB_FAIL(schema_mgr_->refresh_and_add_schema(tenant_ids))) {
        LOG_WARN("fail to refresh schema", KR(ret), K(tenant_ids));
      }
    }
  }
  LOG_INFO("try to async refresh schema", KR(ret));
  return ret;
}

int ObServerSchemaUpdater::try_reload_schema(
    const ObRefreshSchemaInfo &schema_info,
    const bool set_received_schema_version)
{

  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob_server_schema_updeter is not inited.", KR(ret));
  } else if (OB_ISNULL(schema_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else {
    // Try to update received_broadcast_version which used to check if local schema is new enough for SQL execution.
    // Here, we ignore error since set_tenant_received_broadcast_version() may fail before tenant firstly refresh schema.
    int tmp_ret = OB_SUCCESS;
    if (OB_INVALID_TENANT_ID != schema_info.get_tenant_id()
        && schema_info.get_schema_version() > 0
        && set_received_schema_version
        && OB_TMP_FAIL(schema_mgr_->set_tenant_received_broadcast_version(
           schema_info.get_tenant_id(), schema_info.get_schema_version()))) {
      LOG_WARN("fail to set tenant received broadcast version", K(tmp_ret), K(schema_info));
    }

    const bool did_retry = true;
    ObServerSchemaTask refresh_task(ObServerSchemaTask::REFRESH, did_retry, schema_info);
    if (OB_FAIL(task_queue_.add(refresh_task))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("schedule fetch new schema task failed", KR(ret), K(schema_info));
      }
    } else {
      LOG_INFO("schedule fetch new schema task", KR(ret), K(schema_info));
    }
  }
  return ret;
}

int ObServerSchemaUpdater::try_release_schema()
{
  int ret = OB_SUCCESS;
  ObServerSchemaTask release_task(ObServerSchemaTask::RELEASE);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob_server_schema_updeter is not inited.", KR(ret));
  } else if (OB_FAIL(task_queue_.add(release_task))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("schedule release schema task failed", KR(ret));
    }
  } else {
    LOG_INFO("schedule release schema task", KR(ret));
  }
  return ret;
}

int ObServerSchemaUpdater::async_refresh_schema(
    const uint64_t tenant_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObServerSchemaTask refresh_task(ObServerSchemaTask::ASYNC_REFRESH,
                                  tenant_id, schema_version);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob_server_schema_updeter is not inited.", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(task_queue_.add(refresh_task))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("schedule async refresh schema task failed",
               KR(ret), K(tenant_id), K(schema_version));
    }
  } else {
    LOG_INFO("schedule async refresh schema task",
             KR(ret), K(tenant_id), K(schema_version));
  }
  return ret;
}

int ObServerSchemaUpdater::try_load_baseline_schema_version_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_ISNULL(schema_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_ is NULL", KR(ret));
  }

  {
    ObSchemaGetterGuard guard;
    if (FAILEDx(schema_mgr_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
      LOG_WARN("fail to get schema guard", KR(ret));
    } else if (OB_FAIL(guard.get_available_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get avaliable tenant ids", KR(ret));
    }
  }

  int64_t timeout = GCONF.rpc_timeout;
  int64_t baseline_schema_version = OB_INVALID_VERSION; // not used
  FOREACH_X(tenant_id, tenant_ids, OB_SUCC(ret)) { // ignore ret
    int tmp_ret = OB_SUCCESS;
    ObTimeoutCtx ctx;
    if (OB_TMP_FAIL(ctx.set_timeout(timeout))) {
      LOG_WARN("fail to set timeout", KR(tmp_ret), K(*tenant_id), K(timeout));
    } else if (OB_TMP_FAIL(schema_mgr_->get_baseline_schema_version(
      *tenant_id, true/*auto_update*/, baseline_schema_version))) {
      LOG_WARN("fail to update baseline schema version", KR(tmp_ret), K(*tenant_id));
    }
  }
  return ret;
}
}
}
