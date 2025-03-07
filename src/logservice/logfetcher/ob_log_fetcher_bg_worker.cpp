/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetcher_bg_worker.h"
#include "ob_log_fetcher.h"

namespace oceanbase
{
namespace logfetcher
{

void GetServerVersionTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t record_count;
  uint64_t version = 0;
  ObString version_str;
  const char *select_field = "VALUE";
  const char *condition_name = MIN_OBSERVER_VERSION;
  SMART_VAR(ObISQLClient::ReadResult, result) {
    if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT %s FROM %s WHERE NAME = '%s'",
        select_field, sql_proxy_.is_oracle_mode() ? OB_GV_OB_PARAMETERS_ORA_TNAME : OB_GV_OB_PARAMETERS_TNAME,
            condition_name))) {
      LOG_WARN("assign sql string failed", KR(ret), K(source_tenant_id_));
    } else if (OB_FAIL(sql_proxy_.read(result, sql.ptr()))) {
      LOG_WARN("failed to execute read", K(source_tenant_id_), "sql", sql.ptr());
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get mysql result failed", KR(ret));
    } else {
      common::sqlclient::ObMySQLResult &mysql_result = *result.get_result();
      while (OB_SUCC(ret)) {
        if (OB_FAIL(mysql_result.next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to iterate mysql_result");
          }
        } else {
          uint64_t tmp_version = 0;
          (void) GET_COL_IGNORE_NULL(mysql_result.get_varchar, "VALUE", version_str);
          if (OB_FAIL(ObClusterVersion::get_version(version_str, tmp_version))) {
            LOG_WARN("failed to parse cluster version", K(version_str));
          } else if (0 == version || tmp_version < version) {
            version = tmp_version;
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }

      if (OB_SUCC(ret)) {
        if (0 != version) {
          fetcher_.set_source_min_observer_version(version);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if ((version >= MOCK_CLUSTER_VERSION_4_2_4_0 && version < CLUSTER_VERSION_4_3_0_0) ||
        version >= CLUSTER_VERSION_4_3_4_0) {
      TG_CANCEL_TASK(timer_id_, *this);
      LOG_INFO("find compatible min_observer_verion, no need to schedule task", K(version), K(version_str));
    } else {
      LOG_INFO("find lower version", K(version), K(version_str));
    }
  }
}

void UpdateProtoTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  const ObLogFetcherConfig *cfg = nullptr;
  const uint64_t source_min_observer_version = fetcher_.get_source_min_observer_version();
  if (OB_FAIL(fetcher_.get_fetcher_config(cfg))) {
    LOG_WARN("failed to get fetcher config");
  } else if (OB_ISNULL(cfg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null config from fetcher");
  } else if (cfg->logfetcher_parallel_log_transport &&
      ((source_min_observer_version >= MOCK_CLUSTER_VERSION_4_2_4_0 && source_min_observer_version < CLUSTER_VERSION_4_3_0_0) ||
       source_min_observer_version >= CLUSTER_VERSION_4_3_4_0)) {
    fetcher_.update_fetch_log_protocol(obrpc::ObCdcFetchLogProtocolType::RawLogDataProto);
    LOG_INFO("update fetch log protocol to RawLogDataProto",
        "logfetcher_parallel_log_transport", cfg->logfetcher_parallel_log_transport.get(),
        "source_min_observer_version", fetcher_.get_source_min_observer_version());
  } else {
    fetcher_.update_fetch_log_protocol(obrpc::ObCdcFetchLogProtocolType::LogGroupEntryProto);
    LOG_INFO("update fetch log protocol to LogGroupEntryProto",
        "logfetcher_parallel_log_transport", cfg->logfetcher_parallel_log_transport.get(),
        "source_min_observer_version", fetcher_.get_source_min_observer_version());
  }
}

void DataBufferRecycleTask::runTimerTask()
{
  buffer_pool_.try_recycle_expired_buffer();
}

//////////////////////////////// ObLogFetcherBGWorker ////////////////////////////////

ObLogFetcherBGWorker::ObLogFetcherBGWorker():
    is_inited_(false),
    timer_id_(-1),
    tenant_id_(OB_INVALID_TENANT_ID),
    source_tenant_id_(OB_INVALID_TENANT_ID),
    alloc_(),
    task_list_()
{
}

int ObLogFetcherBGWorker::init(const uint64_t tenant_id,
    const uint64_t source_tenant_id,
    ObLogFetcher &log_fetcher,
    ObISQLClient &sql_proxy,
    LogFileDataBufferPool &pool)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogFetcherBGWorker has been initialized", K(is_inited_));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::LogFetcherBGWorker, timer_id_))) {
    LOG_ERROR("failed to create LogFetcher Background Timer");
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    source_tenant_id_ = source_tenant_id;
    alloc_.set_attr(lib::ObMemAttr(tenant_id_, "LogFetchBGTask"));
    if (OB_FAIL(init_task_list_(log_fetcher, sql_proxy, pool))) {
      LOG_ERROR("failed to init bg task list", K(tenant_id_));
    } else {
      LOG_INFO("ObLogFetcherBGWorker finish to init", K(tenant_id_), K(source_tenant_id_),
          K(timer_id_), "task_count", task_list_.count());
    }
  }

  return ret;
}

int ObLogFetcherBGWorker::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcherBGWorker has not been initialized yet", K(is_inited_));
  } else if (OB_FAIL(TG_START(timer_id_))) {
    LOG_ERROR("ObLogFetcherBGWorker failed to start timer", K(timer_id_));
  } else if (OB_FAIL(schedule_task_list_())) {
    LOG_ERROR("ObLogFetcherBGWorker failed to schedule task_list_");
  } else {
    LOG_INFO("ObLogFetcherBGWorker start", K(timer_id_));
  }

  return ret;
}

void ObLogFetcherBGWorker::stop()
{
  TG_STOP(timer_id_);
}

void ObLogFetcherBGWorker::wait()
{
  TG_WAIT(timer_id_);
}

void ObLogFetcherBGWorker::destroy()
{
  if (IS_INIT) {
    TG_DESTROY(timer_id_);
    timer_id_ = -1;
    destroy_task_list_();
    alloc_.reset();
    tenant_id_ = OB_INVALID_TENANT_ID;
    source_tenant_id_ = OB_INVALID_TENANT_ID;
    is_inited_ = false;
  }
}

int ObLogFetcherBGWorker::init_task_list_(ObLogFetcher &log_fetcher,
    ObISQLClient &sql_proxy,
    LogFileDataBufferPool &pool)
{
  int ret = OB_SUCCESS;

  void *tmp_buf = nullptr;
  if (OB_ISNULL(tmp_buf = alloc_.alloc(sizeof(GetServerVersionTask)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc GetServerVersionTask");
  } else if (OB_FAIL(task_list_.push_back(ObLogFetcherBGTask(GetServerVersionTask::SCHEDULE_INTERVAL,
      GetServerVersionTask::NEED_REPEAT, new (tmp_buf) GetServerVersionTask(source_tenant_id_, timer_id_,
      log_fetcher, sql_proxy))))) {
    LOG_ERROR("failed to push back GetServerVersionTask");
  } else if (OB_ISNULL(tmp_buf = alloc_.alloc(sizeof(UpdateProtoTask)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc GetServerVersionTask");
  } else if (OB_FAIL(task_list_.push_back(ObLogFetcherBGTask(UpdateProtoTask::SCHEDULE_INTERVAL,
      UpdateProtoTask::NEED_REPEAT, new (tmp_buf) UpdateProtoTask(log_fetcher))))) {
    LOG_ERROR("failed to push back GetServerVersionTask");
  } else if (OB_ISNULL(tmp_buf = alloc_.alloc(sizeof(DataBufferRecycleTask)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc GetServerVersionTask");
  } else if (OB_FAIL(task_list_.push_back(ObLogFetcherBGTask(DataBufferRecycleTask::SCHEDULE_INTERVAL,
      DataBufferRecycleTask::NEED_REPEAT, new (tmp_buf) DataBufferRecycleTask(pool))))) {
    LOG_ERROR("failed to push back GetServerVersionTask");
  } else {
    LOG_INFO("ObLogFetcherBGWorker finished to init task list", "task_num", task_list_.count());
  }

  return ret;
}

int ObLogFetcherBGWorker::schedule_task_list_()
{
  int ret = OB_SUCCESS;
  run_tasks_once_();
  ARRAY_FOREACH(task_list_, idx) {
    ObLogFetcherBGTask &timer_task = task_list_.at(idx);
    if (OB_ISNULL(timer_task.get_task())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("timer task is null, unexpected", K(timer_task));
    } else if (OB_FAIL(TG_SCHEDULE(timer_id_, *timer_task.get_task(), timer_task.get_schedule_interval(),
        timer_task.need_repeat(), false))) {
      LOG_ERROR("failed to schedule timer task", K(timer_task));
    } else {
      // success
      LOG_INFO("LogFetcherBGWorker start to schedule a timer_task", K(idx), K(timer_task));
    }
  }
  return ret;
}

void ObLogFetcherBGWorker::run_tasks_once_()
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_NORET(task_list_, idx) {
    ObLogFetcherBGTask &timer_task = task_list_.at(idx);
    if (OB_ISNULL(timer_task.get_task())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("timer task is null, unexpected", K(timer_task));
    } else {
      timer_task.get_task()->runTimerTask();
      LOG_INFO("LogFetcherBGWorker finish running timer_task once at start", K(idx), K(timer_task));
    }
  }
}

void ObLogFetcherBGWorker::destroy_task_list_()
{
  ARRAY_FOREACH_NORET(task_list_, idx) {
    ObLogFetcherBGTask &timer_task = task_list_.at(idx);
    ObTimerTask *inner_task = timer_task.get_task();
    inner_task->~ObTimerTask();
    alloc_.free(inner_task);
    timer_task.reset();
  }
  task_list_.reset();
}

}
}