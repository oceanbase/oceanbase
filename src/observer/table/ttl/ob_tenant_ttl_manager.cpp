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

#include "observer/table/ttl/ob_tenant_ttl_manager.h"
#include "share/ob_max_id_fetcher.h"
#include "share/table/ob_ttl_util.h"
#include "lib/oblog/ob_log_module.h"
#include "share/table/ob_table_config_util.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{

namespace table
{

void ObClearTTLHistoryTask::runTimerTask()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (!ObKVFeatureModeUitl::is_ttl_enable()) {
    // do nothing
    LOG_DEBUG("ttl is disable");
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob clear ttl history task is not init", KR(ret));
  } else if (is_paused_) {
    // timer paused, do nothing
  } else if (ObTTLUtil::check_can_do_work()) {
    int ret = OB_SUCCESS;
    const int64_t now = ObTimeUtility::current_time();
    ObSqlString sql;
    if (tenant_id_ == OB_SYS_TENANT_ID) {
    } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
      // do nothinig
    } else {
      sql.reuse();
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      if (!tenant_config.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail get tenant_config", KR(ret), K(tenant_id_));
      } else {
        int64_t delete_timestamp = now - tenant_config->kv_ttl_history_recycle_interval;
        int64_t affect_rows = 0;

        if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE task_update_time < %ld LIMIT %ld ",
                                          share::OB_ALL_KV_TTL_TASK_HISTORY_TNAME,
                                          delete_timestamp,
                                          OB_KV_TTL_GC_COUNT_PER_TASK))) {
          LOG_WARN("fail to assign fmt sql string", KR(ret));
        } else if (OB_FAIL(sql_proxy_->write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affect_rows))) {
          LOG_WARN("fail to execute sql", KR(ret), K(sql));
        } else {
          LOG_DEBUG("success to execute sql", KR(ret), K(sql));
        }
      }
    }
  }
}

int ObClearTTLHistoryTask::init(const uint64_t tenant_id, common::ObMySQLProxy &sql_proxy)
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
    is_inited_ = true;
  }
  return ret;
}

void ObClearTTLHistoryTask::resume()
{
  is_paused_ = false;
}

void ObClearTTLHistoryTask::pause()
{
  is_paused_ = true;
}

int ObTTLTaskScheduler::init(const uint64_t tenant_id, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ttl task scheduler init twice", KR(ret));
  } else {
    tablet_table_pairs_.set_attr(ObMemAttr(tenant_id, "TTLTabletPairs"));
    sql_proxy_ = &sql_proxy;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObTTLTaskScheduler::reload_tenant_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl tenant task mgr not init", KR(ret));
  } else if (ATOMIC_BCAS(&need_reload_, true, false)) {
    lib::ObMutexGuard guard(mutex_);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObTTLStatusField table_id_field;
      table_id_field.field_name_ = ObString("table_id");
      table_id_field.type_ = ObTTLStatusField::UINT_TYPE;
      table_id_field.data_.uint_ = OB_INVALID_ID;

      ObTTLStatusField partition_id_field;
      partition_id_field.field_name_ = ObString("tablet_id");
      partition_id_field.type_ = ObTTLStatusField::UINT_TYPE;
      partition_id_field.data_.uint_ = OB_INVALID_ID;

      ObTTLStatusField tenant_id_field;
      tenant_id_field.field_name_ = ObString("tenant_id");
      tenant_id_field.type_ = ObTTLStatusField::UINT_TYPE;
      tenant_id_field.data_.uint_ = tenant_id_;

      ObTTLStatusFieldArray filters;
      ObTTLStatusArray ttl_task_arr;
      if (OB_FAIL(filters.push_back(table_id_field)) ||
          OB_FAIL(filters.push_back(partition_id_field)) ||
          OB_FAIL(filters.push_back(tenant_id_field))) {
        LOG_WARN("fail to push back field into array", KR(ret));
      } else if (OB_FAIL(ObTTLUtil::read_ttl_tasks(tenant_id_,
                                                   share::OB_ALL_KV_TTL_TASK_TNAME,
                                                   *sql_proxy_, filters, ttl_task_arr))) {
        LOG_WARN("fail to read ttl tasks status", KR(ret));
      } else if (ttl_task_arr.empty()) {
        tenant_task_.reset();
      } else if (ttl_task_arr.size() == 1) {
        ObTTLStatus &task = ttl_task_arr.at(0);
        tenant_task_.ttl_status_ = task;
        tenant_task_.is_finished_ = false;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected tenant ttl task count", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      FLOG_INFO("reload tenant task", K_(tenant_task));
    } else {
      ret = OB_EAGAIN;
      ATOMIC_STORE(&need_reload_, true);
      LOG_WARN("fail to reload tenant task", KR(ret), K_(tenant_task));
    }
  }
  return ret;
}

ObTTLTaskStatus ObTTLTaskScheduler::next_status(int64_t curr)
{
  ObTTLTaskStatus next = ObTTLTaskStatus::OB_TTL_TASK_INVALID;
  ObTTLTaskStatus curr_status = EVAL_TASK_PURE_STATUS(curr);
  if (curr_status == ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE ||
      curr_status == ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL) {
    next = ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE;
  } else if (curr_status == ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE ||
             curr_status == ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND) {
    next = curr_status;
  }
  return next;
}

int ObTTLTaskScheduler::calc_next_task_state(ObTTLTaskType user_cmd_type,
                                             ObTTLTaskStatus curr_state,
                                             ObTTLTaskStatus &next_state)
{
  int ret = OB_SUCCESS;
  if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND &&
      user_cmd_type == ObTTLTaskType::OB_TTL_RESUME) {
    next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE;
  } else if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE &&
      user_cmd_type == ObTTLTaskType::OB_TTL_SUSPEND) {
    next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND;
  } else if (curr_state != ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL &&
      user_cmd_type == ObTTLTaskType::OB_TTL_CANCEL) {
    next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL;
  } else {
    ret = OB_TTL_CMD_NOT_ALLOWED;
    const char *status_cstr = ObTTLUtil::get_ttl_tenant_status_cstr(curr_state);
    LOG_USER_ERROR(OB_TTL_CMD_NOT_ALLOWED, status_cstr);
    LOG_WARN("ttl command is not allowed in current tenant ttl status",
      KR(ret), K(curr_state), K(user_cmd_type), K_(tenant_id));
  }
  return ret;
}

int ObTTLTaskScheduler::add_ttl_task(ObTTLTaskType task_type)
{
  int ret = OB_SUCCESS;
  TRIGGER_TYPE trigger_type = TRIGGER_TYPE::USER_TRIGGER;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl tenant task mgr not init", KR(ret));
  } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
    ret = OB_TTL_TENANT_IS_RESTORE;
    LOG_USER_ERROR(OB_TTL_TENANT_IS_RESTORE);
    LOG_WARN("cannot execute ttl task during tenant is restoring", K_(tenant_id), KR(ret));
  } else if (OB_FAIL(reload_tenant_task())) {
    LOG_WARN("fail to reload tenant task", KR(ret));
  } else {
    lib::ObMutexGuard guard(mutex_);
    ObTTLTaskStatus next_state = OB_TTL_TASK_INVALID;
    ObTTLTaskStatus curr_state = EVAL_TASK_PURE_STATUS(tenant_task_.ttl_status_.status_);
    LOG_INFO("add ttl task old task", K(tenant_task_.ttl_status_), K(tenant_task_.is_finished_));
    if (!tenant_task_.is_finished_) {
      if (OB_FAIL(calc_next_task_state(task_type, curr_state, next_state))) {
        LOG_WARN("fail to apply user cmd on current ttl tasks", KR(ret));
      } else {
        LOG_INFO("user task results:", K(task_type), K(curr_state), K(next_state));
        if (next_state == curr_state) {
          // duplicate request, do nothing
        } else if (next_state != curr_state) {
          // can transform to next state
          if (OB_FAIL(update_task_status(tenant_task_.ttl_status_.task_id_,
                                         static_cast<int64_t>(next_state), *sql_proxy_))) {
            LOG_WARN("fail to update ttl tasks", KR(ret));
          } else {
            tenant_task_.ttl_status_.status_ = static_cast<int64_t>(next_state);
          }
        }
      }
    } else {
      // not task or task finished already, only accept trigger command
      if (task_type != ObTTLTaskType::OB_TTL_TRIGGER) {
        ret = OB_TTL_NO_TASK_RUNNING;
        LOG_USER_ERROR(OB_TTL_NO_TASK_RUNNING);
        LOG_WARN("not ttl task running currently, only trigger command is supported",
          KR(ret), K(task_type), K(curr_state));
      } else if (OB_FAIL(add_ttl_task_internal(TRIGGER_TYPE::USER_TRIGGER))) {
        LOG_WARN("fail to add ttl task", KR(ret), K_(tenant_id));
      }
    }
  }

  return ret;
}


int ObTTLTaskScheduler::add_ttl_task_internal(TRIGGER_TYPE trigger_type)
{
  int ret = OB_SUCCESS;
  bool is_active_time = false;
  bool enable_ttl = ObTTLUtil::is_enable_ttl(tenant_id_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl tenant task mgr not init", KR(ret));
  } else if (!enable_ttl) {
    ret = OB_TTL_NOT_ENABLE;
    LOG_USER_ERROR(OB_TTL_NOT_ENABLE);
    LOG_WARN("ttl is not enable currently", KR(ret), K_(tenant_id));
  } else if (!tenant_task_.is_finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("there is ttl task executing already", KR(ret));
  } else if (OB_FAIL(fetch_ttl_task_id(tenant_id_, tenant_task_.ttl_status_.task_id_))) {
    LOG_WARN("fail to fetch ttl task id", KR(ret));
  } else {
    int64_t cur_time = ObTimeUtility::current_time();
    tenant_task_.ttl_status_.task_start_time_ = cur_time;
    tenant_task_.ttl_status_.task_update_time_ = cur_time;
    tenant_task_.ttl_status_.tenant_id_ = tenant_id_;
    tenant_task_.ttl_status_.trigger_type_ = static_cast<int64_t>(trigger_type);
    tenant_task_.ttl_status_.status_ = static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE);
    tenant_task_.is_finished_ = false;
    if (OB_FAIL(insert_tenant_task(tenant_task_.ttl_status_))) {
      LOG_WARN("fail to insert ttl task into __all_ttl_task_status.", KR(ret));
      tenant_task_.reset();
    }
  }
  return ret;
}

int ObTTLTaskScheduler::insert_tenant_task(ObTTLStatus& ttl_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl tenant task mgr not init", KR(ret));
  } else if (OB_FAIL(ObTTLUtil::insert_ttl_task(ttl_task.tenant_id_,
    share::OB_ALL_KV_TTL_TASK_TNAME, *sql_proxy_, ttl_task))) {
    LOG_WARN("fail to insert tenant status.", KR(ret));
  }
  LOG_INFO("finish insert tenant ttl task", KR(ret), K(ttl_task));
  return ret;
}

int ObTTLTaskScheduler::update_task_status(uint64_t task_id,
                                           int64_t status,
                                           common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObTTLStatusKey key(tenant_id_, OB_INVALID_ID, OB_INVALID_ID, task_id);
  ObTTLStatusFieldArray update_fields;

  ObTTLStatusField status_field;
  status_field.field_name_ = ObString("status");
  status_field.type_ = ObTTLStatusField::INT_TYPE;
  status_field.data_.int_ = status;

  ObTTLStatusField update_time_field;
  update_time_field.field_name_ = ObString("task_update_time");
  update_time_field.type_ = ObTTLStatusField::INT_TYPE;
  update_time_field.data_.int_ = static_cast<int64_t>(ObTimeUtility::current_time());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl tenant task mgr not init", KR(ret));
  } else if (OB_FAIL(update_fields.push_back(status_field)) ||
      OB_FAIL(update_fields.push_back(update_time_field))) {
    LOG_WARN("fail to push back update fields.", KR(ret));
  } else {
    if (OB_FAIL(ObTTLUtil::update_ttl_task(tenant_id_,
                                           share::OB_ALL_KV_TTL_TASK_TNAME,
                                           proxy,
                                           key,
                                           update_fields))) {
      LOG_WARN("fail to update ttl task status.", KR(ret), K(tenant_id_), K(task_id), K(status));
    } else {
      LOG_DEBUG("success to update ttl tasks status", KR(ret), K(tenant_id_), K(task_id), K(status));
    }
  }

  return ret;
}

int ObTTLTaskScheduler::delete_task(const uint64_t tenant_id, const uint64_t task_id)
{
  int ret = OB_SUCCESS;
  ObTTLStatusKey key(tenant_id_, OB_INVALID_ID, OB_INVALID_ID, task_id);
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl tenant task mgr not init", KR(ret));
  } else if (OB_FAIL(ObTTLUtil::delete_ttl_task(tenant_id,
                                        share::OB_ALL_KV_TTL_TASK_TNAME,
                                        *sql_proxy_, key, affected_rows))) {
    LOG_WARN("fail to delete ttl tasks status", KR(ret), K_(tenant_id), K(task_id));
  } else {
    LOG_DEBUG("success to delete ttl tasks status", KR(ret), K_(tenant_id), K(task_id), K(affected_rows));
  }

  return ret;
}

int ObTTLTaskScheduler::in_active_time(bool& is_active_time)
{
  int ret = OB_SUCCESS;
  is_active_time = false;
  ObTTLDutyDuration duration;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));

  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObTTLUtil::parse(tenant_config->kv_ttl_duty_duration, duration))) {
    LOG_WARN("fail parse ttl dury duration", KR(ret));
  } else if (ObTTLUtil::current_in_duration(duration)) {
    is_active_time = true;
  }

  return ret;
}

int ObTTLTaskScheduler::try_add_periodic_task()
{
  int ret = OB_SUCCESS;
  TRIGGER_TYPE trigger_type = TRIGGER_TYPE::PERIODIC_TRIGGER;
  bool is_active_time = false;
  bool enable_ttl = ObTTLUtil::is_enable_ttl(tenant_id_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl tenant task mgr not init", KR(ret));
  } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cann't process ttl task, maybe tenant is restoring", K_(tenant_id), KR(ret));
  } else if (need_skip_run()) {
    ret = OB_EAGAIN;
    FLOG_INFO("exit timer task once cuz leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));
  } else if (!enable_ttl) {
    // do nothing
  } else if (OB_FAIL(in_active_time(is_active_time))) {
    LOG_WARN("fail to check is in active time", KR(ret));
  } else if (is_active_time) {
    if (!periodic_launched_) {
      lib::ObMutexGuard guard(mutex_);
      if (tenant_task_.is_finished_ && OB_FAIL(add_ttl_task_internal(TRIGGER_TYPE::PERIODIC_TRIGGER))) {
        LOG_WARN("fail to add ttl task", KR(ret), K_(tenant_id));
      } else {
        periodic_launched_ = true;
      }
    }
  } else {
    periodic_launched_ = false;
  }
  return ret;
}

int ObTTLTaskScheduler::check_all_tablet_task()
{
  int ret = OB_SUCCESS;
  bool need_move = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl tenant task mgr not init", KR(ret));
  } else if (tenant_task_.ttl_status_.status_ == OB_TTL_TASK_INVALID || tenant_task_.ttl_status_.status_ == OB_RS_TTL_TASK_SUSPEND) {
    // do nothing
  } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
    // do nothing
  } else if (OB_FAIL(check_task_need_move(need_move))) {
    LOG_WARN("fail to check task need move", KR(ret), K_(tenant_id));
  } else if (need_move) {
    {
      lib::ObMutexGuard guard(mutex_);
      if (need_skip_run()) {
        ret = OB_EAGAIN;
        FLOG_INFO("exit timer task once cuz leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));
      } else if (ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND == tenant_task_.ttl_status_.status_) {
        ret = OB_EAGAIN;
        LOG_WARN("task status changed during check task need move", KR(ret));
      } else if (ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE != tenant_task_.ttl_status_.status_) {
        tenant_task_.ttl_status_.status_ = static_cast<uint64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE);
        ObMySQLTransaction trans;
        if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
          LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));
        } else if (OB_FAIL(update_task_status(tenant_task_.ttl_status_.task_id_, OB_RS_TTL_TASK_MOVE, trans))) {
          LOG_WARN("fail to update task status", KR(ret));
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

    if (OB_SUCC(ret)) {
      if (OB_FAIL(move_all_task_to_history_table())) {
        LOG_WARN("fail to move all tasks to history table", KR(ret), K_(tenant_id), K(tenant_task_.ttl_status_.table_id_));
      } else {
        tenant_task_.reset();
        FLOG_INFO("tenant task is finished", K_(tenant_task));
      }
    }
  }

  return ret;
}

int ObTTLTaskScheduler::check_one_tablet_task(common::ObISQLClient &sql_client,
                                              const uint64_t table_id,
                                              const ObTabletID tablet_id,
                                              bool &is_finished)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  is_finished = false;
  SMART_VAR(ObISQLClient::ReadResult, res) {
    sqlclient::ObMySQLResult* result = nullptr;
    if (OB_FAIL(sql.append_fmt(
        "SELECT count(*) as cnt FROM %s WHERE tenant_id = %ld and task_id = %ld and "
        " table_id = %ld and tablet_id = %ld and (status = %ld or status = %ld) ",
        OB_ALL_KV_TTL_TASK_TNAME,
        tenant_id_,
        tenant_task_.ttl_status_.task_id_,
        table_id,
        tablet_id.id(),
        static_cast<int64_t>(ObTTLTaskStatus::OB_TTL_TASK_FINISH),
        static_cast<int64_t>(ObTTLTaskStatus::OB_TTL_TASK_CANCEL)))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(sql_client.read(res, gen_meta_tenant_id(tenant_id_), sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K_(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get mysql result failed", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next result", KR(ret), K(sql));
    } else {
      int64_t cnt = 0;
      EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "cnt", cnt, int64_t,
          true/*skip_null_error*/, false/*skip_column_error*/, 0/*default value*/);
      if (OB_SUCC(ret) and (1 == cnt)) {
        is_finished = true;
      }
    }
  }
  return ret;
}

int ObTTLTaskScheduler::fetch_ttl_task_id(uint64_t tenant_id, int64_t &new_task_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl tenant task mgr not init", KR(ret));
  } else {
    uint64_t tmp_task_id = OB_INVALID_ID;
    share::ObMaxIdFetcher id_fetcher(*sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(tenant_id,
                                            share::OB_MAX_USED_TTL_TASK_ID_TYPE,
                                            tmp_task_id, 0))) {
      LOG_WARN("fail to fetch new ttl task id", KR(ret), "id_type", share::OB_MAX_USED_TTL_TASK_ID_TYPE);
    } else {
      new_task_id = tmp_task_id;
    }
  }
  return ret;
}

void ObTTLTaskScheduler::reset_local_tenant_task()
{
  lib::ObMutexGuard guard(mutex_);
  tenant_task_.reset();
}

void ObTTLTaskScheduler::resume()
{
 ATOMIC_STORE(&is_leader_, true);
 ATOMIC_STORE(&need_do_for_switch_, true);
}

void ObTTLTaskScheduler::pause()
{
  ATOMIC_STORE(&is_leader_, false);
  ATOMIC_STORE(&need_do_for_switch_, true);
}

int ObTenantTTLManager::init(const uint64_t tenant_id, ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant ttl mgr init twice", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TenantTTLManager, tg_id_))) {
    LOG_WARN("fail to init timer", KR(ret));
  } else if (OB_FAIL(task_scheduler_.init(tenant_id, sql_proxy))) {
    LOG_WARN("fail to init task scheduler", K(tenant_id));
  } else if (OB_FAIL(clear_ttl_history_task_.init(tenant_id, sql_proxy))) {
    LOG_WARN("fail to init clear history task", K(tenant_id));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    LOG_INFO("tenant ttl mgr is inited", K_(tenant_id));
  }
  return ret;
}

int ObTenantTTLManager::start()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("tenant ttl manager begin to start", K_(tenant_id));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (FALSE_IT(task_scheduler_.set_need_reload(true))) {
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("init ttl scheduler fail", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, task_scheduler_, SCHEDULE_PERIOD, true))) {
    LOG_WARN("fail to schedule ttl task scheduler", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, clear_ttl_history_task_,
              ObClearTTLHistoryTask::OB_KV_TTL_GC_INTERVAL, true))) {
    LOG_WARN("fail to start ttl clear history task", KR(ret));
  }
  FLOG_INFO("tenant ttl manager finish to start", KR(ret), K_(tenant_id));

  return ret;
}

void ObTenantTTLManager::wait()
{
  FLOG_INFO("tenant ttl manager start to wait", K_(tenant_id));
  TG_WAIT(tg_id_);
  FLOG_INFO("tenant ttl manager finish to wait", K_(tenant_id));
}

void ObTenantTTLManager::stop()
{
  FLOG_INFO("tenant ttl manager start to stop", K_(tenant_id));
  TG_STOP(tg_id_);
  FLOG_INFO("tenant ttl manager finish to stop", K_(tenant_id));
}

void ObTenantTTLManager::destroy()
{
  FLOG_INFO("tenant ttl manager start to destroy", K_(tenant_id));
  TG_DESTROY(tg_id_);
  FLOG_INFO("tenant ttl manager finish to destroy", K_(tenant_id));
}

void ObTTLTaskScheduler::runTimerTask()
{
  DEBUG_SYNC(BEFORE_TTL_SCHEDULER_RUN);
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (!ObKVFeatureModeUitl::is_ttl_enable()) {
    // do nothing
    LOG_DEBUG("ttl is disable");
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl task mgr not init", KR(ret));
  } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
    // do nothing
  } else if (ATOMIC_BCAS(&need_do_for_switch_, true, false)) {
    // need skip this round for waiting follower finish executing task
    if (is_leader_) {
      FLOG_INFO("need wait for switch leader, skip schedule once", K_(need_do_for_switch));
    }
  } else if (!is_leader_) {
    // timer paused, do nothing
  } else if (OB_FAIL(reload_tenant_task())) {
    LOG_WARN("fail to process tenant task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(try_add_periodic_task())) {
    LOG_WARN("fail to try add periodic task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_all_tablet_task())) {
    LOG_WARN("fail to check all tablet task", KR(ret), K_(tenant_id));
  }
  uint64_t current = ObTimeUtility::current_time();
  LOG_DEBUG("runTimerTask", KR(ret), K_(tenant_id), K(current));
}

int ObTenantTTLManager::handle_user_ttl(const obrpc::ObTTLRequestArg& arg)
{
  int ret = OB_SUCCESS;
  ObTTLTaskType user_ttl_req_type = static_cast<ObTTLTaskType>(arg.cmd_code_);
  bool enable_ttl = ObTTLUtil::is_enable_ttl(tenant_id_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (tenant_id_ == OB_SYS_TENANT_ID) {
    // do nothing
  } else if (!enable_ttl) {
    ret = OB_TTL_NOT_ENABLE;
    LOG_USER_ERROR(OB_TTL_NOT_ENABLE);
    LOG_WARN("ttl is not enable currently", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(task_scheduler_.add_ttl_task(static_cast<ObTTLTaskType>(arg.cmd_code_)))) {
    LOG_WARN("fail to add ttl task", KR(ret), K_(tenant_id), K(user_ttl_req_type));
  }

  FLOG_INFO("finish handle user ttl cmd", KR(ret), K(arg), K_(tenant_id));
  return ret;
}

int ObTTLTaskScheduler::move_all_task_to_history_table()
{
  int ret = OB_SUCCESS;
  int64_t one_move_rows = TBALET_CHECK_BATCH_SIZE;
  while (OB_SUCC(ret) && one_move_rows == TBALET_CHECK_BATCH_SIZE) {
    ObMySQLTransaction trans;
    if (need_skip_run()) {
      ret = OB_EAGAIN;
      FLOG_INFO("exit timer task once cuz leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));
    } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
      LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObTTLUtil::move_task_to_history_table(tenant_id_, tenant_task_.ttl_status_.task_id_,
                                                             trans, TBALET_CHECK_BATCH_SIZE, one_move_rows))) {
      LOG_WARN("fail to move task to history table", KR(ret), K_(tenant_id));
    }

    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObMySQLTransaction trans;
    if (need_skip_run()) {
      ret = OB_EAGAIN;
      FLOG_INFO("exit timer task once cuz leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));
    } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
      LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(update_task_status(tenant_task_.ttl_status_.task_id_, OB_TTL_TASK_FINISH, trans))) {
      LOG_WARN("fail to update task status", KR(ret));
    } else if (OB_FAIL(ObTTLUtil::move_tenant_task_to_history_table(tenant_id_, tenant_task_.ttl_status_.task_id_,
                                                                    trans))) {
      LOG_WARN("fail to move tenant task to history table", KR(ret), K_(tenant_id));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }

  return ret;
}

void ObTenantTTLManager::resume()
{
  clear_ttl_history_task_.resume();
  task_scheduler_.resume();
  task_scheduler_.set_need_reload(true);
}

void ObTenantTTLManager::pause()
{
  clear_ttl_history_task_.pause();
  task_scheduler_.pause();
}

int ObTTLTaskScheduler::check_task_need_move(bool &need_move)
{
  int ret = OB_SUCCESS;
  need_move = false;
  if (OB_RS_TTL_TASK_MOVE == tenant_task_.ttl_status_.status_ || OB_RS_TTL_TASK_CANCEL == tenant_task_.ttl_status_.status_) {
    // cancel will also need move all tasks into history table now
    need_move = true;
  } else if (OB_FAIL(check_all_tablet_finished(need_move))) {
    LOG_WARN("fail to check all tablet task finished", KR(ret));
  }
  return ret;
}

int ObTTLTaskScheduler::check_all_tablet_finished(bool &all_finished)
{
  DEBUG_SYNC(BEFORE_CHECK_TTL_TASK_FINISH);
  int ret = OB_SUCCESS;
  all_finished = true;
  ObSEArray<uint64_t, DEFAULT_TABLE_ARRAY_SIZE> table_id_array;
  if (OB_FAIL(ObTTLUtil::get_tenant_table_ids(tenant_id_, table_id_array))) {
    LOG_WARN("fail to get tenant table ids", KR(ret), K_(tenant_id));
  } else if (!table_id_array.empty() && OB_FAIL(tablet_table_pairs_.reserve(DEFAULT_TABLET_PAIR_SIZE))) {
    LOG_WARN("fail to reserve", KR(ret));
  }

  // 2. get all ttl table ids
  int64_t start_idx = 0;
  int64_t end_idx = 0;
  while (OB_SUCC(ret) && start_idx < table_id_array.count() && all_finished) {
    {
      // temp schema guard to loop tablet table ids
      ObSchemaGetterGuard schema_guard;
      start_idx = end_idx;
      end_idx = MIN(table_id_array.count(), start_idx + TBALE_CHECK_BATCH_SIZE);
      bool is_ttl_table = false;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
      }
      for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx; ++idx) {
        const int64_t table_id = table_id_array.at(idx);
        const ObTableSchema *table_schema = nullptr;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
          LOG_WARN("fail to get simple schema", KR(ret), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table schema is null", KR(ret), K(table_id));
        } else if (OB_FAIL(ObTTLUtil::check_is_ttl_table(*table_schema, is_ttl_table))) {
          LOG_WARN("fail to check is ttl table", KR(ret));
        } else if (is_ttl_table) {
          ObArray<ObTabletID> tablet_ids;
          if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
            LOG_WARN("fail to get tablet ids", KR(ret), K(table_id));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
              ObTabletTablePair cur_pair;
              ObTabletID tablet_id = tablet_ids[i];
              if (OB_FAIL(cur_pair.init(tablet_id, table_id))) {
                LOG_WARN("fail to init tablet_ls_pair", KR(ret), K(i), K(tablet_id), K(table_id));
              } else if (OB_FAIL(tablet_table_pairs_.push_back(cur_pair))) {
                LOG_WARN("fail to push back pair", KR(ret), K(cur_pair));
              }
            }
          }
        }
      }
    } // end scope
    if (OB_SUCC(ret) && OB_FAIL(check_tablet_table_finished(tablet_table_pairs_, all_finished))) {
      LOG_WARN("fail to check tablet table finished", KR(ret));
    }
    tablet_table_pairs_.reuse();
  }
  tablet_table_pairs_.reset();

  return ret;
}

int ObTTLTaskScheduler::check_tablet_table_finished(ObIArray<ObTabletTablePair> &pairs, bool &all_finished)
{
  int ret = OB_SUCCESS;
  int64_t start_idx = 0;
  int64_t end_idx = 0;
  while (OB_SUCC(ret) && start_idx < pairs.count() && all_finished) {
    start_idx = end_idx;
    end_idx = MIN(pairs.count(), start_idx + TBALET_CHECK_BATCH_SIZE);
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
      LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));
    } else {
      for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx && all_finished; ++idx) {
        const int64_t table_id = pairs.at(idx).get_table_id();
        const ObTabletID tablet_id = pairs.at(idx).get_tablet_id();
        if (OB_FAIL(check_one_tablet_task(trans, table_id, tablet_id, all_finished))) {
          LOG_WARN("fail to check tablet task", KR(ret), K(table_id), K(tablet_id));
        }
      }
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

} // end namespace table
} // end namespace oceanbase