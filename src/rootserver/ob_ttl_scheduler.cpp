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

#include "rootserver/ob_ttl_scheduler.h"
#include "rootserver/ob_root_service.h"
#include "share/table/ob_ttl_util.h"
#include "share/ob_max_id_fetcher.h"

namespace oceanbase
{

namespace rootserver
{

ObClearTTLStatusHistoryTask::ObClearTTLStatusHistoryTask(ObRootService& rs)
  : root_service_(rs)
{
}

void ObClearTTLStatusHistoryTask::runTimerTask()
{
  if (ObTTLUtil::check_can_do_work()) {
    int ret = OB_SUCCESS;

    ObSchemaGetterGuard schema_guard;
    ObArray<uint64_t> tenant_ids;

    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_schema_guard(schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant ids", K(ret));
    } else {
      const int64_t now = ObTimeUtility::current_time();
      ObSqlString sql;

      for (size_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (tenant_id == OB_SYS_TENANT_ID) {
        } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id)) {
          // do nothinig
        } else {
          sql.reuse();
          omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
          if (!tenant_config.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail get tenant_config", K(ret), K(tenant_id));
          } else {
            int64_t delete_timestamp = now - tenant_config->kv_ttl_history_recycle_interval;
            int64_t affect_rows = 0;
            
            if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE gmt_create < usec_to_time(%ld) AND tenant_id = %ld LIMIT 2048 ",
                                              share::OB_ALL_KV_TTL_TASK_HISTORY_TNAME,
                                              delete_timestamp,
                                              tenant_id))) {
              LOG_WARN("fail to assign fmt sql string", K(ret));
            } else if (OB_FAIL(root_service_.get_sql_proxy().write(tenant_id, sql.ptr(), affect_rows))) {
              LOG_WARN("fail to execute sql", K(ret), K(sql));
            } else {
              LOG_DEBUG("success to execute sql", K(ret), K(sql));
            }
          }
        }
      }    
    }
  }
}

ObTTLTenantTaskMgr& ObTTLTenantTaskMgr::get_instance()
{
  static ObTTLTenantTaskMgr instance_;
  return instance_;
}

int ObTTLTenantTaskMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTTLTenantTaskMgr::read_tenant_status(uint64_t tenant_id, 
                                           ObTTLStatusArray& tenant_tasks)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    
    ObTTLStatusField table_id_field;
    table_id_field.field_name_ = ObString("table_id");
    table_id_field.type_ = ObTTLStatusField::UINT_TYPE;
    table_id_field.data_.uint_ = OB_INVALID_ID;

    ObTTLStatusField partition_id_field;
    partition_id_field.field_name_ = ObString("partition_id");
    partition_id_field.type_ = ObTTLStatusField::UINT_TYPE;
    partition_id_field.data_.uint_ = OB_INVALID_ID;

    ObTTLStatusField tenant_id_field;
    tenant_id_field.field_name_ = ObString("tenant_id");
    tenant_id_field.type_ = ObTTLStatusField::UINT_TYPE;
    tenant_id_field.data_.uint_ = tenant_id;

    ObTTLStatusFieldArray filters;

    if (OB_FAIL(filters.push_back(table_id_field)) ||
        OB_FAIL(filters.push_back(partition_id_field)) ||
        OB_FAIL(filters.push_back(tenant_id_field))) {
      LOG_WARN("fail to push back field into array", K(ret));
    } else if (OB_FAIL(ObTTLUtil::read_ttl_tasks(tenant_id,
                                                 share::OB_ALL_KV_TTL_TASK_TNAME,
                                                 *GCTX.sql_proxy_, filters, tenant_tasks))) {
      LOG_WARN("fail to read ttl tasks status", K(ret));
    }
  }

  return ret;
}

int ObTTLTenantTaskMgr::add_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (size_t i = 0; i < ten_task_arr_.count() && !found; ++i) {
    if (ten_task_arr_.at(i).tenant_id_ == tenant_id) {
      found = true;
    }
  }

  if (found) {
    ret = OB_TENANT_EXIST;
  } else {
    ObTTLTenantTask tenant_task;
    tenant_task.tenant_id_ = tenant_id;
    if (OB_FAIL(ten_task_arr_.push_back(tenant_task))) {
      LOG_WARN("fail to store tenant task", K(ret));
    } else {
      LOG_INFO("add tenant to tenant task array", K(tenant_id), K(tenant_task));
    }
  }
  return ret;
}

void ObTTLTenantTaskMgr::delete_tenant(uint64_t tenant_id)
{
  for (size_t i = 0; i < ten_task_arr_.count(); ++i) {
    ObTTLTenantTask& ten_task = ten_task_arr_.at(i);
    if (ten_task.tenant_id_ == tenant_id) {
      ten_task.reset();
      ten_task_arr_.remove(i);
      LOG_INFO("remove tennat task in tenant task array", K(tenant_id), K(ten_task_arr_));
      break;
    }
  }
}

ObTTLTaskStatus ObTTLTenantTaskMgr::next_status(int64_t curr)
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

int ObTTLTenantTaskMgr::user_cmd_upon_task(ObTTLTaskType task_type,
                                           ObTTLTaskStatus curr_state,
                                           ObTTLTaskStatus &next_state,
                                           bool &add_new_task)
{
  int ret = OB_SUCCESS;
  add_new_task = false;

  if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL) {
    next_state = curr_state;
    if (task_type == ObTTLTaskType::OB_TTL_RESUME || task_type == ObTTLTaskType::OB_TTL_SUSPEND) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("fail to modify ttl tasks status", K(ret), K(curr_state), K(task_type));
    } else if (task_type == ObTTLTaskType::OB_TTL_TRIGGER) {
      add_new_task = true;
    }
  } else if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND) {
    next_state = curr_state;
    if (task_type == ObTTLTaskType::OB_TTL_TRIGGER) {
      next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL;
      add_new_task = true;
    } else if (task_type == ObTTLTaskType::OB_TTL_CANCEL) {
      next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL;
    } else if (task_type == ObTTLTaskType::OB_TTL_RESUME) {
      next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE;
    }
  } else if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE) {
    next_state = curr_state;
    if (task_type == ObTTLTaskType::OB_TTL_TRIGGER) {
      next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL;
      add_new_task = true;
    } else if (task_type == ObTTLTaskType::OB_TTL_CANCEL) {
      next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL;
    } else if (task_type == ObTTLTaskType::OB_TTL_SUSPEND) {
      next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND;
    }
  } else if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE) {
      next_state = curr_state;
    if (task_type == ObTTLTaskType::OB_TTL_TRIGGER) {
      add_new_task = true;
    } else if (task_type == ObTTLTaskType::OB_TTL_CANCEL ||
                task_type == ObTTLTaskType::OB_TTL_SUSPEND ||
                task_type == ObTTLTaskType::OB_TTL_RESUME) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("fail to modify ttl tasks status, current task already finished", K(ret), K(curr_state), K(task_type));
    }
  }
  return ret;
}

int ObTTLTenantTaskMgr::add_ttl_task(uint64_t tenant_id,
                                     ObTTLTaskType task_type)
{
  int ret = OB_SUCCESS;
  ObTTLTenantTask* tenant_ptr = NULL;  
  TRIGGER_TYPE trigger_type = TRIGGER_TYPE::USER_TRIGGER;  

  lib::ObMutexGuard guard(mutex_);

  if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("check tenant, can't process ttl task", K(tenant_id), K(ret));
  } else if (OB_FAIL(refresh_tenant(tenant_id))) {
    LOG_WARN("fail to refresh tenant task", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_ptr(tenant_id, tenant_ptr))) {
    LOG_WARN("fail to get tenant task ptr", K(ret));
  } else {
    ObTTLTenantTask& tenant_ref = *tenant_ptr;
    size_t task_count = tenant_ref.tasks_.count();

    if (task_count > 0) {
      RsTenantTask& rs_task = tenant_ref.tasks_.at(task_count - 1);
      ObTTLTaskStatus next_state, curr_state;
      curr_state = EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_);
    
      LOG_INFO("add ttl task old task", K(rs_task.ttl_status_), K(task_count));
      if (task_count > 1) {
        /**
         * task_count == 2
         * first task must be in cancel state or move state
         * second task hasn't started, the new added task state should be OB_RS_TTL_TASK_CREATE
         *   consider the following :
         * there's a task in the queue and a new use trigger come, here should cancel the first task 
         * and add new task in queue, while canceling the first task a new user suspend task come
         * we change the second task status as OB_RS_TTL_TASK_SUSPEND
         * 
        */
        if (curr_state != ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE && 
            curr_state != ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("curr status not init", K(ret), K(curr_state), K(task_type), K(trigger_type));
        } else {
          /**
           * there's a task not started in the queue
           * if USER CMD is cancel, delete the second task directly
          */
          if (task_type == ObTTLTaskType::OB_TTL_CANCEL) {
            LOG_INFO("remove tasks", K(tenant_ref.tasks_.at(task_count - 1)));
            if (OB_FAIL(delete_task(tenant_id, rs_task.ttl_status_.task_id_))) {
              LOG_WARN("fail to delete ttl tasks status", K(ret));
            } else if (OB_FAIL(tenant_ref.tasks_.remove(task_count - 1))) {
              LOG_WARN("fail to remove ttl tasks status", K(ret));
            }
          } else {
            /**
             * there's a task not started in the queue
             * if USER CMD is TRIGGER or RESUME, set the task state as OB_RS_TTL_TASK_CREATE
             * or if USER CMD is SUSPEND, change the task state as SUSPEND
            */
            if (task_type == ObTTLTaskType::OB_TTL_TRIGGER || 
                task_type == ObTTLTaskType::OB_TTL_RESUME) {
              next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE;
            } else if (task_type == ObTTLTaskType::OB_TTL_SUSPEND) {
              next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND;
            }

            if (curr_state != next_state) {
              if (OB_FAIL(update_task_status(tenant_id, rs_task.ttl_status_.task_id_, static_cast<int64_t>(next_state), *GCTX.sql_proxy_))) {
                LOG_WARN("fail to update ttl tasks", K(ret));
              } else {
                // update memory status only
                rs_task.ttl_status_.status_ = static_cast<int64_t>(next_state);
              }
            }
          }
        }
      } else { // task_count == 1
        LOG_INFO("add ttl task old task", K(rs_task.ttl_status_), K(task_count));
        bool add_new_task = false;
        int64_t responsed = EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_);

        /**
         * the priodic may not switch task status in time 
         * here do a check is need switch to next status,
         * update the status of tasks
        */
        if (responsed || is_all_responsed(rs_task)) {
          // if all server responsed, switch to next state
          if (OB_FAIL(update_task_on_all_responsed(rs_task))) {
            LOG_WARN("fail to update ttl tasks", K(tenant_id), K(rs_task.ttl_status_.task_id_), K(task_type), K(curr_state));
          } else {
            curr_state = EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_);
            responsed = EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_);
            LOG_INFO("success update task status", K(tenant_id), K(rs_task.ttl_status_.task_id_), K(curr_state),  K(task_type));
          }
        }
        
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(user_cmd_upon_task(task_type, curr_state, next_state, add_new_task))) {
          LOG_WARN("fail to aaply user cmd on current ttl tasks", K(ret));
        } else {
          LOG_INFO("user task results:", K(task_type), K(curr_state), K(next_state), K(add_new_task));
          ObTTLTaskType ttl_task_type = ObTTLTaskType::OB_TTL_INVALID;
          ttl_task_type = eval_task_cmd_type(next_state);

          if (next_state == curr_state && responsed) {
          } else if (next_state != curr_state) {
            // update status
            if (OB_FAIL(update_task_status(tenant_id, rs_task.ttl_status_.task_id_,
                                           static_cast<int64_t>(next_state), *GCTX.sql_proxy_))) {
              LOG_WARN("fail to update ttl tasks", K(ret));
            } else {
              rs_task.all_responsed_ = false;
              rs_task.ttl_status_.status_ = static_cast<int64_t>(next_state);
              rs_task.set_servers_not_responsed();
            }
            
            if (OB_FAIL(ret)) {
            } else if (ttl_task_type != ObTTLTaskType::OB_TTL_INVALID && 
                       OB_FAIL(send_server_task_req(rs_task, ttl_task_type))) {
              LOG_WARN("fail to send server task ttl request", K(ret), K(ttl_task_type), K(next_state), K(curr_state));
            }
          }
          
          if (OB_SUCC(ret) && add_new_task &&
              OB_FAIL(add_ttl_task_internal(tenant_id, TRIGGER_TYPE::USER_TRIGGER, false))) {
            LOG_WARN("fail to add ttl task", K(ret), K(tenant_id));
          }
        }
      }
    } else if (task_count == 0) {
      if (task_type != ObTTLTaskType::OB_TTL_TRIGGER) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("fail to add ttl task as internal can't support current oper.", K(ret), K(task_type));
      } else if (OB_FAIL(add_ttl_task_internal(tenant_id, TRIGGER_TYPE::USER_TRIGGER, true))) {
        LOG_WARN("fail to add ttl task", K(ret), K(tenant_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task count", K(ret));
    }
  }

  if (ret == OB_EAGAIN) {
    // it's a success cmd, cannot return OB_EAGAIN to user 
    LOG_INFO("reset OB_EAGAIN to OB_SUCCESS, because ttl scheduler will resend later");
    ret = OB_SUCCESS;
  }

  return ret;
}


int ObTTLTenantTaskMgr::add_ttl_task_internal(uint64_t tenant_id,
                                              TRIGGER_TYPE trigger_type,
                                              bool sync_server)
{
  int ret = OB_SUCCESS;

  ObTTLTenantTask* tenant_ptr = NULL;
  bool is_active_time = false;

  if (OB_FAIL(get_tenant_ptr(tenant_id, tenant_ptr))) {
    LOG_WARN("fail to get tenant task ptr", K(ret));
  } else if (OB_FAIL(in_active_time(tenant_id, is_active_time))) {
    LOG_WARN("fail to eval active time", K(ret));
  } else {
    bool enable_ttl = is_enable_ttl(tenant_id);
    ObTTLTenantTask& tenant_ref = *tenant_ptr;
    size_t task_count = tenant_ref.tasks_.count();

    if (OB_FAIL(tenant_ref.tasks_.push_back(RsTenantTask()))) {
      LOG_WARN("fail to push back rs tenant task", K(ret));
    } else {
      RsTenantTask& new_task = tenant_ref.tasks_.at(task_count);
      if (OB_FAIL(fetch_ttl_task_id(tenant_id, new_task.ttl_status_.task_id_))) {
        LOG_WARN("fail to fetch ttl task id", K(ret));
        tenant_ref.tasks_.remove(task_count);
      } else {
        int64_t cur_time = ObTimeUtility::current_time();
        new_task.ttl_status_.task_start_time_ = cur_time;
        new_task.ttl_status_.task_update_time_ = cur_time;
        new_task.ttl_status_.tenant_id_ = tenant_id;
        new_task.ttl_status_.trigger_type_ = static_cast<int64_t>(trigger_type);
        new_task.ttl_status_.status_ = static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE);

        if (OB_FAIL(insert_tenant_task(new_task.ttl_status_))) {
          LOG_WARN("fail to insert ttl task into __all_ttl_task_status.", K(ret));
          tenant_ref.tasks_.remove(task_count);
        } else if (sync_server && OB_FAIL(send_server_task_req(new_task, ObTTLTaskType::OB_TTL_TRIGGER))) {
          LOG_WARN("fail to send server task ttl request", K(ret),
                     K(trigger_type), K(new_task.ttl_status_.task_id_));
        }
      }
    }
  }

  return ret;
}

bool ObTTLTenantTaskMgr::need_retry_task(RsTenantTask& rs_task)
{
  bool bool_ret = false;
  int64_t cur_time = ObTimeUtility::current_time();
  bool_ret = (cur_time - rs_task.ttl_status_.task_update_time_ >= OB_TTL_TASK_RETRY_INTERVAL) || 
             (rs_task.server_infos_.count() == 0);
  return bool_ret;
}

ObTTLTaskType ObTTLTenantTaskMgr::eval_task_cmd_type(ObTTLTaskStatus status)
{
  ObTTLTaskType task_type = ObTTLTaskType::OB_TTL_INVALID;
  if (status == ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL) {
    task_type = ObTTLTaskType::OB_TTL_CANCEL;
  } else if (status == ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE) {
    task_type = ObTTLTaskType::OB_TTL_TRIGGER;
  } else if (status == ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND) {
    task_type = ObTTLTaskType::OB_TTL_SUSPEND;
  } else if (status == ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE) {
    task_type = ObTTLTaskType::OB_TTL_MOVE;
  }
  
  return task_type;
}


int ObTTLTenantTaskMgr::process_tenant_tasks(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTTLTenantTask* tenant_ptr = NULL;
  bool is_active_time = false;
  bool enable_ttl = is_enable_ttl(tenant_id);
   
  lib::ObMutexGuard guard(mutex_);
  
  if (OB_FAIL(get_tenant_ptr(tenant_id, tenant_ptr))) {
    LOG_WARN("fail to get tenant task ptr", K(ret));
  } else if (OB_FAIL(in_active_time(tenant_id, is_active_time))) {
    LOG_WARN("fail to eval active time", K(ret));
  } else {
    ObTTLTenantTask& tenant_ref = *tenant_ptr;
    size_t task_count = tenant_ref.tasks_.count();

    if (task_count > 0) {
      RsTenantTask& cur_task = tenant_ref.tasks_.at(0);
      ObTTLTaskStatus next_state, curr_state;

      curr_state = EVAL_TASK_PURE_STATUS(cur_task.ttl_status_.status_);
      int64_t status_responsed = EVAL_TASK_RESPONSE(cur_task.ttl_status_.status_);
      ObTTLTaskType ttl_task_type = ObTTLTaskType::OB_TTL_INVALID;

      LOG_INFO("process_tenant_tasks begin", K(tenant_id), K(task_count), K(status_responsed),
                                             K(curr_state), K(task_count));

      if (task_count > 1) {
        if (!(curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL || 
            curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to eval active time", K(ret));
        } else {
          ttl_task_type = eval_task_cmd_type(curr_state);
          if (status_responsed) {
            next_state = next_status(curr_state);
            if (curr_state == next_state) {
              if (OB_FAIL(delete_task(tenant_id, cur_task.ttl_status_.task_id_))) {
                LOG_WARN("fail to store tenant task", K(ret));
              } else {
                LOG_INFO("delete finished task", K(cur_task.ttl_status_));
                tenant_ref.tasks_.remove(0);
              }
            }
          } else if (ttl_task_type != ObTTLTaskType::OB_TTL_INVALID &&
                     OB_FAIL(send_server_task_req(cur_task, ttl_task_type))) {
            LOG_WARN("fail to send server task ttl request", K(ret), K(ttl_task_type), K(curr_state));
          }
        }
      } else {
        LOG_INFO("process_tenant_tasks begin", K(tenant_id), K(task_count), K(status_responsed),
                                               K(curr_state), K(task_count));
        // task_count == 1
        if (status_responsed) {
          next_state = next_status(curr_state);
          if (next_state != curr_state) {
            ttl_task_type = eval_task_cmd_type(next_state);
          } else {
            if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND) {
            } else if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE) {
              if (!ObTTLTime::is_same_day(ObTimeUtility::current_time(), cur_task.ttl_status_.task_start_time_)) {
                if (OB_FAIL(delete_task(tenant_id, cur_task.ttl_status_.task_id_))) {
                  LOG_WARN("fail to store tenant task", K(ret));
                } else {
                  LOG_DEBUG("delete finished task", K(cur_task.ttl_status_));
                  tenant_ref.tasks_.remove(0);
                }
              }
            }
          }
        } else {
          next_state = curr_state;
          ttl_task_type = eval_task_cmd_type(next_state);
        }

        if (status_responsed) {
        } else if (cur_task.ttl_status_.trigger_type_ == TRIGGER_TYPE::USER_TRIGGER) {
        } else if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE) {
          if (!is_active_time || !enable_ttl) {
            ttl_task_type = ObTTLTaskType::OB_TTL_SUSPEND;
            next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND;
          }
        } else if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND) {
          if (is_active_time && enable_ttl) {
            ttl_task_type = ObTTLTaskType::OB_TTL_RESUME;
            next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE;
          }
        }

        if (OB_SUCC(ret) && curr_state != next_state) {
          if (OB_FAIL(update_task_status(tenant_id, cur_task.ttl_status_.task_id_,
                                                static_cast<int64_t>(next_state), *GCTX.sql_proxy_))) {
            LOG_WARN("fail to update ttl tasks", K(ret));
          } else {
            cur_task.all_responsed_ = false;
            cur_task.set_servers_not_responsed();
            cur_task.ttl_status_.status_ = static_cast<int64_t>(next_state);
          }
        }

        if (OB_SUCC(ret) && ttl_task_type != ObTTLTaskType::OB_TTL_INVALID &&
            (curr_state != next_state || need_retry_task(cur_task)) && 
            OB_FAIL(send_server_task_req(cur_task, ttl_task_type))) {
          LOG_WARN("fail to send server task ttl request", K(ret), K(ttl_task_type),
                  K(next_state), K(curr_state), K(cur_task.ttl_status_.task_id_));
        }
      }
    }
  }
  LOG_DEBUG("process_tenant_tasks end", K(ret), K(tenant_id));
  return ret;
}

int ObTTLTenantTaskMgr::insert_tenant_task(ObTTLStatus& ttl_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTTLUtil::insert_ttl_task(ttl_task.tenant_id_,
                                         share::OB_ALL_KV_TTL_TASK_TNAME,
                                         *GCTX.sql_proxy_, ttl_task))) {
    LOG_WARN("fail to insert tenant status.", K(ret));
  } else {
    LOG_DEBUG("success to insert ttl task", K(ttl_task));
  }

  return ret;
}

int ObTTLTenantTaskMgr::update_task_status(uint64_t tenant_id,
                                           uint64_t task_id,
                                           int64_t status,
                                           common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObTTLStatusKey key(tenant_id, OB_INVALID_ID, OB_INVALID_ID, task_id);
  ObTTLStatusFieldArray update_fields;

  ObTTLStatusField status_field;
  status_field.field_name_ = ObString("status");
  status_field.type_ = ObTTLStatusField::INT_TYPE;
  status_field.data_.int_ = status;

  ObTTLStatusField update_time_field;
  update_time_field.field_name_ = ObString("task_update_time");
  update_time_field.type_ = ObTTLStatusField::INT_TYPE;
  update_time_field.data_.int_ = static_cast<int64_t>(ObTimeUtility::current_time());

  if (OB_FAIL(update_fields.push_back(status_field)) ||
      OB_FAIL(update_fields.push_back(update_time_field))) {
    LOG_WARN("fail to push back update fields.", K(ret));
  } else {
    if (OB_FAIL(ObTTLUtil::update_ttl_task(tenant_id,
                                           share::OB_ALL_KV_TTL_TASK_TNAME,
                                           proxy,
                                           key,
                                           update_fields))) {
      LOG_WARN("fail to update ttl task status.", K(ret), K(tenant_id), K(task_id), K(status));
    } else {
      LOG_DEBUG("success to update ttl tasks status", K(ret), K(tenant_id), K(task_id), K(status));
    }
  }

  return ret;
}

int ObTTLTenantTaskMgr::delete_task(uint64_t tenant_id, uint64_t task_id)
{
  int ret = OB_SUCCESS;
  ObTTLStatusKey key(tenant_id, OB_INVALID_ID, OB_INVALID_ID, task_id);
  int64_t affected_rows = 0;
  if (OB_FAIL(ObTTLUtil::delete_ttl_task(tenant_id,
                                        share::OB_ALL_KV_TTL_TASK_TNAME,
                                        *GCTX.sql_proxy_, key, affected_rows))) {
    LOG_WARN("fail to delete ttl tasks status", K(ret), K(tenant_id), K(task_id));
  } else {
    LOG_DEBUG("success to delete ttl tasks status", K(ret), K(tenant_id), K(task_id), K(affected_rows));
  }

  return ret;
}

bool ObTTLTenantTaskMgr::tenant_exist(uint64_t tenant_id)
{

  bool bool_ret = false;
  for (size_t i = 0; !bool_ret && i < ten_task_arr_.count(); ++i) {
    if (ten_task_arr_.at(i).tenant_id_ == tenant_id) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObTTLTenantTaskMgr::update_tenant_tasks(uint64_t tenant_id, ObTTLStatusArray& tasks)
{
  int ret = OB_SUCCESS;
  
  ObTTLTenantTask* task_ptr = NULL;
  if (tasks.count() == 0) {
    // do nothing
  } else if (tasks.count() > 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to update task", K(ret));
  } else if (OB_FAIL(get_tenant_ptr(tenant_id, task_ptr))) {
    LOG_WARN("fail to get tenant tasks ptr", K(ret));
  } else {
    ObTTLTenantTask& tenant_task = *task_ptr;
    tenant_task.tasks_.reuse();

    RsTenantTask rs_task1, rs_task2;
    size_t count = tasks.count();
    if (count > 1) {
      if (tasks.at(0).task_start_time_ > tasks.at(1).task_start_time_) {
        rs_task1.ttl_status_ = tasks.at(1);
        rs_task2.ttl_status_ = tasks.at(0);
      } else {
        rs_task1.ttl_status_ = tasks.at(0);
        rs_task2.ttl_status_ = tasks.at(1);
      }
    } else if (count > 0) {
      rs_task1.ttl_status_ = tasks.at(0);
    }

    if (count > 0 && OB_FAIL(tenant_task.tasks_.push_back(rs_task1))) {
      LOG_WARN("fail to store ttl task status", K(ret), K(count));
    } else if (count > 1 && OB_FAIL(tenant_task.tasks_.push_back(rs_task2))) {
      LOG_WARN("fail to store ttl task status", K(ret), K(count));
    } else {
      LOG_DEBUG("success to store ttl task status", K(ret), K(count), K(tenant_id));
    }

    if (OB_SUCC(ret)) {
      tenant_task.need_refresh_ = false;
    } else {
      tenant_task.tasks_.reuse();
    }
  }

  return ret;
}


bool ObTTLTenantTaskMgr::need_refresh_tenant(uint64_t tenant_id)
{
  bool bool_ret = false;
  for (size_t i = 0; i < ten_task_arr_.count(); ++i) {
    if (ten_task_arr_.at(i).tenant_id_ == tenant_id) {
      bool_ret = ten_task_arr_.at(i).need_refresh_ && (ten_task_arr_.at(i).tasks_.count() <= 0);
      break;
    }
  }
  return bool_ret;
}

int ObTTLTenantTaskMgr::refresh_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!tenant_exist(tenant_id) && 
      OB_FAIL(add_tenant(tenant_id))) {
    LOG_WARN("fail to add tenant", K(ret), K(tenant_id)); 
  } else if (need_refresh_tenant(tenant_id)) {
    ObTTLStatusArray ttl_tasks;
    if (OB_FAIL(read_tenant_status(tenant_id, ttl_tasks))) {
      LOG_WARN("fail to read tenant tasks", K(ret));
    } else if (OB_FAIL(update_tenant_tasks(tenant_id, ttl_tasks))) {
      LOG_WARN("fail to update tenant tasks", K(ret), K(tenant_id));
    }

    LOG_INFO("refresh tenant task from system table", K(tenant_id));
  }
  return ret;
}

int ObTTLTenantTaskMgr::refresh_all()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else {
    LOG_INFO("get all tenant ids", K(tenant_ids));
    for (size_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(refresh_tenant(tenant_id))) {
        LOG_WARN("fail to refresh tenant", K(ret), K(tenant_id));
      }
    }
  }

  return ret;
}

int ObTTLTenantTaskMgr::get_tenant_ids(ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  }
  return ret;
}

int ObTTLTenantTaskMgr::in_active_time(uint64_t tenant_id, bool& is_active_time)
{
  int ret = OB_SUCCESS;
  
  is_active_time = false;

  ObTTLDutyDuration duration;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObTTLUtil::parse(tenant_config->kv_ttl_duty_duration, duration))) {
    LOG_WARN("fail parse ttl dury duration", K(ret));
  } else if (ObTTLUtil::current_in_duration(duration)) {
    is_active_time = true;
  }

  return ret;
}

bool ObTTLTenantTaskMgr::is_enable_ttl(uint64_t tenant_id)
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  return tenant_config.is_valid() && tenant_config->enable_kv_ttl;
}

int ObTTLTenantTaskMgr::alter_status_and_add_ttl_task(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTTLTenantTask* tenant_ptr = NULL;
  bool enable_ttl = is_enable_ttl(tenant_id);
  bool is_active_time = false;
  
  lib::ObMutexGuard guard(mutex_);

  if (OB_FAIL(in_active_time(tenant_id, is_active_time))) {
    LOG_WARN("fail to eval active time", K(ret));
  } else if (OB_FAIL(get_tenant_ptr(tenant_id, tenant_ptr))) {
    LOG_WARN("fail to get tenant task ptr, need refresh", K(tenant_id), K(ret));
  } else {
    ObTTLTenantTask& tenant_ref = *tenant_ptr;
    size_t task_count = tenant_ref.tasks_.count();
    
    LOG_DEBUG("alter_status_and_add_ttl_task begin", K(tenant_id), K(task_count));
    if (task_count > 0) {
      RsTenantTask& rs_task = tenant_ref.tasks_.at(0);
      
      ObTTLTaskStatus cur_state = EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_);
      int64_t status_responsed = EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_);
      ObTTLTaskStatus next_state = cur_state;

      LOG_DEBUG("alter task status current task:", K(ret), K(tenant_id), K(rs_task.ttl_status_), K(status_responsed), K(cur_state));

      if (status_responsed || is_all_responsed(rs_task)) {
        LOG_INFO("alter status and add ttl task", K(rs_task), K(status_responsed));
        if (!status_responsed) {
          int64_t tmp_status = 0;
          SET_TASK_STATUS(tmp_status, cur_state, 1);
          if (OB_FAIL(update_task_status(tenant_id, rs_task.ttl_status_.task_id_, tmp_status, *GCTX.sql_proxy_))) {
              LOG_WARN("fail to update ttl tasks", K(ret));
          } else {
            rs_task.set_servers_not_responsed();
            rs_task.all_responsed_ = false;

            // update stauts and update time
            rs_task.ttl_status_.status_ = tmp_status;
            cur_state = EVAL_TASK_PURE_STATUS(tmp_status);
          }
        }
        LOG_INFO("alter status and add ttl task", K(rs_task), K(status_responsed));

        if (OB_SUCC(ret)) {
          // if all sever responsed, should jump to next state
          // if cur_state == next_state, means the current state is SUSPEND or MOVE
          next_state = next_status(cur_state);
          if (cur_state == next_state) {
            if (cur_state == ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE) {
              // task count > 1 means there's another task in queue, delete the moved status task
              // if the task is not the today started task, should delete and add new priodic task
              if (task_count > 1 || 
                  !ObTTLTime::is_same_day(ObTimeUtility::current_time(), rs_task.ttl_status_.task_start_time_)) {
                if (OB_FAIL(delete_task(tenant_id, rs_task.ttl_status_.task_id_))) {
                  LOG_WARN("fail to store tenant task", K(ret));
                } else {
                  LOG_INFO("delete finished task", K(rs_task.ttl_status_));
                  tenant_ref.tasks_.remove(0);
                }
              }
            }
          } else {
             /**
              * cur_state != next_state means cur_state is OB_RS_TTL_TASK_CREATE or OB_RS_TTL_TASK_CANCEL
              * next state should be move
              *  send move to servers, update status
             */
            LOG_INFO("alter status and add ttl task", K(next_state));
            if (OB_FAIL(update_task_status(tenant_id, rs_task.ttl_status_.task_id_, next_state, *GCTX.sql_proxy_))) {
                LOG_WARN("fail to update ttl tasks", K(ret));
            } else {
              LOG_INFO("alter status and add ttl task", K(next_state));
              rs_task.ttl_status_.status_ = static_cast<int64_t>(next_state);
              rs_task.set_servers_not_responsed();
              if (OB_FAIL(send_server_task_req(rs_task, ObTTLTaskType::OB_TTL_MOVE))) {               
                LOG_WARN("fail to send server task ttl request", K(ret), K(next_state), K(cur_state));
              }
            }
          }
        }
      }
    } else if (!tenant_ref.is_del_) { // task_count == 0    
      if (is_active_time && enable_ttl) {
        TRIGGER_TYPE trigger_type = TRIGGER_TYPE::PERIODIC_TRIGGER;
        if (OB_FAIL(add_ttl_task_internal(tenant_id, trigger_type, true))) {
          LOG_WARN("fail to add new ttl task internal", K(ret), K(trigger_type));
        }
        
        LOG_DEBUG("alter_status_and_add_ttl_task, add tasks", K(ret));
      }
    }
  }
  
  LOG_DEBUG("alter_status_and_add_ttl_task end", K(ret), K(tenant_id));

  return ret;
}
// need lock
int ObTTLTenantTaskMgr::get_tenant_ptr(uint64_t tenant_id, ObTTLTenantTask*& tenant_tasks)
{
  int ret = OB_TENANT_NOT_EXIST;
  for (size_t i = 0; i < ten_task_arr_.count(); ++i) {
    ObTTLTenantTask& t_task = ten_task_arr_.at(i);
    if (t_task.tenant_id_ == tenant_id) {
      tenant_tasks = &t_task;
      ret = OB_SUCCESS;
      break;
    }
  }

  return ret;
}

int ObTTLTenantTaskMgr::get_task_ptr(uint64_t tenant_id, uint64_t task_id, RsTenantTask*& tenant_task)
{
  int ret = OB_SUCCESS;
  ObTTLTenantTask* tenant_tasks = NULL;

  if (OB_FAIL(get_tenant_ptr(tenant_id, tenant_tasks))) {
    LOG_WARN("fail to get tenant tasks entry", K(ret));
  } else {
    ret = OB_SEARCH_NOT_FOUND;
    for (size_t i = 0; i < tenant_tasks->tasks_.count(); ++i) {
      RsTenantTask& rs_task = tenant_tasks->tasks_.at(i);
      if (rs_task.ttl_status_.task_id_ == task_id) {
        ret = OB_SUCCESS;
        tenant_task = &rs_task;
        break;
      }
    }
  }
  return ret;
}

int ObTTLTenantTaskMgr::get_tenant_tasks(uint64_t tenant_id, ObTTLTenantTask& tenant_tasks)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);

  ObTTLTenantTask* t_task = NULL;
  if (OB_FAIL(get_tenant_ptr(tenant_id, t_task))) {
    LOG_WARN("fail to get tenant tasks ptr", K(ret), K(tenant_id));
  } else {
    tenant_tasks = *t_task;
  }
  return ret;
}

bool ObTTLTenantTaskMgr::is_all_responsed(RsTenantTask& task) 
{
  bool bret = true;
  const TTLServerInfos& server_infos = task.server_infos_;
  if (server_infos.empty()) {
    bret = false;
  } else if (!task.all_responsed_) {
    for (size_t i = 0; i < server_infos.count() && bret; ++i) {
      bret = server_infos.at(i).is_responsed_;
    }
    task.all_responsed_ = bret;
  }
  return bret;
}

int ObTTLTenantTaskMgr::get_task(uint64_t tenant_id, uint64_t task_id, RsTenantTask& rs_task)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);

  RsTenantTask* task_ptr = NULL;
  if (OB_FAIL(get_task_ptr(tenant_id, task_id, task_ptr))) {
    LOG_WARN("fail to get tenant tasks ptr", K(ret), K(tenant_id));
  } else {
    rs_task = *task_ptr;
  }
  return ret;
}

int ObTTLTenantTaskMgr::get_server_infos(uint64_t tenant_id,
                                         TTLServerInfos& server_infos)
{
  int ret = OB_SUCCESS;
  server_infos.reuse();
  ObServerManager& server_mgr = GCTX.root_service_->get_server_mgr();
  ObUnitManager& unit_mgr = GCTX.root_service_->get_unit_mgr();
  // use hash set to eliminate duplicate server
  ServerSet server_set;
  if (OB_FAIL(server_set.create(hash::cal_next_prime(1000)))) {
    LOG_WARN("fail to create hash bucket.", K(ret));
  } else if (!server_mgr.has_build() || !unit_mgr.check_inner_stat()) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("server manager or unit manager hasn't built", "server_mgr built",
             server_mgr.has_build(), "unit_mgr built", unit_mgr.check_inner_stat(), K(ret));
  } else {
    ObArray<uint64_t> pool_ids;
    if (OB_FAIL(unit_mgr.get_pool_ids_of_tenant(tenant_id, pool_ids))) {
      LOG_WARN("get_pool_ids_of_tenant failed", K(tenant_id), K(ret));
    } else {
      ObArray<share::ObUnitInfo> unit_infos;
      for (size_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
        unit_infos.reuse();
        if (OB_FAIL(unit_mgr.get_unit_infos_of_pool(pool_ids.at(i), unit_infos))) {
          LOG_WARN("get_unit_infos_of_pool failed", "pool_id", pool_ids.at(i), K(ret));
        } else {
          for (size_t j = 0; OB_SUCC(ret) && j < unit_infos.count(); ++j) {
            bool is_alive = false;
            const share::ObUnit& unit = unit_infos.at(j).unit_;
            if (OB_FAIL(server_mgr.check_server_alive(unit.server_, is_alive))) {
              LOG_WARN("check_server_alive failed", "server", unit.server_, K(ret));
            } else if (is_alive) { 
              if (OB_FAIL(server_set.set_refactored(unit.server_))) {
                if (OB_HASH_EXIST == ret) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("fail to stop server info set", KR(ret), K(unit.server_));
                }
              }
            }
          }  // for unit infos end
        }
      }  // for pool ids end

      if (OB_SUCC(ret)) {
        if (OB_FAIL(server_infos.reserve(server_set.size()))) {
          LOG_WARN("fail to reserve server infos", KR(ret), K(server_set.size()));
        }

        ServerSet::iterator iter = server_set.begin();
        for (; OB_SUCC(ret) && iter != server_set.end(); ++iter) {
          ObTTLServerInfo tmp_server_info;
          tmp_server_info.addr_ = iter->first;
          if (OB_FAIL(server_infos.push_back(tmp_server_info))) {
            LOG_WARN("fail to push back server info", K(ret));
          }
        }
      }
    }
  }

  if (server_set.created()) {
    server_set.destroy();
  }

  return ret;
}

int ObTTLTenantTaskMgr::dispatch_ttl_request(const TTLServerInfos& server_infos, 
                                             uint64_t tenant_id, int ttl_cmd,
                                             int trigger_type, int64_t task_id)
{
  int ret = OB_SUCCESS;
  const int64_t rpc_timeout = max(GCONF.rpc_timeout, 3 * 1000L * 1000L);
  obrpc::ObTTLRequestArg arg;
  arg.cmd_code_ = ttl_cmd;
  arg.task_id_ = task_id;
  arg.trigger_type_ = trigger_type;
  arg.tenant_id_ = tenant_id;

  ObArray<int32_t> ret_arr;
  if (OB_FAIL(ret_arr.reserve(server_infos.count()))) {
    LOG_WARN("fail to assign send addrs, for reserve space", K(ret), K(ret_arr.count()));
  }

  int send_cnt = 0;
  rootserver::ObTTLProxy proxy(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::ttl_request);
  for (size_t i = 0; OB_SUCC(ret) && i < server_infos.count(); ++i) {
    if (!server_infos.at(i).is_responsed_) {
      const ObAddr& addr = server_infos.at(i).addr_;
      if (OB_FAIL(proxy.call(addr, rpc_timeout, arg))) {
        LOG_WARN("fail to send rpc", K(ret), K(addr), K(rpc_timeout), K(arg));
      } else {
        send_cnt++;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(proxy.wait_all(ret_arr))) {
      LOG_WARN("rpc_proxy wait failed", K(ret));
    } else if (ret_arr.count() != send_cnt ||
               ret_arr.count() != proxy.get_results().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("return count not match", K(ret), K(ret_arr.count()),
                                         K(proxy.get_results().count()), K(send_cnt));
    } else {
      for (int64_t i = ret_arr.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        if (OB_FAIL(ret_arr.at(i))) {
          LOG_WARN("rpc execute failed", KR(ret), K(i));
        } else {
          const obrpc::ObTTLResult* result = proxy.get_results().at(i);
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", K(ret), K(i));
          } else {
            // todo@dazhi: won't return these retcode synchronously
            // if (result->ret_code_ == OB_NOT_MASTER || result->ret_code_ == OB_ENTRY_NOT_EXIST) {
            //   server_infos.at(i).is_responsed_ = true;
            //   LOG_INFO("send ttl server ttl request, ignore server", K(server_infos.at(i)), K(result->ret_code_));
            // }
          }
        }
      }
    }
  }

  LOG_INFO("send ttl server ttl request", K(ret), K(arg), K(send_cnt), K(server_infos.count()), K(server_infos));
  return ret;
}

int ObTTLTenantTaskMgr::send_server_task_req(RsTenantTask& task, ObTTLTaskType task_type)
{
  int ret = OB_SUCCESS;

  task.all_responsed_ = false;
  if (task.server_infos_.empty() &&
        OB_FAIL(get_server_infos(task.ttl_status_.tenant_id_, task.server_infos_))) {
    LOG_WARN("fail to get server addrs", K(ret));
  } else if (OB_FAIL(dispatch_ttl_request(task.server_infos_,
                                          task.ttl_status_.tenant_id_,
                                          static_cast<int>(task_type),
                                          static_cast<int>(task.ttl_status_.trigger_type_),
                                          task.ttl_status_.task_id_))) {
    LOG_WARN("fail to dispatch ttl request", K(ret), K(task.ttl_status_.trigger_type_),
             K(task.ttl_status_.tenant_id_), K(task.ttl_status_.task_id_));
  } else {
    task.ttl_status_.task_update_time_ = ObTimeUtility::current_time();
  }

  return ret;
}

int ObTTLTenantTaskMgr::fetch_ttl_task_id(uint64_t tenant_id, int64_t &new_task_id)
{
  int ret = OB_SUCCESS;
  uint64_t tmp_task_id = OB_INVALID_ID;
  share::ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
  if (OB_FAIL(id_fetcher.fetch_new_max_id(tenant_id,
                                          share::OB_MAX_USED_TTL_TASK_ID_TYPE,
                                          tmp_task_id, 0))) {
    LOG_WARN("fail to fetch new ttl task id", K(ret), "id_type", share::OB_MAX_USED_TTL_TASK_ID_TYPE);
  } else {
    new_task_id = extract_pure_id(tmp_task_id);
  }
  return ret;
}

int ObTTLTenantTaskMgr::rsp_task_status(ObTTLTaskType rsp_task_type, ObTTLTaskStatus rs_status)
{
  int ret = OB_SUCCESS;
  switch (rs_status) {
    case ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE: {
      if (rsp_task_type != ObTTLTaskType::OB_TTL_TRIGGER) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to process rsp task status", K(ret), K(rsp_task_type));
      }
      break;
    }
    case ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND: {
      if (rsp_task_type != ObTTLTaskType::OB_TTL_SUSPEND) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to process rsp task status", K(ret), K(rsp_task_type));
      }
      break;
    }
    case ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL: {
      if (rsp_task_type != ObTTLTaskType::OB_TTL_CANCEL) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to process rsp task status", K(ret), K(rsp_task_type));        
      }
      break;
    }
    case ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE: {
      if (rsp_task_type != ObTTLTaskType::OB_TTL_MOVE) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to process rsp task status", K(ret), K(rsp_task_type));
      }
      break;
    }
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rsp task status", K(ret), K(rs_status), K(rsp_task_type));
      break;
    }
  }
  return ret;
}

void ObTTLTenantTaskMgr::reset_local_tenant_task()
{
  lib::ObMutexGuard guard(mutex_);
  for (size_t i = 0; i < ten_task_arr_.count(); ++i) {
    ObTTLTenantTask& t_task = ten_task_arr_.at(i);
    t_task.reset();
  }
}

void ObTTLTenantTaskMgr::refresh_deleted_tenants()
{
  int ret = OB_SUCCESS;
 
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    for (size_t i = 0; i < ten_task_arr_.count() && OB_SUCC(ret); ++i) {
      bool tenant_not_exist = false;
      uint64_t tenant_id = ten_task_arr_.at(i).tenant_id_;

      if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(tenant_id, tenant_not_exist))) {
        LOG_WARN("fail to check  tenant exists", K(ret), K(tenant_id));
      } else if (tenant_not_exist) {
        bool exist = false;
        ten_task_arr_.at(i).is_del_ = true;
        for (size_t k = 0; k < del_ten_arr_.count() && !exist; ++k) {
          exist = (del_ten_arr_.at(k) == tenant_id);
        }
        
        if (!exist) {
          if (OB_FAIL(del_ten_arr_.push_back(tenant_id))) {
            LOG_WARN("fail to store deleted tenant id", K(ret));
          } else {
            LOG_INFO("add tennat id to del tenant array", K(ret), K(tenant_id), K(del_ten_arr_));
          }
        }
      }
    }
  }
}

void ObTTLTenantTaskMgr::proc_deleted_tenant()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  refresh_deleted_tenants();

  for (int64_t i = del_ten_arr_.count() - 1; i >= 0 ; --i) {
    uint64_t tenant_id = del_ten_arr_.at(i);
    ObTTLTenantTask* task_ptr = NULL;
    if (OB_FAIL(get_tenant_ptr(tenant_id, task_ptr))) {
      if (ret == OB_TENANT_NOT_EXIST) {
        del_ten_arr_.remove(i);
      } else {
        LOG_WARN("fail to get tenant task ptr", K(ret));
      }
    } else {
      ObTTLTenantTask& task_ref = *task_ptr;
      task_ref.reset();
      delete_tenant(tenant_id);
      del_ten_arr_.remove(i);
    }
  }
}

int ObTTLTenantTaskMgr::process_tenant_task_rsp(uint64_t tenant_id, 
                                                int64_t task_id, 
                                                int64_t task_type,
                                                const ObAddr& server_addr)
{
  int ret = OB_SUCCESS;
  TTLMGR.refresh_all();
  lib::ObMutexGuard guard(mutex_);
  RsTenantTask* rs_task_ptr = NULL;

  if (OB_FAIL(get_task_ptr(tenant_id, task_id, rs_task_ptr))) {
    LOG_WARN("fail to get tasks ptr", K(ret), K(tenant_id), K(task_id));
  } else {
    RsTenantTask& rs_task = *rs_task_ptr;

    if (OB_FAIL(rsp_task_status(static_cast<ObTTLTaskType>(task_type), EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_)))) {
      LOG_WARN("response task type incorrect",
               K(ret), K(tenant_id), K(task_id), K(task_type), K(EVAL_TASK_PURE_STATUS(rs_task.ttl_status_.status_)));
    } else if (OB_FAIL(rs_task.set_server_responsed(server_addr))) {
      LOG_WARN("fail to set server responsed", K(ret), K(tenant_id), K(task_id));
    } else if (!EVAL_TASK_RESPONSE(rs_task.ttl_status_.status_) && 
                is_all_responsed(rs_task)) {
      rs_task.all_responsed_ = true;
      LOG_INFO("all server rsp collecte", K(tenant_id), K(task_id), K(rs_task.server_infos_), K(rs_task.ttl_status_.status_));
      if (OB_FAIL(update_task_on_all_responsed(rs_task))) {
        LOG_WARN("fail to update ttl tasks", K(ret), K(task_id), K(task_type));
      } else {
        LOG_INFO("success update task status", K(tenant_id), K(task_id), K(task_type), K(rs_task.ttl_status_.status_));
      }
    }
  }

  return ret;
}

int ObTTLTenantTaskMgr::update_task_on_all_responsed(RsTenantTask& task)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = task.ttl_status_.tenant_id_;

  ObTTLTaskStatus next_state, cur_state;
  cur_state = EVAL_TASK_PURE_STATUS(task.ttl_status_.status_);
  next_state = next_status(cur_state);
  bool is_move = false;

  int64_t task_status = static_cast<int64_t>(next_state);
  if (next_state == cur_state) {
    // move or suspend
    SET_TASK_RESPONSE(task_status, 1);
    if (cur_state == OB_RS_TTL_TASK_MOVE) {
      is_move = true;
    }
  }
  
  if (task.ttl_status_.status_ == task_status) {
    // SUSPEND or MOVED
  } else {
    ObMySQLTransaction trans; 
    uint64_t task_id = task.ttl_status_.task_id_;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null GCTX.sql_proxy_", K(ret));
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) { 
      LOG_WARN("fail to start transation", K(ret), K(tenant_id));
    } else if (OB_FAIL(update_task_status(tenant_id, task_id, task_status, *GCTX.sql_proxy_))) {
      LOG_WARN("fail to update ttl tasks", K(ret), K(task_id), K(cur_state), K(next_state), K(task_status));
    } else if (is_move && OB_FAIL(ObTTLUtil::remove_all_task_to_history_table(tenant_id, task_id, trans))) {
      // NOTE: if parition was removed and observer restart, task won't be moved by observer itself
      LOG_WARN("fail to move task to history table", K(tenant_id), K(task_id));
    } else {}

    if (trans.is_started()) {
      bool commit = (OB_SUCCESS == ret);
      int tmp_ret = ret;
      if (OB_FAIL(trans.end(commit))) {
        LOG_WARN("faile to end trans", "commit", commit, K(ret));
      }
      ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    }
    
    if (OB_SUCC(ret)) {
      // update stauts and update time
      task.ttl_status_.status_ = task_status;
      task.all_responsed_ = false;
      task.set_servers_not_responsed();
    }
  }

  return ret;
}

int ObTTLScheduler::init()
{
  int ret = OB_SUCCESS; 
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ttl scheduler init twice", K(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("ttl scheduler is inited");
  }
  (void)TTLMGR.init();
  return ret;
}

int ObTTLScheduler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl scheduler is not init", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::TTLScheduler))) {
    LOG_WARN("init ttl scheduler fail", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TTLScheduler, *this, SCHEDULE_PERIOD, true))) {
    LOG_WARN("failed to schedule ttl scheduler", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TTLScheduler, clear_ttl_history_task_, 
              ObClearTTLStatusHistoryTask::OB_KV_TTL_GC_INTERVAL, true))) {
    LOG_WARN("fail to start ttl clear history task", K(ret));
  } else {
    LOG_INFO("success to start ttl clear history task and start ttl scheduler", K(ret));
  }

  TTLMGR.reset_local_tenant_task();
  TTLMGR.init();
  return ret;
}

void ObTTLScheduler::wait()
{
  TG_WAIT(lib::TGDefIDs::TTLScheduler);
  LOG_INFO("ttl scheduler wait success");
}

void ObTTLScheduler::stop()
{
  TG_STOP(lib::TGDefIDs::TTLScheduler);
  LOG_INFO("ttl scheduler stopped");
}

void ObTTLScheduler::destroy()
{
  TG_DESTROY(lib::TGDefIDs::TTLScheduler);
  LOG_INFO("ttl scheduler destroyed");
}

void ObTTLScheduler::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (ObTTLUtil::check_can_do_work()) {
    TTLMGR.refresh_all();
    ObArray<uint64_t> tenant_ids;
    if (OB_FAIL(TTLMGR.get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant ids", K(ret));
    } else {
      for (size_t idx = 0; idx < tenant_ids.count(); ++idx) {
        uint64_t tenant_id = tenant_ids.at(idx);
        if (tenant_id == OB_SYS_TENANT_ID) {
          // do nothing, continue
        } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id)) {
          // do nothing
        } else if (OB_FAIL(TTLMGR.alter_status_and_add_ttl_task(tenant_id))) {
          LOG_WARN("fail to alter status and add tasks", K(ret), K(tenant_id));
        } else if (OB_FAIL(TTLMGR.process_tenant_tasks(tenant_id))) {
          LOG_WARN("fail to process tenant task", K(ret), K(tenant_id));
        }
      }
      
      TTLMGR.proc_deleted_tenant();
    }
    uint64_t current = ObTimeUtility::current_time();
    LOG_DEBUG("runTimerTask", K(current));  
  }
}

int RsTenantTask::set_server_responsed(const ObAddr& server_addr)
{
  int ret = OB_SUCCESS;
  bool find_server = false;
  TTLServerInfos& server_infos = server_infos_;
  if (OB_UNLIKELY(!server_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(server_addr));
  } else {
    for (int64_t i = 0; i < server_infos.count() && !find_server; ++i) {
      if (server_addr == server_infos.at(i).addr_) {
        server_infos.at(i).is_responsed_ = true;
        find_server = true;
      }
    }

    if (!find_server) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cannot find addr in sever infos", K(ret), K(server_addr), K(server_infos));
    }
  }
  return ret;
}

void RsTenantTask::set_servers_not_responsed()
{
  for (int64_t i = 0; i < server_infos_.count(); ++i) {
    server_infos_.at(i).is_responsed_ = false;
  }
}


} // end namespace rootserver
} // end namespace oceanbase