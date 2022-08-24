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
#include "ob_table_ttl_manager.h"
#include "ob_table_ttl_task.h"
#include "share/ob_srv_rpc_proxy.h"
#include "storage/ob_partition_meta_redo_module.h"
#include "storage/ob_partition_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "ob_htable_filter_operator.h"

using namespace oceanbase::storage;
using namespace oceanbase::observer;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;


/*init/stop function*/
int ObTTLManager::init()
{
  int ret = OB_SUCCESS;
  schema_service_ = &(schema::ObMultiVersionSchemaService::get_instance());
  sql_proxy_ = GCTX.sql_proxy_;
  
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(ttl_tenant_parts_map_.create(DEFAULT_TTL_BUCKET_NUM, 
        ObModIds::TABLE_TTL, ObModIds::TABLE_TTL))) {
    LOG_WARN("fail to create ttl tenant info map", K(ret));
  } else if (OB_FAIL(ttl_timer_.init("TTLTimer"))) {
    LOG_WARN("fail to init timer", K(ret));
  } else {
    is_init_ = true;
    LOG_INFO("success to init ttl manager");
  }
  return ret;
}

int ObTTLManager::start()
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl manager not init", K(ret));
  } else if (OB_FAIL(ttl_timer_.schedule(periodic_task_, periodic_delay_, true))) {
    LOG_WARN("fail to schedule periodic task", K(ret));
  } else {
    is_timer_start_ = true;
    LOG_INFO("ttl manager is started");
  }
  return ret;
}

void ObTTLManager::stop()
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    LOG_WARN("ttl manager not init", K(ret));
  } else if (is_timer_start_) {
    ttl_timer_.cancel_all();
    is_timer_start_ = false;
    LOG_INFO("ttl manager is stoped");
  }
}

void ObTTLManager::destroy()
{
  stop(); 
  cancel_all_tenant_task();
  ttl_timer_.destroy();
  ttl_tenant_parts_map_.destroy();
  is_init_ = false;
  LOG_INFO("ttl manager is destoried");
}

ObTTLManager& ObTTLManager::get_instance()
{
  static ObTTLManager static_instance;
  return static_instance;
}

/*scan & handle function*/
/*has lock problem, need solve*/
int ObTTLManager::scan_all_tenanat_handle_event()
{
  int ret = OB_SUCCESS;
  ObTTLTenantInfo* tenant_info = NULL;
  int64_t tenant_id = OB_INVALID_ID;
  common::ObSArray<ObPartitionKey> sync_sys_partitions;
  common::ObSArray<uint64_t> need_rsp_tenants;
  common::ObSArray<uint64_t> need_move_tenants;
  common::ObSArray<uint64_t> dirty_tenants;
  common::ObSArray<uint64_t> check_tenants;
  
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl manager not init");
  } else {
    common::ObSpinLockGuard guard(lock_);
    for (ttl_tenants_iterator iter = ttl_tenant_parts_map_.begin();
                              iter != ttl_tenant_parts_map_.end() && OB_SUCC(ret); ++iter) {
      tenant_id = iter->first;
      tenant_info = iter->second;
      if (tenant_info->need_check_ && OB_FAIL(check_tenants.push_back(tenant_id))) {
        // after observer restart, need check tenant even when cancel and move state
        LOG_WARN("fail to push back check tenants", K(ret));
      }
      
      if (OB_SUCC(ret) && tenant_info->is_dirty_ && OB_FAIL(dirty_tenants.push_back(tenant_id))) {
        LOG_WARN("fail to push back dirty tenants", K(ret));
      } else if (OB_TTL_TASK_MOVING == tenant_info->state_ &&
                 OB_FAIL(need_move_tenants.push_back(tenant_id))) {
        LOG_WARN("fail to push back move operation", K(tenant_id));
      }

      if (OB_SUCC(ret) && (tenant_info->is_droped_ || tenant_info->rsp_time_ != OB_INVALID_ID) && 
          OB_FAIL(need_rsp_tenants.push_back(tenant_id))) { // todo: remove is_droped_
        LOG_WARN("fail to push back rsp operation", K(tenant_id));
      }
    }
  }

  for (int i = 0; i < check_tenants.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(generate_tenant_tasks(check_tenants.at(i)))) {
      LOG_WARN("fail to generate task in timer", K(tenant_id));
    }
  }

  for (int i = 0; i < dirty_tenants.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(inner_handle_single_tenant_event(dirty_tenants.at(i),  
                                                 sync_sys_partitions))) {
      LOG_WARN("fail to handle tenant event in timer", K(tenant_id));
    }

    /*sync sys table*/
    if (0 != sync_sys_partitions.count()) {
      for (int i = 0; i < sync_sys_partitions.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(sync_sys_table(sync_sys_partitions.at(i)))) {
          LOG_WARN("fail to sync sys table", K(ret));
        }
      }
      sync_sys_partitions.reset();
    }
    
    /*check tenant still dirty*/
    check_ttl_tenant_state(dirty_tenants.at(i));
  }

  /*do moving*/
  for (int i = 0; i < need_move_tenants.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(move_record_to_history_table(need_move_tenants.at(i)))) {
      LOG_WARN("fail to move record to history table", K(ret));
    }
  }

  /*check & do rsp*/
  for (int i = 0; i < need_rsp_tenants.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(check_and_do_rsp(need_rsp_tenants.at(i)))) {
      LOG_WARN("fail to do check and response", K(ret));
    }
  }

  if (OB_TENANT_NOT_EXIST == ret) {
    LOG_INFO("begin to check and reset dropped tenant", K(ret));
    if (OB_FAIL(check_and_reset_droped_tenant())) {
      LOG_WARN("fail to check and reset dropped tenant", K(ret));
    }
  }
  return ret;
}

void ObTTLManager::check_ttl_tenant_state(uint64_t tenant_id)
{
  common::ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  bool tenant_dirty = false;
  bool tenant_finish = true;
  ObTTLTaskCtx* ctx = NULL;

  ObTTLTenantInfo* tenant_info = get_tenant_info(tenant_id, false);
  if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("tenant info is null", K(tenant_id), K(ret));
  } else {
    for (ttl_parts_iterator iter = tenant_info->part_task_map_.begin(); 
           !tenant_dirty && iter != tenant_info->part_task_map_.end(); ++iter) {
      ctx = iter->second;
      if (OB_ISNULL(ctx)) {
        LOG_WARN("fatal err, ttl ctx in map is null", K(tenant_info->tenant_id_));
      } else if (ctx->is_dirty_) {
        tenant_dirty = true;
      } else if (ctx->task_status_ != OB_TTL_TASK_CANCEL &&
                 ctx->task_status_ != OB_TTL_TASK_FINISH) {
        tenant_finish = false;
        tenant_info->is_finished_ = false;
      } 
    }
  }

  if (OB_SUCC(ret) && !tenant_dirty) {
    tenant_info->is_dirty_ = false;
    if (tenant_finish) {
      if (tenant_info->state_ == OB_TTL_TASK_CANCEL || tenant_info->state_ == OB_TTL_TASK_RUNNING) {
        // all task already in cancel or runing status
        tenant_info->is_finished_ = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  LOG_DEBUG("check ttl tenant dirty", K(tenant_info->is_dirty_), K(tenant_info->state_), K(ret), K(tenant_id));
}

void ObTTLManager::mark_tenant_rsp(uint64_t tenant_id, int64_t rsp_time)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  ObTTLTenantInfo* tenant_info = get_tenant_info(tenant_id, false);
  if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("tenant info is null", K(tenant_id));
  } else if (tenant_info->state_ == OB_TTL_TASK_MOVING) {    
    if (OB_FAIL(ttl_tenant_parts_map_.erase_refactored(tenant_id))) {
      LOG_WARN("fail to erase tenant info", K(tenant_id));
    } else {
      tenant_info->destory();
      allocator_.free(tenant_info);
    }
  } else if (tenant_info->rsp_time_ != rsp_time) {
    LOG_WARN("rsp time not match, during sync sys table, rs may resend msy", 
                                  K(tenant_id), K(tenant_info->rsp_time_), K(rsp_time));
  } else {
    tenant_info->rsp_time_ = OB_INVALID_ID;
  }
  LOG_DEBUG("mark tenant response", K(tenant_id));
}

int ObTTLManager::check_and_do_rsp(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool can_rsp = false;
  int64_t rsp_time = OB_INVALID_ID;
  ObTTLTaskStatus rsp_status;
  ObTTLTenantInfo* tenant_info = NULL;
  ObTTLTaskCtx* ctx = NULL;
  {
    common::ObSpinLockGuard guard(lock_);
    tenant_info = get_tenant_info(tenant_id, false);
    if (OB_ISNULL(tenant_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant info is null", K(tenant_id));
    } else if (tenant_info->rsp_time_ != OB_INVALID_ID) {
      can_rsp = true;
      rsp_status = tenant_info->state_;
      rsp_time = tenant_info->rsp_time_;
      for (ttl_parts_iterator iter = tenant_info->part_task_map_.begin(); 
                              can_rsp && iter != tenant_info->part_task_map_.end();
                              ++iter) {
        ctx = iter->second;
        if (OB_ISNULL(ctx)) {
          can_rsp = false;
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("ttl ctx is null", K(tenant_id), K(iter->first));
        } else if (ctx->is_dirty_) {
          can_rsp = false;
        } else if (OB_TTL_TASK_CANCEL == ctx->task_status_ ||
                   OB_TTL_TASK_FINISH == ctx->task_status_ ) {
          //do nothing
        } else if (rsp_status != ctx->task_status_ ||
                   rsp_status == OB_TTL_TASK_RUNNING) {
          can_rsp = false;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (can_rsp && OB_FAIL(response_ttl_cmd(tenant_id, tenant_info->task_id_, tenant_info->cmd_type_))) {
      LOG_WARN("fail to response ttl task to rs", K(ret), K(tenant_info->task_id_), K(rsp_status));
    }

    if (OB_SUCC(ret) && (can_rsp)) {
      mark_tenant_rsp(tenant_id, rsp_time);
    }
  }
  LOG_DEBUG("finish check and do response", K(ret), K(tenant_id), K(can_rsp), K(rsp_status), K(rsp_time));
  return ret;
}

/* transformation of state of tenant info (most be driven by rs):
 *    invalid -> running -> moving
 *                       -> canceling -> moving
 *                       -> pending -> running
 *                                  -> canceling
 *            -> moving
 *            -> canceling
 *            -> pending
 */

int ObTTLManager::check_cmd_state_valid(const common::ObTTLTaskStatus current_state,
                                        const common::ObTTLTaskStatus incoming_state)
{
  int ret = OB_SUCCESS;
  switch (incoming_state) {
    case OB_TTL_TASK_RUNNING: {
      if (current_state != OB_TTL_TASK_PENDING && current_state != OB_TTL_TASK_INVALID &&
          current_state != OB_TTL_TASK_RUNNING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("receive rs cmd, but current tenant state is unmatached",
                  K(ret), K(current_state), K(incoming_state));
      }
      break;
    }
    case OB_TTL_TASK_MOVING: {
      if (current_state != OB_TTL_TASK_RUNNING && current_state != OB_TTL_TASK_CANCEL &&
          current_state != OB_TTL_TASK_INVALID && current_state != OB_TTL_TASK_MOVING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("receive a move cmd, current task state is unmatached", K(current_state));
      }
      break;
    }
    case OB_TTL_TASK_PENDING: {
      if (current_state != OB_TTL_TASK_RUNNING && current_state != OB_TTL_TASK_INVALID &&
          current_state != OB_TTL_TASK_PENDING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("receive rs cmd, but current tenant state is unmatached",
                  K(ret), K(current_state), K(incoming_state));
      }
      break;
    }
    case OB_TTL_TASK_CANCEL: {
      if (current_state != OB_TTL_TASK_PENDING && current_state != OB_TTL_TASK_RUNNING &&
          current_state != OB_TTL_TASK_INVALID && current_state != OB_TTL_TASK_CANCEL) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("receive rs cmd, but current tenant state is unmatached",
                  K(ret), K(current_state), K(incoming_state));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid incoming status", K(ret), K(incoming_state));
      break;
    }
  }
  return ret;
}

int ObTTLManager::transform_cmd_to_state(const ObTTLRequestArg::TTLRequestType& cmd, 
                                         common::ObTTLTaskStatus& state)
{
  int ret = OB_SUCCESS;
  if (cmd == ObTTLRequestArg::TTL_TRIGGER_TYPE || cmd == ObTTLRequestArg::TTL_RESUME_TYPE) {
    state = OB_TTL_TASK_RUNNING;
  } else if (cmd == ObTTLRequestArg::TTL_SUSPEND_TYPE) {
    state = OB_TTL_TASK_PENDING;
  } else if (cmd == ObTTLRequestArg::TTL_CANCEL_TYPE) {
    state = OB_TTL_TASK_CANCEL;
  } else if (cmd == ObTTLRequestArg::TTL_MOVE_TYPE) {
    state = OB_TTL_TASK_MOVING;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(cmd), K(state));
  }
  return ret;
}

// get cmd type from rs state
ObTTLRequestArg::TTLRequestType ObTTLManager::transform_state_to_cmd(const int64_t state)
{
  ObTTLRequestArg::TTLRequestType task_type = ObTTLRequestArg::TTL_INVALID_TYPE;
  switch (state) {
    case static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE): {
      task_type = ObTTLRequestArg::TTL_TRIGGER_TYPE;
      break;
    }
    case static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND): {
      task_type = ObTTLRequestArg::TTL_SUSPEND_TYPE;
      break;
    }
    case static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL): {
      task_type = ObTTLRequestArg::TTL_CANCEL_TYPE;
      break;
    }
    case static_cast<int64_t>(ObTTLTaskStatus::OB_RS_TTL_TASK_MOVE): {
      task_type = ObTTLRequestArg::TTL_MOVE_TYPE;
      break;
    }
    default: {
      break;
    }
  }
  return task_type;
}

// RS TTL message entrance
int ObTTLManager::proc_rs_cmd(uint64_t tenant_id, uint64_t task_id, 
                              bool is_usr_trigger, ObTTLRequestArg::TTLRequestType cmd)
{
  int ret = OB_SUCCESS;
  ObTTLTenantInfo* tenant_info = NULL;
  common::ObTTLTaskStatus expected_state;
  const bool create_if_not_exists = true;

  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl manager not init", K(tenant_id), K(task_id), K(is_usr_trigger));
  } else if (task_id == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid task id", K(tenant_id), K(ret));
  } else if (OB_FAIL(transform_cmd_to_state(cmd, expected_state))) {
    LOG_WARN("invalid cmd type", K(tenant_id), K(task_id), K(is_usr_trigger), K(cmd));
  } else {}
  
  if (OB_SUCC(ret)) {
    common::ObSpinLockGuard guard(lock_);
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(tenant_info = get_tenant_info(tenant_id, create_if_not_exists))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get ttl tenant info", K(tenant_id), K(create_if_not_exists));
    } else if (tenant_info->need_check_) {
      ret = OB_EAGAIN;
      LOG_INFO("tenant info need check, please resend message later", KPC(tenant_info), K(expected_state));
    } else if (OB_UNLIKELY(tenant_info->task_id_ != OB_INVALID_ID && tenant_info->task_id_ != task_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ttl task id is wrong", K(ret), K(tenant_id), K(tenant_info->task_id_), K(task_id));
    } else if (OB_FAIL(check_cmd_state_valid(tenant_info->state_, expected_state))) {
      LOG_WARN("ttl cmd state machine is wrong", K(ret), K(tenant_id), K(task_id), K(is_usr_trigger));
    } else {
      tenant_info->cmd_type_ = cmd;
      if (OB_INVALID_ID == tenant_info->task_id_) {
        // new ttl tenant info
        tenant_info->task_id_ = task_id;
        tenant_info->is_usr_trigger_ = is_usr_trigger;
        tenant_info->state_ = expected_state;
        tenant_info->need_check_ = true;
        tenant_info->is_dirty_ = true;
         if (OB_TTL_TASK_MOVING == expected_state) {
          // after restart, rs send moving means all tasks was finished or canceled
          tenant_info->is_finished_ = true; 
        }
        LOG_INFO("new tenent info", K(ret), K(tenant_id), KPC(tenant_info));
      } 
      
      if (OB_TTL_TASK_MOVING == expected_state && !tenant_info->is_finished_) {
        ret = OB_EAGAIN;
        LOG_WARN("can not move a unfinished task", K(ret), K(tenant_id), KPC(tenant_info));
      } else {
        tenant_info->state_ = expected_state;
        tenant_info->is_dirty_ = true;
      }

      if (OB_SUCC(ret)) {
        //receive the msg, need to rsp
        tenant_info->rsp_time_ = ObTimeUtility::current_time();
      }
    }
  }
  LOG_INFO("finish process rs cmd", K(ret), K(tenant_id), K(task_id), K(expected_state));
  return ret;
}

void ObTTLManager::mark_tenant_need_check(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTTLTenantInfo* tenant_info = NULL;
  if (common::ObTTLUtil::check_can_process_tenant_tasks(tenant_id)) {
    common::ObSpinLockGuard guard(lock_);
    if (OB_NOT_NULL(tenant_info = get_tenant_info(tenant_id, false))) {
      tenant_info->need_check_ = true;
    }
  }
  
  LOG_DEBUG("finsh mark tenant need check", K(ret));
}

void ObTTLManager::on_leader_active(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObTTLPara para;
  bool can_ttl = false;
  uint64_t tenant_id = pkey.get_tenant_id();
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl manager not init");
  } else if (!pkey.is_valid()) {
    // do nothing
  } else if(OB_SYS_TENANT_ID == tenant_id) {
    // do nothing
  } else if (!common::ObTTLUtil::check_can_process_tenant_tasks(tenant_id)) {
    //do nothing
  } else if (OB_FAIL(check_partition_can_gen_ttl(pkey, para, can_ttl))) {
    LOG_WARN("fail to check partition can ttl", K(ret), K(pkey), K(para));
  } else if (can_ttl) {
    mark_tenant_need_check(tenant_id);
  } 
}

void ObTTLManager::on_schema_changed(uint64_t schema_changed_tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl manager not init");
  } else if (!common::ObTTLUtil::check_can_process_tenant_tasks(schema_changed_tenant_id)) {
    //do nothing
  } else {
    mark_tenant_need_check(schema_changed_tenant_id);
  }
}

int ObTTLManager::report_task_status(ObTTLTaskInfo& task_info, ObTTLPara& task_para, bool& is_stop)
{
  int ret = OB_SUCCESS;
  ObTTLTenantInfo* tenant_info = NULL;
  ObTTLTaskCtx* ctx = NULL;
  bool task_need_retry = false;
  uint64_t tenant_id = task_info.pkey_.get_tenant_id();

  common::ObSpinLockGuard guard(lock_);
  is_stop = true;
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl manager is not enable,", K(ret), K(is_init_), K(is_timer_start_), K(task_info.pkey_));
  } else if (OB_ISNULL(tenant_info = get_tenant_info(tenant_id, false))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the tenant info in map is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_info->part_task_map_.get_refactored(task_info.pkey_, ctx))) {
    LOG_WARN("fail to get partition task", K(ret), K(tenant_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition task ctx is null", K(ret));
  } else {
    ctx->last_modify_time_ = ObTimeUtility::current_time();
    mark_ttl_ctx_dirty(tenant_info, ctx);
    if (OB_FAIL(deep_copy_task(ctx, task_info))) {
      LOG_WARN("fail to refresh ttl ctx content", K(ret), K(tenant_id));
    } else if (OB_SUCCESS == task_info.err_code_) {
      if (tenant_info->ttl_continue_ && OB_TTL_TASK_RUNNING == tenant_info->state_) {
        task_para = ctx->ttl_para_; //the latest ttl & maxversion
        is_stop = false;
      } else {
        LOG_INFO("pending current task", K(tenant_info->state_), K(tenant_info->ttl_continue_));
      }
    } else if (OB_ITER_END == task_info.err_code_) {
      ctx->task_status_ = OB_TTL_TASK_FINISH;
      ctx->task_info_.err_code_ = OB_SUCCESS;
    } else if (OB_NOT_MASTER == task_info.err_code_ || 
               OB_PARTITION_NOT_EXIST == task_info.err_code_ ||
               OB_TABLE_NOT_EXIST == task_info.err_code_) {
      LOG_INFO("Cancel current task since partition state change", 
              K(task_info.err_code_), K(task_info.pkey_));
      ctx->task_status_ = OB_TTL_TASK_CANCEL;
      ctx->is_invalid_ = true;
    } else {
      LOG_WARN("Task Report error", K(task_info.err_code_), K(task_info.pkey_));
      ctx->task_status_ = OB_TTL_TASK_PENDING;
      ctx->failure_times_++;
    }
  }

  //schedule task
  if (is_stop && OB_FAIL(try_schedule_remaining_tasks(tenant_info, ctx))) {
    LOG_WARN("fail to try schedule task", K(ret));
  } 
  return ret;
}

int ObTTLManager::generate_one_partition_task(ObTTLTaskInfo& task_info, ObTTLPara& para)
{
  int ret = OB_SUCCESS;
  ObTTLTaskCtx* ctx = NULL;
  uint64_t tenant_id = task_info.pkey_.get_tenant_id();
  ObTTLTenantInfo* tenant_info = NULL;
  common::ObSpinLockGuard guard(lock_);

  tenant_info = get_tenant_info(tenant_id, false);
  if (OB_ISNULL(tenant_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get ttl tenant info", K(ret), K(tenant_id));
  } else if (OB_NOT_NULL(ctx = get_one_partition_ctx(task_info.pkey_))) {
    LOG_INFO("ttl ctx exist", K(ret), K(task_info.pkey_));
  } else {
    char *ctx_buf = static_cast<char *>(tenant_info->allocator_.alloc(sizeof(ObTTLTaskCtx)));
    if (OB_ISNULL(ctx_buf)) {
      LOG_WARN("fail to alloc ttl task ctx", K(ret));
    } else {
      ctx = new(ctx_buf)ObTTLTaskCtx();
      ctx->task_status_ = OB_TTL_TASK_PREPARE;
      ctx->ttl_para_ = para;
      mark_ttl_ctx_dirty(tenant_info, ctx);
      ctx->task_info_.pkey_ = task_info.pkey_;
      ctx->task_info_.is_user_trigger_ = tenant_info->is_usr_trigger_;
      ctx->task_info_.task_id_ = tenant_info->task_id_;
      if (OB_FAIL(deep_copy_task(ctx, task_info))) {
        LOG_WARN("fail tp deep copy task", K(ret));
      } else {
        if (OB_FAIL(tenant_info->part_task_map_.set_refactored(task_info.pkey_, ctx))) {
          LOG_WARN("fail to insert ttl task ctx into map", K(ret), K(task_info.pkey_));
          tenant_info->allocator_.free(ctx);
          ctx = NULL;
        }
      }
    }
  }
  LOG_DEBUG("finish generate one partition task", K(ret), K(task_info.pkey_), K(para));
  return ret;
}

void ObTTLManager::mark_tenant_checked(uint64_t tenant_id)
{
  common::ObSpinLockGuard guard(lock_);
  ObTTLTenantInfo* tenant_info = get_tenant_info(tenant_id, false);
  if (OB_ISNULL(tenant_info)) {
    LOG_WARN("fail to get ttl tenant info", K(tenant_id));
  } else {
    tenant_info->need_check_ = false;
  }
}

int ObTTLManager::generate_tenant_tasks(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionArrayGuard partitions;
  ObIPartitionGroup *partition = NULL;
  bool can_ttl = false;

  if (OB_FAIL(ObPartitionService::get_instance().get_all_partitions(partitions))) {
    LOG_WARN("fail to get all partition", K(ret), K(tenant_id));
  } else {
    //filter the partition
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      ObPartitionArray pkeys;
      ObPartitionState state;
      state = partitions.at(i)->get_partition_state();
      if (!is_leader_state(state)) {
        // do nothing, the partition should be a leader
      } else if (OB_FAIL(partitions.at(i)->get_all_pg_partition_keys(pkeys))) {
        LOG_WARN("fail to get all pg partition keys", "pg_key", partitions.at(i)->get_partition_key(), K(pkeys));
      } else {
        for (int pkey_index = 0; OB_SUCC(ret) && pkey_index < pkeys.count(); ++pkey_index) {
          ObTTLPara para;
          ObPartitionKey pkey = pkeys.at(pkey_index);
          if ((tenant_id != pkey.get_tenant_id())) {
            //do nothing
          } else if (OB_FAIL(check_partition_can_gen_ttl(pkey, para, can_ttl))) {
            LOG_WARN("fail to check partition can get ttl", K(ret), K(pkey), K(para));
          } else {
            if (can_ttl) {
              ObTTLTaskInfo task_info;
              task_info.pkey_ = pkey;
              if (OB_FAIL(generate_one_partition_task(task_info, para))) {
                LOG_WARN("fail to generate task", K(ret), K(task_info), K(para));
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      mark_tenant_checked(tenant_id);
    }
  }
  LOG_DEBUG("finish generate tenant tasks", K(ret), K(tenant_id));
  return ret;
}

void OBTTLTimerPeriodicTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (common::ObTTLUtil::check_can_do_work()) {
    if (OB_FAIL(ObTTLManager::get_instance().check_all_tenant_mem())) {
      LOG_WARN("fail to check all tenant memory", K(ret));
    }

    int tmp_ret = ret;
    if (OB_FAIL(ObTTLManager::get_instance().scan_all_tenanat_handle_event())) {
      LOG_WARN("fail to scan and handle all tenant event", K(ret), K(tmp_ret));
    }
  }
}

/*get single tenant or partition info*/
ObTTLManager::ObTTLTenantInfo* ObTTLManager::get_tenant_info(uint64_t tenant_id, bool create_if_not_exist)
{

  int ret = OB_SUCCESS;
  ObTTLTenantInfo* tenant_info = NULL;

  if (OB_FAIL(ttl_tenant_parts_map_.get_refactored(tenant_id, tenant_info)) ) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      if (create_if_not_exist) {
        ret = OB_SUCCESS;
        LOG_INFO("begin to create tenant ttl task", K(tenant_id));
        tenant_info = (ObTTLTenantInfo*)allocator_.alloc(sizeof(ObTTLTenantInfo));
        if (OB_ISNULL(tenant_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to alloc mem for ttl tenant info", K(ret));
        } else {
          new(tenant_info)ObTTLTenantInfo();
          tenant_info->tenant_id_ = tenant_id;
          tenant_info->ttl_continue_ = false;
          tenant_info->is_dirty_ = true;
          if(OB_FAIL(tenant_info->part_task_map_.create(DEFAULT_TTL_BUCKET_NUM, 
                  ObModIds::TABLE_TTL, ObModIds::TABLE_TTL))) {
            LOG_WARN("fail to create ttl partition map", K(ret), K(tenant_id), K(create_if_not_exist));
          } else if (OB_FAIL(ttl_tenant_parts_map_.set_refactored(tenant_id, tenant_info))) {
            LOG_WARN("fail to insert ttl tenant info into map", K(ret));
          } 
        }

        if (OB_FAIL(ret) && OB_NOT_NULL(tenant_info)) {
          LOG_WARN("fail to create tenant info", K(ret));
          tenant_info->part_task_map_.destroy();
          allocator_.free(tenant_info);
          tenant_info = NULL;
        }
      } 
    } 
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fatel error, tenant info in map is null", K(ret));
  }
  return tenant_info;
}

ObTTLTaskCtx* ObTTLManager::get_one_partition_ctx(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObTTLTenantInfo* tenant_info = NULL;
  ObTTLTaskCtx* ttl_task_ctx = NULL;

  if (OB_FAIL(ttl_tenant_parts_map_.get_refactored(pkey.get_tenant_id(), tenant_info))) {
    LOG_DEBUG("fail to get tenant info", K(ret), K(pkey.get_tenant_id()));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the tenant info in map is null", K(ret));
  } else if (OB_FAIL(tenant_info->part_task_map_.get_refactored(pkey, ttl_task_ctx))) {
    LOG_DEBUG("fail to get partition task", K(ret), K(pkey.get_tenant_id()));
  } else if (OB_ISNULL(ttl_task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition task ctx is null", K(ret));
  } 
  return ttl_task_ctx;
}

/*other inner function*/
int ObTTLManager::deep_copy_task(ObTTLTaskCtx* ctx, ObTTLTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ctx is null", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, task_info.row_key_, ctx->task_info_.row_key_)) ) {
    LOG_WARN("fail to deep copy first key", K(ret), K(task_info.row_key_));
  } else {
    ctx->task_info_.ttl_del_cnt_ = task_info.ttl_del_cnt_;
    ctx->task_info_.max_version_del_cnt_ = task_info.max_version_del_cnt_;
    ctx->task_info_.scan_cnt_ = task_info.scan_cnt_;
    ctx->task_info_.err_code_ = task_info.err_code_;
  }
  return ret;
}

int ObTTLManager::generate_ttl_dag(ObTTLTaskInfo& task_info, ObTTLPara& para)
{
  int ret = OB_SUCCESS;
  ObDagScheduler &dag_scheduler = ObDagScheduler::get_instance();
  ObTableTTLDag *dag = NULL;
  ObTableTTLDeleteTask *delete_task = NULL;

  if (OB_FAIL(dag_scheduler.alloc_dag(dag))) {
    LOG_WARN("fail to alloc dag", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, dag is null", K(ret), KP(dag));
  } else if (OB_FAIL(dag->init(para, task_info))) {
    LOG_WARN("fail to init ttl dag", K(ret), K(task_info.pkey_));
  } else if (OB_FAIL(dag->alloc_task(delete_task))) {
    LOG_WARN("fail to alloc ttl prepare task", K(ret));
  } else if (OB_ISNULL(delete_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, prepare task is null", K(ret), KP(delete_task));
  } else if (OB_FAIL(delete_task->init(para, task_info))) {
    LOG_WARN("fail to init ttl prepare task", K(ret));
  } else if (OB_FAIL(dag->add_task(*delete_task))) {
    LOG_WARN("fail to add ttl prepare task to dag", K(ret));
  } else if (OB_FAIL(dag_scheduler.add_dag(dag))) {
    if (OB_EAGAIN == ret) {
      LOG_DEBUG("ttl dag already exists, no need to schedule once again", K(ret));
    } else if (OB_SIZE_OVERFLOW == ret) {
      LOG_DEBUG("dag is full", K(ret));
    } else {
      LOG_WARN("fail to add dag to queue", K(ret));
    }
  }

  LOG_INFO("obs get rpc to build table api ttl dag", K(ret), K(task_info.pkey_), K(para));  
  return ret;
}

int ObTTLManager::inner_handle_single_tenant_event(uint64_t tenant_id, common::ObSArray<ObPartitionKey>& parts_array)
{
  int ret = OB_SUCCESS;
  bool upd_sys_table = false;
  ObTTLTenantInfo* tenant_info = NULL;
  ObTTLTaskCtx* ctx = NULL;
  common::ObSpinLockGuard guard(lock_);
  if (OB_ISNULL(tenant_info = (ObTTLTenantInfo*)get_tenant_info(tenant_id, false))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get ttl tenant info", K(ret));
  } else {
    for (ttl_parts_iterator iter = tenant_info->part_task_map_.begin(); 
                            iter != tenant_info->part_task_map_.end(); ++iter) {
      ctx = iter->second;
      if (OB_ISNULL(ctx)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fatal err, ttl ctx in map is null", K(tenant_info->tenant_id_), K(ret));
      } else if (OB_FAIL(inner_handle_one_partition_event(tenant_info, ctx))) {
        LOG_WARN("fail to handle one partition event", K(ret), K(ctx->task_info_.pkey_));
      } else if (ctx->is_dirty_ && OB_FAIL(parts_array.push_back(ctx->task_info_.pkey_))) {
        LOG_WARN("fail to pushback ttl pk", K(ret), K(ctx->task_info_.pkey_)); 
      }
    }
  }
  
  LOG_DEBUG("finish handle single tenant event", K(ret), K(tenant_id));
  return ret;
}

/*just change memory status*/
int ObTTLManager::inner_handle_one_partition_event(ObTTLTenantInfo* tenant_info, ObTTLTaskCtx* ctx)
{
  int ret = OB_SUCCESS;
  bool try_schedule = false;
  
  if (OB_ISNULL(ctx) || OB_ISNULL(tenant_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("tenant info or ctx is null", K(ret), K(tenant_info), K(ctx));
  } else if (ctx->task_status_ != tenant_info->state_) {
    if (OB_TTL_TASK_RUNNING == tenant_info->state_) {
      if (OB_TTL_TASK_PENDING == ctx->task_status_) {
        try_schedule = true;
      } else if (OB_TTL_TASK_PREPARE == ctx->task_status_ ||
                 OB_TTL_TASK_FINISH == ctx->task_status_ ||
                 OB_TTL_TASK_CANCEL == ctx->task_status_) {
        // do nothing
      } else {
        LOG_WARN("no expected task status", KPC(tenant_info), KPC(ctx));
      }
    } else if (OB_TTL_TASK_PENDING == tenant_info->state_) {
      if (OB_TTL_TASK_RUNNING == ctx->task_status_ ||
          OB_TTL_TASK_PREPARE == ctx->task_status_) {
        ctx->task_status_ = tenant_info->state_;
        mark_ttl_ctx_dirty(tenant_info, ctx);
      } else if (OB_TTL_TASK_FINISH == tenant_info->state_){
        // do nothing, no need schedule finish task again
      } else {
        LOG_WARN("no expected task status", KPC(tenant_info), KPC(ctx));
      }
    } else if (OB_TTL_TASK_CANCEL == tenant_info->state_) {
      if (OB_TTL_TASK_PREPARE == ctx->task_status_ ||
          OB_TTL_TASK_RUNNING == ctx->task_status_ ||
          OB_TTL_TASK_PENDING == ctx->task_status_ ||
          OB_TTL_TASK_FINISH == ctx->task_status_) {
        ctx->task_status_ = tenant_info->state_;
        mark_ttl_ctx_dirty(tenant_info, ctx);
      } else {
        LOG_WARN("no expected task status", KPC(tenant_info), KPC(ctx));
      }
    } else if (OB_TTL_TASK_MOVING == tenant_info->state_) {
      if (OB_TTL_TASK_PREPARE == ctx->task_status_) {
        ctx->task_status_ = OB_TTL_TASK_FINISH; // will refresh real status from task table
        mark_ttl_ctx_dirty(tenant_info, ctx);
      } else if (OB_TTL_TASK_FINISH == ctx->task_status_ ||
                 OB_TTL_TASK_CANCEL == ctx->task_status_) {
        // do nothing, normal partition task
      } else {
        LOG_WARN("no expected task status", KPC(tenant_info), KPC(ctx));
      }
    } else {
      LOG_WARN("invalid ttl tenant task state", K(tenant_info->state_));
    }

    if (try_schedule && OB_FAIL(try_schedule_task(tenant_info, ctx))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("fail to try schedule dag task", K(ret), K(ctx->task_info_.pkey_));
      } else {
        ret = OB_SUCCESS;
      }
    }
    LOG_DEBUG("handle one partition event", K(ret), K(ctx->task_status_), K(tenant_info->state_));
  }
  return ret;
}

void ObTTLManager::cancel_all_tenant_task()
{
  ObTTLTenantInfo* tenant_info = NULL;
  {
    common::ObSpinLockGuard guard(lock_);
    for (ttl_tenants_iterator iter = ttl_tenant_parts_map_.begin();
                                iter != ttl_tenant_parts_map_.end(); ++iter) {
      tenant_info = iter->second;
      if (OB_ISNULL(tenant_info)) {
        LOG_WARN("tenant info in the map is null", K(iter->first));
      } else {
        tenant_info->state_ = OB_TTL_TASK_CANCEL;
        tenant_info->is_dirty_ = true;
      }
    }
  }
  scan_all_tenanat_handle_event();
}

int ObTTLManager::check_all_tenant_mem()
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl manager not init");
  } else {
    common::ObSpinLockGuard guard(lock_);
    ObTTLTenantInfo* tenant_info = NULL;
    for (ttl_tenants_iterator iter = ttl_tenant_parts_map_.begin();
                              iter != ttl_tenant_parts_map_.end(); ++iter) {
      uint64_t tenant_id = iter->first;                            
      tenant_info = iter->second;
      if (OB_ISNULL(tenant_info) || OB_INVALID_TENANT_ID == tenant_id) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), KP(tenant_info), K(tenant_id));
      } else if (!common::ObTTLUtil::check_can_process_tenant_tasks(tenant_id)) {
        tenant_info->ttl_continue_ = false;
        LOG_DEBUG("will stop all the running task", K(tenant_id));
      } else {
        int64_t active_memstore_used = 0;
        int64_t total_memstore_used = 0;
        int64_t minor_freeze_trigger = 0;
        int64_t memstore_limit = 0;
        int64_t freeze_cnt = 0;
        if (OB_FAIL(common::ObTenantManager::get_instance().get_tenant_memstore_cond(
                                tenant_info->tenant_id_,
                                active_memstore_used,
                                total_memstore_used,
                                minor_freeze_trigger,
                                memstore_limit,
                                freeze_cnt)) ) {
          LOG_WARN("fail to get tenant memstore info for tenant ", K(ret), K(tenant_id));
        } else if (total_memstore_used > minor_freeze_trigger) {
          if (tenant_info->ttl_continue_) {
            tenant_info->ttl_continue_ = false;
            LOG_INFO("will stop all the running ttl task",  K(tenant_id), K(total_memstore_used), K(minor_freeze_trigger));
          }
        } else {
          if (!tenant_info->ttl_continue_) {
            tenant_info->ttl_continue_ = true;
            LOG_INFO("continue to execute all the ttl task", K(tenant_id), K(total_memstore_used), K(minor_freeze_trigger));
          }
        }
      }
    }
  }

  return ret;
}

int ObTTLManager::get_ttl_para_from_schema(const schema::ObTableSchema *table_schema, 
                                           ObTTLPara& para, bool& can_ttl)
{
  table::ObHColumnDescriptor hc_desc;
  int ret = OB_SUCCESS;
  can_ttl = false;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is null", K(ret));
  } else if (!table_schema->get_comment_str().empty()) {
    if (!is_ttl_comment(table_schema->get_comment_str())) {
      // do nothing
    } else if (OB_FAIL(hc_desc.from_string(table_schema->get_comment_str()))) {
      LOG_WARN("fail to get ttl para from schema", K(table_schema->get_comment_str()), K(ret));
    } else {
      para.ttl_ =  hc_desc.get_time_to_live();
      para.max_version_ = hc_desc.get_max_version();
      if (OB_LIKELY(para.is_valid())) {
        can_ttl = true;
        LOG_DEBUG("success to find a ttl partition", K(ret), K(para));
      }
    }
  } else {}
  return ret;
}

bool ObTTLManager::is_ttl_comment(const ObString &str)
{
  bool bret = false;
  const int32_t length = str.length(); 
  const char *start = str.ptr();
  const char *end = str.ptr() + length;

  while (start < end && isspace(*start)) {
    start++;
  }
  if (start < end && *start == '{') {
    start++;
    while (start < end && isspace(*start)) {
      start++;
    }
    if (start < end) {
      const char *hcolumn_str = "\"HColumnDescriptor\"";
      const uint32_t hcolumn_str_len = strlen(hcolumn_str); 
      if (end - start >= hcolumn_str_len) {
        bret = (0 == STRNCASECMP(start, hcolumn_str, hcolumn_str_len));
      }
    }
  }

  return bret;
}

int ObTTLManager::check_partition_can_gen_ttl(const ObPartitionKey& pkey,
                                              ObTTLPara &para, bool& can_ttl)
{
  int ret = OB_SUCCESS;
  const schema::ObTableSchema *table_schema = NULL;
  schema::ObSchemaGetterGuard schema_guard;
  ObTTLTaskCtx* ttl_ctx = NULL;
  can_ttl = false;
  
  if (OB_UNLIKELY(!pkey.is_valid() || pkey.is_pg())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(pkey.is_pg()));
  } else {
    uint64_t tenant_id = pkey.get_tenant_id();
    uint64_t table_id = pkey.get_table_id();
    if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("get table schema failed", K(table_id), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("null table schema", K(ret));
    } else if (table_schema->is_in_recyclebin()) {
      // do nothing
    } else if (table_schema->get_comment_str().empty()) {
      // do nothing
    } else if (OB_NOT_NULL(ttl_ctx = get_one_partition_ctx(pkey))) {
      LOG_DEBUG("partition task exist", K(pkey));
    } else if (OB_FAIL(get_ttl_para_from_schema(table_schema, para, can_ttl))) {
      LOG_WARN("fail to get ttl para", K(pkey));
    } else {}
  }
  
  return ret;
}

int ObTTLManager::try_schedule_prepare_task(ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  ObTTLTenantInfo* tenant_info = get_tenant_info(pkey.get_tenant_id(), false);
  ObTTLTaskCtx* ctx = get_one_partition_ctx(pkey);
  if (OB_ISNULL(tenant_info) || OB_ISNULL(ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("ttl tenant info or ctx is null", K(ret));
  } else if (ctx->task_status_ != OB_TTL_TASK_PREPARE) {
    // do nothing
  } else if (FALSE_IT(ctx->task_status_ = OB_TTL_TASK_PENDING)) {
  } else if (OB_FAIL(try_schedule_task(tenant_info, ctx))) {
    if (OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("fail to schedule task", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } 
  LOG_DEBUG("try schedule prepare task", K(ret), K(pkey.get_tenant_id()), K(pkey.get_table_id()));
  return ret;
}

int ObTTLManager::sync_sys_table(ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(lib::ObLabel("TTLStatusRecord"));
  uint64_t tenant_id = pkey.get_tenant_id();
  ObTTLTaskCtx* ctx = NULL;

  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else {
    common::ObSpinLockGuard guard(lock_);
    ctx = get_one_partition_ctx(pkey);
    if (OB_ISNULL(ctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ctx is null", K(ret));
    } else if (OB_UNLIKELY(!ctx->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid partition task ctx", K(ret), KPC(ctx));
    } else {}
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(ctx->need_refresh_)) {
    switch (ctx->task_status_) {
      case OB_TTL_TASK_FINISH: // tenant_info must be in moving status
      case OB_TTL_TASK_PREPARE: {
        if (OB_FAIL(refresh_partition_task(*ctx, true /*refresh_status*/, true))) {
          LOG_WARN("fail to refresh partition task from task table", K(ret));
        } else {
          if (ctx->task_info_.err_code_ == OB_NOT_MASTER ||
              (ctx->task_status_ != OB_TTL_TASK_FINISH && ctx->task_status_ != OB_TTL_TASK_CANCEL)) {
            ctx->task_status_ = OB_TTL_TASK_PREPARE;
          }
          ctx->need_refresh_ = false;
          ctx->task_info_.err_code_ = OB_SUCCESS;
        }
        break;
      }
      case OB_TTL_TASK_RUNNING: 
      case OB_TTL_TASK_PENDING: 
      case OB_TTL_TASK_CANCEL: {
        if (OB_FAIL(refresh_partition_task(*ctx, false /*refresh_status*/))) {
          LOG_WARN("fail to refresh partition task from task table", K(ret));
        } else {
          ctx->need_refresh_ = false;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ttl task status", K(ret));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    common::ObTTLStatus ttl_record; 
    switch (ctx->task_status_) {
      case OB_TTL_TASK_PREPARE: {
        ObMySQLTransaction trans;
        ObTTLStatusFieldArray filters;
        common::ObTTLStatusArray ttl_records;
        ObTTLStatusFieldArray filter;
        bool commit = false;
        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(construct_task_record_filter(ctx->task_info_.task_id_,
                                                 ctx->task_info_.pkey_.get_table_id(),
                                                 ctx->task_info_.pkey_.get_partition_id(),
                                                 filters))) {
          LOG_WARN("fail to construct task record filter", K(ret));
        } else if (OB_FAIL(trans.start(get_sql_proxy(), tenant_id))) {
          LOG_WARN("fail to start transation", K(ret), K(tenant_id));
        } else if (OB_FAIL(ObTTLUtil::read_ttl_tasks(tenant_id, share::OB_ALL_KV_TTL_TASK_TNAME,
                                                     trans, filters, ttl_records, true, &allocator))) {
          LOG_WARN("fail to get ttl tasks", K(ret));
        } else {
          if (ttl_records.empty()) {
            if (OB_FAIL(construct_sys_table_record(ctx, ttl_record))) {
              LOG_WARN("fail to construct sys table record", K(ret));
            } else if (OB_FAIL(ObTTLUtil::insert_ttl_task(tenant_id, share::OB_ALL_KV_TTL_TASK_TNAME,
                                                          trans, ttl_record))) {
              LOG_WARN("fail to insert ttl task", K(ret));
            }
          } else {
            if (ttl_records.count() != 1) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpect ttl records count", K(ret), K(ttl_records.count()));
            } else { /* do nothing, prepare task only sync once with true need_refrsh flag */ }
          }
        }
       
        if (trans.is_started()) {
          bool commit = (OB_SUCCESS == ret);
          int tmp_ret = ret;
          if (OB_FAIL(trans.end(commit))) {
            LOG_WARN("faile to end trans", "commit", commit, K(ret));
          }
          ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
        }
        
        // change prepare state to running/pending
        if (OB_SUCC(ret) && OB_FAIL(try_schedule_prepare_task(pkey))) { 
          LOG_WARN("fail to schedule prepare task", K(ret));
        }
        break;
      }
      case OB_TTL_TASK_FINISH: 
      case OB_TTL_TASK_RUNNING: 
      case OB_TTL_TASK_PENDING: 
      case OB_TTL_TASK_CANCEL: {
        ObMySQLTransaction trans;
        ObTTLStatusFieldArray filters;
        common::ObTTLStatusArray ttl_records;
        ObTTLStatusFieldArray filter;
        bool commit = false;
        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(construct_task_record_filter(ctx->task_info_.task_id_,
                                                 ctx->task_info_.pkey_.get_table_id(),
                                                 ctx->task_info_.pkey_.get_partition_id(),
                                                 filters))) {
          LOG_WARN("fail to construct task record filter", K(ret));
        } else if (OB_FAIL(trans.start(get_sql_proxy(), tenant_id))) {
          LOG_WARN("fail to start transation", K(ret));
        } else if (OB_FAIL(ObTTLUtil::read_ttl_tasks(tenant_id, share::OB_ALL_KV_TTL_TASK_TNAME,
                                                     trans, filters, ttl_records, true, &allocator))) {
          LOG_WARN("fail to get ttl tasks", K(ret));
        } else {
          if (ttl_records.empty()) {
            if (OB_FAIL(construct_sys_table_record(ctx, ttl_record))) {
              LOG_WARN("fail to construct sys table record", K(ret));
            } else if (OB_FAIL(ObTTLUtil::insert_ttl_task(tenant_id, share::OB_ALL_KV_TTL_TASK_TNAME,
                                                          trans, ttl_record))) {
              LOG_WARN("fail to insert ttl task", K(ret));
            }
          } else {
            if (ttl_records.count() != 1) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpect ttl records count", K(ret), K(ttl_records.count()));
            } else if (OB_FAIL(construct_sys_table_record(ctx, ttl_record))) {
              LOG_WARN("fail to construct sys table record", K(ret));
             } else if (OB_FAIL(ObTTLUtil::update_ttl_task_all_fields(tenant_id, 
                                                                share::OB_ALL_KV_TTL_TASK_TNAME,
                                                                trans, ttl_record))) {
              LOG_WARN("fail to update ttl task in sys table", K(ret), K(ttl_record));
            }
          }
        }
       
        if (trans.is_started()) {
          bool commit = (OB_SUCCESS == ret);
          int tmp_ret = ret;
          if (OB_FAIL(trans.end(commit))) {
            LOG_WARN("faile to end trans", "commit", commit, K(ret));
          }
          ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ttl task status", K(ret));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    //mark ctx dirty false
    common::ObSpinLockGuard guard(lock_);
    ObTTLTaskCtx* ctx = get_one_partition_ctx(pkey);
    if (OB_ISNULL(ctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ctx is null", K(ret));
    } else {
      ctx->is_dirty_ = false;
    }
  }
  LOG_DEBUG("finish sync sys table", K(ret), K(pkey));
  return ret;
}

int ObTTLManager::construct_sys_table_record(ObTTLTaskCtx* ctx, common::ObTTLStatus& ttl_record)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaild null ttl task ctx", K(ret));
  } else if (OB_UNLIKELY(!ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(ctx));
  } else {
    ttl_record.tenant_id_ = ctx->task_info_.pkey_.get_tenant_id();
    ttl_record.table_id_ = ctx->task_info_.pkey_.get_table_id();
    ttl_record.partition_id_ = ctx->task_info_.pkey_.get_partition_id();
    ttl_record.task_id_ = ctx->task_info_.task_id_;

    ttl_record.task_start_time_ = ctx->task_start_time_;
    ttl_record.task_update_time_ = ctx->last_modify_time_;
    ttl_record.trigger_type_ = static_cast<int>(ctx->task_info_.is_user_trigger_); 
    ttl_record.status_ = static_cast<int>(ctx->task_status_);

    ttl_record.ttl_del_cnt_ = ctx->task_info_.ttl_del_cnt_;
    ttl_record.max_version_del_cnt_ = ctx->task_info_.max_version_del_cnt_;
    ttl_record.scan_cnt_ = ctx->task_info_.scan_cnt_;
    ttl_record.row_key_ = ctx->task_info_.row_key_; // shallow copy
    ttl_record.ret_code_ = common::ob_error_name(ctx->task_info_.err_code_);
  }
  return ret;
}

int ObTTLManager::construct_task_record_filter(const uint64_t& task_id,
                                               const uint64_t& table_id,
                                               const uint64_t& partition_id,
                                               ObTTLStatusFieldArray& filter)
{
  int ret = OB_SUCCESS;
  ObTTLStatusField task_id_field;
  task_id_field.field_name_ = ObString("task_id");
  task_id_field.type_ = ObTTLStatusField::UINT_TYPE;
  task_id_field.data_.uint_ = task_id ;

  ObTTLStatusField table_id_field;
  table_id_field.field_name_ = ObString("table_id");
  table_id_field.type_ = ObTTLStatusField::UINT_TYPE;
  table_id_field.data_.uint_ = table_id;

  ObTTLStatusField partition_id_field;
  partition_id_field.field_name_ = ObString("partition_id");
  partition_id_field.type_ = ObTTLStatusField::UINT_TYPE;
  partition_id_field.data_.uint_ = partition_id;

  if (OB_FAIL(filter.push_back(task_id_field))) {
    LOG_WARN("failt to push back", K(ret));
  } else if (OB_FAIL(filter.push_back(table_id_field))) {
    LOG_WARN("failt to push back", K(ret));
  } else if (OB_FAIL(filter.push_back(partition_id_field))) {
    LOG_WARN("failt to push back", K(ret));
  }
  return ret;
}

int ObTTLManager::copy_all_tenant_ctxs(common::ObSArray<ObTTLTaskCtx *>& ctx_array, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  ObTTLTenantInfo* tenant_info = get_tenant_info(tenant_id, false);
  ObTTLTaskCtx* ctx = NULL;
  if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to get ttl tenant info", K(ret));
  } else {
    for (ttl_parts_iterator iter = tenant_info->part_task_map_.begin(); 
            OB_SUCC(ret) && iter != tenant_info->part_task_map_.end(); ++iter) {
      ctx = iter->second;
      if (OB_ISNULL(ctx)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fatal err, ttl ctx in map is null", K(tenant_info->tenant_id_), K(ret));
      } else if (OB_FAIL(ctx_array.push_back(ctx))) {
        LOG_WARN("fail to push back ctx array", K(ret));
      }
    }
  }
  return ret;
}

int ObTTLManager::move_record_to_history_table(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator;
  common::ObSArray<ObTTLTaskCtx *> need_move_ctxs;
  uint64_t move_rows_cnt = 0;
  ObTTLTenantInfo* tenant_info = get_tenant_info(tenant_id, false);

  if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to get ttl tenant info", K(ret));
  } else if (!tenant_info->is_finished_) {
    LOG_INFO("new partition leader on , cannot move right now", K(ret), KPC(tenant_info));  
  } else if (OB_FAIL(copy_all_tenant_ctxs(need_move_ctxs, tenant_id))) {
    LOG_WARN("fail to deep copy ctx", K(ret));
  } else {
    for (int i = 0; i < need_move_ctxs.count() && OB_SUCC(ret); ++i) {
      if (!need_move_ctxs.at(i)->is_moved_) {
        ObMySQLTransaction trans;
        common::ObTTLStatus ttl_record;
        if (OB_FAIL(construct_sys_table_record(need_move_ctxs.at(i), ttl_record))) {
          LOG_WARN("fail to construct sys table record", K(ret), KPC(need_move_ctxs.at(i)));
        } else if (OB_FAIL(trans.start(get_sql_proxy(), ttl_record.tenant_id_))) {
          LOG_WARN("failt to start trans", K(ret), K(ttl_record.tenant_id_));
        } else {
          ObTTLStatusKey key(ttl_record.tenant_id_, ttl_record.table_id_, 
                            ttl_record.partition_id_, ttl_record.task_id_);
          int64_t affected_rows = 0;
          if (OB_FAIL(common::ObTTLUtil::delete_ttl_task(ttl_record.tenant_id_,
                                                         share::OB_ALL_KV_TTL_TASK_TNAME,
                                                         trans, key, affected_rows))) {
            LOG_WARN("fail to delete ttl record in __all_kv_ttl_task", K(ret));
          } else {
            if (affected_rows == 1) {
              // NOTE: use replace instead of insert , because when old partition leader
              // move record to history table first, new partition leader may write partition task
              // into task table and move, it will cause OB_ERR_PRIMARY_KEY_DUPLICATE 
              if (OB_FAIL(ObTTLUtil::replace_ttl_task(ttl_record.tenant_id_,
                                                      share::OB_ALL_KV_TTL_TASK_HISTORY_TNAME,
                                                      trans, ttl_record))) {
                LOG_WARN("fail to replace into ttl task into __all_kv_ttl_task_history.", K(ret));
              }
            } else if (affected_rows == 0) {
              LOG_INFO("delete affecte 0 row, record maybe moved by other observer", K(ret));
            } else {
              LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
            }
          }

          bool commit = (OB_SUCCESS == ret);
          int tmp_ret = ret;
          if (OB_FAIL(trans.end(commit))) {
            LOG_WARN("fail to end transaction", K(ret), K(commit));
          }
          ret = OB_SUCCESS == tmp_ret ? ret : tmp_ret;

          if (OB_SUCC(ret)) {
            need_move_ctxs.at(i)->is_moved_ = true; 
            move_rows_cnt += affected_rows;
          }
        }
      }
    }
  }
  LOG_INFO("finish move record to history table", K(ret), K(tenant_id), K(need_move_ctxs), K(move_rows_cnt));
  return ret;
}


int ObTTLManager::response_ttl_cmd(const uint64_t& tenant_id, const uint64_t& task_id, 
                                   ObTTLRequestArg::TTLRequestType cmd_type)
{
  int ret = OB_SUCCESS;
  ObTTLRequestArg::TTLRequestType cmd;
  if (OB_UNLIKELY(!is_init_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTTLManager has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == task_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(task_id));
  } else {
    ObTTLResponseArg arg;
    arg.tenant_id_ = tenant_id;
    arg.task_id_ = task_id;
    arg.server_addr_ = GCTX.self_addr_; 
    arg.task_status_ = static_cast<uint8_t>(cmd_type);
    ObAddr rs_addr;
    if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("innner system error, rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("fail to get rootservice address", K(ret), K(rs_addr));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).ttl_response(arg))) {
      if (OB_TENANT_NOT_EXIST == ret && OB_FAIL(mark_tenant_droped(tenant_id))) {
        LOG_WARN("fail to mark tenant droped", K(ret), K(tenant_id));
      } else {
        LOG_WARN("fail to send ttl response to rs", K(ret), K(arg));
      }
    }
  }
  LOG_INFO("finish reponse ttl task status", K(ret), K(tenant_id), K(task_id), K(cmd_type));
  return ret;
}

int ObTTLManager::mark_tenant_droped(const uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  ObTTLTenantInfo* tenant_info = get_tenant_info(tenant_id, false);
  if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("tenant info is null", K(tenant_id));
  } else {
    bool tenant_not_exist = false;
    schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_schema_guard(schema_guard))) { // double check
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(tenant_id, tenant_not_exist))) {
      LOG_WARN("fail to check tenant exists", K(ret), K(tenant_id)); 
    } else if (tenant_not_exist) {
      // tenant_info->is_dirty_ = false; // no need to scan and sync sys table
      // tenant_info->need_check_ = false; // no need to check partitions
      // tenant_info->rsp_time_ == OB_INVALID_ID; // no need response
      // tenant_info->state_ = OB_TTL_TASK_INVALID;
      // tenant_info->is_droped_ = true; 
      // tenant_info->part_task_map_.reuse();
      if (OB_FAIL(ttl_tenant_parts_map_.erase_refactored(tenant_id))) {
        LOG_WARN("fail to erase tenant info", K(tenant_id));
      } else {
        tenant_info->destory();
        allocator_.free(tenant_info);
      }
      LOG_INFO("tenant is droped, drop tenant info directly", K(tenant_id));
    }
  }
  return ret;
}

int ObTTLManager::check_and_reset_droped_tenant()
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  for (ttl_tenants_iterator iter = ttl_tenant_parts_map_.begin();
                            iter != ttl_tenant_parts_map_.end() && OB_SUCC(ret); ++iter) {
    uint64_t tenant_id = iter->first;
    ObTTLTenantInfo *tenant_info = iter->second;
    if (OB_ISNULL(tenant_info)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("tenant info is null", K(ret), K(tenant_id));
    } else {
      bool tenant_not_exist = false;
      schema::ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_schema_guard(schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret));
      } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(tenant_id, tenant_not_exist))) {
        LOG_WARN("fail to check tenant exists", K(ret), K(tenant_id)); 
      } else if (tenant_not_exist) {
        tenant_info->is_dirty_ = false; // no need to scan and sync sys table
        tenant_info->need_check_ = false; // no need to check partitions
        tenant_info->rsp_time_ == OB_INVALID_ID; // no need response
        tenant_info->state_ = OB_TTL_TASK_INVALID;
        tenant_info->is_droped_ = true; 
        tenant_info->part_task_map_.reuse();
        LOG_INFO("tenant is droped, reset partition task map", K(tenant_id), KPC(tenant_info));
      }
    }
  }
  return ret;
}

int ObTTLManager::from_ttl_record(ObPartitionKey& pkey, common::ObTTLStatus& record, bool with_status /*true*/, bool with_err_code /*true*/)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    ObTTLTenantInfo *tenant_info = get_tenant_info(pkey.get_tenant_id(), false);
    ObTTLTaskCtx* ctx = get_one_partition_ctx(pkey);
    if (OB_ISNULL(ctx) || OB_ISNULL(tenant_info)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("unexpected null value", K(ret), KP(ctx), KP(tenant_info));
    } else if (pkey.get_tenant_id() != record.tenant_id_ ||
               pkey.get_partition_id() != record.partition_id_ ||
               pkey.get_table_id() != record.table_id_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fatel error, record not match pkey", K(pkey), K(record));
    } else {
      ctx->task_info_.pkey_ = pkey;
      ctx->task_info_.task_id_ = record.task_id_;
      ctx->task_start_time_ = record.task_start_time_;
      ctx->task_end_time_ = record.task_update_time_;
      ctx->task_info_.is_user_trigger_ = record.trigger_type_ == TRIGGER_TYPE::USER_TRIGGER;
      ctx->task_info_.ttl_del_cnt_ = record.ttl_del_cnt_;
      ctx->task_info_.max_version_del_cnt_ = record.max_version_del_cnt_;
      ctx->task_info_.scan_cnt_ = record.scan_cnt_;
      if (with_err_code) {
        if (record.ret_code_.compare("OB_SUCCESS") == 0) {
          ctx->task_info_.err_code_ = OB_SUCCESS;  
        } else if (record.ret_code_.compare("OB_NOT_MASTER") == 0) {
          ctx->task_info_.err_code_ = OB_NOT_MASTER;  
        } else {
          ctx->task_info_.err_code_ = OB_INVALID_ERROR;
        }
      }
      if (with_status) {
        ctx->task_status_ = static_cast<ObTTLTaskStatus>(record.status_);
      }
      if (!record.row_key_.empty()) {
        char *rowkey_buf = static_cast<char *>(tenant_info->allocator_.alloc(record.row_key_.length()));
        if (OB_ISNULL(rowkey_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else {
          MEMCPY(rowkey_buf, record.row_key_.ptr(), record.row_key_.length());
          ctx->task_info_.row_key_.assign(rowkey_buf, record.row_key_.length());
        }
      }
    }
  }
  LOG_DEBUG("finish from ttl record", K(ret), K(pkey));
  return ret;  
}

bool ObTTLManager::can_schedule_tenant(const ObTTLTenantInfo &tenant_info)
{
  return tenant_info.ttl_continue_ && tenant_info.state_ == OB_TTL_TASK_RUNNING;
}

bool ObTTLManager::can_schedule_task(const ObTTLTaskCtx &ttl_task)
{
  return ttl_task.task_status_ == OB_TTL_TASK_PENDING;
}

int ObTTLManager::try_schedule_remaining_tasks(ObTTLTenantInfo* tenant_info, const ObTTLTaskCtx *current_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null tenant info", K(ret));
  } else if (can_schedule_tenant(*tenant_info)) {
    ObTTLTaskCtx* ctx = nullptr;
    for (ttl_parts_iterator iter = tenant_info->part_task_map_.begin(); 
                            iter != tenant_info->part_task_map_.end()
                            && OB_SUCC(ret); ++iter) {
      ctx = iter->second;
      if (OB_ISNULL(ctx)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_ERROR("fatal err, ttl ctx in map is null", K(ret), K(tenant_info->tenant_id_));
      } else if (current_ctx == ctx) {
        // do nothing
      } else if (can_schedule_task(*ctx)) {
        if (OB_FAIL(try_schedule_task(tenant_info, ctx))) {
          if (OB_SIZE_OVERFLOW != ret) {
            LOG_WARN("fail to schedule task", K(ret));
          }
        }
      }
    }
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

// try schedule partition task, reutrn OB_SIZE_OVERFLOW if dag scheduler is full 
int ObTTLManager::try_schedule_task(ObTTLTenantInfo* tenant_info, ObTTLTaskCtx* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_info) || OB_ISNULL(ctx)) {
    LOG_WARN("invalid argument", K(ret), KP(tenant_info), KP(ctx));
  } else if (can_schedule_tenant(*tenant_info) && can_schedule_task(*ctx)) {
    if (OB_FAIL(generate_ttl_dag(ctx->task_info_, ctx->ttl_para_))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else if (OB_SIZE_OVERFLOW == ret) {
        // do noting
      } else {
        LOG_WARN("fail to generate dag task", K(ret));
      }
    } else {
      if (ctx->task_start_time_ == OB_INVALID_ID) {
        ctx->task_start_time_ = ObTimeUtility::current_time();
      }
      ctx->task_status_ = OB_TTL_TASK_RUNNING;
      mark_ttl_ctx_dirty(tenant_info, ctx);
    }
  } else {
    LOG_DEBUG("status when try schedule task", K(tenant_info->ttl_continue_), K(tenant_info->state_), K(ctx->task_status_));
  }
  return ret;
}

void ObTTLManager::mark_ttl_ctx_dirty(ObTTLTenantInfo* tenant_info, ObTTLTaskCtx* ctx)
{
  if (OB_ISNULL(tenant_info) || OB_ISNULL(ctx)) {
     LOG_WARN("ttl tenant_info or ctx is null", K(tenant_info), K(ctx));
  } else {
    ctx->is_dirty_ = true;
    tenant_info->is_dirty_ = true;
  }
}

int ObTTLManager::refresh_partition_task(ObTTLTaskCtx &ttl_task, bool refresh_status, bool refresh_retcode /*false*/)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObTTLStatusFieldArray filters;
  common::ObTTLStatusArray ttl_records;
  ObTTLStatusFieldArray filter;
  ObPartitionKey pkey = ttl_task.task_info_.pkey_;
  uint64_t tenant_id = pkey.get_tenant_id();
  ObTTLTenantInfo *tenant_info = get_tenant_info(pkey.get_tenant_id(), false);
  if (!ttl_task.is_valid() || OB_ISNULL(tenant_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ttl_task), KP(tenant_info));
  } else if (OB_FAIL(construct_task_record_filter(ttl_task.task_info_.task_id_,
                                           pkey.get_table_id(),
                                           pkey.get_partition_id(),
                                           filters))) {
    LOG_WARN("fail to construct task record filter", K(ret), K(ttl_task));
  } else if (OB_FAIL(trans.start(get_sql_proxy(), tenant_id))) {
    LOG_WARN("fail to start transation", K(ret));
  } else if (OB_FAIL(ObTTLUtil::read_ttl_tasks(tenant_id, share::OB_ALL_KV_TTL_TASK_TNAME,
                                               trans, filters, ttl_records, true, &tenant_info->allocator_))) {
    LOG_WARN("fail to get ttl tasks", K(ret), K(tenant_id), K(filters));
  } else {
    if (ttl_records.empty()) {
      // do nothing
    } else {
      if (ttl_records.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect ttl records count", K(ret), K(ttl_records.count()));
      } else if (OB_FAIL(from_ttl_record(pkey, ttl_records.at(0), refresh_status, refresh_retcode))) {
        LOG_WARN("fail to convert from ttl record", K(ret), K(pkey), K(refresh_status));
      }
    }
  }
  if (trans.is_started()) {
    bool commit = (OB_SUCCESS == ret);
    int tmp_ret = ret;
    if (OB_FAIL(trans.end(commit))) {
      LOG_WARN("faile to end trans", "commit", commit, K(ret));
    }
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  return ret;
}
