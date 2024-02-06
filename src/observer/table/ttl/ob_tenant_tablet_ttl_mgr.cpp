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
#include "ob_tenant_tablet_ttl_mgr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/table/ob_htable_filter_operator.h"
#include "observer/table/ob_htable_utils.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "observer/table/ttl/ob_table_ttl_task.h"
#include "observer/table/ob_table_service.h"
#include "share/table/ob_table_config_util.h"

namespace oceanbase
{

using namespace storage;
using namespace obrpc;
using namespace share;
using namespace table;
using namespace observer;

namespace table
{

int ObTenantTabletTTLMgr::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get sql proxy from GCTX", KR(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get ls", KR(ret));
  } else if (OB_FAIL(init(MTL_ID()))) {
    LOG_WARN("fail to init tenant ttl mgr", KR(ret), K(MTL_ID()));
  } else {
    ls_ = ls;
    sql_proxy_ = GCTX.sql_proxy_;
    LOG_INFO("success to init tenant ttl mgr", K(MTL_ID()));
  }
  return ret;
}

int ObTenantTabletTTLMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  schema_service_ = &(schema::ObMultiVersionSchemaService::get_instance());

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TenantTabletTTLMgr, tg_id_))) {
    LOG_WARN("fail to init timer", KR(ret));
  } else if (OB_FAIL(alloc_tenant_info(tenant_id))) {
    LOG_WARN("fail to alloc tenant info", KR(ret), K(MTL_ID()));
  } else {
    tablet_table_pairs_.set_attr(ObMemAttr(tenant_id, "TTLTablePairs"));
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  FLOG_INFO("finish init tenant_tablet_ttl_mgr", K_(tenant_id), K(ret), KPC_(ls));
  return ret;
}

int ObTenantTabletTTLMgr::switch_to_leader()
{
  int64_t start_time_us = ObTimeUtility::current_time();
  FLOG_INFO("tenant_tablet_ttl_mgr: begin to switch_to_leader", K_(tenant_id), KPC_(ls), K(start_time_us));
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K_(tenant_id));
  } else if (!has_start_) {
    if (OB_FAIL(start())) {
      LOG_WARN("fail to start tablet ttl mgr", K(ret));
    } else {
      has_start_ = true;
    }
  } else {
    resume();
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("tenant_tablet_ttl_mgr: finish to switch_to_leader", KR(ret), K_(tenant_id), KPC_(ls), K(cost_us));

  return ret;
}

int ObTenantTabletTTLMgr::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObTenantTabletTTLMgr::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  inner_switch_to_follower();
  return ret;
}

void ObTenantTabletTTLMgr::switch_to_follower_forcedly()
{
  inner_switch_to_follower();
}

void ObTenantTabletTTLMgr::inner_switch_to_follower()
{
  FLOG_INFO("tenant_tablet_ttl_mgr: begin to switch_to_follower", K_(tenant_id), KPC_(ls));
  const int64_t start_time_us = ObTimeUtility::current_time();
  pause();
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("tenant_tablet_ttl_mgr: finish to switch_to_follower", K_(tenant_id), KPC_(ls), K(cost_us));
}

void ObTenantTabletTTLMgr::resume()
{
  is_paused_ = false;
}

void ObTenantTabletTTLMgr::pause()
{
  is_paused_ = true;
}

int ObTenantTabletTTLMgr::start()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("tenant_tablet_ttl_mgr: begin to start", KPC_(ls), K_(tenant_id));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl mgr not init", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("failed to create ObTenantTabletTTLMgr thread", K(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, periodic_task_, periodic_delay_, true))) {
    LOG_WARN("fail to schedule periodic task", KR(ret), K_(tg_id));
  } else {
    is_timer_start_ = true;
  }
  FLOG_INFO("tenant_tablet_ttl_mgr: finish to start", K(ret), KPC_(ls), K_(tenant_id));
  return ret;
}

void ObTenantTabletTTLMgr::stop()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("tenant_tablet_ttl_mgr: begin to stop", K_(tenant_id), KPC_(ls));
  if (is_timer_start_) {
    // ensure TG_STOP which will acuiqre timer monitor lock
    // before acquire ObTenantTabletTTLMgr's lock_ to avoid lock deadlock
    // because the lock order of timer task is:
    // 1) acquire timer monitor lock
    // 2) acquire ObTenantTabletTTLMgr's lock_
    TG_STOP(tg_id_);
    is_timer_start_ = false;
    common::ObSpinLockGuard guard(lock_);
    // set is_paused_ to true to ensure after stop, not new TTL dag task will be generate,
    // i.e., dag_ref won't increase anymore
    is_paused_ = true;
  }
  FLOG_INFO("tenant_tablet_ttl_mgr: finish to stop", K(ret), K_(is_timer_start), K_(tenant_id), KPC_(ls));
}

void ObTenantTabletTTLMgr::wait()
{
  FLOG_INFO("tenant_tablet_ttl_mgr: begin to wait", KPC_(ls), K_(tenant_id));
  TG_WAIT(tg_id_);
  FLOG_INFO("tenant_tablet_ttl_mgr: finish to wait", KPC_(ls), K_(tenant_id));
}

void ObTenantTabletTTLMgr::destroy()
{
  FLOG_INFO("tenant_tablet_ttl_mgr: begin to destroy", KPC_(ls), K_(tenant_id));
  wait();
  TG_DESTROY(tg_id_);
  is_inited_ = false;
  FLOG_INFO("tenant_tablet_ttl_mgr: finish to destroy", KPC_(ls), K_(tenant_id));
}

int ObTenantTabletTTLMgr::check_and_handle_event()
{
  int ret = OB_SUCCESS;
  bool need_check = false;
  bool is_dirty = false;
  bool is_finished = false;
  ObTimeGuard guard("ObTenantTabletTTLMgr::check_and_handle_event", TTL_NORMAL_TIME_THRESHOLD);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl manager not init", KR(ret));
  } else if (is_paused_) {
    // do nothing, not leader
  } else {
    if (OB_FAIL(check_schema_version())) {
      LOG_WARN("fail to check schema version", KR(ret));
    } else {
      common::ObSpinLockGuard guard(lock_);
      // after observer restart, need check tenant even when cancel and move state
      is_dirty = local_tenant_task_.is_dirty_;
      is_finished = local_tenant_task_.is_finished_;
      need_check = !is_finished && local_tenant_task_.need_check_;
    }

    if (OB_SUCC(ret) && need_check) {
      if (OB_FAIL(check_and_generate_tablet_tasks())) {
        LOG_WARN("fail to generate tablet tasks", K_(tenant_id));
      }
    }

    if (OB_SUCC(ret) && (is_dirty || !is_finished)) {
      common::ObSArray<ObTabletID> dirty_tasks;
      if (OB_FAIL(handle_all_tablet_event(dirty_tasks))) {
        LOG_WARN("fail to handle tenant event in timer", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(sync_all_dirty_task(dirty_tasks))) {
        LOG_WARN("fail to sync all dirty task", KR(ret), K_(tenant_id));
      }

      check_ttl_tenant_state(); // check whether tenant still dirty or not
    }
  }

  return ret;
}

void ObTenantTabletTTLMgr::check_ttl_tenant_state()
{
  common::ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  bool tenant_dirty = false;
  bool tenant_finish = true;
  ObTTLTaskCtx* ctx = nullptr;

  for (tablet_task_iter iter = local_tenant_task_.tablet_task_map_.begin();
          !tenant_dirty && iter != local_tenant_task_.tablet_task_map_.end(); ++iter) {
    ctx = iter->second;
    if (OB_ISNULL(ctx)) {
      LOG_WARN("fatal err, ttl ctx in map is null", K(local_tenant_task_.tenant_id_));
    } else if (ctx->is_dirty_) {
      tenant_dirty = true;
    } else if (ctx->task_status_ != OB_TTL_TASK_CANCEL &&
                ctx->task_status_ != OB_TTL_TASK_FINISH) {
      tenant_finish = false;
      local_tenant_task_.is_finished_ = false;
    }
  }
  if (OB_SUCC(ret) && !tenant_dirty) {
    local_tenant_task_.is_dirty_ = false;
    if (tenant_finish) {
      // all task already in cancel or runing status
      if (local_tenant_task_.state_ == OB_TTL_TASK_CANCEL || local_tenant_task_.state_ == OB_TTL_TASK_RUNNING) {
        local_tenant_task_.is_finished_ = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tenant ttl state", KR(ret), K(local_tenant_task_.state_));
      }
    }
  }

  if (OB_SUCC(ret) && local_tenant_task_.is_finished_) {
    FLOG_INFO("local ls ttl task is finished", K_(local_tenant_task), KPC_(ls));
    local_tenant_task_.reuse();
  }

  LOG_DEBUG("check ttl tenant dirty", K(local_tenant_task_.is_dirty_), K(local_tenant_task_.state_), KR(ret), K_(tenant_id));
}

int ObTenantTabletTTLMgr::check_cmd_state_valid(const common::ObTTLTaskStatus current_state,
                                                const common::ObTTLTaskStatus incoming_state)
{
  int ret = OB_SUCCESS;
  switch (incoming_state) {
    case OB_TTL_TASK_RUNNING: {
      if (current_state != OB_TTL_TASK_PENDING && current_state != OB_TTL_TASK_INVALID &&
          current_state != OB_TTL_TASK_RUNNING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("receive rs cmd, but current tenant state is unmatached",
                  KR(ret), K(current_state), K(incoming_state));
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
                  KR(ret), K(current_state), K(incoming_state));
      }
      break;
    }
    case OB_TTL_TASK_CANCEL: {
      if (current_state != OB_TTL_TASK_PENDING && current_state != OB_TTL_TASK_RUNNING &&
          current_state != OB_TTL_TASK_INVALID && current_state != OB_TTL_TASK_CANCEL) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("receive rs cmd, but current tenant state is unmatached",
                  KR(ret), K(current_state), K(incoming_state));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid incoming status", KR(ret), K(incoming_state));
      break;
    }
  }
  return ret;
}

void ObTenantTabletTTLMgr::mark_tenant_need_check()
{
  int ret = OB_SUCCESS;
  if (common::ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
    common::ObSpinLockGuard guard(lock_);
    local_tenant_task_.need_check_ = true;
  }
  LOG_DEBUG("finsh mark tenant need check", KR(ret));
}

void ObTenantTabletTTLMgr::on_schema_changed(uint64_t schema_changed_tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ttl manager not init");
  } else if (!common::ObTTLUtil::check_can_process_tenant_tasks(schema_changed_tenant_id)) {
    //do nothing
  } else {
    mark_tenant_need_check();
  }
}

int ObTenantTabletTTLMgr::report_task_status(ObTTLTaskInfo& task_info, ObTTLTaskParam& task_para,
                                             bool& is_stop, bool need_copy_task/* true*/)
{
  int ret = OB_SUCCESS;
  ObTTLTaskCtx* ctx = nullptr;
  bool task_need_retry = false;
  common::ObSpinLockGuard guard(lock_);
  is_stop = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl mgr is not init,", KR(ret));
  } else if (OB_FAIL(local_tenant_task_.tablet_task_map_.get_refactored(task_info.tablet_id_, ctx))) {
    LOG_WARN("fail to get tablet task", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the tablet task ctx is null", KR(ret));
  } else {
    if (!ObKVFeatureModeUitl::is_ttl_enable()) {
      local_tenant_task_.ttl_continue_ = false;
      LOG_DEBUG("local_tenant_task mark continue is false");
    }
    // lock task ctx for update
    common::ObSpinLockGuard ctx_guard(ctx->lock_);
    ctx->last_modify_time_ = ObTimeUtility::current_time();
    mark_ttl_ctx_dirty(local_tenant_task_, *ctx);
    if (need_copy_task && OB_FAIL(deep_copy_task(ctx, task_info, task_para))) {
      LOG_WARN("fail to deep copy task", KR(ret), K_(tenant_id), K(task_info), K(task_para));
      ctx->task_status_ = OB_TTL_TASK_PENDING;
      ctx->failure_times_++;
    } else if (OB_SUCCESS == task_info.err_code_) {
      if (local_tenant_task_.ttl_continue_ && OB_TTL_TASK_RUNNING == local_tenant_task_.state_) {
        task_para = ctx->ttl_para_;
        is_stop = false;
      } else {
        ctx->task_status_ = OB_TTL_TASK_PENDING;
        LOG_INFO("pending current task", K(local_tenant_task_.state_), K(local_tenant_task_.ttl_continue_));
      }
    } else if (OB_ITER_END == task_info.err_code_) {
      ctx->task_status_ = OB_TTL_TASK_FINISH;
      ctx->task_info_.err_code_ = OB_SUCCESS;
    } else if (OB_NOT_MASTER == task_info.err_code_ ||
               OB_PARTITION_NOT_EXIST == task_info.err_code_ ||
               OB_TABLE_NOT_EXIST == task_info.err_code_ ||
               OB_ERR_UNKNOWN_TABLE == task_info.err_code_ ||
               OB_LS_NOT_EXIST == task_info.err_code_) {
      LOG_INFO("cancel current task since partition state change",
              K(task_info.err_code_), K(task_info.tablet_id_));
      ctx->task_status_ = OB_TTL_TASK_CANCEL;
      ctx->is_invalid_ = true;
    } else {
      LOG_WARN("task report error", K(task_info.err_code_), K(task_info.tablet_id_));
      ctx->task_status_ = OB_TTL_TASK_PENDING;
      ctx->failure_times_++;
    }
  }

  // schedule remaining tasks
  if (is_stop) {
    LOG_INFO("stop current task", K(ret), KPC(ctx), K_(local_tenant_task));
    if (OB_FAIL(try_schedule_remaining_tasks(ctx))) {
      LOG_WARN("fail to try schedule task", KR(ret));
    }
  }
  return ret;
}

int ObTenantTabletTTLMgr::generate_batch_tablet_task(ObIArray<share::ObTabletTablePair>& tablet_pairs,
                                                     hash::ObHashMap<uint64_t, ObTTLTaskParam> &param_map)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_pairs.count(); i++) {
    ObTTLTaskInfo task_info;
    uint64_t table_id = tablet_pairs.at(i).get_table_id();
    task_info.tablet_id_ = tablet_pairs.at(i).get_tablet_id();
    task_info.tenant_id_ = tenant_id_;
    task_info.table_id_ = table_id;
    ObTTLTaskParam param;
    if (OB_FAIL(param_map.get_refactored(table_id, param))) {
      LOG_WARN("fail to get ttl param", KR(ret), K(table_id));
    } else if (OB_FAIL(generate_one_tablet_task(task_info, param))) {
      LOG_WARN("fail to generate task", KR(ret), K(task_info), K(param));
    }
  }

  FLOG_INFO("finish generate tablet task", KR(ret), KPC_(ls), K(tablet_pairs));
  return ret;
}

int ObTenantTabletTTLMgr::generate_one_tablet_task(ObTTLTaskInfo& task_info, const ObTTLTaskParam& param)
{
  int ret = OB_SUCCESS;
  ObTTLTaskCtx* ctx = nullptr;
  common::ObSpinLockGuard guard(lock_);
  int64_t task_time = ObTimeUtility::current_time();

  if (OB_NOT_NULL(ctx = get_one_tablet_ctx(task_info.tablet_id_))) {
    LOG_INFO("ttl ctx exist", KR(ret), K(task_info.tablet_id_));
  } else {
    char *ctx_buf = static_cast<char *>(local_tenant_task_.allocator_.alloc(sizeof(ObTTLTaskCtx)));
    if (OB_ISNULL(ctx_buf)) {
      LOG_WARN("fail to alloc ttl task ctx", KR(ret));
    } else {
      ctx = new(ctx_buf)ObTTLTaskCtx();
      ctx->task_status_ = OB_TTL_TASK_PREPARE;
      ctx->ttl_para_ = param;
      mark_ttl_ctx_dirty(local_tenant_task_, *ctx);
      ctx->task_start_time_ = task_time;
      ctx->last_modify_time_ = task_time;
      ctx->task_info_.tablet_id_ = task_info.tablet_id_;
      ctx->task_info_.is_user_trigger_ = local_tenant_task_.is_usr_trigger_;
      ctx->task_info_.task_id_ = local_tenant_task_.task_id_;
      ctx->task_info_.ls_id_ = ls_->get_ls_id();
      ctx->task_info_.tenant_id_ = task_info.tenant_id_;
      ctx->task_info_.table_id_ = task_info.table_id_;
      if (OB_FAIL(deep_copy_task(ctx, task_info, param))) {
        LOG_WARN("fail tp deep copy task", KR(ret));
      } else {
        if (OB_FAIL(local_tenant_task_.tablet_task_map_.set_refactored(task_info.tablet_id_, ctx))) {
          LOG_WARN("fail to insert ttl task ctx into map", KR(ret), K(task_info.tablet_id_));
          local_tenant_task_.allocator_.free(ctx);
          ctx = nullptr;
        }
      }
    }
  }
  LOG_DEBUG("finish generate one partition task", KR(ret), K(task_info.tablet_id_), K(param));
  return ret;
}

void ObTenantTabletTTLMgr::mark_tenant_checked()
{
  common::ObSpinLockGuard guard(lock_);
  local_tenant_task_.need_check_ = false;
}

// scan all ttl tablet in current LS, and generate ttl dag task if necessary
int ObTenantTabletTTLMgr::check_and_generate_tablet_tasks()
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObTenantTabletTTLMgr::check_and_generate_tablet_tasks", TTL_NORMAL_TIME_THRESHOLD);
  bool can_ttl = false;
  ObTabletHandle tablet_handle;
  ObSEArray<const schema::ObTableSchema *, 64> table_schema_arr;
  const schema::ObTableSchema *table_schema = nullptr;
  ObSEArray<uint64_t, DEFAULT_TABLE_ARRAY_SIZE> table_id_array;
  hash::ObHashMap<uint64_t, table::ObTTLTaskParam> param_map; // table_id, task param
  ObMemAttr bucket_attr(tenant_id_, "TTLTaskParBuck");
  ObMemAttr node_attr(tenant_id_, "TTLTasParkNode");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl mgr not init", KR(ret));
  } else if (OB_FAIL(ObTTLUtil::get_tenant_table_ids(tenant_id_, table_id_array))) {
    LOG_WARN("fail to get tenant table ids", KR(ret), K_(tenant_id));
  } else if (!table_id_array.empty() && OB_FAIL(tablet_table_pairs_.reserve(DEFAULT_TABLET_PAIR_SIZE))) {
    LOG_WARN("fail to reserve", KR(ret));
  } else if (!table_id_array.empty() && OB_FAIL(param_map.create(DEFAULT_PARAM_BUCKET_SIZE, bucket_attr, node_attr))) {
    LOG_WARN("fail to create param map", KR(ret));
  }

  int64_t start_idx = 0;
  int64_t end_idx = 0;
  while (OB_SUCC(ret) && start_idx < table_id_array.count()) {
    {
      // temp schema guard to loop table ids
      ObSchemaGetterGuard schema_guard;
      start_idx = end_idx;
      end_idx = MIN(table_id_array.count(), start_idx + TBALE_GENERATE_BATCH_SIZE);
      bool is_ttl_table = false;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
      }
      for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx; ++idx) {
        const int64_t table_id = table_id_array.at(idx);
        const ObTableSchema *table_schema = nullptr;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
          LOG_WARN("failed to get simple schema", KR(ret), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table schema is null", KR(ret), K(table_id), K_(tenant_id));
        } else if (OB_FAIL(ObTTLUtil::check_is_ttl_table(*table_schema, is_ttl_table))) {
          LOG_WARN("fail to check is ttl table", KR(ret));
        } else if (is_ttl_table) {
          ObArray<ObTabletID> tablet_ids;
          ObTTLTaskParam ttl_param;
          if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
            LOG_WARN("fail to get tablet ids", KR(ret), K(table_id));
          } else if (OB_FAIL(get_ttl_para_from_schema(table_schema, ttl_param))) {
            LOG_WARN("fail to get ttl para");
          } else if (OB_FAIL(param_map.set_refactored(table_id, ttl_param))) {
            LOG_WARN("fail to push back table ttl param pairs", KR(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
              if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(tablet_ids.at(i), tablet_handle))) {
                if (OB_TABLET_NOT_EXIST != ret) {
                  LOG_WARN("fail to get tablet", K(ret), K(tablet_ids.at(i)));
                } else {
                  ret = OB_SUCCESS;
                }
              } else if (OB_NOT_NULL(get_one_tablet_ctx(tablet_ids.at(i)))) {
                // do nothing
              } else {
                ObTabletTablePair cur_pair;
                ObTabletID tablet_id = tablet_ids.at(i);
                if (OB_FAIL(cur_pair.init(tablet_id, table_id))) {
                  LOG_WARN("fail to init tablet_ls_pair", KR(ret), K(i), K(tablet_id), K(table_id));
                } else if (OB_FAIL(tablet_table_pairs_.push_back(cur_pair))) {
                  LOG_WARN("fail to push back pair", KR(ret), K(cur_pair));
                }
              }
            }
          }
        }
      }
    } // end scope
    if (OB_SUCC(ret) && OB_FAIL(generate_batch_tablet_task(tablet_table_pairs_, param_map))) {
      LOG_WARN("fail to generate batch tablet task", KR(ret));
    }
    tablet_table_pairs_.reuse();
  }
  tablet_table_pairs_.reset();

  if (OB_SUCC(ret)) {
    mark_tenant_checked();
  }
  FLOG_INFO("finish generate tenant tasks", KR(ret), KPC_(ls), K_(tenant_id));
  return ret;
}

void OBTTLTimerPeriodicTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  ObTimeGuard guard("OBTTLTimerPeriodicTask::runTimerTask", TTL_TIME_TASKER_THRESHOLD);
  if (!ObKVFeatureModeUitl::is_ttl_enable()) {
    // do nothing
    LOG_DEBUG("ttl is disable");
  } else if (common::ObTTLUtil::check_can_do_work()) {
    if (OB_FAIL(tablet_ttl_mgr_.check_tenant_memory())) {
      LOG_WARN("fail to check all tenant memory", KR(ret));
    }

    // explicit cover error code
    ret = OB_SUCCESS;
    if (OB_FAIL(tablet_ttl_mgr_.reload_tenant_task())) {
      LOG_WARN("fail to reload tenant task", KR(ret));
    }

    // explicit cover error code
    ret = OB_SUCCESS;
    if (OB_FAIL(tablet_ttl_mgr_.check_and_handle_event())) {
      LOG_WARN("fail to scan and handle all tenant event", KR(ret));
    }
  }
}

int ObTenantTabletTTLMgr::alloc_tenant_info(uint64_t tenant_id)
{

  int ret = OB_SUCCESS;
  local_tenant_task_.tenant_id_ = tenant_id;
  local_tenant_task_.ttl_continue_ = false;
  local_tenant_task_.is_dirty_ = true;
  ObMemAttr bucket_attr(tenant_id, "TTLTaskBucket");
  ObMemAttr node_attr(tenant_id, "TTLTaskNode");
  if(OB_FAIL(local_tenant_task_.tablet_task_map_.create(DEFAULT_TTL_BUCKET_NUM, bucket_attr, node_attr))) {
    LOG_WARN("fail to create ttl partition map", KR(ret), K(tenant_id));
  } else {}

  FLOG_INFO("finish create tenant ttl task", K(tenant_id), K(ret), KPC_(ls));
  return ret;
}

ObTTLTaskCtx* ObTenantTabletTTLMgr::get_one_tablet_ctx(const ObTabletID& tablet_id)
{
  int ret = OB_SUCCESS;
  ObTTLTaskCtx* ttl_task_ctx = nullptr;

  if (OB_FAIL(local_tenant_task_.tablet_task_map_.get_refactored(tablet_id, ttl_task_ctx))) {
    LOG_DEBUG("fail to get partition task", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(ttl_task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition task ctx is null", KR(ret));
  }
  return ttl_task_ctx;
}

/*other inner function*/
int ObTenantTabletTTLMgr::deep_copy_task(ObTTLTaskCtx* ctx, ObTTLTaskInfo& task_info, const ObTTLTaskParam &task_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ctx is null", KR(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, task_info.row_key_, ctx->task_info_.row_key_)) ) {
    LOG_WARN("fail to deep copy rowkey", KR(ret), K(task_info.row_key_));
  } else {
    ctx->task_info_.ttl_del_cnt_ = task_info.ttl_del_cnt_;
    ctx->task_info_.max_version_del_cnt_ = task_info.max_version_del_cnt_;
    ctx->task_info_.scan_cnt_ = task_info.scan_cnt_;
    ctx->task_info_.err_code_ = task_info.err_code_;
  }
  return ret;
}

int ObTenantTabletTTLMgr::generate_ttl_dag(ObTTLTaskInfo& task_info, ObTTLTaskParam& para)
{
  int ret = OB_SUCCESS;
  ObTableTTLDag *dag = nullptr;
  ObTableTTLDeleteTask *delete_task = nullptr;

  ObTenantDagScheduler *dag_scheduler = nullptr;
  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be NULL", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(dag))) {
    LOG_WARN("fail to alloc dag", KR(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, dag is null", KR(ret), KP(dag));
  } else if (OB_FAIL(dag->init(para, task_info))) {
    LOG_WARN("fail to init ttl dag", KR(ret));
  } else if (OB_FAIL(dag->alloc_task(delete_task))) {
    LOG_WARN("fail to alloc ttl delete task", KR(ret));
  } else if (OB_ISNULL(delete_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, prepare task is null", KR(ret), KP(delete_task));
  } else if (OB_FAIL(delete_task->init(this, para, task_info))) {
    LOG_WARN("fail to init ttl delete task", KR(ret));
  } else if (OB_FAIL(dag->add_task(*delete_task))) {
    LOG_WARN("fail to add ttl delete task to dag", KR(ret));
  } else if (OB_FAIL(dag_scheduler->add_dag(dag))) {
    if (OB_EAGAIN == ret) {
      LOG_DEBUG("ttl dag already exists, no need to schedule once again", KR(ret));
    } else if (OB_SIZE_OVERFLOW == ret) {
      LOG_DEBUG("dag is full", KR(ret));
    } else {
      LOG_WARN("fail to add dag to queue", KR(ret));
    }
  } else {
    FLOG_INFO("build ttl dag success", KR(ret), K(para), K(task_info));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(dag)) {
    dag_scheduler->free_dag(*dag);
  }
  LOG_INFO("finish to build table api ttl dag", KR(ret), K(para), K(task_info));
  return ret;
}

int ObTenantTabletTTLMgr::handle_all_tablet_event(common::ObSArray<ObTabletID>& tablets)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObTenantTabletTTLMgr::handle_all_tablet_event", TTL_NORMAL_TIME_THRESHOLD);
  ObTTLTaskCtx* ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl mgr not init", KR(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    for (tablet_task_iter iter = local_tenant_task_.tablet_task_map_.begin();
        iter != local_tenant_task_.tablet_task_map_.end(); ++iter) {
      ctx = iter->second;
      if (OB_ISNULL(ctx)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("fatal err, ttl ctx in map is null", KR(ret));
      } else if (OB_FAIL(handle_one_tablet_event(ctx))) {
        LOG_WARN("fail to handle one tablet event", KR(ret));
      } else if (ctx->is_dirty_ && OB_FAIL(tablets.push_back(ctx->task_info_.tablet_id_))) {
        LOG_WARN("fail to push back ttl pk", KR(ret));
      }
    }
  }

  FLOG_INFO("finish handle single tenant event", KR(ret), K_(tenant_id), KPC_(ls));
  return ret;
}

int ObTenantTabletTTLMgr::handle_one_tablet_event(ObTTLTaskCtx* ctx)
{
  int ret = OB_SUCCESS;
  bool try_schedule = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl mgr not init", KR(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ttl task ctx is null", KR(ret), K_(tenant_id), K(ctx));
  } else if (ctx->task_status_ != local_tenant_task_.state_) {
    if (OB_TTL_TASK_RUNNING == local_tenant_task_.state_) {
      if (OB_TTL_TASK_PENDING == ctx->task_status_) {
        try_schedule = true;
      } else if (OB_TTL_TASK_PREPARE == ctx->task_status_ ||
                 OB_TTL_TASK_FINISH == ctx->task_status_ ||
                 OB_TTL_TASK_CANCEL == ctx->task_status_) {
        // do nothing
      } else {
        LOG_WARN("no expected task status", K(local_tenant_task_), KPC(ctx));
      }
    } else if (OB_TTL_TASK_PENDING == local_tenant_task_.state_) {
      if (OB_TTL_TASK_RUNNING == ctx->task_status_ ||
          OB_TTL_TASK_PREPARE == ctx->task_status_) {
        ctx->task_status_ = local_tenant_task_.state_;
        mark_ttl_ctx_dirty(local_tenant_task_, *ctx);
      } else if (OB_TTL_TASK_FINISH == local_tenant_task_.state_){
        // do nothing, no need schedule finish task again
      } else {
        LOG_WARN("no expected task status", K(local_tenant_task_), KPC(ctx));
      }
    } else if (OB_TTL_TASK_CANCEL == local_tenant_task_.state_) {
      if (OB_TTL_TASK_PREPARE == ctx->task_status_ ||
          OB_TTL_TASK_RUNNING == ctx->task_status_ ||
          OB_TTL_TASK_PENDING == ctx->task_status_ ||
          OB_TTL_TASK_FINISH == ctx->task_status_) {
        ctx->task_status_ = local_tenant_task_.state_;
        mark_ttl_ctx_dirty(local_tenant_task_, *ctx);
      } else {
        LOG_WARN("no expected task status", K(local_tenant_task_), KPC(ctx));
      }
    } else if (OB_TTL_TASK_MOVING == local_tenant_task_.state_) {
      if (OB_TTL_TASK_PREPARE == ctx->task_status_) {
        ctx->task_status_ = OB_TTL_TASK_FINISH; // will refresh real status from task table
        mark_ttl_ctx_dirty(local_tenant_task_, *ctx);
      } else if (OB_TTL_TASK_FINISH == ctx->task_status_ ||
                 OB_TTL_TASK_CANCEL == ctx->task_status_) {
        // do nothing, normal partition task
      } else {
        LOG_WARN("no expected task status", K(local_tenant_task_), KPC(ctx));
      }
    } else {
      LOG_WARN("invalid ttl tenant task state", K(local_tenant_task_.state_));
    }

    if (OB_SUCC(ret) && try_schedule && OB_FAIL(try_schedule_task(ctx))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("fail to try schedule dag task", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    LOG_DEBUG("handle one partition event", KR(ret), K(ctx->task_status_), K(local_tenant_task_.state_));
  }
  return ret;
}

int ObTenantTabletTTLMgr::check_tenant_memory()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl mgr not init", KR(ret));
  } else if (!is_paused_) {
    common::ObSpinLockGuard guard(lock_);
    bool last_ttl_continue = local_tenant_task_.ttl_continue_;
    int64_t total_memstore_used = 0;
    int64_t minor_freeze_trigger = 0;
    if (!common::ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
      local_tenant_task_.ttl_continue_ = false;
    } else {
      int64_t active_memstore_used = 0;
      int64_t memstore_limit = 0;
      int64_t freeze_cnt = 0;
      if (OB_FAIL(MTL(storage::ObTenantFreezer *)->get_tenant_memstore_cond(
                              active_memstore_used,
                              total_memstore_used,
                              minor_freeze_trigger,
                              memstore_limit,
                              freeze_cnt)) ) {
        LOG_WARN("fail to get tenant memstore info for tenant ", KR(ret), K_(tenant_id));
      } else if (total_memstore_used > minor_freeze_trigger) {
        local_tenant_task_.ttl_continue_ = false;
      } else {
        local_tenant_task_.ttl_continue_ = true;
      }
    }
    if (last_ttl_continue != local_tenant_task_.ttl_continue_) {
      FLOG_INFO("ttl continue value changed",  K_(tenant_id), "ttl_continue",
        local_tenant_task_.ttl_continue_, K(total_memstore_used), K(minor_freeze_trigger));
    }
  }

  return ret;
}

int ObTenantTabletTTLMgr::get_ttl_para_from_schema(const schema::ObTableSchema *table_schema,
                                                   ObTTLTaskParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is null", KR(ret));
  } else if (!table_schema->get_kv_attributes().empty()) {
    if (OB_FAIL(ObHTableUtils::check_htable_schema(*table_schema))) {
      LOG_WARN("fail to check htable schema", KR(ret), K(table_schema->get_table_name()));
    } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(table_schema->get_kv_attributes(), param.max_version_, param.ttl_))) {
      LOG_WARN("fail to parse kv attributes", KR(ret), K(table_schema->get_kv_attributes()));
    } else {
      param.is_htable_ = true;
      LOG_DEBUG("success to find a hbase ttl partition", KR(ret), K(param));
    }
  } else if (!table_schema->get_ttl_definition().empty()) {
    LOG_DEBUG("success to find a table ttl partition", KR(ret), K(param));
  }

  if (OB_SUCC(ret)) {
    param.tenant_id_ = table_schema->get_tenant_id();
    param.database_id_ = table_schema->get_database_id();
    param.user_id_ = table_schema->get_define_user_id();
    param.table_id_ = table_schema->get_table_id();
  }
  return ret;
}

// task already lock in ObTenantTabletTTLMgr::sync_sys_table
int ObTenantTabletTTLMgr::try_schedule_prepare_task(ObTabletID& tablet_id)
{
  int ret = OB_SUCCESS;
  ObTTLTaskCtx* ctx = get_one_tablet_ctx(tablet_id);
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("ttl tenant info or ctx is null", KR(ret));
  } else if (ctx->task_status_ != OB_TTL_TASK_PREPARE) {
    // do nothing
  } else if (FALSE_IT(ctx->task_status_ = OB_TTL_TASK_PENDING)) {
  } else if (OB_FAIL(try_schedule_task(ctx))) {
    if (OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("fail to schedule task", KR(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  LOG_DEBUG("try schedule prepare task", KR(ret));
  return ret;
}

int ObTenantTabletTTLMgr::sync_all_dirty_task(ObIArray<ObTabletID>& dirty_tasks)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObTenantTabletTTLMgr::sync_all_dirty_record", TTL_NORMAL_TIME_THRESHOLD);
  for (int i = 0; OB_SUCC(ret) && i < dirty_tasks.count(); i++) {
    if (OB_FAIL(sync_sys_table(dirty_tasks.at(i)))) {
      LOG_WARN("fail to sync sys table", KR(ret));
    }
  }
  return ret;
}

int ObTenantTabletTTLMgr::sync_sys_table(ObTabletID& tablet_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(lib::ObLabel("TTLStatusRecord"));
  ObTTLTaskCtx* ctx = nullptr;
  {
    common::ObSpinLockGuard guard(lock_);
    ctx = get_one_tablet_ctx(tablet_id);
    if (OB_ISNULL(ctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ctx is null", KR(ret));
    } else if (OB_UNLIKELY(!ctx->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid partition task ctx", KR(ret), KPC(ctx));
    } else {
      ctx->last_modify_time_ = ObTimeUtility::current_time();
    }
  }

  // lock task ctx for update
  // must not hold lock_ any more in this following code, which will cause deadlock
  // beause dag thread hold lock_ first, and ctx->lock_ second at report_task_status
  common::ObSpinLockGuard ctx_guard(ctx->lock_);
  if (OB_SUCC(ret) && OB_UNLIKELY(ctx->need_refresh_)) {
    switch (ctx->task_status_) {
      case OB_TTL_TASK_FINISH: // tenant_info must be in moving status
      case OB_TTL_TASK_PREPARE: {
        if (OB_FAIL(refresh_tablet_task(*ctx, true /*refresh_status*/, true))) {
          LOG_WARN("fail to refresh partition task from task table", KR(ret));
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
        if (OB_FAIL(refresh_tablet_task(*ctx, false /*refresh_status*/))) {
          LOG_WARN("fail to refresh partition task from task table", KR(ret));
        } else {
          ctx->need_refresh_ = false;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ttl task status", KR(ret));
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
        bool is_exists = false;
        if (OB_FAIL(trans.start(get_sql_proxy(), gen_meta_tenant_id(tenant_id_)))) {
          LOG_WARN("fail to start transation", KR(ret), K_(tenant_id));
        } else if (OB_FAIL(ObTTLUtil::check_tenant_state(tenant_id_, trans, local_tenant_task_.state_, local_tenant_task_.task_id_))) {
          FLOG_INFO("local tenant task state is different from sys table", KR(ret), K_(tenant_id), K(local_tenant_task_.state_));
        } else if (OB_FAIL(ObTTLUtil::check_ttl_task_exists(tenant_id_, trans, ctx->task_info_.task_id_,
                    ctx->task_info_.table_id_, ctx->task_info_.tablet_id_, is_exists))) {
          LOG_WARN("fail to check ttl task exist");
        } else if (!is_exists) {
          if (OB_FAIL(construct_sys_table_record(ctx, ttl_record))) {
            LOG_WARN("fail to construct sys table record", KR(ret));
          } else if (OB_FAIL(ObTTLUtil::insert_ttl_task(tenant_id_, share::OB_ALL_KV_TTL_TASK_TNAME,
                                                        trans, ttl_record))) {
            LOG_WARN("fail to insert ttl task", KR(ret));
          }
        }

        if (trans.is_started()) {
          bool commit = (OB_SUCCESS == ret);
          int tmp_ret = ret;
          if (OB_FAIL(trans.end(commit))) {
            LOG_WARN("faile to end trans", "commit", commit, KR(ret));
          }
          ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
        }

        // change prepare state to running/pending
        if (OB_SUCC(ret) && OB_FAIL(try_schedule_prepare_task(tablet_id))) {
          LOG_WARN("fail to schedule prepare task", KR(ret));
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
        bool is_exists = false;
        if (OB_FAIL(trans.start(get_sql_proxy(), gen_meta_tenant_id(tenant_id_)))) {
          LOG_WARN("fail to start transation", KR(ret), K_(tenant_id));
        } else if (OB_FAIL(ObTTLUtil::check_tenant_state(tenant_id_, trans, local_tenant_task_.state_, local_tenant_task_.task_id_))) {
          FLOG_INFO("local tenant task state is different from sys table", KR(ret), K_(tenant_id), K(local_tenant_task_.state_));
        } else if (OB_FAIL(ObTTLUtil::check_ttl_task_exists(tenant_id_, trans, ctx->task_info_.task_id_,
                    ctx->task_info_.table_id_, ctx->task_info_.tablet_id_, is_exists))) {
          LOG_WARN("fail to check ttl task exist");
        } else if (!is_exists) {
          if (OB_FAIL(construct_sys_table_record(ctx, ttl_record))) {
            LOG_WARN("fail to construct sys table record", KR(ret));
          } else if (OB_FAIL(ObTTLUtil::insert_ttl_task(tenant_id_, share::OB_ALL_KV_TTL_TASK_TNAME,
                                                        trans, ttl_record))) {
            LOG_WARN("fail to insert ttl task", KR(ret));
          }
        } else {
          if (OB_FAIL(construct_sys_table_record(ctx, ttl_record))) {
            LOG_WARN("fail to construct sys table record", KR(ret));
            } else if (OB_FAIL(ObTTLUtil::update_ttl_task_all_fields(tenant_id_,
                                                              share::OB_ALL_KV_TTL_TASK_TNAME,
                                                              trans, ttl_record))) {
            LOG_WARN("fail to update ttl task in sys table", KR(ret), K(ttl_record));
          }
        }

        if (trans.is_started()) {
          bool commit = (OB_SUCCESS == ret);
          int tmp_ret = ret;
          if (OB_FAIL(trans.end(commit))) {
            LOG_WARN("faile to end trans", "commit", commit, KR(ret));
          }
          ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
        }
        break;
      }

      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ttl task status", KR(ret));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    //mark ctx dirty false
    if (OB_ISNULL(ctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ctx is null", KR(ret));
    } else {
      ctx->is_dirty_ = false;
      LOG_INFO("finish mark ctx dirty false", KR(ret), K(tablet_id), KPC(ctx));
    }
  }
  return ret;
}

int ObTenantTabletTTLMgr::construct_sys_table_record(ObTTLTaskCtx* ctx, common::ObTTLStatus& ttl_record)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaild null ttl task ctx", KR(ret));
  } else if (OB_UNLIKELY(!ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(ctx));
  } else {
    ttl_record.tenant_id_ = tenant_id_;
    ttl_record.table_id_ = ctx->task_info_.table_id_;
    ttl_record.tablet_id_ = ctx->task_info_.tablet_id_.id();
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

int ObTenantTabletTTLMgr::construct_task_record_filter(const uint64_t& task_id,
                                                       const uint64_t& table_id,
                                                       ObTabletID& tablet_id,
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
  partition_id_field.field_name_ = ObString("tablet_id");
  partition_id_field.type_ = ObTTLStatusField::UINT_TYPE;
  partition_id_field.data_.uint_ = tablet_id.id();

  if (OB_FAIL(filter.push_back(task_id_field))) {
    LOG_WARN("failt to push back", KR(ret));
  } else if (OB_FAIL(filter.push_back(table_id_field))) {
    LOG_WARN("failt to push back", KR(ret));
  } else if (OB_FAIL(filter.push_back(partition_id_field))) {
    LOG_WARN("failt to push back", KR(ret));
  }
  return ret;
}

int ObTenantTabletTTLMgr::from_ttl_record(ObTabletID& tablet_id, common::ObTTLStatus& record, bool with_status /*true*/, bool with_err_code /*true*/)
{
  int ret = OB_SUCCESS;
  ObTTLTaskCtx* ctx = get_one_tablet_ctx(tablet_id);
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected null value", KR(ret), KP(ctx));
  } else if (tenant_id_ != record.tenant_id_ ||
             tablet_id.id() != record.tablet_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("record do not match", KR(ret), K(record));
  } else {
    ctx->task_info_.tablet_id_ = tablet_id;
    ctx->task_info_.task_id_ = record.task_id_;
    ctx->task_start_time_ = record.task_start_time_;
    ctx->last_modify_time_ = record.task_update_time_;
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
      char *rowkey_buf = static_cast<char *>(local_tenant_task_.allocator_.alloc(record.row_key_.length()));
      if (OB_ISNULL(rowkey_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", KR(ret));
      } else {
        MEMCPY(rowkey_buf, record.row_key_.ptr(), record.row_key_.length());
        ctx->task_info_.row_key_.assign(rowkey_buf, record.row_key_.length());
      }
    }
  }
  LOG_DEBUG("finish from ttl record", KR(ret), K(tablet_id));
  return ret;
}

bool ObTenantTabletTTLMgr::can_schedule_tenant(const ObTTLTenantInfo &tenant_info)
{
  return tenant_info.ttl_continue_ && tenant_info.state_ == OB_TTL_TASK_RUNNING;
}

bool ObTenantTabletTTLMgr::can_schedule_task(const ObTTLTaskCtx &ttl_task)
{
  return !is_paused_ && ttl_task.task_status_ == OB_TTL_TASK_PENDING;
}

int ObTenantTabletTTLMgr::try_schedule_remaining_tasks(const ObTTLTaskCtx *current_ctx)
{
  int ret = OB_SUCCESS;
  if (can_schedule_tenant(local_tenant_task_)) {
    ObTTLTaskCtx* ctx = nullptr;
    for (tablet_task_iter iter = local_tenant_task_.tablet_task_map_.begin();
                            iter != local_tenant_task_.tablet_task_map_.end()
                            && OB_SUCC(ret); ++iter) {
      ctx = iter->second;
      if (OB_ISNULL(ctx)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_ERROR("fatal err, ttl ctx in map is null", KR(ret), K(local_tenant_task_.tenant_id_));
      } else if (current_ctx == ctx) {
        // do nothing
      } else {
        common::ObSpinLockGuard guard(ctx->lock_);
        if (can_schedule_task(*ctx)) {
          if (OB_FAIL(try_schedule_task(ctx))) {
            if (OB_SIZE_OVERFLOW != ret) {
              LOG_WARN("fail to schedule task", KR(ret));
            }
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
int ObTenantTabletTTLMgr::try_schedule_task(ObTTLTaskCtx* ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ttl task ctx is null", KR(ret));
  } else if (can_schedule_tenant(local_tenant_task_) && can_schedule_task(*ctx)) {
    if (OB_FAIL(generate_ttl_dag(ctx->task_info_, ctx->ttl_para_))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else if (OB_SIZE_OVERFLOW == ret) {
        // do noting
      } else {
        LOG_WARN("fail to generate dag task", KR(ret));
      }
    } else {
      inc_dag_ref();
      if (ctx->task_start_time_ == OB_INVALID_ID) {
        ctx->task_start_time_ = ObTimeUtility::current_time();
      }
      ctx->task_status_ = OB_TTL_TASK_RUNNING;
      mark_ttl_ctx_dirty(local_tenant_task_, *ctx);
    }
  } else {
    LOG_DEBUG("status when try schedule task", K(local_tenant_task_.ttl_continue_), K(local_tenant_task_.state_), K(ctx->task_status_));
  }
  return ret;
}

void ObTenantTabletTTLMgr::mark_ttl_ctx_dirty(ObTTLTenantInfo& tenant_info, ObTTLTaskCtx& ctx)
{
  ctx.is_dirty_ = true;
  local_tenant_task_.is_dirty_ = true;
}

int ObTenantTabletTTLMgr::refresh_tablet_task(ObTTLTaskCtx &ttl_task, bool refresh_status, bool refresh_retcode /*false*/)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObTTLStatusFieldArray filters;
  common::ObTTLStatusArray ttl_records;
  ObTTLStatusFieldArray filter;
  ObTabletID tablet_id = ttl_task.task_info_.tablet_id_;
  if (!ttl_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ttl_task));
  } else if (OB_FAIL(construct_task_record_filter(ttl_task.task_info_.task_id_,
                                                  ttl_task.task_info_.table_id_,
                                                  ttl_task.task_info_.tablet_id_,
                                                  filters))) {
    LOG_WARN("fail to construct task record filter", KR(ret), K(ttl_task));
  } else if (OB_FAIL(trans.start(get_sql_proxy(), gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("fail to start transation", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObTTLUtil::read_ttl_tasks(tenant_id_, share::OB_ALL_KV_TTL_TASK_TNAME,
                                               trans, filters, ttl_records, true, &local_tenant_task_.allocator_))) {
    LOG_WARN("fail to get ttl tasks", KR(ret), K_(tenant_id), K(filters));
  } else {
    if (ttl_records.empty()) {
      // do nothing
    } else {
      if (ttl_records.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect ttl records count", KR(ret), K(ttl_records.count()));
      } else if (OB_FAIL(from_ttl_record(ttl_task.task_info_.tablet_id_, ttl_records.at(0), refresh_status, refresh_retcode))) {
        LOG_WARN("fail to convert from ttl record", KR(ret), K(refresh_status));
      }
    }
  }
  if (trans.is_started()) {
    bool commit = (OB_SUCCESS == ret);
    int tmp_ret = ret;
    if (OB_FAIL(trans.end(commit))) {
      LOG_WARN("faile to end trans", "commit", commit, KR(ret));
    }
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  return ret;
}

// 1. scan and get the latest tenant ttl task
// 2. check the status and change local tenant info
int ObTenantTabletTTLMgr::reload_tenant_task()
{
  int ret = OB_SUCCESS;
  ObTTLStatus tenant_task;
  common::ObSpinLockGuard guard(lock_);
  ObTTLTaskStatus expected_state;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (is_paused_) {
    // do nothing, not leader
  } else if (OB_FAIL(ObTTLUtil::read_tenant_ttl_task(tenant_id_, *sql_proxy_, tenant_task))) {
    if (OB_ITER_END == ret) {
      // do nothing
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to read tenant ttl task", KR(ret), K_(tenant_id));
    }
  } else if (!local_tenant_task_.is_finished_ && local_tenant_task_.task_id_ != tenant_task.task_id_) {
    FLOG_INFO("tenant task is finished, but local tenant task is not, maybe schema changed",
      KR(ret), K_(local_tenant_task), K(tenant_task.task_id_));
      local_tenant_task_.reuse();
  } else if (OB_RS_TTL_TASK_MOVE == static_cast<ObTTLTaskStatus>(tenant_task.status_)) {
    FLOG_INFO("tenant task is moving now, tablet ttl task should not continue",
      KR(ret), K_(local_tenant_task), K(tenant_task.task_id_));
    local_tenant_task_.reuse();
  } else if (OB_FAIL(ObTTLUtil::transform_tenant_state(static_cast<ObTTLTaskStatus>(tenant_task.status_), expected_state))) {
    LOG_WARN("fail to transform ttl tenant task status", KR(ret), K(tenant_task.status_));
  } else if (OB_FAIL(check_cmd_state_valid(local_tenant_task_.state_, expected_state))) {
    LOG_WARN("ttl cmd state machine is wrong", KR(ret), K_(tenant_id), K(tenant_task), K(expected_state));
  } else {
    if (local_tenant_task_.is_finished_ && local_tenant_task_.task_id_ !=  tenant_task.task_id_) {
      // new ttl request
      local_tenant_task_.task_id_ = tenant_task.task_id_;
      local_tenant_task_.is_usr_trigger_ = (tenant_task.trigger_type_ == USER_TRIGGER);
      local_tenant_task_.state_ = expected_state;
      local_tenant_task_.need_check_ = true;
      local_tenant_task_.is_dirty_ = true;
      local_tenant_task_.is_finished_ = false;
      FLOG_INFO("new ttl task", KR(ret), K_(tenant_id), K_(local_tenant_task));
    } else if (local_tenant_task_.task_id_ != tenant_task.task_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task id is mismatch", KR(ret), K(local_tenant_task_.task_id_), K(tenant_task.task_id_));
    } else if (!local_tenant_task_.is_finished_ && local_tenant_task_.state_ != expected_state) {
      FLOG_INFO("old ttl task changed", KR(ret), K_(tenant_id), K_(local_tenant_task), K(tenant_task));
      // current tenant task status changed
      local_tenant_task_.state_ = expected_state;
      local_tenant_task_.is_dirty_ = true;
    }
  }
  return ret;
}

int ObTenantTabletTTLMgr::check_schema_version()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  int64_t schema_version = 0;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret), K_(tenant_id));
  } else if (!ObSchemaService::is_formal_version(schema_version)) {
    ret = OB_EAGAIN;
    LOG_INFO("is not a formal_schema_version", KR(ret), K(schema_version));
  } else if (local_schema_version_ == OB_INVALID_VERSION ||  local_schema_version_ < schema_version) {
    FLOG_INFO("schema changed, mark tenant need check", KR(ret), K_(local_schema_version), K(schema_version));
    local_schema_version_ = schema_version;
    mark_tenant_need_check();
  }
  return ret;
}

int ObTenantTabletTTLMgr::safe_to_destroy(bool &is_safe)
{
  int ret = OB_SUCCESS;
  is_safe = true;
  int64_t dag_ref = get_dag_ref();
  if (0 != dag_ref) {
    if (REACH_TIME_INTERVAL(60L * 1000000)) {  // 60s
      LOG_WARN("tenant ttl tablet mgr can't destroy", K(dag_ref));
    }
    is_safe = false;
  }
  return ret;
}

} // table
} // oceanbase
