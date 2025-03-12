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
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "observer/table/ttl/ob_table_ttl_task.h"
#include "observer/table/ob_table_service.h"
#include "share/table/ob_table_config_util.h"
#include "src/share/table/redis/ob_redis_util.h"

namespace oceanbase
{

using namespace storage;
using namespace obrpc;
using namespace share;
using namespace table;
using namespace observer;

namespace table
{

int ObTabletTTLScheduler::init(ObLS *ls)
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

int ObTabletTTLScheduler::init(const uint64_t tenant_id)
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

int ObTabletTTLScheduler::switch_to_leader()
{
  int64_t start_time_us = ObTimeUtility::current_time();
  FLOG_INFO("ObTabletTTLScheduler: begin to switch_to_leader", K_(tenant_id), KPC_(ls), K(start_time_us));
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check_inner_stat", KR(ret), K_(tenant_id));
  } else if (!has_start_) {
    if (OB_FAIL(start())) {
      LOG_WARN("fail to start tablet ttl mgr", K(ret));
    } else {
      has_start_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    ATOMIC_STORE(&is_leader_, true);
    ATOMIC_STORE(&need_do_for_switch_, true);
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("ObTabletTTLScheduler: finish to switch_to_leader", KR(ret), K_(tenant_id), KPC_(ls), K(cost_us));

  return ret;
}

int ObTabletTTLScheduler::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObTabletTTLScheduler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  inner_switch_to_follower();
  return ret;
}

void ObTabletTTLScheduler::switch_to_follower_forcedly()
{
  inner_switch_to_follower();
}

void ObTabletTTLScheduler::inner_switch_to_follower()
{
  FLOG_INFO("ObTabletTTLScheduler: begin to switch_to_follower", K_(tenant_id), KPC_(ls));
  const int64_t start_time_us = ObTimeUtility::current_time();
  ATOMIC_STORE(&is_leader_, false);
  ATOMIC_STORE(&need_do_for_switch_, true);
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("ObTabletTTLScheduler: finish to switch_to_follower", K_(tenant_id), KPC_(ls), K(cost_us));
}

int ObTabletTTLScheduler::start()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTabletTTLScheduler: begin to start", KPC_(ls), K_(tenant_id));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl mgr not init", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to create ObTabletTTLScheduler thread", K(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, periodic_task_, periodic_delay_, true))) {
    LOG_WARN("fail to schedule periodic task", KR(ret), K_(tg_id));
  } else {
    is_timer_start_ = true;
  }
  FLOG_INFO("ObTabletTTLScheduler: finish to start", K(ret), KPC_(ls), K_(tenant_id));
  return ret;
}

void ObTabletTTLScheduler::stop()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("ObTabletTTLScheduler: begin to stop", K_(tenant_id), KPC_(ls));
  if (is_timer_start_) {
    // ensure TG_STOP which will acuiqre timer monitor lock
    // before acquire ObTabletTTLScheduler's lock_ to avoid lock deadlock
    // because the lock order of timer task is:
    // 1) acquire timer monitor lock
    // 2) acquire ObTabletTTLScheduler's lock_
    TG_STOP(tg_id_);
    is_timer_start_ = false;
    common::ObSpinLockGuard guard(lock_);
    // set is_leader_ to false to ensure after stop, not new TTL dag task will be generate,
    // i.e., dag_ref won't increase anymore
    ATOMIC_STORE(&is_leader_, false);
  }
  FLOG_INFO("ObTabletTTLScheduler: finish to stop", K(ret), K_(is_timer_start), K_(tenant_id), KPC_(ls));
}

void ObTabletTTLScheduler::wait()
{
  FLOG_INFO("ObTabletTTLScheduler: begin to wait", KPC_(ls), K_(tenant_id));
  TG_WAIT(tg_id_);
  FLOG_INFO("ObTabletTTLScheduler: finish to wait", KPC_(ls), K_(tenant_id));
}

void ObTabletTTLScheduler::destroy()
{
  FLOG_INFO("ObTabletTTLScheduler: begin to destroy", KPC_(ls), K_(tenant_id));
  wait();
  TG_DESTROY(tg_id_);
  tg_id_ = -1;
  is_inited_ = false;
  FLOG_INFO("ObTabletTTLScheduler: finish to destroy", KPC_(ls), K_(tenant_id));
}

int ObTabletTTLScheduler::check_and_handle_event()
{
  int ret = OB_SUCCESS;
  bool need_check = false;
  bool is_dirty = false;
  bool is_finished = false;
  ObTimeGuard guard("ObTabletTTLScheduler::check_and_handle_event", TTL_NORMAL_TIME_THRESHOLD);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl manager not init", KR(ret));
  } else if (!is_leader_) {
    // do nothing, not leader
  } else {
    if (OB_FAIL(check_schema_version())) {
      LOG_WARN("fail to check schema version", KR(ret));
    } else {
      common::ObSpinLockGuard guard(lock_);
      // after observer restart, need check tenant even when cancel and move state
      is_dirty = local_tenant_task_.is_dirty_;
      is_finished = local_tenant_task_.state_ == OB_TTL_TASK_FINISH;
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

void ObTabletTTLScheduler::check_ttl_tenant_state()
{
  common::ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  bool tenant_dirty = false;
  bool tenant_finish = true;
  ObTTLTaskCtx* ctx = nullptr;

  if (local_tenant_task_.need_check_) {
    // when local tenant task need check(maybe schema changed), this task cannot finish
    // we should end this process(don't execute else) and check tenant task first
  } else {
    for (tablet_task_iter iter = local_tenant_task_.tablet_task_map_.begin();
            OB_SUCC(ret) && !tenant_dirty && iter != local_tenant_task_.tablet_task_map_.end(); ++iter) {
      ctx = iter->second;
      if (need_skip_run()) {
        ret = OB_EAGAIN;
        FLOG_INFO("skip this run cuz of leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));
      } else if (OB_ISNULL(ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fatal err, ttl ctx in map is null", K(local_tenant_task_.tenant_id_));
      } else {
        tenant_dirty = tenant_dirty ? tenant_dirty : ctx->is_dirty_;
        tenant_finish = tenant_finish ?  (ctx->task_status_ == OB_TTL_TASK_CANCEL || ctx->task_status_ == OB_TTL_TASK_FINISH) : tenant_finish;
      }
    }
    if (OB_SUCC(ret) && !tenant_dirty) {
      local_tenant_task_.is_dirty_ = false;
      if (tenant_finish) {
        local_tenant_task_.state_ = OB_TTL_TASK_FINISH;
        FLOG_INFO("local ls ttl task is finished", K_(local_tenant_task), KPC_(ls));
      }
    }
  }

  LOG_DEBUG("check ttl tenant dirty", K(local_tenant_task_.is_dirty_), K(local_tenant_task_.state_), KR(ret), K_(tenant_id));
}

void ObTabletTTLScheduler::mark_tenant_need_check()
{
  int ret = OB_SUCCESS;
  if (common::ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
    common::ObSpinLockGuard guard(lock_);
    if (local_tenant_task_.task_id_ != OB_INVALID_ID) {
      local_tenant_task_.need_check_ = true;
      if (local_tenant_task_.state_ == OB_TTL_TASK_FINISH) {
        // this local tenant task(outdated) should not be finished when we mark tenant need check,
        // instead, we should check and decide whether it should be finished in the future
        local_tenant_task_.state_ = OB_TTL_TASK_RUNNING;
      }
      FLOG_INFO("finish mark tenant need check", K(local_tenant_task_));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unpected task id is found", KR(ret), K(local_tenant_task_));
    }
  }
  LOG_DEBUG("finsh mark tenant need check", KR(ret));
}

int ObTabletTTLScheduler::report_task_status(ObTTLTaskInfo& task_info, ObTTLTaskParam& task_para,
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
    ctx->in_queue_ = false;
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
      FLOG_INFO("task execute finished", KR(ret));
    } else if (OB_NOT_MASTER == task_info.err_code_ ||
               OB_PARTITION_NOT_EXIST == task_info.err_code_ ||
               OB_TABLE_NOT_EXIST == task_info.err_code_ ||
               OB_ERR_UNKNOWN_TABLE == task_info.err_code_ ||
               OB_LS_NOT_EXIST == task_info.err_code_) {
      LOG_INFO("cancel current task since partition state change",
              K(task_info.err_code_), K(task_info.tablet_id_));
      ctx->task_status_ = OB_TTL_TASK_CANCEL;
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

int ObTabletTTLScheduler::generate_batch_tablet_task(ObIArray<share::ObTabletTablePair>& tablet_pairs,
                                                     hash::ObHashMap<uint64_t, ObTTLTaskParam> &param_map)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_pairs.count(); i++) {
    if (need_skip_run()) {
      ret = OB_EAGAIN;
      FLOG_INFO("skip this run cuz of leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));
    } else {
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
  }

  FLOG_INFO("finish generate tablet task", KR(ret), KPC_(ls), K(tablet_pairs));
  return ret;
}

int ObTabletTTLScheduler::generate_one_tablet_task(ObTTLTaskInfo& task_info, const ObTTLTaskParam& param)
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
      ret = OB_ALLOCATE_MEMORY_FAILED;
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
        }
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(ctx)) {
      ctx->~ObTTLTaskCtx();
      local_tenant_task_.allocator_.free(ctx);
      ctx = nullptr;
    }
  }

  LOG_DEBUG("finish generate one partition task", KR(ret), K(task_info.tablet_id_), K(param));
  return ret;
}

void ObTabletTTLScheduler::mark_tenant_checked()
{
  common::ObSpinLockGuard guard(lock_);
  local_tenant_task_.need_check_ = false;
}

// scan all ttl tablet in current LS, and generate ttl dag task if necessary
int ObTabletTTLScheduler::check_and_generate_tablet_tasks()
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObTabletTTLScheduler::check_and_generate_tablet_tasks", TTL_NORMAL_TIME_THRESHOLD);
  ObTabletHandle tablet_handle;
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
        } else if (OB_FAIL(check_is_ttl_table(*table_schema, is_ttl_table))) {
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
  ObCurTraceId::init(GCONF.self_addr_);
  ObTimeGuard guard("OBTTLTimerPeriodicTask::runTimerTask", TTL_TIME_TASKER_THRESHOLD);
  tablet_ttl_mgr_.run_task();
}

void ObTabletTTLScheduler::run_task()
{
  int ret = OB_SUCCESS;
  if (!ObTTLUtil::is_enable_ttl(tenant_id_)) {
    // do nothing
    LOG_DEBUG("ttl is disable");
  } else if (ATOMIC_BCAS(&need_do_for_switch_, true, false)) {
    // reuse and skip task once
    if (OB_FAIL(do_after_leader_switch())) {
      LOG_WARN("fail to do after leader switch", K(ret));
      ATOMIC_BCAS(&need_do_for_switch_, false, true);
    } else {
      common::ObSpinLockGuard guard(lock_); // need lock for reuse tenant task
      local_tenant_task_.reuse();
      FLOG_INFO("resue local tenant task cuz of switch leader");
    }
  } else if (common::ObTTLUtil::check_can_do_work()) {
    if (OB_FAIL(check_tenant_memory())) {
      LOG_WARN("fail to check all tenant memory", KR(ret));
    }

    // explicit cover error code
    ret = OB_SUCCESS;
    if (OB_FAIL(reload_tenant_task())) {
      LOG_WARN("fail to reload tenant task", KR(ret));
    }

    // explicit cover error code
    ret = OB_SUCCESS;
    if (OB_FAIL(check_and_handle_event())) {
      LOG_WARN("fail to scan and handle all tenant event", KR(ret));
    }
  }
}

int ObTabletTTLScheduler::alloc_tenant_info(uint64_t tenant_id)
{

  int ret = OB_SUCCESS;
  local_tenant_task_.tenant_id_ = tenant_id;
  local_tenant_task_.ttl_continue_ = false;
  local_tenant_task_.is_dirty_ = false;
  ObMemAttr bucket_attr(tenant_id, "TTLTaskBucket");
  ObMemAttr node_attr(tenant_id, "TTLTaskNode");
  if (OB_FAIL(local_tenant_task_.tablet_task_map_.create(DEFAULT_TTL_BUCKET_NUM, bucket_attr, node_attr))) {
    LOG_WARN("fail to create ttl partition map", KR(ret), K(tenant_id));
  } else {}

  FLOG_INFO("finish create tenant ttl task", K(tenant_id), K(ret), KPC_(ls));
  return ret;
}

ObTTLTaskCtx* ObTabletTTLScheduler::get_one_tablet_ctx(const ObTabletID& tablet_id)
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
int ObTabletTTLScheduler::deep_copy_task(ObTTLTaskCtx* ctx, ObTTLTaskInfo& task_info, const ObTTLTaskParam &task_param, bool with_rowkey_copy /*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ctx is null", KR(ret));
  } else if (with_rowkey_copy && OB_FAIL(ctx->deep_copy_rowkey(task_info.row_key_)) ) {
    LOG_WARN("fail to deep copy rowkey", KR(ret), K(task_info.row_key_));
  } else {
    ctx->task_info_.ttl_del_cnt_ += task_info.ttl_del_cnt_;
    ctx->task_info_.max_version_del_cnt_ += task_info.max_version_del_cnt_;
    ctx->task_info_.scan_cnt_ += task_info.scan_cnt_;
    ctx->task_info_.err_code_ = task_info.err_code_;
    task_info.reset_cnt();
  }
  return ret;
}

int ObTabletTTLScheduler::handle_all_tablet_event(common::ObSArray<ObTabletID>& tablets)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObTabletTTLScheduler::handle_all_tablet_event", TTL_NORMAL_TIME_THRESHOLD);
  ObTTLTaskCtx* ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl mgr not init", KR(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    for (tablet_task_iter iter = local_tenant_task_.tablet_task_map_.begin();
        iter != local_tenant_task_.tablet_task_map_.end(); ++iter) {
      ctx = iter->second;
      if (need_skip_run()) {
        ret = OB_EAGAIN;
        FLOG_INFO("skip this run cuz of leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));
      } else if (OB_ISNULL(ctx)) {
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

int ObTabletTTLScheduler::handle_one_tablet_event(ObTTLTaskCtx* ctx)
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

int ObTabletTTLScheduler::check_tenant_memory()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl mgr not init", KR(ret));
  } else if (is_leader_) {
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

int ObTabletTTLScheduler::get_ttl_para_from_schema(const schema::ObTableSchema *table_schema,
                                                   ObTTLTaskParam &param)
{
  int ret = OB_SUCCESS;
  ObKVAttr attr;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema is null", KR(ret));
  } else if (!table_schema->get_kv_attributes().empty()) {
    if (OB_FAIL(ObTTLUtil::parse_kv_attributes(table_schema->get_kv_attributes(), attr))) {
      LOG_WARN("fail to parse kv attributes", KR(ret), K(table_schema->get_kv_attributes()));
    } else if (attr.type_ == ObKVAttr::ObTTLTableType::HBASE) {
      if (!ObHTableUtils::is_htable_schema(*table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to check htable schema", KR(ret), K(table_schema->get_table_name()));
      }
    } else if (attr.type_ == ObKVAttr::ObTTLTableType::REDIS) {
      if (OB_FAIL(ObRedisHelper::check_redis_ttl_schema(*table_schema, attr.redis_model_))) {
        LOG_WARN("fail to check redis schema", KR(ret), K(table_schema->get_table_name()));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid ObKVAttr type", K(ret), K(attr));
    }

    if (OB_SUCC(ret)) {
      param.max_version_ = attr.max_version_;
      param.ttl_ = attr.ttl_;
      param.is_htable_ = (attr.type_ == ObKVAttr::ObTTLTableType::HBASE);
      param.is_redis_table_ = (attr.type_ == ObKVAttr::ObTTLTableType::REDIS);
      param.is_redis_ttl_ = attr.is_redis_ttl_;
      param.redis_model_ = attr.redis_model_;
      LOG_DEBUG("success to find a hbase ttl partition", KR(ret), K(param));
    }
  }

  if (OB_SUCC(ret)) {
    param.tenant_id_ = table_schema->get_tenant_id();
    param.database_id_ = table_schema->get_database_id();
    param.user_id_ = table_schema->get_define_user_id();
    param.table_id_ = table_schema->get_table_id();
  }
  return ret;
}

// task already lock in ObTabletTTLScheduler::sync_sys_table
int ObTabletTTLScheduler::try_schedule_prepare_task(ObTabletID& tablet_id)
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

int ObTabletTTLScheduler::sync_all_dirty_task(ObIArray<ObTabletID>& dirty_tasks)
{
  int ret = OB_SUCCESS;
  bool tenant_state_changed = false;
  ObTimeGuard guard("ObTabletTTLScheduler::sync_all_dirty_record", TTL_NORMAL_TIME_THRESHOLD);
  for (int i = 0; OB_SUCC(ret) && i < dirty_tasks.count() && !tenant_state_changed; i++) {
    // tenant_state_changed is true means that tenant status is changed, we should refresh our status first
    if (need_skip_run()) {
      ret = OB_EAGAIN;
      FLOG_INFO("skip this run cuz of leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));
    } else if (OB_FAIL(sync_sys_table(dirty_tasks.at(i), tenant_state_changed))) {
      LOG_WARN("fail to sync sys table", KR(ret));
    }
  }
  return ret;
}

int ObTabletTTLScheduler::sync_sys_table_op(ObTTLTaskCtx* ctx,
                                            bool force_update,
                                            bool &tenant_state_changed)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  common::ObTTLStatus ttl_record;
  bool commit = false;
  int tmp_ret = OB_SUCCESS;
  bool is_exists = false;
  bool is_end_state = false;
  if (OB_FAIL(trans.start(get_sql_proxy(), gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("fail to start transation", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObTTLUtil::check_task_status_from_sys_table(tenant_id_, trans, ctx->task_info_.task_id_,
              ctx->task_info_.table_id_, ctx->task_info_.tablet_id_, is_exists, is_end_state))) {
    LOG_WARN("fail to check ttl task exist");
  } else if (is_end_state) {
    // record in system table is end state, do nothing
    FLOG_INFO("Finished / Canceled in sys table, could not sync sys table", K(local_tenant_task_));
  } else if (!is_exists) {
    if (OB_FAIL(construct_sys_table_record(ctx, ttl_record))) {
      LOG_WARN("fail to construct sys table record", KR(ret));
    } else if (OB_FAIL(ObTTLUtil::insert_ttl_task(tenant_id_, share::OB_ALL_KV_TTL_TASK_TNAME,
                                                  trans, ttl_record))) {
      LOG_WARN("fail to insert ttl task", KR(ret));
    }
  } else if (force_update) {
    if (OB_FAIL(construct_sys_table_record(ctx, ttl_record))) {
      LOG_WARN("fail to construct sys table record", KR(ret));
      } else if (OB_FAIL(ObTTLUtil::update_ttl_task_all_fields(tenant_id_,
                                                               share::OB_ALL_KV_TTL_TASK_TNAME,
                                                               trans, ttl_record))) {
      LOG_WARN("fail to update ttl task in sys table", KR(ret), K(ttl_record));
    }
  }

  // check and ensure the tenant status not change in this transaction
  // when tablet task is adready in terminal status, do not need check tenant state
  // beause we won't change tablet status in such case
  if (OB_SUCC(ret) && !is_end_state && OB_FAIL(ObTTLUtil::check_tenant_state(tenant_id_,
                                                                             get_tenant_task_table_id(),
                                                                             trans,
                                                                             local_tenant_task_.state_,
                                                                             local_tenant_task_.task_id_,
                                                                             tenant_state_changed))) {
    FLOG_INFO("local tenant task state is different from sys table", KR(ret),
      K_(tenant_id), K(local_tenant_task_.state_));
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

int ObTabletTTLScheduler::sync_sys_table(ObTabletID& tablet_id, bool &tenant_state_changed)
{
  int ret = OB_SUCCESS;
  tenant_state_changed = false;
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

  if (OB_SUCC(ret) && !ctx->in_queue_) {
    switch (ctx->task_status_) {
      case OB_TTL_TASK_PREPARE: {
        if (OB_FAIL(sync_sys_table_op(ctx, false, tenant_state_changed))) {
          LOG_WARN("fail to sync ttl record into sys table", KR(ret));
        } else if (OB_FAIL(try_schedule_prepare_task(tablet_id))) {
          // change prepare state to running/pending
          LOG_WARN("fail to schedule prepare task", KR(ret));
        }
        break;
      }
      case OB_TTL_TASK_FINISH:
      case OB_TTL_TASK_RUNNING:
      case OB_TTL_TASK_PENDING:
      case OB_TTL_TASK_CANCEL: {
        if (OB_FAIL(sync_sys_table_op(ctx, true, tenant_state_changed))) {
          LOG_WARN("fail to sync ttl record into sys table", KR(ret));
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

  if (OB_SUCC(ret) && !ctx->in_queue_) {
    //mark ctx dirty false
    ctx->is_dirty_ = false;
    LOG_INFO("finish mark ctx dirty false", KR(ret), K(tablet_id), KPC(ctx));
  }
  return ret;
}

int ObTabletTTLScheduler::construct_sys_table_record(ObTTLTaskCtx* ctx, common::ObTTLStatus& ttl_record)
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
    ttl_record.task_type_ = local_tenant_task_.task_type_;
  }
  return ret;
}

int ObTabletTTLScheduler::construct_task_record_filter(const uint64_t& task_id,
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

int ObTabletTTLScheduler::from_ttl_record(ObTabletID& tablet_id, common::ObTTLStatus& record, bool with_status /*true*/, bool with_err_code /*true*/)
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
      if (OB_FAIL(ctx->deep_copy_rowkey(record.row_key_))) {
        LOG_WARN("fail to deep copy rowkey", KR(ret), K(record.row_key_));
      }
    }
  }
  LOG_DEBUG("finish from ttl record", KR(ret), K(tablet_id));
  return ret;
}

bool ObTabletTTLScheduler::can_schedule_tenant(const ObTTLTenantInfo &tenant_info)
{
  return tenant_info.ttl_continue_ && tenant_info.state_ == OB_TTL_TASK_RUNNING;
}

bool ObTabletTTLScheduler::can_schedule_task(const ObTTLTaskCtx &ttl_task)
{
  return is_leader_ && ttl_task.task_status_ == OB_TTL_TASK_PENDING;
}

int ObTabletTTLScheduler::try_schedule_remaining_tasks(const ObTTLTaskCtx *current_ctx)
{
  int ret = OB_SUCCESS;
  if (can_schedule_tenant(local_tenant_task_)) {
    ObTTLTaskCtx* ctx = nullptr;
    for (tablet_task_iter iter = local_tenant_task_.tablet_task_map_.begin();
                            iter != local_tenant_task_.tablet_task_map_.end()
                            && OB_SUCC(ret); ++iter) {
      ctx = iter->second;
      if (need_skip_run()) {
        ret = OB_EAGAIN;
        FLOG_INFO("skip this run cuz of leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));
      } else if (OB_ISNULL(ctx)) {
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
int ObTabletTTLScheduler::try_schedule_task(ObTTLTaskCtx* ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ttl task ctx is null", KR(ret));
  } else if (can_schedule_tenant(local_tenant_task_) && can_schedule_task(*ctx)) {
    ret = generate_ttl_dag<ObTableTTLDeleteTask, ObTableTTLDag>(ctx->task_info_, ctx->ttl_para_);
    if (ret != OB_SUCCESS) {
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
      ctx->in_queue_ = true;
      ctx->task_status_ = OB_TTL_TASK_RUNNING;
      // mark ctx dirty later in report_task_status in case of watting too long in dag queue
    }
  } else {
    LOG_DEBUG("status when try schedule task", K(local_tenant_task_.ttl_continue_), K(local_tenant_task_.state_), K(ctx->task_status_));
  }
  return ret;
}

void ObTabletTTLScheduler::mark_ttl_ctx_dirty(ObTTLTenantInfo& tenant_info, ObTTLTaskCtx& ctx)
{
  ctx.is_dirty_ = true;
  local_tenant_task_.is_dirty_ = true;
}

int ObTabletTTLScheduler::refresh_tablet_task(ObTTLTaskCtx &ttl_task, bool refresh_status, bool refresh_retcode /*false*/)
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
int ObTabletTTLScheduler::reload_tenant_task()
{
  common::ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  ObTTLStatus tenant_task;
  ObTTLTaskStatus expected_state;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_leader_) {
    // do nothing
  } else if (OB_FAIL(ObTTLUtil::read_tenant_ttl_task(tenant_id_, get_tenant_task_table_id(), *sql_proxy_, tenant_task))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      local_tenant_task_.reuse();
    } else {
      LOG_WARN("fail to read tenant ttl task", KR(ret), K_(tenant_id));
    }
  } else if (OB_RS_TTL_TASK_MOVE == static_cast<ObTTLTaskStatus>(tenant_task.status_) ||
             OB_RS_TTL_TASK_CANCEL == static_cast<ObTTLTaskStatus>(tenant_task.status_)) {
    local_tenant_task_.reuse();
    FLOG_INFO("tenant task is finish now, reuse local tenant task", KR(ret), K_(local_tenant_task), K(tenant_task.task_id_));
  } else if (OB_FAIL(ObTTLUtil::transform_tenant_state(static_cast<ObTTLTaskStatus>(tenant_task.status_), expected_state))) {
    LOG_WARN("fail to transform ttl tenant task status", KR(ret), K(tenant_task.status_));
  } else if (local_tenant_task_.task_id_ != tenant_task.task_id_) {
    local_tenant_task_.reuse();
    local_tenant_task_.task_id_ = tenant_task.task_id_;
    local_tenant_task_.is_usr_trigger_ = (tenant_task.trigger_type_ == USER_TRIGGER);
    local_tenant_task_.state_ = expected_state;
    local_tenant_task_.need_check_ = true;
    local_tenant_task_.is_dirty_ = true;
    local_tenant_task_.is_reused_ = false;
    local_tenant_task_.task_type_ = tenant_task.task_type_;
    FLOG_INFO("new ttl task", KR(ret), K_(tenant_id), K_(local_tenant_task));
  } else if (OB_TTL_TASK_FINISH == static_cast<ObTTLTaskStatus>(local_tenant_task_.state_)) {
    // do nothing
  } else if (local_tenant_task_.state_ != expected_state) {
    FLOG_INFO("change local tenant task status", KR(ret), K_(tenant_id), K_(local_tenant_task), K(tenant_task));
    // current tenant task status changed
    local_tenant_task_.state_ = expected_state;
    local_tenant_task_.is_dirty_ = true;
  } else {/* task status not change, do nothing */}
  FLOG_INFO("finish reload tenant task", K(local_tenant_task_), K(tenant_task), K_(is_leader));
  return ret;
}

int ObTabletTTLScheduler::check_schema_version()
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
    FLOG_INFO("schema changed", KR(ret), K_(local_schema_version), K(schema_version));
    local_schema_version_ = schema_version;
    mark_tenant_need_check();
  }
  return ret;
}

int ObTabletTTLScheduler::safe_to_destroy(bool &is_safe)
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

int ObTTLTaskCtx::deep_copy_rowkey(const ObString &rowkey)
{
  int ret = OB_SUCCESS;
  task_info_.row_key_.reset();
  rowkey_cp_allcoator_.reuse();
  if (OB_FAIL(ob_write_string(rowkey_cp_allcoator_, rowkey, task_info_.row_key_))) {
    LOG_WARN("fail to deep copy rowkey", KR(ret), K(rowkey));
  }
  return ret;
}

// reuse means this task id is finished
void ObTabletTTLScheduler::ObTTLTenantInfo::reuse()
{
  if (OB_UNLIKELY(!is_reused_)) {
    for (TabletTaskMap::const_iterator iter = tablet_task_map_.begin(); iter != tablet_task_map_.end();
        ++iter) {
      ObTTLTaskCtx *ctx = iter->second;
      if (OB_NOT_NULL(ctx)) {
        ctx->~ObTTLTaskCtx();
      }
    }
    tablet_task_map_.reuse();
    allocator_.reset();
    is_usr_trigger_ = false;
    need_check_ = false;
    is_dirty_ = false;
    ttl_continue_ = true;
    state_ = common::ObTTLTaskStatus::OB_TTL_TASK_FINISH;
    is_reused_ = true;
    task_id_ = OB_INVALID_ID;
    FLOG_INFO("reuse tenant info", K(*this));
  }
}

/**
 * ---------------------------------------- ObTabletHRowkeyTTLScheduler ----------------------------------------
 */


int ObTabletHRowkeyTTLScheduler::report_task_status(ObTTLTaskInfo& task_info, ObTTLTaskParam& task_para,
                                                    bool& is_stop, bool need_copy_task/* true*/)
{
  int ret = OB_SUCCESS;
  ObTTLTaskCtx* ctx = nullptr;
  common::ObSpinLockGuard guard(lock_);
  is_stop = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("hrowkey ttl scheduler is not init,", KR(ret));
  } else if (OB_FAIL(local_tenant_task_.tablet_task_map_.get_refactored(task_info.tablet_id_, ctx))) {
    LOG_WARN("fail to get tablet task ctx", KR(ret), K_(task_info.tablet_id), K_(tenant_id));
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
    ctx->in_queue_ = false;
    mark_ttl_ctx_dirty(local_tenant_task_, *ctx);
    // A tablet task is associated with multiple rowkey ttl tasks,
    // so it's meaningless to copy the rowkey of current task
    if (need_copy_task && OB_FAIL(deep_copy_task(ctx, task_info, task_para, false))) {
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
    } else {
      ctx->task_status_ = OB_TTL_TASK_PENDING;
      if (OB_ITER_END == task_info.err_code_) {
        ctx->task_info_.err_code_ = OB_SUCCESS;
        LOG_INFO("one rowkey TTL task execute finished", KR(ret));
      } else if (OB_NOT_MASTER == task_info.err_code_ || OB_PARTITION_NOT_EXIST == task_info.err_code_ ||
                 OB_TABLE_NOT_EXIST == task_info.err_code_ || OB_ERR_UNKNOWN_TABLE == task_info.err_code_ ||
                 OB_LS_NOT_EXIST == task_info.err_code_) {
        LOG_INFO("cancel current task since partition state change", K(task_info.err_code_), K(task_info.tablet_id_));
      } else {
        LOG_WARN("task report error", K(task_info.err_code_), K(task_info.tablet_id_));
        ctx->failure_times_++;
      }
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


/**
 * ---------------------------------------- ObTenantTabletTTLMgr ----------------------------------------
 */

ObTenantTabletTTLMgr::ObTenantTabletTTLMgr()
  : is_inited_(false),
    tenant_id_(MTL_ID()),
    ls_id_(ObLSID::INVALID_LS_ID),
    vec_tg_id_(0),
    tablet_ttl_schedulers_()
{
  tablet_ttl_schedulers_.set_attr(ObMemAttr(MTL_ID(), "TabletTTLSchArr"));
}

int ObTenantTabletTTLMgr::init(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init ObTenantTabletTTLMgr twice", K(ret));
  } else if (OB_FAIL(alloc_and_init_tablet_scheduler<ObTabletTTLScheduler>(ls))) {
    LOG_WARN("fail to alloc and init tablet scheduler", K(ret), KPC(ls));
  } else if (OB_FAIL(alloc_and_init_tablet_scheduler<ObTabletHRowkeyTTLScheduler>(ls))) {
    LOG_WARN("fail to alloc and init hrowkey ttl scheduler", K(ret), KPC(ls));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TenantTabletTTLMgr, vec_tg_id_))) {
    LOG_WARN("fail to init timer", KR(ret));
  } else if (OB_FAIL(TG_START(vec_tg_id_))) {
    LOG_WARN("fail to create ObTenantTabletTTLMgr thread", K(ret), K_(vec_tg_id));
  } else if (OB_FAIL(vector_idx_scheduler_.init(MTL_ID(), ls, vec_tg_id_))) {
    LOG_WARN("fail to init vector idx scheduler", KR(ret), K(MTL_ID()));
  } else {
    ls_id_ = ls->get_ls_id();
    is_inited_ = true;
  }
  return ret;
}

template <typename T>
int ObTenantTabletTTLMgr::alloc_and_init_tablet_scheduler(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  T *tablet_scheduler = NULL;
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ls", K(ret));
  } else if (OB_ISNULL(tablet_scheduler = OB_NEWx(T, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(tablet_scheduler->init(ls))) {
    tablet_scheduler->~T();
    LOG_WARN("fail to init ttl tablet scheduler", K(ret), KPC(ls));
  } else if (OB_FAIL(tablet_ttl_schedulers_.push_back(tablet_scheduler))) {
    tablet_scheduler->~T();
    LOG_WARN("fail to add ttl tablet scheduler", K(ret));
  }
  return ret;
}

#define DELEGATE_TTL_SCHEDULERS_WITH_RET(func_name, ...) \
    int ret = OB_SUCCESS;                                     \
    int64_t start_time_us = ObTimeUtility::current_time();    \
    FLOG_INFO("ObTenantTabletTTLMgr: start to " MSTR(func_name), K_(tenant_id), K_(ls_id), K(start_time_us)); \
    if (IS_NOT_INIT) {                                                                  \
      ret = OB_NOT_INIT;                                                                \
      LOG_WARN("tablet ttl mgr is not init,", KR(ret));                                 \
    }                                                                                   \
    for (int i = 0; OB_SUCC(ret) && i < tablet_ttl_schedulers_.count(); i++) {          \
      ObTabletTTLScheduler *ttl_scheduler = tablet_ttl_schedulers_.at(i);               \
      if (OB_ISNULL(ttl_scheduler)) {                                                   \
        ret = OB_ERR_UNEXPECTED;                                                        \
        LOG_WARN("unexpected null ttl scheduler", K(ret), K(i));                        \
      } else if (OB_FAIL(ttl_scheduler->func_name(__VA_ARGS__))) {      \
        LOG_WARN("fail to " MSTR(func_name), K(ret), KPC(ttl_scheduler));               \
      }                                                                                 \
    }                                                                                   \
    const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;              \
    FLOG_INFO("ObTenantTabletTTLMgr: finish to " MSTR(func_name), KR(ret), K_(tenant_id), K_(ls_id), K(cost_us)); \
    return ret;

#define DELEGATE_TTL_SCHEDULERS_NOT_RET(func_name, ...) \
    int64_t start_time_us = ObTimeUtility::current_time();    \
    FLOG_INFO("ObTenantTabletTTLMgr: start to " MSTR(func_name), K_(tenant_id), K_(ls_id), K(start_time_us)); \
    if (IS_INIT) {                                                                        \
      for (int i = 0; i < tablet_ttl_schedulers_.count(); i++) {                          \
        ObTabletTTLScheduler *ttl_scheduler = tablet_ttl_schedulers_.at(i);               \
        if (OB_NOT_NULL(ttl_scheduler)) {                                                 \
          ttl_scheduler->func_name(__VA_ARGS__);                                          \
        }                                                                                 \
      }                                                                                   \
    }                                                                                     \
    const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;                \
    FLOG_INFO("ObTenantTabletTTLMgr: finish to " MSTR(func_name), K_(tenant_id), K_(ls_id), K(cost_us));

int ObTenantTabletTTLMgr::switch_to_leader()
{
  DELEGATE_TTL_SCHEDULERS_WITH_RET(switch_to_leader);
}

int ObTenantTabletTTLMgr::resume_leader()
{
  DELEGATE_TTL_SCHEDULERS_WITH_RET(resume_leader);
}

int ObTenantTabletTTLMgr::safe_to_destroy(bool &is_safe_destroy)
{
  int ret = OB_SUCCESS;
  is_safe_destroy = true;
  if (IS_INIT) {
    for (int i = 0; OB_SUCC(ret) && is_safe_destroy && i < tablet_ttl_schedulers_.count(); i++) {
      ObTabletTTLScheduler *ttl_scheduler = tablet_ttl_schedulers_.at(i);
      if (OB_ISNULL(ttl_scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ttl scheduler", K(ret), K(i));
      } else if (OB_FAIL(ttl_scheduler->safe_to_destroy(is_safe_destroy))) {
        LOG_WARN("fail to safe to destory", K(ret), KPC(ttl_scheduler));
      }
    }
  }
  return ret;
}

int ObTenantTabletTTLMgr::switch_to_follower_gracefully()
{
  DELEGATE_TTL_SCHEDULERS_WITH_RET(switch_to_follower_gracefully);
}

void ObTenantTabletTTLMgr::switch_to_follower_forcedly()
{
  DELEGATE_TTL_SCHEDULERS_NOT_RET(switch_to_follower_forcedly);
}

void ObTenantTabletTTLMgr::stop()
{
  if (vec_tg_id_ != 0) {
    TG_STOP(vec_tg_id_);
    vector_idx_scheduler_.stop();
  }
  DELEGATE_TTL_SCHEDULERS_NOT_RET(stop);
}

void ObTenantTabletTTLMgr::destroy()
{
  if (vec_tg_id_ != 0) {
    TG_WAIT(vec_tg_id_);
    TG_DESTROY(vec_tg_id_);
  }
  DELEGATE_TTL_SCHEDULERS_NOT_RET(~ObTabletTTLScheduler);
}

int ObTabletHRowkeyTTLScheduler::try_schedule_task(ObTTLTaskCtx* ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ttl task ctx is null", KR(ret));
  } else if (can_schedule_tenant(local_tenant_task_) && can_schedule_task(*ctx)) {
    if (OB_FAIL(generate_rowkey_ttl_task())) {
      LOG_WARN("fail to generate rowkey ttl task", K(ret));
    }
  } else {
    LOG_DEBUG("status when try schedule task", K(local_tenant_task_.ttl_continue_), K(local_tenant_task_.state_), K(ctx->task_status_));
  }
  return ret;
}

int ObTabletHRowkeyTTLScheduler::generate_rowkey_ttl_task()
{
  int ret = OB_SUCCESS;
  HRowkeyQueueNode *queue_node = nullptr;
  HRowkeyDedupMap dedupmap(hrowkey_queue_);
  if (OB_FAIL(dedupmap.init())) {
    LOG_WARN("fail to init hbase rowkey deduplicate map", K(ret));
  } else {
    // add queue node into deduplicate map
    bool stop = false;
    for (int64_t i = 0; OB_SUCC(ret) && !stop && i < MAX_HROWKEY_NUM_PER_TASK; i++) {
      if (OB_FAIL(hrowkey_queue_.pop(queue_node))) {
        if (ret != OB_ENTRY_NOT_EXIST) {
          LOG_WARN("fail to pop queue node", K(ret));
        } else {
          LOG_DEBUG("hrowkey queue is empty", K(ret));
          ret = OB_SUCCESS;
          stop = true;
        }
      } else {
        LOG_DEBUG("pop queue node", K(ret), KPC(queue_node));
        if (OB_FAIL(dedupmap.add_queue_node(queue_node))) {
          if (OB_ENTRY_EXIST == ret) {
            LOG_DEBUG("queue node exists", K(ret), KPC(queue_node));
            ret = OB_SUCCESS;
            stop = true;
          } else {
            LOG_WARN("fail to add queue node", K(ret));
          }
          hrowkey_queue_.free_queue_node(queue_node);
        }
      }
    }
  }

  // traverse deduplicate map and generate ttl task
  if (OB_SUCC(ret)) {
    ObSEArray<ObString, 8> rowkeys;
    HRowkeyDedupMap::HRowkeyTabletMap::iterator iter = dedupmap.map_.begin();
    for (; OB_SUCC(ret) && iter != dedupmap.map_.end(); ++iter) {
      HRowkeyDedupMapKey &key = iter->first;
      HRowkeyDedupMap::HRowkeySet *hrowkey_set = iter->second;
      if (OB_ISNULL(hrowkey_set)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null hrowkey set", K(ret));
      } else {
        TraverseArgs args(rowkeys);
        hrowkey_set->iter_rbtree(hrowkey_set, NULL, &traverse_rowkey_set, &args);

        // generate rowkey ttl task
        ObTTLTaskCtx* ctx = nullptr;
        if (OB_ISNULL(ctx = get_one_tablet_ctx(key.tablet_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cannot find tablet task ctx", K(ret));
        } else {
          ObTTLHRowkeyTaskParam task_param(ctx->ttl_para_, rowkeys);
          ret = ObTabletTTLScheduler::generate_ttl_dag<ObTableHRowKeyTTLDelTask, ObTableTTLDag, ObTTLHRowkeyTaskParam>(ctx->task_info_, task_param);
          if (OB_FAIL(ret)) {
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
            ctx->in_queue_ = true;
            ctx->task_status_ = OB_TTL_TASK_RUNNING;
          }
        }
      }
      rowkeys.reuse();
    }
  }

  return ret;
}

int ObTabletHRowkeyTTLScheduler::init(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hrowkey_queue_.init())) {
    LOG_WARN("fail to init hrowkey queue", K(ret));
  } else if (OB_FAIL(ObTabletTTLScheduler::init(ls))) {
    LOG_WARN("fail to init hbae rowkey ttl scheduler", K(ret));
  }
  return ret;
}

int ObTabletHRowkeyTTLScheduler::do_after_leader_switch()
{
  int ret = OB_SUCCESS;
  if (true == ATOMIC_LOAD(&is_leader_)) {
    if (OB_FAIL(MTL(ObHTableRowkeyMgr*)->register_rowkey_queue(ls_->get_ls_id(), hrowkey_queue_))) {
      LOG_WARN("fail to register rowkey queue", K(ret), KPC_(ls));
    }
  } else {
    if (OB_FAIL(MTL(ObHTableRowkeyMgr*)->unregister_rowkey_queue(ls_->get_ls_id(), hrowkey_queue_))) {
      LOG_WARN("fail to register rowkey queue", K(ret), KPC_(ls));
    }
  }
  return ret;
}

int ObTabletHRowkeyTTLScheduler::HRowkeyDedupMap::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr bucket_attr(MTL_ID(), "HRkDedupMapBkt");
  ObMemAttr node_attr(MTL_ID(), "HRkDedupMapNode");
  if (OB_FAIL(map_.create(1024, bucket_attr, node_attr))) {
    LOG_WARN("fail to create ttl partition map", KR(ret), K(MTL_ID()));
  } else {}
  return ret;
}

void ObTabletHRowkeyTTLScheduler::HRowkeyDedupMap::destroy()
{
  // reclaim rowkey memory
  HRowkeyTabletMap::iterator iter = map_.begin();
  for (; iter != map_.end(); ++iter) {
    HRowkeySet *hrowkey_set = iter->second;
    FreeHRowkeyArgs free_args(hrowkey_owner_);
    hrowkey_set->destroy(hrowkey_set, &free_hrowkey, &free_args);
  }
}

int ObTabletHRowkeyTTLScheduler::HRowkeyDedupMap::add_queue_node(HRowkeyQueueNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("queue node is null", K(ret));
  } else {
    HRowkeyDedupMapKey key(node->table_id_, node->tablet_id_);
    HRowkeySet *rowkey_set = NULL;
    if (OB_FAIL(map_.get_refactored(key, rowkey_set))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("fail to get rowkey set", K(ret), K(key));
      } else {
        ret = OB_SUCCESS;
        // create new rowkey set and insert into
        if (OB_ISNULL(rowkey_set = OB_NEWx(HRowkeySet, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else if (OB_FAIL(map_.set_refactored(key, rowkey_set))) {
          LOG_WARN("fail to set rowkey set");
        } else if (OB_FAIL(rowkey_set->insert(node))) {
          LOG_WARN("fail to insert into rowkey set", K(ret));
        }
      }
    } else if (OB_ISNULL(rowkey_set)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null rowkey set", K(ret));
    } else {
      // insert into
      HRowkeyNode *exist_node = NULL;
      if (OB_FAIL(rowkey_set->search(node, exist_node))) {
        LOG_WARN("fail to search hrowkey node", K(ret), K(key));
      } else if (OB_NOT_NULL(exist_node)) {
        ret = OB_ENTRY_EXIST;
        LOG_DEBUG("hbase rowkey exist", K(ret), K(key));
      } else if (OB_FAIL(rowkey_set->insert(node))) {
        LOG_WARN("fail to insert into rowkey set", K(ret));
      }
    }
  }
  return ret;
}

void ObTabletHRowkeyTTLScheduler::free_hrowkey(HRowkeyNode *cur_node, void *args)
{
  FreeHRowkeyArgs *free_args = static_cast<FreeHRowkeyArgs *>(args);
  if (OB_ISNULL(free_args)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "free args is null");
  } else {
    free_args->hrowkey_owner_.free_queue_node(cur_node);
  }
}

HRowkeyNode *ObTabletHRowkeyTTLScheduler::traverse_rowkey_set(HRowkeyDedupMap::HRowkeySet *tree,
                                                              HRowkeyNode *cur_node,
                                                              void *args)
{
  int ret = OB_SUCCESS;
  TraverseArgs *traverse_args = static_cast<TraverseArgs *>(args);
  if (OB_ISNULL(cur_node) || OB_ISNULL(traverse_args)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur node or traverse agrs is null", K(ret));
  } else {
    ObIArray<ObString> &rowkeys = traverse_args->rowkeys_;
    if (OB_FAIL(rowkeys.push_back(cur_node->hrowkey_))) {
      LOG_WARN("fail to push back hbase rowkey", K(ret));
    }
  }
  return NULL;
}

int ObTabletHRowkeyTTLScheduler::check_is_ttl_table(const ObTableSchema &table_schema, bool &is_ttl_table)
{
  return ObTTLUtil::check_is_htable_ttl(table_schema, is_ttl_table);
}

} // table
} // oceanbase
