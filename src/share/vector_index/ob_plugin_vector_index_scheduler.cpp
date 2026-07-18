/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER
#include "ob_plugin_vector_index_scheduler.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/ob_debug_sync.h"
#include "storage/ob_value_row_iterator.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace share
{
int ObPluginVectorIndexLoadScheduler::init(uint64_t tenant_id, ObLS *ls, int ttl_timer_tg_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  if (OB_ISNULL(vector_index_service) || OB_ISNULL(ls) || ttl_timer_tg_id == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant vector index load task fail",
      KP(vector_index_service), KP(ls), K(ttl_timer_tg_id), KR(ret));
  } else {
    vector_index_service_ = vector_index_service;
    ls_ = ls;
    tenant_id_ = tenant_id;
    interval_factor_ = 1;
    is_inited_ = true;
    ttl_tablet_timer_tg_id_ = ttl_timer_tg_id;
    basic_period_ = VEC_INDEX_SCHEDULER_BASIC_PERIOD;
    cb_.scheduler_ = this;
    if (OB_FAIL(TG_SCHEDULE(ttl_timer_tg_id, *this, basic_period_, true))) {
      LOG_WARN("fail to schedule periodic task", KR(ret), K(ttl_timer_tg_id));
    } else {
      ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
      // LS is valid here but may not be in ls_map yet (register_to_service runs before add_ls_to_map).
      if (OB_FAIL(vector_index_service_->acquire_vector_index_mgr(
              ls_->get_ls_id(), index_ls_mgr, false /*check_ls_exist*/))) {
        LOG_WARN("fail to get vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
      } else if (OB_ISNULL(index_ls_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
      } else if (index_ls_mgr->is_ls_destroying()) {
        // Stale orphan mgr left behind by a prior LS instance that skipped the
        // normal safe-destroy path (e.g. gc_ls_after_replay_slog). This LS
        // instance is brand new, so evict it and create a fresh mgr. This is
        // unexpected but recoverable, report it and then reset ret to continue.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[VEC_INDEX] evict stale destroying vec index mgr on ls re-init",
                 KR(ret), K(tenant_id_), K(ls_->get_ls_id()), KP(index_ls_mgr));
        ret = OB_SUCCESS;
        index_ls_mgr = nullptr;
        if (OB_FAIL(vector_index_service_->remove_ls_index_mgr(ls_->get_ls_id()))) {
          LOG_WARN("failed to remove stale ls index mgr", KR(ret), K(ls_->get_ls_id()));
        } else if (OB_FAIL(vector_index_service_->acquire_vector_index_mgr(
                       ls_->get_ls_id(), index_ls_mgr, false /*check_ls_exist*/))) {
          LOG_WARN("fail to re-acquire vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
        } else if (OB_ISNULL(index_ls_mgr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid vector index ls mgr after evict", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
        }
      }
    }
  }
  return ret;
}

void ObPluginVectorIndexLoadScheduler::runTimerTask()
{
  // not work anymore, see ObVecIdxAsyncTaskScheduler
  // ObCurTraceId::init(GCONF.self_addr_);
  // ObTimeGuard guard("ObPluginVectorIndexLoadScheduler::runTimerTask",
  //   VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD);
  // run_task();
}

bool ObPluginVectorIndexLoadScheduler::disallow_cancel_task(const ObVecIndexAsyncTaskCtx *task_ctx)
{
  return ObVecIndexAsyncTaskUtil::disallow_cancel_task(task_ctx);
}

void ObPluginVectorIndexLoadScheduler::clean_deprecated_adapters()
{
  DEBUG_SYNC(BEFORE_CLEAN_DEPRECATED_ADAPTERS);
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, DEFAULT_TABLE_ARRAY_SIZE> local_tablet_id_array;
  ObSEArray<ObTabletID, DEFAULT_TABLE_ARRAY_SIZE> detach_tablet_id_array;
  ObSEArray<ObTabletID, DEFAULT_TABLE_ARRAY_SIZE> remove_tenant_tablet_id_array;
  bool clear_ls_follower_adapter = false;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (OB_FAIL(vector_index_service_->get_ls_index_mgr_map().get_refactored(ls_->get_ls_id(), index_ls_mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
    }
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  }

  if (OB_SUCC(ret)) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail get tenant_config", KR(ret), K(tenant_id_));
    } else {
      clear_ls_follower_adapter = !ATOMIC_LOAD(&is_leader_) && !tenant_config->load_vector_index_on_follower;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(index_ls_mgr)) {
    bool is_in_migration = false;
    if (OB_FAIL(index_ls_mgr->collect_adaptor_tablet_ids(local_tablet_id_array))) {
      LOG_WARN("failed to collect adaptor tablet ids", KR(ret), K(ls_->get_ls_id()));
    } else if (OB_FAIL(check_ls_in_vector_migration(is_in_migration))) {
      LOG_WARN("failed to check ls migration status", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < local_tablet_id_array.count(); i++) {
      const ObTabletID tablet_id = local_tablet_id_array.at(i);
      ObPluginVectorIndexAdapterGuard local_guard;
      ObPluginVectorIndexAdapterGuard tenant_guard;
      ObSchemaGetterGuard schema_guard;
      const ObSimpleTableSchemaV2 *table_schema = NULL;
      ObTabletHandle tablet_handle;
      bool need_detach = false;
      bool need_remove_tenant = false;
      int tenant_ret = OB_SUCCESS;
      if (OB_FAIL(index_ls_mgr->get_adapter_inst_guard(tablet_id, local_guard))) {
        if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get local adaptor guard", KR(ret), K(tablet_id), K(ls_->get_ls_id()));
        }
      } else if (OB_ISNULL(local_guard.get_adatper())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("local adaptor is null", KR(ret), K(tablet_id), K(ls_->get_ls_id()));
      } else if (FALSE_IT(tenant_ret = vector_index_service_->get_tenant_adapter_inst_guard(tablet_id, tenant_guard))) {
      } else if (tenant_ret != OB_SUCCESS && tenant_ret != OB_HASH_NOT_EXIST) {
        ret = tenant_ret;
        LOG_WARN("failed to get tenant adaptor guard", KR(ret), K(tablet_id));
      } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_,
                                                              local_guard.get_adatper()->get_inc_table_id(),
                                                              table_schema))) {
        LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(local_guard.get_adatper()->get_inc_table_id()));
        ret = OB_SUCCESS; // override error to success, because it is not critical
      } else if (OB_ISNULL(table_schema) || table_schema->is_in_recyclebin()) {
        // remove adapter if tablet not exist or is in recyclebin
        LOG_INFO("remove adapter if tablet not exist or is in recyclebin", K(tablet_id));
        need_detach = true;
        need_remove_tenant = true;
      } else if (clear_ls_follower_adapter) {
        LOG_INFO("clean ls follower adapter", K(tablet_id));
        need_detach = true;
        need_remove_tenant = true;
      } else if (OB_HASH_NOT_EXIST == tenant_ret) {
        // in migration, skip detach
        if (!is_in_migration) {
          need_detach = true;
        } else {
          LOG_INFO("in migration, skip detach", K(tablet_id), K(is_in_migration));
        }
      } else if (tenant_guard.get_adatper() != local_guard.get_adatper()) {
        need_detach = true;
      } else if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(tablet_id, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
        } else {
          ret = OB_SUCCESS;
          need_detach = true;
          LOG_INFO("fail to get tablet, need detach", K(ret), K(tablet_id));
        }
      }

      if (OB_SUCC(ret) && need_detach) {
        if (OB_FAIL(detach_tablet_id_array.push_back(tablet_id))) {
          LOG_WARN("failed to record detach tablet id", KR(ret), K(tablet_id));
        }
      }
      if (OB_SUCC(ret) && need_remove_tenant) {
        // Only mark adaptor cancel when releasing from tenant_map. Detach-only
        // (e.g. transfer) must not poison an adaptor reused on another LS.
        if (OB_NOT_NULL(local_guard.get_adatper())) {
          local_guard.get_adatper()->set_need_cancel_task();
        }
        if (OB_FAIL(remove_tenant_tablet_id_array.push_back(tablet_id))) {
          LOG_WARN("failed to record tenant detach tablet id", KR(ret), K(tablet_id));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < detach_tablet_id_array.count(); i++) {
      bool detached = false;
      // cancel async task before detach to avoid leaving a dangling task ctx
      if (OB_FAIL(index_ls_mgr->detach_adapter(detach_tablet_id_array.at(i), detached))) {
        LOG_WARN("failed to detach local adaptor", KR(ret), K(ls_->get_ls_id()), K(detach_tablet_id_array.at(i)));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < remove_tenant_tablet_id_array.count(); i++) {
      if (OB_FAIL(vector_index_service_->erase_tenant_vec_adaptor(remove_tenant_tablet_id_array.at(i)))) {
        if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to erase tenant adaptor", KR(ret), K(remove_tenant_tablet_id_array.at(i)));
        }
      }
    }
  }
}

void ObPluginVectorIndexLoadScheduler::clean_deprecated_ivf_caches()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, DEFAULT_TABLE_ARRAY_SIZE> delete_cache_tablet_id_array;
  delete_cache_tablet_id_array.reset();
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;

  if (OB_FAIL(vector_index_service_->get_ls_index_mgr_map().get_refactored(ls_->get_ls_id(), index_ls_mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
    }
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(index_ls_mgr)) {
    {
      // Acquire read lock to protect ivf_cache_mgr_map iteration
      RWLock::RLockGuard lock_guard(index_ls_mgr->get_adapter_map_lock());
      IvfCacheMgrMap &ivf_cache_mgr_map = index_ls_mgr->get_ivf_cache_mgr_map();

      // Iterate through all IVF cache managers (in lock scope, only mark for deletion)
      FOREACH_X(iter, ivf_cache_mgr_map, OB_SUCC(ret)) {
        ObTabletID cache_tablet_id = iter->first;
        ObIvfCacheMgr *cache_mgr = iter->second;
        ObTabletHandle tablet_handle;
        bool need_delete = false;
        if (OB_ISNULL(cache_mgr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null cache mgr skip current loop", KR(ret), K(cache_tablet_id));
          continue;
        }
         // Check if table schema exists and is in recyclebin
         ObSchemaGetterGuard schema_guard;
         const ObSimpleTableSchemaV2 *simple_table_schema = nullptr;
         if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
           LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
         } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, cache_mgr->get_table_id(), simple_table_schema))) {
           LOG_WARN("failed to get simple table schema", KR(ret), K(tenant_id_), K(cache_mgr->get_table_id()));
         } else if (OB_ISNULL(simple_table_schema) || simple_table_schema->is_in_recyclebin()) {
           // remove cache if table not exist or is in recyclebin
           need_delete = true;
         }
         // Check if tablet exists on this ls (only if table schema check passed)
         if (!need_delete && OB_SUCC(ret)) {
           if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(cache_tablet_id, tablet_handle))) {
             if (OB_TABLET_NOT_EXIST != ret) {
               LOG_WARN("fail to get tablet", K(ret), K(cache_tablet_id));
             } else {
               ret = OB_SUCCESS; // not found, moved from this ls
               need_delete = true;
             }
           }
         }
        // Add to deletion list if needed
        if (need_delete) {
          if (OB_FAIL(delete_cache_tablet_id_array.push_back(cache_tablet_id))) {
            LOG_WARN("push back cache tablet id failed",
              K(delete_cache_tablet_id_array.count()), K(cache_tablet_id), KR(ret));
          }
        }
      }
    } // Release lock here

    // Perform actual deletion outside the lock
    if (delete_cache_tablet_id_array.count() > 0) {
      LOG_INFO("try erase ivf cache managers",
        K(index_ls_mgr->get_ls_id()), K(delete_cache_tablet_id_array.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < delete_cache_tablet_id_array.count(); i++) {
      ObTabletID tablet_id = delete_cache_tablet_id_array.at(i);
      // Use the existing erase_ivf_cache_mgr which handles reference counting properly
      if (OB_FAIL(index_ls_mgr->erase_ivf_cache_mgr(tablet_id))) {
        if (ret != OB_HASH_NOT_EXIST) {
          LOG_WARN("failed to erase ivf cache manager",
            K(index_ls_mgr->get_ls_id()), K(tablet_id), KR(ret));
        } else { // already removed
          ret = OB_SUCCESS;
        }
      } else {
        LOG_INFO("ivf cache mgr erased during cleanup",
          K(tablet_id), K(index_ls_mgr->get_ls_id()));
      }
    }
    delete_cache_tablet_id_array.reset();
  }
}

bool ObPluginVectorIndexLoadScheduler::check_can_do_work()
{
  bool bret = true;
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool is_oracle_mode = false;

  if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K_(tenant_id));
  } else if (is_oracle_mode) {
    bret = false;
    LOG_DEBUG("vector index not support oracle mode", K_(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_3_3_0) {
    bret = false;
    LOG_DEBUG("vector index can not work with data version less than 4_3_3", K(tenant_data_version));
  } else if (is_user_tenant(tenant_id_)) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id_), tenant_data_version))) {
      bret = false;
      LOG_WARN("get tenant data version failed", K(ret));
    } else if (tenant_data_version < DATA_VERSION_4_3_3_0) {
      bret = false;
      LOG_DEBUG("vector index can not work with data version less than 4_3_3", K(tenant_data_version));
    }
  }
  return bret;
}

int ObPluginVectorIndexLoadScheduler::check_schema_version()
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

int ObPluginVectorIndexLoadScheduler::check_index_adpter_exist(ObPluginVectorIndexMgr *mgr)
{
  int ret = OB_SUCCESS;
  if (!mgr->get_vec_adaptor_map().empty()) {
    // complete adapter not empty, need check for transfer and updates
    mark_tenant_need_check();
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_need_maintence_ls_follower()
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", KR(ret), K(MTL_ID()));
  } else if ((!ATOMIC_LOAD(&is_leader_) && tenant_config->load_vector_index_on_follower) || ATOMIC_LOAD(&need_refresh_)) {
    mark_tenant_need_check();
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_transfer_scn(
    ObPluginVectorIndexMgr *mgr,
    bool &need_update_transfer_scn,
    share::SCN &current_transfer_scn)
{
  int ret = OB_SUCCESS;
  need_update_transfer_scn = false;
  current_transfer_scn = share::SCN::invalid_scn();
  if (OB_ISNULL(mgr)) {
    // skip
  } else if (OB_FAIL(ls_->get_transfer_scn(current_transfer_scn))) {
    LOG_WARN("failed to get ls transfer scn", KR(ret), K(ls_->get_ls_id()));
  } else if (!mgr->get_last_transfer_scn().is_valid()
             || mgr->get_last_transfer_scn() != current_transfer_scn) {
    mark_tenant_need_check();
    need_update_transfer_scn = current_transfer_scn.is_valid();
    LOG_INFO("finish check transfer scn", KR(ret), K(current_transfer_scn), K(mgr->get_last_transfer_scn()), K(ls_->get_ls_id()));
  }
  return ret;
}

void ObPluginVectorIndexLoadScheduler::mark_tenant_need_check()
{
  int ret = OB_SUCCESS;
  if (common::ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
    local_tenant_task_.need_check_ = true;
    FLOG_INFO("finish mark tenant need check", K(local_tenant_task_));
  }
  LOG_DEBUG("finsh mark tenant need check", KR(ret), K(local_tenant_task_.need_check_));
}

int ObPluginVectorIndexLoadScheduler::check_is_vector_index_table(const ObSimpleTableSchemaV2 &table_schema,
                                                                  bool &is_vector_index_table,
                                                                  bool &is_shared_index_table)
{
  int ret = OB_SUCCESS;
  is_vector_index_table = false;
  is_shared_index_table = false;
  if (table_schema.is_index_table() && !table_schema.is_in_recyclebin()) {
    if (table_schema.is_vec_delta_buffer_type()
        || table_schema.is_vec_index_id_type()
        || table_schema.is_vec_index_snapshot_data_type()
        || table_schema.is_hybrid_vec_index_log_type()
        || table_schema.is_hybrid_vec_index_embedded_type()) {
      is_vector_index_table = true;
    } else if (table_schema.is_vec_rowkey_vid_type()
        || table_schema.is_vec_vid_rowkey_type()) {
      is_shared_index_table = true;
    }
  }
  return ret;
}

void ObPluginVectorIndexLoadScheduler::mark_tenant_checked()
{
  local_tenant_task_.need_check_ = false;
}

// scan all vector tablet in current tenant/LS
int ObPluginVectorIndexLoadScheduler::collect_tablet_info_for_maintenance(
    const int64_t table_id,
    const ObSimpleTableSchemaV2 *table_schema,
    VecAcquireCtxMap &ctx_map,
    VecSharedInfoMap &shared_map,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = table_schema->get_index_type();
  ObArray<ObTabletID> tablet_ids;
  bool is_shared_table = schema::is_vec_rowkey_vid_type(index_type)
                         || schema::is_vec_vid_rowkey_type(index_type);

  if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("fail to get tablet ids", KR(ret), K(table_id));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret));
  }

  // For non-shared tables, compute index identity prefix
  ObString prefix;
  ObString identity_copy;
  if (OB_SUCC(ret) && !is_shared_table) {
    if (OB_FAIL(ObPluginVectorIndexUtils::get_vector_index_prefix(*table_schema, prefix))) {
      LOG_WARN("fail to get vector index prefix", KR(ret), K(table_id));
    } else if (OB_FAIL(ob_write_string(allocator, prefix, identity_copy))) {
      LOG_WARN("fail to deep copy index identity", KR(ret), K(prefix));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    ObTabletHandle tablet_handle;
    if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(tablet_ids.at(i), tablet_handle))) {
      if (OB_TABLET_NOT_EXIST != ret) {
        LOG_WARN("fail to get tablet", K(ret), K(tablet_ids.at(i)));
      } else {
        ret = OB_SUCCESS; // not found, continue loop
      }
    } else {
      ObTabletID data_tablet_id = tablet_handle.get_obj()->get_data_tablet_id();

      if (is_shared_table) {
        // Shared tables (rowkey_vid / vid_rowkey): store into shared_map by data_tablet_id
        ObVectorIndexSharedInfo shared_info;
        const int overwrite = 1;
        int hash_ret = shared_map.get_refactored(data_tablet_id, shared_info);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          // new entry
        } else if (OB_SUCCESS != hash_ret) {
          ret = hash_ret;
          LOG_WARN("fail to get shared info from map", K(ret), K(data_tablet_id));
          break;
        }
        if (schema::is_vec_rowkey_vid_type(index_type)) {
          shared_info.rowkey_vid_tablet_id_ = tablet_ids.at(i);
          shared_info.rowkey_vid_table_id_ = table_id;
        } else if (schema::is_vec_vid_rowkey_type(index_type)) {
          shared_info.vid_rowkey_tablet_id_ = tablet_ids.at(i);
          shared_info.vid_rowkey_table_id_ = table_id;
        }
        if (OB_SUCC(ret) && OB_FAIL(shared_map.set_refactored(data_tablet_id, shared_info, overwrite))) {
          LOG_WARN("fail to set shared info in map", K(ret), K(data_tablet_id));
        }
      } else {
        // Non-shared tables: use (data_tablet_id, index_identity) as key
        ObPluginVectorIndexIdentity key(data_tablet_id, identity_copy);
        ObVectorIndexAcquireCtx ctx;
        int hash_ret = ctx_map.get_refactored(key, ctx);
        if (OB_HASH_NOT_EXIST == hash_ret) {
          ctx.reset();
          ctx.data_tablet_id_ = data_tablet_id;
          ctx.data_table_id_ = table_schema->get_data_table_id();
        } else if (OB_SUCCESS != hash_ret) {
          ret = hash_ret;
          LOG_WARN("fail to get ctx from map", K(ret), K(key));
          break;
        }

        if (schema::is_vec_delta_buffer_type(index_type)
            || schema::is_hybrid_vec_index_log_type(index_type)) {
          ctx.inc_tablet_id_ = tablet_ids.at(i);
          ctx.inc_table_id_ = table_id;
        } else if (schema::is_vec_index_id_type(index_type)) {
          ctx.vbitmap_tablet_id_ = tablet_ids.at(i);
          ctx.vbitmap_table_id_ = table_id;
        } else if (schema::is_vec_index_snapshot_data_type(index_type)) {
          ctx.snapshot_tablet_id_ = tablet_ids.at(i);
          ctx.snapshot_table_id_ = table_id;
        } else if (schema::is_hybrid_vec_index_embedded_type(index_type)) {
          ctx.embedded_tablet_id_ = tablet_ids.at(i);
          ctx.embedded_table_id_ = table_id;
        }

        const int overwrite = 1;
        if (OB_SUCC(ret) && OB_FAIL(ctx_map.set_refactored(key, ctx, overwrite))) {
          LOG_WARN("fail to set ctx in map", K(ret), K(key));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::create_or_update_adaptors(VecAcquireCtxMap &ctx_map)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id = ls_->get_ls_id();

  FOREACH_X(iter, ctx_map, OB_SUCC(ret)) {
    ObVectorIndexAcquireCtx &ctx = iter->second;
    if (!ctx.is_inc_valid()) {
      // no inc tablet info (only shared tables without corresponding inc table), skip
      continue;
    }

    ObPluginVectorIndexAdapterGuard adapter_guard;
    ObString vec_idx_params;
    int64_t dim = 0;

    // get vec_idx_params and dim from inc table schema
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *inc_schema = nullptr;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
            tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(
            tenant_id_, ctx.inc_table_id_, inc_schema))) {
      LOG_WARN("fail to get inc table schema", K(ret), K(ctx.inc_table_id_));
    } else if (OB_ISNULL(inc_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("inc table schema is null", K(ret), K(ctx.inc_table_id_));
    } else {
      vec_idx_params = inc_schema->get_index_params();
      if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(*inc_schema, dim))) {
        LOG_WARN("fail to get vec_index_col_param", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(vector_index_service_->acquire_adapter_guard(
            ls_id, ctx, adapter_guard, &vec_idx_params, dim))) {
      LOG_WARN("fail to acquire adapter guard by ctx", K(ret), K(ls_id), K(ctx));
    } else if (OB_NOT_NULL(adapter_guard.get_adatper())) {
      adapter_guard.get_adatper()->reset_idle();
    }
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_has_vector_index(ObIArray<uint64_t> &vec_table_id_array)
{
  int ret = OB_SUCCESS;
  bool has_ivf_index = false;
  if (OB_FAIL(ObPluginVectorIndexUtils::get_tenant_vector_index_ids(tenant_id_, has_ivf_index, vec_table_id_array))) {
    LOG_WARN("fail to get tenant table ids", KR(ret), K_(tenant_id));
  }
  return ret;
}

// scan all vector tablet in current tenant/LS
int ObPluginVectorIndexLoadScheduler::execute_adapter_maintenance(ObIArray<uint64_t> &vec_table_id_array)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObPluginVectorIndexLoadScheduler::check_and_generate_tablet_tasks",
                    VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD);

  VecAcquireCtxMap ctx_map;
  VecSharedInfoMap shared_map;
  ObArenaAllocator identity_allocator(ObMemAttr(tenant_id_, "VecIdxIdentity"));
  ObMemAttr memattr(tenant_id_, "VecIdxInfo");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet vector index scheduler not init", KR(ret));
  } else {
    clean_deprecated_adapters();
  }

  if (current_memory_config_ != 0) { // has memory for new adapter

    if (!vec_table_id_array.empty()) {
      if (OB_FAIL(ctx_map.create(DEFAULT_TABLE_ARRAY_SIZE, memattr, memattr))) {
        LOG_WARN("fail to create ctx map", KR(ret));
      } else if (OB_FAIL(shared_map.create(DEFAULT_TABLE_ARRAY_SIZE, memattr, memattr))) {
        LOG_WARN("fail to create shared map", KR(ret));
      }
    }

    bool need_maintenance = true;
    if (OB_SUCC(ret)) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      if (!tenant_config.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail get tenant_config", KR(ret), K(MTL_ID()));
      } else if (!ATOMIC_LOAD(&is_leader_) && !tenant_config->load_vector_index_on_follower) {
        need_maintenance = false;
        LOG_INFO("dont not need maintenance ls follower", K(ret), K(tenant_config->load_vector_index_on_follower));
      }
    }

    int64_t start_idx = 0;
    int64_t end_idx = 0;

    // Phase 1: Collect all auxiliary table info into ctx_map (non-shared) and shared_map (shared)
    while (OB_SUCC(ret) && need_maintenance && start_idx < vec_table_id_array.count()) {
      ObSchemaGetterGuard schema_guard;
      start_idx = end_idx;
      end_idx = MIN(vec_table_id_array.count(), start_idx + TBALE_GENERATE_BATCH_SIZE);

      bool is_vector_index = false;
      bool is_shared_index = false;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
      }

      for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx; ++idx) {
        const int64_t table_id = vec_table_id_array.at(idx);
        const ObSimpleTableSchemaV2 *table_schema = nullptr;
        if (is_sys_table(table_id)) {
          // do nothing
        } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id, table_schema))) {
          LOG_WARN("failed to get simple schema", KR(ret), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table schema is null", KR(ret), K(table_id), K_(tenant_id));
        } else if (table_schema->is_in_recyclebin()) {
          // do nothing
        } else if (table_schema->is_unavailable_index()) {
          LOG_INFO("skip unavailable index during maintenance",
                   K(table_id), K(table_schema->get_table_name()));
        } else if (OB_FAIL(check_is_vector_index_table(*table_schema, is_vector_index, is_shared_index))) {
          LOG_WARN("fail to check is vector index", KR(ret));
        } else if ((is_vector_index || is_shared_index)
                   && OB_FAIL(collect_tablet_info_for_maintenance(table_id, table_schema, ctx_map, shared_map, identity_allocator))) {
          LOG_WARN("fail to collect tablet info", KR(ret), K(table_id));
        }
      }
    }

    // Phase 1.5: Fill shared table info (rowkey_vid / vid_rowkey) into each ctx from shared_map
    if (OB_SUCC(ret) && need_maintenance) {
      FOREACH_X(iter, ctx_map, OB_SUCC(ret)) {
        ObVectorIndexAcquireCtx &ctx = iter->second;
        ObTabletID data_tablet_id = ctx.data_tablet_id_;
        ObVectorIndexSharedInfo shared_info;
        int hash_ret = shared_map.get_refactored(data_tablet_id, shared_info);
        if (OB_SUCCESS == hash_ret) {
          ctx.rowkey_vid_tablet_id_ = shared_info.rowkey_vid_tablet_id_;
          ctx.rowkey_vid_table_id_ = shared_info.rowkey_vid_table_id_;
          ctx.vid_rowkey_tablet_id_ = shared_info.vid_rowkey_tablet_id_;
          ctx.vid_rowkey_table_id_ = shared_info.vid_rowkey_table_id_;
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          // no shared table info for this data tablet, that's ok
        } else {
          ret = hash_ret;
          LOG_WARN("fail to get shared info", K(ret), K(data_tablet_id));
        }
      }
    }
    bool is_in_migration = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_ls_in_vector_migration(is_in_migration))) {
      LOG_WARN("fail to check ls migration status", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
    } else if (is_in_migration) {
      LOG_INFO("ls is in migration, skip create adaptors", K(tenant_id_), K(ls_->get_ls_id()));
      need_maintenance = false;
    }

    if (OB_FAIL(ret)) {
    } else if (need_maintenance && OB_FAIL(create_or_update_adaptors(ctx_map))) {
      LOG_WARN("fail to create or update adaptors", KR(ret));
    } else if (need_maintenance) {
      mark_tenant_checked();
    }
  }

  LOG_INFO("finish generate tenant tablet tasks", KR(ret), K_(tenant_id), K(ls_->get_ls_id()));
  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_tenant_memory()
{
  // ToDo:
  // 1. check vector index memory usage
  // 2. check adaptor number limit if needed
  // 3. set condition: if out of use, only do clean task
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id_, current_memory_config_))) {
    LOG_WARN("failed to get vector mem limit size.", K(ret), K_(tenant_id));
    ret = OB_SUCCESS;
    current_memory_config_ = 0;
  } else {
    LOG_INFO("get vector mem limit size", KR(ret), K_(tenant_id), K_(current_memory_config));
  }
  return ret;
}

int read_tenant_task_status(uint64_t tenant_id,
                            common::ObISQLClient *sql_client,
                            ObVectorIndexTenantStatus& tenant_task)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_task.tenant_id_ = tenant_id;
    tenant_task.status_ = OB_RS_TTL_TASK_CREATE;
  }
  return ret;
}

// 1. check if loading feature is allowed:
//    read from sys table with tenant id, special table id & special tablet id, not implemented
// 2. check if need mem load task
//    from log replay, or long time not processed
int ObPluginVectorIndexLoadScheduler::reload_tenant_task()
{
  int ret = OB_SUCCESS;
  ObVectorIndexTenantStatus tenant_task;
  ObVectorIndexTaskStatus expected_state;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPluginVectorIndexLoadScheduler not init", KR(ret));
  } else if (OB_FAIL(get_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get ls mgr", K(ret));
  } else if (OB_FAIL(read_tenant_task_status(tenant_id_, NULL, tenant_task))) {
    LOG_WARN("fail to read vector index tenant task", KR(ret), K_(tenant_id));
  } else if (OB_RS_TTL_TASK_MOVE == static_cast<ObTTLTaskStatus>(tenant_task.status_) ||
             OB_RS_TTL_TASK_CANCEL == static_cast<ObTTLTaskStatus>(tenant_task.status_)) {
    FLOG_INFO("tenant task is finish now, reuse local tenant task",
      KR(ret), K_(local_tenant_task), K(tenant_task.task_id_));
  } else if (OB_FAIL(ObTTLUtil::transform_tenant_state(static_cast<ObTTLTaskStatus>(tenant_task.status_), expected_state))) {
    LOG_WARN("fail to transform vector index tenant task status", KR(ret), K(tenant_task.status_));
  } else if (expected_state != OB_TTL_TASK_RUNNING) { // currently, only running state expected
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index tenant task status",
      KR(ret), K(tenant_task.status_), K(expected_state), K(local_tenant_task_));
  } else {
    if (OB_ISNULL(index_ls_mgr)) {
    } else if (index_ls_mgr->get_ls_task_ctx().state_ != expected_state) {
      if (expected_state != OB_TTL_TASK_RUNNING) {
        FLOG_INFO("vector index schedular is not running now", KR(ret), K(index_ls_mgr->get_ls_task_ctx()));
      }
      // currently, only finish/running vs running
      // if change from running to finish/cancel release context
      index_ls_mgr->get_ls_task_ctx().reuse();
      index_ls_mgr->get_ls_task_ctx().task_id_++; // not used, ++ if overall task status changed
      index_ls_mgr->get_ls_task_ctx().need_check_ = true;
      // all finish
      index_ls_mgr->get_ls_task_ctx().state_ = expected_state;
    }
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::execute_one_memdata_sync_task(ObPluginVectorIndexMgr *mgr,
                                                                    ObPluginVectorIndexTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  bool try_schedule = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("memdata load scheduler not init", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(mgr) || OB_ISNULL(task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memdata load vector index adapter or ctx is null", KR(ret), KPC(mgr), KPC(task_ctx));
  } else {
    common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
    if (task_ctx->task_status_ != mgr->get_ls_task_ctx().state_) {
      // only pending task could be changed to running, reuse ttl task status
      if (OB_TTL_TASK_RUNNING == mgr->get_ls_task_ctx().state_) {
        if (OB_TTL_TASK_PREPARE == task_ctx->task_status_) {
          try_schedule = true;
        } else if (OB_TTL_TASK_FINISH == task_ctx->task_status_
                   || OB_TTL_TASK_CANCEL == task_ctx->task_status_) {
          // do nothing
          LOG_INFO("memdata load task finish or cancelled", K(mgr->get_ls_task_ctx()), KPC(task_ctx));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memdata load no expected task status", KR(ret), K(mgr->get_ls_task_ctx()), KPC(task_ctx));
        }
      } else { // ls not running
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("memdata load unexpect ls task status", KR(ret), KPC(mgr), KPC(task_ctx));
      }
    } else { // if is running do nothing, if not need schedular.
      LOG_INFO("nmemdata load o need to schedule task", K(mgr->get_ls_task_ctx()), KPC(task_ctx));
    }

    if (OB_SUCC(ret)
        && try_schedule
        && OB_FAIL(try_schedule_task(mgr, task_ctx))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("fail to try schedule dag task", KR(ret));
      } else {
        ret = OB_SUCCESS; // size overflow schedule later
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexLoadScheduler::try_schedule_task(ObPluginVectorIndexMgr *mgr,
                                                        ObPluginVectorIndexTaskCtx *task_ctx)
{
  // check and gen dag
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(task_ctx) || OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index adapter or memdata load ctx is null", KPC(mgr), KR(ret));
  } else if (can_schedule_tenant(mgr) && can_schedule_task(task_ctx)) {
    if (OB_FAIL(generate_vec_idx_memdata_dag(mgr, task_ctx))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else if (OB_SIZE_OVERFLOW == ret) { // do nothing, handled by caller
      } else {
        LOG_WARN("fail to generate vector index memdata load dag task", KR(ret));
      }
    } else {
      inc_dag_ref();
      task_ctx->in_queue_ = true;
      // dag maybe already finished, and set status to finish/cancel,
      // but here change it to running and could not be scheduler later
      task_ctx->task_status_ = OB_TTL_TASK_RUNNING;
      LOG_DEBUG("memdata sync task scheduled", K(task_ctx->index_tablet_id_),
               K(task_ctx->index_table_id_), K(task_ctx->failure_times_));
    }
  } else {
    LOG_DEBUG("status when try schedule task", KPC(mgr), K(task_ctx));
  }

  return ret;
}

int ObPluginVectorIndexLoadScheduler::try_schedule_remaining_tasks(ObPluginVectorIndexMgr *mgr,
                                                                   ObPluginVectorIndexTaskCtx *current_ctx)
{
  int ret = OB_SUCCESS;
  // // called in dag thread, maintaince thread may reuse this map if it is just finished.
  // // but reuse happen in next maintaince cycle (10s), so it is safe here.
  // VectorIndexMemSyncMap &current_task_map = mgr->get_mem_sync_info().get_processing_map();
  // FOREACH_X(iter, current_task_map, OB_SUCC(ret)) {
  //   ObPluginVectorIndexTaskCtx *task_ctx = iter->second;
  //   if (OB_ISNULL(task_ctx)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("get invalid task ctx", KR(ret), KPC(task_ctx));
  //   } else if (task_ctx == current_ctx) {
  //     // bypass
  //   } else {
  //     common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
  //     if (can_schedule_task(task_ctx) && task_ctx->task_status_ == OB_TTL_TASK_PREPARE) {
  //       LOG_INFO("try schedule remaining task", KPC(task_ctx), KPC(current_ctx));
  //       if (OB_FAIL(try_schedule_task(mgr, task_ctx))) {
  //         if (OB_SIZE_OVERFLOW != ret) {
  //           LOG_WARN("fail to try schedule dag task", KR(ret));
  //         }
  //       }
  //     }
  //   }
  // }

  // if (OB_SIZE_OVERFLOW == ret) { // task queue full, schedule later
  //   ret = OB_SUCCESS;
  // }

  return ret;
}

// reserved control funtions, remove if not used finally
bool ObPluginVectorIndexLoadScheduler::can_schedule_tenant(const ObPluginVectorIndexMgr *mgr)
{
  bool bret = true;
  if (OB_ISNULL(mgr) || is_stopped()) {
    bret = false;
  }
  return bret;
}

// reserved control funtions, remove if not used finally
bool ObPluginVectorIndexLoadScheduler::can_schedule_task(const ObPluginVectorIndexTaskCtx *task_ctx)
{
  bool bret = true;
  if (OB_ISNULL(task_ctx)) {
    bret = false;
  }
  return bret;
}

int ObPluginVectorIndexLoadScheduler::generate_vec_idx_memdata_dag(ObPluginVectorIndexMgr *mgr,
                                                                   ObPluginVectorIndexTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  ObVectorIndexDag *dag = nullptr;
  ObVectorIndexTask *memdata_sync_task = nullptr;

  ObTenantDagScheduler *dag_scheduler = nullptr;
  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be null", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(dag))) {
    LOG_WARN("fail to alloc vector index memdata sync dag", KR(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, vector index memdata sync dag is null", KR(ret), KP(dag));
  } else if (OB_FAIL(dag->init(mgr, task_ctx))) {
    LOG_WARN("fail to init vector index memdata sync dag", KR(ret));
  } else if (OB_FAIL(dag->alloc_task(memdata_sync_task))) {
    LOG_WARN("fail to alloc vector index memdata sync task", KR(ret));
  } else if (OB_ISNULL(memdata_sync_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, vector index memdata sync task is null", KR(ret), KP(memdata_sync_task));
  } else if (OB_FAIL(memdata_sync_task->init(this, mgr, task_ctx))) {
    LOG_WARN("fail to init vector index memdata sync task", KR(ret));
  } else if (OB_FAIL(dag->add_task(*memdata_sync_task))) {
    LOG_WARN("fail to add vector index memdata sync task", KR(ret));
  } else if (OB_FAIL(dag_scheduler->add_dag(dag))) {
    // handle special ret code by caller
    if (OB_EAGAIN == ret) {
      LOG_INFO("vector index memdata sync dag already exists, no need to re-schedule", KR(ret));
    } else if (OB_SIZE_OVERFLOW == ret) {
      LOG_INFO("dag scheduler is full", KR(ret));
    } else {
      LOG_WARN("fail to add vector index memdata sync dag to queue", KR(ret));
    }
  } else if (task_ctx->task_start_time_ == 0) {
    task_ctx->task_start_time_ = ObTimeUtility::current_time();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::get_truncate_version(mgr->get_tenant_id(), task_ctx->index_table_id_,
                  task_ctx->truncate_version_))) {
      LOG_WARN("fail to get table truncate version", K(ret), K(tmp_ret), K(task_ctx->index_table_id_));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(dag)) {
    dag_scheduler->free_dag(*dag);
  }
  return ret;
}

// call try_schedule_remaining_tasks inside
// if all task finish, reset process map
int ObPluginVectorIndexLoadScheduler::check_task_state(ObPluginVectorIndexMgr *mgr,
                                                       ObPluginVectorIndexTaskCtx *task_ctx,
                                                       bool &is_stop)
{
  int ret = OB_SUCCESS;
  // stop current task
  is_stop = true;
  // do memsync task even if schema changed
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load scheduler not init", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(mgr) || OB_ISNULL(task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index adapter or memdata load ctx is null", KR(ret), KPC(mgr), KPC(task_ctx));
  } else {
    common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
    const int64_t e2e_cost_us = (task_ctx->task_start_time_ > 0)
        ? (ObTimeUtility::current_time() - task_ctx->task_start_time_)
        : -1;
    // change log level to debug later
    if (task_ctx->task_status_ == OB_TTL_TASK_CANCEL
        || task_ctx->task_status_ == OB_TTL_TASK_FINISH) {
      // do nothing, schedule next
      LOG_INFO("cancel current memdata sync task", KR(ret), KPC(task_ctx));
    } else if (task_ctx->task_status_ == OB_TTL_TASK_RUNNING) {
      // will schedule this
      if (task_ctx->err_code_ == OB_SUCCESS) {
        task_ctx->task_status_ = OB_TTL_TASK_FINISH;
        LOG_INFO("current memdata sync task finish",
          K(tenant_id_), K(ls_->get_ls_id()), K(task_ctx->index_table_id_), K(e2e_cost_us));
        // task success, schedule next
      } else if (in_retry_list(task_ctx->err_code_)) {
        task_ctx->failure_times_++;
        LOG_INFO("current memdata sync task failed, will retry",
          K(tenant_id_), K(ls_->get_ls_id()), K(task_ctx->index_table_id_),
          K(e2e_cost_us), K(task_ctx->err_code_), K(task_ctx->failure_times_));
        task_ctx->task_status_ = OB_TTL_TASK_PREPARE; // reset to prepare state, will rescheduled by timer or dag task
      } else if (OB_PARTITION_NOT_EXIST == task_ctx->err_code_
                 || OB_PARTITION_IS_BLOCKED == task_ctx->err_code_
                 || OB_TABLE_NOT_EXIST == task_ctx->err_code_
                 || OB_ERR_UNKNOWN_TABLE == task_ctx->err_code_
                 || OB_LS_NOT_EXIST == task_ctx->err_code_
                 || OB_TABLET_NOT_EXIST == task_ctx->err_code_) {
        task_ctx->task_status_ = OB_TTL_TASK_CANCEL;
        LOG_INFO("current memdata sync task canceled due to partition state change",
          K(tenant_id_), K(ls_->get_ls_id()), K(task_ctx->index_table_id_),
          K(e2e_cost_us), K(task_ctx->err_code_));
        // canceled, schedule next
      } else if (OB_ALLOCATE_MEMORY_FAILED == task_ctx->err_code_
                 || OB_ERR_VSAG_MEM_LIMIT_EXCEEDED == task_ctx->err_code_) {
        task_ctx->task_status_ = OB_TTL_TASK_CANCEL;
        LOG_WARN("current memdata sync task canceled due to resource limit",
          K(tenant_id_), K(ls_->get_ls_id()), K(task_ctx->index_table_id_),
          K(e2e_cost_us), K(task_ctx->err_code_));
      } else { // retry
        task_ctx->failure_times_++;
        LOG_WARN("current memdata sync task failed, will retry",
          K(tenant_id_), K(ls_->get_ls_id()), K(task_ctx->index_table_id_),
          K(e2e_cost_us), K(task_ctx->err_code_), K(task_ctx->failure_times_));
        if (task_ctx->failure_times_ >= 3) {
          task_ctx->task_status_ = OB_TTL_TASK_CANCEL;
          LOG_WARN("current memdata sync task failed too many times, cancel it",
            K(tenant_id_), K(ls_->get_ls_id()), K(task_ctx->index_table_id_),
            K(e2e_cost_us), K(task_ctx->err_code_), K(task_ctx->failure_times_));
        } else {
          task_ctx->task_status_ = OB_TTL_TASK_PREPARE; // reset to prepare state, will rescheduled by timer or dag task
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task status", KR(ret), KPC(task_ctx));
      task_ctx->task_status_ = OB_TTL_TASK_CANCEL;
    }
  }

  // current task stopped, schedule remaining tasks
  if (is_stop && OB_SUCC(ret)) {
    LOG_INFO("stop current memdata sync task", KR(ret), KPC(task_ctx));
    if (OB_FAIL(try_schedule_remaining_tasks(mgr, task_ctx))) {
      LOG_WARN("fail to schedule remaining tasks", KR(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_ls_task_state(ObPluginVectorIndexMgr *mgr)
{
  int ret = OB_SUCCESS;
  // mgr cannot be null here.
  bool processing_finished = false;
  uint32_t total_count = 0;
  uint32_t finished_count = 0;
  if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null mgr", K(tenant_id_), K(ls_->get_ls_id()));
  } else if (OB_FAIL(mgr->get_mem_sync_info().count_processing_finished(processing_finished,
                                                                        total_count,
                                                                        finished_count))) {
    LOG_WARN("failed to count finished tasks", KR(ret));
  } else if (processing_finished) {
    ObPluginVectorIndexLSTaskCtx &ls_task_ctx = mgr->get_ls_task_ctx();
    ls_task_ctx.all_finished_ = true;
    ls_task_ctx.need_memdata_sync_ = false;
    LOG_INFO("memdata sync all task finished",
      K(tenant_id_), K(ls_->get_ls_id()), K(finished_count), K(total_count),
      K(processing_finished), K(ls_task_ctx.all_finished_),
      K(mgr->get_mem_sync_info().get_processing_size()),
      K(mgr->get_mem_sync_info().get_waiting_size()));
  } else {
      LOG_DEBUG("memdata sync task remaining",
      K(tenant_id_), K(ls_->get_ls_id()), K(finished_count), K(total_count),
      K(processing_finished), K(mgr->get_ls_task_ctx().all_finished_),
      K(mgr->get_mem_sync_info().get_processing_size()),
      K(mgr->get_mem_sync_info().get_waiting_size()));
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_and_execute_adapter_maintenance_task(ObPluginVectorIndexMgr *&mgr, ObIArray<uint64_t> &vec_table_id_array)
{
  int ret = OB_SUCCESS;
  bool need_update_transfer_scn = false;
  share::SCN current_transfer_scn = share::SCN::invalid_scn();
  if (OB_FAIL(check_schema_version())) {
    LOG_WARN("fail to check schema version", KR(ret));
  } else if (OB_NOT_NULL(mgr) && OB_FAIL(check_index_adpter_exist(mgr))) {
    LOG_WARN("fail to check index adpter exist", KR(ret));
  } else if (OB_FAIL(check_need_maintence_ls_follower())) {
    LOG_WARN("fail to check need maintence ls follower", KR(ret));
  } else if (OB_NOT_NULL(mgr) && OB_FAIL(check_transfer_scn(mgr, need_update_transfer_scn, current_transfer_scn))) {
    LOG_WARN("fail to check transfer scn", KR(ret));
  } else if (local_tenant_task_.need_check_) {
    if (OB_FAIL(execute_adapter_maintenance(vec_table_id_array))) {
      LOG_WARN("fail to generate tablet tasks", K_(tenant_id));
    } else if (need_update_transfer_scn) {
      ATOMIC_STORE(&need_refresh_, true);
      mgr->set_last_transfer_scn(current_transfer_scn);
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(mgr)) {
      tmp_ret = vector_index_service_->get_ls_index_mgr_map().get_refactored(ls_->get_ls_id(), mgr);
      if (tmp_ret == OB_SUCCESS) {
      } else if (tmp_ret == OB_HASH_NOT_EXIST) {
        tmp_ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get vector index ls mgr", KR(tmp_ret), K(tenant_id_), K(ls_->get_ls_id()));
      }
    }
    if (OB_NOT_NULL(mgr)) {
      mgr->dump_all_inst(); // for debug, remove later
    }
  }

  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_and_execute_ivf_cache_maintenance_task(ObPluginVectorIndexMgr *&mgr)
{
  int ret = OB_SUCCESS;

  // Schema version check
  if (OB_FAIL(check_schema_version())) {
    LOG_WARN("fail to check schema version for ivf cache", KR(ret));
  } else if (OB_ISNULL(mgr)) {
    // mgr is null, no cache to clean
  } else if (mgr->get_ivf_cache_mgr_map().empty()) {
    // no cache to clean
  } else {
    // Execute IVF cache cleanup
    clean_deprecated_ivf_caches();
    LOG_INFO("finish ivf cache maintenance task", K_(tenant_id), K(ls_->get_ls_id()));
  }

  return ret;
}

int ObPluginVectorIndexLoadScheduler::log_tablets_need_memdata_sync(ObPluginVectorIndexMgr *mgr)
{
  // Notice: only sync complete adapter, partial adapter will be merged to complete next timer schedule
  int ret = OB_SUCCESS;
  bool need_submit_log = false;
  bool is_in_migration = false;
  const bool need_refresh = ATOMIC_LOAD(&need_refresh_);
  const bool is_leader = ATOMIC_LOAD(&is_leader_);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", KR(ret), K(tenant_id_));
  } else if (need_refresh && !tenant_config->load_vector_index_on_follower && !is_leader) {
    ATOMIC_STORE(&need_refresh_, false);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_ls_in_vector_migration(is_in_migration))) {
    LOG_WARN("fail to check ls migration status", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (is_in_migration) {
    LOG_INFO("ls is in migration, skip memdata sync task", K(tenant_id_), K(ls_->get_ls_id()));
    need_refresh_ = false;
  }
  if (OB_SUCC(ret) && !is_in_migration) {
    // lock to avoid concurrent modify tablet_id_array and tablet_id_array;
    common::ObSpinLockGuard ctx_guard(logging_lock_);
    if (is_logging_) {
      // do-nothing
      FLOG_INFO("vector index memdata sync is logging");
    } else {
      table_id_array_.reuse();
      tablet_id_array_.reuse();

      // follower just refresh adapter statistics, leader submit log need memdata sync
      RWLock::RLockGuard lock_guard(mgr->get_adapter_map_lock());
      FOREACH_X(iter, mgr->get_vec_adaptor_map(), OB_SUCC(ret)) {
        ObPluginVectorIndexAdaptor *adapter = iter->second;
        bool need_sync = false;
        bool can_read_index = false;
        if (OB_ISNULL(adapter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null adapter", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
        } else if (iter->first != adapter->get_inc_tablet_id()) {
          // do nothing
        } else if (!adapter->is_ready_complete()) {
          // adaptor not fully initialized, skip
          LOG_INFO("adaptor not ready complete, skip", K(tenant_id_), K(ls_->get_ls_id()), KPC(adapter));
        } else if (OB_INVALID_ID == adapter->get_inc_table_id()) {
          // do nothing
        } else if (tablet_id_array_.count() >= ObVectorIndexSyncLogCb::VECTOR_INDEX_MAX_SYNC_COUNT) {
          // do nothing, wait for next schedule
        } else if (!need_refresh && OB_FAIL(adapter->check_need_sync_to_follower_or_do_opt_task(mgr, is_leader, need_sync))) {
          LOG_WARN("fail to check need memdata sync", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
        } else if (need_refresh && OB_FAIL(adapter->check_snapshot_table_can_read_index(can_read_index))) {
          LOG_WARN("fail to check snapshot table can read index", KR(ret),
              K(tenant_id_), K(ls_->get_ls_id()), KPC(adapter));
          ret = OB_SUCCESS;
        } else if (need_refresh && !can_read_index) {
          LOG_INFO("snapshot table not ready, skip refresh memdata sync", K(tenant_id_),
              K(ls_->get_ls_id()), KPC(adapter));
        } else if ((need_refresh || need_sync) && is_leader) {
          if (OB_FAIL(tablet_id_array_.push_back(iter->first))) {
            LOG_WARN("fail to push tablet id need memdata sync", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
          } else if (OB_FAIL(table_id_array_.push_back(adapter->get_inc_table_id()))) {
            LOG_WARN("fail to push table id need memdata sync", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
          } else {
            need_submit_log = true;
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (need_submit_log) {
    if (need_refresh) {
      if (OB_FAIL(mgr->get_mem_sync_info().add_task_to_waiting_map(mgr->get_vec_adaptor_map()))) {
        TRANS_LOG(WARN, "fail to add complete adaptor to waiting map",KR(ret), K(tenant_id_));
      }
    } else if (tenant_config->load_vector_index_on_follower) {
      if (OB_FAIL(submit_log_())) {
        TRANS_LOG(WARN, "fail to submit vector index memdata sync log",KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
      } else {
        TRANS_LOG(INFO, "submit vector index memdata sync log success", KR(ret), K(need_refresh), K(tenant_id_), K(ls_->get_ls_id()));
      }
    }
  } else if (!is_leader && need_refresh) {
    if (OB_FAIL(mgr->get_mem_sync_info().add_task_to_waiting_map(mgr->get_vec_adaptor_map()))) {
      TRANS_LOG(WARN, "fail to add complete adaptor to waiting map",KR(ret), K(tenant_id_));
    }
  }
  ATOMIC_STORE(&need_refresh_, false);

  return ret;
}

int ObPluginVectorIndexLoadScheduler::submit_memdata_sync_log_for_tablets(
    const ObIArray<common::ObTabletID> &tablet_ids,
    const ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  bool need_submit_log = false;
  ObPluginVectorIndexMgr *mgr = nullptr;
  ObSchemaGetterGuard schema_guard;
  ObVectorIndexTabletIDArray submit_tablet_ids;
  ObVectorIndexTableIDArray submit_table_ids;
  if (tablet_ids.count() != table_ids.count() || tablet_ids.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet_ids or table_ids", KR(ret), K(tablet_ids.count()), K(table_ids.count()));
  } else if (OB_FAIL(vector_index_service_->acquire_vector_index_mgr(ls_->get_ls_id(), mgr))) {
    LOG_WARN("fail to acquire vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      const uint64_t table_id = table_ids.at(i);
      if (tablet_id.is_valid()) {
        if (OB_FAIL(submit_tablet_ids.push_back(tablet_id))) {
          LOG_WARN("fail to push back tablet_id", KR(ret), K(i), K(tablet_id), K(table_id));
        } else if (OB_FAIL(submit_table_ids.push_back(table_id))) {
          LOG_WARN("fail to push back table_id", KR(ret), K(i), K(tablet_id), K(table_id));
        }
      } else {
        const ObTableSchema *table_schema = nullptr;
        ObArray<ObTabletID> expanded_tablet_ids;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
          LOG_WARN("failed to get table schema", KR(ret), K(table_id), K_(tenant_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table schema is null", KR(ret), K(table_id), K_(tenant_id));
        } else if (OB_FAIL(table_schema->get_tablet_ids(expanded_tablet_ids))) {
          LOG_WARN("fail to get tablet ids", KR(ret), K(table_id));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < expanded_tablet_ids.count(); ++j) {
            const ObTabletID &expanded_tablet_id = expanded_tablet_ids.at(j);
            if (OB_FAIL(submit_tablet_ids.push_back(expanded_tablet_id))) {
              LOG_WARN("fail to push back expanded tablet_id", KR(ret), K(j), K(expanded_tablet_id), K(table_id));
            } else if (OB_FAIL(submit_table_ids.push_back(table_id))) {
              LOG_WARN("fail to push back expanded table_id", KR(ret), K(j), K(expanded_tablet_id), K(table_id));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && submit_tablet_ids.count() <= 0) {
      ret = OB_ITEM_NOT_SETTED;
      LOG_WARN("no tablet need submit memdata sync log", KR(ret), K(tablet_ids), K(table_ids));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t wait_interval_us = 1000L;
    const int64_t wait_timeout_us = 10L * 1000L * 1000L;
    int64_t current_idx = 0;
    int64_t wait_start_time_us = 0;
    while (OB_SUCC(ret) && current_idx < submit_tablet_ids.count()) {
      int64_t submit_cnt = 0;
      {
        common::ObSpinLockGuard ctx_guard(logging_lock_);
        if (is_logging_) {
          if (0 == wait_start_time_us) {
            wait_start_time_us = ObTimeUtility::current_time();
          } else if (ObTimeUtility::current_time() - wait_start_time_us >= wait_timeout_us) {
            ret = OB_TIMEOUT;
            LOG_WARN("wait memdata sync log callback timeout", KR(ret), K(current_idx),
                    K(submit_tablet_ids.count()), K(wait_timeout_us), K(tenant_id_), K(ls_->get_ls_id()));
          }
        } else {
          wait_start_time_us = 0;
          submit_cnt = MIN(static_cast<int64_t>(ObVectorIndexSyncLogCb::VECTOR_INDEX_MAX_SYNC_COUNT),
                          submit_tablet_ids.count() - current_idx);
          tablet_id_array_.reuse();
          table_id_array_.reuse();
          for (int64_t i = 0; OB_SUCC(ret) && i < submit_cnt; ++i) {
            if (OB_FAIL(tablet_id_array_.push_back(submit_tablet_ids.at(current_idx + i)))) {
              LOG_WARN("fail to push back submit tablet_id", KR(ret), K(i), K(current_idx));
            } else if (OB_FAIL(table_id_array_.push_back(submit_table_ids.at(current_idx + i)))) {
              LOG_WARN("fail to push back submit table_id", KR(ret), K(i), K(current_idx));
            }
          }
          if (OB_SUCC(ret)) {
            need_submit_log = true;
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (need_submit_log) {
        if (OB_FAIL(submit_log_())) {
          LOG_WARN("fail to submit memdata sync log for manual trigger", KR(ret),
                  K(tenant_id_), K(ls_->get_ls_id()), K(current_idx), K(submit_cnt));
        } else {
          current_idx += submit_cnt;
          need_submit_log = false;
        }
      } else {
        ob_usleep(wait_interval_us);
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::execute_all_memdata_sync_task(ObPluginVectorIndexMgr *mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else {
    // other threads will not process current map, it is save to just use iter
    VectorIndexMemSyncMap &current_map = mgr->get_mem_sync_info().get_processing_map();
    FOREACH(iter, current_map) {
      // sync_task could countinue even if ls role change
      // however forced sync is not need by leader ls
      // if (OB_FAIL(execute_one_memdata_sync_task(mgr, iter->second))) {
      //   LOG_WARN("fail to execute_one_memdata_sync_task", KR(ret), K(iter->first));
      // }
    }
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_and_execute_memdata_sync_task(ObPluginVectorIndexMgr *mgr)
{
  int ret = OB_SUCCESS;
  bool need_mem_data_sync = false;
  bool force_mem_data_sync = false;
  if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (OB_FAIL(mgr->check_need_mem_data_sync_task(need_mem_data_sync, 0 == get_dag_ref()))) {
    LOG_WARN("fail to check need mem data sync task",
      KR(ret), K(mgr->get_ls_task_ctx()), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (need_mem_data_sync) {
    mgr->get_ls_task_ctx().non_memdata_task_cycle_ = 0;
    mgr->get_ls_task_ctx().need_memdata_sync_ = true;
  } else {
    mgr->get_ls_task_ctx().non_memdata_task_cycle_++;
    if (mgr->get_ls_task_ctx().non_memdata_task_cycle_
        > ObPluginVectorIndexLSTaskCtx::NON_MEMDATA_TASK_CYCLE_MAX) {
      mgr->get_ls_task_ctx().non_memdata_task_cycle_ = 0;
      // disable force sync currently
      // mgr->get_ls_task_ctx().need_memdata_sync_ = true;
      // force_mem_data_sync = true;
      FLOG_INFO("not receive any sync task log", K(tenant_id_), K(ls_->get_ls_id()));
    }
  }


  if (OB_SUCC(ret)
      && force_mem_data_sync
      && (current_memory_config_ != 0)
      && !ATOMIC_LOAD(&is_leader_)) {
    // push all local tablet to sync candidate
    if (OB_FAIL(mgr->get_mem_sync_info().add_task_to_waiting_map(mgr->get_vec_adaptor_map()))) {
      LOG_WARN("fail to add task to waiting map", KR(ret));
    }
  }

  if (OB_SUCC(ret) && mgr->get_ls_task_ctx().need_memdata_sync_) {
    if (OB_FAIL(execute_all_memdata_sync_task(mgr))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("fail to try schedule memedata_sync dag task", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    check_ls_task_state(mgr);
  } else {
    // do nothing
  }

  return ret;
}

int ObPluginVectorIndexLoadScheduler::get_ls_mgr(ObPluginVectorIndexMgr *&index_ls_mgr)
{
  int ret = OB_SUCCESS;
  index_ls_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl manager not init", KR(ret));
  } else if (OB_FAIL(vector_index_service_->get_ls_index_mgr_map().get_refactored(ls_->get_ls_id(), index_ls_mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
    }
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_and_execute_tasks(ObIArray<uint64_t> &vec_table_id_array)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret), K_(tenant_id));
  } else if (tenant_data_version >= DATA_VERSION_4_6_0_1) {
    // new scheduler ObVecIdxAsyncTaskScheduler takes over, skip legacy path
  } else {
    // min_data_version < 4.6.0.0: run legacy path for rolling upgrade compatibility
    ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
    if (OB_FAIL(get_ls_mgr(index_ls_mgr))) {
      LOG_WARN("fail to get ls mgr", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(index_ls_mgr)
               && index_ls_mgr->get_ls_task_ctx().state_ != OB_TTL_TASK_RUNNING) {
      LOG_INFO("not vector index scheduler running",
        K(index_ls_mgr->get_ls_task_ctx().state_), K(ls_->get_ls_id()));
    } else {
      if (can_schedule(ObVectorTaskScheduleType::ADAPTER_MAINTENANCE)
          && OB_FAIL(check_and_execute_adapter_maintenance_task(index_ls_mgr, vec_table_id_array))) {
        LOG_WARN("fail to check and execute adapter maintenance task",
          KR(ret), K_(tenant_id), K(ls_->get_ls_id()));
      }

      if (!is_leader_) {
        if (OB_FAIL(check_and_execute_ivf_cache_maintenance_task(index_ls_mgr))) {
          LOG_WARN("fail to check and execute ivf cache maintenance task",
            KR(ret), K_(tenant_id), K(ls_->get_ls_id()));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(index_ls_mgr)
          && (current_memory_config_ != 0)
          && can_schedule(ObVectorTaskScheduleType::FOLLOWER_SYNC)
          && OB_FAIL(log_tablets_need_memdata_sync(index_ls_mgr))) {
        LOG_WARN("fail to log tablets need memdata sync",
          KR(ret), K_(tenant_id), K(ls_->get_ls_id()));
      }

      ret = OB_SUCCESS;
      if (can_schedule(ObVectorTaskScheduleType::FOLLOWER_SYNC)
          && OB_NOT_NULL(index_ls_mgr)
          && OB_FAIL(check_and_execute_memdata_sync_task(index_ls_mgr))) {
        LOG_WARN("fail to check and execute memdata sync task",
          KR(ret), K_(tenant_id), K(ls_->get_ls_id()));
      }
    }
  }
  return ret;
}

void ObPluginVectorIndexLoadScheduler::run_task()
{
  ObCurTraceId::init(GCONF.self_addr_);
  ObTimeGuard guard("ObPluginVectorIndexLoadScheduler::run_task",
                     VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD);
  int ret = OB_SUCCESS;
  bool can_process = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", KR(ret));
  } else if (OB_FAIL(ObTTLUtil::check_can_process_tenant_tasks(tenant_id_, can_process))) {
    // check ObMultiVersionSchemaService ready
    LOG_WARN("check_can_process_tenant_tasks failed", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (!can_process) {
    LOG_INFO("schema service not ready", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (ATOMIC_BCAS(&need_do_for_switch_, true, false)) {
    // reserved, do nothing
    LOG_INFO("switch leader", K(tenant_id_), K(ls_->get_ls_id()), "is_leader", ATOMIC_LOAD(&is_leader_), K(is_stopped_));
  } else if (check_can_do_work()){
    check_can_schedule();
    ObSEArray<uint64_t, DEFAULT_TABLE_ARRAY_SIZE> vec_table_id_array;
    if (OB_FAIL(check_tenant_memory())) {
      LOG_WARN("check vector index resource failed", KR(ret));
    } else if (OB_FAIL(check_has_vector_index(vec_table_id_array))) {
      LOG_WARN("check vector index schema failed", KR(ret));
    } else if (OB_FAIL(reload_tenant_task())) {
      LOG_WARN("fail to reload tenant task", KR(ret));
    } else if (OB_FAIL(check_and_execute_tasks(vec_table_id_array))) {
      LOG_WARN("fail to scan and handle all tenant event", KR(ret));
    }
    schedule_finish();
  }
}

OB_SERIALIZE_MEMBER(ObVectorIndexSyncLog, flags_, tablet_id_array_, table_id_array_)

int ObPluginVectorIndexLoadScheduler::check_ls_in_vector_migration(bool &is_in_migration)
{
  int ret = OB_SUCCESS;
  ObMigrationStatus migration_status = OB_MIGRATION_STATUS_NONE;
  bool enable_migrate_vector_index = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get tenant_config", KR(ret), K(tenant_id_));
  } else if (FALSE_IT(enable_migrate_vector_index = tenant_config->_enable_migrate_vector_index)) {
  } else if (!enable_migrate_vector_index) {
    is_in_migration = false;
  } else if (OB_FAIL(ls_->get_migration_status(migration_status))) {
    LOG_WARN("fail to get ls migration status", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else {
    is_in_migration = (OB_MIGRATION_STATUS_NONE != migration_status);
  }
  if (OB_SUCC(ret) && is_in_migration) {
    LOG_INFO("ls is in vector migration", K(tenant_id_), K(ls_->get_ls_id()), K(migration_status));
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::submit_log_()
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard ctx_guard(logging_lock_);
  if (is_logging_) {
    // do-nothing
    FLOG_INFO("vector index memdata sync is logging");
  } else if (OB_ISNULL(cb_.scheduler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scheduler point is null, not inited?", KR(ret));
  } else if (tablet_id_array_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get empty tablet id array", KR(ret));
  } else {
    ObVectorIndexSyncLog ls_log(tablet_id_array_, table_id_array_);
    const int64_t sync_task_count = tablet_id_array_.count();
    palf::LSN lsn;
    SCN base_scn = SCN::min_scn();
    SCN scn;
    logservice::ObLogBaseHeader
        base_header(logservice::ObLogBaseType::VEC_INDEX_LOG_BASE_TYPE,
                    logservice::ObReplayBarrierType::NO_NEED_BARRIER); // no need reply hint
    uint32_t log_size = base_header.get_serialize_size() + ls_log.get_serialize_size();
    if (log_size > ObVectorIndexSyncLogCb::VECTOR_INDEX_SYNC_LOG_MAX_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("log size is too large", KR(ret), K(log_size), K(tablet_id_array_.count()));
    } else if (OB_ISNULL(cb_.log_buffer_)) {
      cb_.log_buffer_ = static_cast<char *>(ob_malloc(ObVectorIndexSyncLogCb::VECTOR_INDEX_SYNC_LOG_MAX_LENGTH,
                                                      ObMemAttr(tenant_id_,
                                                      "VEC_INDEX_LOG")));
      if (OB_ISNULL(cb_.log_buffer_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc vec index memdata sync log buffer", KR(ret), K(log_size));
      }
    }

    int64_t pos = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(base_header.serialize(cb_.log_buffer_,
                                             ObVectorIndexSyncLogCb::VECTOR_INDEX_SYNC_LOG_MAX_LENGTH,
                                             pos))) {
      TRANS_LOG(WARN, "ObVectorIndexSyncLog serialize base header error",
        KR(ret), KP(cb_.log_buffer_), K(pos), K(sync_task_count), K(log_size));
    } else if (OB_FAIL(ls_log.serialize(cb_.log_buffer_,
                                        ObVectorIndexSyncLogCb::VECTOR_INDEX_SYNC_LOG_MAX_LENGTH,
                                        pos))) {
      TRANS_LOG(WARN, "ObVectorIndexSyncLog serialize vec index memdata sync log error",
        KR(ret), KP(cb_.log_buffer_), K(pos), K(sync_task_count), K(log_size));
    } else if (OB_FAIL(ls_->get_log_handler()->append(cb_.log_buffer_,
                                                      pos,
                                                      base_scn,
                                                      false,
                                                      false,
                                                      &cb_,
                                                      lsn,
                                                      scn))) {
      cb_.reset();
      TRANS_LOG(WARN, "vector index memdata sync log submit error",
        KR(ret), KP(cb_.log_buffer_), K(pos), K(sync_task_count), K(log_size));
    } else {
      is_logging_ = true;
      TRANS_LOG(INFO, "submit vector index memdata sync log success",
        K(tenant_id_), K(ls_->get_ls_id()), K(sync_task_count), K(log_size),
        K(base_scn), K(lsn), K(scn), K(tablet_id_array_.count()));
    }
    tablet_id_array_.reuse();
    table_id_array_.reuse();
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::handle_submit_callback(const bool success)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard ctx_guard(logging_lock_);
  is_logging_ = false;
  TRANS_LOG(INFO, "submit vector index memdata sync log success",
            K(tenant_id_), K(ls_->get_ls_id()), K(success));
  return ret;
}

int ObPluginVectorIndexLoadScheduler::handle_replay_result(ObVectorIndexSyncLog &ls_log)
{
  int ret = OB_SUCCESS;
  bool is_in_migration = false;
  ObPluginVectorIndexMgr *mgr = nullptr;
  if (OB_FAIL(vector_index_service_->acquire_vector_index_mgr(ls_->get_ls_id(), mgr))) {
    LOG_WARN("fail to acquire vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (OB_FAIL(check_ls_in_vector_migration(is_in_migration))) {
    LOG_WARN("fail to check ls migration status", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (is_in_migration) {
    LOG_INFO("ls is in migration, skip replay memdata sync task",
             K(tenant_id_), K(ls_->get_ls_id()));
  } else if (OB_FAIL(mgr->get_mem_sync_info().add_task_to_waiting_map(ls_log))){
    LOG_WARN("memdata sync failed to add task", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  }
  if (ret == OB_ALLOCATE_MEMORY_FAILED) {
    LOG_WARN("memory allocation failed during replay, task may be incomplete",
             KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::replay(const void *buffer,
                                             const int64_t buf_size,
                                             const palf::LSN &lsn,
                                             const share::SCN &log_scn)
{
  int ret = OB_SUCCESS;
  logservice::ObLogBaseHeader base_header;
  int64_t tmp_pos = 0;
  const char *log_buf = static_cast<const char *>(buffer);
  ObVectorIndexTabletIDArray tmp_tablet_id_array;
  ObVectorIndexTableIDArray tmp_table_id_array;
  ObVectorIndexSyncLog ls_log(tmp_tablet_id_array, tmp_table_id_array);

  // need ls, and mgr
  if (OB_FAIL(base_header.deserialize(log_buf, buf_size, tmp_pos))) {
    TRANS_LOG(WARN, "log base header deserialize error", K(ret), KP(buffer), K(buf_size), K(lsn), K(log_scn));
  } else if (OB_FAIL(ls_log.deserialize((char *)buffer, buf_size, tmp_pos))) {
    TRANS_LOG(WARN, "desrialize tx_log_body error", K(ret), KP(buffer), K(buf_size), K(lsn), K(log_scn));
  } else if (OB_FAIL(handle_replay_result(ls_log))) {
    TRANS_LOG(WARN, "handle replay result fail", K(ret), K(ls_log), K(log_scn));
  } else {
    // do nothing
  }
  LOG_INFO("ObPluginVectorIndexLoadScheduler replay", K(ret), K(ls_log), K(base_header));
  return ret;
}

// checkpoint interfaces
int ObPluginVectorIndexLoadScheduler::flush(share::SCN &scn)
{
  UNUSED(scn);
  return OB_SUCCESS;
}

share::SCN ObPluginVectorIndexLoadScheduler::get_rec_scn()
{
  return share::SCN::max_scn();
}

// role change interfaces

int ObPluginVectorIndexLoadScheduler::switch_to_leader()
{
  int64_t start_time_us = ObTimeUtility::current_time();
  FLOG_INFO("vector index scheduler: begin to switch_to_leader", K_(tenant_id), KPC_(ls), K(start_time_us));
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load scheduler not inited", KR(ret));
  } else {
    ATOMIC_STORE(&is_leader_, true);
    ATOMIC_STORE(&need_do_for_switch_, true);
    ATOMIC_STORE(&need_refresh_, true);
  }
  if (OB_SUCC(ret) && check_can_do_work()) {
    ObPluginVectorIndexMgr *mgr = nullptr;
    if (OB_SUCC(vector_index_service_->get_ls_index_mgr_map().get_refactored(ls_->get_ls_id(), mgr))
        && OB_NOT_NULL(mgr)) {
      mgr->set_identity_ts(ObTimeUtility::current_time());
    }
    (void) ObPluginVectorIndexUtils::set_ls_leader_flag(ls_->get_ls_id(), ATOMIC_LOAD(&is_leader_));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(vector_index_service_)) {
    // Cancel residual follower tasks before resuming leader tasks
    inner_switch_to_leader_();
    // Mark LS for deferred task recovery on next scheduler timer cycle
    vector_index_service_->mark_ls_need_resume(ls_->get_ls_id());
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("vector index scheduler: finish to switch_to_leader", KR(ret), K_(tenant_id), KPC_(ls), K(cost_us));
  return ret;
}

int ObPluginVectorIndexLoadScheduler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  inner_switch_to_follower_();
  return ret;
}

void ObPluginVectorIndexLoadScheduler::switch_to_follower_forcedly()
{
  inner_switch_to_follower_();
}

void ObPluginVectorIndexLoadScheduler::inner_switch_to_follower_()
{
  FLOG_INFO("vector index scheduler: begin to switch_to_follower", K_(tenant_id), KPC_(ls));
  const int64_t start_time_us = ObTimeUtility::current_time();
  ATOMIC_STORE(&is_leader_, false);
  ATOMIC_STORE(&need_do_for_switch_, true);
  (void) ObPluginVectorIndexUtils::set_ls_leader_flag(ls_->get_ls_id(), ATOMIC_LOAD(&is_leader_));
  if (OB_NOT_NULL(vector_index_service_)) {
    const int tmp_ret = vector_index_service_->cancel_ls_leader_tasks(ls_->get_ls_id());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "fail to cancel ls leader tasks on follower switch", KR(tmp_ret), KPC_(ls));
    }
    ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
    int acquire_ret = vector_index_service_->acquire_vector_index_mgr(
        ls_->get_ls_id(), index_ls_mgr);
    if (OB_SUCCESS == acquire_ret && OB_NOT_NULL(index_ls_mgr)) {
      ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
      ObAsyncTaskQueueInvalidateFunc invalidate_func(task_opt);
      int foreach_ret = task_opt.get_async_task_map().foreach_refactored(invalidate_func);
      if (OB_SUCCESS != foreach_ret) {
        LOG_WARN_RET(foreach_ret, "fail to invalidate queue in async task map",
                     KR(foreach_ret), KPC_(ls));
      }
    } else if (OB_HASH_NOT_EXIST != acquire_ret) {
      LOG_WARN_RET(acquire_ret, "fail to acquire vector index mgr for queue invalidation",
                   KR(acquire_ret), KPC_(ls));
    }
    // Mark this LS's follower executor for deferred R1 sweep on next scheduler timer cycle
    vector_index_service_->mark_ls_need_resume_for_follower(ls_->get_ls_id());
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("vector index scheduler: finish to switch_to_follower", K_(tenant_id), KPC_(ls), K(cost_us));
}

int ObPluginVectorIndexLoadScheduler::check_other_ls_transfer_maintenance_finished(bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = true;
  bool is_oracle_mode = false;
  storage::ObLSService *ls_service = MTL(storage::ObLSService *);
  if (OB_ISNULL(vector_index_service_) || OB_ISNULL(ls_) || OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument for vector index transfer maintenance check",
        KR(ret), KP(vector_index_service_), KP(ls_), KP(ls_service), K(tenant_id_));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K_(tenant_id));
  } else if (is_oracle_mode) {
    // Oracle tenants skip vector index maintenance entirely (see check_can_do_work),
    // so last_transfer_scn would never advance. Treat as finished to avoid dead wait.
  } else {
    // The current LS is already in safe destroy and will not run new maintenance
    // tasks. Wait for other LS schedulers to finish their transfer maintenance
    // before removing tenant-level adaptors that they may still need to attach.
    TCRWLock::RLockGuard lock_guard(vector_index_service_->get_ls_mgr_map_lock());
    FOREACH_X(iter, vector_index_service_->get_ls_index_mgr_map(), OB_SUCC(ret) && is_finished) {
      const ObLSID &other_ls_id = iter->first;
      ObPluginVectorIndexMgr *other_mgr = iter->second;
      if (other_ls_id == ls_->get_ls_id()) {
        // Skip self: this scheduler is stopping, and safe_to_destroy() is the
        // final reconciliation point for this LS.
      } else if (other_ls_id.is_sys_ls()) {
        // SYS LS (ls_id=1) does not participate in user vector index transfer maintenance.
      } else if (OB_ISNULL(other_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vector index ls mgr is null", KR(ret), K(other_ls_id));
      } else {
        storage::ObLSHandle other_ls_handle;
        storage::ObLS *other_ls = nullptr;
        share::SCN other_transfer_scn = share::SCN::invalid_scn();
        if (OB_FAIL(ls_service->get_ls(other_ls_id, other_ls_handle, storage::ObLSGetMod::STORAGE_MOD))) {
          if (OB_LS_NOT_EXIST == ret) {
            ret = OB_SUCCESS; // LS already destroyed, its maintenance must have finished
          } else {
            LOG_WARN("failed to get other ls for vector index safe destroy", KR(ret), K(other_ls_id), K(ls_->get_ls_id()));
          }
        } else if (OB_ISNULL(other_ls = other_ls_handle.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("other ls is null", KR(ret), K(other_ls_id), K(ls_->get_ls_id()));
        } else if (OB_FAIL(other_ls->get_transfer_scn(other_transfer_scn))) {
          LOG_WARN("failed to get other ls transfer scn", KR(ret), K(other_ls_id), K(ls_->get_ls_id()));
        } else if (!other_ls->is_running()) {
          // Other LS is also stopping/offline; its scheduler is stopped and
          // last_transfer_scn will never advance. Avoid mutual dead wait when
          // multiple LSes destroy concurrently.
          LOG_INFO("other ls is not running, skip wait for vector index maintenance",
              K(other_ls_id), K(ls_->get_ls_id()));
        } else if (other_mgr->get_last_transfer_scn() != other_transfer_scn) {
          is_finished = false;
          LOG_INFO("other vector index ls maintenance is not finished",
              K(other_ls_id), K(other_transfer_scn), K(other_mgr->get_last_transfer_scn()), K(ls_->get_ls_id()));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::detach_and_clean_adaptors_for_destroy(ObPluginVectorIndexMgr *index_ls_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_ls_mgr) || OB_ISNULL(vector_index_service_) || OB_ISNULL(ls_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for vector index adaptor destroy cleanup",
        KR(ret), KP(index_ls_mgr), KP(vector_index_service_), KP(ls_));
  } else {
    // 1. detach local adaptors
    ObSEArray<ObTabletID, DEFAULT_TABLE_ARRAY_SIZE> local_tablet_id_array;
    if (OB_FAIL(index_ls_mgr->collect_adaptor_tablet_ids(local_tablet_id_array))) {
      LOG_WARN("failed to collect adaptor tablet ids", KR(ret), K(ls_->get_ls_id()));
    } else {
      LOG_INFO("[VEC_INDEX][LS_DESTROY] collected local vector adaptors on ls destroy",
          K(ls_->get_ls_id()), "local_adaptor_count", local_tablet_id_array.count(),
          K(local_tablet_id_array));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < local_tablet_id_array.count(); i++) {
      const ObTabletID tablet_id = local_tablet_id_array.at(i);
      ObPluginVectorIndexAdapterGuard tenant_guard;
      bool detached = false;
      if (OB_FAIL(index_ls_mgr->detach_adapter(tablet_id, detached))) {
        LOG_WARN("failed to detach adaptor on safe destroy", KR(ret), K(tablet_id), K(ls_->get_ls_id()));
      // tenant-only adaptors are attached to ls_map before tablet deletion, so
      // local_tablet_id_array is the single source for destroy cleanup here.
      } else if (OB_FAIL(vector_index_service_->get_tenant_adapter_inst_guard(tablet_id, tenant_guard))) {
        if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[VEC_INDEX][LS_DESTROY] failed to recheck tenant adaptor after ls destroy cleanup",
              KR(ret), K(tablet_id), K(ls_->get_ls_id()));
        }
      } else if (OB_ISNULL(tenant_guard.get_adatper())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[VEC_INDEX][LS_DESTROY] tenant adaptor is null after ls destroy cleanup",
            KR(ret), K(tablet_id), K(ls_->get_ls_id()));
      } else if (0 == tenant_guard.get_adatper()->get_ls_ref()) {
        int tmp_ret = vector_index_service_->erase_tenant_vec_adaptor(tablet_id);
        if (tmp_ret != OB_SUCCESS && tmp_ret != OB_HASH_NOT_EXIST) {
          ret = tmp_ret;
          LOG_WARN("[VEC_INDEX][LS_DESTROY] failed to erase tenant adaptor by local ls map on ls destroy",
              KR(ret), K(tablet_id), K(ls_->get_ls_id()));
        } else {
          LOG_INFO("[VEC_INDEX][LS_DESTROY] erased tenant adaptor by local ls map on ls destroy",
              K(tablet_id), K(ls_->get_ls_id()), KPC(tenant_guard.get_adatper()));
        }
      } else {
        LOG_INFO("[VEC_INDEX][LS_DESTROY] tenant adaptor remains after ls destroy cleanup",
            K(tablet_id), K(ls_->get_ls_id()), KPC(tenant_guard.get_adatper()),
            "ls_ref", tenant_guard.get_adatper()->get_ls_ref());
      }
    }
  }
  return ret;
}

void ObPluginVectorIndexLoadScheduler::inner_switch_to_leader_()
{
  FLOG_INFO("vector index scheduler: begin to cancel follower tasks on switch_to_leader",
            K_(tenant_id), KPC_(ls));
  const int64_t start_time_us = ObTimeUtility::current_time();
  if (OB_NOT_NULL(vector_index_service_)) {
    const int tmp_ret = vector_index_service_->cancel_ls_follower_tasks(ls_->get_ls_id());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "fail to cancel ls follower tasks on leader switch", KPC_(ls));
    }
    ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
    int acquire_ret = vector_index_service_->acquire_vector_index_mgr(
        ls_->get_ls_id(), index_ls_mgr);
    if (OB_SUCCESS == acquire_ret && OB_NOT_NULL(index_ls_mgr)) {
      ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
      ObAsyncTaskQueueInvalidateFunc invalidate_func(task_opt);
      int foreach_ret = task_opt.get_async_task_map().foreach_refactored(invalidate_func);
      if (OB_SUCCESS != foreach_ret) {
        LOG_WARN_RET(foreach_ret, "fail to invalidate queue in async task map on leader switch",
                     KPC_(ls));
      }
    } else if (OB_HASH_NOT_EXIST != acquire_ret) {
      LOG_WARN_RET(acquire_ret, "fail to acquire vector index mgr for queue invalidation on leader switch",
                   KPC_(ls));
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("vector index scheduler: finish cancel follower tasks on switch_to_leader",
            K_(tenant_id), KPC_(ls), K(cost_us));
}

int ObPluginVectorIndexLoadScheduler::safe_to_destroy(bool &is_safe)
{
  int ret = OB_SUCCESS;
  is_safe = true;
  DEBUG_SYNC(BEFORE_DESTROY_COLLECT_ADAPTERS);

  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  int64_t remain_task_cnt = 0;
  if (OB_NOT_NULL(vector_index_service_)) {
    if (OB_FAIL(vector_index_service_->get_ls_index_mgr_map().get_refactored(ls_->get_ls_id(), index_ls_mgr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
      }
    } else if (OB_NOT_NULL(index_ls_mgr)) {
      // A ctx is only removed from the map after its inner-table row is updated to
      // FINISH and all ownership flags (in_thread_pool_/in_queue_/in_cancel_/
      // cancel_post_work_pending_) are clear, so map empty == all tasks fully
      // cancelled and persisted.
      common::ObSpinLockGuard guard(index_ls_mgr->task_ctx_lock_);
      remain_task_cnt = index_ls_mgr->get_async_task_opt().get_async_task_map().size();
    }
  }

  int64_t dag_ref = get_dag_ref();
  if (0 != dag_ref || 0 != remain_task_cnt) {
    // Normal path: scheduler tick is still running and handles task cancellation
    // via check_and_schedule_*'s is_stop() branch each round.  Just wait.
    //
    // Fallback path: if the tenant-level ObVecIdxAsyncTaskScheduler has already
    // been stopped (e.g. tenant force-stop), the tick will never fire again.
    // Calling util cancel directly here is safe — no race.
    if (OB_NOT_NULL(vector_index_service_)
        && OB_NOT_NULL(index_ls_mgr)
        && vector_index_service_->get_vec_idx_async_task_sched().is_scheduler_stopped()) {
      int tmp_ret = ObVecIndexAsyncTaskUtil::cancel_all_async_tasks_for_destroy(index_ls_mgr);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to cancel async tasks (scheduler stopped fallback)", K(tmp_ret));
      }
    }
    if (REACH_TIME_INTERVAL(60L * 1000000)) {  // 60s
      LOG_WARN("vector index scheduler can't destroy",
               K(dag_ref), K(remain_task_cnt));
    }
    is_safe = false;
  } else if (OB_NOT_NULL(vector_index_service_)) {
    // All async tasks are done. Release SHARE_MOD ObLSHandle refs held by per-LS
    // executors before the ref_cnt gate below; otherwise ref_cnt stays > 1 until
    // remove_and_destroy_ls_executors, which was previously only reached after ref_cnt <= 1.
    LOG_INFO("remove and destroy ls executors in safe_to_destroy", K(ls_->get_ls_id()));
    vector_index_service_->get_vec_idx_async_task_sched()
        .remove_and_destroy_ls_executors(ls_->get_ls_id());
  }

  // Query paths indirectly hold an ObLSHandle via the DAS sub-iter's
  // ObTableScanIterator::ctx_guard_. While that handle is alive the mgr may
  // still be in use and must not be freed. ObLS::safe_to_destroy outside also
  // waits for ref_cnt to drop to 1 (the safe_destroy_task itself); gate here
  // to keep the destroy order consistent.
  if (is_safe && OB_NOT_NULL(index_ls_mgr)
      && ls_->get_ref_mgr().get_total_ref_cnt() > 1) {
    if (REACH_TIME_INTERVAL(60L * 1000000)) {
      LOG_WARN("vector index mgr destroy blocked by outstanding ls handle",
               K(ls_->get_ls_id()),
               "ref_cnt", ls_->get_ref_mgr().get_total_ref_cnt());
    }
    is_safe = false;
  }

  if (is_safe && OB_NOT_NULL(index_ls_mgr)) {
    bool other_ls_maintenance_finished = true;
    if (OB_FAIL(check_other_ls_transfer_maintenance_finished(other_ls_maintenance_finished))) {
      LOG_WARN("failed to check other ls transfer maintenance", KR(ret), K(ls_->get_ls_id()));
    } else if (!other_ls_maintenance_finished) {
      is_safe = false;
    }
  }

  if (OB_SUCC(ret) && is_safe && OB_NOT_NULL(index_ls_mgr)) {
    if (OB_FAIL(detach_and_clean_adaptors_for_destroy(index_ls_mgr))) {
      LOG_WARN("failed to detach and clean adaptors for destroy", KR(ret), K(ls_->get_ls_id()));
    } else if (OB_FAIL(vector_index_service_->remove_ls_index_mgr(ls_->get_ls_id()))) {
      LOG_WARN("failed to remove ls index mgr on destroy", KR(ret), K(ls_->get_ls_id()));
    } else {
      LOG_INFO("successfully removed ls index mgr on destroy", K(ls_->get_ls_id()));
    }
  }
  return ret;
}

void ObPluginVectorIndexLoadScheduler::stop()
{
  if (ATOMIC_BCAS(&is_stopped_, false, true)) {
    int ret = OB_SUCCESS;
    ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
    if (OB_NOT_NULL(vector_index_service_)) {
      // ObLS::stop_() -> tablet_ttl_mgr_.stop() stops this scheduler's TG before this call,
      // then later ObLS::safe_to_destroy() checks ref_mgr_; release ObLSHandle refs here.
      if (OB_FAIL(vector_index_service_->get_ls_index_mgr_map().get_refactored(ls_->get_ls_id(), index_ls_mgr))) {
        LOG_WARN("fail to get vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
      } else if (OB_NOT_NULL(index_ls_mgr)) {
        index_ls_mgr->stop_vec_idx_async_executor_bind();
        index_ls_mgr->get_async_task_opt().set_stop();
        // Set mgr-level destroying flag immediately so that any in-progress
        // adaptor / serialize / utils operations (which poll is_ls_destroying())
        // abort as early as possible. this mgr will be destroyed after ls is destroyed.
        index_ls_mgr->set_ls_destroying();
      }
    }
    FLOG_INFO("vector index task scheduler stop", K(ls_->get_ls_id()));
  }
}

void ObPluginVectorIndexLoadScheduler::destroy()
{
  if (is_inited_ && OB_NOT_NULL(vector_index_service_) && OB_NOT_NULL(ls_)) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(vector_index_service_->remove_ls_index_mgr(ls_->get_ls_id()))) {
      LOG_WARN("failed to remove ls index mgr on scheduler destroy", KR(ret),
               K(tenant_id_), K(ls_->get_ls_id()));
    }
  }
  LOG_INFO("vector index scheduler destroy", K(ls_->get_ls_id()), K(is_inited_));
}

// ------ implement mem sync task ------
int ObVectorIndexDag::init(ObPluginVectorIndexMgr *mgr, ObPluginVectorIndexTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(mgr) || OB_ISNULL(task_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mgr), KP(task_ctx));
  } else {
    compat_mode_ = lib::Worker::CompatMode::MYSQL; // only support mysql now
    param_.tenant_id_ = mgr->get_tenant_id();
    param_.ls_id_ = mgr->get_ls_id();
    param_.table_id_ = task_ctx->index_table_id_;
    param_.tablet_id_ = task_ctx->index_tablet_id_;
    param_.task_ctx_ = task_ctx;

    is_inited_ = true;
  }
  return ret;
}

bool ObVectorIndexDag::operator==(const ObIDag& other) const
{
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObVectorIndexDag &other_dag = static_cast<const ObVectorIndexDag&>(other);
    if (OB_UNLIKELY(!param_.is_valid() || !other_dag.param_.is_valid())) {
      LOG_ERROR_RET(OB_ERR_SYS, "invalid argument", K_(param), K(other_dag.param_));
    } else {
      is_equal = (param_ == other_dag.param_);
    }
  }
  return is_equal;
}

uint64_t ObVectorIndexDag::hash() const
{
  int64_t hash_value = 0;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid())) {
    LOG_ERROR_RET(OB_ERR_SYS, "invalid argument", K(is_inited_), K_(param));
  } else {
    hash_value = common::murmurhash(&param_.tenant_id_, sizeof(param_.tenant_id_), hash_value);
    hash_value += param_.ls_id_.hash();
    hash_value += common::murmurhash(&param_.tenant_id_, sizeof(param_.tenant_id_), hash_value);
    hash_value += param_.tablet_id_.hash();
  }
  return hash_value;
}

int ObVectorIndexDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVectorIndexDag has not been initialized", K(is_inited_), K_(param));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "vector index memdata sync task: "
                                     "tenant_id = %ld, ls_id = %ld, table_id = %ld, tablet_id = %ld",
                                     param_.tenant_id_,
                                     param_.ls_id_.id(),
                                     param_.table_id_,
                                     param_.tablet_id_.id()))) {
    LOG_WARN("fail to fill dag key", KR(ret), K(param_));
  }
  return ret;
}

int ObVectorIndexDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVectorIndexDag has not been initialized", K(is_inited_), K_(param));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                    static_cast<int64_t>(param_.tenant_id_),
                    static_cast<int64_t>(param_.ls_id_.id()),
                    static_cast<int64_t>(param_.table_id_),
                    static_cast<int64_t>(param_.tablet_id_.id())))) {
    LOG_WARN("fail to fill info param", KR(ret), K_(param));
  }
  return ret;
}

int ObVectorIndexTask::init(ObPluginVectorIndexLoadScheduler *schedular,
                            ObPluginVectorIndexMgr *mgr,
                            ObPluginVectorIndexTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(schedular) || OB_ISNULL(mgr) || OB_ISNULL(task_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(schedular), KP(mgr), KP(task_ctx));
  } else {
    allocator_.set_tenant_id(mgr->get_tenant_id());
    ls_id_ = mgr->get_ls_id();
    vec_idx_scheduler_ = schedular;
    vec_idx_mgr_ = mgr;
    task_ctx_ = task_ctx;
    read_snapshot_.reset();
    is_inited_ = true;
  }
  return ret;
}

int ObVectorIndexTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index task has not been initialized", K(is_inited_));
  } else if (OB_ISNULL(vec_idx_mgr_) || OB_ISNULL(task_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(vec_idx_mgr_), KP(task_ctx_));
  } else if (vec_idx_scheduler_->is_stopped()) {
    common::ObSpinLockGuard ctx_guard(task_ctx_->lock_);
    task_ctx_->err_code_ = OB_SUCCESS;
    task_ctx_->task_status_ = OB_TTL_TASK_FINISH;
    LOG_INFO("vec index scheduler is stopped, memdata sync task mark finish", KR(ret), KPC(task_ctx_));
  } else {
    bool need_stop = false;
    const uint64_t tenant_id = vec_idx_scheduler_->get_tenant_id();
    lib::ContextParam param;
    // use dag mtl id for param refer to TTLtask
    param.set_mem_attr(MTL_ID(), "VecIdxTaskCP", ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    CREATE_WITH_TEMP_CONTEXT(param) {

      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (!tenant_config.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail get tenant_config", KR(ret), K(MTL_ID()));
      } else if (!vec_idx_scheduler_->get_ls_leader() && !tenant_config->load_vector_index_on_follower) {
        need_stop = true;
        common::ObSpinLockGuard ctx_guard(task_ctx_->lock_);
        task_ctx_->task_status_ = OB_TTL_TASK_CANCEL;
        LOG_INFO("do not load memdata to ls follower", K(ret), K(tenant_config->load_vector_index_on_follower));
      }

      if (OB_FAIL(ret) || need_stop) {
      } else if (OB_FAIL(process_one())) {
        LOG_WARN("fail to process one", KR(ret), K(ls_id_), KPC(task_ctx_));
      }
      ret = OB_SUCCESS; // continue to try schedular remainig tasks

      if (OB_FAIL(vec_idx_scheduler_->check_task_state(vec_idx_mgr_, task_ctx_, need_stop))) {
        LOG_WARN("fail to check task state", KR(ret), K(ls_id_), KPC(task_ctx_));
        ret = OB_SUCCESS; // cover memdata sync failure
      }
    }
  }
  vec_idx_scheduler_->dec_dag_ref();
  return ret;
}

int ObVectorIndexTask::process_one()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtil::current_time();
  ObPluginVectorIndexAdapterGuard adpt_guard;
  ObPluginVectorIndexAdapterGuard new_adpt_guard;
  bool is_leader = false;

  if (OB_FAIL(ObPluginVectorIndexUtils::get_ls_leader_flag(ls_id_, is_leader))) {
    LOG_WARN("memdata sync fail to get ls leader flag", KR(ret), K(ls_id_), KPC(task_ctx_));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::get_read_scn(is_leader, ls_id_, read_snapshot_))) {
    LOG_WARN("memdata sync fail to get read scn", KR(ret), K(ls_id_), KPC(task_ctx_));
  } else if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard(task_ctx_->index_tablet_id_, adpt_guard))) {
    LOG_WARN("memdata sync fail to get adapter instance", KR(ret), K(ls_id_), KPC(task_ctx_));
  } else {
    common::ObSpinLockGuard ctx_guard(adpt_guard.get_adatper()->get_reload_lock());
    if (vec_idx_scheduler_->get_ls_leader() && adpt_guard.get_adatper()->get_reload_finish()) {
      // do nothing
    } else if (OB_FAIL(ObPluginVectorIndexUtils::refresh_memdata(ls_id_,
                                                                 adpt_guard.get_adatper(),
                                                                 read_snapshot_,
                                                                 allocator_,
                                                                 /*task_ctx*/ nullptr))) {
      LOG_WARN("memdata sync fail to refresh memdata", KR(ret), K(ls_id_), KPC(task_ctx_));
    } else if (vec_idx_scheduler_->get_ls_leader()) {
      adpt_guard.get_adatper()->set_reload_finish(true);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard(task_ctx_->index_tablet_id_, new_adpt_guard))) {
    LOG_WARN("memdata sync fail to get adapter instance", KR(ret), K(ls_id_), KPC(task_ctx_));
  }

  if (OB_SUCC(ret)) {
    task_ctx_->err_code_ = OB_SUCCESS;
    new_adpt_guard.get_adatper()->sync_succ();
  } else {
    task_ctx_->err_code_ = ret;
    if (OB_NOT_NULL(new_adpt_guard.get_adatper())) {
      new_adpt_guard.get_adatper()->sync_fail(ret);
    } else if (OB_NOT_NULL(adpt_guard.get_adatper())) {
      adpt_guard.get_adatper()->sync_fail(ret);
    }
  }

  int64_t cur_time = ObTimeUtil::current_time();
  int64_t cost = cur_time - start_time;
  LOG_INFO("memdata sync finish process one", K(cost), K(allocator_.used()), K(allocator_.total()),
    K(ls_id_), KPC(task_ctx_), K(cur_time - task_ctx_->task_start_time_));
  allocator_.reset();
  LOG_INFO("memdata sync check allocator use", K(allocator_.used()), K(allocator_.total()),
    K(ls_id_), KPC(task_ctx_));

  return ret;
}

int ObVectorIndexMemSyncInfo::init(int64_t hash_capacity, uint64_t tenant_id, ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  if (OB_FAIL(first_mem_sync_map_.create(hash_capacity, "VecIdxTaskMap", "VecIdxTaskMap", tenant_id))) {
    LOG_WARN("fail to create first mem sync set", K(tenant_id), K(ls_id), KR(ret));
  } else if (OB_FAIL(second_mem_sync_map_.create(hash_capacity, "VecIdxTaskMap", "VecIdxTaskMap", tenant_id))) {
    LOG_WARN("fail to create second mem sync set", K(ls_id), K(ls_id), KR(ret));
  } else if (OB_FAIL(deferred_map_.create(hash_capacity, "VecIdxDeferMap", "VecIdxDeferMap", tenant_id))) {
    LOG_WARN("fail to create deferred mem sync set", K(tenant_id), K(ls_id), KR(ret));
  }
  return ret;
}

void ObVectorIndexMemSyncInfo::destroy()
{
  // if count != 0 and not all finish, error!
  first_mem_sync_map_.destroy();
  first_task_allocator_.reset();
  second_mem_sync_map_.destroy();
  second_task_allocator_.reset();
  deferred_map_.destroy();
}

int ObVectorIndexMemSyncInfo::add_deferred_task(
    const ObTabletID &tablet_id, uint64_t table_id, int ret_code)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  common::ObSpinLockGuard guard(switch_lock_);
  ObVectorIndexMemSyncDeferredInfo info;
  if (!tablet_id.is_valid() || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid deferred mem sync task", KR(ret), K(tablet_id), K(table_id), K(ret_code));
  } else if (OB_FAIL(deferred_map_.get_refactored(tablet_id, info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      const int64_t delay_us = 5L * 1000L * 1000L;
      info = ObVectorIndexMemSyncDeferredInfo(table_id, ret_code, now, now + delay_us);
      if (OB_FAIL(deferred_map_.set_refactored(tablet_id, info))) {
        LOG_WARN("fail to add deferred mem sync task", KR(ret), K(tablet_id), K(info));
      }
    }
  } else {
    ++info.defer_count_;
    info.table_id_ = table_id;
    info.ret_code_ = ret_code;
    info.last_defer_ts_ = now;
    const int64_t delay_sec = MIN(60L, 5L << MIN(info.defer_count_ - 1, 4L));
    info.next_schedule_ts_ = now + delay_sec * 1000L * 1000L;
    if (OB_FAIL(deferred_map_.set_refactored(tablet_id, info, 1 /* overwrite */))) {
      LOG_WARN("fail to update deferred mem sync task", KR(ret), K(tablet_id), K(info));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("defer mem sync tablet", K(tablet_id), K(info));
  }
  return ret;
}

int ObVectorIndexMemSyncInfo::promote_due_deferred_tasks()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  ObSEArray<ObTabletID, 16> due_tablets;
  common::ObSpinLockGuard guard(switch_lock_);
  FOREACH_X(iter, deferred_map_, OB_SUCC(ret)) {
    if (iter->second.next_schedule_ts_ <= now
        && OB_FAIL(due_tablets.push_back(iter->first))) {
      LOG_WARN("fail to collect due deferred mem sync tablet", KR(ret), K(iter->first));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < due_tablets.count(); ++i) {
    const ObTabletID tablet_id = due_tablets.at(i);
    ObVectorIndexMemSyncDeferredInfo info;
    VectorIndexMemSyncMap &waiting_map = get_waiting_map();
    ObVecIndexAsyncTaskCtx *existing_ctx = nullptr;
    if (OB_FAIL(deferred_map_.get_refactored(tablet_id, info))) {
      LOG_WARN("fail to get due deferred mem sync tablet", KR(ret), K(tablet_id));
    } else if (OB_SUCCESS == waiting_map.get_refactored(tablet_id, existing_ctx)) {
      // A newer clog or maintenance event has already re-armed this tablet.
    } else if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to check waiting mem sync tablet", KR(ret), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
      char *buf = static_cast<char *>(get_waiting_allocator().alloc(sizeof(ObVecIndexAsyncTaskCtx)));
      ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate promoted mem sync task", KR(ret), K(tablet_id));
      } else if (FALSE_IT(task_ctx = new(buf) ObVecIndexAsyncTaskCtx())) {
      } else if (FALSE_IT(task_ctx->task_status_.tablet_id_ = tablet_id)) {
      } else if (FALSE_IT(task_ctx->task_status_.table_id_ = info.table_id_)) {
      } else if (OB_FAIL(waiting_map.set_refactored(tablet_id, task_ctx))) {
        task_ctx->~ObVecIndexAsyncTaskCtx();
        get_waiting_allocator().free(task_ctx);
        LOG_WARN("fail to promote deferred mem sync tablet", KR(ret), K(tablet_id), K(info));
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = deferred_map_.erase_refactored(tablet_id);
      if (OB_SUCCESS != tmp_ret && OB_HASH_NOT_EXIST != tmp_ret) {
        LOG_WARN("fail to erase promoted deferred mem sync tablet", K(tmp_ret), K(tablet_id));
      } else {
        LOG_INFO("promote deferred mem sync tablet to waiting map", K(tablet_id), K(info));
      }
    }
  }
  return ret;
}

void ObVectorIndexMemSyncInfo::switch_processing_map()
{
  // prevent context switch when other thread using waiting map
  common::ObSpinLockGuard ctx_guard(switch_lock_);
  processing_first_mem_sync_ = !processing_first_mem_sync_;
}

int ObVectorIndexMemSyncInfo::count_processing_finished(bool &is_finished,
                                                        uint32_t &total_count,
                                                        uint32_t &finished_count)
{
  // check all tasks in processing map finished
  int ret = OB_SUCCESS;
  is_finished = false;
  uint32_t count = 0;
  ObSEArray<ObTabletID, 16> tablet_id_array;

  VectorIndexMemSyncMap &current_task_map = get_processing_map();
  FOREACH(iter, current_task_map) {
    ObTabletID tablet_id = iter->first;
    ObVecIndexAsyncTaskCtx *ctx = iter->second;
    if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
      LOG_WARN("failed to collect tablet id for memdata sync summary", K(ret), K(tablet_id));
      ret = OB_SUCCESS;
    }
    if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memdata sync get null memdta_ctx", KPC(ctx));
    } else if (ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH ||
              ctx->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL) {
      count++;
    }
  }

  finished_count = count;
  total_count = current_task_map.size();
  LOG_INFO("dump memdata sync task summary", K(finished_count), K(total_count), K(tablet_id_array));

  if (OB_SUCC(ret)) {
    if (count > 0 && count == total_count) {
      is_finished = true;
    }
  }
  return ret;
}

int ObVectorIndexMemSyncInfo::add_task_to_waiting_map(ObVectorIndexSyncLog &ls_log)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard ctx_guard(switch_lock_); // prevent context switch in maintance thread
  VectorIndexMemSyncMap &waiting_task_map = get_waiting_map();
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_log.get_tablet_id_array().count(); i++) {
    ObTabletID tablet_id = ls_log.get_tablet_id_array().at(i);
    uint64_t table_id = ls_log.get_table_id_array().at(i);
    ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
    ObVecIndexAsyncTaskCtx *tmp_task_ctx = nullptr;
    if (OB_FAIL(waiting_task_map.get_refactored(tablet_id, tmp_task_ctx))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        char *task_ctx_buf =
          static_cast<char *>(get_waiting_allocator().alloc(sizeof(ObVecIndexAsyncTaskCtx)));
        if (OB_ISNULL(task_ctx_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("memdata sync fail to alloc task ctx", KR(ret));
        } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
        } else if (FALSE_IT(task_ctx->task_status_.tablet_id_ = tablet_id)) {
        } else if (FALSE_IT(task_ctx->task_status_.table_id_ = table_id)) {
        } else if (OB_FAIL(waiting_task_map.set_refactored(tablet_id, task_ctx))) {
          LOG_WARN("memdata sync failed to set vector index task ctx", K(ret), K(tablet_id),
                   KPC(task_ctx));
        } else {
          LOG_INFO("memdata sync success get replay vector index task ctx", K(ret), K(tablet_id),
                   KPC(task_ctx));
        }
      }
    } else { // task already set, not scheduled
      LOG_INFO("memdata sync duplicate vector index task ctx", K(ret), K(tablet_id), KPC(tmp_task_ctx));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(task_ctx)) {
      task_ctx->~ObVecIndexAsyncTaskCtx();
      get_waiting_allocator().free(task_ctx);
      task_ctx = nullptr;
    }
  }
  return ret;
}

int ObVectorIndexMemSyncInfo::add_task_to_waiting_map(VectorIndexAdaptorMap &adapter_map)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard ctx_guard(switch_lock_); // prevent context switch in maintance thread
  VectorIndexMemSyncMap &current_map = get_waiting_map();
  FOREACH(iter, adapter_map) {
    // only use complete adapter, tablet id of no.3 aux index table
    ObPluginVectorIndexAdaptor *adapter = iter->second;
    ObTabletID tablet_id = iter->first;
    if (!adapter->is_ready_complete()) {
      // adaptor not fully initialized, skip
      LOG_INFO("adaptor not ready complete, skip memdata sync", K(ret), K(tablet_id), KPC(adapter));
    } else if (OB_INVALID_ID == adapter->get_inc_table_id()) {
      // do nothing
      LOG_INFO("adapter inc table id is invalid, skip memdata sync", K(ret), K(tablet_id), KPC(adapter));
    } else if (tablet_id == adapter->get_inc_tablet_id()) {
      ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
      ObVecIndexAsyncTaskCtx *tmp_task_ctx = nullptr;
      if (OB_FAIL(current_map.get_refactored(tablet_id, tmp_task_ctx))) {
        if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
          char *task_ctx_buf =
            static_cast<char *>(get_waiting_allocator().alloc(sizeof(ObVecIndexAsyncTaskCtx)));
          if (OB_ISNULL(task_ctx_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("memdata sync fail to alloc task ctx", KR(ret));
          } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
          } else if (FALSE_IT(task_ctx->task_status_.tablet_id_ = tablet_id)) {
          } else if (FALSE_IT(task_ctx->task_status_.table_id_ = adapter->get_inc_table_id())) {
          } else if (OB_FAIL(current_map.set_refactored(tablet_id, task_ctx))) {
            LOG_WARN("memdata sync failed to set vector index task ctx", K(ret), K(tablet_id),
                     KPC(task_ctx));
          } else {
            LOG_INFO("memdata sync success set force index task ctx", K(ret), K(tablet_id),
                     KPC(task_ctx));
          }
        }
      } else {
        LOG_INFO("memdata sync duplicate vector index task ctx", K(ret), K(tablet_id), KPC(tmp_task_ctx));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(task_ctx)) {
        task_ctx->~ObVecIndexAsyncTaskCtx();
        get_waiting_allocator().free(task_ctx);
        task_ctx = nullptr;
      }
    }
  }
  return ret;
}

int ObVectorIndexMemSyncInfo::add_task_to_waiting_map(ObTabletID &tablet_id, int64_t table_id)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard ctx_guard(switch_lock_); // prevent context switch in maintance thread
  VectorIndexMemSyncMap &current_map = get_waiting_map();
  ObVecIndexAsyncTaskCtx* task_ctx = nullptr;
  ObVecIndexAsyncTaskCtx* tmp_task_ctx = nullptr;
  if (OB_FAIL(current_map.get_refactored(tablet_id, tmp_task_ctx))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      char *task_ctx_buf =
        static_cast<char *>(get_waiting_allocator().alloc(sizeof(ObVecIndexAsyncTaskCtx)));
      if (OB_ISNULL(task_ctx_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memdata sync fail to alloc task ctx", KR(ret));
      } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
      } else if (FALSE_IT(task_ctx->task_status_.tablet_id_ = tablet_id)) {
      } else if (FALSE_IT(task_ctx->task_status_.table_id_ = table_id)) {
      } else if (OB_FAIL(current_map.set_refactored(tablet_id, task_ctx))) {
        LOG_WARN("memdata sync failed to set vector index task ctx", K(ret), K(tablet_id),
                 KPC(task_ctx));
      } else {
        LOG_INFO("memdata sync success set force index task ctx", K(ret), K(tablet_id), KPC(task_ctx));
      }
    }
  } else {
    LOG_INFO("memdata sync duplicate vector index task ctx", K(ret), K(tablet_id), KPC(tmp_task_ctx));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(task_ctx)) {
    task_ctx->~ObVecIndexAsyncTaskCtx();
    get_waiting_allocator().free(task_ctx);
    task_ctx = nullptr;
  }
  return ret;
}

void ObVectorIndexMemSyncInfo::check_and_switch_if_needed(bool &need_sync,
                                                          bool &all_finished,
                                                          bool can_release_processing_ctx)
{
  // only called in maintance thread.
  if (get_processing_map().size() > 0) {
    if (all_finished) {
      if (can_release_processing_ctx) {
        // Delay releasing task ctx objects until every memdata dag has exited.
        get_processing_map().reuse();
        get_processing_allocator().reset();
        all_finished = false;
        LOG_INFO("memdata sync release processing set",
          K(processing_first_mem_sync_), K(get_processing_map().size()), K(get_waiting_map().size()));
      } else {
        LOG_INFO("memdata sync defer releasing processing set until dags finish",
          K(processing_first_mem_sync_), K(get_processing_map().size()), K(get_waiting_map().size()));
      }
    } else {
      need_sync = true; // continue sync current processing set
      LOG_INFO("memdata sync continue processing set",
        K(processing_first_mem_sync_), K(get_processing_map().size()), K(get_waiting_map().size()));
    }
  }
  if (!need_sync && get_waiting_map().size() > 0) {
    // procession_set is empty, wating list not empty
    need_sync = true;
    // need lock in swith, replay thread may adding tasks to waiting map
    // switch without lock may let task alloc by allocator B set to map A
    switch_processing_map();
    LOG_INFO("memdata sync switch processing set to waiting set",
      K(processing_first_mem_sync_),
      K(get_processing_map().size()),
      K(get_waiting_map().size()));
  }
  // both map empty, do nothing
}

int ObVectorIndexSyncLogCb::on_success()
{
  ATOMIC_SET(&is_success_, true);
  if (OB_NOT_NULL(scheduler_)) {
    scheduler_->handle_submit_callback(true);
  }
  ATOMIC_SET(&is_callback_invoked_, true);
  return OB_SUCCESS;
}

int ObVectorIndexSyncLogCb::on_failure()
{
  if (OB_NOT_NULL(scheduler_)) {
    scheduler_->handle_submit_callback(false);
  }
  ATOMIC_SET(&is_callback_invoked_, true);
  return OB_SUCCESS;
}

}
}
