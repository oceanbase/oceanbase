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
#include "ob_plugin_vector_index_scheduler.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"

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
    basic_period_ = VEC_INDEX_SCHEDULAR_BASIC_PERIOD;
    cb_.scheduler_ = this;
    if (OB_FAIL(TG_SCHEDULE(ttl_timer_tg_id, *this, basic_period_, true))) {
      LOG_WARN("fail to schedule periodic task", KR(ret), K(ttl_timer_tg_id));
    }
  }
  return ret;
}

void ObPluginVectorIndexLoadScheduler::runTimerTask()
{
  ObCurTraceId::init(GCONF.self_addr_);
  ObTimeGuard guard("ObPluginVectorIndexLoadScheduler::runTimerTask",
    VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD);
  run_task();
}

void ObPluginVectorIndexLoadScheduler::clean_deprecated_adapters()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, DEFAULT_TABLE_ARRAY_SIZE> delete_tablet_id_array;
  delete_tablet_id_array.reset();

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
    FOREACH_X(iter, index_ls_mgr->get_complete_adapter_map(), OB_SUCC(ret)) {
      ObPluginVectorIndexAdaptor *adapter = iter->second;
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *table_schema;
      ObTabletID tablet_id = iter->first;
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, adapter->get_vbitmap_table_id(), table_schema))) {
        LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(adapter->get_vbitmap_table_id()));
      } else if (OB_ISNULL(table_schema) || table_schema->is_in_recyclebin()) {
        // remove adapter if tablet not exist or is in recyclebin
        if (OB_FAIL(delete_tablet_id_array.push_back(adapter->get_inc_tablet_id()))) {
          LOG_WARN("push back table id failed",
            K(delete_tablet_id_array.count()), K(adapter->get_inc_tablet_id()), KR(ret));
        } else if (OB_FAIL(delete_tablet_id_array.push_back(adapter->get_vbitmap_tablet_id()))) {
          LOG_WARN("push back table id failed",
            K(delete_tablet_id_array.count()), K(adapter->get_vbitmap_tablet_id()), KR(ret));
        } else if (OB_FAIL(delete_tablet_id_array.push_back(adapter->get_snap_tablet_id()))) {
          LOG_WARN("push back table id failed",
            K(delete_tablet_id_array.count()), K(adapter->get_snap_tablet_id()), KR(ret));
        }
      } else if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(tablet_id, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
        } else {
          ret = OB_SUCCESS; // not found, moved from this ls
          if (OB_FAIL(delete_tablet_id_array.push_back(tablet_id))) {
            LOG_WARN("push back table id failed",
              K(delete_tablet_id_array.count()), K(adapter->get_inc_tablet_id()), KR(ret));
          }
        }
      }
    }
    if (delete_tablet_id_array.count() > 0) {
      LOG_INFO("try erase complete vector index adapter",
          K(index_ls_mgr->get_ls_id()), K(delete_tablet_id_array.count()));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < delete_tablet_id_array.count(); i++) {
      if (OB_FAIL(index_ls_mgr->erase_complete_adapter(delete_tablet_id_array.at(i)))) {
        if (ret != OB_HASH_NOT_EXIST) {
          LOG_WARN("failed to erase full vector index adapter",
            K(index_ls_mgr->get_ls_id()), K(delete_tablet_id_array.at(i)), KR(ret));
        } else { // already removed
          ret = OB_SUCCESS;
        }
      }
    }

    delete_tablet_id_array.reset();

    FOREACH_X(iter, index_ls_mgr->get_partial_adapter_map(), OB_SUCC(ret)) {
      ObPluginVectorIndexAdaptor *adapter = iter->second;
      ObTabletID tablet_id = iter->first;
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(tablet_id, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
        } else {
          ret = OB_SUCCESS; // not found, moved from this ls
          if (OB_FAIL(delete_tablet_id_array.push_back(tablet_id))) {
            LOG_WARN("push back table id failed",
              K(delete_tablet_id_array.count()), K(adapter->get_inc_tablet_id()), KR(ret));
          }
        }
      } else {
        // tablet exist, but it may in recyclebin, cannot check schema if it is partial adapter from dml
        // add count here if more then 3 loops not merged, remove them.
        adapter->inc_idle();
        if (adapter->is_deprecated()) {
          if (OB_FAIL(delete_tablet_id_array.push_back(tablet_id))) {
            LOG_WARN("push back table id failed",
              K(delete_tablet_id_array.count()), K(adapter->get_inc_tablet_id()), KR(ret));
          }
        }
      }
    }

    if (delete_tablet_id_array.count() > 0) {
      LOG_INFO("try erase partial vector index adapter",
            K(index_ls_mgr->get_ls_id()), K(delete_tablet_id_array.count()));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < delete_tablet_id_array.count(); i++) {
      if (OB_FAIL(index_ls_mgr->erase_partial_adapter(delete_tablet_id_array.at(i)))) {
        if (ret != OB_HASH_NOT_EXIST) {
          LOG_WARN("failed to erase full vector index adapter",
            K(index_ls_mgr->get_ls_id()), K(delete_tablet_id_array.at(i)), KR(ret));
        } else { // already removed
          ret = OB_SUCCESS;
        }
      }
    }

    delete_tablet_id_array.reset();
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
  if (!mgr->get_partial_adapter_map().empty() || !mgr->get_complete_adapter_map().empty()) {
    // partial map not empty, exist adapter create by dml/ddl data complement/query
    // complete adapter not empty, also need check for transfer
    mark_tenant_need_check();
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

int ObPluginVectorIndexLoadScheduler::check_is_vector_index_table(const ObTableSchema &table_schema,
                                                                  bool &is_vector_index_table,
                                                                  bool &is_shared_index_table)
{
  int ret = OB_SUCCESS;
  is_vector_index_table = false;
  is_shared_index_table = false;
  if (table_schema.is_index_table() && !table_schema.is_in_recyclebin()) {
    if (table_schema.is_vec_delta_buffer_type()
        || table_schema.is_vec_index_id_type()
        || table_schema.is_vec_index_snapshot_data_type()) {
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

int ObPluginVectorIndexLoadScheduler::acquire_adapter_in_maintenance(const int64_t table_id,
                                                                     const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = table_schema->get_index_type();
  ObLSID ls_id = ls_->get_ls_id();
  ObArray<ObTabletID> tablet_ids;

  if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("fail to get tablet ids", KR(ret), K(table_id));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret));
  } else {
    ObTabletHandle tablet_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(tablet_ids.at(i), tablet_handle))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablet", K(ret), K(tablet_ids.at(i)));
        } else {
          ret = OB_SUCCESS; // not found, continue loop
        }
      } else {
        ObPluginVectorIndexAdapterGuard adapter_guard;
        ObString index_identity;
        // Notice:only no.3 aux table has vec_idx_params
        ObString vec_idx_params = table_schema->get_index_params();
        int64_t dim = 0;
        if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(*table_schema, dim))) {
          LOG_WARN("fail to get vec_index_col_param", K(ret));
        } else if (OB_FAIL(vector_index_service_->acquire_adapter_guard(ls_id,
                                                                 tablet_ids.at(i),
                                                                 index_type,
                                                                 adapter_guard,
                                                                 &vec_idx_params,
                                                                 dim))) {
          LOG_WARN("fail to acquire adapter gurad", K(ret), K(ls_id));
        } else if (adapter_guard.get_adatper()->is_complete()) { // get create type ?
          // already exist full adapter, bypass
        } else if (OB_FAIL(adapter_guard.get_adatper()->
            set_table_id(ObPluginVectorIndexUtils::index_type_to_record_type(index_type), table_id))) {
          LOG_WARN("fail to set table id", K(ret), K(ls_id), K(tablet_ids.at(i)));
        } else if (OB_FAIL(adapter_guard.get_adatper()->
            set_tablet_id(VIRT_DATA, tablet_handle.get_obj()->get_data_tablet_id()))) {
          LOG_WARN("fail to fill partial index adapter info",
            K(ret), K(ls_id), K(tablet_ids.at(i)), K(tablet_handle.get_obj()->get_data_tablet_id()));
        } else if (OB_FAIL(ObPluginVectorIndexUtils::get_vector_index_prefix(*table_schema,
                                                                             index_identity))) {
          LOG_WARN("fail to get index identity", KR(ret));
        } else if (OB_FAIL(adapter_guard.get_adatper()->set_index_identity(index_identity))) {
          LOG_WARN("fail to set index identity", KR(ret), KPC(adapter_guard.get_adatper()));
        } else {
          adapter_guard.get_adatper()->reset_idle();
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::set_shared_table_info_in_maintenance(
  const int64_t table_id,
  const ObTableSchema *table_schema,
  ObVecIdxSharedTableInfoMap &shared_table_info_map)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = table_schema->get_index_type();
  ObArray<ObTabletID> tablet_ids;

  if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("fail to get tablet ids", KR(ret), K(table_id));
  } else {
    ObTabletHandle tablet_handle;
    ObVectorIndexSharedTableInfo info;
    ObTabletID data_tablet_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(tablet_ids.at(i), tablet_handle))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("fail to get tablet", K(ret), K(tablet_ids.at(i)));
        } else {
          ret = OB_SUCCESS; // not found, continue loop
        }
      } else if (FALSE_IT(data_tablet_id = tablet_handle.get_obj()->get_data_tablet_id())) {
      } else if (OB_FAIL(shared_table_info_map.get_refactored(data_tablet_id, info))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get shared table info", K(ret), K(tablet_ids.at(i)));
        } else { // OB_HASH_NOT_EXIST
          if (index_type == INDEX_TYPE_VEC_ROWKEY_VID_LOCAL) {
            info.rowkey_vid_table_id_ = table_id;
            info.rowkey_vid_tablet_id_ = tablet_ids.at(i);
          } else {
            info.vid_rowkey_table_id_ = table_id;
            info.vid_rowkey_tablet_id_ = tablet_ids.at(i);
          }
          info.data_table_id_ = table_schema->get_data_table_id();
          if (OB_FAIL(shared_table_info_map.set_refactored(data_tablet_id, info))) {
            LOG_WARN("fail to set shared table info", K(ret), K(data_tablet_id));
          }
        }
      } else {
        if (index_type == INDEX_TYPE_VEC_ROWKEY_VID_LOCAL) {
          info.rowkey_vid_table_id_ = table_id;
          info.rowkey_vid_tablet_id_ = tablet_ids.at(i);
        } else {
          info.vid_rowkey_table_id_ = table_id;
          info.vid_rowkey_tablet_id_ = tablet_ids.at(i);
        }
        info.data_table_id_ = table_schema->get_data_table_id();
        const int overwrite = 1;
        if (!info.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid shared table info", K(ret), K(info));
        } else if (OB_FAIL(shared_table_info_map.set_refactored(data_tablet_id, info, overwrite))) {
          LOG_WARN("fail to set shared table info", K(ret), K(data_tablet_id));
        }
      }
    }
  }

  return ret;
}


// scan all vector tablet in current tenant/LS
int ObPluginVectorIndexLoadScheduler::execute_adapter_maintenance()
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObPluginVectorIndexLoadScheduler::check_and_generate_tablet_tasks",
                    VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD);
  const schema::ObTableSchema *table_schema = nullptr;
  ObSEArray<uint64_t, DEFAULT_TABLE_ARRAY_SIZE> table_id_array;

  ObVecIdxSharedTableInfoMap shared_table_info_map;
  ObMemAttr memattr(tenant_id_, "VecIdxInfo");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet vector index scheduler not init", KR(ret));
  } else {
    clean_deprecated_adapters();
  }

  if (current_memory_config_ != 0) { // has memory for new adapter

    if (OB_FAIL(ObTTLUtil::get_tenant_table_ids(tenant_id_, table_id_array))) {
        LOG_WARN("fail to get tenant table ids", KR(ret), K_(tenant_id));
    } else if (!table_id_array.empty()
                && OB_FAIL(shared_table_info_map.create(DEFAULT_TABLE_ARRAY_SIZE, memattr, memattr))) {
      LOG_WARN("fail to create param map", KR(ret));
    }

    int64_t start_idx = 0;
    int64_t end_idx = 0;

    while (OB_SUCC(ret) && start_idx < table_id_array.count()) {
      ObSchemaGetterGuard schema_guard;
      start_idx = end_idx;
      end_idx = MIN(table_id_array.count(), start_idx + TBALE_GENERATE_BATCH_SIZE);

      bool is_vector_index = false;
      bool is_shared_index = false;
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
      }

      for (int64_t idx = start_idx; OB_SUCC(ret) && idx < end_idx; ++idx) {
        const int64_t table_id = table_id_array.at(idx);
        const ObTableSchema *table_schema = nullptr;
        if (is_sys_table(table_id)) {
          // do nothing
        } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
          LOG_WARN("failed to get simple schema", KR(ret), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table schema is null", KR(ret), K(table_id), K_(tenant_id));
        } else if (table_schema->is_in_recyclebin()) {
          // do nothing
        } else if (OB_FAIL(check_is_vector_index_table(*table_schema, is_vector_index, is_shared_index))) {
          LOG_WARN("fail to check is vector index", KR(ret));
        } else if (is_vector_index
                  && OB_FAIL(acquire_adapter_in_maintenance(table_id, table_schema))) {
          // for one vector_index table
          LOG_WARN("fail to create adapter in maintenance", KR(ret), K(table_id));
        } else if (is_shared_index
                  && OB_FAIL(set_shared_table_info_in_maintenance(table_id,
                                                                  table_schema,
                                                                  shared_table_info_map))) {
          LOG_WARN("fail to set shared table info", KR(ret), K(table_id));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(vector_index_service_->check_and_merge_adapter(ls_->get_ls_id(), shared_table_info_map))) {
      LOG_WARN("fail to merge parital adapter task", KR(ret));
    } else {
      mark_tenant_checked();
    }
  }

  LOG_INFO("finish generate tenant tablet tasks", KR(ret), K_(tenant_id));
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPluginVectorIndexLoadScheduler not init", KR(ret));
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
    ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
    if (OB_FAIL(vector_index_service_->get_ls_index_mgr_map().get_refactored(ls_->get_ls_id(), index_ls_mgr))) {
      if (OB_HASH_NOT_EXIST == ret) { // do nothing
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get vector index ls mgr", KR(ret),
                K(tenant_task.status_), K(tenant_id_), K(ls_->get_ls_id()));
      }
    } else if (OB_ISNULL(index_ls_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid vector index ls mgr", KR(ret),
                K(tenant_task.status_), K(tenant_id_), K(ls_->get_ls_id()));
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
      if (task_ctx->task_start_time_ == OB_INVALID_ID) {
        task_ctx->task_start_time_ = ObTimeUtility::current_time();
      }
      task_ctx->in_queue_ = true;
      // dag maybe already finished, and set status to finish/cancel,
      // but here change it to running and could not be scheduler later
      task_ctx->task_status_ = OB_TTL_TASK_RUNNING;
    }
  } else {
    LOG_DEBUG("status when try schedule task", KPC(mgr), K(task_ctx));
  }

  return OB_SUCCESS;
}

int ObPluginVectorIndexLoadScheduler::try_schedule_remaining_tasks(ObPluginVectorIndexMgr *mgr,
                                                                   ObPluginVectorIndexTaskCtx *current_ctx)
{
  int ret = OB_SUCCESS;
  // called in dag thread, maintaince thread may reuse this map if it is just finished.
  // but reuse happen in next maintaince cycle (10s), so it is safe here.
  VectorIndexMemSyncMap &current_task_map = mgr->get_mem_sync_info().get_processing_map();
  FOREACH_X(iter, current_task_map, OB_SUCC(ret)) {
    ObPluginVectorIndexTaskCtx *task_ctx = iter->second;
    if (OB_ISNULL(task_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid task ctx", KR(ret), KPC(task_ctx));
    } else if (task_ctx == current_ctx) {
      // bypass
    } else {
      common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
      if (can_schedule_task(task_ctx) && task_ctx->task_status_ == OB_TTL_TASK_PREPARE) {
        LOG_INFO("try schedule remaining task", KPC(task_ctx), KPC(current_ctx));
        if (OB_FAIL(try_schedule_task(mgr, task_ctx))) {
          if (OB_SIZE_OVERFLOW != ret) {
            LOG_WARN("fail to try schedule dag task", KR(ret));
          }
        }
      }
    }
  }

  if (OB_SIZE_OVERFLOW == ret) { // task queue full, schedule later
    ret = OB_SUCCESS;
  }

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
  } else {
    FLOG_INFO("build vector index memdata sync dag success", KR(ret), KPC(task_ctx));
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
    // change log level to debug later
    if (task_ctx->task_status_ == OB_TTL_TASK_CANCEL
        || task_ctx->task_status_ == OB_TTL_TASK_FINISH) {
      // do nothing, schedule next
      LOG_INFO("cancel current memdata sync task", KR(ret), KPC(task_ctx));
    } else if (task_ctx->task_status_ == OB_TTL_TASK_RUNNING) {
      // will schedule this
      if (task_ctx->err_code_ == OB_SUCCESS) {
        task_ctx->task_status_ = OB_TTL_TASK_FINISH;
        LOG_INFO("current memdata sync task finish", KR(ret), KPC(task_ctx));
        // task success, schedule next
      } else if (OB_PARTITION_NOT_EXIST == task_ctx->err_code_
                 || OB_PARTITION_IS_BLOCKED == task_ctx->err_code_
                 || OB_TABLE_NOT_EXIST == task_ctx->err_code_
                 || OB_ERR_UNKNOWN_TABLE == task_ctx->err_code_
                 || OB_LS_NOT_EXIST == task_ctx->err_code_
                 || OB_TABLET_NOT_EXIST == task_ctx->err_code_
                 || OB_REPLICA_NOT_READABLE == task_ctx->err_code_) {
        LOG_INFO("cancel current memdata sync task since partition state change", KR(ret), KPC(task_ctx));
        task_ctx->task_status_ = OB_TTL_TASK_CANCEL;
        // canceled, schedule next
      } else if (OB_ALLOCATE_MEMORY_FAILED == task_ctx->err_code_
                 || OB_ERR_VSAG_MEM_LIMIT_EXCEEDED == task_ctx->err_code_) {
        LOG_WARN("cancel current memdata sync task since out of resources", KR(ret), KPC(task_ctx));
        task_ctx->task_status_ = OB_TTL_TASK_CANCEL;
      } else { // retry
        LOG_WARN("current memdata sync task report error, will retry", KR(ret), KPC(task_ctx));
        task_ctx->task_status_ = OB_TTL_TASK_PREPARE; // reset ot prepare state, will rescheduler by timer or dag task
        task_ctx->failure_times_++;
        if (task_ctx->failure_times_ >= 3) {
          task_ctx->task_status_ = OB_TTL_TASK_CANCEL;
          LOG_WARN("current memdata sync task failed too many times, cancel it", KR(ret), KPC(task_ctx));
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
      K(tenant_id_), K(ls_->get_ls_id()), K(finished_count), K(total_count));
  } else {
    LOG_INFO("memdata sync task remaining",
      K(tenant_id_), K(ls_->get_ls_id()), K(finished_count), K(total_count));
  }
  return ret;
}

int ObPluginVectorIndexLoadScheduler::check_and_execute_adapter_maintenance_task(ObPluginVectorIndexMgr *&mgr)
{
  int ret = OB_SUCCESS;
  bool need_check = false;
  bool is_dirty = false;
  bool is_finished = false;
  // if schema version change, or exist partial adapter(create by access) need do maintenance
  if (OB_FAIL(check_schema_version())) {
    LOG_WARN("fail to check schema version", KR(ret));
  } else if (OB_NOT_NULL(mgr) && OB_FAIL(check_index_adpter_exist(mgr))) {
    LOG_WARN("fail to check exist paritial index adapter", KR(ret));
  } else if (local_tenant_task_.need_check_) {
    if (OB_FAIL(execute_adapter_maintenance())) {
      LOG_WARN("fail to generate tablet tasks", K_(tenant_id));
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

int ObPluginVectorIndexLoadScheduler::log_tablets_need_memdata_sync(ObPluginVectorIndexMgr *mgr)
{
  // Notice: only sync complete adapter, partial adapter will be merged to complete next timer schedule
  int ret = OB_SUCCESS;
  bool need_submit_log = false;
  if (OB_SUCC(ret)) {
    // lock to avoid concurrent modify tablet_id_array and tablet_id_array;
    common::ObSpinLockGuard ctx_guard(logging_lock_);
    if (is_logging_) {
      // do-nothing
      FLOG_INFO("vector index memdata sync is logging");
    } else {
      table_id_array_.reuse();
      tablet_id_array_.reuse();

      // follower just refresh adapter statistics, leader submit log need memdata sync

      FOREACH_X(iter, mgr->get_complete_adapter_map(), OB_SUCC(ret)) {
        ObPluginVectorIndexAdaptor *adapter = iter->second;
        bool need_sync = false;
        if (OB_ISNULL(adapter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null adapter", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
        } else if (iter->first != adapter->get_inc_tablet_id()) {
          // do nothing
        } else if (tablet_id_array_.count() >= ObVectorIndexSyncLogCb::VECTOR_INDEX_MAX_SYNC_COUNT) {
          // do nothing, wait for next schedule
        } else if (!need_refresh_ && OB_FAIL(adapter->check_need_sync_to_follower(need_sync))) {
          LOG_WARN("fail to check need memdata sync", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
        } else if ((need_refresh_ || need_sync) && is_leader_) {
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
  } else if (!need_submit_log) {
    // do nothing
  } else if (is_leader_) {
    if (need_refresh_) {
      if (OB_FAIL(mgr->get_mem_sync_info().add_task_to_waiting_map(mgr->get_complete_adapter_map()))) {
        TRANS_LOG(WARN, "fail to add complete adaptor to waiting map",KR(ret), K(tenant_id_));
      }
    } else if (OB_FAIL(submit_log_())) {
      TRANS_LOG(WARN, "fail to submit vector index memdata sync log",KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
    } else {
      TRANS_LOG(INFO, "submit vector index memdata sync log success", KR(ret), K(need_refresh_), K(tenant_id_), K(ls_->get_ls_id()));
    }
  } else if (!is_leader_ && need_refresh_) {
    if (OB_FAIL(mgr->get_mem_sync_info().add_task_to_waiting_map(mgr->get_complete_adapter_map()))) {
      TRANS_LOG(WARN, "fail to add complete adaptor to waiting map",KR(ret), K(tenant_id_));
    }
  }
  need_refresh_ = false;

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
      if (OB_FAIL(execute_one_memdata_sync_task(mgr, iter->second))) {
        LOG_WARN("fail to execute_one_memdata_sync_task", KR(ret), K(iter->first));
      }
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
  } else if (OB_FAIL(mgr->check_need_mem_data_sync_task(need_mem_data_sync))) {
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
      && !is_leader_) {
    // push all local tablet to sync candidate
    if (OB_FAIL(mgr->get_mem_sync_info().add_task_to_waiting_map(mgr->get_complete_adapter_map()))) {
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

int ObPluginVectorIndexLoadScheduler::check_and_execute_tasks()
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObPluginVectorIndexLoadScheduler::check_and_handle_event",
                    VEC_INDEX_LOAD_TIME_NORMAL_THRESHOLD);
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
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

  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(index_ls_mgr) && index_ls_mgr->get_ls_task_ctx().state_ != OB_TTL_TASK_RUNNING) {
    // do nothing, ToDo: change log level later
    LOG_INFO("not vector index schedular running",
      K(index_ls_mgr->get_ls_task_ctx().state_), K(ls_->get_ls_id()));
  } else {
    // Notice: index_ls_mgr maybe null
    // create / remove adapter, check need update & write mem sync log
    if (OB_FAIL(check_and_execute_adapter_maintenance_task(index_ls_mgr))) { // Tips: do merge
      LOG_WARN("fail to check and execute adapter maintenance task",
        KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
    }
    // Notice: leader write sync log, do memdata_sync only one loop(role changed from follower to leader)
    // explicit cover error code
    ret = OB_SUCCESS;
    // write tablets need memdata sync to clog
    if (OB_NOT_NULL(index_ls_mgr)
        && (current_memory_config_ != 0)
        && OB_FAIL(log_tablets_need_memdata_sync(index_ls_mgr))) { // Tips: check if need check to follower
      LOG_WARN("fail to log tablets need memdata sync", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
    }

    // explicit cover error code
    ret = OB_SUCCESS;
    // mem_sync task
    if (OB_NOT_NULL(index_ls_mgr) && OB_FAIL(check_and_execute_memdata_sync_task(index_ls_mgr))) {
      LOG_WARN("fail to check and execute memdata sync task",
        KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", KR(ret));
  } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
    // check ObMultiVersionSchemaService ready
    LOG_INFO("schema service not ready", KR(ret));
  } else if (ATOMIC_BCAS(&need_do_for_switch_, true, false)) {
    // reserved, do nothing
    LOG_INFO("switch leader", K(tenant_id_), K(ls_->get_ls_id()), K(is_leader_), K(is_stopped_));
  } else if (check_can_do_work()){
    if (OB_FAIL(check_tenant_memory())) {
      LOG_WARN("check vector index resource failed", KR(ret));
    } else if (OB_FAIL(reload_tenant_task())) {
      LOG_WARN("fail to reload tenant task", KR(ret));
    } else if (OB_FAIL(check_and_execute_tasks())) {
      LOG_WARN("fail to scan and handle all tenant event", KR(ret));
    }
  }
}

OB_SERIALIZE_MEMBER(ObVectorIndexSyncLog, flags_, tablet_id_array_, table_id_array_)

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
        KR(ret), KP(cb_.log_buffer_), K(pos));
    } else if (OB_FAIL(ls_log.serialize(cb_.log_buffer_,
                                        ObVectorIndexSyncLogCb::VECTOR_INDEX_SYNC_LOG_MAX_LENGTH,
                                        pos))) {
      TRANS_LOG(WARN, "ObVectorIndexSyncLog serialize vec index memdata sync log error",
        KR(ret), KP(cb_.log_buffer_), K(pos));
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
        KR(ret), KP(cb_.log_buffer_), K(pos));
    } else {
      is_logging_ = true;
      TRANS_LOG(INFO, "submit vector index memdata sync log success",
        K(tenant_id_), K(ls_->get_ls_id()), K(base_scn), K(lsn), K(scn));
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
  ObPluginVectorIndexMgr *mgr = nullptr;
  if (OB_FAIL(vector_index_service_->acquire_vector_index_mgr(ls_->get_ls_id(), mgr))) {
    LOG_WARN("fail to acquire vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else if (OB_FAIL(mgr->get_mem_sync_info().add_task_to_waiting_map(ls_log))){
    LOG_WARN("memdata sync failed to add task", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));
  }
  if (ret == OB_ALLOCATE_MEMORY_FAILED) {
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
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("vector index scheduler: finish to switch_to_follower", K_(tenant_id), KPC_(ls), K(cost_us));
}

int ObPluginVectorIndexLoadScheduler::safe_to_destroy(bool &is_safe)
{
  int ret = OB_SUCCESS;
  is_safe = true;
  int64_t dag_ref = get_dag_ref();
  if (0 != dag_ref) {
    if (REACH_TIME_INTERVAL(60L * 1000000)) {  // 60s
      LOG_WARN("vector index scheduler can't destroy", K(dag_ref));
    }
    is_safe = false;
  }
  return ret;
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

int64_t ObVectorIndexDag::hash() const
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

    while(!need_stop && OB_SUCC(ret)) {
      lib::ContextParam param;
      // use dag mtl id for param refer to TTLtask
      param.set_mem_attr(MTL_ID(), "VecIdxTaskCP", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
      CREATE_WITH_TEMP_CONTEXT(param) {
        if (OB_FAIL(process_one())) {
          LOG_WARN("fail to process one", KR(ret), K(ls_id_), KPC(task_ctx_));
        }
        ret = OB_SUCCESS; // continue to try schedular remainig tasks

        if (OB_FAIL(vec_idx_scheduler_->check_task_state(vec_idx_mgr_, task_ctx_, need_stop))) {
          LOG_WARN("fail to check task state", KR(ret), K(ls_id_), KPC(task_ctx_));
          ret = OB_SUCCESS; // cover memdata sync failure
        }
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

  if (OB_FAIL(ObPluginVectorIndexUtils::get_task_read_snapshot(ls_id_, read_snapshot_))) {
    LOG_WARN("memdata sync fail to get task read snapshot", KR(ret), K(ls_id_), KPC(task_ctx_));
  } else if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard(task_ctx_->index_tablet_id_, adpt_guard))) {
    LOG_WARN("memdata sync fail to get adapter instance", KR(ret), K(ls_id_), KPC(task_ctx_));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::refresh_memdata(ls_id_,
                                                               adpt_guard.get_adatper(),
                                                               read_snapshot_,
                                                               allocator_))) {
    LOG_WARN("memdata sync fail to refresh memdata", KR(ret), K(ls_id_), KPC(task_ctx_));
  }

  if (OB_SUCC(ret)) {
    task_ctx_->err_code_ = OB_SUCCESS;
    adpt_guard.get_adatper()->sync_finish();
    adpt_guard.get_adatper()->reset_sync_idle_count();
  } else {
    task_ctx_->err_code_ = ret;
    if (OB_NOT_NULL(adpt_guard.get_adatper())) {
      adpt_guard.get_adatper()->sync_finish();
      adpt_guard.get_adatper()->sync_fail();
      adpt_guard.get_adatper()->reset_sync_idle_count();
    }
  }

  int64_t cost = ObTimeUtil::current_time() - start_time;
  LOG_INFO("memdata sync finish process one", K(cost), K(allocator_.used()), K(allocator_.total()),
    K(ls_id_), KPC(task_ctx_));
  allocator_.reset();
  LOG_INFO("memdata sync check allocator use", K(allocator_.used()), K(allocator_.total()),
    K(ls_id_), KPC(task_ctx_));

  return ret;
}

int ObVectorIndexMemSyncInfo::init(int64_t hash_capacity, uint64_t tenant_id, ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(first_mem_sync_map_.create(hash_capacity, "VecIdxTaskMap", "VecIdxTaskMap", tenant_id))) {
    LOG_WARN("fail to create first mem sync set", K(tenant_id), K(ls_id), KR(ret));
  } else if (OB_FAIL(second_mem_sync_map_.create(hash_capacity, "VecIdxTaskMap", "VecIdxTaskMap", tenant_id))) {
    LOG_WARN("fail to create second mem sync set", K(ls_id), K(ls_id), KR(ret));
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
  // check all tasks in processing map finished or cancelled
  int ret = OB_SUCCESS;
  is_finished = false;
  uint32_t count = 0;

  VectorIndexMemSyncMap &current_task_map = get_processing_map();
  FOREACH(iter, current_task_map) {
    ObPluginVectorIndexTaskCtx *ctx = iter->second;
    if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memdata sync get null memdta_ctx", KPC(ctx));
    } else if (ctx->task_status_ == OB_TTL_TASK_FINISH // need a waiting state, maybe false finish
               || ctx->task_status_ == OB_TTL_TASK_CANCEL) {
      count++;
    }
  }

  finished_count = count;
  total_count = current_task_map.size();

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
    char *task_ctx_buf =
      static_cast<char *>(get_waiting_allocator().alloc(sizeof(ObPluginVectorIndexTaskCtx)));
    ObPluginVectorIndexTaskCtx* task_ctx = nullptr;
    ObPluginVectorIndexTaskCtx* tmp_task_ctx = nullptr;
    if (OB_ISNULL(task_ctx_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memdata sync fail to alloc task ctx", KR(ret));
    } else if (FALSE_IT(task_ctx = new(task_ctx_buf)ObPluginVectorIndexTaskCtx(tablet_id, table_id))) {
    } else if (OB_FAIL(waiting_task_map.get_refactored(tablet_id, tmp_task_ctx))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        if (OB_FAIL(waiting_task_map.set_refactored(tablet_id, task_ctx))) {
          LOG_WARN("memdata sync failed to set vector index task ctx", K(ret), K(tablet_id), KPC(task_ctx));
        } else {
          LOG_INFO("memdata sync success get replay vector index task ctx", K(ret), K(tablet_id), KPC(task_ctx));
        }
      }
    } else { // // task already set, not scheduled
      LOG_INFO("memdata sync duplicate vector index task ctx", K(ret), K(tablet_id), KPC(task_ctx));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(task_ctx)) {
      task_ctx->~ObPluginVectorIndexTaskCtx();
      get_waiting_allocator().free(task_ctx); // arena allocator not really free
      task_ctx = nullptr;
    }
  }
  return ret;
}

int ObVectorIndexMemSyncInfo::add_task_to_waiting_map(VectorIndexAdaptorMap &adapter_map)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard ctx_guard(switch_lock_); // prevent context switch in maintance thread
  VectorIndexMemSyncMap &current_map = get_processing_map();
  FOREACH(iter, adapter_map) {
    // only use complete adapter, tablet id of no.3 aux index table
    ObPluginVectorIndexAdaptor *adapter = iter->second;
    ObTabletID tablet_id = iter->first;
    if (tablet_id == adapter->get_inc_tablet_id()) {
      char *task_ctx_buf =
        static_cast<char *>(get_processing_allocator().alloc(sizeof(ObPluginVectorIndexTaskCtx)));
      ObPluginVectorIndexTaskCtx* task_ctx = nullptr;
      if (OB_ISNULL(task_ctx_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memdata sync fail to alloc task ctx", KR(ret));
      } else if (FALSE_IT(task_ctx = new(task_ctx_buf)ObPluginVectorIndexTaskCtx(tablet_id, adapter->get_inc_table_id()))) {
      } else if (OB_FAIL(current_map.set_refactored(tablet_id, task_ctx))) {
        LOG_WARN("memdata sync failed to set vector index task ctx", K(ret), K(tablet_id), KPC(task_ctx));
      } else {
        LOG_INFO("memdata sync success set force index task ctx", K(ret), K(tablet_id), KPC(task_ctx));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(task_ctx)) {
        task_ctx->~ObPluginVectorIndexTaskCtx();
        get_processing_allocator().free(task_ctx);
        task_ctx = nullptr;
      }
    }
  }
  return ret;
}

void ObVectorIndexMemSyncInfo::check_and_switch_if_needed(bool &need_sync, bool &all_finished)
{
  // only called in maintance thread.
  if (get_processing_map().size() > 0) {
    if (all_finished) {
      // current processing task all finished;
      get_processing_map().reuse();
      get_processing_allocator().reset();
      all_finished = false;
      LOG_INFO("memdata sync release processing set",
        K(processing_first_mem_sync_), K(get_processing_map().size()), K(get_waiting_map().size()));
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
