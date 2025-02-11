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

#include "ob_table_load_service.h"
#include "observer/omt/ob_tenant.h"
#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_coordinator_ctx.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "rootserver/ob_partition_exchange.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace lib;
using namespace rootserver;
using namespace share::schema;
using namespace storage;
using namespace table;
using namespace omt;

/**
 * ObCheckTenantTask
 */

void ObTableLoadService::ObCheckTenantTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("table load check tenant");
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check_tenant", KR(ret));
    // abort all client task
    service_.abort_all_client_task(ret);
    // fail all current tasks
    service_.fail_all_ctx(ret);
  }
}

/**
 * ObHeartBeatTask
 */

void ObTableLoadService::ObHeartBeatTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("table load heart beat");
  ObTableLoadManager &manager = service_.get_manager();
  ObArray<ObTableLoadTableCtx *> table_ctx_array;
  table_ctx_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(manager.get_all_table_ctx(table_ctx_array))) {
    LOG_WARN("fail to get all table ctx", KR(ret));
  }
  for (int64_t i = 0; i < table_ctx_array.count(); ++i) {
    ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
    if (nullptr != table_ctx->coordinator_ctx_
        && table_ctx->coordinator_ctx_->enable_heart_beat()) {
      ObTableLoadCoordinator coordinator(table_ctx);
      if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      } else if (OB_FAIL(coordinator.heart_beat())) {
        LOG_WARN("fail to coordinator heart beat", KR(ret));
      }
    }
    manager.revert_table_ctx(table_ctx);
  }
}

/**
 * ObGCTask
 */

void ObTableLoadService::ObGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("table load start gc");
  ObTableLoadManager &manager = service_.get_manager();
  ObArray<ObTableLoadTableCtx *> table_ctx_array;
  table_ctx_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(manager.get_all_table_ctx(table_ctx_array))) {
    LOG_WARN("fail to get all  table ctx", KR(ret));
  }
  for (int64_t i = 0; i < table_ctx_array.count(); ++i) {
    ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
    if (gc_mark_delete(table_ctx)) {
    } else if (gc_heart_beat_expired_ctx(table_ctx)) {
    } else if (gc_table_not_exist_ctx(table_ctx)) {
    }
    manager.revert_table_ctx(table_ctx);
  }
}

bool ObTableLoadService::ObGCTask::gc_mark_delete(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  bool is_removed = false;
  if (OB_ISNULL(table_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table ctx is null", KR(ret));
    is_removed = true;
  } else {
    const uint64_t table_id = table_ctx->param_.table_id_;
    const int64_t task_id = table_ctx->ddl_param_.task_id_;
    const uint64_t dest_table_id = table_ctx->ddl_param_.dest_table_id_;
    // check if table ctx is removed
    if (!table_ctx->is_in_map()) {
      LOG_DEBUG("table ctx is removed", K(table_id), K(task_id), K(dest_table_id),
                "ref_count", table_ctx->get_ref_count());
      is_removed = true;
    }
    // check is mark delete
    else if (table_ctx->is_mark_delete()) {
      if (table_ctx->is_stopped() && OB_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
        LOG_WARN("fail to remove table ctx", KR(ret), K(table_id), K(task_id), K(dest_table_id));
      }
      is_removed = true; // skip other gc
    }
  }
  return is_removed;
}

bool ObTableLoadService::ObGCTask::gc_heart_beat_expired_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  bool is_removed = false;
  if (OB_ISNULL(table_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table ctx is null", KR(ret));
    is_removed = true;
  } else {
    const uint64_t table_id = table_ctx->param_.table_id_;
    const int64_t task_id = table_ctx->ddl_param_.task_id_;
    const uint64_t dest_table_id = table_ctx->ddl_param_.dest_table_id_;
    // check if table ctx is removed
    if (!table_ctx->is_in_map()) {
      LOG_DEBUG("table ctx is removed", K(table_id), K(task_id), K(dest_table_id),
                "ref_count", table_ctx->get_ref_count());
      is_removed = true;
    }
    // check if heart beat expired, ignore coordinator
    else if (nullptr == table_ctx->coordinator_ctx_ && nullptr != table_ctx->store_ctx_) {
      if (OB_UNLIKELY(
            table_ctx->store_ctx_->check_heart_beat_expired(HEART_BEEAT_EXPIRED_TIME_US))) {
        FLOG_INFO("store heart beat expired, abort", K(table_id), K(task_id), K(dest_table_id));
        bool is_stopped = false;
        ObTableLoadStore::abort_ctx(table_ctx, is_stopped);
        table_ctx->mark_delete();
        if (is_stopped && OB_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
          LOG_WARN("fail to remove table ctx", KR(ret), K(table_id), K(task_id), K(dest_table_id));
        }
        is_removed = true; // skip other gc
      }
    }
  }
  return is_removed;
}

bool ObTableLoadService::ObGCTask::gc_table_not_exist_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  bool is_removed = false;
  if (OB_ISNULL(table_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table ctx is null", KR(ret));
    is_removed = true;
  } else {
    const uint64_t table_id = table_ctx->param_.table_id_;
    const uint64_t hidden_table_id = table_ctx->ddl_param_.dest_table_id_;
    // check if table ctx is removed
    if (!table_ctx->is_in_map()) {
      LOG_DEBUG("table ctx is removed", K(table_id), "ref_count", table_ctx->get_ref_count());
      is_removed = true;
    }
    // check if table ctx is activated
    else if (table_ctx->get_ref_count() > 2) {
      LOG_DEBUG("table load ctx is active", K(table_id), "ref_count", table_ctx->get_ref_count());
    }
    // check if table ctx can be recycled
    else {
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id, hidden_table_id, schema_guard,
                                                      table_schema))) {
        if (OB_UNLIKELY(OB_TABLE_NOT_EXIST != ret && OB_TENANT_NOT_EXIST != ret)) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(hidden_table_id));
        } else {
          LOG_INFO("hidden table not exist, gc table load ctx", K(table_id), K(hidden_table_id));
          ObTableLoadService::remove_ctx(table_ctx);
          is_removed = true;
        }
      } else if (table_schema->is_in_recyclebin()) {
        LOG_INFO("hidden table is in recyclebin, gc table load ctx", K(table_id),
                 K(hidden_table_id));
        ObTableLoadService::remove_ctx(table_ctx);
        is_removed = true;
      } else {
        LOG_DEBUG("table load ctx is running", K(table_id), K(hidden_table_id));
      }
    }
  }
  return is_removed;
}

/**
 * ObReleaseTask
 */

void ObTableLoadService::ObReleaseTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("table load start release");
  service_.manager_.gc_table_ctx_in_list();
  service_.manager_.gc_client_task_in_list();
}

/**
 * ObClientTaskAutoAbortTask
 */

void ObTableLoadService::ObClientTaskAutoAbortTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("table load auto abort client task");
  ObArray<ObTableLoadClientTask *> client_task_array;
  client_task_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(service_.manager_.get_all_client_task(client_task_array))) {
    LOG_WARN("fail to get all client task", KR(ret));
  } else {
    for (int64_t i = 0; i < client_task_array.count(); ++i) {
      ObTableLoadClientTask *client_task = client_task_array.at(i);
      if (OB_UNLIKELY(ObTableLoadClientStatus::ERROR == client_task->get_status() ||
                      client_task->check_status() != OB_SUCCESS)) {
        client_task->abort();
      }
      service_.manager_.revert_client_task(client_task);
    }
  }
}

/**
 * ObClientTaskPurgeTask
 */

void ObTableLoadService::ObClientTaskPurgeTask::runTimerTask()
{
  LOG_DEBUG("table load purge client task");
  purge_client_task();
  purge_client_task_brief();
}

int ObTableLoadService::ObClientTaskPurgeTask::add_client_task_brief(
  ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(client_task));
  } else {
    ObTableLoadUniqueKey key(client_task->get_table_id(), client_task->get_task_id());
    ObTableLoadClientTaskBrief *client_task_brief = nullptr;
    if (OB_FAIL(service_.manager_.acquire_client_task_brief(client_task_brief))) {
      LOG_WARN("fail to acquire client task brief", KR(ret));
    } else {
      client_task_brief->task_id_ = client_task->get_task_id();
      client_task_brief->table_id_ = client_task->get_table_id();
      client_task->get_status(client_task_brief->client_status_, client_task_brief->error_code_);
      client_task_brief->result_info_ = client_task->get_result_info();
      client_task_brief->active_time_ = ObTimeUtil::current_time();
      if (OB_FAIL(service_.manager_.add_client_task_brief(key, client_task_brief))) {
        LOG_WARN("fail to add client task brief", KR(ret));
      }
    }
    if (nullptr != client_task_brief) {
      service_.manager_.revert_client_task_brief(client_task_brief);
      client_task_brief = nullptr;
    }
  }
  return ret;
}

void ObTableLoadService::ObClientTaskPurgeTask::purge_client_task()
{
  int ret = OB_SUCCESS;
  ObArray<ObTableLoadClientTask *> client_task_array;
  client_task_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(service_.manager_.get_all_client_task(client_task_array))) {
    LOG_WARN("fail to get all client task", KR(ret));
  }
  for (int64_t i = 0; i < client_task_array.count(); ++i) {
    ObTableLoadClientTask *client_task = client_task_array.at(i);
    ObTableLoadUniqueKey key(client_task->get_table_id(), client_task->get_task_id());
    ObTableLoadClientStatus client_status = client_task->get_status();
    if (client_status != ObTableLoadClientStatus::COMMIT &&
        client_status != ObTableLoadClientStatus::ABORT) {
      // ignore
    }
    // remove client task
    else if (OB_FAIL(service_.manager_.remove_client_task(key, client_task))) {
      LOG_WARN("fail to remove client task", KR(ret), K(key), KPC(client_task));
    }
    // add client task brief
    else if (OB_FAIL(add_client_task_brief(client_task))) {
      LOG_WARN("fail to add client task brief", KR(ret));
    }
    service_.manager_.revert_client_task(client_task);
  }
}

void ObTableLoadService::ObClientTaskPurgeTask::purge_client_task_brief()
{
  int ret = OB_SUCCESS;
  const int64_t expired_ts = ObTimeUtil::current_time() - CLIENT_TASK_BRIEF_RETENTION_PERIOD;
  ObArray<ObTableLoadClientTaskBrief *> client_task_brief_array;
  client_task_brief_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(service_.manager_.get_all_client_task_brief(client_task_brief_array))) {
    LOG_WARN("fail to get all client task brief", KR(ret));
  }
  for (int64_t i = 0; i < client_task_brief_array.count(); ++i) {
    ObTableLoadClientTaskBrief *brief = client_task_brief_array.at(i);
    if (brief->active_time_ >= expired_ts) {
      // ignore
    } else {
      ObTableLoadUniqueKey key(brief->table_id_, brief->task_id_);
      if (OB_FAIL(service_.manager_.remove_client_task_brief(key, brief))) {
        LOG_WARN("fail to remove client task brief", KR(ret), K(key), KPC(brief));
      }
    }
    service_.manager_.revert_client_task_brief(brief);
  }
}

/**
 * ObTableLoadService
 */

int ObTableLoadService::mtl_new(ObTableLoadService *&service)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObMemAttr attr(tenant_id, ObModIds::OMT_TENANT);
  service = OB_NEW(ObTableLoadService, attr, tenant_id);
  if (OB_ISNULL(service)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(tenant_id));
  }
  return ret;
}

void ObTableLoadService::mtl_destroy(ObTableLoadService *&service)
{
  if (OB_UNLIKELY(nullptr == service)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "meta mem mgr is nullptr", KP(service));
  } else {
    OB_DELETE(ObTableLoadService, ObModIds::OMT_TENANT, service);
    service = nullptr;
  }
}

int ObTableLoadService::check_tenant()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObTenant *tenant = nullptr;
  if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", KR(ret), K(tenant_id));
  } else if (tenant->get_unit_status() == ObUnitInfoGetter::ObUnitStatus::UNIT_MARK_DELETING
    || tenant->get_unit_status() == ObUnitInfoGetter::ObUnitStatus::UNIT_WAIT_GC_IN_OBSERVER
    || tenant->get_unit_status() == ObUnitInfoGetter::ObUnitStatus::UNIT_DELETING_IN_OBSERVER) {
    ret = OB_EAGAIN;
    LOG_WARN("unit is migrate out, should retry direct load", KR(ret), K(tenant->get_unit_status()));
  } else if (OB_UNLIKELY(ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL !=
                         tenant->get_unit_status() && ObUnitInfoGetter::ObUnitStatus::UNIT_MIGRATE_OUT != tenant->get_unit_status())) {
    ret = OB_ERR_UNEXPECTED_UNIT_STATUS;
    LOG_WARN("unit status not normal", KR(ret), K(tenant->get_unit_status()));
  }
  return ret;
}

int ObTableLoadService::check_support_direct_load(
    const uint64_t table_id,
    const ObDirectLoadMethod::Type method,
    const ObDirectLoadInsertMode::Type insert_mode,
    const ObDirectLoadMode::Type load_mode,
    const ObDirectLoadLevel::Type load_level,
    const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(
          ObTableLoadSchema::get_table_schema(tenant_id, table_id, schema_guard, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
    } else {
      ret = check_support_direct_load(schema_guard, table_schema, method, insert_mode, load_mode, load_level, column_ids);
    }
  }
  return ret;
}

int ObTableLoadService::check_support_direct_load(ObSchemaGetterGuard &schema_guard,
                                                  uint64_t table_id,
                                                  const ObDirectLoadMethod::Type method,
                                                  const ObDirectLoadInsertMode::Type insert_mode,
                                                  const ObDirectLoadMode::Type load_mode,
                                                  const ObDirectLoadLevel::Type load_level,
                                                  const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema is null", KR(ret));
    } else {
      ret = check_support_direct_load(schema_guard, table_schema, method, insert_mode, load_mode, load_level, column_ids);
    }
  }
  return ret;
}

static const char *InsertOverwritePrefix = "insert overwrite with ";
static const char *EmptyPrefix = "";
int ObTableLoadService::check_support_direct_load(ObSchemaGetterGuard &schema_guard,
                                                  const ObTableSchema *table_schema,
                                                  const ObDirectLoadMethod::Type method,
                                                  const ObDirectLoadInsertMode::Type insert_mode,
                                                  const ObDirectLoadMode::Type load_mode,
                                                  const ObDirectLoadLevel::Type load_level,
                                                  const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_schema ||
                  !ObDirectLoadMethod::is_type_valid(method) ||
                  !ObDirectLoadInsertMode::is_type_valid(insert_mode) ||
                  !ObDirectLoadMode::is_type_valid(load_mode) ||
                  !ObDirectLoadLevel::is_type_valid(load_level) ||
                  column_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema), K(method), K(insert_mode), K(load_mode),
             K(load_level), K(column_ids));
  } else {
    const uint64_t tenant_id = MTL_ID();
    uint64_t compat_version = 0;
    bool trigger_enabled = false;
    bool has_fts_index = false;
    bool has_multivalue_index = false;
    bool has_non_normal_local_index = false;
    bool is_heap_table_with_single_unique_index = false;
    // check if it is a user table
    const char *tmp_prefix = ObDirectLoadMode::is_insert_overwrite(load_mode) ? InsertOverwritePrefix : EmptyPrefix;

    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
    } else if (!table_schema->is_user_table()) {
      ret = OB_NOT_SUPPORTED;
      if (lib::is_oracle_mode() && table_schema->is_tmp_table()) {
        LOG_WARN("direct-load does not support oracle temporary table", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support oracle temporary table", tmp_prefix);
      } else if (table_schema->is_view_table()) {
        LOG_WARN("direct-load does not support view table", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support view table", tmp_prefix);
      } else if (table_schema->is_mlog_table()) {
        LOG_WARN("direct-load does not support materialized view log table", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support materialized view log table", tmp_prefix);
      } else {
        LOG_WARN("direct-load does not support non-user table", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support non-user table", tmp_prefix);
      }
    }
    // check if exists full-text search index
    else if (OB_FAIL(table_schema->check_has_fts_index(schema_guard, has_fts_index))) {
      LOG_WARN("fail to check has full-text search index", K(ret));
    } else if (has_fts_index) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table has full-text search index", KR(ret));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has full-text search index", tmp_prefix);
    }
    // check if the trigger is enabled
    else if (OB_FAIL(table_schema->check_has_trigger_on_table(schema_guard, trigger_enabled))) {
      LOG_WARN("failed to check has trigger in table", KR(ret));
    } else if (trigger_enabled) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table with trigger enabled", KR(ret), K(trigger_enabled));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table with trigger enabled", tmp_prefix);
    }
    // check if table has mlog
    else if (table_schema->required_by_mview_refresh() && !table_schema->mv_container_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table required by materialized view refresh", KR(ret));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table required by materialized view refresh", tmp_prefix);
    }
    // check for columns
    else if (OB_FAIL(check_support_direct_load_for_columns(table_schema, load_mode))) {
      LOG_WARN("fail to check support direct load for columns", KR(ret));
    }
    // check for default value
    else if (OB_FAIL(check_support_direct_load_for_default_value(table_schema, column_ids))) {
      LOG_WARN("fail to check support direct load for default value", KR(ret), K(column_ids));
    }
    // check for partition level
    else if (ObDirectLoadLevel::PARTITION == load_level
             && OB_FAIL(check_support_direct_load_for_partition_level(schema_guard,
                                                                      table_schema,
                                                                      method,
                                                                      compat_version))) {
      LOG_WARN("fail to check support direct load for partition level", KR(ret));
    }
    // check insert overwrite
    else if (ObDirectLoadMode::is_insert_overwrite(load_mode)
             && compat_version < DATA_VERSION_4_3_2_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("version lower than 4.3.2.0 does not support insert overwrite", KR(ret));
      FORWARD_USER_ERROR_MSG(ret, "version lower than 4.3.2.0 does not support insert overwrite");
    }
    // incremental direct-load
    else if (ObDirectLoadMethod::is_incremental(method)) {
      if (GCTX.is_shared_storage_mode()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("in share storage mode, using incremental direct-load is not supported", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "in share storage mode, using incremental direct-load is not supported");
      } else if (!ObDirectLoadInsertMode::is_valid_for_incremental_method(insert_mode)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("using incremental direct-load without inc_replace or normal is not supported", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "using incremental direct-load without inc_replace or normal is not supported");
      } else if (compat_version < DATA_VERSION_4_3_1_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("version lower than 4.3.1.0 does not support incremental direct-load", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "version lower than 4.3.1.0 does not support incremental direct-load");
      } else if (table_schema->get_simple_index_infos().count() > 0 &&
                 OB_FAIL(ObTableLoadSchema::check_has_non_local_index(
                   schema_guard, table_schema, has_non_normal_local_index))) {
        LOG_WARN("fail to check support direct load for local index", KR(ret));
      } else if (table_schema->get_simple_index_infos().count() > 0 &&
                 OB_FAIL(ObTableLoadSchema::check_is_heap_table_with_single_unique_index(
                   schema_guard, table_schema, is_heap_table_with_single_unique_index))) {
        LOG_WARN("fail to check support direct load for heap table with single local unique index", KR(ret));
      } else if (table_schema->get_simple_index_infos().count() > 0 && compat_version < DATA_VERSION_4_3_4_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN(
          "version lower than 4.3.4.0 incremental direct-load does not support table with index",
          KR(ret));
        FORWARD_USER_ERROR_MSG(
          ret,
          "version lower than 4.3.4.0 incremental direct-load does not support table with index");
      } else if (has_non_normal_local_index && compat_version < DATA_VERSION_4_3_5_1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN(
          "version lower than 4.3.5.1 incremental direct-load only support table with "
          "normal local index",
          KR(ret));
        FORWARD_USER_ERROR_MSG(
          ret,
          "version lower than 4.3.5.1 incremental direct-load only support table with "
          "normal local index");
      } else if (has_non_normal_local_index && !is_heap_table_with_single_unique_index) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN(
          "unsupported index type exists, "
          "incremental direct-load does only support "
          "normal local index or "
          "single local unique index in heap table",
          KR(ret));
        FORWARD_USER_ERROR_MSG(ret,
                               "unsupported index type exists, "
                               "incremental direct-load does only support "
                               "normal local index or "
                               "single local unique index in heap table");
      } else if (table_schema->get_foreign_key_infos().count() > 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("incremental direct-load does not support table with foreign keys", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "incremental direct-load does not support table with foreign keys");
      } else if (table_schema->has_check_constraint() &&
                 (ObDirectLoadMode::LOAD_DATA == load_mode ||
                  ObDirectLoadMode::TABLE_LOAD == load_mode)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("incremental direct-load does not support table with check constraints", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "incremental direct-load does not support table with check constraints");
      }
    }
    // full direct-load
    else if (ObDirectLoadMethod::is_full(method)) {
      if (OB_UNLIKELY(!ObDirectLoadInsertMode::is_valid_for_full_method(insert_mode))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected insert mode for full direct-load", KR(ret), K(method), K(insert_mode));
      } else if (ObDirectLoadInsertMode::OVERWRITE == insert_mode
                 && compat_version < DATA_VERSION_4_3_1_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("version lower than 4.3.1.0 does not support insert overwrite mode", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "version lower than 4.3.1.0 does not support insert overwrite mode");
      }
    }
  }
  return ret;
}

int ObTableLoadService::check_support_direct_load_for_columns(
    const ObTableSchema *table_schema,
    const ObDirectLoadMode::Type load_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_schema || !ObDirectLoadMode::is_type_valid(load_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema), K(load_mode));
  } else {
    const char *tmp_prefix = ObDirectLoadMode::is_insert_overwrite(load_mode) ? InsertOverwritePrefix : EmptyPrefix;
    const bool is_px_mode = ObDirectLoadMode::is_px_mode(load_mode);
    for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
         OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
      ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", KR(ret), KP(column_schema));
      } else if (column_schema->is_unused()) {
        // 快速删除列, 仍然需要写宏块, 直接填null
        // TODO : udt类型SQL写入的列数与存储层列数不匹配, 暂时先不做支持
        if (column_schema->is_xmltype()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("direct-load does not support table has drop xmltype column instant", KR(ret), KPC(column_schema));
          FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has drop xmltype column instant", tmp_prefix);
        } else if (column_schema->get_udt_set_id() > 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("direct-load does not support table has drop udt column instant", KR(ret), KPC(column_schema));
          FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has drop udt column instant", tmp_prefix);
        }
      } else if (column_schema->is_generated_column()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct-load does not support table has generated column", KR(ret), KPC(column_schema));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has generated column", tmp_prefix);
      } else if (column_schema->get_meta_type().is_null()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct-load does not support table has null column", KR(ret), KPC(column_schema));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has null column", tmp_prefix);
      } else if (!is_px_mode && column_schema->is_geometry()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct-load does not support table has geometry column", KR(ret), KPC(column_schema));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has geometry column", tmp_prefix);
      } else if (column_schema->is_roaringbitmap()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct-load does not support table has roaringbitmap column", KR(ret), KPC(column_schema));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has roaringbitmap column", tmp_prefix);
      } else if (column_schema->is_xmltype()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct-load does not support table has xmltype column", KR(ret), KPC(column_schema));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has xmltype column", tmp_prefix);
      } else if (column_schema->get_udt_set_id() > 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct-load does not support table has udt column", KR(ret), KPC(column_schema));
        FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has udt column", tmp_prefix);
      }
    }
  }
  return ret;
}

int ObTableLoadService::check_support_direct_load_for_default_value(
    const ObTableSchema *table_schema,
    const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_schema || column_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema), K(column_ids));
  } else {
    ObArray<ObColDesc> column_descs;
    if (OB_FAIL(table_schema->get_column_ids(column_descs, true/*no_virtual*/))) {
      STORAGE_LOG(WARN, "fail to get column descs", KR(ret), KPC(table_schema));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
      const ObColDesc &col_desc = column_descs.at(i);
      bool found_column = ObColumnSchemaV2::is_hidden_pk_column_id(col_desc.col_id_);
      for (int64_t j = 0; !found_column && j < column_ids.count(); ++j) {
        if (col_desc.col_id_ == column_ids.at(j)) {
          found_column = true;
        }
      }
      if (!found_column) {
        const ObColumnSchemaV2 *column_schema = table_schema->get_column_schema(col_desc.col_id_);
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema", KR(ret), K(col_desc));
        }
        // 快速删除列
        // 对于insert into, sql会填充null
        // 对于load data和java api, 用户无法指定被删除的列写数据, 旁路导入在类型转换时直接填充null
        else if (column_schema->is_unused()) {
        }
        // 自增列
        else if (column_schema->is_autoincrement() || column_schema->is_identity_column()) {
        }
        // 默认值是表达式
        else if (OB_UNLIKELY(lib::is_mysql_mode() && column_schema->get_cur_default_value().is_ext())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("direct-load does not support column default value is ext", KR(ret), KPC(column_schema));
          FORWARD_USER_ERROR_MSG(ret, "direct-load does not support column default value is ext");
        } else if (OB_UNLIKELY(lib::is_oracle_mode() && column_schema->is_default_expr_v2_column())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("direct-load does not support column default value is expr", KR(ret), KPC(column_schema));
          FORWARD_USER_ERROR_MSG(ret, "direct-load does not support column default value is expr");
        }
        // 没有默认值, 且为NOT NULL
        // 例外:枚举类型默认为第一个
        else if (OB_UNLIKELY(column_schema->is_not_null_for_write() &&
                             column_schema->get_cur_default_value().is_null() &&
                             !column_schema->get_meta_type().is_enum())) {
          ret = OB_ERR_NO_DEFAULT_FOR_FIELD;
          LOG_WARN("column doesn't have a default value", KR(ret), KPC(column_schema));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadService::check_support_direct_load_for_partition_level(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *table_schema,
    const ObDirectLoadMethod::Type method,
    const uint64_t compat_version)
{
  int ret = OB_SUCCESS;
  if (ObDirectLoadMethod::is_incremental(method)) {
    // do nothing
  } else if (ObDirectLoadMethod::is_full(method)) {
    if (OB_FAIL(ObPartitionExchange::check_exchange_partition_for_direct_load(
        schema_guard, table_schema, compat_version))) {
      LOG_WARN("fail to check exchange partition", KR(ret), KPC(table_schema), K(compat_version));
    }
  }
  return ret;
}

int ObTableLoadService::alloc_ctx(ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (OB_UNLIKELY(service->is_stop_)) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("service is stop", KR(ret));
  } else if (OB_FAIL(service->get_manager().acquire_table_ctx(table_ctx))) {
    LOG_WARN("fail to acquire table ctx", KR(ret));
  }
  return ret;
}
void ObTableLoadService::free_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    service->get_manager().release_table_ctx(table_ctx);
  }
}

int ObTableLoadService::add_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (OB_UNLIKELY(service->is_stop_)) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("service is stop", KR(ret));
  } else {
    ObTableLoadUniqueKey key(table_ctx->param_.table_id_, table_ctx->ddl_param_.task_id_);
    ret = service->get_manager().add_table_ctx(key, table_ctx);
  }
  return ret;
}

int ObTableLoadService::remove_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadResourceReleaseArg release_arg;
    release_arg.tenant_id_ = MTL_ID();
    release_arg.task_key_ = ObTableLoadUniqueKey(table_ctx->param_.table_id_, table_ctx->ddl_param_.task_id_);
    if (OB_FAIL(service->get_manager().remove_table_ctx(release_arg.task_key_, table_ctx))) {
      LOG_WARN("fail to remove_table_ctx", KR(ret), K(release_arg.task_key_));
    } else {
      if (table_ctx->is_assigned_memory()) {
        if (OB_TMP_FAIL(service->assigned_memory_manager_.recycle_memory(table_ctx->param_.task_need_sort_, table_ctx->param_.avail_memory_))) {
          LOG_WARN("fail to recycle_memory", KR(tmp_ret), K(release_arg.task_key_));
        }
        table_ctx->reset_assigned_memory();
      }
      if (table_ctx->is_assigned_resource()) {
        if (OB_TMP_FAIL(ObTableLoadService::delete_assigned_task(release_arg))) {
          LOG_WARN("fail to delete assigned task", KR(tmp_ret), K(release_arg));
        }
        table_ctx->reset_assigned_resource();
      }
    }
  }
  return ret;
}

int ObTableLoadService::get_ctx(const ObTableLoadUniqueKey &key, ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->get_manager().get_table_ctx(key, table_ctx);
  }
  return ret;
}

void ObTableLoadService::put_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    service->get_manager().revert_table_ctx(table_ctx);
  }
}

ObTableLoadService::ObTableLoadService(const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    manager_(tenant_id),
    check_tenant_task_(*this),
    heart_beat_task_(*this),
    gc_task_(*this),
    release_task_(*this),
    client_task_auto_abort_task_(*this),
    client_task_purge_task_(*this),
    is_stop_(false),
    is_inited_(false)
{
}

int ObTableLoadService::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService init twice", KR(ret), KP(this));
  } else if (OB_FAIL(manager_.init())) {
    LOG_WARN("fail to init table ctx manager", KR(ret));
  } else if (OB_FAIL(assigned_memory_manager_.init())) {
    LOG_WARN("fail to init assigned memory manager", KR(ret));
  } else if (OB_FAIL(assigned_task_manager_.init())) {
    LOG_WARN("fail to init assigned task manager", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(timer_.set_run_wrapper_with_ret(MTL_CTX()))) {
      LOG_WARN("fail to set gc timer's run wrapper", KR(ret));
    } else if (OB_FAIL(timer_.init("TLD_Timer", ObMemAttr(MTL_ID(), "TLD_TIMER")))) {
      LOG_WARN("fail to init gc timer", KR(ret));
    } else if (OB_FAIL(timer_.schedule(check_tenant_task_, CHECK_TENANT_INTERVAL, true))) {
      LOG_WARN("fail to schedule check tenant task", KR(ret));
    } else if (OB_FAIL(timer_.schedule(heart_beat_task_, HEART_BEEAT_INTERVAL, true))) {
      LOG_WARN("fail to schedule heart beat task", KR(ret));
    } else if (OB_FAIL(timer_.schedule(gc_task_, GC_INTERVAL, true))) {
      LOG_WARN("fail to schedule gc task", KR(ret));
    } else if (OB_FAIL(timer_.schedule(release_task_, RELEASE_INTERVAL, true))) {
      LOG_WARN("fail to schedule release task", KR(ret));
    } else if (OB_FAIL(timer_.schedule(client_task_auto_abort_task_,
                                       CLIENT_TASK_AUTO_ABORT_INTERVAL, true))) {
      LOG_WARN("fail to schedule client task auto abort task", KR(ret));
    } else if (OB_FAIL(
                 timer_.schedule(client_task_purge_task_, CLIENT_TASK_PURGE_INTERVAL, true))) {
      LOG_WARN("fail to schedule client task purge task", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadService::stop()
{
  int ret = OB_SUCCESS;
  is_stop_ = true;
  timer_.stop();
  return ret;
}

void ObTableLoadService::wait()
{
  timer_.wait();
  release_all_ctx();
}

void ObTableLoadService::destroy()
{
  is_inited_ = false;
  timer_.destroy();
}

void ObTableLoadService::abort_all_client_task(int error_code)
{
  int ret = OB_SUCCESS;
  ObArray<ObTableLoadClientTask *> client_task_array;
  client_task_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(manager_.get_all_client_task(client_task_array))) {
    LOG_WARN("fail to get all client task", KR(ret));
  } else {
    for (int i = 0; i < client_task_array.count(); ++i) {
      ObTableLoadClientTask *client_task = client_task_array.at(i);
      client_task->abort(error_code);
      manager_.revert_client_task(client_task);
    }
  }
}

void ObTableLoadService::fail_all_ctx(int error_code)
{
  int ret = OB_SUCCESS;
  ObArray<ObTableLoadTableCtx *> table_ctx_array;
  bool is_stopped = false;
  table_ctx_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(manager_.get_all_table_ctx(table_ctx_array))) {
    LOG_WARN("fail to get all table ctx list", KR(ret));
  } else {
    for (int i = 0; i < table_ctx_array.count(); ++i) {
      ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
      // fail coordinator
      if (nullptr != table_ctx->coordinator_ctx_) {
        table_ctx->coordinator_ctx_->set_status_error(error_code);
        ObTableLoadStore::abort_ctx(table_ctx, is_stopped);
      }
      // fail store
      if (nullptr != table_ctx->store_ctx_) {
        table_ctx->store_ctx_->set_status_error(error_code);
        ObTableLoadStore::abort_ctx(table_ctx, is_stopped);
      }
      manager_.revert_table_ctx(table_ctx);
    }
  }
}

void ObTableLoadService::release_all_ctx()
{
  int ret = OB_SUCCESS;
  // 1. check all obj removed
  bool all_removed = false;
  do {
    // 通知后台线程快速退出
    abort_all_client_task(OB_CANCELED);
    fail_all_ctx(OB_CANCELED);

    // 移除对象
    manager_.remove_inactive_table_ctx();
    manager_.remove_inactive_client_task();
    manager_.remove_all_client_task_brief();

    manager_.check_all_obj_removed(all_removed);
    if (!all_removed) {
      ob_usleep(100_ms);
    }
  } while (!all_removed);
  // 2. check all obj released
  bool all_released = false;
  do {
    // 释放对象
    manager_.gc_table_ctx_in_list();
    manager_.gc_client_task_in_list();

    manager_.check_all_obj_released(all_released);
    if (!all_released) {
      ob_usleep(100_ms);
    }
  } while (!all_released);
}

int ObTableLoadService::get_memory_limit(int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
  const int64_t LIMIT_SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE = 50;
  ObObj value;
  int64_t pctg = 0;
  int64_t tenant_id = MTL_ID();
  ObSchemaGetterGuard schema_guard;
  const ObSysVarSchema *var_schema = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_system_variable(tenant_id, SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE, var_schema))) {
    LOG_WARN("get tenant system variable failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(var_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var_schema is null");
  } else if (OB_FAIL(var_schema->get_value(NULL, NULL, value))) {
    LOG_WARN("get value from var_schema failed", K(ret), K(*var_schema));
  } else if (OB_FAIL(value.get_int(pctg))) {
    LOG_WARN("get int from value failed", K(ret), K(value));
  } else {
    memory_limit = lib::get_tenant_memory_limit(tenant_id) * MIN(pctg, LIMIT_SYS_VAR_OB_SQL_WORK_AREA_PERCENTAGE) / 100;
  }
  return ret;
}

int ObTableLoadService::add_assigned_task(ObDirectLoadResourceApplyArg &arg)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->assigned_task_manager_.add_assigned_task(arg);
  }
  return ret;
}

int ObTableLoadService::delete_assigned_task(ObDirectLoadResourceReleaseArg &arg)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    if (OB_FAIL(service->assigned_task_manager_.delete_assigned_task(arg.task_key_))) {
      LOG_WARN("fail to delete_assigned_task", KR(ret), K(arg.task_key_));
    } else if (OB_FAIL(ObTableLoadResourceService::release_resource(arg))) {
      LOG_WARN("fail to release resource", KR(ret));
      ret = OB_SUCCESS;   // 允许失败，资源管理模块可以回收
    }
  }

  return ret;
}

int ObTableLoadService::assign_memory(bool is_sort, int64_t assign_memory)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->assigned_memory_manager_.assign_memory(is_sort, assign_memory);
  }
  return ret;
}

int ObTableLoadService::recycle_memory(bool is_sort, int64_t assign_memory)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->assigned_memory_manager_.recycle_memory(is_sort, assign_memory);
  }

  return ret;
}

int ObTableLoadService::get_sort_memory(int64_t &sort_memory)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->assigned_memory_manager_.get_sort_memory(sort_memory);
  }
  return ret;
}

int ObTableLoadService::refresh_and_check_resource(ObDirectLoadResourceCheckArg &arg, ObDirectLoadResourceOpRes &res)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    res.avail_memory_ = service->assigned_memory_manager_.get_avail_memory();
    if (!arg.first_check_ && OB_FAIL(service->assigned_memory_manager_.refresh_avail_memory(arg.avail_memory_))) {
      LOG_WARN("fail to refresh_avail_memory", KR(ret));
    } else if (OB_FAIL(service->assigned_task_manager_.get_assigned_tasks(res.assigned_array_))) {
      LOG_WARN("fail to get_assigned_tasks", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
