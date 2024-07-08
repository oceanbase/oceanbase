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

#include "observer/table_load/ob_table_load_service.h"
#include "observer/omt/ob_tenant.h"
#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_coordinator_ctx.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace lib;
using namespace share::schema;
using namespace storage;
using namespace table;
using namespace omt;

/**
 * ObCheckTenantTask
 */

int ObTableLoadService::ObCheckTenantTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService::ObCheckTenantTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadService::ObCheckTenantTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService::ObCheckTenantTask not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("table load check tenant", K(tenant_id_));
    if (OB_FAIL(ObTableLoadService::check_tenant())) {
      LOG_WARN("fail to check_tenant", KR(ret));
      // abort all client task
      service_.abort_all_client_task();
      // fail all current tasks
      service_.fail_all_ctx(ret);
    }
  }
}

/**
 * ObHeartBeatTask
 */

int ObTableLoadService::ObHeartBeatTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService::ObHeartBeatTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadService::ObHeartBeatTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService::ObHeartBeatTask not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("table load heart beat", K(tenant_id_));
    ObTableLoadManager &manager = service_.get_manager();
    ObArray<ObTableLoadTableCtx *> table_ctx_array;
    table_ctx_array.set_tenant_id(MTL_ID());
    if (OB_FAIL(manager.get_all_table_ctx(table_ctx_array))) {
      LOG_WARN("fail to get all table ctx", KR(ret), K(tenant_id_));
    }
    for (int64_t i = 0; i < table_ctx_array.count(); ++i) {
      ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
      if (nullptr != table_ctx->coordinator_ctx_ && table_ctx->coordinator_ctx_->enable_heart_beat()) {
        ObTableLoadCoordinator coordinator(table_ctx);
        if (OB_FAIL(coordinator.init())) {
          LOG_WARN("fail to init coordinator", KR(ret));
        } else if (OB_FAIL(coordinator.heart_beat())) {
          LOG_WARN("fail to coordinator heart beat", KR(ret));
        }
      }
      manager.put_table_ctx(table_ctx);
    }
  }
}

/**
 * ObGCTask
 */

int ObTableLoadService::ObGCTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService::ObGCTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadService::ObGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService::ObGCTask not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("table load start gc", K(tenant_id_));
    ObTableLoadManager &manager = service_.get_manager();
    ObArray<ObTableLoadTableCtx *> table_ctx_array;
    table_ctx_array.set_tenant_id(MTL_ID());
    if (OB_FAIL(manager.get_all_table_ctx(table_ctx_array))) {
      LOG_WARN("fail to get all  table ctx", KR(ret), K(tenant_id_));
    }
    for (int64_t i = 0; i < table_ctx_array.count(); ++i) {
      ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
      if (gc_mark_delete(table_ctx)) {
      } else if (gc_heart_beat_expired_ctx(table_ctx)) {
      } else if (gc_table_not_exist_ctx(table_ctx)) {
      }
      manager.put_table_ctx(table_ctx);
    }
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
    if (table_ctx->is_dirty()) {
      LOG_DEBUG("table load ctx is dirty", K(tenant_id_), K(table_id), K(task_id), K(dest_table_id),
                "ref_count", table_ctx->get_ref_count());
      is_removed = true;
    }
    // check is mark delete
    else if (table_ctx->is_mark_delete()) {
      if (table_ctx->is_stopped() && OB_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
        LOG_WARN("fail to remove table ctx", KR(ret), K(tenant_id_), K(table_id), K(task_id), K(dest_table_id));
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
    if (table_ctx->is_dirty()) {
      LOG_DEBUG("table load ctx is dirty", K(tenant_id_), K(table_id), K(task_id), K(dest_table_id),
                "ref_count", table_ctx->get_ref_count());
      is_removed = true;
    }
    // check if heart beat expired, ignore coordinator
    else if (nullptr == table_ctx->coordinator_ctx_ &&
             nullptr != table_ctx->store_ctx_ &&
             table_ctx->store_ctx_->enable_heart_beat_check()) {
      if (OB_UNLIKELY(
            table_ctx->store_ctx_->check_heart_beat_expired(HEART_BEEAT_EXPIRED_TIME_US))) {
        FLOG_INFO("store heart beat expired, abort", K(tenant_id_), K(table_id), K(task_id), K(dest_table_id));
        bool is_stopped = false;
        ObTableLoadStore::abort_ctx(table_ctx, is_stopped);
        table_ctx->mark_delete();
        if (is_stopped && OB_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
          LOG_WARN("fail to remove table ctx", KR(ret), K(tenant_id_), K(table_id), K(task_id), K(dest_table_id));
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
  bool is_removed = false;
  if (OB_ISNULL(table_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table ctx is null", KR(ret));
    is_removed = true;
  } else {
    const uint64_t table_id = table_ctx->param_.table_id_;
    const uint64_t hidden_table_id = table_ctx->ddl_param_.dest_table_id_;
    // check if table ctx is removed
    if (table_ctx->is_dirty()) {
      LOG_DEBUG("table load ctx is dirty", K(tenant_id_), K(table_id), "ref_count",
                table_ctx->get_ref_count());
      is_removed = true;
    }
    // check if table ctx is activated
    else if (table_ctx->get_ref_count() > 1) {
      LOG_DEBUG("table load ctx is active", K(tenant_id_), K(table_id), "ref_count",
                table_ctx->get_ref_count());
    }
    // check if table ctx can be recycled
    else {
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id_, hidden_table_id, schema_guard,
                                                      table_schema))) {
        if (OB_UNLIKELY(OB_TABLE_NOT_EXIST != ret && OB_TENANT_NOT_EXIST != ret)) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id_), K(hidden_table_id));
        } else {
          LOG_INFO("hidden table not exist, gc table load ctx", K(tenant_id_), K(table_id),
                   K(hidden_table_id));
          ObTableLoadService::remove_ctx(table_ctx);
          is_removed = true;
        }
      } else if (table_schema->is_in_recyclebin()) {
        LOG_INFO("hidden table is in recyclebin, gc table load ctx", K(tenant_id_), K(table_id),
                 K(hidden_table_id));
        ObTableLoadService::remove_ctx(table_ctx);
        is_removed = true;
      } else {
        LOG_DEBUG("table load ctx is running", K(tenant_id_), K(table_id), K(hidden_table_id));
      }
    }
  }
  return is_removed;
}

/**
 * ObReleaseTask
 */

int ObTableLoadService::ObReleaseTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService::ObReleaseTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadService::ObReleaseTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService::ObReleaseTask not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("table load start release", K(tenant_id_));
    ObArray<ObTableLoadTableCtx *> releasable_table_ctx_array;
    releasable_table_ctx_array.set_tenant_id(MTL_ID());
    if (OB_FAIL(service_.manager_.get_releasable_table_ctx_list(releasable_table_ctx_array))) {
      LOG_WARN("fail to get releasable table ctx list", KR(ret), K(tenant_id_));
    }
    for (int64_t i = 0; i < releasable_table_ctx_array.count(); ++i) {
      ObTableLoadTableCtx *table_ctx = releasable_table_ctx_array.at(i);
      const uint64_t table_id = table_ctx->param_.table_id_;
      const uint64_t hidden_table_id = table_ctx->ddl_param_.dest_table_id_;
      const int64_t task_id = table_ctx->ddl_param_.task_id_;
      LOG_INFO("free table ctx", K(tenant_id_), K(table_id), K(hidden_table_id), K(task_id),
               KP(table_ctx));
      ObTableLoadService::free_ctx(table_ctx);
    }
  }
}

/**
 * ObClientTaskAutoAbortTask
 */

int ObTableLoadService::ObClientTaskAutoAbortTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService::ObClientTaskAutoAbortTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadService::ObClientTaskAutoAbortTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService::ObClientTaskAutoAbortTask not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("table load auto abort client task", K(tenant_id_));
    ObArray<ObTableLoadClientTask *> client_task_array;
    client_task_array.set_tenant_id(MTL_ID());
    if (OB_FAIL(service_.get_client_service().get_all_client_task(client_task_array))) {
      LOG_WARN("fail to get all client task", KR(ret));
    } else {
      for (int64_t i = 0; i < client_task_array.count(); ++i) {
        ObTableLoadClientTask *client_task = client_task_array.at(i);
        if (OB_UNLIKELY(ObTableLoadClientStatus::ERROR == client_task->get_status() ||
                        client_task->get_exec_ctx()->check_status() != OB_SUCCESS)) {
          client_task->abort();
        }
        ObTableLoadClientService::revert_task(client_task);
      }
    }
  }
}

/**
 * ObClientTaskPurgeTask
 */

int ObTableLoadService::ObClientTaskPurgeTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService::ObClientTaskPurgeTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadService::ObClientTaskPurgeTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService::ObClientTaskPurgeTask not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("table load purge client task", K(tenant_id_));
    service_.get_client_service().purge_client_task();
    service_.get_client_service().purge_client_task_brief();
  }
}

/**
 * ObTableLoadService
 */

int ObTableLoadService::mtl_init(ObTableLoadService *&service)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(service));
  } else if (OB_FAIL(service->init(tenant_id))) {
    LOG_WARN("fail to init table load service", KR(ret), K(tenant_id));
  }
  return ret;
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
    const storage::ObDirectLoadMode::Type load_mode)
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
      ret = check_support_direct_load(schema_guard, table_schema, method, insert_mode, load_mode);
    }
  }
  return ret;
}

int ObTableLoadService::check_support_direct_load(ObSchemaGetterGuard &schema_guard,
                                                  uint64_t table_id,
                                                  const ObDirectLoadMethod::Type method,
                                                  const ObDirectLoadInsertMode::Type insert_mode,
                                                  const storage::ObDirectLoadMode::Type load_mode)
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
      ret = check_support_direct_load(schema_guard, table_schema, method, insert_mode, load_mode);
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
                                                  const storage::ObDirectLoadMode::Type load_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_schema ||
                  !ObDirectLoadMethod::is_type_valid(method) ||
                  !ObDirectLoadInsertMode::is_type_valid(insert_mode)) ||
                  !ObDirectLoadMode::is_type_valid(load_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema), K(method), K(insert_mode));
  } else {
    const uint64_t tenant_id = MTL_ID();
    bool trigger_enabled = false;
    bool has_udt_column = false;
    bool has_fts_index = false;
    bool has_multivalue_index = false;
    bool has_invisible_column = false;
    bool has_unused_column = false;
    // check if it is a user table
    const char *tmp_prefix = ObDirectLoadMode::is_insert_overwrite(load_mode) ? InsertOverwritePrefix : EmptyPrefix;

    if (!table_schema->is_user_table()) {
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
    // check if exists multi-value index
    else if (OB_FAIL(table_schema->check_has_multivalue_index(schema_guard, has_multivalue_index))) {
      LOG_WARN("fail to check has multivalue index", K(ret));
    } else if (has_multivalue_index) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table has multi-value index", KR(ret));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has multi-value index", tmp_prefix);
    }
    // check if exists generated column
    else if (OB_UNLIKELY(table_schema->has_generated_column())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table has generated column", KR(ret));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has generated column", tmp_prefix);
    }
    // check if the trigger is enabled
    else if (OB_FAIL(table_schema->check_has_trigger_on_table(schema_guard, trigger_enabled))) {
      LOG_WARN("failed to check has trigger in table", KR(ret));
    } else if (trigger_enabled) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table with trigger enabled", KR(ret), K(trigger_enabled));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table with trigger enabled", tmp_prefix);
    }
    // check has udt column
    else if (OB_FAIL(ObTableLoadSchema::check_has_udt_column(table_schema, has_udt_column))) {
      LOG_WARN("fail to check has udt column", KR(ret));
    } else if (has_udt_column) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table has udt column", KR(ret));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has udt column", tmp_prefix);
    }
    // check has invisible column
    else if (OB_FAIL(ObTableLoadSchema::check_has_invisible_column(table_schema, has_invisible_column))) {
      LOG_WARN("fail to check has invisible column", KR(ret));
    } else if (has_invisible_column) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table has invisible column", KR(ret));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has invisible column", tmp_prefix);
    }
    // check has unused column
    else if (OB_FAIL(ObTableLoadSchema::check_has_unused_column(table_schema, has_unused_column))) {
      LOG_WARN("fail to check has unused column", KR(ret));
    } else if (has_unused_column) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table has unused column", KR(ret));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table has unused column", tmp_prefix);
    }
    // check if table has mlog
    else if (table_schema->has_mlog_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table with materialized view log", KR(ret));
      FORWARD_USER_ERROR_MSG(ret, "%sdirect-load does not support table with materialized view log", tmp_prefix);
    } else if (ObDirectLoadMethod::is_incremental(method)) { // incremental direct-load
      uint64_t compat_version = 0;
      if (!ObDirectLoadInsertMode::is_valid_for_incremental_method(insert_mode)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("using incremental direct-load without inc_replace or normal is not supported", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "using incremental direct-load without inc_replace or normal is not supported");
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
        LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
      } else if (compat_version < DATA_VERSION_4_3_1_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("version lower than 4.3.1.0 does not support incremental direct-load", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "version lower than 4.3.1.0 does not support incremental direct-load");
      } else if (table_schema->get_index_tid_count() > 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("incremental direct-load does not support table with indexes", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "incremental direct-load does not support table with indexes");
      } else if (table_schema->get_foreign_key_infos().count() > 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("incremental direct-load does not support table with foreign keys", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "incremental direct-load does not support table with foreign keys");
      } else if (table_schema->has_check_constraint() && (ObDirectLoadMode::LOAD_DATA == load_mode || ObDirectLoadMode::TABLE_LOAD == load_mode)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("incremental direct-load does not support table with check constraints", KR(ret));
        FORWARD_USER_ERROR_MSG(ret, "incremental direct-load does not support table with check constraints");
      }
    } else if (ObDirectLoadMethod::is_full(method)) { // full direct-load
      if (OB_UNLIKELY(!ObDirectLoadInsertMode::is_valid_for_full_method(insert_mode))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected insert mode for full direct-load", KR(ret), K(method), K(insert_mode));
      } else if (ObDirectLoadMode::is_insert_overwrite(load_mode)) {
        uint64_t compat_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
          LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
        } else if (compat_version < DATA_VERSION_4_3_2_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("version lower than 4.3.2.0 does not support insert overwrite", KR(ret));
          FORWARD_USER_ERROR_MSG(ret, "version lower than 4.3.2.0 does not support insert overwrite");
        } else if (table_schema->get_foreign_key_infos().count() > 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("insert overwrite with incremental direct-load does not support table with foreign keys", KR(ret));
          FORWARD_USER_ERROR_MSG(ret, "insert overwrite with direct-load does not support table with foreign keys");
        }
      }
    }
  }
  return ret;
}

ObTableLoadTableCtx *ObTableLoadService::alloc_ctx()
{
  return OB_NEW(ObTableLoadTableCtx, ObMemAttr(MTL_ID(), "TLD_TableCtxVal"));
}

void ObTableLoadService::free_ctx(ObTableLoadTableCtx *table_ctx)
{
  if (OB_NOT_NULL(table_ctx)) {
    OB_DELETE(ObTableLoadTableCtx, "TLD_TableCtxVal", table_ctx);
    table_ctx = nullptr;
  }
}

int ObTableLoadService::add_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (service->is_stop_) {
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
    common::ObAddr leader;
    ObDirectLoadResourceReleaseArg release_arg;
    release_arg.tenant_id_ = MTL_ID();
    release_arg.task_key_ = ObTableLoadUniqueKey(table_ctx->param_.table_id_, table_ctx->ddl_param_.task_id_);
    bool is_sort = (table_ctx->param_.exe_mode_ == ObTableLoadExeMode::MULTIPLE_HEAP_TABLE_COMPACT ||
                    table_ctx->param_.exe_mode_ == ObTableLoadExeMode::MEM_COMPACT);
    if (OB_FAIL(service->get_manager().remove_table_ctx(release_arg.task_key_, table_ctx))) {
      LOG_WARN("fail to remove_table_ctx", KR(ret), K(release_arg.task_key_));
    } else if (table_ctx->is_assigned_memory() &&
               OB_FAIL(service->assigned_memory_manager_.recycle_memory(is_sort, table_ctx->param_.avail_memory_))) {
      LOG_WARN("fail to recycle_memory", KR(ret), K(release_arg.task_key_));
    } else if (table_ctx->is_assigned_resource()) {
      if (OB_FAIL(service->assigned_task_manager_.delete_assigned_task(release_arg.task_key_))) {
        LOG_WARN("fail to delete_assigned_task", KR(ret), K(release_arg.task_key_));
      } else if (OB_FAIL(ObTableLoadResourceService::release_resource(release_arg))) {
        LOG_WARN("fail to release resource", KR(ret));
        ret = OB_SUCCESS;   // 允许失败，资源管理模块可以回收
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
    service->get_manager().put_table_ctx(table_ctx);
  }
}

ObTableLoadService::ObTableLoadService()
  : check_tenant_task_(*this),
    heart_beat_task_(*this),
    gc_task_(*this),
    release_task_(*this),
    client_task_auto_abort_task_(*this),
    client_task_purge_task_(*this),
    is_stop_(false),
    is_inited_(false)
{
}

int ObTableLoadService::init(uint64_t tenant_id)
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
  } else if (OB_FAIL(client_service_.init())) {
    LOG_WARN("fail to init client service", KR(ret));
  } else if (OB_FAIL(check_tenant_task_.init(tenant_id))) {
    LOG_WARN("fail to init check tenant task", KR(ret));
  } else if (OB_FAIL(heart_beat_task_.init(tenant_id))) {
    LOG_WARN("fail to init heart beat task", KR(ret));
  } else if (OB_FAIL(gc_task_.init(tenant_id))) {
    LOG_WARN("fail to init gc task", KR(ret));
  } else if (OB_FAIL(release_task_.init(tenant_id))) {
    LOG_WARN("fail to init release task", KR(ret));
  } else if (OB_FAIL(client_task_auto_abort_task_.init(tenant_id))) {
    LOG_WARN("fail to init client task auto abort task", KR(ret));
  } else if (OB_FAIL(client_task_purge_task_.init(tenant_id))) {
    LOG_WARN("fail to init client task purge task", KR(ret));
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
    timer_.set_run_wrapper(MTL_CTX());
    if (OB_FAIL(timer_.init("TLD_Timer", ObMemAttr(MTL_ID(), "TLD_TIMER")))) {
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

void ObTableLoadService::abort_all_client_task()
{
  int ret = OB_SUCCESS;
  ObArray<ObTableLoadClientTask *> client_task_array;
  client_task_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(client_service_.get_all_client_task(client_task_array))) {
    LOG_WARN("fail to get all client task", KR(ret));
  } else {
    for (int i = 0; i < client_task_array.count(); ++i) {
      ObTableLoadClientTask *client_task = client_task_array.at(i);
      client_task->abort();
      ObTableLoadClientService::revert_task(client_task);
    }
  }
}

void ObTableLoadService::fail_all_ctx(int error_code)
{
  int ret = OB_SUCCESS;
  ObArray<ObTableLoadTableCtx *> table_ctx_array;
  table_ctx_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(manager_.get_all_table_ctx(table_ctx_array))) {
    LOG_WARN("fail to get all table ctx list", KR(ret));
  } else {
    for (int i = 0; i < table_ctx_array.count(); ++i) {
      ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
      // fail coordinator
      if (nullptr != table_ctx->coordinator_ctx_) {
        table_ctx->coordinator_ctx_->set_status_error(error_code);
      }
      // fail store
      if (nullptr != table_ctx->store_ctx_) {
        table_ctx->store_ctx_->set_status_error(error_code);
      }
      manager_.put_table_ctx(table_ctx);
    }
  }
}

void ObTableLoadService::release_all_ctx()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  // 1. check all ctx are removed
  while (OB_SUCC(ret)) {
    if (REACH_TIME_INTERVAL(30 * 1000 * 1000)) {
      LOG_INFO("[DIRECT LOAD]", "client_task_count", client_service_.get_client_task_count(),
               "table_ctx_count", manager_.get_table_ctx_count());
    }
    abort_all_client_task();
    fail_all_ctx(OB_ERR_UNEXPECTED_UNIT_STATUS);
    ObArray<ObTableLoadTableCtx *> table_ctx_array;
    table_ctx_array.set_tenant_id(MTL_ID());
    if (OB_FAIL(manager_.get_inactive_table_ctx_list(table_ctx_array))) {
      LOG_WARN("fail to get inactive table ctx list", KR(ret), K(tenant_id));
    } else {
      for (int i = 0; i < table_ctx_array.count(); ++i) {
        ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
        const uint64_t table_id = table_ctx->param_.table_id_;
        const uint64_t hidden_table_id = table_ctx->ddl_param_.dest_table_id_;
        // check if table ctx is removed
        if (table_ctx->is_dirty()) {
          LOG_DEBUG("table load ctx is dirty", K(tenant_id), K(table_id), "ref_count",
                    table_ctx->get_ref_count());
        }
        // check if table ctx is activated
        else if (table_ctx->get_ref_count() > 1) {
          LOG_DEBUG("table load ctx is active", K(tenant_id), K(table_id), "ref_count",
                    table_ctx->get_ref_count());
        } else {
          LOG_INFO("tenant exit, remove table load ctx", K(tenant_id), K(table_id),
                   K(hidden_table_id));
          remove_ctx(table_ctx);
        }
        manager_.put_table_ctx(table_ctx);
      }
    }
    if (0 == manager_.get_table_ctx_count()) {
      break;
    } else {
      ob_usleep(1 * 1000 * 1000);
    }
  }
  // 2. release all ctx
  while (OB_SUCC(ret)) {
    if (REACH_TIME_INTERVAL(30 * 1000 * 1000)) {
      LOG_INFO("[DIRECT LOAD DIRTY LIST]", "count", manager_.get_dirty_list_count());
    }
    ObArray<ObTableLoadTableCtx *> table_ctx_array;
    table_ctx_array.set_tenant_id(MTL_ID());
    if (OB_FAIL(manager_.get_releasable_table_ctx_list(table_ctx_array))) {
      LOG_WARN("fail to get releasable table ctx list", KR(ret));
    }
    for (int64_t i = 0; i < table_ctx_array.count(); ++i) {
      ObTableLoadTableCtx *table_ctx = table_ctx_array.at(i);
      const uint64_t table_id = table_ctx->param_.table_id_;
      const uint64_t hidden_table_id = table_ctx->ddl_param_.dest_table_id_;
      const int64_t task_id = table_ctx->ddl_param_.task_id_;
      LOG_INFO("free table ctx", K(tenant_id), K(table_id), K(hidden_table_id), K(task_id),
               KP(table_ctx));
      ObTableLoadService::free_ctx(table_ctx);
    }
    if (0 == manager_.get_dirty_list_count()) {
      break;
    } else {
      ob_usleep(1 * 1000 * 1000);
    }
  }
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
