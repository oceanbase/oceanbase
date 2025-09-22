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
    if (nullptr != table_ctx->coordinator_ctx_ &&
        table_ctx->coordinator_ctx_->enable_heart_beat()) {
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
    else if (nullptr == table_ctx->coordinator_ctx_ && nullptr != table_ctx->store_ctx_ &&
             table_ctx->store_ctx_->enable_heart_beat_check()) {
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
    service->destroy();
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
      ret = check_support_direct_load(schema_guard, table_schema, column_ids);
    }
  }
  return ret;
}

int ObTableLoadService::check_support_direct_load(ObSchemaGetterGuard &schema_guard,
                                                  uint64_t table_id,
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
      ret = check_support_direct_load(schema_guard, table_schema, column_ids);
    }
  }
  return ret;
}

int ObTableLoadService::check_support_direct_load(ObSchemaGetterGuard &schema_guard,
                                                  const ObTableSchema *table_schema,
                                                  const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_schema || column_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema), K(column_ids));
  } else {
    bool trigger_enabled = false;
    bool has_udt_column = false;
    bool has_unused_column = false;
    // check if it is an oracle temporary table
    if (lib::is_oracle_mode() && table_schema->is_tmp_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support oracle temporary table", KR(ret));
    }
    // check if it is a view
    else if (table_schema->is_view_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support view table", KR(ret));
    }
    // check if exists generated column
    else if (OB_UNLIKELY(table_schema->has_generated_column())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table has generated column", KR(ret));
    }
    // check if the trigger is enabled
    else if (OB_FAIL(table_schema->check_has_trigger_on_table(schema_guard, trigger_enabled))) {
      LOG_WARN("failed to check has trigger in table", KR(ret));
    } else if (trigger_enabled) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table with trigger enabled", KR(ret), K(trigger_enabled));
    }
    // check has udt column
    else if (OB_FAIL(ObTableLoadSchema::check_has_udt_column(table_schema, has_udt_column))) {
      LOG_WARN("fail to check has udt column", KR(ret));
    } else if (has_udt_column) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table has udt column", KR(ret));
    }
    // check has unused column
    else if (OB_FAIL(ObTableLoadSchema::check_has_unused_column(table_schema, has_unused_column))) {
      LOG_WARN("fail to check has unused column", KR(ret));
    } else if (has_unused_column) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support table has unused column", KR(ret));
    }
    // check default column
    if (OB_SUCC(ret)) {
      ObArray<ObColDesc> column_descs;
      if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
        STORAGE_LOG(WARN, "fail to get column descs", KR(ret), KPC(table_schema));
      } else if (column_ids.count() == (table_schema->is_heap_table() ? column_descs.count() - 1
                                                                      : column_descs.count())) {
        // non default column
      } else {
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
            // 自增列
            else if (column_schema->is_autoincrement() || column_schema->is_identity_column()) {
            }
            // 默认值是表达式
            else if (OB_UNLIKELY(lib::is_mysql_mode() && column_schema->get_cur_default_value().is_ext())) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("direct-load does not support column default value is ext", KR(ret),
                       KPC(column_schema), K(column_schema->get_cur_default_value()));
              FORWARD_USER_ERROR_MSG(ret, "direct-load does not support column default value is ext");
            } else if (OB_UNLIKELY(lib::is_oracle_mode() && column_schema->is_default_expr_v2_column())) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("direct-load does not support column default value is expr", KR(ret),
                       KPC(column_schema), K(column_schema->get_cur_default_value()));
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
        if (OB_TMP_FAIL(service->assigned_memory_manager_.recycle_memory(table_ctx->param_.need_sort_, table_ctx->param_.avail_memory_))) {
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
    tg_id_(INVALID_TG_ID),
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
    timer_.set_run_wrapper(MTL_CTX());
    if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TLD_HTimer, tg_id_))) {
      LOG_WARN("fail to create tld heart beat timer", KR(ret));
    } else if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("fail to start tld heart beat timer", KR(ret));
    } else if (OB_FAIL(TG_SCHEDULE(tg_id_, heart_beat_task_, HEART_BEEAT_INTERVAL, true))) {
      LOG_WARN("fail to schedule heart beat task", KR(ret));
    } else if (OB_FAIL(timer_.init("TLD_Timer", ObMemAttr(MTL_ID(), "TLD_TIMER")))) {
      LOG_WARN("fail to init gc timer", KR(ret));
    } else if (OB_FAIL(timer_.schedule(check_tenant_task_, CHECK_TENANT_INTERVAL, true))) {
      LOG_WARN("fail to schedule check tenant task", KR(ret));
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
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
  return ret;
}

void ObTableLoadService::wait()
{
  timer_.wait();
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
  release_all_ctx();
}

void ObTableLoadService::destroy()
{
  is_inited_ = false;
  timer_.destroy();
  if (INVALID_TG_ID != tg_id_) {
    TG_DESTROY(tg_id_);
    tg_id_ = INVALID_TG_ID;
  }
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
