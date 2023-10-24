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

#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "share/ob_tenant_mgr.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "observer/ob_server.h"
#include "observer/virtual_table/ob_tenant_all_tables.h"
#include "observer/virtual_table/ob_tenant_show_tables.h"
#include "observer/virtual_table/ob_tenant_virtual_warning.h"
#include "observer/virtual_table/ob_tenant_virtual_current_tenant.h"
#include "observer/virtual_table/ob_session_variables.h"
#include "observer/virtual_table/ob_global_variables.h"
#include "observer/virtual_table/ob_table_columns.h"
#include "observer/virtual_table/ob_table_index.h"
#include "observer/virtual_table/ob_show_create_database.h"
#include "observer/virtual_table/ob_show_create_table.h"
#include "observer/virtual_table/ob_show_create_tablegroup.h"
#include "observer/virtual_table/ob_show_create_procedure.h"
#include "observer/virtual_table/ob_show_grants.h"
#include "observer/virtual_table/ob_information_table_privileges_table.h"
#include "observer/virtual_table/ob_information_user_privileges_table.h"
#include "observer/virtual_table/ob_information_schema_privileges_table.h"
#include "observer/virtual_table/ob_information_columns_table.h"
#include "observer/virtual_table/ob_information_parameters_table.h"
#include "observer/virtual_table/ob_information_session_variables_table.h"
#include "observer/virtual_table/ob_information_global_status_table.h"
#include "observer/virtual_table/ob_information_session_status_table.h"
#include "observer/virtual_table/ob_information_kvcache_table.h"
#include "observer/virtual_table/ob_all_concurrency_object_pool.h"
#include "observer/virtual_table/ob_mysql_user_table.h"
#include "observer/virtual_table/ob_mysql_db_table.h"
#include "observer/virtual_table/ob_mysql_proc_table.h"
#include "observer/virtual_table/ob_information_table_constraints_table.h"
#include "observer/virtual_table/ob_information_check_constraints_table.h"
#include "observer/virtual_table/ob_information_referential_constraints_table.h"
#include "observer/virtual_table/ob_information_partitions_table.h"
#include "observer/virtual_table/ob_all_virtual_session_event.h"
#include "observer/virtual_table/ob_all_virtual_session_wait.h"
#include "observer/virtual_table/ob_all_virtual_session_wait_history.h"
#include "observer/virtual_table/ob_all_virtual_session_stat.h"
#include "observer/virtual_table/ob_all_disk_stat.h"
#include "observer/virtual_table/ob_mem_leak_checker_info.h"
#include "observer/virtual_table/ob_all_virtual_malloc_sample_info.h"
#include "observer/virtual_table/ob_all_latch.h"
#include "observer/virtual_table/ob_all_data_type_class_table.h"
#include "observer/virtual_table/ob_all_data_type_table.h"
#include "observer/virtual_table/ob_all_virtual_tenant_memstore_info.h"
#include "observer/virtual_table/ob_all_virtual_tablet_info.h"
#include "observer/virtual_table/ob_all_virtual_server_blacklist.h"
#include "observer/virtual_table/ob_all_virtual_sys_parameter_stat.h"
#include "observer/virtual_table/ob_all_virtual_tenant_parameter_stat.h"
#include "observer/virtual_table/ob_all_virtual_tenant_parameter_info.h"
#include "observer/virtual_table/ob_all_virtual_memstore_info.h"
#include "observer/virtual_table/ob_all_virtual_minor_freeze_info.h"
#include "observer/virtual_table/ob_gv_sql_audit.h"
#include "observer/virtual_table/ob_gv_sql.h"
#include "observer/virtual_table/ob_show_database_status.h"
#include "observer/virtual_table/ob_show_tenant_status.h"
#include "observer/virtual_table/ob_all_virtual_sys_stat.h"
#include "observer/virtual_table/ob_all_virtual_sys_event.h"
#include "observer/virtual_table/ob_all_virtual_tx_stat.h"
#include "observer/virtual_table/ob_all_virtual_tx_lock_stat.h"
#include "observer/virtual_table/ob_all_virtual_tx_scheduler_stat.h"
#include "observer/virtual_table/ob_all_virtual_tx_ctx_mgr_stat.h"
#include "observer/virtual_table/ob_all_virtual_weak_read_stat.h"
#include "observer/virtual_table/ob_tenant_virtual_statname.h"
#include "observer/virtual_table/ob_tenant_virtual_event_name.h"
#include "observer/virtual_table/ob_all_virtual_engine_table.h"
#include "observer/virtual_table/ob_all_virtual_files_table.h"
#include "observer/virtual_table/ob_all_virtual_ls_info.h"
#include "observer/virtual_table/ob_all_plan_cache_stat.h"
#include "observer/virtual_table/ob_plan_cache_plan_explain.h"
#include "observer/virtual_table/ob_all_virtual_ps_stat.h"
#include "observer/virtual_table/ob_all_virtual_ps_item_info.h"
#include "observer/virtual_table/ob_show_processlist.h"
#include "observer/virtual_table/ob_all_virtual_session_info.h"
#include "observer/virtual_table/ob_all_virtual_memory_info.h"
#include "observer/virtual_table/ob_all_virtual_raid_stat.h"
#include "observer/virtual_table/ob_virtual_obrpc_send_stat.h"
#include "observer/virtual_table/ob_virtual_span_info.h"
#include "observer/virtual_table/ob_all_virtual_proxy_schema.h"
#include "observer/virtual_table/ob_virtual_proxy_server_stat.h"
#include "observer/virtual_table/ob_virtual_proxy_sys_variable.h"
#include "observer/virtual_table/ob_all_virtual_tablet_sstable_macro_info.h"
#include "observer/virtual_table/ob_virtual_sql_plan_monitor.h"
#include "observer/virtual_table/ob_virtual_ash.h"
#include "observer/virtual_table/ob_all_virtual_arbitration_member_info.h"
#include "observer/virtual_table/ob_all_virtual_arbitration_service_status.h"
#include "observer/virtual_table/ob_virtual_sql_monitor_statname.h"
#include "observer/virtual_table/ob_virtual_sql_plan_statistics.h"
#include "observer/virtual_table/ob_virtual_sql_monitor.h"
#include "observer/virtual_table/ob_tenant_virtual_outline.h"
#include "observer/virtual_table/ob_tenant_virtual_concurrent_limit_sql.h"
#include "observer/virtual_table/ob_all_virtual_proxy_partition_info.h"
#include "observer/virtual_table/ob_all_virtual_proxy_partition.h"
#include "observer/virtual_table/ob_all_virtual_proxy_sub_partition.h"
#include "observer/virtual_table/ob_all_virtual_proxy_routine.h" // ObAllVirtualProxyRoutine
#include "observer/virtual_table/ob_all_virtual_sys_task_status.h"
#include "observer/virtual_table/ob_all_virtual_macro_block_marker_status.h"
#include "observer/virtual_table/ob_all_virtual_lock_wait_stat.h"
#include "observer/virtual_table/ob_all_virtual_long_ops_status.h"
#include "observer/virtual_table/ob_all_virtual_tenant_memstore_allocator_info.h"
#include "observer/virtual_table/ob_all_virtual_server_object_pool.h"
#include "observer/virtual_table/ob_all_virtual_io_stat.h"
#include "observer/virtual_table/ob_all_virtual_bad_block_table.h"
#include "observer/virtual_table/ob_agent_virtual_table.h"
#include "observer/virtual_table/ob_iterate_virtual_table.h"
#include "observer/virtual_table/ob_iterate_private_virtual_table.h" // ObIteratePrivateVirtualTable
#include "observer/virtual_table/ob_all_virtual_id_service.h"
#include "observer/virtual_table/ob_all_virtual_timestamp_service.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/virtual_table/ob_core_meta_table.h"
#include "rootserver/virtual_table/ob_virtual_core_inner_table.h"
#include "rootserver/ob_root_inspection.h"
#include "observer/virtual_table/ob_tenant_virtual_charset.h"
#include "observer/virtual_table/ob_tenant_virtual_collation.h"
#include "observer/virtual_table/ob_all_virtual_dtl_channel.h"
#include "observer/virtual_table/ob_all_virtual_dtl_memory.h"
#include "observer/virtual_table/ob_all_virtual_dtl_first_cached_buffer.h"
#include "observer/virtual_table/ob_tenant_virtual_get_object_definition.h"
#include "observer/virtual_table/ob_all_virtual_sql_workarea_history_stat.h"
#include "observer/virtual_table/ob_all_virtual_sql_workarea_active.h"
#include "observer/virtual_table/ob_all_virtual_sql_workarea_histogram.h"
#include "observer/virtual_table/ob_all_virtual_sql_workarea_memory_info.h"
#include "observer/virtual_table/ob_all_virtual_table_mgr.h"
#include "observer/virtual_table/ob_all_virtual_px_worker_stat.h"
#include "observer/virtual_table/ob_all_virtual_tablet_store_stat.h"
#include "observer/virtual_table/ob_all_virtual_server_schema_info.h"
#include "observer/virtual_table/ob_all_virtual_memory_context_stat.h"
#include "observer/virtual_table/ob_all_virtual_audit_operation.h"
#include "observer/virtual_table/ob_all_virtual_audit_action.h"
#include "observer/virtual_table/ob_all_virtual_dump_tenant_info.h"
#include "observer/virtual_table/ob_all_virtual_dag_warning_history.h"
#include "observer/virtual_table/ob_all_virtual_dag.h"
#include "observer/virtual_table/ob_all_virtual_compaction_diagnose_info.h"
#include "observer/virtual_table/ob_all_virtual_compaction_suggestion.h"
#include "observer/virtual_table/ob_all_virtual_server_compaction_progress.h"
#include "observer/virtual_table/ob_all_virtual_server_compaction_event_history.h"
#include "observer/virtual_table/ob_all_virtual_tablet_compaction_progress.h"
#include "observer/virtual_table/ob_all_virtual_tablet_compaction_history.h"
#include "observer/virtual_table/ob_all_virtual_tablet_compaction_info.h"
#include "observer/virtual_table/ob_all_virtual_tablet_ddl_kv_info.h"
#include "observer/virtual_table/ob_all_virtual_tablet_pointer_status.h"
#include "observer/virtual_table/ob_all_virtual_storage_meta_memory_status.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_virtual_table_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_tenant_id_schema_version.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/tx/ob_ts_mgr.h"
#include "observer/virtual_table/ob_all_virtual_tx_data.h"
#include "observer/virtual_table/ob_all_virtual_tx_data_table.h"
#include "observer/virtual_table/ob_all_virtual_transaction_freeze_checkpoint.h"
#include "observer/virtual_table/ob_all_virtual_transaction_checkpoint.h"
#include "observer/virtual_table/ob_all_virtual_checkpoint.h"
#include "observer/virtual_table/ob_virtual_open_cursor_table.h"
#include "observer/virtual_table/ob_all_virtual_tablet_encrypt_info.h"
#include "observer/virtual_table/ob_all_virtual_tenant_ctx_memory_info.h"
#include "observer/virtual_table/ob_all_virtual_tenant_memory_info.h"
#include "observer/virtual_table/ob_all_virtual_master_key_version_info.h"
#include "observer/virtual_table/ob_all_virtual_io_status.h"
#include "observer/virtual_table/ob_information_triggers_table.h"
#include "observer/virtual_table/ob_show_create_trigger.h"
#include "observer/virtual_table/ob_all_virtual_px_target_monitor.h"
#include "observer/virtual_table/ob_all_virtual_dblink_info.h"
#include "observer/virtual_table/ob_all_virtual_load_data_stat.h"
#include "observer/virtual_table/ob_all_virtual_dtl_interm_result_monitor.h"
#include "observer/virtual_table/ob_all_virtual_log_stat.h"
#include "observer/virtual_table/ob_all_virtual_apply_stat.h"
#include "observer/virtual_table/ob_all_virtual_ha_diagnose.h"
#include "observer/virtual_table/ob_all_virtual_replay_stat.h"
#include "observer/virtual_table/ob_all_virtual_unit.h"
#include "observer/virtual_table/ob_all_virtual_server.h"
#include "observer/virtual_table/ob_all_virtual_obj_lock.h"
#include "rootserver/virtual_table/ob_all_virtual_backup_task_scheduler_stat.h"
#include "observer/virtual_table/ob_all_virtual_ls_archive_stat.h"
#include "observer/virtual_table/ob_all_virtual_dml_stats.h"
#include "observer/virtual_table/ob_tenant_virtual_privilege.h"
#include "observer/virtual_table/ob_all_virtual_kvcache_store_memblock.h"
#include "observer/virtual_table/ob_information_query_response_time.h"
#include "observer/virtual_table/ob_all_virtual_kvcache_handle_leak_info.h"
#include "observer/virtual_table/ob_all_virtual_schema_memory.h"
#include "observer/virtual_table/ob_all_virtual_schema_slot.h"
#include "rootserver/virtual_table/ob_all_virtual_ls_replica_task_plan.h"
#include "observer/virtual_table/ob_all_virtual_archive_dest_status.h"
#include "observer/virtual_table/ob_virtual_show_trace.h"
#include "observer/virtual_table/ob_all_virtual_sql_plan.h"
#include "observer/virtual_table/ob_all_virtual_mds_node_stat.h"
#include "observer/virtual_table/ob_all_virtual_mds_event_history.h"
#include "observer/virtual_table/ob_all_virtual_dup_ls_lease_mgr.h"
#include "observer/virtual_table/ob_all_virtual_dup_ls_tablets.h"
#include "observer/virtual_table/ob_all_virtual_opt_stat_gather_monitor.h"
#include "observer/virtual_table/ob_all_virtual_thread.h"
#include "observer/virtual_table/ob_all_virtual_dup_ls_tablet_set.h"
#include "observer/virtual_table/ob_all_virtual_px_p2p_datahub.h"
#include "observer/virtual_table/ob_all_virtual_ls_log_restore_status.h"
#include "observer/virtual_table/ob_all_virtual_tablet_buffer_info.h"
#include "observer/virtual_table/ob_virtual_flt_config.h"

#include "observer/virtual_table/ob_all_virtual_kv_connection.h"
#include "observer/virtual_table/ob_tenant_show_restore_preview.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace rootserver;
using namespace transaction;
namespace observer
{
#define NEW_VIRTUAL_TABLE(virtual_table, vt, ...)                                       \
  ({                                                                                    \
	  void *tmp_ptr = NULL;                                                               \
    vt = NULL;                                                                          \
    if (OB_UNLIKELY(NULL == (tmp_ptr = allocator.alloc(sizeof(virtual_table))))) {      \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                  \
      SERVER_LOG(ERROR, "fail to alloc memory", K(pure_tid), K(ret));                   \
    } else if (OB_UNLIKELY(NULL == (vt = new (tmp_ptr) virtual_table(__VA_ARGS__)))) {  \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                  \
      SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));                            \
    } else {                                                                            \
      vt_iter = static_cast<ObVirtualTableIterator *>(vt);                              \
      if (OB_FAIL(vt_iter->set_key_ranges(params.key_ranges_))) {                       \
          allocator.free(tmp_ptr);                                                      \
          SERVER_LOG(WARN, "fail to set key ranges", K(ret), K(params));                \
      } else {                                                                          \
        if (lib::is_oracle_mode() && is_oracle_mapping_virtual_table(data_table_id)) {  \
          vt_iter->set_convert_flag();                                                  \
        }                                                                               \
        vt_iter->set_session(session);                                                  \
        vt_iter->set_effective_tenant_id(real_tenant_id);                               \
        vt_iter->set_schema_guard(&schema_guard);                                       \
        vt_iter->set_table_schema(table_schema);                                        \
        vt_iter->set_index_schema(index_schema);                                        \
        vt_iter->set_scan_flag(params.scan_flag_);                                      \
        SERVER_LOG(DEBUG, "set schema guard");                                          \
      }                                                                                 \
    }                                                                                   \
    ret;                                                                                \
  })

#define BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA                                              \
        [&]() {                                                                         \
          if (!processed) {                                                             \
            processed = true;                                                           \
            switch (pure_tid) {

#define END_CREATE_VT_ITER_SWITCH_LAMBDA                                                \
              default: {                                                                \
                processed = false;                                                      \
                break;                                                                  \
              }                                                                         \
            }                                                                           \
          }                                                                             \
        }();


ObVirtualTableIteratorFactory::ObVirtualTableIteratorFactory(ObVTIterCreator &vt_iter_creator) :
    ObIVirtualTableIteratorFactory(),
    vt_iter_creator_(vt_iter_creator)
{
}

ObVirtualTableIteratorFactory::ObVirtualTableIteratorFactory(ObRootService &root_service,
                                                             common::ObAddr &addr,
                                                             common::ObServerConfig *config) :
    ObIVirtualTableIteratorFactory(),
    vt_iter_creator_(root_service, addr, config)
{
}

ObVirtualTableIteratorFactory::~ObVirtualTableIteratorFactory()
{
}

int ObVirtualTableIteratorFactory::create_virtual_table_iterator(ObVTableScanParam &params,
                                                                 ObVirtualTableIterator *&vt_iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vt_iter_creator_.create_vt_iter(params, vt_iter))) {
    SERVER_LOG(WARN, "create_vt_iter failed", K(params.index_id_), K(ret));
  }
  return ret;
}

int ObVirtualTableIteratorFactory::revert_virtual_table_iterator(ObVirtualTableIterator *vt_iter)
{
int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == vt_iter)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "vt_iter is NULL, can not free it");
  } else {
    vt_iter->~ObVirtualTableIterator();
    vt_iter = NULL;
  }
  return ret;
}

int ObVirtualTableIteratorFactory::check_can_create_iter(common::ObVTableScanParam &params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vt_iter_creator_.check_can_create_iter(params))) {
    SERVER_LOG(WARN, "create_vt_iter failed", K(params.index_id_), K(ret));
  }
  return ret;
}

int ObVTIterCreator::get_latest_expected_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t table_version,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *&t_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid schema version", K(table_version), K(ret));
  // FIXME: ATTENTION!!! get_cluster_schema_guard() will be deprecated soon, don't use again.
  } else if (OB_FAIL(root_service_.get_schema_service().get_cluster_schema_guard(schema_guard))) {
    SERVER_LOG(WARN, "get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, t_schema))) {
    SERVER_LOG(WARN, "get table schema failed", K(tenant_id), K(table_id), K(ret));
  } else if(NULL == t_schema
            || OB_UNLIKELY(table_version != t_schema->get_schema_version())) {
    ret = OB_SCHEMA_ERROR;
    if (NULL != t_schema) {
      SERVER_LOG(WARN, "current schema version", K(t_schema->get_schema_version()));
    }
    t_schema = NULL;
  }
  return ret;
}

// %table.get_index_name() contain the name of table name, to simplify the check logic,
// we only check if %index_name is a suffix of %table.get_index_name()
int ObVTIterCreator::check_is_index(const share::schema::ObTableSchema &table,
    const char *index_name, bool &is_index) const
{
  int ret = OB_SUCCESS;
  if (NULL == index_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_name));
  } else {
    is_index = false;
    int64_t index_name_len = strlen(index_name);
    if (table.is_index_table()) {
      ObString name;
      if (OB_FAIL(table.get_index_name(name))) {
        LOG_WARN("get index name failed", K(ret));
      } else {
        if (name.case_compare(index_name) == 0) {
          is_index = true;
        } else if (name.length() > index_name_len + 1
            && '_' == name.ptr()[name.length() - index_name_len - 1]) {
          is_index = (0 == strncasecmp(name.ptr() + name.length() - index_name_len,
              index_name, index_name_len));
        }
      }
    }
  }
  return ret;
}

int ObVTIterCreator::create_vt_iter(ObVTableScanParam &params,
                                    ObVirtualTableIterator *&vt_iter)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const ObTableSchema *index_schema = NULL;

  ObSchemaGetterGuard &schema_guard = params.get_schema_guard();
  // We also support index on virtual table.
  uint64_t index_id = params.index_id_;
  const uint64_t tenant_id = params.tenant_id_;
  if (OB_UNLIKELY(OB_INVALID_ID == index_id)) {
     ret = OB_INVALID_ARGUMENT;
     SERVER_LOG(WARN, "invalid index_id", K(index_id), K(ret));
  } else if (OB_FAIL(get_latest_expected_schema(tenant_id,
                                                index_id,
                                                params.schema_version_,
                                                schema_guard,
                                                index_schema))) {
    SERVER_LOG(WARN,
               "failed to get expected schema",
               K(ret),
               K(index_id),
               K(params.schema_version_),
               K(index_schema),
               K(&root_service_.get_schema_service()));
  } else {
    if (index_schema->is_index_table()) {
      // access via index
      if (OB_FAIL(schema_guard.get_table_schema(
                  index_schema->get_tenant_id(),
                  index_schema->get_data_table_id(),
                  table_schema))) {
        LOG_WARN("get data table schema failed", K(ret), K(index_id), K(index_schema->get_data_table_id()));
      } else if (NULL == table_schema) {
        ret = OB_TABLE_NOT_EXIST;
        SERVER_LOG(WARN, "failed to get table schema", K(ret), K(index_id), K(index_schema->get_data_table_id()));
      }
    } else {
      // access data table directly
      table_schema = index_schema;
    }

    if (OB_SUCC(ret)) {
      uint64_t data_table_id = table_schema->get_table_id();
      uint64_t org_pure_tid = data_table_id;
      uint64_t pure_tid = is_oracle_mapping_virtual_table(org_pure_tid)
                          ? get_origin_tid_by_oracle_mapping_tid(org_pure_tid)
                          : org_pure_tid;
      int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_VT_CREATE_ERROR);
      if (OB_UNLIKELY(OB_SUCCESS != simulate_error)) {
        ret = simulate_error;
      } else if (OB_UNLIKELY(is_only_rs_virtual_table(data_table_id) && !root_service_.is_full_service())) {
        if (!root_service_.in_service()) {
          ret = OB_RS_SHUTDOWN;
          SERVER_LOG(WARN, "rootservice is shutdown", K(ret));
        } else {
          ret = OB_RS_NOT_MASTER;
          SERVER_LOG(WARN, "rootservice is not the master", K(ret));
        }
      } else if (OB_ISNULL(params.op_)
                 || OB_ISNULL(params.op_->get_eval_ctx().exec_ctx_.get_my_session())
                 || OB_ISNULL(index_schema)
                 || OB_ISNULL(GCTX.sql_engine_)
                 || OB_ISNULL(GCTX.schema_service_)
                 || OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN,
                   "some variable is NULL",
                   K(ret),
                   KP(params.op_),
                   KP(index_schema),
                   KP(GCTX.sql_engine_),
                   KP(GCTX.schema_service_),
                   KP(GCTX.sql_proxy_));
      } else if (!lib::is_oracle_mode()
                 && (is_ora_sys_view_table(pure_tid)
                     || is_ora_virtual_table(pure_tid))) {
        ret = OB_NOT_SUPPORTED;
        SERVER_LOG(WARN, "access oracle's virtual table/sys view in mysql mode",
                   K(ret), K(pure_tid));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "access oracle's virtual table/sys view in mysql mode");
      } else {
        void *tmp_ptr = NULL;
        ObIAllocator &allocator = *params.scan_allocator_;
        ObSQLSessionInfo *session = params.op_->get_eval_ctx().exec_ctx_.get_my_session();
        uint64_t real_tenant_id = session->get_effective_tenant_id();

        bool processed = false;

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_SCHEMA_PRIVILEGES_OLD_TID: {
            ObInfoSchemaSchemaPrivilegesTable *schema_privileges = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaSchemaPrivilegesTable, schema_privileges))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              schema_privileges->set_allocator(&allocator);
              schema_privileges->set_tenant_id(real_tenant_id);
              schema_privileges->set_user_id(session->get_user_id());
              vt_iter = static_cast<ObVirtualTableIterator *>(schema_privileges);
            }
            break;
          }
          case OB_USER_PRIVILEGES_OLD_TID: {
            ObInfoSchemaUserPrivilegesTable *user_privileges = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaUserPrivilegesTable, user_privileges))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              user_privileges->set_allocator(&allocator);
              user_privileges->set_tenant_id(real_tenant_id);
              user_privileges->set_user_id(session->get_user_id());
              vt_iter = static_cast<ObVirtualTableIterator *>(user_privileges);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PRIVILEGE_TID: {
            ObTenantVirtualPrivilege *privilege_iter = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantVirtualPrivilege, privilege_iter))) {
              SERVER_LOG(WARN, "create virtual table iterator failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(privilege_iter);
            }
            break;
          }
          case OB_TABLE_PRIVILEGES_OLD_TID: {
            ObInfoSchemaTablePrivilegesTable *table_privileges = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaTablePrivilegesTable, table_privileges))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              table_privileges->set_allocator(&allocator);
              table_privileges->set_tenant_id(real_tenant_id);
              table_privileges->set_user_id(session->get_user_id());
              vt_iter = static_cast<ObVirtualTableIterator *>(table_privileges);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_ALL_TABLE_TID: {
            ObTenantAllTables *tenant_all_tables = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantAllTables, tenant_all_tables))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              tenant_all_tables->set_allocator(&allocator);
              tenant_all_tables->set_tenant_id(real_tenant_id);
              tenant_all_tables->set_sql_proxy(GCTX.sql_proxy_);
              vt_iter = static_cast<ObVirtualTableIterator *>(tenant_all_tables);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_TABLES_TID: {
            ObTenantShowTables *tenant_show_tables = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantShowTables, tenant_show_tables))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              tenant_show_tables->set_allocator(&allocator);
              tenant_show_tables->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(tenant_show_tables);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_SCHEMA_TID: {
            ObAllVirtualProxySchema *avps = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxySchema, avps))) {
              SERVER_LOG(ERROR, "fail to new", KR(ret), K(pure_tid));
            } else if (OB_FAIL(avps->init(
                params.force_refresh_lc_,
                root_service_.get_schema_service(),
                GCTX.location_service_,
                GCTX.sql_proxy_,
                &allocator))) {
              LOG_WARN("fail to init ObAllVirtualProxySchema", KR(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(avps);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_ROUTINE_TID: {
            ObAllVirtualProxyRoutine *proxy_routine = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxyRoutine, proxy_routine))) {
              LOG_ERROR("ObAllVirtualProxyRoutine construct failed", KR(ret));
            } else if (OB_FAIL(proxy_routine->init(
                root_service_.get_schema_service(),
                &allocator))) {
              LOG_WARN("fail to init ObAllVirtualProxyRoutine", KR(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(proxy_routine);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CORE_META_TABLE_TID: {
            ObCoreMetaTable *core_meta_table = NULL;
            if (!root_service_.is_major_freeze_done()) {
              // Some obtest cases detect rootservice status by select this virtual table
              ret = OB_SERVER_IS_INIT;
              RS_LOG(WARN, "RS major freeze not finished", KR(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObCoreMetaTable, core_meta_table))) {
              SERVER_LOG(ERROR, "ObCoreMetaTable construct failed", KR(ret));
            } else if (OB_FAIL(core_meta_table->init(root_service_.get_lst_operator(),
                                                      &schema_guard))) {
              SERVER_LOG(WARN, "core_meta_table init failed", KR(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(core_meta_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID: {
            ObVritualCoreInnerTable *core_all_table = NULL;
            const char *table_name = NULL;
            if (OB_FAIL(ObSchemaUtils::get_all_table_name(real_tenant_id, table_name))) {
              LOG_WARN("fail to get all table name", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVritualCoreInnerTable, core_all_table))) {
              SERVER_LOG(ERROR, "ObCoreAllTable construct failed", K(ret));
            } else if (OB_FAIL(core_all_table->init(*GCTX.sql_proxy_,
                                                    table_name,
                                                    pure_tid,
                                                    &schema_guard))) {
              SERVER_LOG(WARN, "core_all_table init failed", "table_name",
                                table_name, K(pure_tid), K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(core_all_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID: {
            ObVritualCoreInnerTable *core_column_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVritualCoreInnerTable, core_column_table))) {
              SERVER_LOG(ERROR, "ObCoreColumnTable construct failed", K(ret));
            } else if (OB_FAIL(core_column_table->init(*GCTX.sql_proxy_,
                                                       OB_ALL_COLUMN_TNAME,
                                                       pure_tid,
                                                       &schema_guard))) {
              SERVER_LOG(WARN, "core_column_table init failed", "table_name",
                          OB_ALL_COLUMN_TNAME, K(pure_tid), K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(core_column_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_BACKUP_SCHEDULE_TASK_TID: {
            ObAllBackupScheduleTaskStat *all_task_stat = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_UNLIKELY(NULL == omt)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get tenant fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllBackupScheduleTaskStat, all_task_stat, omt))) {
              SERVER_LOG(ERROR, "ObAllBackupScheduleTaskStat construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(all_task_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_TID: {
            ObAllVirtualLSReplicaTaskPlan *task_plan = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualLSReplicaTaskPlan, task_plan))) {
              SERVER_LOG(ERROR, "failed to init ObAllVirtualLSReplicaTaskPlan", KR(ret));
            } else if (OB_FAIL(task_plan->init(
                                   root_service_.get_schema_service(),
                                   root_service_.get_root_balancer().get_disaster_recovery_worker()))) {
              SERVER_LOG(WARN, "all_virtual_ls_replica_task_plan table init failed", KR(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(task_plan);
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID: {
            ObUpgradeInspection *upgrade_insepction = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObUpgradeInspection, upgrade_insepction))) {
              SERVER_LOG(ERROR, "ObUpgradeInspection construct failed", K(ret));
            } else if (OB_FAIL(upgrade_insepction->init(root_service_.get_schema_service(),
                                                        root_service_.get_root_inspection()))) {
              SERVER_LOG(WARN, "all_virtual_upgrade_inspection table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(upgrade_insepction);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TID: {
            ObAllVirtualTenantMemstoreInfo *gv_tenant_memstore_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTenantMemstoreInfo, gv_tenant_memstore_info))) {
              SERVER_LOG(ERROR, "ObAllVirtualTenantMemstoreInfo construct failed", K(ret));
            } else {
              gv_tenant_memstore_info->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(gv_tenant_memstore_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MEMORY_INFO_TID: {
            ObAllVirtualMemoryInfo *all_virtual_memory_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualMemoryInfo, all_virtual_memory_info))) {
              all_virtual_memory_info->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_memory_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_OBRPC_STAT_TID: {
            ObVirtualObRpcSendStat *rpc_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObVirtualObRpcSendStat, rpc_stat))) {
              rpc_stat->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(rpc_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TID: {
            ObAllVirtualSysParameterStat *all_virtual_sys_parameter_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualSysParameterStat,
                                          all_virtual_sys_parameter_stat))) {
              vt_iter = static_cast<ObAllVirtualSysParameterStat *>(all_virtual_sys_parameter_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID: {
            ObAllVirtualTenantParameterStat *all_virtual_tenant_parameter_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTenantParameterStat,
                                          all_virtual_tenant_parameter_stat))) {
              if (OB_FAIL(all_virtual_tenant_parameter_stat->init(params.scan_flag_.is_show_seed_))) {
                SERVER_LOG(WARN, "init tenant parameter stat iter fail", KR(ret), K(real_tenant_id),
                    K(params.scan_flag_.is_show_seed_));
              } else {
                vt_iter = static_cast<ObAllVirtualTenantParameterStat *>(all_virtual_tenant_parameter_stat);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_TID: {
            ObAllVirtualTenantParameterInfo *all_virtual_tenant_parameter_info = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTenantParameterInfo,
                                          all_virtual_tenant_parameter_info))) {
              vt_iter = static_cast<ObAllVirtualTenantParameterInfo *>(all_virtual_tenant_parameter_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MEMSTORE_INFO_TID: {
            ObAllVirtualMemstoreInfo *all_virtual_memstore_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualMemstoreInfo, all_virtual_memstore_info))) {
              SERVER_LOG(ERROR, "ObAllVirtualMemstoreInfo construct failed", K(ret));
            } else {
              all_virtual_memstore_info->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_memstore_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MINOR_FREEZE_INFO_TID: {
            ObAllVirtualMinorFreezeInfo *all_virtual_minor_freeze_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualMinorFreezeInfo, all_virtual_minor_freeze_info))) {
              SERVER_LOG(ERROR, "ObAllVirtualMinorFreezeInfo construct failed", K(ret));
            } else {
              all_virtual_minor_freeze_info->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_minor_freeze_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LS_INFO_TID: {
            ObAllVirtualLSInfo *all_virtual_ls_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualLSInfo, all_virtual_ls_info))) {
              SERVER_LOG(ERROR, "ObAllVirtualLSInfo construct failed", K(ret));
            } else {
              all_virtual_ls_info->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_ls_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_OBJ_LOCK_TID: {
            ObAllVirtualObjLock *all_virtual_obj_lock = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualObjLock, all_virtual_obj_lock))) {
              SERVER_LOG(ERROR, "ObAllVirtualObjLock construct failed", K(ret));
            } else {
              all_virtual_obj_lock->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_obj_lock);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_INFO_TID: {
            ObAllVirtualTabletInfo *all_virtual_tablet_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTabletInfo, all_virtual_tablet_info))) {
              SERVER_LOG(ERROR, "ObAllVirtualTabletInfo construct failed", K(ret));
            } else {
              all_virtual_tablet_info->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_tablet_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TX_DATA_TID: {
            ObAllVirtualTxData *all_virtual_tx_data = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTxData, all_virtual_tx_data))) {
              SERVER_LOG(ERROR, "ObAllVirtualMemstoreInfo construct failed", K(ret));
            } else {
              all_virtual_tx_data->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_tx_data);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TX_DATA_TABLE_TID: {
            ObAllVirtualTxDataTable *all_virtual_tx_data_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTxDataTable, all_virtual_tx_data_table))) {
              SERVER_LOG(ERROR, "ObAllVirtualMemstoreInfo construct failed", K(ret));
            } else {
              all_virtual_tx_data_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_tx_data_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANSACTION_FREEZE_CHECKPOINT_TID: {
            ObAllVirtualFreezeCheckpointInfo *all_virtual_transaction_freeze_checkpoint = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualFreezeCheckpointInfo,
                  all_virtual_transaction_freeze_checkpoint))) {
              SERVER_LOG(ERROR, "ObAllVirtualFreezeCheckpointInfo construct failed", K(ret));
            } else {
              all_virtual_transaction_freeze_checkpoint->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_transaction_freeze_checkpoint);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANSACTION_CHECKPOINT_TID: {
            ObAllVirtualTransCheckpointInfo *all_virtual_trans_checkpoint = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTransCheckpointInfo,
                  all_virtual_trans_checkpoint))) {
              SERVER_LOG(ERROR, "ObAllVirtualTransCheckpointInfo construct failed", K(ret));
            } else {
              all_virtual_trans_checkpoint->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_trans_checkpoint);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CHECKPOINT_TID: {
            ObAllVirtualCheckpointInfo *all_virtual_checkpoint = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualCheckpointInfo,
                  all_virtual_checkpoint))) {
              SERVER_LOG(ERROR, "ObAllVirtualCheckpointInfo construct failed", K(ret));
            } else {
              all_virtual_checkpoint->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_checkpoint);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLE_MGR_TID: {
            ObAllVirtualTableMgr *table_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTableMgr, table_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualMemstoreInfo construct failed", K(ret));
            } else if (OB_FAIL(table_mgr->init(&allocator))) {
              SERVER_LOG(WARN, "failed to init all virtual table mgr", K(ret));
            } else {
              table_mgr->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(table_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_STORAGE_META_MEMORY_STATUS_TID: {
            ObAllVirtualStorageMetaMemoryStatus *mem_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualStorageMetaMemoryStatus, mem_status))) {
              SERVER_LOG(ERROR, "ObAllVirtualStorageMetaMemoryStatus construct failed", K(ret));
            } else if (OB_FAIL(mem_status->init(&allocator, addr_))) {
              SERVER_LOG(WARN, "failed to init ObAllVirtualStorageMetaMemoryStatus", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(mem_status);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_POINTER_STATUS_TID: {
            ObAllVirtualTabletPtr *tablet_ptr_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTabletPtr, tablet_ptr_status))) {
              SERVER_LOG(ERROR, "ObAllVirtualTabletPtr construct failed", K(ret));
            } else if (OB_FAIL(tablet_ptr_status->init(&allocator, addr_))) {
              SERVER_LOG(WARN, "failed to init ObAllVirtualTabletPtr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(tablet_ptr_status);
            }
            break;
          }
          case OB_ALL_VIRTUAL_RAID_STAT_TID: {
            ObAllVirtualRaidStat *raid_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualRaidStat, raid_stat))) {
              SERVER_LOG(ERROR, "ObAllVirtualDiskStat construct failed", K(ret));
            } else if (OB_FAIL(raid_stat->init(addr_))) {
              SERVER_LOG(WARN, "failed to init all_virtual_raid_stat", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(raid_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_DDL_KV_INFO_TID: {
            ObAllVirtualTabletDDLKVInfo *ddl_kv_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTabletDDLKVInfo, ddl_kv_info))) {
              SERVER_LOG(ERROR, "ObAllVirtualTabletDDLKVInfo construct failed", K(ret));
            } else {
              ddl_kv_info->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(ddl_kv_info);
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_ALL_VIRTUAL_DUP_LS_LEASE_MGR_TID: {
            ObAllVirtualDupLSLeaseMgr *dup_ls_lease_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDupLSLeaseMgr,
                                          dup_ls_lease_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualDupLSLeaseMgr construct failed", K(ret));
            } else if (OB_FAIL(dup_ls_lease_mgr->init(addr_))) {
              SERVER_LOG(WARN, "all_virtual_dup_ls_lease_mgr init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(dup_ls_lease_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DUP_LS_TABLETS_TID: {
            ObAllVirtualDupLSTablets *dup_ls_tablets = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDupLSTablets,
                                          dup_ls_tablets))) {
              SERVER_LOG(ERROR, "failed to init ObAllVirtualDupLSTabletsr", K(ret));
            } else if (OB_FAIL(dup_ls_tablets->init(addr_))) {
              SERVER_LOG(WARN, "fail to init all_virtual_dup_ls_tablets", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(dup_ls_tablets);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DUP_LS_TABLET_SET_TID: {
            ObAllVirtualDupLSTabletSet *dup_ls_tablet_set = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDupLSTabletSet,
                                          dup_ls_tablet_set))) {
              SERVER_LOG(ERROR, "failed to init ObAllVirtualDMmlStats", K(ret));
            } else if (OB_FAIL(dup_ls_tablet_set->init(addr_))) {
              SERVER_LOG(WARN, "fail to init all_virtual_dup_ls_tablet_set", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(dup_ls_tablet_set);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_STAT_TID: {
            ObGVTxStat *gv_tx_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTxStat, gv_tx_stat))) {
              SERVER_LOG(ERROR, "ObGVTxStat construct failed", K(ret));
            } else if (OB_FAIL(gv_tx_stat->init())) {
              SERVER_LOG(WARN, "fail to init all_virtual_trans_stat", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(gv_tx_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_SCHEDULER_TID: {
            ObGVTxSchedulerStat *gv_tx_scheduler_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTxSchedulerStat, gv_tx_scheduler_stat))) {
              SERVER_LOG(ERROR, "ObGVTxSchedulerStat construct failed", K(ret));
            } else if (OB_FAIL(gv_tx_scheduler_stat->init())) {
              SERVER_LOG(WARN, "fail to init all_virtual_trans_scheduler", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(gv_tx_scheduler_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID: {
            ObGVTxLockStat *gv_tx_lock_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTxLockStat, gv_tx_lock_stat))) {
              SERVER_LOG(ERROR, "ObGVTxLockStat construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(gv_tx_lock_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_CTX_MGR_STAT_TID: {
            ObGVTxCtxMgrStat *gv_tx_ctx_mgr_stat = NULL;
            transaction::ObTransService *txs = MTL(transaction::ObTransService*);
            if (OB_UNLIKELY(NULL == txs)) {
              SERVER_LOG(WARN, "invalid argument", KP(txs));
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTxCtxMgrStat,
                                                 gv_tx_ctx_mgr_stat, txs))) {
              SERVER_LOG(ERROR, "gv_tx_ctx_mgr_stat construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(gv_tx_ctx_mgr_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TID: {
              ObAllPlanCacheBase *pcs = NULL;
              bool is_index = false;
              if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
                LOG_WARN("check is index failed", K(ret));
              } else if (is_index) {
                SERVER_LOG(DEBUG,
                            "scan __all_virtual_plan_cache_stat table using tenant_id",
                            K(pure_tid));
                if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllPlanCacheStatI1, pcs))) {
                  LOG_WARN("new virtual table failed", K(ret));
                }
              } else {
                OZ(NEW_VIRTUAL_TABLE(ObAllPlanCacheStat, pcs));
              }

              if (OB_SUCC(ret)) {
                vt_iter = static_cast<ObVirtualTableIterator *>(pcs);
              }
            } break;
          case OB_ALL_VIRTUAL_PLAN_STAT_TID: {
            ObAllPlanCacheBase *pcs = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVSql, pcs))) {
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(pcs);
            }
          } break;
          case OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID: {
            ObPlanCachePlanExplain *px = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObPlanCachePlanExplain, px))) {
              SERVER_LOG(WARN, "fail to allocate vtable iterator", K(ret));
            } else {
              px->set_allocator(&allocator);
              // px->set_plan_cache_manager(GCTX.sql_engine_->get_plan_cache_manager());
              vt_iter = static_cast<ObVirtualTableIterator *>(px);
            }
          } break;
          case OB_ALL_VIRTUAL_PS_STAT_TID: {
            ObAllVirtualPsStat *ps_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualPsStat, ps_stat))) {
              SERVER_LOG(ERROR, "ObAllVritualPsStat construct failed", K(ret));
            } else if (OB_ISNULL(ps_stat)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "ps_stat init failed", K(ret));
            } else {
              // init code
              vt_iter = static_cast<ObAllVirtualPsStat *>(ps_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PS_ITEM_INFO_TID: {
            ObAllVirtualPsItemInfo *ps_item_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualPsItemInfo, ps_item_info))) {
              SERVER_LOG(ERROR, "ObAllVritualPsCacheStat construct failed", K(ret));
            } else if (OB_ISNULL(ps_item_info)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "ps_item_info init failed", K(ret));
            } else {
              // init code
              vt_iter = static_cast<ObAllVirtualPsItemInfo *>(ps_item_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID: {
            ObAllVirtualProxyPartitionInfo *pi = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxyPartitionInfo, pi))) {
              SERVER_LOG(ERROR, "fail to new", K(pure_tid), K(ret));
            } else {
              pi->set_schema_service(root_service_.get_schema_service());
              pi->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(pi);
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_ALL_VIRTUAL_PROXY_PARTITION_TID: {
            ObAllVirtualProxyPartition *pi = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxyPartition, pi))) {
              SERVER_LOG(ERROR, "fail to new", K(pure_tid), K(ret));
            } else {
              pi->set_schema_service(root_service_.get_schema_service());
              pi->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(pi);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID: {
            ObAllVirtualProxySubPartition *pi = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxySubPartition, pi))) {
              SERVER_LOG(ERROR, "fail to new", K(pure_tid), K(ret));
            } else {
              pi->set_schema_service(root_service_.get_schema_service());
              pi->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(pi);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID:
          {
            ObSessionVariables *session_variables = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObSessionVariables, session_variables))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              const ObSysVariableSchema *sys_variable_schema = NULL;
              if (OB_FAIL(schema_guard.get_sys_variable_schema(real_tenant_id, sys_variable_schema))) {
                SERVER_LOG(WARN, "get sys variable schema failed", K(ret));
              } else if (OB_ISNULL(sys_variable_schema)) {
                ret = OB_TENANT_NOT_EXIST;
                SERVER_LOG(WARN, "sys variable schema is null", K(ret));
              } else {
                session_variables->set_sys_variable_schema(sys_variable_schema);
                vt_iter = static_cast<ObVirtualTableIterator *>(session_variables);
              }
            }
            break;
          }
          case OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID:
          {
            ObGlobalVariables *global_variables = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGlobalVariables, global_variables))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              global_variables->set_sql_proxy(GCTX.sql_proxy_);
              const ObSysVariableSchema *sys_variable_schema = NULL;
              if (OB_FAIL(schema_guard.get_sys_variable_schema(real_tenant_id, sys_variable_schema))) {
                SERVER_LOG(WARN, "get sys variable schema failed", K(ret));
              } else if (OB_ISNULL(sys_variable_schema)) {
                ret = OB_TENANT_NOT_EXIST;
                SERVER_LOG(WARN, "sys variable schema is null", K(ret));
              } else {
                global_variables->set_sys_variable_schema(sys_variable_schema);
                vt_iter = static_cast<ObVirtualTableIterator *>(global_variables);
              }
            }
            break;
          }
          case OB_TENANT_VIRTUAL_TABLE_COLUMN_TID:
          {
            ObTableColumns *table_columns = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTableColumns, table_columns))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(table_columns);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_TABLE_INDEX_TID: {
            ObTableIndex *table_index = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTableIndex, table_index))) {
              table_index->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(table_index);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID: {
            ObShowCreateDatabase *create_database = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowCreateDatabase, create_database))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(create_database);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID:
          {
            ObShowCreateTable *create_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowCreateTable, create_table))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(create_table);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID:
          {
            ObShowCreateTablegroup *create_tablegroup = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowCreateTablegroup, create_tablegroup))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(create_tablegroup);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TID:
          {
            ObShowCreateProcedure *create_proc = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowCreateProcedure, create_proc))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(create_proc);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_TID:
          {
              ObShowCreateTrigger *create_tg = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowCreateTrigger, create_tg))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(create_tg);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_RESTORE_PREVIEW_TID:
          {
            ObTenantShowRestorePreview *restore_preview = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantShowRestorePreview, restore_preview))) {
              if (OB_FAIL(restore_preview->init())) {
                SERVER_LOG(WARN, "failed to init restore preview", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(restore_preview);
              }
            }
            break;
          }
          case OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID:
          {
            ObGetObjectDefinition *get_object_def = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObGetObjectDefinition, get_object_def))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(get_object_def);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID:
          {
            ObShowGrants *show_grants = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowGrants, show_grants))) {
              show_grants->set_tenant_id(real_tenant_id);
              show_grants->set_user_id(session->get_user_id());
              session->get_session_priv_info(show_grants->get_session_priv());
              vt_iter = static_cast<ObVirtualTableIterator *>(show_grants);
            }
            break;
          }
          case OB_SESSION_VARIABLES_TID: {
            ObInfoSchemaSessionVariablesTable *session_variables = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaSessionVariablesTable,
                                          session_variables))) {
              const ObSysVariableSchema *sys_variable_schema = NULL;
              if (OB_FAIL(schema_guard.get_sys_variable_schema(real_tenant_id, sys_variable_schema))) {
                SERVER_LOG(WARN, "get sys variable schema failed", K(ret));
              } else if (OB_ISNULL(sys_variable_schema)) {
                ret = OB_TENANT_NOT_EXIST;
                SERVER_LOG(WARN, "sys variable schema is null", K(ret));
              } else {
                session_variables->set_sys_variable_schema(sys_variable_schema);
                vt_iter = static_cast<ObVirtualTableIterator *>(session_variables);
              }
            }
            break;
          }
          case OB_GLOBAL_STATUS_TID: {
            ObInfoSchemaGlobalStatusTable *global_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaGlobalStatusTable, global_status))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              global_status->set_cur_session(session);
              global_status->set_global_ctx(&GCTX);
              vt_iter = static_cast<ObVirtualTableIterator *>(global_status);
            }
            break;
          }
          case OB_SESSION_STATUS_TID: {
            ObInfoSchemaSessionStatusTable *session_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaSessionStatusTable, session_status))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              session_status->set_cur_session(session);
              session_status->set_global_ctx(&GCTX);
              vt_iter = static_cast<ObVirtualTableIterator *>(session_status);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REFERENTIAL_CONSTRAINTS_OLD_TID: {
            ObInfoSchemaReferentialConstraintsTable *referential_constraint = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaReferentialConstraintsTable,
                                          referential_constraint))) {
              referential_constraint->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(referential_constraint);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLE_CONSTRAINTS_OLD_TID: {
            ObInfoSchemaTableConstraintsTable *table_constraint = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaTableConstraintsTable,
                                          table_constraint))) {
              table_constraint->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(table_constraint);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CHECK_CONSTRAINTS_OLD_TID: {
            ObInfoSchemaCheckConstraintsTable* check_constraint = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaCheckConstraintsTable, check_constraint))) {
              check_constraint->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(check_constraint);
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TID: {
            ObInfoSchemaColumnsTable *columns = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaColumnsTable, columns))) {
              columns->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(columns);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROCESSLIST_TID:
          {
            ObShowProcesslist *processlist_show = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowProcesslist, processlist_show))) {
              processlist_show->set_session_mgr(GCTX.session_mgr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(processlist_show);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SESSION_INFO_TID:
          {
            ObAllVirtualSessionInfo *session_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualSessionInfo, session_info))) {
              session_info->set_session_mgr(GCTX.session_mgr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(session_info);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_DATABASE_STATUS_TID: {
            ObShowDatabaseStatus *database_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowDatabaseStatus, database_status))) {
              database_status->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(database_status);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_TENANT_STATUS_TID: {
            ObShowTenantStatus *tenant_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowTenantStatus, tenant_status))) {
              tenant_status->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(tenant_status);
            }
            break;
          }
          case OB_USER_TID: {
            ObMySQLUserTable *mysql_user_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObMySQLUserTable, mysql_user_table))) {
              mysql_user_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(mysql_user_table);
            }
            break;
          }
          case OB_DB_TID: {
            ObMySQLDBTable *mysql_db_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObMySQLDBTable, mysql_db_table))) {
              mysql_db_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(mysql_db_table);
            }
            break;
          }
          case OB_PROC_TID: {
            ObMySQLProcTable *mysql_proc_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObMySQLProcTable, mysql_proc_table))) {
              mysql_proc_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(mysql_proc_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRIGGERS_OLD_TID: {
            ObInfoSchemaTriggersTable *tg_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaTriggersTable, tg_table))) {
              tg_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(tg_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARAMETERS_OLD_TID: {
            ObInformationParametersTable *information_parameters_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInformationParametersTable, information_parameters_table))) {
              information_parameters_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObInformationParametersTable *>(information_parameters_table);
            }
            break;
          }
          case OB_PARTITIONS_OLD_TID: {
            ObInfoSchemaPartitionsTable *partitions_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaPartitionsTable, partitions_table))) {
              partitions_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(partitions_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_KVCACHE_INFO_TID: {
            ObInfoSchemaKvCacheTable *cache_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaKvCacheTable, cache_table))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              cache_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(cache_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_KVCACHE_HANDLE_LEAK_INFO_TID: {
            ObAllVirtualKVCacheHandleLeakInfo *handle_leak_info_table = nullptr;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualKVCacheHandleLeakInfo, handle_leak_info_table))) {
              SERVER_LOG(ERROR, "Fail to create __all_virtual_kvcache_handle_leak_info table", K(ret));
            } else {
              handle_leak_info_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(handle_leak_info_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CONCURRENCY_OBJECT_POOL_TID: {
            ObAllConcurrencyObjectPool *object_pool = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllConcurrencyObjectPool, object_pool))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(object_pool);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MALLOC_SAMPLE_INFO_TID: {
            ObMallocSampleInfo *malloc_sample_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObMallocSampleInfo, malloc_sample_info))) {
              malloc_sample_info->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(malloc_sample_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MEM_LEAK_CHECKER_INFO_TID: {
            ObMemLeakCheckerInfo *leak_checker = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObMemLeakCheckerInfo, leak_checker))) {
              leak_checker->set_allocator(&allocator);
              leak_checker->set_tenant_id(session->get_priv_tenant_id());
              leak_checker->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(leak_checker);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LATCH_TID: {
            ObAllLatch *all_latch = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllLatch, all_latch))) {
              all_latch->set_allocator(&allocator);
              all_latch->set_addr(addr_);
              vt_iter = all_latch;
            }
            break;
          }
          case OB_TENANT_VIRTUAL_WARNING_TID: {
            ObTenantVirtualWarning *warning = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantVirtualWarning,
                                          warning))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(warning);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRACE_SPAN_INFO_TID:
          {
            ObVirtualSpanInfo *trace = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualSpanInfo,
                                          trace))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              trace->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(trace);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SHOW_TRACE_TID:
          {
            ObVirtualShowTrace *show_trace = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualShowTrace,
                                          show_trace))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              show_trace->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(show_trace);
            }
            break;
          }
          case OB_ALL_VIRTUAL_FLT_CONFIG_TID:
          {
            ObVirtualFLTConfig *flt_conf = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualFLTConfig,
                                          flt_conf))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(flt_conf);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_CURRENT_TENANT_TID: {
            ObTenantVirtualCurrentTenant *curr_tenant = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantVirtualCurrentTenant,
                                          curr_tenant))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              curr_tenant->set_sql_proxy(GCTX.sql_proxy_);
              vt_iter = static_cast<ObVirtualTableIterator *>(curr_tenant);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TID: {
            ObAllDataTypeClassTable *all_tc_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllDataTypeClassTable, all_tc_table))) {
              all_tc_table->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_tc_table);
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_ALL_VIRTUAL_DATA_TYPE_TID: {
            ObAllDataTypeTable *all_type_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllDataTypeTable, all_type_table))) {
              all_type_table->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_type_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SESSION_EVENT_TID: {
            ObAllVirtualSessionEvent *session_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG,
                          "scan __all_virtual_session_event table using tenant_id",
                          K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSessionEventI1, session_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              OZ(NEW_VIRTUAL_TABLE(ObAllVirtualSessionEvent, session_table));
            }

            if (OB_SUCC(ret)) {
              session_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(session_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SESSION_WAIT_TID: {
            ObAllVirtualSessionWait *session_wait_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG,
                          "scan __all_virtual_session_wait table using tenant_id",
                          K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSessionWaitI1, session_wait_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              OZ(NEW_VIRTUAL_TABLE(ObAllVirtualSessionWait, session_wait_table));
            }

            if (OB_SUCC(ret)) {
              session_wait_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(session_wait_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TID: {
            ObAllVirtualSessionWaitHistory *session_wait_history_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG,
                          "scan __all_virtual_session_wait_history table using tenant_id",
                          K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSessionWaitHistoryI1,
                                            session_wait_history_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              OZ(NEW_VIRTUAL_TABLE(ObAllVirtualSessionWaitHistory, session_wait_history_table));
            }

            if (OB_SUCC(ret)) {
              session_wait_history_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(session_wait_history_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SESSTAT_TID: {
            ObAllVirtualSessionStat *session_stat_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG,
                          "scan __all_virtual_session_stat table using tenant_id",
                          K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSessionStatI1, session_stat_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              OZ(NEW_VIRTUAL_TABLE(ObAllVirtualSessionStat, session_stat_table));
            }

            if (OB_SUCC(ret)) {
              session_stat_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(session_stat_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DISK_STAT_TID: {
            ObInfoSchemaDiskStatTable *disk_stat_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaDiskStatTable, disk_stat_table))) {
              disk_stat_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(disk_stat_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SYSSTAT_TID: {
            ObAllVirtualSysStat *sys_stat_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG,
                          "scan __all_virtual_sys_stat table using tenant_id",
                          K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSysStatI1, sys_stat_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              OZ(NEW_VIRTUAL_TABLE(ObAllVirtualSysStat, sys_stat_table));
            }

            if (OB_SUCC(ret)) {
              sys_stat_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(sys_stat_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SYSTEM_EVENT_TID: {
            ObAllVirtualSysEvent *sys_event_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG,
                          "scan __all_virtual_sys_event table using tenant_id",
                          K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSysEventI1, sys_event_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              OZ(NEW_VIRTUAL_TABLE(ObAllVirtualSysEvent, sys_event_table));
            }

            if (OB_SUCC(ret)) {
              sys_event_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(sys_event_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_TID: {
            ObInfoSchemaQueryResponseTimeTable* query_response_time_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaQueryResponseTimeTable, query_response_time_table))) {
              query_response_time_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(query_response_time_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_AUDIT_TID: {
            ObGvSqlAudit *sql_audit_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObGvSqlAudit, sql_audit_table))) {
              sql_audit_table->set_allocator(&allocator);
              sql_audit_table->set_addr(addr_);

              // optimizer choose to use index i1('tenant_id', 'request_id') to scan
              bool is_index = false;
              if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
                LOG_WARN("check is index failed", K(ret));
              } else if (is_index) {
                sql_audit_table->use_index_scan();
              }

              if (OB_SUCC(ret)) {
                vt_iter = static_cast<ObVirtualTableIterator *>(sql_audit_table);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_TID: {
            ObAllVirtualServerObjectPool *server_object_pool = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualServerObjectPool, server_object_pool))) {
              server_object_pool->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(server_object_pool);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_STATNAME_TID: {
            ObTenantVirtualStatname *stat_name = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantVirtualStatname, stat_name))) {
              stat_name->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(stat_name);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_EVENT_NAME_TID: {
            ObTenantVirtualEventName *event_name = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantVirtualEventName, event_name))) {
              event_name->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(event_name);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ENGINE_TID: {
            ObAllVirtualEngineTable *all_engines_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualEngineTable, all_engines_table))) {
              all_engines_table->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_engines_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LOG_STAT_TID: {
            ObAllVirtualPalfStat *palf_stat = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_UNLIKELY(NULL == omt)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get tenant fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualPalfStat, palf_stat, omt))) {
              SERVER_LOG(ERROR, "ObAllVirtualPalfStat construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(palf_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_TID : {
            ObAllVirtualArbMemberInfo *virtual_arb_info = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_UNLIKELY(NULL == omt)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get tenant fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualArbMemberInfo, virtual_arb_info))) {
              SERVER_LOG(ERROR, "ObAllVirtualArbMemberInfo construct fail", K(ret));
            } else if (OB_FAIL(virtual_arb_info->init(GCTX.schema_service_, omt))) {
              SERVER_LOG(WARN, "fail to init ObAllVirtualArbMemberInfo", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(virtual_arb_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_TID: {
            ObAllVirtualArbServiceStatus *virtual_arb_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualArbServiceStatus, virtual_arb_status))) {
              SERVER_LOG(ERROR, "ObAllVirtualArbServiceStatus construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(virtual_arb_status);
            }
            break;
          }
	        case OB_ALL_VIRTUAL_APPLY_STAT_TID: {
            ObAllVirtualApplyStat *apply_stat = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_UNLIKELY(NULL == omt)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get tenant fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualApplyStat, apply_stat, omt))) {
              SERVER_LOG(ERROR, "ObAllVirtualApplyStat construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObAllVirtualApplyStat *>(apply_stat);
            }
            break;
          }
	        case OB_ALL_VIRTUAL_REPLAY_STAT_TID: {
            ObAllVirtualReplayStat *replay_stat = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_UNLIKELY(NULL == omt)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get tenant fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualReplayStat, replay_stat, omt))) {
              SERVER_LOG(ERROR, "ObAllVirtualReplayStat construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObAllVirtualReplayStat *>(replay_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ARCHIVE_STAT_TID: {
            ObAllVirtualLSArchiveStat *ls_archive_stat = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_UNLIKELY(NULL == omt)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get tenant fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualLSArchiveStat, ls_archive_stat, omt))) {
              SERVER_LOG(ERROR, "ObAllVirtualLSArchiveStat construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(ls_archive_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_HA_DIAGNOSE_TID: {
            ObAllVirtualHADiagnose *diagnose_info = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_UNLIKELY(NULL == omt)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get tenant fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualHADiagnose, diagnose_info, omt))) {
              SERVER_LOG(ERROR, "ObAllVirtualHADiagnose construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObAllVirtualHADiagnose *>(diagnose_info);
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_ALL_VIRTUAL_FILES_TID: {
            ObAllVirtualFilesTable *all_files_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualFilesTable, all_files_table))) {
              all_files_table->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_files_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TID: {
            ObVirtualProxyServerStat *server_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualProxyServerStat, server_stat))) {
              SERVER_LOG(ERROR, "fail to new ObVirtualProxyServerStat", K(pure_tid), K(ret));
            } else if (OB_FAIL(server_stat->init(*GCTX.schema_service_,
                                                  GCTX.sql_proxy_,
                                                  config_))) {
              SERVER_LOG(WARN, "fail to init ObVirtualProxyServerStat", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(server_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_TID: {
            ObVirtualProxySysVariable *sys_variable = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualProxySysVariable, sys_variable))) {
              SERVER_LOG(ERROR, "fail to new ObVirtualProxySysVariable", K(pure_tid), K(ret));
            } else if (OB_FAIL(sys_variable->init(*GCTX.schema_service_, config_))) {
              SERVER_LOG(WARN, "fail to init ObVirtualProxySysVariable", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(sys_variable);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_TID: {
            ObAllVirtualTabletSSTableMacroInfo *sstable_macro_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTabletSSTableMacroInfo, sstable_macro_info))) {
              if (OB_FAIL(sstable_macro_info->init(&allocator, addr_))) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualPartitionSSTableMergeInfo, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(sstable_macro_info);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TID: {
            ObVirtualSqlPlanMonitor *plan_monitor = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObVirtualSqlPlanMonitor, plan_monitor))) {
              plan_monitor->set_allocator(&allocator);
              plan_monitor->set_addr(addr_);
              // optimizer choose to use index i1('tenant_id', 'request_id') to scan
              bool is_index = false;
              if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
                LOG_WARN("check is index failed", K(ret));
              } else if (is_index) {
                plan_monitor->use_index_scan();
              }
              if (OB_SUCC(ret)) {
                vt_iter = static_cast<ObVirtualTableIterator *>(plan_monitor);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_ASH_TID: {
            ObVirtualASH *ash = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG,
                          "scan __all_virtual_ash table using index",
                          K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualASHI1, ash))) {
                LOG_WARN("new ash index virtual table failed", K(ret));
              }
            } else {
              OZ(NEW_VIRTUAL_TABLE(ObVirtualASH, ash));
            }
            if (OB_SUCC(ret)) {
              ash->set_allocator(&allocator);
              ash->set_addr(addr_);
              if (OB_SUCC(ret)) {
                vt_iter = static_cast<ObVirtualTableIterator *>(ash);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TID: {
            ObVirtualSqlMonitorStatname *stat_name = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObVirtualSqlMonitorStatname, stat_name))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(stat_name);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_OUTLINE_TID: {
            ObTenantVirtualOutline *outline = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantVirtualOutline, outline))) {
              outline->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(outline);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID: {
            ObTenantVirtualConcurrentLimitSql *limit_sql = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantVirtualConcurrentLimitSql, limit_sql))) {
              limit_sql->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator *>(limit_sql);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SYS_TASK_STATUS_TID: {
            ObAllVirtualSysTaskStatus *sys_task_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualSysTaskStatus, sys_task_status))) {
              if (OB_FAIL(sys_task_status->init(SYS_TASK_STATUS_MGR))) {
                SERVER_LOG(WARN, "fail to init migration_status", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(sys_task_status);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_TID: {
            ObAllVirtualMacroBlockMarkerStatus *all_virtual_marker_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualMacroBlockMarkerStatus, all_virtual_marker_status))) {
              blocksstable::ObMacroBlockMarkerStatus marker_status;
              if (OB_FAIL(OB_SERVER_BLOCK_MGR.get_marker_status(marker_status))) {
                SERVER_LOG(WARN, "failed to get marker info", K(ret));
              } else if (OB_FAIL(all_virtual_marker_status->init(marker_status))) {
                SERVER_LOG(WARN, "fail to init marker_status", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_marker_status);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_BUFFER_INFO_TID: {
            ObAllVirtualTabletBufferInfo *all_virtual_tablet_buffer_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTabletBufferInfo, all_virtual_tablet_buffer_info))) {
              if (OB_FAIL(all_virtual_tablet_buffer_info->init(addr_))) {
                SERVER_LOG(WARN, "fail to init tablet_buffer_info", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_tablet_buffer_info);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_BLACKLIST_TID: {
            ObAllVirtualServerBlacklist *server_blacklist = NULL;
            if (OB_SUCCESS == NEW_VIRTUAL_TABLE(ObAllVirtualServerBlacklist, server_blacklist)) {
              server_blacklist->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(server_blacklist);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TID: {
            ObAllVirtualLockWaitStat *lock_wait_stat = NULL;
            if (OB_SUCCESS == NEW_VIRTUAL_TABLE(ObAllVirtualLockWaitStat, lock_wait_stat)) {
              vt_iter = static_cast<ObVirtualTableIterator *>(lock_wait_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_TID: {
            ObVirtualArchiveDestStatus *archive_dest_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualArchiveDestStatus, archive_dest_status))) {
              SERVER_LOG(ERROR, "fail to new ObVirtualArchiveDestStatus", K(ret));
            } else if (OB_FAIL(archive_dest_status->init(GCTX.sql_proxy_))) {
              SERVER_LOG(ERROR, "fail to init ObVirtualArchiveDestStatus", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(archive_dest_status);
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_ALL_VIRTUAL_LONG_OPS_STATUS_TID: {
            ObAllVirtualLongOpsStatus *long_ops_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualLongOpsStatus, long_ops_status))) {
              SERVER_LOG(ERROR, "fail to placement new ObAllVirtualLongOpsStatus", K(ret));
            } else {
              long_ops_status->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(long_ops_status);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_CHARSET_TID: {
            ObTenantVirtualCharset *charset = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantVirtualCharset,
                                          charset))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(charset);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_COLLATION_TID: {
            ObTenantVirtualCollation *collation = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantVirtualCollation,
                                          collation))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(collation);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_TID: {
            ObAllVirtualTenantMemstoreAllocatorInfo *tenant_mem_allocator_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTenantMemstoreAllocatorInfo,
                                          tenant_mem_allocator_info))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(tenant_mem_allocator_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_IO_STAT_TID: {
            ObAllVirtualIOStat *all_io_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualIOStat, all_io_stat))) {
              SERVER_LOG(WARN, "failed to allocate ObAllVirtualIOStat", K(ret));
            } else {
              vt_iter = static_cast<ObAllVirtualIOStat *>(all_io_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_TID: {
            ObVirtualBadBlockTable *bad_block_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObVirtualBadBlockTable, bad_block_table))) {
              if (OB_FAIL(bad_block_table->init(addr_))) {
                SERVER_LOG(WARN, "bad_block_table init failed", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(bad_block_table);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_DTL_CHANNEL_TID: {
            ObAllVirtualDtlChannel *dtl_ch = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualDtlChannel, dtl_ch))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(dtl_ch);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DTL_MEMORY_TID: {
            ObAllVirtualDtlMemory *dtl_mem = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualDtlMemory, dtl_mem))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(dtl_mem);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PX_WORKER_STAT_TID: {
            ObAllPxWorkerStatTable *px_worker_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllPxWorkerStatTable, px_worker_stat))) {
              px_worker_stat->set_allocator(&allocator);
              px_worker_stat->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(px_worker_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PX_P2P_DATAHUB_TID: {
            ObAllPxP2PDatahubTable *px_p2p_datahub = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllPxP2PDatahubTable, px_p2p_datahub))) {
              px_p2p_datahub->set_allocator(&allocator);
              px_p2p_datahub->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(px_p2p_datahub);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_STORE_STAT_TID: {
            ObAllVirtualTabletStoreStat *part_table_store_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTabletStoreStat, part_table_store_stat))) {
              if (OB_FAIL(part_table_store_stat->init())) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualTabletStoreStat,", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(part_table_store_stat);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TID: {
            ObAllVirtualServerSchemaInfo *server_schema_info = NULL;
            share::schema::ObMultiVersionSchemaService &schema_service =
                                                          root_service_.get_schema_service();
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualServerSchemaInfo,
                                          server_schema_info, schema_service))) {
              SERVER_LOG(ERROR, "ObAllVirtualServerSchemaInfo construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(server_schema_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SCHEMA_MEMORY_TID: {
            ObAllVirtualSchemaMemory *schema_memory = NULL;
            share::schema::ObMultiVersionSchemaService &schema_service =
                                                          root_service_.get_schema_service();
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSchemaMemory,
                                          schema_memory, schema_service))) {
              SERVER_LOG(ERROR, "ObAllVirtualSchemaMemory construct fail", KR(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(schema_memory);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SCHEMA_SLOT_TID: {
            ObAllVirtualSchemaSlot *schema_slot = NULL;
            share::schema::ObMultiVersionSchemaService &schema_service =
                                                          root_service_.get_schema_service();
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSchemaSlot,
                                          schema_slot, schema_service))) {
              SERVER_LOG(ERROR, "ObAllVirtualSchemaSlot construct fail", KR(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(schema_slot);
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_TID: {
            ObAllVirtualMemoryContextStat *memory_context_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualMemoryContextStat, memory_context_stat))) {
              SERVER_LOG(ERROR, "ObAllVirtualMemoryContextStat construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(memory_context_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DUMP_TENANT_INFO_TID: {
            ObAllVirtualDumpTenantInfo *dump_tenant = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDumpTenantInfo, dump_tenant))) {
              SERVER_LOG(ERROR, "ObAllVirtualDumpTenantInfo construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(dump_tenant);
            }
            break;
          }
          case OB_ALL_VIRTUAL_AUDIT_OPERATION_TID: {
            ObAllVirtualAuditOperationTable *audit_operation_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualAuditOperationTable, audit_operation_table))) {
              SERVER_LOG(ERROR, "ObAllVirtualAuditOperationTable construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(audit_operation_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_AUDIT_ACTION_TID: {
            ObAllVirtualAuditActionTable *audit_action_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualAuditActionTable, audit_action_table))) {
              SERVER_LOG(ERROR, "ObAllVirtualAuditActionTable construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(audit_action_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_TID: {
            ObSqlWorkareaHistoryStat *wa_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObSqlWorkareaHistoryStat, wa_stat))) {
              vt_iter = static_cast<ObSqlWorkareaHistoryStat *>(wa_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_TID: {
            ObSqlWorkareaActive *wa_active = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObSqlWorkareaActive, wa_active))) {
              vt_iter = static_cast<ObSqlWorkareaActive *>(wa_active);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_TID: {
            ObSqlWorkareaHistogram *wa_hist = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObSqlWorkareaHistogram, wa_hist))) {
              vt_iter = static_cast<ObSqlWorkareaHistogram *>(wa_hist);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_TID: {
            ObSqlWorkareaMemoryInfo *wa_memory_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObSqlWorkareaMemoryInfo, wa_memory_info))) {
              vt_iter = static_cast<ObSqlWorkareaMemoryInfo *>(wa_memory_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_UNIT_TID: {
            ObAllVirtualUnit *unit = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualUnit, unit))) {
              SERVER_LOG(ERROR, "ObAllVirtualUnit construct failed", K(ret));
            } else if (OB_FAIL(unit->init(addr_))) {
              SERVER_LOG(WARN, "failed to init all_virtual_unit", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(unit);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_TID: {
            ObAllVirtualServer *server = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualServer, server))) {
              SERVER_LOG(ERROR, "ObAllVirtualServer construct failed", KR(ret));
            } else if (OB_FAIL(server->init(addr_))) {
              SERVER_LOG(WARN, "failed to init all_virtual_server", KR(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(server);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ID_SERVICE_TID: {
            ObAllVirtualIDService *id_service = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualIDService, id_service))) {
              SERVER_LOG(ERROR, "ObAllVirtualIDService construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(id_service);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_TID: {
            ObAllVirtualTimestampService *timestamp_service = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTimestampService, timestamp_service))) {
              SERVER_LOG(ERROR, "ObAllVirtualTimestampService construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(timestamp_service);
            }
            break;
          }
          case OB_ALL_VIRTUAL_OPEN_CURSOR_TID: {
            ObVirtualOpenCursorTable *open_cursors = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualOpenCursorTable, open_cursors))) {
              SERVER_LOG(ERROR, "ObVirtual open cursor table failed", K(ret));
            } else {
              open_cursors->set_allocator(&allocator);
              open_cursors->set_session_mgr(GCTX.session_mgr_);
              OZ (open_cursors->set_addr(addr_));
              OX (vt_iter = static_cast<ObVirtualOpenCursorTable*>(open_cursors));
            }
            break;
          }
          case OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_TID: {
            ObAllVirtualDagWarningHistory *dag_warning_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDagWarningHistory, dag_warning_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualDagWarningHistory construct failed", K(ret));
            } else if (OB_FAIL(dag_warning_mgr->init())) {
              SERVER_LOG(WARN, "failed to init dag_warning_mgr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(dag_warning_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DAG_TID: {
            ObAllVirtualDag *dag_info_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDag, dag_info_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualDag construct failed", K(ret));
            } else if (OB_FAIL(dag_info_mgr->init())) {
              SERVER_LOG(WARN, "failed to init dag_info_mgr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(dag_info_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DAG_SCHEDULER_TID: {
            ObAllVirtualDagScheduler *dag_scheduler_info_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDagScheduler, dag_scheduler_info_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualDagScheduler construct failed", K(ret));
            } else if (OB_FAIL(dag_scheduler_info_mgr->init())) {
              SERVER_LOG(WARN, "failed to init dag_scheduler_info_mgr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(dag_scheduler_info_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_TID: {
            ObAllVirtualServerCompactionProgress *progress_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualServerCompactionProgress, progress_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualServerCompactionProgress construct failed", K(ret));
            } else if (OB_FAIL(progress_mgr->init())) {
              SERVER_LOG(WARN, "failed to init progress_mgr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(progress_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_COMPACTION_EVENT_HISTORY_TID: {
            ObAllVirtualServerCompactionEventHistory *event_history = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualServerCompactionEventHistory, event_history))) {
              SERVER_LOG(ERROR, "ObAllVirtualServerCompactionEventHistory construct failed", K(ret));
            } else if (OB_FAIL(event_history->init())) {
              SERVER_LOG(WARN, "failed to init event_history", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(event_history);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_TID:{
            ObAllVirtualTabletCompactionProgress *progress_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTabletCompactionProgress, progress_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualPartitionCompactionProgress construct failed", K(ret));
            } else if (OB_FAIL(progress_mgr->init())) {
              SERVER_LOG(WARN, "failed to init progress_mgr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(progress_mgr);
            }
            break;
          }

          case OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_TID:{
            ObAllVirtualCompactionDiagnoseInfo *info_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualCompactionDiagnoseInfo, info_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualCompactionDiagnoseInfo construct failed", K(ret));
            } else if (OB_FAIL(info_mgr->init())) {
              SERVER_LOG(WARN, "failed to init progress_mgr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(info_mgr);
            }
            break;
          }

          case OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_TID: {
            ObAllVirtualCompactionSuggestion *suggestion_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualCompactionSuggestion, suggestion_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualCompactionSuggestion construct failed", K(ret));
            } else if (OB_FAIL(suggestion_mgr->init())) {
              SERVER_LOG(WARN, "failed to init suggestion_mgr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(suggestion_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_TID: {
            ObAllVirtualTabletCompactionHistory *history = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTabletCompactionHistory, history))) {
              if (OB_FAIL(history->init())) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualPartitionComapctionHistory, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(history);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_IO_CALIBRATION_STATUS_TID: {
            ObAllVirtualIOCalibrationStatus *calibration_status = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualIOCalibrationStatus, calibration_status))) {
              if (OB_FAIL(calibration_status->init(addr_))) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualIOCalibrationStatus, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(calibration_status);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_IO_BENCHMARK_TID: {
            ObAllVirtualIOBenchmark *io_benchmark = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualIOBenchmark, io_benchmark))) {
              if (OB_FAIL(io_benchmark->init(addr_))) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualIOBenchmark, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(io_benchmark);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_IO_QUOTA_TID: {
            ObAllVirtualIOQuota *io_quota= nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualIOQuota, io_quota))) {
              if (OB_FAIL(io_quota->init(addr_))) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualIOQuota, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(io_quota);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_COMPACTION_INFO_TID: {
            ObAllVirtualTabletCompactionInfo *info_mgr = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTabletCompactionInfo, info_mgr))) {
              if (OB_FAIL(info_mgr->init(&allocator, addr_))) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualTabletCompactionInfo", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(info_mgr);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_IO_SCHEDULER_TID: {
            ObAllVirtualIOScheduler *io_scheduler = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualIOScheduler, io_scheduler))) {
              if (OB_FAIL(io_scheduler->init(addr_))) {
                SERVER_LOG(WARN, "fail to init io_scheduler_tid, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(io_scheduler);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_TID: {
            ObAllVirtualTabletEncryptInfo *partition_encrypt_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTabletEncryptInfo, partition_encrypt_info))) {
              if (OB_FAIL(partition_encrypt_info->init(&allocator, addr_))) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualTabletEncryptInfo, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator *>(partition_encrypt_info);
              }
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

        BEGIN_CREATE_VT_ITER_SWITCH_LAMBDA
          case OB_ALL_VIRTUAL_TENANT_CTX_MEMORY_INFO_TID: {
            ObAllVirtualTenantCtxMemoryInfo *all_virtual_tenant_ctx_memory_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTenantCtxMemoryInfo,
                                          all_virtual_tenant_ctx_memory_info))) {
              all_virtual_tenant_ctx_memory_info->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_tenant_ctx_memory_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MASTER_KEY_VERSION_INFO_TID: {
            ObAllVirtualMasterKeyVersionInfo *master_key_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualMasterKeyVersionInfo, master_key_info))) {
              vt_iter = static_cast<ObAllVirtualMasterKeyVersionInfo *>(master_key_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_TID: {
            ObAllVirtualTenantMemoryInfo *all_virtual_tenant_memory_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTenantMemoryInfo,
                                          all_virtual_tenant_memory_info))) {
              all_virtual_tenant_memory_info->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_tenant_memory_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PX_TARGET_MONITOR_TID: {
            ObAllVirtualPxTargetMonitor *all_px_target_monitor = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualPxTargetMonitor, all_px_target_monitor))) {
              SERVER_LOG(WARN, "ObAllVirtualPxTargetMonitor construct failed", K(ret));
            } else if (OB_FAIL(all_px_target_monitor->init())) {
              SERVER_LOG(WARN, "init ObAllVirtualPxTargetMonitor failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(all_px_target_monitor);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DBLINK_INFO_TID: {
            ObAllVirtualDblinkInfo *dblink_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDblinkInfo, dblink_info))) {
              SERVER_LOG(ERROR, "ObVirtual dblink info table failed", K(ret));
            } else {
              dblink_info->set_allocator(&allocator);
              dblink_info->set_tenant_id(real_tenant_id);
              OZ (dblink_info->set_addr(addr_));
              OX (vt_iter = static_cast<ObAllVirtualDblinkInfo*>(dblink_info));
            }
            break;
          }
          case OB_ALL_VIRTUAL_LOAD_DATA_STAT_TID: {
            ObAllVirtualLoadDataStat *load_data_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualLoadDataStat, load_data_stat))) {
              SERVER_LOG(ERROR, "failed to init ObAllVirtualLoadDataStat", K(ret));
            } else {
              load_data_stat->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(load_data_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_KVCACHE_STORE_MEMBLOCK_TID: {
            ObAllVirtualKVCacheStoreMemblock *kvcache_store_memblock = nullptr;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualKVCacheStoreMemblock, kvcache_store_memblock))) {
              SERVER_LOG(ERROR, "Fail to create __all_virtual_kvcache_store_memblock", K(ret));
            } else {
              kvcache_store_memblock->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(kvcache_store_memblock);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_TID: {
            ObAllDtlIntermResultMonitor *dtl_interm_result_monitor = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllDtlIntermResultMonitor, dtl_interm_result_monitor))) {
              SERVER_LOG(ERROR, "failed to init ObAllDtlIntermResultMonitor", K(ret));
            } else {
              dtl_interm_result_monitor->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(dtl_interm_result_monitor);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DML_STATS_TID: {
            ObAllVirtualDMmlStats *dml_stats_result_monitor = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDMmlStats, dml_stats_result_monitor))) {
              SERVER_LOG(ERROR, "failed to init ObAllVirtualDMmlStats", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(dml_stats_result_monitor);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MDS_NODE_STAT_TID: {
            ObAllVirtualMdsNodeStat *mds_node_stat = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_UNLIKELY(NULL == omt)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get tenant fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualMdsNodeStat, mds_node_stat, omt))) {
              SERVER_LOG(ERROR, "ObAllVirtualMdsNodeStat construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObAllVirtualMdsNodeStat *>(mds_node_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MDS_EVENT_HISTORY_TID: {
            ObAllVirtualMdsEventHistory *mds_node_stat = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_UNLIKELY(NULL == omt)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get tenant fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualMdsEventHistory, mds_node_stat, omt))) {
              SERVER_LOG(ERROR, "ObAllVirtualMdsEventHistory construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObAllVirtualMdsEventHistory *>(mds_node_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_PLAN_TID: {
            ObAllVirtualSqlPlan *sql_plan_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualSqlPlan, sql_plan_table))) {
              sql_plan_table->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator *>(sql_plan_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_THREAD_TID: {
            ObAllVirtualThread *all_virtual_thread = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualThread, all_virtual_thread))) {
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_thread);
            }
            break;
          }
          case OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_TID: {
            ObAllVirtualOptStatGatherMonitor *opt_stats_gather_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualOptStatGatherMonitor, opt_stats_gather_stat))) {
              SERVER_LOG(ERROR, "failed to init ObAllVirtualOptStatGatherMonitor", K(ret));
            } else {
              opt_stats_gather_stat->set_allocator(&allocator);
              opt_stats_gather_stat->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator *>(opt_stats_gather_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_TID: {
            ObVirtualLSLogRestoreStatus *ls_log_restore_status = NULL;
            omt::ObMultiTenant *omt = GCTX.omt_;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualLSLogRestoreStatus, ls_log_restore_status))) {
              SERVER_LOG(ERROR, "failed to init ObVirtualLSLogRestoreStatus", K(ret));
            } else if (OB_FAIL(ls_log_restore_status->init(omt))) {
              SERVER_LOG(WARN, "fail to init ObVirtualLSLogRestoreStatus with omt", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(ls_log_restore_status);
            }
            break;
          }
          case OB_ALL_VIRTUAL_KV_CONNECTION_TID:
          {
            ObAllVirtualKvConnection *kv_connection = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualKvConnection, kv_connection))) {
              kv_connection->set_connection_mgr(&table::ObTableConnectionMgr::get_instance());
              vt_iter = static_cast<ObVirtualTableIterator *>(kv_connection);
            }
            break;
          }
        END_CREATE_VT_ITER_SWITCH_LAMBDA

#define AGENT_VIRTUAL_TABLE_CREATE_ITER
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef AGENT_VIRTUAL_TABLE_CREATE_ITER

#define ITERATE_VIRTUAL_TABLE_CREATE_ITER
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef ITERATE_VIRTUAL_TABLE_CREATE_ITER

#define ITERATE_PRIVATE_VIRTUAL_TABLE_CREATE_ITER
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef ITERATE_PRIVATE_VIRTUAL_TABLE_CREATE_ITER

        if (OB_SUCC(ret) && !processed) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid virtual table id",
                           K(ret), K(pure_tid), K(data_table_id), K(index_id));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(vt_iter)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "vt_iter is NULL", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObVTIterCreator::check_can_create_iter(ObVTableScanParam &params)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const ObTableSchema *index_schema = NULL;

  ObSchemaGetterGuard &schema_guard = params.get_schema_guard();
  // We also support index on virtual table.
  uint64_t index_id = params.index_id_;
  const uint64_t tenant_id = params.tenant_id_;
  if (OB_UNLIKELY(OB_INVALID_ID == index_id)) {
     ret = OB_INVALID_ARGUMENT;
     SERVER_LOG(WARN, "invalid index_id", K(index_id), K(ret));
  } else if (OB_FAIL(get_latest_expected_schema(tenant_id,
                                                index_id,
                                                params.schema_version_,
                                                schema_guard,
                                                index_schema))) {
    SERVER_LOG(WARN, "failed to get expected schema", K(ret),
               K(index_id), K(params.schema_version_), K(index_schema),
               K(&root_service_.get_schema_service()));
  } else {
    if (index_schema->is_index_table()) {
      // access via index
      if (OB_FAIL(schema_guard.get_table_schema(
                  index_schema->get_tenant_id(),
                  index_schema->get_data_table_id(),
                  table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(index_id), K(index_schema->get_data_table_id()));
      } else if (NULL == index_schema) {
        ret = OB_TABLE_NOT_EXIST;
        SERVER_LOG(WARN, "failed to get table schema",
                   K(ret), K(index_id), K(index_schema->get_data_table_id()));
      }
    } else {
      // access data table directly
      table_schema = index_schema;
    }

    if (OB_SUCC(ret)) {
      uint64_t data_table_id = table_schema->get_table_id();
      if (OB_UNLIKELY(is_only_rs_virtual_table(data_table_id) && !root_service_.is_full_service())) {
        if (!root_service_.in_service()) {
          ret = OB_RS_SHUTDOWN;
          SERVER_LOG(WARN, "rootservice is shutdown", K(ret));
        } else {
          ret = OB_RS_NOT_MASTER;
          SERVER_LOG(WARN, "rootservice is not the master", K(ret));
        }
      }
    }
  }
  return ret;
}


}/* ns observer*/
}/* ns oceanbase */
