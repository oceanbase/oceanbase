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
#include "observer/virtual_table/ob_information_referential_constraints_table.h"
#include "observer/virtual_table/ob_information_partitions_table.h"
#include "observer/virtual_table/ob_all_virtual_session_event.h"
#include "observer/virtual_table/ob_all_virtual_session_wait.h"
#include "observer/virtual_table/ob_all_virtual_session_wait_history.h"
#include "observer/virtual_table/ob_all_virtual_session_stat.h"
#include "observer/virtual_table/ob_all_virtual_clog_stat.h"
#include "observer/virtual_table/ob_all_storage_stat.h"
#include "observer/virtual_table/ob_all_disk_stat.h"
#include "observer/virtual_table/ob_mem_leak_checker_info.h"
#include "observer/virtual_table/ob_partition_sstable_image_info_table.h"
#include "observer/virtual_table/ob_all_latch.h"
#include "observer/virtual_table/ob_all_data_type_class_table.h"
#include "observer/virtual_table/ob_all_data_type_table.h"
#include "observer/virtual_table/ob_gv_tenant_memstore_info.h"
#include "observer/virtual_table/ob_all_virtual_server_memory_info.h"
#include "observer/virtual_table/ob_all_virtual_server_clog_stat.h"
#include "observer/virtual_table/ob_all_virtual_server_blacklist.h"
#include "observer/virtual_table/ob_all_virtual_sys_parameter_stat.h"
#include "observer/virtual_table/ob_all_virtual_tenant_parameter_stat.h"
#include "observer/virtual_table/ob_all_virtual_tenant_parameter_info.h"
#include "observer/virtual_table/ob_all_virtual_memstore_info.h"
#include "observer/virtual_table/ob_gv_partition_info.h"
#include "observer/virtual_table/ob_gv_sql_audit.h"
#include "observer/virtual_table/ob_gv_sql.h"
#include "observer/virtual_table/ob_show_database_status.h"
#include "observer/virtual_table/ob_show_tenant_status.h"
#include "observer/virtual_table/ob_all_virtual_sys_stat.h"
#include "observer/virtual_table/ob_all_virtual_sys_event.h"
#include "observer/virtual_table/ob_all_virtual_trans_stat.h"
#include "observer/virtual_table/ob_all_virtual_pg_partition_info.h"
#include "observer/virtual_table/ob_all_virtual_trans_lock_stat.h"
#include "observer/virtual_table/ob_all_virtual_duplicate_partition_mgr_stat.h"
#include "observer/virtual_table/ob_all_virtual_trans_result_info.h"
#include "observer/virtual_table/ob_all_virtual_trans_audit.h"
#include "observer/virtual_table/ob_all_virtual_trans_sql_audit.h"
#include "observer/virtual_table/ob_all_virtual_trans_mgr_stat.h"
#include "observer/virtual_table/ob_all_virtual_trans_mem_stat.h"
#include "observer/virtual_table/ob_all_virtual_weak_read_stat.h"
#include "observer/virtual_table/ob_all_virtual_election_info.h"
#include "observer/virtual_table/ob_all_virtual_election_group_info.h"
#include "observer/virtual_table/ob_all_virtual_election_mem_stat.h"
#include "observer/virtual_table/ob_all_virtual_election_event_history.h"
#include "observer/virtual_table/ob_tenant_partition_stat.h"
#include "observer/virtual_table/ob_tenant_virtual_statname.h"
#include "observer/virtual_table/ob_tenant_virtual_event_name.h"
#include "observer/virtual_table/ob_all_virtual_partition_replay_status.h"
#include "observer/virtual_table/ob_all_virtual_engine_table.h"
#include "observer/virtual_table/ob_all_virtual_files_table.h"
#include "observer/virtual_table/ob_all_plan_cache_stat.h"
#include "observer/virtual_table/ob_plan_cache_plan_explain.h"
#include "observer/virtual_table/ob_all_virtual_ps_stat.h"
#include "observer/virtual_table/ob_all_virtual_ps_item_info.h"
#include "observer/virtual_table/ob_show_processlist.h"
#include "observer/virtual_table/ob_show_interm_result.h"
#include "observer/virtual_table/ob_all_virtual_memory_info.h"
#include "observer/virtual_table/ob_all_virtual_tenant_disk_stat.h"
#include "observer/virtual_table/ob_all_virtual_raid_stat.h"
#include "observer/virtual_table/ob_virtual_obrpc_send_stat.h"
#include "observer/virtual_table/ob_virtual_trace_log.h"
#include "observer/virtual_table/ob_all_virtual_proxy_schema.h"
#include "observer/virtual_table/ob_virtual_proxy_server_stat.h"
#include "observer/virtual_table/ob_virtual_proxy_sys_variable.h"
#include "observer/virtual_table/ob_all_virtual_partition_sstable_merge_info.h"
#include "observer/virtual_table/ob_all_virtual_partition_sstable_macro_info.h"
#include "observer/virtual_table/ob_all_virtual_partition_store_info.h"
#include "observer/virtual_table/ob_virtual_sql_plan_monitor.h"
#include "observer/virtual_table/ob_virtual_sql_monitor_statname.h"
#include "observer/virtual_table/ob_virtual_sql_plan_statistics.h"
#include "observer/virtual_table/ob_virtual_sql_monitor.h"
#include "observer/virtual_table/ob_tenant_virtual_outline.h"
#include "observer/virtual_table/ob_tenant_virtual_concurrent_limit_sql.h"
#include "observer/virtual_table/ob_all_virtual_proxy_partition_info.h"
#include "observer/virtual_table/ob_all_virtual_proxy_partition.h"
#include "observer/virtual_table/ob_all_virtual_proxy_sub_partition.h"
#include "observer/virtual_table/ob_all_virtual_proxy_route.h"
#include "observer/virtual_table/ob_all_virtual_partition_amplification_stat.h"
#include "observer/virtual_table/ob_all_virtual_partition_migration_status.h"
#include "observer/virtual_table/ob_all_virtual_sys_task_status.h"
#include "observer/virtual_table/ob_all_virtual_macro_block_marker_status.h"
#include "observer/virtual_table/ob_all_virtual_lock_wait_stat.h"
#include "observer/virtual_table/ob_all_virtual_long_ops_status.h"
#include "observer/virtual_table/ob_all_virtual_partition_location.h"
#include "observer/virtual_table/ob_all_virtual_partition_item.h"
#include "observer/virtual_table/ob_all_virtual_tenant_memstore_allocator_info.h"
#include "observer/virtual_table/ob_all_virtual_server_object_pool.h"
#include "observer/virtual_table/ob_all_virtual_io_stat.h"
#include "observer/virtual_table/ob_all_virtual_bad_block_table.h"
#include "observer/virtual_table/ob_all_virtual_partition_split_info.h"
#include "observer/virtual_table/ob_agent_virtual_table.h"
#include "observer/virtual_table/ob_iterate_virtual_table.h"
#include "observer/virtual_table/ob_all_virtual_timestamp_service.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/virtual_table/ob_core_meta_table.h"
#include "rootserver/virtual_table/ob_virtual_core_inner_table.h"
#include "observer/virtual_table/ob_all_virtual_zone_stat.h"
#include "rootserver/virtual_table/ob_all_server_stat.h"
#include "rootserver/virtual_table/ob_all_rebalance_task_stat.h"
#include "rootserver/ob_root_inspection.h"
#include "rootserver/virtual_table/ob_all_tenant_stat.h"
#include "rootserver/virtual_table/ob_all_rebalance_tenant_stat.h"
#include "rootserver/virtual_table/ob_all_rebalance_map_stat.h"
#include "rootserver/virtual_table/ob_all_rebalance_map_item_stat.h"
#include "rootserver/virtual_table/ob_all_rebalance_unit_migrate_stat.h"
#include "rootserver/virtual_table/ob_all_rebalance_unit_distribution_stat.h"
#include "rootserver/virtual_table/ob_all_rebalance_unit_stat.h"
#include "rootserver/virtual_table/ob_all_rebalance_replica_stat.h"
#include "rootserver/virtual_table/ob_all_virtual_leader_stat.h"
#include "rootserver/virtual_table/ob_all_partition_table.h"
#include "rootserver/virtual_table/ob_all_virtual_rootservice_stat.h"
#include "rootserver/virtual_table/ob_all_replica_task.h"
#include "rootserver/virtual_table/ob_all_cluster.h"
#include "observer/virtual_table/ob_all_virtual_election_priority.h"
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
#include "observer/virtual_table/ob_all_virtual_partition_audit.h"
#include "observer/virtual_table/ob_all_virtual_partition_table_store_stat.h"
#include "observer/virtual_table/ob_all_virtual_server_schema_info.h"
#include "observer/virtual_table/ob_all_virtual_memory_context_stat.h"
#include "observer/virtual_table/ob_all_virtual_deadlock_stat.h"
#include "observer/virtual_table/ob_all_virtual_dump_tenant_info.h"
#include "observer/virtual_table/ob_all_virtual_reserved_table_mgr.h"
#include "observer/virtual_table/ob_all_virtual_dag_warning_history.h"
#include "observer/virtual_table/ob_tenant_show_restore_preview.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_virtual_table_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/ob_partition_migration_status.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "ob_all_virtual_pg_backup_log_archive_status.h"
#include "ob_all_virtual_server_backup_log_archive_status.h"
#include "observer/virtual_table/ob_all_virtual_table_modifications.h"
#include "observer/virtual_table/ob_all_virtual_trans_table_status.h"
#include "ob_all_virtual_pg_log_archive_stat.h"
#include "observer/virtual_table/ob_virtual_open_cursor_table.h"
#include "observer/virtual_table/ob_all_virtual_backupset_history_mgr.h"
#include "observer/virtual_table/ob_all_virtual_backup_clean_info.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace rootserver;
using namespace transaction;
namespace observer {
#define NEW_VIRTUAL_TABLE(virtual_table, vt, ...)                                      \
  ({                                                                                   \
    vt = NULL;                                                                         \
    if (OB_UNLIKELY(NULL == (tmp_ptr = allocator.alloc(sizeof(virtual_table))))) {     \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                 \
      SERVER_LOG(ERROR, "fail to alloc memory", K(pure_tid), K(ret));                  \
    } else if (OB_UNLIKELY(NULL == (vt = new (tmp_ptr) virtual_table(__VA_ARGS__)))) { \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                 \
      SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));                           \
    } else {                                                                           \
      vt_iter = static_cast<ObVirtualTableIterator*>(vt);                              \
      if (OB_FAIL(vt_iter->set_key_ranges(params.key_ranges_))) {                      \
        allocator.free(tmp_ptr);                                                       \
        SERVER_LOG(WARN, "fail to set key ranges", K(ret), K(params));                 \
      } else {                                                                         \
        if (lib::is_oracle_mode() && is_oracle_mapping_virtual_table(data_table_id)) { \
          vt_iter->set_convert_flag();                                                 \
        }                                                                              \
        vt_iter->set_session(session);                                                 \
        vt_iter->set_schema_guard(&schema_guard);                                      \
        vt_iter->set_table_schema(table_schema);                                       \
        vt_iter->set_index_schema(index_schema);                                       \
        vt_iter->set_scan_flag(params.scan_flag_);                                     \
        SERVER_LOG(DEBUG, "set schema guard");                                         \
      }                                                                                \
    }                                                                                  \
    ret;                                                                               \
  })

ObVirtualTableIteratorFactory::ObVirtualTableIteratorFactory(ObVTIterCreator& vt_iter_creator)
    : ObIVirtualTableIteratorFactory(), vt_iter_creator_(vt_iter_creator)
{}

ObVirtualTableIteratorFactory::ObVirtualTableIteratorFactory(
    ObRootService& root_service, common::ObAddr& addr, common::ObServerConfig* config)
    : ObIVirtualTableIteratorFactory(), vt_iter_creator_(root_service, addr, config)
{}

ObVirtualTableIteratorFactory::~ObVirtualTableIteratorFactory()
{}

int ObVirtualTableIteratorFactory::create_virtual_table_iterator(
    ObVTableScanParam& params, ObVirtualTableIterator*& vt_iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vt_iter_creator_.create_vt_iter(params, vt_iter))) {
    SERVER_LOG(WARN, "create_vt_iter failed", K(params.index_id_), K(ret));
  }
  return ret;
}

int ObVirtualTableIteratorFactory::revert_virtual_table_iterator(ObVirtualTableIterator* vt_iter)
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

int ObVTIterCreator::get_latest_expected_schema(const uint64_t table_id, const int64_t table_version,
    ObSchemaGetterGuard& schema_guard, const ObTableSchema*& t_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid schema version", K(table_version), K(ret));
  } else if (OB_FAIL(root_service_.get_schema_service().get_schema_guard(schema_guard))) {
    SERVER_LOG(WARN, "get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, t_schema))) {
    SERVER_LOG(WARN, "get table schema failed", K(table_id), K(ret));
  } else if (NULL == t_schema || OB_UNLIKELY(table_version != t_schema->get_schema_version())) {
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
int ObVTIterCreator::check_is_index(
    const share::schema::ObTableSchema& table, const char* index_name, bool& is_index) const
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
        } else if (name.length() > index_name_len + 1 && '_' == name.ptr()[name.length() - index_name_len - 1]) {
          is_index = (0 == strncasecmp(name.ptr() + name.length() - index_name_len, index_name, index_name_len));
        }
      }
    }
  }
  return ret;
}

int ObVTIterCreator::create_vt_iter(ObVTableScanParam& params, ObVirtualTableIterator*& vt_iter)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_schema = NULL;

  ObSchemaGetterGuard& schema_guard = params.get_schema_guard();
  // We also support index on virtual table.
  uint64_t index_id = params.index_id_;
  if (OB_UNLIKELY(OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid index_id", K(index_id), K(ret));
  } else if (OB_FAIL(get_latest_expected_schema(
                 params.pkey_.table_id_, params.schema_version_, schema_guard, table_schema))) {
    SERVER_LOG(WARN,
        "failed to get expected schema",
        K(ret),
        K(index_id),
        K(params.schema_version_),
        K(table_schema),
        K(&root_service_.get_schema_service()));
  } else {
    if (params.pkey_.table_id_ != index_id) {
      // access via index
      if (OB_FAIL(schema_guard.get_table_schema(index_id, index_schema))) {
      } else if (NULL == index_schema) {
        ret = OB_TABLE_NOT_EXIST;
        SERVER_LOG(WARN, "failed to get index schema", K(ret), "table_id", params.pkey_.table_id_, K(index_id));
      }
    } else {
      // access data table directly
      index_schema = table_schema;
    }

    if (OB_SUCC(ret)) {
      uint64_t data_table_id =
          index_schema->is_index_table() ? index_schema->get_data_table_id() : index_schema->get_table_id();
      uint64_t org_pure_tid = extract_pure_id(data_table_id);
      uint64_t pure_tid = is_oracle_mapping_virtual_table(org_pure_tid)
                              ? get_origin_tid_by_oracle_mapping_tid(org_pure_tid)
                              : org_pure_tid;
      if (OB_UNLIKELY(data_table_id != params.pkey_.table_id_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected table id", K(ret), K(data_table_id), K(params.pkey_.table_id_));
      } else if (OB_UNLIKELY(is_only_rs_virtual_table(data_table_id) && !root_service_.is_full_service())) {
        if (!root_service_.in_service()) {
          ret = OB_RS_SHUTDOWN;
          SERVER_LOG(WARN, "rootservice is shutdown", K(ret));
        } else {
          ret = OB_RS_NOT_MASTER;
          SERVER_LOG(WARN, "rootservice is not the master", K(ret));
        }
      } else if (OB_ISNULL(params.expr_ctx_.calc_buf_) || OB_ISNULL(params.expr_ctx_.my_session_) ||
                 OB_ISNULL(index_schema) || OB_ISNULL(GCTX.sql_engine_) || OB_ISNULL(GCTX.pt_operator_) ||
                 OB_ISNULL(GCTX.schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN,
            "some variable is NULL",
            K(ret),
            K(params.expr_ctx_.calc_buf_),
            K(params.expr_ctx_.my_session_),
            K(index_schema),
            K(GCTX.sql_engine_),
            K(GCTX.pt_operator_),
            K(GCTX.schema_service_));
      } else if (!lib::is_oracle_mode() && (is_ora_sys_view_table(pure_tid) || is_ora_virtual_table(pure_tid))) {
        ret = OB_NOT_SUPPORTED;
        SERVER_LOG(WARN, "access oracle's virtual table/sys view in mysql mode", K(ret), K(pure_tid));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "access oracle's virtual table/sys view in mysql mode");
      } else {
        void* tmp_ptr = NULL;
        ObIAllocator& allocator = *params.scan_allocator_;
        ObSQLSessionInfo* session = params.expr_ctx_.my_session_;
        uint64_t real_tenant_id = session->get_effective_tenant_id();
        switch (pure_tid) {
          case OB_SCHEMA_PRIVILEGES_TID: {
            ObInfoSchemaSchemaPrivilegesTable* schema_privileges = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaSchemaPrivilegesTable, schema_privileges))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              schema_privileges->set_allocator(&allocator);
              schema_privileges->set_tenant_id(real_tenant_id);
              schema_privileges->set_user_id(session->get_user_id());
              vt_iter = static_cast<ObVirtualTableIterator*>(schema_privileges);
            }
            break;
          }
          case OB_USER_PRIVILEGES_TID: {
            ObInfoSchemaUserPrivilegesTable* user_privileges = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaUserPrivilegesTable, user_privileges))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              user_privileges->set_allocator(&allocator);
              user_privileges->set_tenant_id(real_tenant_id);
              user_privileges->set_user_id(session->get_user_id());
              vt_iter = static_cast<ObVirtualTableIterator*>(user_privileges);
            }
            break;
          }
          case OB_TABLE_PRIVILEGES_TID: {
            ObInfoSchemaTablePrivilegesTable* table_privileges = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaTablePrivilegesTable, table_privileges))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              table_privileges->set_allocator(&allocator);
              table_privileges->set_tenant_id(real_tenant_id);
              table_privileges->set_user_id(session->get_user_id());
              vt_iter = static_cast<ObVirtualTableIterator*>(table_privileges);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_ALL_TABLE_TID: {
            ObTenantAllTables* tenant_all_tables = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantAllTables, tenant_all_tables))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              tenant_all_tables->set_allocator(&allocator);
              tenant_all_tables->set_tenant_id(real_tenant_id);
              tenant_all_tables->set_sql_proxy(GCTX.sql_proxy_);
              vt_iter = static_cast<ObVirtualTableIterator*>(tenant_all_tables);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_TABLES_TID: {
            ObTenantShowTables* tenant_show_tables = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantShowTables, tenant_show_tables))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              tenant_show_tables->set_allocator(&allocator);
              tenant_show_tables->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(tenant_show_tables);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_SCHEMA_TID: {
            ObAllVirtualProxySchema* avps = NULL;
            ObPartitionLocationCache* pl_cache = ObServer::get_instance().get_gctx().location_cache_;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxySchema, avps))) {
              SERVER_LOG(ERROR, "fail to new", K(pure_tid), K(ret));
            } else if (OB_FAIL(avps->set_pl_cache(pl_cache))) {
              SERVER_LOG(WARN, "fail to set pl cache", K(pl_cache), K(ret));
            } else {
              avps->set_schema_service(root_service_.get_schema_service());
              avps->set_sql_proxy(GCTX.sql_proxy_);
              avps->set_allocator(&allocator);
              avps->set_tenant_id(real_tenant_id);
              avps->set_config(config_);
              avps->set_refresh_hint(params.force_refresh_lc_);
              vt_iter = static_cast<ObVirtualTableIterator*>(avps);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CORE_META_TABLE_TID: {
            ObCoreMetaTable* core_meta_table = NULL;
            if (!root_service_.is_major_freeze_done()) {
              // Some obtest cases detect rootservice status by select this virtual table
              ret = OB_SERVER_IS_INIT;
              RS_LOG(WARN, "RS major freeze not finished", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObCoreMetaTable, core_meta_table))) {
              SERVER_LOG(ERROR, "ObCoreMetaTable construct failed", K(ret));
            } else if (OB_FAIL(core_meta_table->init(root_service_.get_pt_operator(), &schema_guard))) {
              SERVER_LOG(WARN, "core_meta_table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(core_meta_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CORE_ROOT_TABLE_TID: {
            ObVritualCoreInnerTable* core_root_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVritualCoreInnerTable, core_root_table))) {
              SERVER_LOG(ERROR, "ObCoreRootTable construct failed", K(ret));
            } else if (OB_FAIL(core_root_table->init(root_service_.get_sql_proxy(),
                           OB_ALL_ROOT_TABLE_TNAME,
                           combine_id(OB_SYS_TENANT_ID, pure_tid),
                           &schema_guard))) {
              SERVER_LOG(
                  WARN, "core_root_table init failed", "table_name", OB_ALL_ROOT_TABLE_TNAME, K(pure_tid), K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(core_root_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID: {
            ObVritualCoreInnerTable* core_all_table = NULL;
            const char* table_name = NULL;
            if (OB_FAIL(ObSchemaUtils::get_all_table_name(OB_SYS_TENANT_ID, table_name))) {
              LOG_WARN("fail to get all table name", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVritualCoreInnerTable, core_all_table))) {
              SERVER_LOG(ERROR, "ObCoreAllTable construct failed", K(ret));
            } else if (OB_FAIL(core_all_table->init(root_service_.get_sql_proxy(),
                           table_name,
                           combine_id(OB_SYS_TENANT_ID, pure_tid),
                           &schema_guard))) {
              SERVER_LOG(WARN, "core_all_table init failed", "table_name", table_name, K(pure_tid), K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(core_all_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_TABLE_TID: {
            ObAllPartitionTable* all_partition_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllPartitionTable, all_partition_table))) {
              SERVER_LOG(ERROR, "ObAllPartitionTable construct failed", K(ret));
            } else if (OB_FAIL(all_partition_table->init(root_service_.get_pt_operator(),
                           root_service_.get_schema_service(),
                           root_service_.get_server_mgr(),
                           &schema_guard))) {
              SERVER_LOG(WARN, "all_partition_table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_partition_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID: {
            ObVritualCoreInnerTable* core_column_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVritualCoreInnerTable, core_column_table))) {
              SERVER_LOG(ERROR, "ObCoreColumnTable construct failed", K(ret));
            } else if (OB_FAIL(core_column_table->init(root_service_.get_sql_proxy(),
                           OB_ALL_COLUMN_TNAME,
                           combine_id(OB_SYS_TENANT_ID, pure_tid),
                           &schema_guard))) {
              SERVER_LOG(WARN, "core_column_table init failed", "table_name", OB_ALL_COLUMN_TNAME, K(pure_tid), K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(core_column_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_FREEZE_INFO_TID: {
            ObVritualCoreInnerTable* freeze_info_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVritualCoreInnerTable, freeze_info_table))) {
              SERVER_LOG(ERROR, "ObCoreAllTable construct failed", K(ret));
            } else if (OB_FAIL(freeze_info_table->init(root_service_.get_sql_proxy(),
                           ObFreezeInfoProxy::OB_ALL_FREEZE_INFO_TNAME,
                           combine_id(OB_SYS_TENANT_ID, pure_tid),
                           &schema_guard))) {
              SERVER_LOG(WARN,
                  "freeze_info_table init failed",
                  "table_name",
                  ObFreezeInfoProxy::OB_ALL_FREEZE_INFO_TNAME,
                  K(pure_tid),
                  K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(freeze_info_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ZONE_STAT_TID: {
            ObAllVirtualZoneStat* all_zone_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualZoneStat, all_zone_stat))) {
              SERVER_LOG(ERROR, "ObAllVirtualZoneStat construct failed", K(ret));
            } else if (OB_FAIL(all_zone_stat->init(root_service_.get_schema_service(), GCTX.sql_proxy_))) {
              SERVER_LOG(WARN, "all_virtual_zone_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_zone_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_STAT_TID: {
            ObAllServerStat* all_server_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllServerStat, all_server_stat))) {
              SERVER_LOG(ERROR, "ObAllServerStat construct failed", K(ret));
            } else if (OB_FAIL(all_server_stat->init(root_service_.get_schema_service(),
                           root_service_.get_unit_mgr(),
                           root_service_.get_server_mgr(),
                           root_service_.get_leader_coordinator()))) {
              SERVER_LOG(WARN, "all_virtual_server_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_server_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REBALANCE_TASK_STAT_TID: {
            ObAllRebalanceTaskStat* all_task_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllRebalanceTaskStat, all_task_stat))) {
              SERVER_LOG(ERROR, "ObAllRebalanceTaskStat construct failed", K(ret));
            } else if (OB_FAIL(all_task_stat->init(
                           root_service_.get_schema_service(), root_service_.get_rebalance_task_mgr()))) {
              SERVER_LOG(WARN, "all_virtual_rebalance_task_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_task_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REBALANCE_TENANT_STAT_TID: {
            ObAllRebalanceTenantStat* rebalance_tenant_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllRebalanceTenantStat, rebalance_tenant_stat))) {
              SERVER_LOG(ERROR, "ObAllRebalanceTenantStat construct failed", K(ret));
            } else if (OB_FAIL(rebalance_tenant_stat->init(root_service_.get_schema_service(),
                           root_service_.get_unit_mgr(),
                           root_service_.get_server_mgr(),
                           root_service_.get_pt_operator(),
                           root_service_.get_remote_pt_operator(),
                           root_service_.get_zone_mgr(),
                           root_service_.get_rebalance_task_mgr(),
                           root_service_.get_root_balancer()))) {
              SERVER_LOG(WARN, "all_virtual_rebalance_tenant_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(rebalance_tenant_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REBALANCE_MAP_STAT_TID: {
            ObAllRebalanceMapStat* rebalance_map_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllRebalanceMapStat, rebalance_map_stat))) {
              SERVER_LOG(ERROR, "ObAllRebalanceMapStat construct failed", K(ret));
            } else if (OB_FAIL(rebalance_map_stat->init(root_service_.get_schema_service(),
                           root_service_.get_unit_mgr(),
                           root_service_.get_server_mgr(),
                           root_service_.get_pt_operator(),
                           root_service_.get_remote_pt_operator(),
                           root_service_.get_zone_mgr(),
                           root_service_.get_rebalance_task_mgr(),
                           root_service_.get_root_balancer()))) {
              SERVER_LOG(WARN, "all_virtual_rebalance_map_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(rebalance_map_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REBALANCE_MAP_ITEM_STAT_TID: {
            ObAllRebalanceMapItemStat* rebalance_map_item_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllRebalanceMapItemStat, rebalance_map_item_stat))) {
              SERVER_LOG(ERROR, "ObAllRebalanceMapItemStat construct failed", K(ret));
            } else if (OB_FAIL(rebalance_map_item_stat->init(root_service_.get_schema_service(),
                           root_service_.get_unit_mgr(),
                           root_service_.get_server_mgr(),
                           root_service_.get_pt_operator(),
                           root_service_.get_remote_pt_operator(),
                           root_service_.get_zone_mgr(),
                           root_service_.get_rebalance_task_mgr(),
                           root_service_.get_root_balancer()))) {
              SERVER_LOG(WARN, "all_virtual_rebalance_map_item_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(rebalance_map_item_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REBALANCE_UNIT_MIGRATE_STAT_TID: {
            ObAllRebalanceUnitMigrateStat* rebalance_unit_migrate_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllRebalanceUnitMigrateStat,
                    rebalance_unit_migrate_stat,
                    root_service_.get_unit_mgr(),
                    root_service_.get_leader_coordinator(),
                    root_service_.get_server_mgr(),
                    root_service_.get_zone_mgr()))) {
              SERVER_LOG(ERROR, "ObAllRebalanceUnitMigrateStat construct failed", K(ret));
            } else if (OB_FAIL(rebalance_unit_migrate_stat->init(root_service_.get_sql_proxy(),
                           *config_,
                           root_service_.get_schema_service(),
                           root_service_.get_root_balancer()))) {
              SERVER_LOG(WARN, "fail to init", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(rebalance_unit_migrate_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REBALANCE_UNIT_DISTRIBUTION_STAT_TID: {
            ObAllRebalanceUnitDistributionStat* rebalance_unit_dist_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllRebalanceUnitDistributionStat,
                    rebalance_unit_dist_stat,
                    root_service_.get_unit_mgr(),
                    root_service_.get_leader_coordinator(),
                    root_service_.get_server_mgr(),
                    root_service_.get_zone_mgr()))) {
              SERVER_LOG(ERROR, "ObAllRebalanceUnitDistributionStat construct failed", K(ret));
            } else if (OB_FAIL(rebalance_unit_dist_stat->init(root_service_.get_sql_proxy(),
                           *config_,
                           root_service_.get_schema_service(),
                           root_service_.get_root_balancer()))) {
              SERVER_LOG(WARN, "fail to init", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(rebalance_unit_dist_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REBALANCE_UNIT_STAT_TID: {
            ObAllRebalanceUnitStat* rebalance_unit_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllRebalanceUnitStat, rebalance_unit_stat))) {
              SERVER_LOG(ERROR, "ObAllRebalanceUnitStat construct failed", K(ret));
            } else if (OB_FAIL(rebalance_unit_stat->init(root_service_.get_schema_service(),
                           root_service_.get_unit_mgr(),
                           root_service_.get_server_mgr(),
                           root_service_.get_pt_operator(),
                           root_service_.get_remote_pt_operator(),
                           root_service_.get_zone_mgr(),
                           root_service_.get_rebalance_task_mgr(),
                           root_service_.get_root_balancer()))) {
              SERVER_LOG(WARN, "all_virtual_rebalance_unit_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(rebalance_unit_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REBALANCE_REPLICA_STAT_TID: {
            ObAllRebalanceReplicaStat* rebalance_replica_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllRebalanceReplicaStat, rebalance_replica_stat))) {
              SERVER_LOG(ERROR, "ObAllRebalanceReplicaStat construct failed", K(ret));
            } else if (OB_FAIL(rebalance_replica_stat->init(root_service_.get_schema_service(),
                           root_service_.get_unit_mgr(),
                           root_service_.get_server_mgr(),
                           root_service_.get_pt_operator(),
                           root_service_.get_remote_pt_operator(),
                           root_service_.get_zone_mgr(),
                           root_service_.get_rebalance_task_mgr(),
                           root_service_.get_root_balancer()))) {
              SERVER_LOG(WARN, "all_virtual_rebalance_replica_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(rebalance_replica_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LEADER_STAT_TID: {
            ObAllVirtualLeaderStat* all_virtual_leader_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualLeaderStat, all_virtual_leader_stat))) {
              SERVER_LOG(ERROR, "ObAllVirtualLeaderStat construct failed", K(ret));
            } else if (OB_FAIL(all_virtual_leader_stat->init(
                           root_service_.get_leader_coordinator(), root_service_.get_schema_service()))) {
              SERVER_LOG(WARN, "all_virtual_leader_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_virtual_leader_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ROOTSERVICE_STAT_TID: {
            ObAllVirtualRootserviceStat* viter = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualRootserviceStat, viter))) {
              SERVER_LOG(ERROR, "ObAllVirtualRootserviceStat construct failed", K(ret));
            } else if (OB_FAIL(viter->init(root_service_))) {
              SERVER_LOG(WARN, "init failed", K(ret));
            } else {
              vt_iter = viter;
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_AMPLIFICATION_STAT_TID: {
            ObAllPartitionAmplificationStat* partition_amplification_stat = NULL;
            storage::ObPartitionService* partition_service = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllPartitionAmplificationStat, partition_amplification_stat))) {
              SERVER_LOG(ERROR, "alloc failed", K(pure_tid), K(ret));
            } else if (NULL == (partition_service = &(ObServer::get_instance().get_partition_service()))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "Partition service is NULL");
            } else if (OB_FAIL(partition_amplification_stat->init(addr_, partition_service))) {
              SERVER_LOG(WARN, "all_virtual_partition_amplification_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(partition_amplification_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_STORE_INFO_TID: {
            ObAllPartitionStoreInfo* info = NULL;
            storage::ObPartitionService* partition_service = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllPartitionStoreInfo, info))) {
              SERVER_LOG(ERROR, "alloc failed", K(pure_tid), K(ret));
            } else if (NULL == (partition_service = &(ObServer::get_instance().get_partition_service()))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "Partition service is NULL");
            } else if (OB_FAIL(info->init(addr_, partition_service))) {
              SERVER_LOG(WARN, "all_virtual_partition_sorted_stores_info table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID: {
            ObUpgradeInspection* upgrade_insepction = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObUpgradeInspection, upgrade_insepction))) {
              SERVER_LOG(ERROR, "ObUpgradeInspection construct failed", K(ret));
            } else if (OB_FAIL(upgrade_insepction->init(
                           root_service_.get_schema_service(), root_service_.get_root_inspection()))) {
              SERVER_LOG(WARN, "all_virtual_upgrade_inspection table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(upgrade_insepction);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_STAT_TID: {
            ObAllTenantStat* all_tenant_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllTenantStat, all_tenant_stat))) {
              SERVER_LOG(ERROR, "ObAllTenantStat construct failed", K(ret));
            } else if (OB_FAIL(
                           all_tenant_stat->init(root_service_.get_schema_service(), root_service_.get_sql_proxy()))) {
              SERVER_LOG(WARN, "all_virtual_tenant_stat table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_tenant_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TID: {
            ObGVTenantMemstoreInfo* gv_tenant_memstore_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTenantMemstoreInfo, gv_tenant_memstore_info))) {
              SERVER_LOG(ERROR, "ObGVTenantMemstoreInfo construct failed", K(ret));
            } else {
              gv_tenant_memstore_info->set_addr(addr_);
              gv_tenant_memstore_info->set_tenant_mgr(&ObTenantManager::get_instance());
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_tenant_memstore_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_TID: {
            ObAllVirtualServerMemoryInfo* server_memory_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualServerMemoryInfo, server_memory_info))) {
              SERVER_LOG(WARN, "ObGVTenantMemstoreInfo construct failed", K(ret));
            } else {
              server_memory_info->set_addr(addr_);
              server_memory_info->set_tenant_mgr(&ObTenantManager::get_instance());
              vt_iter = static_cast<ObVirtualTableIterator*>(server_memory_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MEMORY_INFO_TID: {
            ObAllVirtualMemoryInfo* all_virtual_memory_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualMemoryInfo, all_virtual_memory_info))) {
              all_virtual_memory_info->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator*>(all_virtual_memory_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_OBRPC_STAT_TID: {
            ObVirtualObRpcSendStat* rpc_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObVirtualObRpcSendStat, rpc_stat))) {
              rpc_stat->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator*>(rpc_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TID: {
            ObAllVirtualSysParameterStat* all_virtual_sys_parameter_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualSysParameterStat, all_virtual_sys_parameter_stat))) {
              vt_iter = static_cast<ObAllVirtualSysParameterStat*>(all_virtual_sys_parameter_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID: {
            // TODO: show_tenant_id is not valid now, its valus is always -1
            // It is designed for executing following query in system tenant:
            //   show parameters like 'workarea_size_policy' tenant='tt1'
            // to show tenant config of tt1.
            uint64_t show_tenant_id = params.expr_ctx_.phy_plan_ctx_->get_tenant_id();
            if (OB_INVALID_TENANT_ID == show_tenant_id || OB_INVALID_ID == show_tenant_id) {
              show_tenant_id = real_tenant_id;
            }
            ObAllVirtualTenantParameterStat* all_virtual_tenant_parameter_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTenantParameterStat, all_virtual_tenant_parameter_stat))) {
              all_virtual_tenant_parameter_stat->set_exec_tenant(show_tenant_id);
              all_virtual_tenant_parameter_stat->set_show_seed(params.expr_ctx_.phy_plan_ctx_->get_show_seed());
              vt_iter = static_cast<ObAllVirtualTenantParameterStat*>(all_virtual_tenant_parameter_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_TID: {
            ObAllVirtualTenantParameterInfo* all_virtual_tenant_parameter_info = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTenantParameterInfo, all_virtual_tenant_parameter_info))) {
              vt_iter = static_cast<ObAllVirtualTenantParameterInfo*>(all_virtual_tenant_parameter_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MEMSTORE_INFO_TID: {
            ObAllVirtualMemstoreInfo* all_virtual_memstore_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualMemstoreInfo, all_virtual_memstore_info))) {
              SERVER_LOG(ERROR, "ObAllVirtualMemstoreInfo construct failed", K(ret));
            } else {
              storage::ObPartitionService* partition_service = &(OBSERVER.get_partition_service());
              all_virtual_memstore_info->set_addr(addr_);
              all_virtual_memstore_info->set_partition_service(partition_service);
              vt_iter = static_cast<ObVirtualTableIterator*>(all_virtual_memstore_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLE_MGR_TID: {
            ObAllVirtualTableMgr* table_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTableMgr, table_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualMemstoreInfo construct failed", K(ret));
            } else if (OB_FAIL(table_mgr->init())) {
              SERVER_LOG(WARN, "failed to init all_virtual_memstore_info", K(ret));
            } else {
              table_mgr->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(table_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_RAID_STAT_TID: {
            ObAllVirtualRaidStat* raid_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualRaidStat, raid_stat))) {
              SERVER_LOG(ERROR, "ObAllVirtualDiskStat construct failed", K(ret));
            } else if (OB_FAIL(raid_stat->init(addr_))) {
              SERVER_LOG(WARN, "failed to init all_virtual_raid_stat", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(raid_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_STAT_TID: {
            ObGVTransStat* gv_trans_stat = NULL;
            transaction::ObTransService* trans_service = OBSERVER.get_partition_service().get_trans_service();
            if (OB_UNLIKELY(NULL == trans_service)) {
              SERVER_LOG(WARN, "invalid argument");
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTransStat, gv_trans_stat, trans_service))) {
              SERVER_LOG(ERROR, "ObGVTransStatInfo construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_trans_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID: {
            ObGVTransLockStat* gv_trans_lock_stat = NULL;
            transaction::ObTransService* trans_service = OBSERVER.get_partition_service().get_trans_service();
            if (OB_UNLIKELY(NULL == trans_service)) {
              SERVER_LOG(WARN, "invalid argument");
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTransLockStat, gv_trans_lock_stat, trans_service))) {
              SERVER_LOG(ERROR, "ObGVTransLockStatInfo construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_trans_lock_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_RESULT_INFO_STAT_TID: {
            ObGVTransResultInfo* gv_trans_result_info = NULL;
            transaction::ObTransService* trans_service = OBSERVER.get_partition_service().get_trans_service();
            if (OB_UNLIKELY(NULL == trans_service)) {
              SERVER_LOG(WARN, "invalid argument");
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTransResultInfo, gv_trans_result_info, trans_service))) {
              SERVER_LOG(ERROR, "ObGVTransResultInfo construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_trans_result_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DUPLICATE_PARTITION_MGR_STAT_TID: {
            ObGVDuplicatePartitionMgrStat* stat = NULL;
            transaction::ObTransService* trans_service = OBSERVER.get_partition_service().get_trans_service();
            if (OB_UNLIKELY(NULL == trans_service)) {
              SERVER_LOG(WARN, "invalid argument");
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVDuplicatePartitionMgrStat, stat, trans_service))) {
              SERVER_LOG(ERROR, "ObGVDuplicatePartitionMgrStat construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_AUDIT_TID: {
            ObAllVirtualTransAudit* all_virtual_trans_audit = NULL;
            transaction::ObTransService* trans_service = OBSERVER.get_partition_service().get_trans_service();
            if (OB_UNLIKELY(NULL == trans_service)) {
              SERVER_LOG(WARN, "invalid argument");
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTransAudit, all_virtual_trans_audit))) {
              SERVER_LOG(ERROR, "ObGVTransResultInfo construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_virtual_trans_audit);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_SQL_AUDIT_TID: {
            ObAllVirtualTransSQLAudit* all_virtual_trans_sql_audit = NULL;
            transaction::ObTransService* trans_service = OBSERVER.get_partition_service().get_trans_service();
            if (OB_UNLIKELY(NULL == trans_service)) {
              SERVER_LOG(WARN, "invalid argument");
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTransSQLAudit, all_virtual_trans_sql_audit))) {
              SERVER_LOG(ERROR, "ObGVTransResultInfo construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_virtual_trans_sql_audit);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ELECTION_INFO_TID: {
            ObGVElectionInfo* gv_election_info = NULL;
            election::ObElectionMgr* election_mgr = static_cast<election::ObElectionMgr*>(
                observer::ObServer::get_instance().get_partition_service().get_election_mgr());
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVElectionInfo, gv_election_info, election_mgr))) {
              SERVER_LOG(ERROR, "ObGVElectionInfo construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_election_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ELECTION_GROUP_INFO_TID: {
            ObGVElectionGroupInfo* gv_eg_info = NULL;
            election::ObElectionMgr* election_mgr = static_cast<election::ObElectionMgr*>(
                observer::ObServer::get_instance().get_partition_service().get_election_mgr());
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVElectionGroupInfo, gv_eg_info, election_mgr))) {
              SERVER_LOG(ERROR, "ObGVElectionInfo construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_eg_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ELECTION_MEM_STAT_TID: {
            ObGVElectionMemStat* gv_election_mem_stat = NULL;
            election::ObElectionMgr* election_mgr = static_cast<election::ObElectionMgr*>(
                observer::ObServer::get_instance().get_partition_service().get_election_mgr());
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVElectionMemStat, gv_election_mem_stat, election_mgr))) {
              SERVER_LOG(ERROR, "ObGVElectionMemStat construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_election_mem_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ELECTION_EVENT_HISTORY_TID: {
            ObGVElectionEventHistory* gv_election_event_history_info = NULL;
            election::ObElectionMgr* election_mgr = static_cast<election::ObElectionMgr*>(
                observer::ObServer::get_instance().get_partition_service().get_election_mgr());
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVElectionEventHistory, gv_election_event_history_info, election_mgr))) {
              SERVER_LOG(ERROR, "ObGVElectionEventHistory construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_election_event_history_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_MGR_STAT_TID: {
            ObGVTransPartitionMgrStat* gv_trans_partition_stat = NULL;
            transaction::ObTransService* trans_service = OBSERVER.get_partition_service().get_trans_service();
            if (OB_UNLIKELY(NULL == trans_service)) {
              SERVER_LOG(WARN, "invalid argument", KP(trans_service));
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTransPartitionMgrStat, gv_trans_partition_stat, trans_service))) {
              SERVER_LOG(ERROR, "ObGVTransPartitionMgrStatInfo construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_trans_partition_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_MEM_STAT_TID: {
            ObGVTransMemoryStat* gv_trans_mem_stat = NULL;
            transaction::ObTransService* trans_service = OBSERVER.get_partition_service().get_trans_service();
            if (OB_UNLIKELY(NULL == trans_service)) {
              SERVER_LOG(WARN, "invalid argument", KP(trans_service));
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVTransMemoryStat, gv_trans_mem_stat, trans_service))) {
              SERVER_LOG(ERROR, "ObGVTransMemoryStat construct failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_trans_mem_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_INFO_TID: {
            ObGVPartitionInfo* gv_partition_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVPartitionInfo, gv_partition_info))) {
              SERVER_LOG(ERROR, "ObGVPartitionInfo construct failed", K(ret));
            } else {
              storage::ObPartitionService* partition_service =
                  &(observer::ObServer::get_instance().get_partition_service());
              gv_partition_info->set_addr(addr_);
              gv_partition_info->set_partition_service(partition_service);
              vt_iter = static_cast<ObVirtualTableIterator*>(gv_partition_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PG_PARTITION_INFO_TID: {
            ObPGPartitionInfo* pg_partition_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObPGPartitionInfo, pg_partition_info))) {
              SERVER_LOG(ERROR, "ObPGPartitionInfo construct failed", K(ret));
            } else {
              storage::ObPartitionService* partition_service =
                  &(observer::ObServer::get_instance().get_partition_service());
              pg_partition_info->set_addr(addr_);
              pg_partition_info->set_partition_service(partition_service);
              vt_iter = static_cast<ObVirtualTableIterator*>(pg_partition_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_WEAK_READ_STAT_TID: {
            ObAllVirtualWeakReadStat* all_weak_read_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualWeakReadStat, all_weak_read_stat))) {
              SERVER_LOG(ERROR, "ObAllVirtualWeakReadStat", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_weak_read_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TID: {
            ObAllPlanCacheBase* pcs = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG, "scan __all_virtual_plan_cache_stat table using tenant_id", K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllPlanCacheStatI1, pcs))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              NEW_VIRTUAL_TABLE(ObAllPlanCacheStat, pcs);
            }

            if (OB_SUCC(ret)) {
              pcs->set_plan_cache_manager(GCTX.sql_engine_->get_plan_cache_manager());
              vt_iter = static_cast<ObVirtualTableIterator*>(pcs);
            }
          } break;
          case OB_ALL_VIRTUAL_PLAN_STAT_TID: {
            ObAllPlanCacheBase* pcs = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGVSql, pcs))) {
            } else {
              pcs->set_plan_cache_manager(GCTX.sql_engine_->get_plan_cache_manager());
              vt_iter = static_cast<ObVirtualTableIterator*>(pcs);
            }
          } break;
          case OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID: {
            ObPlanCachePlanExplain* px = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObPlanCachePlanExplain, px))) {
              SERVER_LOG(WARN, "fail to allocate vtable iterator", K(ret));
            } else {
              px->set_allocator(&allocator);
              //              px->set_plan_cache_manager(GCTX.sql_engine_->get_plan_cache_manager());
              vt_iter = static_cast<ObVirtualTableIterator*>(px);
            }
          } break;
          case OB_ALL_VIRTUAL_PS_STAT_TID: {
            ObAllVirtualPsStat* ps_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualPsStat, ps_stat))) {
              SERVER_LOG(ERROR, "ObAllVritualPsStat construct failed", K(ret));
            } else if (OB_ISNULL(ps_stat)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "ps_stat init failed", K(ret));
            } else {
              // init code
              ps_stat->set_plan_cache_manager(GCTX.sql_engine_->get_plan_cache_manager());
              vt_iter = static_cast<ObAllVirtualPsStat*>(ps_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PS_ITEM_INFO_TID: {
            ObAllVirtualPsItemInfo* ps_item_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualPsItemInfo, ps_item_info))) {
              SERVER_LOG(ERROR, "ObAllVritualPsCacheStat construct failed", K(ret));
            } else if (OB_ISNULL(ps_item_info)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "ps_item_info init failed", K(ret));
            } else {
              // init code
              ps_item_info->set_plan_cache_manager(GCTX.sql_engine_->get_plan_cache_manager());
              vt_iter = static_cast<ObAllVirtualPsItemInfo*>(ps_item_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID: {
            ObAllVirtualProxyPartitionInfo* pi = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxyPartitionInfo, pi))) {
              SERVER_LOG(ERROR, "fail to new", K(pure_tid), K(ret));
            } else {
              pi->set_schema_service(root_service_.get_schema_service());
              pi->set_calc_buf(params.expr_ctx_.calc_buf_);
              vt_iter = static_cast<ObVirtualTableIterator*>(pi);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_PARTITION_TID: {
            ObAllVirtualProxyPartition* pi = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxyPartition, pi))) {
              SERVER_LOG(ERROR, "fail to new", K(pure_tid), K(ret));
            } else {
              pi->set_schema_service(root_service_.get_schema_service());
              pi->set_calc_buf(params.expr_ctx_.calc_buf_);
              vt_iter = static_cast<ObVirtualTableIterator*>(pi);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID: {
            ObAllVirtualProxySubPartition* pi = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxySubPartition, pi))) {
              SERVER_LOG(ERROR, "fail to new", K(pure_tid), K(ret));
            } else {
              pi->set_schema_service(root_service_.get_schema_service());
              pi->set_calc_buf(params.expr_ctx_.calc_buf_);
              vt_iter = static_cast<ObVirtualTableIterator*>(pi);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_ROUTE_TID: {
            ObAllVirtualProxyRoute* pi = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualProxyRoute, pi))) {
              SERVER_LOG(ERROR, "fail to new", K(pure_tid), K(ret));
            } else {
              pi->set_calc_buf(params.expr_ctx_.calc_buf_);
              vt_iter = static_cast<ObVirtualTableIterator*>(pi);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID: {
            ObSessionVariables* session_variables = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObSessionVariables, session_variables))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              const ObSysVariableSchema* sys_variable_schema = NULL;
              if (OB_FAIL(schema_guard.get_sys_variable_schema(real_tenant_id, sys_variable_schema))) {
                SERVER_LOG(WARN, "get sys variable schema failed", K(ret));
              } else if (OB_ISNULL(sys_variable_schema)) {
                ret = OB_TENANT_NOT_EXIST;
                SERVER_LOG(WARN, "sys variable schema is null", K(ret));
              } else {
                session_variables->set_sys_variable_schema(sys_variable_schema);
                vt_iter = static_cast<ObVirtualTableIterator*>(session_variables);
              }
            }
            break;
          }
          case OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID: {
            ObGlobalVariables* global_variables = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObGlobalVariables, global_variables))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              global_variables->set_sql_proxy(GCTX.sql_proxy_);
              const ObSysVariableSchema* sys_variable_schema = NULL;
              if (OB_FAIL(schema_guard.get_sys_variable_schema(real_tenant_id, sys_variable_schema))) {
                SERVER_LOG(WARN, "get sys variable schema failed", K(ret));
              } else if (OB_ISNULL(sys_variable_schema)) {
                ret = OB_TENANT_NOT_EXIST;
                SERVER_LOG(WARN, "sys variable schema is null", K(ret));
              } else {
                global_variables->set_sys_variable_schema(sys_variable_schema);
                vt_iter = static_cast<ObVirtualTableIterator*>(global_variables);
              }
            }
            break;
          }
          case OB_TENANT_VIRTUAL_TABLE_COLUMN_TID: {
            ObTableColumns* table_columns = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTableColumns, table_columns))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(table_columns);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_TABLE_INDEX_TID: {
            ObTableIndex* table_index = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTableIndex, table_index))) {
              table_index->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(table_index);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID: {
            ObShowCreateDatabase* create_database = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowCreateDatabase, create_database))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(create_database);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID: {
            ObShowCreateTable* create_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowCreateTable, create_table))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(create_table);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID: {
            ObShowCreateTablegroup* create_tablegroup = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowCreateTablegroup, create_tablegroup))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(create_tablegroup);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TID: {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          case OB_VIRTUAL_SHOW_RESTORE_PREVIEW_TID: {
            ObTenantShowRestorePreview* restore_preview = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantShowRestorePreview, restore_preview))) {
              SERVER_LOG(WARN, "ObTenantShowRestorePreview construct fail", K(ret));
            } else if (OB_FAIL(restore_preview->init())) {
              SERVER_LOG(WARN, "ObTenantShowRestorePreview init fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(restore_preview);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID: {
            ObGetObjectDefinition* get_object_def = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObGetObjectDefinition, get_object_def))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(get_object_def);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID: {
            ObShowGrants* show_grants = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowGrants, show_grants))) {
              show_grants->set_tenant_id(real_tenant_id);
              show_grants->set_user_id(session->get_user_id());
              session->get_session_priv_info(show_grants->get_session_priv());
              vt_iter = static_cast<ObVirtualTableIterator*>(show_grants);
            }
            break;
          }
          case OB_SESSION_VARIABLES_TID: {
            ObInfoSchemaSessionVariablesTable* session_variables = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaSessionVariablesTable, session_variables))) {
              const ObSysVariableSchema* sys_variable_schema = NULL;
              if (OB_FAIL(schema_guard.get_sys_variable_schema(real_tenant_id, sys_variable_schema))) {
                SERVER_LOG(WARN, "get sys variable schema failed", K(ret));
              } else if (OB_ISNULL(sys_variable_schema)) {
                ret = OB_TENANT_NOT_EXIST;
                SERVER_LOG(WARN, "sys variable schema is null", K(ret));
              } else {
                session_variables->set_sys_variable_schema(sys_variable_schema);
                vt_iter = static_cast<ObVirtualTableIterator*>(session_variables);
              }
            }
            break;
          }
          case OB_GLOBAL_STATUS_TID: {
            ObInfoSchemaGlobalStatusTable* global_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaGlobalStatusTable, global_status))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              global_status->set_cur_session(session);
              global_status->set_global_ctx(&GCTX);
              vt_iter = static_cast<ObVirtualTableIterator*>(global_status);
            }
            break;
          }
          case OB_SESSION_STATUS_TID: {
            ObInfoSchemaSessionStatusTable* session_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaSessionStatusTable, session_status))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              session_status->set_cur_session(session);
              session_status->set_global_ctx(&GCTX);
              vt_iter = static_cast<ObVirtualTableIterator*>(session_status);
            }
            break;
          }
          case OB_REFERENTIAL_CONSTRAINTS_TID: {
            ObInfoSchemaReferentialConstraintsTable* referential_constraint = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaReferentialConstraintsTable, referential_constraint))) {
              referential_constraint->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(referential_constraint);
            }
            break;
          }
          case OB_TABLE_CONSTRAINTS_TID: {
            ObInfoSchemaTableConstraintsTable* table_constraint = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaTableConstraintsTable, table_constraint))) {
              table_constraint->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(table_constraint);
            }
            break;
          }
          case OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TID: {
            ObInfoSchemaColumnsTable* columns = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaColumnsTable, columns))) {
              columns->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(columns);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROCESSLIST_TID: {
            ObShowProcesslist* processlist_show = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowProcesslist, processlist_show))) {
              processlist_show->set_session_mgr(GCTX.session_mgr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(processlist_show);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_INTERM_RESULT_TID: {
            ObShowIntermResult* interm_result_show = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowIntermResult, interm_result_show))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(interm_result_show);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_DATABASE_STATUS_TID: {
            ObShowDatabaseStatus* database_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowDatabaseStatus, database_status))) {
              database_status->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(database_status);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_TENANT_STATUS_TID: {
            ObShowTenantStatus* tenant_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObShowTenantStatus, tenant_status))) {
              tenant_status->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(tenant_status);
            }
            break;
          }
          case OB_USER_TID: {
            ObMySQLUserTable* mysql_user_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObMySQLUserTable, mysql_user_table))) {
              mysql_user_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(mysql_user_table);
            }
            break;
          }
          case OB_DB_TID: {
            ObMySQLDBTable* mysql_db_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObMySQLDBTable, mysql_db_table))) {
              mysql_db_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(mysql_db_table);
            }
            break;
          }
          case OB_PROC_TID: {
            ObMySQLProcTable* mysql_proc_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObMySQLProcTable, mysql_proc_table))) {
              mysql_proc_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(mysql_proc_table);
            }
            break;
          }
          case OB_PARAMETERS_TID: {
            ObInformationParametersTable* information_parameters_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInformationParametersTable, information_parameters_table))) {
              information_parameters_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObInformationParametersTable*>(information_parameters_table);
            }
            break;
          }
          case OB_PARTITIONS_TID: {
            ObInfoSchemaPartitionsTable* partitions_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaPartitionsTable, partitions_table))) {
              partitions_table->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(partitions_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_KVCACHE_INFO_TID: {
            ObInfoSchemaKvCacheTable* cache_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObInfoSchemaKvCacheTable, cache_table))) {
              SERVER_LOG(ERROR, "fail to new", K(ret), K(pure_tid));
            } else {
              cache_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(cache_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CONCURRENCY_OBJECT_POOL_TID: {
            ObAllConcurrencyObjectPool* object_pool = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllConcurrencyObjectPool, object_pool))) {
              object_pool->set_allocator(&allocator);
              object_pool->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(object_pool);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MEM_LEAK_CHECKER_INFO_TID: {
            ObMemLeakCheckerInfo* leak_checker = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObMemLeakCheckerInfo, leak_checker))) {
              leak_checker->set_allocator(&allocator);
              leak_checker->set_tenant_id(session->get_priv_tenant_id());
              leak_checker->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(leak_checker);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LATCH_TID: {
            ObAllLatch* all_latch = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllLatch, all_latch))) {
              all_latch->set_allocator(&allocator);
              all_latch->set_addr(addr_);
              vt_iter = all_latch;
            }
            break;
          }
          case OB_TENANT_VIRTUAL_WARNING_TID: {
            ObTenantVirtualWarning* warning = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantVirtualWarning, warning))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(warning);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRACE_LOG_TID: {
            ObVirtualTraceLog* trace = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualTraceLog, trace))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(trace);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_CURRENT_TENANT_TID: {
            ObTenantVirtualCurrentTenant* curr_tenant = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantVirtualCurrentTenant, curr_tenant))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              curr_tenant->set_sql_proxy(GCTX.sql_proxy_);
              vt_iter = static_cast<ObVirtualTableIterator*>(curr_tenant);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TID: {
            ObAllDataTypeClassTable* all_tc_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllDataTypeClassTable, all_tc_table))) {
              all_tc_table->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator*>(all_tc_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DATA_TYPE_TID: {
            ObAllDataTypeTable* all_type_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllDataTypeTable, all_type_table))) {
              all_type_table->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator*>(all_type_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SESSION_EVENT_TID: {
            ObAllVirtualSessionEvent* session_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG, "scan __all_virtual_session_event table using tenant_id", K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSessionEventI1, session_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              NEW_VIRTUAL_TABLE(ObAllVirtualSessionEvent, session_table);
            }

            if (OB_SUCC(ret)) {
              session_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(session_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SESSION_WAIT_TID: {
            ObAllVirtualSessionWait* session_wait_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG, "scan __all_virtual_session_wait table using tenant_id", K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSessionWaitI1, session_wait_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              NEW_VIRTUAL_TABLE(ObAllVirtualSessionWait, session_wait_table);
            }

            if (OB_SUCC(ret)) {
              session_wait_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(session_wait_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TID: {
            ObAllVirtualSessionWaitHistory* session_wait_history_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG, "scan __all_virtual_session_wait_history table using tenant_id", K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSessionWaitHistoryI1, session_wait_history_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              NEW_VIRTUAL_TABLE(ObAllVirtualSessionWaitHistory, session_wait_history_table);
            }

            if (OB_SUCC(ret)) {
              session_wait_history_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(session_wait_history_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SESSTAT_TID: {
            ObAllVirtualSessionStat* session_stat_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG, "scan __all_virtual_session_stat table using tenant_id", K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSessionStatI1, session_stat_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              NEW_VIRTUAL_TABLE(ObAllVirtualSessionStat, session_stat_table);
            }

            if (OB_SUCC(ret)) {
              session_stat_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(session_stat_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_STORAGE_STAT_TID: {
            ObInfoSchemaStorageStatTable* storage_stat_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaStorageStatTable, storage_stat_table))) {
              storage_stat_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(storage_stat_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DISK_STAT_TID: {
            ObInfoSchemaDiskStatTable* disk_stat_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObInfoSchemaDiskStatTable, disk_stat_table))) {
              disk_stat_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(disk_stat_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_DISK_STAT_TID: {
            ObAllVirtualTenantDiskStat* all_virtual_tenant_disk_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTenantDiskStat, all_virtual_tenant_disk_stat))) {
              all_virtual_tenant_disk_stat->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator*>(all_virtual_tenant_disk_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_SSTABLE_IMAGE_INFO_TID: {
            ObPartitionSstableImageInfoTable* patition_sstable_image_info_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObPartitionSstableImageInfoTable, patition_sstable_image_info_table))) {
              SERVER_LOG(ERROR, "ObPartitionSstableImageInfoTable construct failed", K(ret));
            } else {
              patition_sstable_image_info_table->set_allocator(&allocator);
              if (OB_FAIL(patition_sstable_image_info_table->init(
                      OBSERVER.get_partition_service(), OBSERVER.get_schema_service(), addr_))) {
                SERVER_LOG(WARN, "init failed", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(patition_sstable_image_info_table);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SYSSTAT_TID: {
            ObAllVirtualSysStat* sys_stat_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG, "scan __all_virtual_sys_stat table using tenant_id", K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSysStatI1, sys_stat_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              NEW_VIRTUAL_TABLE(ObAllVirtualSysStat, sys_stat_table);
            }

            if (OB_SUCC(ret)) {
              sys_stat_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(sys_stat_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SYSTEM_EVENT_TID: {
            ObAllVirtualSysEvent* sys_event_table = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG, "scan __all_virtual_sys_event table using tenant_id", K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualSysEventI1, sys_event_table))) {
                LOG_WARN("new virtual table failed", K(ret));
              }
            } else {
              NEW_VIRTUAL_TABLE(ObAllVirtualSysEvent, sys_event_table);
            }

            if (OB_SUCC(ret)) {
              sys_event_table->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(sys_event_table);
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
            ObGvSqlAudit* sql_audit_table = NULL;
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
                vt_iter = static_cast<ObVirtualTableIterator*>(sql_audit_table);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_TID: {
            ObAllVirtualServerObjectPool* server_object_pool = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualServerObjectPool, server_object_pool))) {
              server_object_pool->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(server_object_pool);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_PARTITION_STAT_TID: {
            ObTenantPartitionStat* partition_stat_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantPartitionStat, partition_stat_table))) {
              if (OB_FAIL(partition_stat_table->init(*GCTX.pt_operator_, *GCTX.schema_service_, real_tenant_id))) {
                SERVER_LOG(WARN, "partition_stat_table init failed", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(partition_stat_table);
              }
            }
            break;
          }
          case OB_TENANT_VIRTUAL_STATNAME_TID: {
            ObTenantVirtualStatname* stat_name = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantVirtualStatname, stat_name))) {
              stat_name->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(stat_name);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_EVENT_NAME_TID: {
            ObTenantVirtualEventName* event_name = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantVirtualEventName, event_name))) {
              event_name->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(event_name);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_REPLAY_STATUS_TID: {
            ObAllVirtualPartitionReplayStatus* ppt_replay_status_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualPartitionReplayStatus, ppt_replay_status_table))) {
              SERVER_LOG(ERROR, "ObAllVirtualPartitionReplayStatus construct failed", K(ret));
            } else {
              storage::ObPartitionService* partition_service = &(OBSERVER.get_partition_service());
              ppt_replay_status_table->set_addr(addr_);
              ppt_replay_status_table->set_partition_service(partition_service);
              vt_iter = static_cast<ObVirtualTableIterator*>(ppt_replay_status_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CLOG_STAT_TID: {
            ObAllVirtualClogStat* clog_stat = NULL;
            storage::ObPartitionService* partition_service =
                &(observer::ObServer::get_instance().get_partition_service());
            if (OB_UNLIKELY(NULL == partition_service)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get partition mgr fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualClogStat, clog_stat, partition_service))) {
              SERVER_LOG(ERROR, "ObAllVirtualClogStat construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(clog_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ENGINE_TID: {
            ObAllVirtualEngineTable* all_engines_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualEngineTable, all_engines_table))) {
              all_engines_table->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator*>(all_engines_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_FILES_TID: {
            ObAllVirtualFilesTable* all_files_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualFilesTable, all_files_table))) {
              all_files_table->set_allocator(&allocator);
              vt_iter = static_cast<ObVirtualTableIterator*>(all_files_table);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TID: {
            ObVirtualProxyServerStat* server_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualProxyServerStat, server_stat))) {
              SERVER_LOG(ERROR, "fail to new ObVirtualProxyServerStat", K(pure_tid), K(ret));
            } else if (OB_FAIL(server_stat->init(*GCTX.schema_service_, GCTX.sql_proxy_, config_))) {
              SERVER_LOG(WARN, "fail to init ObVirtualProxyServerStat", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(server_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_TID: {
            ObVirtualProxySysVariable* sys_variable = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualProxySysVariable, sys_variable))) {
              SERVER_LOG(ERROR, "fail to new ObVirtualProxySysVariable", K(pure_tid), K(ret));
            } else if (OB_FAIL(sys_variable->init(*GCTX.schema_service_, config_))) {
              SERVER_LOG(WARN, "fail to init ObVirtualProxySysVariable", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(sys_variable);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_SSTABLE_MERGE_INFO_TID: {
            ObAllVirtualPartitionSSTableMergeInfo* sstable_merge_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualPartitionSSTableMergeInfo, sstable_merge_info))) {
              if (OB_FAIL(sstable_merge_info->init())) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualPartitionSSTableMergeInfo, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(sstable_merge_info);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_SPLIT_INFO_TID: {
            ObAllVirtualPartitionSplitInfo* partition_split_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualPartitionSplitInfo, partition_split_info))) {
              partition_split_info->set_allocator(&allocator);
              if (OB_FAIL(partition_split_info->init(root_service_.get_schema_service()))) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualPartitionSplitInfo, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(partition_split_info);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_SSTABLE_MACRO_INFO_TID: {
            ObAllVirtualPartitionSSTableMacroInfo* sstable_macro_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualPartitionSSTableMacroInfo, sstable_macro_info))) {
              if (OB_FAIL(sstable_macro_info->init(OBSERVER.get_partition_service()))) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualPartitionSSTableMergeInfo, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(sstable_macro_info);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TID: {
            ObVirtualSqlPlanMonitor* plan_monitor = NULL;
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
                vt_iter = static_cast<ObVirtualTableIterator*>(plan_monitor);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TID: {
            ObVirtualSqlMonitorStatname* stat_name = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObVirtualSqlMonitorStatname, stat_name))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(stat_name);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_MONITOR_TID: {
            ObVirtualSqlMonitor* plan_monitor = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObVirtualSqlMonitor, plan_monitor))) {
              plan_monitor->set_tenant_id(real_tenant_id);
              plan_monitor->set_allocator(&allocator);
              if (OB_FAIL(plan_monitor->set_addr(addr_))) {
                SERVER_LOG(WARN, "fail to set addr", K(ret), K(addr_));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(plan_monitor);
              }
            }
            break;
          }
          case OB_TENANT_VIRTUAL_OUTLINE_TID: {
            ObTenantVirtualOutline* outline = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantVirtualOutline, outline))) {
              outline->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(outline);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID: {
            ObTenantVirtualConcurrentLimitSql* limit_sql = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObTenantVirtualConcurrentLimitSql, limit_sql))) {
              limit_sql->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(limit_sql);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_TID: {
            ObVirtualSqlPlanStatistics* plan_statistics = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObVirtualSqlPlanStatistics, plan_statistics))) {
              plan_statistics->set_pcm(GCTX.sql_engine_->get_plan_cache_manager());
              plan_statistics->set_tenant_id(real_tenant_id);
              vt_iter = static_cast<ObVirtualTableIterator*>(plan_statistics);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_MIGRATION_STATUS_TID: {
            ObAllVirtualPartitionMigrationStatus* migration_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualPartitionMigrationStatus, migration_status))) {
              if (OB_FAIL(migration_status->init(storage::ObPartitionMigrationStatusMgr::get_instance()))) {
                SERVER_LOG(WARN, "fail to init migration_status", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(migration_status);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SYS_TASK_STATUS_TID: {
            ObAllVirtualSysTaskStatus* sys_task_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualSysTaskStatus, sys_task_status))) {
              if (OB_FAIL(sys_task_status->init(SYS_TASK_STATUS_MGR))) {
                SERVER_LOG(WARN, "fail to init migration_status", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(sys_task_status);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_TID: {
            ObAllVirtualMacroBlockMarkerStatus* all_virtual_marker_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualMacroBlockMarkerStatus, all_virtual_marker_status))) {
              blocksstable::ObMacroBlockMarkerStatus marker_status;
              if (OB_FAIL(OB_FILE_SYSTEM.get_marker_status(marker_status))) {
                SERVER_LOG(WARN, "failed to get marker info", K(ret));
              } else if (OB_FAIL(all_virtual_marker_status->init(marker_status))) {
                SERVER_LOG(WARN, "fail to init migration_status", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(all_virtual_marker_status);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_CLOG_STAT_TID: {
            ObAllVirtualServerClogStat* server_clog_stat = NULL;
            if (OB_SUCCESS == NEW_VIRTUAL_TABLE(ObAllVirtualServerClogStat, server_clog_stat)) {
              server_clog_stat->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(server_clog_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_BLACKLIST_TID: {
            ObAllVirtualServerBlacklist* server_blacklist = NULL;
            if (OB_SUCCESS == NEW_VIRTUAL_TABLE(ObAllVirtualServerBlacklist, server_blacklist)) {
              server_blacklist->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(server_blacklist);
            }
            break;
          }
          case OB_ALL_VIRTUAL_ELECTION_PRIORITY_TID: {
            ObAllVirtualElectionPriority* election_priority = NULL;
            storage::ObPartitionService* partition_service =
                &(observer::ObServer::get_instance().get_partition_service());
            if (OB_UNLIKELY(NULL == partition_service)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "get partition mgr fail", K(ret));
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualElectionPriority, election_priority, partition_service))) {
              SERVER_LOG(ERROR, "ObAllVirtualElectionPriority construct fail", K(ret));
            } else {
              election_priority->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(election_priority);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TID: {
            ObAllVirtualLockWaitStat* lock_wait_stat = NULL;
            if (OB_SUCCESS == NEW_VIRTUAL_TABLE(ObAllVirtualLockWaitStat, lock_wait_stat)) {
              vt_iter = static_cast<ObVirtualTableIterator*>(lock_wait_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_CLUSTER_TID: {
            ObAllVirtualCluster* all_cluster = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualCluster, all_cluster))) {
              SERVER_LOG(ERROR, "ObAllVirtualCluster construct failed", K(ret));
            } else if (OB_FAIL(all_cluster->init(root_service_.get_schema_service(),
                           root_service_.get_zone_mgr(),
                           root_service_.get_common_rpc_proxy(),
                           root_service_.get_sql_proxy(),
                           root_service_.get_pt_operator(),
                           root_service_.get_rpc_proxy(),
                           root_service_.get_server_mgr(),
                           root_service_))) {
              SERVER_LOG(WARN, "all cluster init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_cluster);
            }
            break;
          }
          case OB_ALL_VIRTUAL_REPLICA_TASK_TID: {
            ObAllReplicaTask* all_replica_task = NULL;
            bool is_index = false;
            if (OB_FAIL(check_is_index(*index_schema, "i1", is_index))) {
              LOG_WARN("check is index failed", K(ret));
            } else if (is_index) {
              SERVER_LOG(DEBUG, "scan __all_virtual_replica_task table using tenant_id", K(pure_tid));
              if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllReplicaTaskI1, all_replica_task))) {
                SERVER_LOG(ERROR, "ObAllPartitionTable construct failed", K(ret));
              }
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllReplicaTask, all_replica_task))) {
              SERVER_LOG(ERROR, "ObAllPartitionTable construct failed", K(ret));
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(all_replica_task->init(root_service_.get_pt_operator(),
                           root_service_.get_schema_service(),
                           root_service_.get_remote_pt_operator(),
                           root_service_.get_server_mgr(),
                           root_service_.get_unit_mgr(),
                           root_service_.get_zone_mgr(),
                           root_service_.get_rebalance_task_mgr(),
                           root_service_.get_root_balancer()))) {
              SERVER_LOG(WARN, "__all_virtual_replica_task init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_replica_task);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_ITEM_TID: {
            ObAllVirtualPartitionItem* all_partition_item = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualPartitionItem, all_partition_item))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(all_partition_item);
            }
            break;
          }
          case OB_ALL_VIRTUAL_LONG_OPS_STATUS_TID: {
            ObAllVirtualLongOpsStatus* long_ops_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualLongOpsStatus, long_ops_status))) {
              SERVER_LOG(ERROR, "fail to placement new ObAllVirtualLongOpsStatus", K(ret));
            } else if (OB_FAIL(long_ops_status->init())) {
              SERVER_LOG(WARN, "fail to init ObAllVirtualLongOpsStatus", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(long_ops_status);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_LOCATION_TID: {
            ObAllVirtualPartitionLocation* all_partition_location = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualPartitionLocation, all_partition_location))) {
              if (OB_FAIL(all_partition_location->init(root_service_.get_pt_operator()))) {
                SERVER_LOG(WARN, "all_partition_location init failed", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(all_partition_location);
              }
            }
            break;
          }
          case OB_TENANT_VIRTUAL_CHARSET_TID: {
            ObTenantVirtualCharset* charset = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantVirtualCharset, charset))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(charset);
            }
            break;
          }
          case OB_TENANT_VIRTUAL_COLLATION_TID: {
            ObTenantVirtualCollation* collation = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObTenantVirtualCollation, collation))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(collation);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_TID: {
            ObAllVirtualTenantMemstoreAllocatorInfo* tenant_mem_allocator_info = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTenantMemstoreAllocatorInfo, tenant_mem_allocator_info))) {
              SERVER_LOG(WARN, "fail to create virtual table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(tenant_mem_allocator_info);
            }
            break;
          }
          /*
          case OB_ALL_VIRTUAL_META_TABLE_TID: {
            ObAllVirtualMetaTable *all_virtual_meta_table = NULL;
            if (OB_UNLIKELY(NULL == (tmp_ptr = allocator.alloc(sizeof(ObAllVirtualMetaTable))))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SERVER_LOG(ERROR, "alloc failed", K(pure_tid), K(ret));
            } else if (OB_UNLIKELY(NULL == (all_virtual_meta_table = new (tmp_ptr) ObAllVirtualMetaTable()))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SERVER_LOG(ERROR, "ObAllVirtualMetaTable construct failed", K(ret));
            } else if (OB_FAIL(all_virtual_meta_table->init(root_service_.get_pt_operator(),
                                                            root_service_.get_schema_service(),
                                                            &schema_guard))) {
              SERVER_LOG(WARN, "all_virtual_meta_table init failed", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator *>(all_virtual_meta_table);
            }
            break;
          }
          */
          case OB_ALL_VIRTUAL_IO_STAT_TID: {
            ObAllVirtualIOStat* all_io_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualIOStat, all_io_stat))) {
              SERVER_LOG(WARN, "failed to allocate ObAllVirtualIOStat", K(ret));
            } else {
              vt_iter = static_cast<ObAllVirtualIOStat*>(all_io_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_TID: {
            ObVirtualBadBlockTable* bad_block_table = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObVirtualBadBlockTable, bad_block_table))) {
              if (OB_FAIL(bad_block_table->init(addr_))) {
                SERVER_LOG(WARN, "bad_block_table init failed", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(bad_block_table);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_DTL_CHANNEL_TID: {
            ObAllVirtualDtlChannel* dtl_ch = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualDtlChannel, dtl_ch))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(dtl_ch);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DTL_MEMORY_TID: {
            ObAllVirtualDtlMemory* dtl_mem = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualDtlMemory, dtl_mem))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(dtl_mem);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DTL_FIRST_CACHED_BUFFER_TID: {
            ObAllVirtualDtlFirstCachedBuffer* dtl_first_buffer = nullptr;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualDtlFirstCachedBuffer, dtl_first_buffer))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(dtl_first_buffer);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_AUDIT_TID: {
            ObAllVirtualPartitionAudit* partition_audit = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualPartitionAudit, partition_audit))) {
              if (OB_FAIL(partition_audit->init())) {
                SERVER_LOG(WARN, "partition_audit init failed", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(partition_audit);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_PX_WORKER_STAT_TID: {
            ObAllPxWorkerStatTable* px_worker_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllPxWorkerStatTable, px_worker_stat))) {
              px_worker_stat->set_allocator(&allocator);
              px_worker_stat->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(px_worker_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PARTITION_TABLE_STORE_STAT_TID: {
            ObAllVirtualPartitionTableStoreStat* part_table_store_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualPartitionTableStoreStat, part_table_store_stat))) {
              if (OB_FAIL(part_table_store_stat->init(&(ObServer::get_instance().get_partition_service())))) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualPartitionTableStoreStat,", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(part_table_store_stat);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TID: {
            ObAllVirtualServerSchemaInfo* server_schema_info = NULL;
            share::schema::ObMultiVersionSchemaService& schema_service = root_service_.get_schema_service();
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualServerSchemaInfo, server_schema_info, schema_service))) {
              SERVER_LOG(ERROR, "ObAllVirtualServerSchemaInfo construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(server_schema_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_TID: {
            ObAllVirtualMemoryContextStat* memory_context_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualMemoryContextStat, memory_context_stat))) {
              SERVER_LOG(ERROR, "ObAllVirtualMemoryContextStat construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(memory_context_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DUMP_TENANT_INFO_TID: {
            ObAllVirtualDumpTenantInfo* dump_tenant = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDumpTenantInfo, dump_tenant))) {
              SERVER_LOG(ERROR, "ObAllVirtualDumpTenantInfo construct fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(dump_tenant);
            }
            break;
          }
          case OB_ALL_VIRTUAL_DEADLOCK_STAT_TID: {
            ObAllVirtualDeadlockStat* deadlock_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualDeadlockStat, deadlock_stat))) {
              vt_iter = static_cast<ObVirtualTableIterator*>(deadlock_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_TID: {
            ObSqlWorkareaHistoryStat* wa_stat = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObSqlWorkareaHistoryStat, wa_stat))) {
              vt_iter = static_cast<ObSqlWorkareaHistoryStat*>(wa_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_TID: {
            ObSqlWorkareaActive* wa_active = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObSqlWorkareaActive, wa_active))) {
              vt_iter = static_cast<ObSqlWorkareaActive*>(wa_active);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_TID: {
            ObSqlWorkareaHistogram* wa_hist = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObSqlWorkareaHistogram, wa_hist))) {
              vt_iter = static_cast<ObSqlWorkareaHistogram*>(wa_hist);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_TID: {
            ObSqlWorkareaMemoryInfo* wa_memory_info = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObSqlWorkareaMemoryInfo, wa_memory_info))) {
              vt_iter = static_cast<ObSqlWorkareaMemoryInfo*>(wa_memory_info);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PG_BACKUP_LOG_ARCHIVE_STATUS_TID: {
            ObAllVirtualPGBackupLogArchiveStatus* pg_log_archive_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualPGBackupLogArchiveStatus, pg_log_archive_status))) {
              SERVER_LOG(WARN, "ObAllVirtualPGBackupLogArchiveStatus construct fail", K(ret));
            } else if (OB_FAIL(pg_log_archive_status->init(&(OBSERVER.get_partition_service()), addr_))) {
              SERVER_LOG(WARN, "ObAllVirtualPGBackupLogArchiveStatus init fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(pg_log_archive_status);
            }
            break;
          }
          case OB_ALL_VIRTUAL_SERVER_BACKUP_LOG_ARCHIVE_STATUS_TID: {
            ObAllVirtualServerBackupLogArchiveStatus* server_log_archive_status = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualServerBackupLogArchiveStatus, server_log_archive_status))) {
              SERVER_LOG(WARN, "ObAllVirtualServerBackupLogArchiveStatus construct fail", K(ret));
            } else if (OB_FAIL(server_log_archive_status->init(&(OBSERVER.get_partition_service()), addr_))) {
              SERVER_LOG(WARN, "ObAllVirtualServerBackupLogArchiveStatus init fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(server_log_archive_status);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TABLE_MODIFICATIONS_TID: {
            ObAllVirtualTableModifications* table_modifications_infos = nullptr;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTableModifications, table_modifications_infos))) {
              SERVER_LOG(ERROR, "ObAllVirtualTableModifications construct failed", K(ret));
            } else if (OB_FAIL(table_modifications_infos->init())) {
              SERVER_LOG(WARN, "failed to init table_modifications_infos", K(ret));
            } else {
              table_modifications_infos->set_addr(addr_);
              vt_iter = static_cast<ObVirtualTableIterator*>(table_modifications_infos);
            }
            break;
          }
          case OB_ALL_VIRTUAL_PG_LOG_ARCHIVE_STAT_TID: {
            ObAllVirtualPGLogArchiveStat* pg_log_archive_stat = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualPGLogArchiveStat, pg_log_archive_stat))) {
              SERVER_LOG(WARN, "ObAllVirtualPGLogArchiveStat construct fail", K(ret));
            } else if (OB_FAIL(pg_log_archive_stat->init(&(OBSERVER.get_partition_service()), addr_))) {
              SERVER_LOG(WARN, "ObAllVirtualPGLogArchiveStat int fail", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(pg_log_archive_stat);
            }
            break;
          }
          case OB_ALL_VIRTUAL_RESERVED_TABLE_MGR_TID: {
            ObAllVirtualReservedTableMgr* table_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualReservedTableMgr, table_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualReservedTableMgr construct failed", K(ret));
            } else if (OB_FAIL(table_mgr->init())) {
              SERVER_LOG(WARN, "failed to init all_virtual_memstore_info", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(table_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_TID: {
            ObAllVirtualTimestampService* timestamp_service = NULL;
            transaction::ObTransService* trans_service = OBSERVER.get_partition_service().get_trans_service();
            if (OB_UNLIKELY(NULL == trans_service)) {
              SERVER_LOG(WARN, "invalid argument");
              ret = OB_INVALID_ARGUMENT;
            } else if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualTimestampService, timestamp_service, trans_service))) {
              SERVER_LOG(ERROR, "ObAllVirtualTimestampService construct fail", K(ret));
            } else if (OB_FAIL(timestamp_service->init(root_service_.get_schema_service()))) {
              SERVER_LOG(WARN, "fail to init ObAllVirtualTimestampService", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(timestamp_service);
            }
            break;
          }
          case OB_ALL_VIRTUAL_TRANS_TABLE_STATUS_TID: {
            ObAllVirtualTransTableStatus* trans_table_status = NULL;
            if (OB_SUCC(NEW_VIRTUAL_TABLE(ObAllVirtualTransTableStatus, trans_table_status))) {
              trans_table_status->set_allocator(&allocator);
              if (OB_FAIL(trans_table_status->init())) {
                SERVER_LOG(WARN, "fail to init ObAllVirtualTransTableStatus, ", K(ret));
              } else {
                vt_iter = static_cast<ObVirtualTableIterator*>(trans_table_status);
              }
            }
            break;
          }
          case OB_ALL_VIRTUAL_OPEN_CURSOR_TID: {
            ObVirtualOpenCursorTable* open_cursors = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObVirtualOpenCursorTable, open_cursors))) {
              SERVER_LOG(ERROR, "ObVirtual open cursor table failed", K(ret));
            } else {
              open_cursors->set_allocator(&allocator);
              open_cursors->set_session_mgr(GCTX.session_mgr_);
              open_cursors->set_plan_cache_manager(GCTX.sql_engine_->get_plan_cache_manager());
              open_cursors->set_tenant_id(real_tenant_id);
              OZ(open_cursors->set_addr(addr_));
              OX(vt_iter = static_cast<ObVirtualOpenCursorTable*>(open_cursors));
            }
            break;
          }
          case OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_TID: {
            ObAllVirtualDagWarningHistory* dag_warning_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualDagWarningHistory, dag_warning_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualBackupSetHistoryMgr construct failed", K(ret));
            } else if (OB_FAIL(dag_warning_mgr->init())) {
              SERVER_LOG(WARN, "failed to init backup_set_history_mgr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(dag_warning_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_BACKUPSET_HISTORY_MGR_TID: {
            ObAllVirtualBackupSetHistoryMgr* backup_set_history_mgr = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualBackupSetHistoryMgr, backup_set_history_mgr))) {
              SERVER_LOG(ERROR, "ObAllVirtualBackupSetHistoryMgr construct failed", K(ret));
            } else if (OB_FAIL(backup_set_history_mgr->init(*GCTX.sql_proxy_))) {
              SERVER_LOG(WARN, "failed to init backup_set_history_mgr", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(backup_set_history_mgr);
            }
            break;
          }
          case OB_ALL_VIRTUAL_BACKUP_CLEAN_INFO_TID: {
            ObAllVirtualBackupCleanInfo* backup_clean_info_table = NULL;
            if (OB_FAIL(NEW_VIRTUAL_TABLE(ObAllVirtualBackupCleanInfo, backup_clean_info_table))) {
              SERVER_LOG(ERROR, "ObAllVirtualBackupCleanInfo construct failed", K(ret));
            } else if (OB_FAIL(backup_clean_info_table->init(*GCTX.sql_proxy_))) {
              SERVER_LOG(WARN, "failed to init virtual backup clean info table", K(ret));
            } else {
              vt_iter = static_cast<ObVirtualTableIterator*>(backup_clean_info_table);
            }
            break;
          }

#define AGENT_VIRTUAL_TABLE_CREATE_ITER
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef AGENT_VIRTUAL_TABLE_CREATE_ITER

#define ITERATE_VIRTUAL_TABLE_CREATE_ITER
#include "share/inner_table/ob_inner_table_schema_misc.ipp"
#undef ITERATE_VIRTUAL_TABLE_CREATE_ITER
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid virtual table id", K(ret), K(pure_tid), K(data_table_id), K(index_id));
          } break;
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
}  // namespace observer
}  // namespace oceanbase
