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

#ifndef _OB_INNER_TABLE_SCHEMA_H_
#define _OB_INNER_TABLE_SCHEMA_H_

#include "share/ob_define.h"
#include "ob_inner_table_schema_constants.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}

namespace share
{

struct TENANT_VIRTUAL_INTERM_RESULT_CDE {
  enum {
    JOB_ID = common::OB_APP_MIN_COLUMN_ID,
    TASK_ID,
    SLICE_ID,
    EXECUTION_ID,
    SVR_IP,
    SVR_PORT,
    EXPIRE_TIME,
    ROW_COUNT,
    SCANNER_COUNT,
    USED_MEMORY_SIZE,
    USED_DISK_SIZE,
    PARTITION_IP,
    PARTITION_PORT
  };
};


struct ALL_VIRTUAL_PLAN_STAT_CDE {
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    PLAN_ID,
    SQL_ID,
    TYPE,
    IS_BIND_SENSITIVE,
    IS_BIND_AWARE,
    STATEMENT,
    QUERY_SQL,
    SPECIAL_PARAMS,
    PARAM_INFOS,
    SYS_VARS,
    PLAN_HASH,
    FIRST_LOAD_TIME,
    SCHEMA_VERSION,
    MERGED_VERSION,
    LAST_ACTIVE_TIME,
    AVG_EXE_USEC,
    SLOWEST_EXE_TIME,
    SLOWEST_EXE_USEC,
    SLOW_COUNT,
    HIT_COUNT,
    PLAN_SIZE,
    EXECUTIONS,
    DISK_READS,
    DIRECT_WRITES,
    BUFFER_GETS,
    APPLICATION_WAIT_TIME,
    CONCURRENCY_WAIT_TIME,
    USER_IO_WAIT_TIME,
    ROWS_PROCESSED,
    ELAPSED_TIME,
    CPU_TIME,
    LARGE_QUERYS,
    DELAYED_LARGE_QUERYS,
    OUTLINE_VERSION,
    OUTLINE_ID,
    OUTLINE_DATA,
    ACS_SEL_INFO,
    TABLE_SCAN,
    DB_ID,
    EVOLUTION,
    EVO_EXECUTIONS,
    EVO_CPU_TIME,
    TIMEOUT_COUNT,
    PS_STMT_ID,
    DELAYED_PX_QUERYS,
    SESSID,
    TEMP_TABLES,
    IS_USE_JIT,
    OBJECT_TYPE,
    ENABLE_BF_CACHE,
    BF_FILTER_CNT,
    BF_ACCESS_CNT,
    ENABLE_ROW_CACHE,
    ROW_CACHE_HIT_CNT,
    ROW_CACHE_MISS_CNT,
    ENABLE_FUSE_ROW_CACHE,
    FUSE_ROW_CACHE_HIT_CNT,
    FUSE_ROW_CACHE_MISS_CNT,
    HINTS_INFO,
    HINTS_ALL_WORKED,
    PL_SCHEMA_ID,
    IS_BATCHED_MULTI_STMT
  };
};


struct ALL_VIRTUAL_TENANT_PARAMETER_STAT_CDE {
  enum {
    ZONE = common::OB_APP_MIN_COLUMN_ID,
    SVR_TYPE,
    SVR_IP,
    SVR_PORT,
    NAME,
    DATA_TYPE,
    VALUE,
    INFO,
    SECTION,
    SCOPE,
    SOURCE,
    EDIT_LEVEL
  };
};


struct ALL_VIRTUAL_PS_STAT_CDE {
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    STMT_COUNT,
    HIT_COUNT,
    ACCESS_COUNT,
    MEM_HOLD
  };
};


struct ALL_VIRTUAL_PS_ITEM_INFO_CDE {
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    STMT_ID,
    DB_ID,
    PS_SQL,
    PARAM_COUNT,
    STMT_ITEM_REF_COUNT,
    STMT_INFO_REF_COUNT,
    MEM_HOLD,
    STMT_TYPE,
    CHECKSUM,
    EXPIRED
  };
};


struct ALL_VIRTUAL_PLAN_STAT_ORA_CDE {
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    PLAN_ID,
    SQL_ID,
    TYPE,
    IS_BIND_SENSITIVE,
    IS_BIND_AWARE,
    STATEMENT,
    QUERY_SQL,
    SPECIAL_PARAMS,
    PARAM_INFOS,
    SYS_VARS,
    PLAN_HASH,
    FIRST_LOAD_TIME,
    SCHEMA_VERSION,
    MERGED_VERSION,
    LAST_ACTIVE_TIME,
    AVG_EXE_USEC,
    SLOWEST_EXE_TIME,
    SLOWEST_EXE_USEC,
    SLOW_COUNT,
    HIT_COUNT,
    PLAN_SIZE,
    EXECUTIONS,
    DISK_READS,
    DIRECT_WRITES,
    BUFFER_GETS,
    APPLICATION_WAIT_TIME,
    CONCURRENCY_WAIT_TIME,
    USER_IO_WAIT_TIME,
    ROWS_PROCESSED,
    ELAPSED_TIME,
    CPU_TIME,
    LARGE_QUERYS,
    DELAYED_LARGE_QUERYS,
    OUTLINE_VERSION,
    OUTLINE_ID,
    OUTLINE_DATA,
    ACS_SEL_INFO,
    TABLE_SCAN,
    DB_ID,
    EVOLUTION,
    EVO_EXECUTIONS,
    EVO_CPU_TIME,
    TIMEOUT_COUNT,
    PS_STMT_ID,
    DELAYED_PX_QUERYS,
    SESSID,
    TEMP_TABLES,
    IS_USE_JIT,
    OBJECT_TYPE,
    ENABLE_BF_CACHE,
    BF_FILTER_CNT,
    BF_ACCESS_CNT,
    ENABLE_ROW_CACHE,
    ROW_CACHE_HIT_CNT,
    ROW_CACHE_MISS_CNT,
    ENABLE_FUSE_ROW_CACHE,
    FUSE_ROW_CACHE_HIT_CNT,
    FUSE_ROW_CACHE_MISS_CNT,
    HINTS_INFO,
    HINTS_ALL_WORKED,
    PL_SCHEMA_ID,
    IS_BATCHED_MULTI_STMT
  };
};


struct ALL_VIRTUAL_PS_STAT_ORA_CDE {
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    STMT_COUNT,
    HIT_COUNT,
    ACCESS_COUNT,
    MEM_HOLD
  };
};


struct ALL_VIRTUAL_PS_ITEM_INFO_ORA_CDE {
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    STMT_ID,
    DB_ID,
    PS_SQL,
    PARAM_COUNT,
    STMT_ITEM_REF_COUNT,
    STMT_INFO_REF_COUNT,
    MEM_HOLD,
    STMT_TYPE,
    CHECKSUM,
    EXPIRED
  };
};


struct ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_CDE {
  enum {
    ZONE = common::OB_APP_MIN_COLUMN_ID,
    SVR_TYPE,
    SVR_IP,
    SVR_PORT,
    NAME,
    DATA_TYPE,
    VALUE,
    INFO,
    SECTION,
    SCOPE,
    SOURCE,
    EDIT_LEVEL
  };
};

class ObInnerTableSchema
{

public:
  static int all_core_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_root_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_operation_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_v2_schema(share::schema::ObTableSchema &table_schema);
  static int all_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablegroup_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablegroup_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_zone_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_variable_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_statistic_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_config_schema(share::schema::ObTableSchema &table_schema);
  static int all_resource_pool_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_resource_usage_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_schema(share::schema::ObTableSchema &table_schema);
  static int all_charset_schema(share::schema::ObTableSchema &table_schema);
  static int all_collation_schema(share::schema::ObTableSchema &table_schema);
  static int all_local_index_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_dummy_schema(share::schema::ObTableSchema &table_schema);
  static int all_frozen_map_schema(share::schema::ObTableSchema &table_schema);
  static int all_clog_history_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_clog_history_info_v2_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_election_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_recyclebin_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_sub_part_schema(share::schema::ObTableSchema &table_schema);
  static int all_sub_part_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_sub_part_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_sub_part_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_load_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_variable_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_time_zone_schema(share::schema::ObTableSchema &table_schema);
  static int all_time_zone_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_time_zone_transition_schema(share::schema::ObTableSchema &table_schema);
  static int all_time_zone_transition_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_v2_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_index_wait_transaction_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_index_schedule_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_index_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_param_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_param_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_histogram_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_sql_execute_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_index_build_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_build_index_param_schema(share::schema::ObTableSchema &table_schema);
  static int all_global_index_data_src_schema(share::schema::ObTableSchema &table_schema);
  static int all_acquired_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int all_immediate_effect_index_sstable_schema(share::schema::ObTableSchema &table_schema);
  static int all_sstable_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_gc_partition_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraint_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraint_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_ori_schema_version_schema(share::schema::ObTableSchema &table_schema);
  static int all_func_schema(share::schema::ObTableSchema &table_schema);
  static int all_func_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_temp_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_sstable_column_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_value_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_plan_baseline_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_plan_baseline_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_helper_schema(share::schema::ObTableSchema &table_schema);
  static int all_freeze_schema_version_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attr_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attr_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_type_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_weak_read_service_schema(share::schema::ObTableSchema &table_schema);
  static int all_gts_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_gts_schema(share::schema::ObTableSchema &table_schema);
  static int all_partition_member_list_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_partition_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_tablespace_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_tablespace_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_user_failed_login_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_profile_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_profile_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_seed_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_sstable_column_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_record_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_sysauth_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_sysauth_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_backup_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_backup_log_archive_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_log_archive_status_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_backup_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_pg_backup_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_error_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_recovery_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_datafile_recovery_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_backup_clean_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_clean_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_task_clean_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_restore_pg_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_v2_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_validation_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_validation_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_backup_validation_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_validation_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_pg_backup_validation_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_transition_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_transition_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_constraint_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_constraint_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_global_transaction_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_dependency_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_backupset_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_backupset_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_backup_backupset_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_backupset_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_pg_backup_backupset_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_backup_backup_log_archive_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_backup_log_archive_status_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_plan_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_directive_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_mapping_rule_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_backuppiece_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_backuppiece_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_backuppiece_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_backuppiece_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_piece_files_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_set_files_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_consumer_group_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_log_archive_status_v2_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_backup_log_archive_status_v2_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_all_table_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_table_column_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_table_index_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_database_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_table_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_session_variable_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_privilege_grant_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_processlist_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_warning_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_current_tenant_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_database_status_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_tenant_status_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_interm_result_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_partition_stat_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_statname_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_event_name_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_global_variable_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_tables_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_procedure_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_zone_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mem_leak_checker_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_latch_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_kvcache_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_type_class_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_task_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_event_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_concurrency_object_pool_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_storage_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_disk_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_upgrade_inspection_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_mgr_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_election_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_election_mem_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_mem_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_sstable_image_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_root_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_all_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_column_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_parameter_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_replay_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_clog_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trace_log_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_engine_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_server_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_sys_variable_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_schema_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_plan_explain_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_obrpc_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_sstable_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_outline_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_concurrent_limit_sql_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_sstable_macro_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_partition_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_partition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_sub_partition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_route_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_tenant_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_unit_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_replica_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_amplification_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_election_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_store_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_leader_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_migration_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_task_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_macro_block_marker_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_clog_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rootservice_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_election_priority_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_disk_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_map_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_map_item_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_io_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_long_ops_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_unit_migrate_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_unit_distribution_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_object_pool_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_lock_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_election_group_info_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_tablegroup_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_blacklist_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_split_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_result_info_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_duplicate_partition_mgr_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_parameter_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_schema_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memory_context_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dump_tenant_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_parameter_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dag_warning_history_schema(share::schema::ObTableSchema &table_schema);
  static int virtual_show_restore_preview_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_master_key_version_info_schema(share::schema::ObTableSchema &table_schema);
  static int session_variables_schema(share::schema::ObTableSchema &table_schema);
  static int table_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int user_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int schema_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int table_constraints_schema(share::schema::ObTableSchema &table_schema);
  static int global_status_schema(share::schema::ObTableSchema &table_schema);
  static int partitions_schema(share::schema::ObTableSchema &table_schema);
  static int session_status_schema(share::schema::ObTableSchema &table_schema);
  static int user_schema(share::schema::ObTableSchema &table_schema);
  static int db_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_lock_wait_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_item_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_replica_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_location_schema(share::schema::ObTableSchema &table_schema);
  static int proc_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_collation_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_charset_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memstore_allocator_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_mgr_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_freeze_info_schema(share::schema::ObTableSchema &table_schema);
  static int parameters_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_bad_block_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_px_worker_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_audit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_weak_read_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_audit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_v2_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_value_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_cluster_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_table_store_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ddl_operation_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_outline_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_outline_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_synonym_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_synonym_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablegroup_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablegroup_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_part_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_part_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_part_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_part_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_def_sub_part_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_def_sub_part_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sub_part_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sub_part_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_temp_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ori_schema_version_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_variable_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_variable_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_func_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_func_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_package_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_package_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_param_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_param_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_attr_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_attr_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_coll_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_coll_type_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_histogram_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_statistic_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recyclebin_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_gc_partition_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_plan_baseline_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_plan_baseline_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_object_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_object_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_raid_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_log_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dtl_channel_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dtl_memory_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dtl_first_cached_buffer_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_partition_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_role_grantee_map_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_role_grantee_map_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_keystore_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_keystore_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_deadlock_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_tablespace_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_tablespace_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_information_columns_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_pg_partition_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_user_failed_login_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_profile_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_profile_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_security_audit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_security_audit_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trigger_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trigger_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_cluster_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sstable_column_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ps_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ps_item_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_history_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_active_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_histogram_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_security_audit_record_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysauth_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysauth_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_objauth_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_objauth_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_pg_backup_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_pg_backup_log_archive_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_backup_log_archive_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_error_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_timestamp_service_schema(share::schema::ObTableSchema &table_schema);
  static int referential_constraints_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_modifications_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_clean_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_restore_pg_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_object_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_table_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_pg_log_archive_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_monitor_statname_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_open_cursor_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_validation_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_pg_backup_validation_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_time_zone_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_time_zone_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_time_zone_transition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_time_zone_transition_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_files_schema(share::schema::ObTableSchema &table_schema);
  static int files_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dependency_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_object_definition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_reserved_table_mgr_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backupset_history_mgr_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_backupset_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_pg_backup_backupset_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_global_transaction_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_v2_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_part_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sub_part_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_package_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_meta_table_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_statistics_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_plan_explain_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_value_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_object_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_synonym_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_stat_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_statistic_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_table_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_stat_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recyclebin_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_outline_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablegroup_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_privilege_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_parameter_stat_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_table_index_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_charset_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_all_table_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_collation_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_column_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_processlist_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memory_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memstore_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memstore_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_memory_info_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memstore_allocator_info_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_session_variable_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_global_variable_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_table_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_procedure_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_tablegroup_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_privilege_grant_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_table_column_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trace_log_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_concurrent_limit_sql_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_attr_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_coll_type_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_param_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_type_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sstable_checksum_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_info_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_partition_meta_table_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_keystore_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_tablespace_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_profile_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_role_grantee_map_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_privilege_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_security_audit_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_security_audit_history_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trigger_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_px_worker_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ps_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ps_item_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_parameter_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_history_stat_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_active_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_histogram_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_memory_info_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_security_audit_record_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysauth_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysauth_history_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_objauth_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_objauth_history_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_error_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_mgr_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_def_sub_part_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_object_type_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_schema_info_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_history_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_monitor_statname_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_lock_wait_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_open_cursor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_column_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dependency_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_name_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_transition_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_transition_type_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_object_definition_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_param_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_attr_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_coll_type_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_package_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_trigger_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_global_transaction_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_acquired_snapshot_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_v2_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_part_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sub_part_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_package_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_value_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_object_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_synonym_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_stat_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_statistic_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_stat_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recyclebin_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablegroup_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_column_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_attr_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_coll_type_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_param_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_partition_meta_table_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_keystore_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_tablespace_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_profile_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_role_grantee_map_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_privilege_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_security_audit_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_security_audit_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_trigger_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_security_audit_record_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_sysauth_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_sysauth_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_objauth_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_objauth_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_error_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_def_sub_part_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_object_type_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_constraint_column_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_dependency_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_meta_table_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_name_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_transition_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_transition_type_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_res_mgr_plan_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_res_mgr_directive_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_lock_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_res_mgr_mapping_rule_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_res_mgr_consumer_group_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_plan_stat_schema(share::schema::ObTableSchema &table_schema);
  static int schemata_schema(share::schema::ObTableSchema &table_schema);
  static int character_sets_schema(share::schema::ObTableSchema &table_schema);
  static int global_variables_schema(share::schema::ObTableSchema &table_schema);
  static int statistics_schema(share::schema::ObTableSchema &table_schema);
  static int views_schema(share::schema::ObTableSchema &table_schema);
  static int tables_schema(share::schema::ObTableSchema &table_schema);
  static int collations_schema(share::schema::ObTableSchema &table_schema);
  static int collation_character_set_applicability_schema(share::schema::ObTableSchema &table_schema);
  static int processlist_schema(share::schema::ObTableSchema &table_schema);
  static int key_column_usage_schema(share::schema::ObTableSchema &table_schema);
  static int dba_outlines_schema(share::schema::ObTableSchema &table_schema);
  static int engines_schema(share::schema::ObTableSchema &table_schema);
  static int routines_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_event_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_wait_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_wait_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_system_event_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sesstat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int v_statname_schema(share::schema::ObTableSchema &table_schema);
  static int v_event_name_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_event_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_wait_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_wait_history_schema(share::schema::ObTableSchema &table_schema);
  static int v_sesstat_schema(share::schema::ObTableSchema &table_schema);
  static int v_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int v_system_event_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int gv_latch_schema(share::schema::ObTableSchema &table_schema);
  static int gv_memory_schema(share::schema::ObTableSchema &table_schema);
  static int v_memory_schema(share::schema::ObTableSchema &table_schema);
  static int gv_memstore_schema(share::schema::ObTableSchema &table_schema);
  static int v_memstore_schema(share::schema::ObTableSchema &table_schema);
  static int gv_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_stat_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_plan_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_plan_explain_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_plan_explain_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int v_latch_schema(share::schema::ObTableSchema &table_schema);
  static int gv_obrpc_outgoing_schema(share::schema::ObTableSchema &table_schema);
  static int v_obrpc_outgoing_schema(share::schema::ObTableSchema &table_schema);
  static int gv_obrpc_incoming_schema(share::schema::ObTableSchema &table_schema);
  static int v_obrpc_incoming_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_plan_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_plan_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int user_recyclebin_schema(share::schema::ObTableSchema &table_schema);
  static int gv_outline_schema(share::schema::ObTableSchema &table_schema);
  static int gv_concurrent_limit_sql_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_plan_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_plan_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int gv_server_memstore_schema(share::schema::ObTableSchema &table_schema);
  static int gv_unit_load_balance_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_tenant_schema(share::schema::ObTableSchema &table_schema);
  static int gv_database_schema(share::schema::ObTableSchema &table_schema);
  static int gv_table_schema(share::schema::ObTableSchema &table_schema);
  static int gv_unit_schema(share::schema::ObTableSchema &table_schema);
  static int v_unit_schema(share::schema::ObTableSchema &table_schema);
  static int gv_partition_schema(share::schema::ObTableSchema &table_schema);
  static int v_partition_schema(share::schema::ObTableSchema &table_schema);
  static int gv_lock_wait_stat_schema(share::schema::ObTableSchema &table_schema);
  static int v_lock_wait_stat_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_name_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_transition_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_transition_type_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_longops_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_longops_schema(share::schema::ObTableSchema &table_schema);
  static int gv_tenant_memstore_allocator_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_tenant_memstore_allocator_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_tenant_sequence_object_schema(share::schema::ObTableSchema &table_schema);
  static int columns_schema(share::schema::ObTableSchema &table_schema);
  static int gv_minor_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_tenant_px_worker_stat_schema(share::schema::ObTableSchema &table_schema);
  static int v_tenant_px_worker_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_partition_audit_schema(share::schema::ObTableSchema &table_schema);
  static int v_partition_audit_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_cluster_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ps_stat_schema(share::schema::ObTableSchema &table_schema);
  static int v_ps_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ps_item_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ps_item_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_active_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_active_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_histogram_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_histogram_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_sql_workarea_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sql_workarea_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_reference_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_reference_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_timestamp_service_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sstable_schema(share::schema::ObTableSchema &table_schema);
  static int v_sstable_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_archivelog_summary_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_job_details_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_set_details_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_set_expired_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_progress_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_archivelog_progress_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_clean_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_task_clean_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_restore_progress_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_restore_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_server_schema_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_server_schema_info_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ckpt_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_trans_table_status_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_trans_table_status_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_monitor_statname_schema(share::schema::ObTableSchema &table_schema);
  static int gv_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_lock_schema(share::schema::ObTableSchema &table_schema);
  static int v_lock_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_validation_job_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_validation_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_tenant_backup_validation_task_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_validation_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int v_restore_point_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_set_obsolete_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backupset_job_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backupset_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backupset_task_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backupset_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backup_archivelog_summary_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_piece_files_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_set_files_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backuppiece_job_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backuppiece_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backuppiece_task_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backuppiece_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_all_clusters_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_archivelog_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_backup_archivelog_schema(share::schema::ObTableSchema &table_schema);
  static int column_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int show_rabbit_schema(share::schema::ObTableSchema &table_schema);
  static int dba_synonyms_schema(share::schema::ObTableSchema &table_schema);
  static int dba_objects_schema(share::schema::ObTableSchema &table_schema);
  static int all_objects_schema(share::schema::ObTableSchema &table_schema);
  static int user_objects_schema(share::schema::ObTableSchema &table_schema);
  static int dba_sequences_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequences_schema(share::schema::ObTableSchema &table_schema);
  static int user_sequences_schema(share::schema::ObTableSchema &table_schema);
  static int dba_users_schema(share::schema::ObTableSchema &table_schema);
  static int all_users_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonyms_schema(share::schema::ObTableSchema &table_schema);
  static int user_synonyms_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ind_columns_schema(share::schema::ObTableSchema &table_schema);
  static int all_ind_columns_schema(share::schema::ObTableSchema &table_schema);
  static int user_ind_columns_schema(share::schema::ObTableSchema &table_schema);
  static int dba_constraints_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraints_schema(share::schema::ObTableSchema &table_schema);
  static int user_constraints_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_cols_v_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_cols_v_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_cols_v_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_cols_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_cols_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_cols_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_columns_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_columns_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_columns_schema(share::schema::ObTableSchema &table_schema);
  static int all_tables_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tables_schema(share::schema::ObTableSchema &table_schema);
  static int user_tables_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_comments_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_comments_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_comments_schema(share::schema::ObTableSchema &table_schema);
  static int dba_col_comments_schema(share::schema::ObTableSchema &table_schema);
  static int all_col_comments_schema(share::schema::ObTableSchema &table_schema);
  static int user_col_comments_schema(share::schema::ObTableSchema &table_schema);
  static int dba_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int all_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int user_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int dba_cons_columns_schema(share::schema::ObTableSchema &table_schema);
  static int all_cons_columns_schema(share::schema::ObTableSchema &table_schema);
  static int user_cons_columns_schema(share::schema::ObTableSchema &table_schema);
  static int user_segments_schema(share::schema::ObTableSchema &table_schema);
  static int dba_segments_schema(share::schema::ObTableSchema &table_schema);
  static int dba_types_schema(share::schema::ObTableSchema &table_schema);
  static int all_types_schema(share::schema::ObTableSchema &table_schema);
  static int user_types_schema(share::schema::ObTableSchema &table_schema);
  static int dba_type_attrs_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attrs_schema(share::schema::ObTableSchema &table_schema);
  static int user_type_attrs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_coll_types_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_types_schema(share::schema::ObTableSchema &table_schema);
  static int user_coll_types_schema(share::schema::ObTableSchema &table_schema);
  static int dba_procedures_schema(share::schema::ObTableSchema &table_schema);
  static int dba_arguments_schema(share::schema::ObTableSchema &table_schema);
  static int dba_source_schema(share::schema::ObTableSchema &table_schema);
  static int all_procedures_schema(share::schema::ObTableSchema &table_schema);
  static int all_arguments_schema(share::schema::ObTableSchema &table_schema);
  static int all_source_schema(share::schema::ObTableSchema &table_schema);
  static int user_procedures_schema(share::schema::ObTableSchema &table_schema);
  static int user_arguments_schema(share::schema::ObTableSchema &table_schema);
  static int user_source_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int user_part_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int dba_subpart_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int all_subpart_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int user_subpart_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int dba_views_schema(share::schema::ObTableSchema &table_schema);
  static int all_views_schema(share::schema::ObTableSchema &table_schema);
  static int user_views_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_partitions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_subpartitions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_tables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_tables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_part_tables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_partitions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_partitions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_subpartitions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_subpartitions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_subpartition_templates_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_subpartition_templates_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_subpartition_templates_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int user_part_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int all_all_tables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_all_tables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_all_tables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_profiles_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_profiles_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_profiles_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_comments_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_mview_comments_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mview_comments_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_scheduler_program_args_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_program_args_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_scheduler_program_args_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_scheduler_job_args_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_job_args_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_scheduler_job_args_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_errors_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_errors_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_errors_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_methods_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_type_methods_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_type_methods_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_method_params_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_method_params_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_method_params_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tablespaces_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_tablespaces_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ind_expressions_schema(share::schema::ObTableSchema &table_schema);
  static int user_ind_expressions_schema(share::schema::ObTableSchema &table_schema);
  static int all_ind_expressions_schema(share::schema::ObTableSchema &table_schema);
  static int all_ind_partitions_schema(share::schema::ObTableSchema &table_schema);
  static int user_ind_partitions_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ind_partitions_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ind_subpartitions_schema(share::schema::ObTableSchema &table_schema);
  static int all_ind_subpartitions_schema(share::schema::ObTableSchema &table_schema);
  static int user_ind_subpartitions_schema(share::schema::ObTableSchema &table_schema);
  static int dba_roles_schema(share::schema::ObTableSchema &table_schema);
  static int dba_role_privs_schema(share::schema::ObTableSchema &table_schema);
  static int user_role_privs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_privs_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_privs_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_privs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_sys_privs_schema(share::schema::ObTableSchema &table_schema);
  static int user_sys_privs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_col_privs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_col_privs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_col_privs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int role_tab_privs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int role_sys_privs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int role_role_privs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dictionary_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dict_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_triggers_schema(share::schema::ObTableSchema &table_schema);
  static int dba_triggers_schema(share::schema::ObTableSchema &table_schema);
  static int user_triggers_schema(share::schema::ObTableSchema &table_schema);
  static int all_dependencies_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_dependencies_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_dependencies_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_plans_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_plan_directives_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_group_mappings_schema(share::schema::ObTableSchema &table_schema);
  static int dba_recyclebin_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_recyclebin_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_consumer_groups_schema(share::schema::ObTableSchema &table_schema);
  static int gv_outline_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_audit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_audit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_instance_schema(share::schema::ObTableSchema &table_schema);
  static int v_instance_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_plan_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_plan_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_plan_explain_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_plan_explain_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_wait_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_wait_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_wait_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_wait_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_memory_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_memory_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_memstore_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_memstore_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_memstore_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_memstore_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_server_memstore_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sesstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sesstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sysstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sysstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_system_event_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_system_event_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_tenant_memstore_allocator_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_tenant_memstore_allocator_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_concurrent_limit_sql_ora_schema(share::schema::ObTableSchema &table_schema);
  static int nls_session_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int nls_instance_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int nls_database_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_nls_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_version_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_tenant_px_worker_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_tenant_px_worker_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ps_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ps_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ps_item_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ps_item_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_active_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_active_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_histogram_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_histogram_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_sql_workarea_memory_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sql_workarea_memory_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_reference_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_reference_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sstable_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sstable_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_server_schema_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_server_schema_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_plan_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_plan_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_monitor_statname_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_lock_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_lock_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_open_cursor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_open_cursor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_timezone_names_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_global_transaction_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_global_transaction_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_restore_point_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_rsrc_plan_ora_schema(share::schema::ObTableSchema &table_schema);
  static int triggers_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_piece_files_idx_data_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_v2_history_idx_data_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_history_idx_data_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_all_virtual_plan_cache_stat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_event_all_virtual_session_event_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_all_virtual_session_wait_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_all_virtual_session_wait_history_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_all_virtual_system_event_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_all_virtual_sesstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_all_virtual_sysstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_all_virtual_sql_audit_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_replica_task_all_virtual_replica_task_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_monitor_all_virtual_sql_plan_monitor_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_ora_all_virtual_sql_audit_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_ora_all_virtual_plan_cache_stat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_ora_all_virtual_session_wait_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_ora_all_virtual_session_wait_history_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_ora_all_virtual_sesstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_ora_all_virtual_sysstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_ora_all_virtual_system_event_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_monitor_ora_all_virtual_sql_plan_monitor_i1_schema(share::schema::ObTableSchema &table_schema);

private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerTableSchema);
};

typedef int (*schema_create_func)(share::schema::ObTableSchema &table_schema);

const schema_create_func core_table_schema_creators [] = {
  ObInnerTableSchema::all_root_table_schema,
  ObInnerTableSchema::all_table_schema,
  ObInnerTableSchema::all_column_schema,
  ObInnerTableSchema::all_ddl_operation_schema,
  ObInnerTableSchema::all_table_v2_schema,
  NULL,};

const schema_create_func sys_table_schema_creators [] = {
  ObInnerTableSchema::all_meta_table_schema,
  ObInnerTableSchema::all_user_schema,
  ObInnerTableSchema::all_user_history_schema,
  ObInnerTableSchema::all_database_schema,
  ObInnerTableSchema::all_database_history_schema,
  ObInnerTableSchema::all_tablegroup_schema,
  ObInnerTableSchema::all_tablegroup_history_schema,
  ObInnerTableSchema::all_tenant_schema,
  ObInnerTableSchema::all_tenant_history_schema,
  ObInnerTableSchema::all_table_privilege_schema,
  ObInnerTableSchema::all_table_privilege_history_schema,
  ObInnerTableSchema::all_database_privilege_schema,
  ObInnerTableSchema::all_database_privilege_history_schema,
  ObInnerTableSchema::all_table_history_schema,
  ObInnerTableSchema::all_column_history_schema,
  ObInnerTableSchema::all_zone_schema,
  ObInnerTableSchema::all_server_schema,
  ObInnerTableSchema::all_sys_parameter_schema,
  ObInnerTableSchema::tenant_parameter_schema,
  ObInnerTableSchema::all_sys_variable_schema,
  ObInnerTableSchema::all_sys_stat_schema,
  ObInnerTableSchema::all_column_statistic_schema,
  ObInnerTableSchema::all_unit_schema,
  ObInnerTableSchema::all_unit_config_schema,
  ObInnerTableSchema::all_resource_pool_schema,
  ObInnerTableSchema::all_tenant_resource_usage_schema,
  ObInnerTableSchema::all_sequence_schema,
  ObInnerTableSchema::all_charset_schema,
  ObInnerTableSchema::all_collation_schema,
  ObInnerTableSchema::all_local_index_status_schema,
  ObInnerTableSchema::all_dummy_schema,
  ObInnerTableSchema::all_frozen_map_schema,
  ObInnerTableSchema::all_clog_history_info_schema,
  ObInnerTableSchema::all_clog_history_info_v2_schema,
  ObInnerTableSchema::all_rootservice_event_history_schema,
  ObInnerTableSchema::all_privilege_schema,
  ObInnerTableSchema::all_outline_schema,
  ObInnerTableSchema::all_outline_history_schema,
  ObInnerTableSchema::all_election_event_history_schema,
  ObInnerTableSchema::all_recyclebin_schema,
  ObInnerTableSchema::all_part_schema,
  ObInnerTableSchema::all_part_history_schema,
  ObInnerTableSchema::all_sub_part_schema,
  ObInnerTableSchema::all_sub_part_history_schema,
  ObInnerTableSchema::all_part_info_schema,
  ObInnerTableSchema::all_part_info_history_schema,
  ObInnerTableSchema::all_def_sub_part_schema,
  ObInnerTableSchema::all_def_sub_part_history_schema,
  ObInnerTableSchema::all_server_event_history_schema,
  ObInnerTableSchema::all_rootservice_job_schema,
  ObInnerTableSchema::all_unit_load_history_schema,
  ObInnerTableSchema::all_sys_variable_history_schema,
  ObInnerTableSchema::all_restore_job_schema,
  ObInnerTableSchema::all_restore_task_schema,
  ObInnerTableSchema::all_restore_job_history_schema,
  ObInnerTableSchema::all_time_zone_schema,
  ObInnerTableSchema::all_time_zone_name_schema,
  ObInnerTableSchema::all_time_zone_transition_schema,
  ObInnerTableSchema::all_time_zone_transition_type_schema,
  ObInnerTableSchema::all_ddl_id_schema,
  ObInnerTableSchema::all_foreign_key_schema,
  ObInnerTableSchema::all_foreign_key_history_schema,
  ObInnerTableSchema::all_foreign_key_column_schema,
  ObInnerTableSchema::all_foreign_key_column_history_schema,
  ObInnerTableSchema::all_synonym_schema,
  ObInnerTableSchema::all_synonym_history_schema,
  ObInnerTableSchema::all_sequence_v2_schema,
  ObInnerTableSchema::all_tenant_meta_table_schema,
  ObInnerTableSchema::all_index_wait_transaction_status_schema,
  ObInnerTableSchema::all_index_schedule_task_schema,
  ObInnerTableSchema::all_index_checksum_schema,
  ObInnerTableSchema::all_routine_schema,
  ObInnerTableSchema::all_routine_history_schema,
  ObInnerTableSchema::all_routine_param_schema,
  ObInnerTableSchema::all_routine_param_history_schema,
  ObInnerTableSchema::all_table_stat_schema,
  ObInnerTableSchema::all_column_stat_schema,
  ObInnerTableSchema::all_histogram_stat_schema,
  ObInnerTableSchema::all_package_schema,
  ObInnerTableSchema::all_package_history_schema,
  ObInnerTableSchema::all_sql_execute_task_schema,
  ObInnerTableSchema::all_index_build_stat_schema,
  ObInnerTableSchema::all_build_index_param_schema,
  ObInnerTableSchema::all_global_index_data_src_schema,
  ObInnerTableSchema::all_acquired_snapshot_schema,
  ObInnerTableSchema::all_immediate_effect_index_sstable_schema,
  ObInnerTableSchema::all_sstable_checksum_schema,
  ObInnerTableSchema::all_tenant_gc_partition_info_schema,
  ObInnerTableSchema::all_constraint_schema,
  ObInnerTableSchema::all_constraint_history_schema,
  ObInnerTableSchema::all_ori_schema_version_schema,
  ObInnerTableSchema::all_func_schema,
  ObInnerTableSchema::all_func_history_schema,
  ObInnerTableSchema::all_temp_table_schema,
  ObInnerTableSchema::all_sstable_column_checksum_schema,
  ObInnerTableSchema::all_sequence_object_schema,
  ObInnerTableSchema::all_sequence_object_history_schema,
  ObInnerTableSchema::all_sequence_value_schema,
  ObInnerTableSchema::all_tenant_plan_baseline_schema,
  ObInnerTableSchema::all_tenant_plan_baseline_history_schema,
  ObInnerTableSchema::all_ddl_helper_schema,
  ObInnerTableSchema::all_freeze_schema_version_schema,
  ObInnerTableSchema::all_type_schema,
  ObInnerTableSchema::all_type_history_schema,
  ObInnerTableSchema::all_type_attr_schema,
  ObInnerTableSchema::all_type_attr_history_schema,
  ObInnerTableSchema::all_coll_type_schema,
  ObInnerTableSchema::all_coll_type_history_schema,
  ObInnerTableSchema::all_weak_read_service_schema,
  ObInnerTableSchema::all_gts_schema,
  ObInnerTableSchema::all_tenant_gts_schema,
  ObInnerTableSchema::all_partition_member_list_schema,
  ObInnerTableSchema::all_dblink_schema,
  ObInnerTableSchema::all_dblink_history_schema,
  ObInnerTableSchema::all_tenant_partition_meta_table_schema,
  ObInnerTableSchema::all_tenant_role_grantee_map_schema,
  ObInnerTableSchema::all_tenant_role_grantee_map_history_schema,
  ObInnerTableSchema::all_tenant_keystore_schema,
  ObInnerTableSchema::all_tenant_keystore_history_schema,
  ObInnerTableSchema::all_tenant_tablespace_schema,
  ObInnerTableSchema::all_tenant_tablespace_history_schema,
  ObInnerTableSchema::all_tenant_user_failed_login_stat_schema,
  ObInnerTableSchema::all_tenant_profile_schema,
  ObInnerTableSchema::all_tenant_profile_history_schema,
  ObInnerTableSchema::all_tenant_security_audit_schema,
  ObInnerTableSchema::all_tenant_security_audit_history_schema,
  ObInnerTableSchema::all_tenant_trigger_schema,
  ObInnerTableSchema::all_tenant_trigger_history_schema,
  ObInnerTableSchema::all_seed_parameter_schema,
  ObInnerTableSchema::all_tenant_sstable_column_checksum_schema,
  ObInnerTableSchema::all_tenant_security_audit_record_schema,
  ObInnerTableSchema::all_tenant_sysauth_schema,
  ObInnerTableSchema::all_tenant_sysauth_history_schema,
  ObInnerTableSchema::all_tenant_objauth_schema,
  ObInnerTableSchema::all_tenant_objauth_history_schema,
  ObInnerTableSchema::all_tenant_backup_info_schema,
  ObInnerTableSchema::all_restore_info_schema,
  ObInnerTableSchema::all_tenant_backup_log_archive_status_schema,
  ObInnerTableSchema::all_backup_log_archive_status_history_schema,
  ObInnerTableSchema::all_tenant_backup_task_schema,
  ObInnerTableSchema::all_backup_task_history_schema,
  ObInnerTableSchema::all_tenant_pg_backup_task_schema,
  ObInnerTableSchema::all_tenant_error_schema,
  ObInnerTableSchema::all_server_recovery_status_schema,
  ObInnerTableSchema::all_datafile_recovery_status_schema,
  ObInnerTableSchema::all_tenant_backup_clean_info_schema,
  ObInnerTableSchema::all_backup_clean_info_history_schema,
  ObInnerTableSchema::all_backup_task_clean_history_schema,
  ObInnerTableSchema::all_restore_progress_schema,
  ObInnerTableSchema::all_restore_history_schema,
  ObInnerTableSchema::all_tenant_restore_pg_info_schema,
  ObInnerTableSchema::all_table_v2_history_schema,
  ObInnerTableSchema::all_tenant_object_type_schema,
  ObInnerTableSchema::all_tenant_object_type_history_schema,
  ObInnerTableSchema::all_backup_validation_job_schema,
  ObInnerTableSchema::all_backup_validation_job_history_schema,
  ObInnerTableSchema::all_tenant_backup_validation_task_schema,
  ObInnerTableSchema::all_backup_validation_task_history_schema,
  ObInnerTableSchema::all_tenant_pg_backup_validation_task_schema,
  ObInnerTableSchema::all_tenant_time_zone_schema,
  ObInnerTableSchema::all_tenant_time_zone_name_schema,
  ObInnerTableSchema::all_tenant_time_zone_transition_schema,
  ObInnerTableSchema::all_tenant_time_zone_transition_type_schema,
  ObInnerTableSchema::all_tenant_constraint_column_schema,
  ObInnerTableSchema::all_tenant_constraint_column_history_schema,
  ObInnerTableSchema::all_tenant_global_transaction_schema,
  ObInnerTableSchema::all_tenant_dependency_schema,
  ObInnerTableSchema::all_backup_backupset_job_schema,
  ObInnerTableSchema::all_backup_backupset_job_history_schema,
  ObInnerTableSchema::all_tenant_backup_backupset_task_schema,
  ObInnerTableSchema::all_backup_backupset_task_history_schema,
  ObInnerTableSchema::all_tenant_pg_backup_backupset_task_schema,
  ObInnerTableSchema::all_tenant_backup_backup_log_archive_status_schema,
  ObInnerTableSchema::all_backup_backup_log_archive_status_history_schema,
  ObInnerTableSchema::all_res_mgr_plan_schema,
  ObInnerTableSchema::all_res_mgr_directive_schema,
  ObInnerTableSchema::all_res_mgr_mapping_rule_schema,
  ObInnerTableSchema::all_backup_backuppiece_job_schema,
  ObInnerTableSchema::all_backup_backuppiece_job_history_schema,
  ObInnerTableSchema::all_backup_backuppiece_task_schema,
  ObInnerTableSchema::all_backup_backuppiece_task_history_schema,
  ObInnerTableSchema::all_backup_piece_files_schema,
  ObInnerTableSchema::all_backup_set_files_schema,
  ObInnerTableSchema::all_res_mgr_consumer_group_schema,
  ObInnerTableSchema::all_backup_info_schema,
  ObInnerTableSchema::all_backup_log_archive_status_v2_schema,
  ObInnerTableSchema::all_backup_backup_log_archive_status_v2_schema,
  NULL,};

const schema_create_func virtual_table_schema_creators [] = {
  ObInnerTableSchema::tenant_virtual_all_table_schema,
  ObInnerTableSchema::tenant_virtual_table_column_schema,
  ObInnerTableSchema::tenant_virtual_table_index_schema,
  ObInnerTableSchema::tenant_virtual_show_create_database_schema,
  ObInnerTableSchema::tenant_virtual_show_create_table_schema,
  ObInnerTableSchema::tenant_virtual_session_variable_schema,
  ObInnerTableSchema::tenant_virtual_privilege_grant_schema,
  ObInnerTableSchema::all_virtual_processlist_schema,
  ObInnerTableSchema::tenant_virtual_warning_schema,
  ObInnerTableSchema::tenant_virtual_current_tenant_schema,
  ObInnerTableSchema::tenant_virtual_database_status_schema,
  ObInnerTableSchema::tenant_virtual_tenant_status_schema,
  ObInnerTableSchema::tenant_virtual_interm_result_schema,
  ObInnerTableSchema::tenant_virtual_partition_stat_schema,
  ObInnerTableSchema::tenant_virtual_statname_schema,
  ObInnerTableSchema::tenant_virtual_event_name_schema,
  ObInnerTableSchema::tenant_virtual_global_variable_schema,
  ObInnerTableSchema::tenant_virtual_show_tables_schema,
  ObInnerTableSchema::tenant_virtual_show_create_procedure_schema,
  ObInnerTableSchema::all_virtual_core_meta_table_schema,
  ObInnerTableSchema::all_virtual_zone_stat_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_schema,
  ObInnerTableSchema::all_virtual_plan_stat_schema,
  ObInnerTableSchema::all_virtual_mem_leak_checker_info_schema,
  ObInnerTableSchema::all_virtual_latch_schema,
  ObInnerTableSchema::all_virtual_kvcache_info_schema,
  ObInnerTableSchema::all_virtual_data_type_class_schema,
  ObInnerTableSchema::all_virtual_data_type_schema,
  ObInnerTableSchema::all_virtual_server_stat_schema,
  ObInnerTableSchema::all_virtual_rebalance_task_stat_schema,
  ObInnerTableSchema::all_virtual_session_event_schema,
  ObInnerTableSchema::all_virtual_session_wait_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_schema,
  ObInnerTableSchema::all_virtual_system_event_schema,
  ObInnerTableSchema::all_virtual_tenant_memstore_info_schema,
  ObInnerTableSchema::all_virtual_concurrency_object_pool_schema,
  ObInnerTableSchema::all_virtual_sesstat_schema,
  ObInnerTableSchema::all_virtual_sysstat_schema,
  ObInnerTableSchema::all_virtual_storage_stat_schema,
  ObInnerTableSchema::all_virtual_disk_stat_schema,
  ObInnerTableSchema::all_virtual_memstore_info_schema,
  ObInnerTableSchema::all_virtual_partition_info_schema,
  ObInnerTableSchema::all_virtual_upgrade_inspection_schema,
  ObInnerTableSchema::all_virtual_trans_stat_schema,
  ObInnerTableSchema::all_virtual_trans_mgr_stat_schema,
  ObInnerTableSchema::all_virtual_election_info_schema,
  ObInnerTableSchema::all_virtual_election_mem_stat_schema,
  ObInnerTableSchema::all_virtual_sql_audit_schema,
  ObInnerTableSchema::all_virtual_trans_mem_stat_schema,
  ObInnerTableSchema::all_virtual_partition_sstable_image_info_schema,
  ObInnerTableSchema::all_virtual_core_root_table_schema,
  ObInnerTableSchema::all_virtual_core_all_table_schema,
  ObInnerTableSchema::all_virtual_core_column_table_schema,
  ObInnerTableSchema::all_virtual_memory_info_schema,
  ObInnerTableSchema::all_virtual_tenant_stat_schema,
  ObInnerTableSchema::all_virtual_sys_parameter_stat_schema,
  ObInnerTableSchema::all_virtual_partition_replay_status_schema,
  ObInnerTableSchema::all_virtual_clog_stat_schema,
  ObInnerTableSchema::all_virtual_trace_log_schema,
  ObInnerTableSchema::all_virtual_engine_schema,
  ObInnerTableSchema::all_virtual_proxy_server_stat_schema,
  ObInnerTableSchema::all_virtual_proxy_sys_variable_schema,
  ObInnerTableSchema::all_virtual_proxy_schema_schema,
  ObInnerTableSchema::all_virtual_plan_cache_plan_explain_schema,
  ObInnerTableSchema::all_virtual_obrpc_stat_schema,
  ObInnerTableSchema::all_virtual_partition_sstable_merge_info_schema,
  ObInnerTableSchema::all_virtual_sql_monitor_schema,
  ObInnerTableSchema::tenant_virtual_outline_schema,
  ObInnerTableSchema::tenant_virtual_concurrent_limit_sql_schema,
  ObInnerTableSchema::all_virtual_sql_plan_statistics_schema,
  ObInnerTableSchema::all_virtual_partition_sstable_macro_info_schema,
  ObInnerTableSchema::all_virtual_proxy_partition_info_schema,
  ObInnerTableSchema::all_virtual_proxy_partition_schema,
  ObInnerTableSchema::all_virtual_proxy_sub_partition_schema,
  ObInnerTableSchema::all_virtual_proxy_route_schema,
  ObInnerTableSchema::all_virtual_rebalance_tenant_stat_schema,
  ObInnerTableSchema::all_virtual_rebalance_unit_stat_schema,
  ObInnerTableSchema::all_virtual_rebalance_replica_stat_schema,
  ObInnerTableSchema::all_virtual_partition_amplification_stat_schema,
  ObInnerTableSchema::all_virtual_election_event_history_schema,
  ObInnerTableSchema::all_virtual_partition_store_info_schema,
  ObInnerTableSchema::all_virtual_leader_stat_schema,
  ObInnerTableSchema::all_virtual_partition_migration_status_schema,
  ObInnerTableSchema::all_virtual_sys_task_status_schema,
  ObInnerTableSchema::all_virtual_macro_block_marker_status_schema,
  ObInnerTableSchema::all_virtual_server_clog_stat_schema,
  ObInnerTableSchema::all_virtual_rootservice_stat_schema,
  ObInnerTableSchema::all_virtual_election_priority_schema,
  ObInnerTableSchema::all_virtual_tenant_disk_stat_schema,
  ObInnerTableSchema::all_virtual_rebalance_map_stat_schema,
  ObInnerTableSchema::all_virtual_rebalance_map_item_stat_schema,
  ObInnerTableSchema::all_virtual_io_stat_schema,
  ObInnerTableSchema::all_virtual_long_ops_status_schema,
  ObInnerTableSchema::all_virtual_rebalance_unit_migrate_stat_schema,
  ObInnerTableSchema::all_virtual_rebalance_unit_distribution_stat_schema,
  ObInnerTableSchema::all_virtual_server_object_pool_schema,
  ObInnerTableSchema::all_virtual_trans_lock_stat_schema,
  ObInnerTableSchema::all_virtual_election_group_info_schema,
  ObInnerTableSchema::tenant_virtual_show_create_tablegroup_schema,
  ObInnerTableSchema::all_virtual_server_blacklist_schema,
  ObInnerTableSchema::all_virtual_partition_split_info_schema,
  ObInnerTableSchema::all_virtual_trans_result_info_stat_schema,
  ObInnerTableSchema::all_virtual_duplicate_partition_mgr_stat_schema,
  ObInnerTableSchema::all_virtual_tenant_parameter_stat_schema,
  ObInnerTableSchema::all_virtual_server_schema_info_schema,
  ObInnerTableSchema::all_virtual_memory_context_stat_schema,
  ObInnerTableSchema::all_virtual_dump_tenant_info_schema,
  ObInnerTableSchema::all_virtual_tenant_parameter_info_schema,
  ObInnerTableSchema::all_virtual_dag_warning_history_schema,
  ObInnerTableSchema::virtual_show_restore_preview_schema,
  ObInnerTableSchema::all_virtual_master_key_version_info_schema,
  ObInnerTableSchema::session_variables_schema,
  ObInnerTableSchema::table_privileges_schema,
  ObInnerTableSchema::user_privileges_schema,
  ObInnerTableSchema::schema_privileges_schema,
  ObInnerTableSchema::table_constraints_schema,
  ObInnerTableSchema::global_status_schema,
  ObInnerTableSchema::partitions_schema,
  ObInnerTableSchema::session_status_schema,
  ObInnerTableSchema::user_schema,
  ObInnerTableSchema::db_schema,
  ObInnerTableSchema::all_virtual_server_memory_info_schema,
  ObInnerTableSchema::all_virtual_partition_table_schema,
  ObInnerTableSchema::all_virtual_lock_wait_stat_schema,
  ObInnerTableSchema::all_virtual_partition_item_schema,
  ObInnerTableSchema::all_virtual_replica_task_schema,
  ObInnerTableSchema::all_virtual_partition_location_schema,
  ObInnerTableSchema::proc_schema,
  ObInnerTableSchema::tenant_virtual_collation_schema,
  ObInnerTableSchema::tenant_virtual_charset_schema,
  ObInnerTableSchema::all_virtual_tenant_memstore_allocator_info_schema,
  ObInnerTableSchema::all_virtual_table_mgr_schema,
  ObInnerTableSchema::all_virtual_meta_table_schema,
  ObInnerTableSchema::all_virtual_freeze_info_schema,
  ObInnerTableSchema::parameters_schema,
  ObInnerTableSchema::all_virtual_bad_block_table_schema,
  ObInnerTableSchema::all_virtual_px_worker_stat_schema,
  ObInnerTableSchema::all_virtual_trans_audit_schema,
  ObInnerTableSchema::all_virtual_trans_sql_audit_schema,
  ObInnerTableSchema::all_virtual_weak_read_stat_schema,
  ObInnerTableSchema::all_virtual_partition_audit_schema,
  ObInnerTableSchema::all_virtual_sequence_v2_schema,
  ObInnerTableSchema::all_virtual_sequence_value_schema,
  ObInnerTableSchema::all_virtual_cluster_schema,
  ObInnerTableSchema::all_virtual_partition_table_store_stat_schema,
  ObInnerTableSchema::all_virtual_ddl_operation_schema,
  ObInnerTableSchema::all_virtual_outline_schema,
  ObInnerTableSchema::all_virtual_outline_history_schema,
  ObInnerTableSchema::all_virtual_synonym_schema,
  ObInnerTableSchema::all_virtual_synonym_history_schema,
  ObInnerTableSchema::all_virtual_database_privilege_schema,
  ObInnerTableSchema::all_virtual_database_privilege_history_schema,
  ObInnerTableSchema::all_virtual_table_privilege_schema,
  ObInnerTableSchema::all_virtual_table_privilege_history_schema,
  ObInnerTableSchema::all_virtual_database_schema,
  ObInnerTableSchema::all_virtual_database_history_schema,
  ObInnerTableSchema::all_virtual_tablegroup_schema,
  ObInnerTableSchema::all_virtual_tablegroup_history_schema,
  ObInnerTableSchema::all_virtual_table_schema,
  ObInnerTableSchema::all_virtual_table_history_schema,
  ObInnerTableSchema::all_virtual_column_schema,
  ObInnerTableSchema::all_virtual_column_history_schema,
  ObInnerTableSchema::all_virtual_part_schema,
  ObInnerTableSchema::all_virtual_part_history_schema,
  ObInnerTableSchema::all_virtual_part_info_schema,
  ObInnerTableSchema::all_virtual_part_info_history_schema,
  ObInnerTableSchema::all_virtual_def_sub_part_schema,
  ObInnerTableSchema::all_virtual_def_sub_part_history_schema,
  ObInnerTableSchema::all_virtual_sub_part_schema,
  ObInnerTableSchema::all_virtual_sub_part_history_schema,
  ObInnerTableSchema::all_virtual_constraint_schema,
  ObInnerTableSchema::all_virtual_constraint_history_schema,
  ObInnerTableSchema::all_virtual_foreign_key_schema,
  ObInnerTableSchema::all_virtual_foreign_key_history_schema,
  ObInnerTableSchema::all_virtual_foreign_key_column_schema,
  ObInnerTableSchema::all_virtual_foreign_key_column_history_schema,
  ObInnerTableSchema::all_virtual_temp_table_schema,
  ObInnerTableSchema::all_virtual_ori_schema_version_schema,
  ObInnerTableSchema::all_virtual_sys_stat_schema,
  ObInnerTableSchema::all_virtual_user_schema,
  ObInnerTableSchema::all_virtual_user_history_schema,
  ObInnerTableSchema::all_virtual_sys_variable_schema,
  ObInnerTableSchema::all_virtual_sys_variable_history_schema,
  ObInnerTableSchema::all_virtual_func_schema,
  ObInnerTableSchema::all_virtual_func_history_schema,
  ObInnerTableSchema::all_virtual_package_schema,
  ObInnerTableSchema::all_virtual_package_history_schema,
  ObInnerTableSchema::all_virtual_routine_schema,
  ObInnerTableSchema::all_virtual_routine_history_schema,
  ObInnerTableSchema::all_virtual_routine_param_schema,
  ObInnerTableSchema::all_virtual_routine_param_history_schema,
  ObInnerTableSchema::all_virtual_type_schema,
  ObInnerTableSchema::all_virtual_type_history_schema,
  ObInnerTableSchema::all_virtual_type_attr_schema,
  ObInnerTableSchema::all_virtual_type_attr_history_schema,
  ObInnerTableSchema::all_virtual_coll_type_schema,
  ObInnerTableSchema::all_virtual_coll_type_history_schema,
  ObInnerTableSchema::all_virtual_column_stat_schema,
  ObInnerTableSchema::all_virtual_table_stat_schema,
  ObInnerTableSchema::all_virtual_histogram_stat_schema,
  ObInnerTableSchema::all_virtual_column_statistic_schema,
  ObInnerTableSchema::all_virtual_recyclebin_schema,
  ObInnerTableSchema::all_virtual_tenant_gc_partition_info_schema,
  ObInnerTableSchema::all_virtual_tenant_plan_baseline_schema,
  ObInnerTableSchema::all_virtual_tenant_plan_baseline_history_schema,
  ObInnerTableSchema::all_virtual_sequence_object_schema,
  ObInnerTableSchema::all_virtual_sequence_object_history_schema,
  ObInnerTableSchema::all_virtual_raid_stat_schema,
  ObInnerTableSchema::all_virtual_server_log_meta_schema,
  ObInnerTableSchema::all_virtual_dtl_channel_schema,
  ObInnerTableSchema::all_virtual_dtl_memory_schema,
  ObInnerTableSchema::all_virtual_dtl_first_cached_buffer_schema,
  ObInnerTableSchema::all_virtual_dblink_schema,
  ObInnerTableSchema::all_virtual_dblink_history_schema,
  ObInnerTableSchema::all_virtual_tenant_partition_meta_table_schema,
  ObInnerTableSchema::all_virtual_tenant_role_grantee_map_schema,
  ObInnerTableSchema::all_virtual_tenant_role_grantee_map_history_schema,
  ObInnerTableSchema::all_virtual_tenant_keystore_schema,
  ObInnerTableSchema::all_virtual_tenant_keystore_history_schema,
  ObInnerTableSchema::all_virtual_deadlock_stat_schema,
  ObInnerTableSchema::all_virtual_tenant_tablespace_schema,
  ObInnerTableSchema::all_virtual_tenant_tablespace_history_schema,
  ObInnerTableSchema::all_virtual_information_columns_schema,
  ObInnerTableSchema::all_virtual_pg_partition_info_schema,
  ObInnerTableSchema::all_virtual_tenant_user_failed_login_stat_schema,
  ObInnerTableSchema::all_virtual_tenant_profile_schema,
  ObInnerTableSchema::all_virtual_tenant_profile_history_schema,
  ObInnerTableSchema::all_virtual_security_audit_schema,
  ObInnerTableSchema::all_virtual_security_audit_history_schema,
  ObInnerTableSchema::all_virtual_trigger_schema,
  ObInnerTableSchema::all_virtual_trigger_history_schema,
  ObInnerTableSchema::all_virtual_cluster_stats_schema,
  ObInnerTableSchema::all_virtual_sstable_column_checksum_schema,
  ObInnerTableSchema::all_virtual_ps_stat_schema,
  ObInnerTableSchema::all_virtual_ps_item_info_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_history_stat_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_active_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_histogram_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_memory_info_schema,
  ObInnerTableSchema::all_virtual_security_audit_record_schema,
  ObInnerTableSchema::all_virtual_sysauth_schema,
  ObInnerTableSchema::all_virtual_sysauth_history_schema,
  ObInnerTableSchema::all_virtual_objauth_schema,
  ObInnerTableSchema::all_virtual_objauth_history_schema,
  ObInnerTableSchema::all_virtual_backup_info_schema,
  ObInnerTableSchema::all_virtual_backup_task_schema,
  ObInnerTableSchema::all_virtual_pg_backup_task_schema,
  ObInnerTableSchema::all_virtual_pg_backup_log_archive_status_schema,
  ObInnerTableSchema::all_virtual_server_backup_log_archive_status_schema,
  ObInnerTableSchema::all_virtual_error_schema,
  ObInnerTableSchema::all_virtual_timestamp_service_schema,
  ObInnerTableSchema::referential_constraints_schema,
  ObInnerTableSchema::all_virtual_table_modifications_schema,
  ObInnerTableSchema::all_virtual_backup_clean_info_schema,
  ObInnerTableSchema::all_virtual_restore_pg_info_schema,
  ObInnerTableSchema::all_virtual_object_type_schema,
  ObInnerTableSchema::all_virtual_trans_table_status_schema,
  ObInnerTableSchema::all_virtual_pg_log_archive_stat_schema,
  ObInnerTableSchema::all_virtual_sql_plan_monitor_schema,
  ObInnerTableSchema::all_virtual_sql_monitor_statname_schema,
  ObInnerTableSchema::all_virtual_open_cursor_schema,
  ObInnerTableSchema::all_virtual_backup_validation_task_schema,
  ObInnerTableSchema::all_virtual_pg_backup_validation_task_schema,
  ObInnerTableSchema::all_virtual_time_zone_schema,
  ObInnerTableSchema::all_virtual_time_zone_name_schema,
  ObInnerTableSchema::all_virtual_time_zone_transition_schema,
  ObInnerTableSchema::all_virtual_time_zone_transition_type_schema,
  ObInnerTableSchema::all_virtual_constraint_column_schema,
  ObInnerTableSchema::all_virtual_constraint_column_history_schema,
  ObInnerTableSchema::all_virtual_files_schema,
  ObInnerTableSchema::files_schema,
  ObInnerTableSchema::all_virtual_dependency_schema,
  ObInnerTableSchema::tenant_virtual_object_definition_schema,
  ObInnerTableSchema::all_virtual_reserved_table_mgr_schema,
  ObInnerTableSchema::all_virtual_backupset_history_mgr_schema,
  ObInnerTableSchema::all_virtual_backup_backupset_task_schema,
  ObInnerTableSchema::all_virtual_pg_backup_backupset_task_schema,
  ObInnerTableSchema::all_virtual_global_transaction_schema,
  ObInnerTableSchema::all_virtual_table_agent_schema,
  ObInnerTableSchema::all_virtual_column_agent_schema,
  ObInnerTableSchema::all_virtual_database_agent_schema,
  ObInnerTableSchema::all_virtual_sequence_v2_agent_schema,
  ObInnerTableSchema::all_virtual_part_agent_schema,
  ObInnerTableSchema::all_virtual_sub_part_agent_schema,
  ObInnerTableSchema::all_virtual_package_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_meta_table_agent_schema,
  ObInnerTableSchema::all_virtual_sql_audit_ora_schema,
  ObInnerTableSchema::all_virtual_plan_stat_ora_schema,
  ObInnerTableSchema::all_virtual_sql_plan_statistics_agent_schema,
  ObInnerTableSchema::all_virtual_plan_cache_plan_explain_ora_schema,
  ObInnerTableSchema::all_virtual_sequence_value_agent_schema,
  ObInnerTableSchema::all_virtual_sequence_object_agent_schema,
  ObInnerTableSchema::all_virtual_user_agent_schema,
  ObInnerTableSchema::all_virtual_synonym_agent_schema,
  ObInnerTableSchema::all_virtual_foreign_key_agent_schema,
  ObInnerTableSchema::all_virtual_column_stat_agent_schema,
  ObInnerTableSchema::all_virtual_column_statistic_agent_schema,
  ObInnerTableSchema::all_virtual_partition_table_agent_schema,
  ObInnerTableSchema::all_virtual_table_stat_agent_schema,
  ObInnerTableSchema::all_virtual_recyclebin_agent_schema,
  ObInnerTableSchema::tenant_virtual_outline_agent_schema,
  ObInnerTableSchema::all_virtual_routine_agent_schema,
  ObInnerTableSchema::all_virtual_tablegroup_agent_schema,
  ObInnerTableSchema::all_virtual_privilege_agent_schema,
  ObInnerTableSchema::all_virtual_sys_parameter_stat_agent_schema,
  ObInnerTableSchema::tenant_virtual_table_index_agent_schema,
  ObInnerTableSchema::tenant_virtual_charset_agent_schema,
  ObInnerTableSchema::tenant_virtual_all_table_agent_schema,
  ObInnerTableSchema::tenant_virtual_collation_agent_schema,
  ObInnerTableSchema::all_virtual_foreign_key_column_agent_schema,
  ObInnerTableSchema::all_virtual_server_agent_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_ora_schema,
  ObInnerTableSchema::all_virtual_processlist_ora_schema,
  ObInnerTableSchema::all_virtual_session_wait_ora_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_ora_schema,
  ObInnerTableSchema::all_virtual_memory_info_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_memstore_info_ora_schema,
  ObInnerTableSchema::all_virtual_memstore_info_ora_schema,
  ObInnerTableSchema::all_virtual_server_memory_info_agent_schema,
  ObInnerTableSchema::all_virtual_sesstat_ora_schema,
  ObInnerTableSchema::all_virtual_sysstat_ora_schema,
  ObInnerTableSchema::all_virtual_system_event_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_memstore_allocator_info_agent_schema,
  ObInnerTableSchema::tenant_virtual_session_variable_ora_schema,
  ObInnerTableSchema::tenant_virtual_global_variable_ora_schema,
  ObInnerTableSchema::tenant_virtual_show_create_table_ora_schema,
  ObInnerTableSchema::tenant_virtual_show_create_procedure_ora_schema,
  ObInnerTableSchema::tenant_virtual_show_create_tablegroup_ora_schema,
  ObInnerTableSchema::tenant_virtual_privilege_grant_ora_schema,
  ObInnerTableSchema::tenant_virtual_table_column_ora_schema,
  ObInnerTableSchema::all_virtual_trace_log_ora_schema,
  ObInnerTableSchema::tenant_virtual_concurrent_limit_sql_agent_schema,
  ObInnerTableSchema::all_virtual_constraint_agent_schema,
  ObInnerTableSchema::all_virtual_type_agent_schema,
  ObInnerTableSchema::all_virtual_type_attr_agent_schema,
  ObInnerTableSchema::all_virtual_coll_type_agent_schema,
  ObInnerTableSchema::all_virtual_routine_param_agent_schema,
  ObInnerTableSchema::all_virtual_data_type_ora_schema,
  ObInnerTableSchema::all_virtual_table_sys_agent_schema,
  ObInnerTableSchema::all_virtual_sstable_checksum_agent_schema,
  ObInnerTableSchema::all_virtual_partition_info_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_partition_meta_table_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_keystore_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_tablespace_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_profile_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_role_grantee_map_agent_schema,
  ObInnerTableSchema::all_virtual_table_privilege_agent_schema,
  ObInnerTableSchema::all_virtual_security_audit_agent_schema,
  ObInnerTableSchema::all_virtual_security_audit_history_agent_schema,
  ObInnerTableSchema::all_virtual_trigger_agent_schema,
  ObInnerTableSchema::all_virtual_px_worker_stat_ora_schema,
  ObInnerTableSchema::all_virtual_ps_stat_ora_schema,
  ObInnerTableSchema::all_virtual_ps_item_info_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_parameter_stat_ora_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_history_stat_agent_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_active_agent_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_histogram_agent_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_memory_info_agent_schema,
  ObInnerTableSchema::all_virtual_security_audit_record_agent_schema,
  ObInnerTableSchema::all_virtual_sysauth_agent_schema,
  ObInnerTableSchema::all_virtual_sysauth_history_agent_schema,
  ObInnerTableSchema::all_virtual_objauth_agent_schema,
  ObInnerTableSchema::all_virtual_objauth_history_agent_schema,
  ObInnerTableSchema::all_virtual_error_agent_schema,
  ObInnerTableSchema::all_virtual_table_mgr_agent_schema,
  ObInnerTableSchema::all_virtual_def_sub_part_agent_schema,
  ObInnerTableSchema::all_virtual_object_type_agent_schema,
  ObInnerTableSchema::all_virtual_server_schema_info_agent_schema,
  ObInnerTableSchema::all_virtual_dblink_agent_schema,
  ObInnerTableSchema::all_virtual_dblink_history_agent_schema,
  ObInnerTableSchema::all_virtual_sql_plan_monitor_ora_schema,
  ObInnerTableSchema::all_virtual_sql_monitor_statname_ora_schema,
  ObInnerTableSchema::all_virtual_lock_wait_stat_ora_schema,
  ObInnerTableSchema::all_virtual_open_cursor_ora_schema,
  ObInnerTableSchema::all_virtual_constraint_column_agent_schema,
  ObInnerTableSchema::all_virtual_dependency_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_name_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_transition_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_transition_type_agent_schema,
  ObInnerTableSchema::tenant_virtual_object_definition_ora_schema,
  ObInnerTableSchema::all_virtual_routine_param_sys_agent_schema,
  ObInnerTableSchema::all_virtual_type_sys_agent_schema,
  ObInnerTableSchema::all_virtual_type_attr_sys_agent_schema,
  ObInnerTableSchema::all_virtual_coll_type_sys_agent_schema,
  ObInnerTableSchema::all_virtual_package_sys_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_trigger_sys_agent_schema,
  ObInnerTableSchema::all_virtual_routine_sys_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_global_transaction_agent_schema,
  ObInnerTableSchema::all_virtual_acquired_snapshot_agent_schema,
  ObInnerTableSchema::all_virtual_table_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_column_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_database_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_sequence_v2_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_part_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_sub_part_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_package_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_sequence_value_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_sequence_object_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_user_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_synonym_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_foreign_key_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_column_stat_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_column_statistic_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_table_stat_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_recyclebin_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_routine_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tablegroup_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_foreign_key_column_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_constraint_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_type_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_type_attr_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_coll_type_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_routine_param_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_partition_meta_table_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_keystore_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_tablespace_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_profile_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_role_grantee_map_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_table_privilege_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_security_audit_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_security_audit_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_trigger_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_security_audit_record_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_sysauth_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_sysauth_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_objauth_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_objauth_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_error_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_def_sub_part_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_object_type_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_dblink_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_dblink_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_constraint_column_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_dependency_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_meta_table_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_name_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_transition_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_transition_type_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_res_mgr_plan_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_res_mgr_directive_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_trans_lock_stat_ora_schema,
  ObInnerTableSchema::all_virtual_res_mgr_mapping_rule_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_res_mgr_consumer_group_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_all_virtual_plan_cache_stat_i1_schema,
  ObInnerTableSchema::all_virtual_session_event_all_virtual_session_event_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_all_virtual_session_wait_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_all_virtual_session_wait_history_i1_schema,
  ObInnerTableSchema::all_virtual_system_event_all_virtual_system_event_i1_schema,
  ObInnerTableSchema::all_virtual_sesstat_all_virtual_sesstat_i1_schema,
  ObInnerTableSchema::all_virtual_sysstat_all_virtual_sysstat_i1_schema,
  ObInnerTableSchema::all_virtual_sql_audit_all_virtual_sql_audit_i1_schema,
  ObInnerTableSchema::all_virtual_replica_task_all_virtual_replica_task_i1_schema,
  ObInnerTableSchema::all_virtual_sql_plan_monitor_all_virtual_sql_plan_monitor_i1_schema,
  ObInnerTableSchema::all_virtual_sql_audit_ora_all_virtual_sql_audit_i1_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_ora_all_virtual_plan_cache_stat_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_ora_all_virtual_session_wait_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_ora_all_virtual_session_wait_history_i1_schema,
  ObInnerTableSchema::all_virtual_sesstat_ora_all_virtual_sesstat_i1_schema,
  ObInnerTableSchema::all_virtual_sysstat_ora_all_virtual_sysstat_i1_schema,
  ObInnerTableSchema::all_virtual_system_event_ora_all_virtual_system_event_i1_schema,
  ObInnerTableSchema::all_virtual_sql_plan_monitor_ora_all_virtual_sql_plan_monitor_i1_schema,
  NULL,};

const schema_create_func sys_view_schema_creators [] = {
  ObInnerTableSchema::gv_plan_cache_stat_schema,
  ObInnerTableSchema::gv_plan_cache_plan_stat_schema,
  ObInnerTableSchema::schemata_schema,
  ObInnerTableSchema::character_sets_schema,
  ObInnerTableSchema::global_variables_schema,
  ObInnerTableSchema::statistics_schema,
  ObInnerTableSchema::views_schema,
  ObInnerTableSchema::tables_schema,
  ObInnerTableSchema::collations_schema,
  ObInnerTableSchema::collation_character_set_applicability_schema,
  ObInnerTableSchema::processlist_schema,
  ObInnerTableSchema::key_column_usage_schema,
  ObInnerTableSchema::dba_outlines_schema,
  ObInnerTableSchema::engines_schema,
  ObInnerTableSchema::routines_schema,
  ObInnerTableSchema::gv_session_event_schema,
  ObInnerTableSchema::gv_session_wait_schema,
  ObInnerTableSchema::gv_session_wait_history_schema,
  ObInnerTableSchema::gv_system_event_schema,
  ObInnerTableSchema::gv_sesstat_schema,
  ObInnerTableSchema::gv_sysstat_schema,
  ObInnerTableSchema::v_statname_schema,
  ObInnerTableSchema::v_event_name_schema,
  ObInnerTableSchema::v_session_event_schema,
  ObInnerTableSchema::v_session_wait_schema,
  ObInnerTableSchema::v_session_wait_history_schema,
  ObInnerTableSchema::v_sesstat_schema,
  ObInnerTableSchema::v_sysstat_schema,
  ObInnerTableSchema::v_system_event_schema,
  ObInnerTableSchema::gv_sql_audit_schema,
  ObInnerTableSchema::gv_latch_schema,
  ObInnerTableSchema::gv_memory_schema,
  ObInnerTableSchema::v_memory_schema,
  ObInnerTableSchema::gv_memstore_schema,
  ObInnerTableSchema::v_memstore_schema,
  ObInnerTableSchema::gv_memstore_info_schema,
  ObInnerTableSchema::v_memstore_info_schema,
  ObInnerTableSchema::v_plan_cache_stat_schema,
  ObInnerTableSchema::v_plan_cache_plan_stat_schema,
  ObInnerTableSchema::gv_plan_cache_plan_explain_schema,
  ObInnerTableSchema::v_plan_cache_plan_explain_schema,
  ObInnerTableSchema::v_sql_audit_schema,
  ObInnerTableSchema::v_latch_schema,
  ObInnerTableSchema::gv_obrpc_outgoing_schema,
  ObInnerTableSchema::v_obrpc_outgoing_schema,
  ObInnerTableSchema::gv_obrpc_incoming_schema,
  ObInnerTableSchema::v_obrpc_incoming_schema,
  ObInnerTableSchema::gv_sql_schema,
  ObInnerTableSchema::v_sql_schema,
  ObInnerTableSchema::gv_sql_monitor_schema,
  ObInnerTableSchema::v_sql_monitor_schema,
  ObInnerTableSchema::gv_sql_plan_monitor_schema,
  ObInnerTableSchema::v_sql_plan_monitor_schema,
  ObInnerTableSchema::user_recyclebin_schema,
  ObInnerTableSchema::gv_outline_schema,
  ObInnerTableSchema::gv_concurrent_limit_sql_schema,
  ObInnerTableSchema::gv_sql_plan_statistics_schema,
  ObInnerTableSchema::v_sql_plan_statistics_schema,
  ObInnerTableSchema::gv_server_memstore_schema,
  ObInnerTableSchema::gv_unit_load_balance_event_history_schema,
  ObInnerTableSchema::gv_tenant_schema,
  ObInnerTableSchema::gv_database_schema,
  ObInnerTableSchema::gv_table_schema,
  ObInnerTableSchema::gv_unit_schema,
  ObInnerTableSchema::v_unit_schema,
  ObInnerTableSchema::gv_partition_schema,
  ObInnerTableSchema::v_partition_schema,
  ObInnerTableSchema::gv_lock_wait_stat_schema,
  ObInnerTableSchema::v_lock_wait_stat_schema,
  ObInnerTableSchema::time_zone_schema,
  ObInnerTableSchema::time_zone_name_schema,
  ObInnerTableSchema::time_zone_transition_schema,
  ObInnerTableSchema::time_zone_transition_type_schema,
  ObInnerTableSchema::gv_session_longops_schema,
  ObInnerTableSchema::v_session_longops_schema,
  ObInnerTableSchema::gv_tenant_memstore_allocator_info_schema,
  ObInnerTableSchema::v_tenant_memstore_allocator_info_schema,
  ObInnerTableSchema::gv_tenant_sequence_object_schema,
  ObInnerTableSchema::columns_schema,
  ObInnerTableSchema::gv_minor_merge_info_schema,
  ObInnerTableSchema::gv_tenant_px_worker_stat_schema,
  ObInnerTableSchema::v_tenant_px_worker_stat_schema,
  ObInnerTableSchema::gv_partition_audit_schema,
  ObInnerTableSchema::v_partition_audit_schema,
  ObInnerTableSchema::v_ob_cluster_schema,
  ObInnerTableSchema::gv_ps_stat_schema,
  ObInnerTableSchema::v_ps_stat_schema,
  ObInnerTableSchema::gv_ps_item_info_schema,
  ObInnerTableSchema::v_ps_item_info_schema,
  ObInnerTableSchema::gv_sql_workarea_schema,
  ObInnerTableSchema::v_sql_workarea_schema,
  ObInnerTableSchema::gv_sql_workarea_active_schema,
  ObInnerTableSchema::v_sql_workarea_active_schema,
  ObInnerTableSchema::gv_sql_workarea_histogram_schema,
  ObInnerTableSchema::v_sql_workarea_histogram_schema,
  ObInnerTableSchema::gv_ob_sql_workarea_memory_info_schema,
  ObInnerTableSchema::v_ob_sql_workarea_memory_info_schema,
  ObInnerTableSchema::gv_plan_cache_reference_info_schema,
  ObInnerTableSchema::v_plan_cache_reference_info_schema,
  ObInnerTableSchema::v_ob_timestamp_service_schema,
  ObInnerTableSchema::gv_sstable_schema,
  ObInnerTableSchema::v_sstable_schema,
  ObInnerTableSchema::cdb_ob_backup_archivelog_summary_schema,
  ObInnerTableSchema::cdb_ob_backup_job_details_schema,
  ObInnerTableSchema::cdb_ob_backup_set_details_schema,
  ObInnerTableSchema::cdb_ob_backup_set_expired_schema,
  ObInnerTableSchema::cdb_ob_backup_progress_schema,
  ObInnerTableSchema::cdb_ob_backup_archivelog_progress_schema,
  ObInnerTableSchema::cdb_ob_backup_clean_history_schema,
  ObInnerTableSchema::cdb_ob_backup_task_clean_history_schema,
  ObInnerTableSchema::cdb_ob_restore_progress_schema,
  ObInnerTableSchema::cdb_ob_restore_history_schema,
  ObInnerTableSchema::gv_server_schema_info_schema,
  ObInnerTableSchema::v_server_schema_info_schema,
  ObInnerTableSchema::cdb_ckpt_history_schema,
  ObInnerTableSchema::gv_ob_trans_table_status_schema,
  ObInnerTableSchema::v_ob_trans_table_status_schema,
  ObInnerTableSchema::v_sql_monitor_statname_schema,
  ObInnerTableSchema::gv_merge_info_schema,
  ObInnerTableSchema::v_merge_info_schema,
  ObInnerTableSchema::gv_lock_schema,
  ObInnerTableSchema::v_lock_schema,
  ObInnerTableSchema::cdb_ob_backup_validation_job_schema,
  ObInnerTableSchema::cdb_ob_backup_validation_job_history_schema,
  ObInnerTableSchema::cdb_ob_tenant_backup_validation_task_schema,
  ObInnerTableSchema::cdb_ob_backup_validation_task_history_schema,
  ObInnerTableSchema::v_restore_point_schema,
  ObInnerTableSchema::cdb_ob_backup_set_obsolete_schema,
  ObInnerTableSchema::cdb_ob_backup_backupset_job_schema,
  ObInnerTableSchema::cdb_ob_backup_backupset_job_history_schema,
  ObInnerTableSchema::cdb_ob_backup_backupset_task_schema,
  ObInnerTableSchema::cdb_ob_backup_backupset_task_history_schema,
  ObInnerTableSchema::cdb_ob_backup_backup_archivelog_summary_schema,
  ObInnerTableSchema::cdb_ob_backup_piece_files_schema,
  ObInnerTableSchema::cdb_ob_backup_set_files_schema,
  ObInnerTableSchema::cdb_ob_backup_backuppiece_job_schema,
  ObInnerTableSchema::cdb_ob_backup_backuppiece_job_history_schema,
  ObInnerTableSchema::cdb_ob_backup_backuppiece_task_schema,
  ObInnerTableSchema::cdb_ob_backup_backuppiece_task_history_schema,
  ObInnerTableSchema::v_ob_all_clusters_schema,
  ObInnerTableSchema::cdb_ob_backup_archivelog_schema,
  ObInnerTableSchema::cdb_ob_backup_backup_archivelog_schema,
  ObInnerTableSchema::column_privileges_schema,
  ObInnerTableSchema::show_rabbit_schema,
  ObInnerTableSchema::dba_synonyms_schema,
  ObInnerTableSchema::dba_objects_schema,
  ObInnerTableSchema::all_objects_schema,
  ObInnerTableSchema::user_objects_schema,
  ObInnerTableSchema::dba_sequences_schema,
  ObInnerTableSchema::all_sequences_schema,
  ObInnerTableSchema::user_sequences_schema,
  ObInnerTableSchema::dba_users_schema,
  ObInnerTableSchema::all_users_schema,
  ObInnerTableSchema::all_synonyms_schema,
  ObInnerTableSchema::user_synonyms_schema,
  ObInnerTableSchema::dba_ind_columns_schema,
  ObInnerTableSchema::all_ind_columns_schema,
  ObInnerTableSchema::user_ind_columns_schema,
  ObInnerTableSchema::dba_constraints_schema,
  ObInnerTableSchema::all_constraints_schema,
  ObInnerTableSchema::user_constraints_schema,
  ObInnerTableSchema::all_tab_cols_v_schema,
  ObInnerTableSchema::dba_tab_cols_v_schema,
  ObInnerTableSchema::user_tab_cols_v_schema,
  ObInnerTableSchema::all_tab_cols_schema,
  ObInnerTableSchema::dba_tab_cols_schema,
  ObInnerTableSchema::user_tab_cols_schema,
  ObInnerTableSchema::all_tab_columns_schema,
  ObInnerTableSchema::dba_tab_columns_schema,
  ObInnerTableSchema::user_tab_columns_schema,
  ObInnerTableSchema::all_tables_schema,
  ObInnerTableSchema::dba_tables_schema,
  ObInnerTableSchema::user_tables_schema,
  ObInnerTableSchema::dba_tab_comments_schema,
  ObInnerTableSchema::all_tab_comments_schema,
  ObInnerTableSchema::user_tab_comments_schema,
  ObInnerTableSchema::dba_col_comments_schema,
  ObInnerTableSchema::all_col_comments_schema,
  ObInnerTableSchema::user_col_comments_schema,
  ObInnerTableSchema::dba_indexes_schema,
  ObInnerTableSchema::all_indexes_schema,
  ObInnerTableSchema::user_indexes_schema,
  ObInnerTableSchema::dba_cons_columns_schema,
  ObInnerTableSchema::all_cons_columns_schema,
  ObInnerTableSchema::user_cons_columns_schema,
  ObInnerTableSchema::user_segments_schema,
  ObInnerTableSchema::dba_segments_schema,
  ObInnerTableSchema::dba_types_schema,
  ObInnerTableSchema::all_types_schema,
  ObInnerTableSchema::user_types_schema,
  ObInnerTableSchema::dba_type_attrs_schema,
  ObInnerTableSchema::all_type_attrs_schema,
  ObInnerTableSchema::user_type_attrs_schema,
  ObInnerTableSchema::dba_coll_types_schema,
  ObInnerTableSchema::all_coll_types_schema,
  ObInnerTableSchema::user_coll_types_schema,
  ObInnerTableSchema::dba_procedures_schema,
  ObInnerTableSchema::dba_arguments_schema,
  ObInnerTableSchema::dba_source_schema,
  ObInnerTableSchema::all_procedures_schema,
  ObInnerTableSchema::all_arguments_schema,
  ObInnerTableSchema::all_source_schema,
  ObInnerTableSchema::user_procedures_schema,
  ObInnerTableSchema::user_arguments_schema,
  ObInnerTableSchema::user_source_schema,
  ObInnerTableSchema::dba_part_key_columns_schema,
  ObInnerTableSchema::all_part_key_columns_schema,
  ObInnerTableSchema::user_part_key_columns_schema,
  ObInnerTableSchema::dba_subpart_key_columns_schema,
  ObInnerTableSchema::all_subpart_key_columns_schema,
  ObInnerTableSchema::user_subpart_key_columns_schema,
  ObInnerTableSchema::dba_views_schema,
  ObInnerTableSchema::all_views_schema,
  ObInnerTableSchema::user_views_schema,
  ObInnerTableSchema::all_tab_partitions_ora_schema,
  ObInnerTableSchema::all_tab_subpartitions_ora_schema,
  ObInnerTableSchema::all_part_tables_ora_schema,
  ObInnerTableSchema::dba_part_tables_ora_schema,
  ObInnerTableSchema::user_part_tables_ora_schema,
  ObInnerTableSchema::dba_tab_partitions_ora_schema,
  ObInnerTableSchema::user_tab_partitions_ora_schema,
  ObInnerTableSchema::dba_tab_subpartitions_ora_schema,
  ObInnerTableSchema::user_tab_subpartitions_ora_schema,
  ObInnerTableSchema::dba_subpartition_templates_ora_schema,
  ObInnerTableSchema::all_subpartition_templates_ora_schema,
  ObInnerTableSchema::user_subpartition_templates_ora_schema,
  ObInnerTableSchema::dba_part_indexes_schema,
  ObInnerTableSchema::all_part_indexes_schema,
  ObInnerTableSchema::user_part_indexes_schema,
  ObInnerTableSchema::all_all_tables_ora_schema,
  ObInnerTableSchema::dba_all_tables_ora_schema,
  ObInnerTableSchema::user_all_tables_ora_schema,
  ObInnerTableSchema::dba_profiles_ora_schema,
  ObInnerTableSchema::user_profiles_ora_schema,
  ObInnerTableSchema::all_profiles_ora_schema,
  ObInnerTableSchema::all_mview_comments_ora_schema,
  ObInnerTableSchema::user_mview_comments_ora_schema,
  ObInnerTableSchema::dba_mview_comments_ora_schema,
  ObInnerTableSchema::all_scheduler_program_args_ora_schema,
  ObInnerTableSchema::dba_scheduler_program_args_ora_schema,
  ObInnerTableSchema::user_scheduler_program_args_ora_schema,
  ObInnerTableSchema::all_scheduler_job_args_ora_schema,
  ObInnerTableSchema::dba_scheduler_job_args_ora_schema,
  ObInnerTableSchema::user_scheduler_job_args_ora_schema,
  ObInnerTableSchema::all_errors_ora_schema,
  ObInnerTableSchema::dba_errors_ora_schema,
  ObInnerTableSchema::user_errors_ora_schema,
  ObInnerTableSchema::all_type_methods_ora_schema,
  ObInnerTableSchema::dba_type_methods_ora_schema,
  ObInnerTableSchema::user_type_methods_ora_schema,
  ObInnerTableSchema::all_method_params_ora_schema,
  ObInnerTableSchema::dba_method_params_ora_schema,
  ObInnerTableSchema::user_method_params_ora_schema,
  ObInnerTableSchema::dba_tablespaces_ora_schema,
  ObInnerTableSchema::user_tablespaces_ora_schema,
  ObInnerTableSchema::dba_ind_expressions_schema,
  ObInnerTableSchema::user_ind_expressions_schema,
  ObInnerTableSchema::all_ind_expressions_schema,
  ObInnerTableSchema::all_ind_partitions_schema,
  ObInnerTableSchema::user_ind_partitions_schema,
  ObInnerTableSchema::dba_ind_partitions_schema,
  ObInnerTableSchema::dba_ind_subpartitions_schema,
  ObInnerTableSchema::all_ind_subpartitions_schema,
  ObInnerTableSchema::user_ind_subpartitions_schema,
  ObInnerTableSchema::dba_roles_schema,
  ObInnerTableSchema::dba_role_privs_schema,
  ObInnerTableSchema::user_role_privs_schema,
  ObInnerTableSchema::dba_tab_privs_schema,
  ObInnerTableSchema::all_tab_privs_schema,
  ObInnerTableSchema::user_tab_privs_schema,
  ObInnerTableSchema::dba_sys_privs_schema,
  ObInnerTableSchema::user_sys_privs_schema,
  ObInnerTableSchema::dba_col_privs_ora_schema,
  ObInnerTableSchema::user_col_privs_ora_schema,
  ObInnerTableSchema::all_col_privs_ora_schema,
  ObInnerTableSchema::role_tab_privs_ora_schema,
  ObInnerTableSchema::role_sys_privs_ora_schema,
  ObInnerTableSchema::role_role_privs_ora_schema,
  ObInnerTableSchema::dictionary_ora_schema,
  ObInnerTableSchema::dict_ora_schema,
  ObInnerTableSchema::all_triggers_schema,
  ObInnerTableSchema::dba_triggers_schema,
  ObInnerTableSchema::user_triggers_schema,
  ObInnerTableSchema::all_dependencies_ora_schema,
  ObInnerTableSchema::dba_dependencies_ora_schema,
  ObInnerTableSchema::user_dependencies_ora_schema,
  ObInnerTableSchema::dba_rsrc_plans_schema,
  ObInnerTableSchema::dba_rsrc_plan_directives_schema,
  ObInnerTableSchema::dba_rsrc_group_mappings_schema,
  ObInnerTableSchema::dba_recyclebin_ora_schema,
  ObInnerTableSchema::user_recyclebin_ora_schema,
  ObInnerTableSchema::dba_rsrc_consumer_groups_schema,
  ObInnerTableSchema::gv_outline_ora_schema,
  ObInnerTableSchema::gv_sql_audit_ora_schema,
  ObInnerTableSchema::v_sql_audit_ora_schema,
  ObInnerTableSchema::gv_instance_schema,
  ObInnerTableSchema::v_instance_schema,
  ObInnerTableSchema::gv_plan_cache_plan_stat_ora_schema,
  ObInnerTableSchema::v_plan_cache_plan_stat_ora_schema,
  ObInnerTableSchema::gv_plan_cache_plan_explain_ora_schema,
  ObInnerTableSchema::v_plan_cache_plan_explain_ora_schema,
  ObInnerTableSchema::gv_session_wait_ora_schema,
  ObInnerTableSchema::v_session_wait_ora_schema,
  ObInnerTableSchema::gv_session_wait_history_ora_schema,
  ObInnerTableSchema::v_session_wait_history_ora_schema,
  ObInnerTableSchema::gv_memory_ora_schema,
  ObInnerTableSchema::v_memory_ora_schema,
  ObInnerTableSchema::gv_memstore_ora_schema,
  ObInnerTableSchema::v_memstore_ora_schema,
  ObInnerTableSchema::gv_memstore_info_ora_schema,
  ObInnerTableSchema::v_memstore_info_ora_schema,
  ObInnerTableSchema::gv_server_memstore_ora_schema,
  ObInnerTableSchema::gv_sesstat_ora_schema,
  ObInnerTableSchema::v_sesstat_ora_schema,
  ObInnerTableSchema::gv_sysstat_ora_schema,
  ObInnerTableSchema::v_sysstat_ora_schema,
  ObInnerTableSchema::gv_system_event_ora_schema,
  ObInnerTableSchema::v_system_event_ora_schema,
  ObInnerTableSchema::gv_tenant_memstore_allocator_info_ora_schema,
  ObInnerTableSchema::v_tenant_memstore_allocator_info_ora_schema,
  ObInnerTableSchema::gv_plan_cache_stat_ora_schema,
  ObInnerTableSchema::v_plan_cache_stat_ora_schema,
  ObInnerTableSchema::gv_concurrent_limit_sql_ora_schema,
  ObInnerTableSchema::nls_session_parameters_ora_schema,
  ObInnerTableSchema::nls_instance_parameters_ora_schema,
  ObInnerTableSchema::nls_database_parameters_ora_schema,
  ObInnerTableSchema::v_nls_parameters_ora_schema,
  ObInnerTableSchema::v_version_ora_schema,
  ObInnerTableSchema::gv_tenant_px_worker_stat_ora_schema,
  ObInnerTableSchema::v_tenant_px_worker_stat_ora_schema,
  ObInnerTableSchema::gv_ps_stat_ora_schema,
  ObInnerTableSchema::v_ps_stat_ora_schema,
  ObInnerTableSchema::gv_ps_item_info_ora_schema,
  ObInnerTableSchema::v_ps_item_info_ora_schema,
  ObInnerTableSchema::gv_sql_workarea_active_ora_schema,
  ObInnerTableSchema::v_sql_workarea_active_ora_schema,
  ObInnerTableSchema::gv_sql_workarea_histogram_ora_schema,
  ObInnerTableSchema::v_sql_workarea_histogram_ora_schema,
  ObInnerTableSchema::gv_ob_sql_workarea_memory_info_ora_schema,
  ObInnerTableSchema::v_ob_sql_workarea_memory_info_ora_schema,
  ObInnerTableSchema::gv_plan_cache_reference_info_ora_schema,
  ObInnerTableSchema::v_plan_cache_reference_info_ora_schema,
  ObInnerTableSchema::gv_sql_workarea_ora_schema,
  ObInnerTableSchema::v_sql_workarea_ora_schema,
  ObInnerTableSchema::gv_sstable_ora_schema,
  ObInnerTableSchema::v_sstable_ora_schema,
  ObInnerTableSchema::gv_server_schema_info_ora_schema,
  ObInnerTableSchema::v_server_schema_info_ora_schema,
  ObInnerTableSchema::gv_sql_plan_monitor_ora_schema,
  ObInnerTableSchema::v_sql_plan_monitor_ora_schema,
  ObInnerTableSchema::v_sql_monitor_statname_ora_schema,
  ObInnerTableSchema::gv_lock_ora_schema,
  ObInnerTableSchema::v_lock_ora_schema,
  ObInnerTableSchema::gv_open_cursor_ora_schema,
  ObInnerTableSchema::v_open_cursor_ora_schema,
  ObInnerTableSchema::v_timezone_names_ora_schema,
  ObInnerTableSchema::gv_global_transaction_ora_schema,
  ObInnerTableSchema::v_global_transaction_ora_schema,
  ObInnerTableSchema::v_restore_point_ora_schema,
  ObInnerTableSchema::v_rsrc_plan_ora_schema,
  ObInnerTableSchema::triggers_schema,
  NULL,};

const schema_create_func information_schema_table_schema_creators[] = {
  NULL,};

const schema_create_func mysql_table_schema_creators[] = {
  NULL,};

const uint64_t tenant_space_tables [] = {
  OB_ALL_TABLE_TID,
  OB_ALL_COLUMN_TID,
  OB_ALL_DDL_OPERATION_TID,
  OB_ALL_TABLE_V2_TID,
  OB_ALL_USER_TID,
  OB_ALL_USER_HISTORY_TID,
  OB_ALL_DATABASE_TID,
  OB_ALL_DATABASE_HISTORY_TID,
  OB_ALL_TABLEGROUP_TID,
  OB_ALL_TABLEGROUP_HISTORY_TID,
  OB_ALL_TABLE_PRIVILEGE_TID,
  OB_ALL_TABLE_PRIVILEGE_HISTORY_TID,
  OB_ALL_DATABASE_PRIVILEGE_TID,
  OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID,
  OB_ALL_TABLE_HISTORY_TID,
  OB_ALL_COLUMN_HISTORY_TID,
  OB_TENANT_PARAMETER_TID,
  OB_ALL_SYS_VARIABLE_TID,
  OB_ALL_SYS_STAT_TID,
  OB_ALL_COLUMN_STATISTIC_TID,
  OB_ALL_DUMMY_TID,
  OB_ALL_CLOG_HISTORY_INFO_V2_TID,
  OB_ALL_OUTLINE_TID,
  OB_ALL_OUTLINE_HISTORY_TID,
  OB_ALL_RECYCLEBIN_TID,
  OB_ALL_PART_TID,
  OB_ALL_PART_HISTORY_TID,
  OB_ALL_SUB_PART_TID,
  OB_ALL_SUB_PART_HISTORY_TID,
  OB_ALL_PART_INFO_TID,
  OB_ALL_PART_INFO_HISTORY_TID,
  OB_ALL_DEF_SUB_PART_TID,
  OB_ALL_DEF_SUB_PART_HISTORY_TID,
  OB_ALL_SYS_VARIABLE_HISTORY_TID,
  OB_ALL_DDL_ID_TID,
  OB_ALL_FOREIGN_KEY_TID,
  OB_ALL_FOREIGN_KEY_HISTORY_TID,
  OB_ALL_FOREIGN_KEY_COLUMN_TID,
  OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID,
  OB_ALL_SYNONYM_TID,
  OB_ALL_SYNONYM_HISTORY_TID,
  OB_ALL_SEQUENCE_V2_TID,
  OB_ALL_TENANT_META_TABLE_TID,
  OB_ALL_ROUTINE_TID,
  OB_ALL_ROUTINE_HISTORY_TID,
  OB_ALL_ROUTINE_PARAM_TID,
  OB_ALL_ROUTINE_PARAM_HISTORY_TID,
  OB_ALL_TABLE_STAT_TID,
  OB_ALL_COLUMN_STAT_TID,
  OB_ALL_HISTOGRAM_STAT_TID,
  OB_ALL_PACKAGE_TID,
  OB_ALL_PACKAGE_HISTORY_TID,
  OB_ALL_TENANT_GC_PARTITION_INFO_TID,
  OB_ALL_CONSTRAINT_TID,
  OB_ALL_CONSTRAINT_HISTORY_TID,
  OB_ALL_ORI_SCHEMA_VERSION_TID,
  OB_ALL_FUNC_TID,
  OB_ALL_FUNC_HISTORY_TID,
  OB_ALL_TEMP_TABLE_TID,
  OB_ALL_SEQUENCE_OBJECT_TID,
  OB_ALL_SEQUENCE_OBJECT_HISTORY_TID,
  OB_ALL_SEQUENCE_VALUE_TID,
  OB_ALL_TENANT_PLAN_BASELINE_TID,
  OB_ALL_TENANT_PLAN_BASELINE_HISTORY_TID,
  OB_ALL_TYPE_TID,
  OB_ALL_TYPE_HISTORY_TID,
  OB_ALL_TYPE_ATTR_TID,
  OB_ALL_TYPE_ATTR_HISTORY_TID,
  OB_ALL_COLL_TYPE_TID,
  OB_ALL_COLL_TYPE_HISTORY_TID,
  OB_ALL_WEAK_READ_SERVICE_TID,
  OB_ALL_DBLINK_TID,
  OB_ALL_DBLINK_HISTORY_TID,
  OB_ALL_TENANT_PARTITION_META_TABLE_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID,
  OB_ALL_TENANT_KEYSTORE_TID,
  OB_ALL_TENANT_KEYSTORE_HISTORY_TID,
  OB_ALL_TENANT_TABLESPACE_TID,
  OB_ALL_TENANT_TABLESPACE_HISTORY_TID,
  OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID,
  OB_ALL_TENANT_PROFILE_TID,
  OB_ALL_TENANT_PROFILE_HISTORY_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TID,
  OB_ALL_TENANT_TRIGGER_TID,
  OB_ALL_TENANT_TRIGGER_HISTORY_TID,
  OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TID,
  OB_ALL_TENANT_SYSAUTH_TID,
  OB_ALL_TENANT_SYSAUTH_HISTORY_TID,
  OB_ALL_TENANT_OBJAUTH_TID,
  OB_ALL_TENANT_OBJAUTH_HISTORY_TID,
  OB_ALL_TENANT_BACKUP_INFO_TID,
  OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TID,
  OB_ALL_TENANT_BACKUP_TASK_TID,
  OB_ALL_TENANT_PG_BACKUP_TASK_TID,
  OB_ALL_TENANT_ERROR_TID,
  OB_ALL_TENANT_BACKUP_CLEAN_INFO_TID,
  OB_ALL_TENANT_RESTORE_PG_INFO_TID,
  OB_ALL_TABLE_V2_HISTORY_TID,
  OB_ALL_TENANT_OBJECT_TYPE_TID,
  OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TID,
  OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TID,
  OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TID,
  OB_ALL_TENANT_TIME_ZONE_TID,
  OB_ALL_TENANT_TIME_ZONE_NAME_TID,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_TID,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TID,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_TID,
  OB_ALL_TENANT_DEPENDENCY_TID,
  OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TID,
  OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TID,
  OB_ALL_TENANT_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TID,
  OB_ALL_RES_MGR_PLAN_TID,
  OB_ALL_RES_MGR_DIRECTIVE_TID,
  OB_ALL_RES_MGR_MAPPING_RULE_TID,
  OB_ALL_RES_MGR_CONSUMER_GROUP_TID,
  OB_ALL_TABLE_V2_HISTORY_IDX_DATA_TABLE_ID_TID,
  OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID,
  OB_TENANT_VIRTUAL_ALL_TABLE_TID,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_TID,
  OB_TENANT_VIRTUAL_TABLE_INDEX_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID,
  OB_TENANT_VIRTUAL_WARNING_TID,
  OB_TENANT_VIRTUAL_CURRENT_TENANT_TID,
  OB_TENANT_VIRTUAL_DATABASE_STATUS_TID,
  OB_TENANT_VIRTUAL_TENANT_STATUS_TID,
  OB_TENANT_VIRTUAL_INTERM_RESULT_TID,
  OB_TENANT_VIRTUAL_PARTITION_STAT_TID,
  OB_TENANT_VIRTUAL_STATNAME_TID,
  OB_TENANT_VIRTUAL_EVENT_NAME_TID,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID,
  OB_TENANT_VIRTUAL_SHOW_TABLES_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_TID,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID,
  OB_ALL_VIRTUAL_TRACE_LOG_TID,
  OB_ALL_VIRTUAL_ENGINE_TID,
  OB_TENANT_VIRTUAL_OUTLINE_TID,
  OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID,
  OB_SESSION_VARIABLES_TID,
  OB_TABLE_PRIVILEGES_TID,
  OB_USER_PRIVILEGES_TID,
  OB_SCHEMA_PRIVILEGES_TID,
  OB_TABLE_CONSTRAINTS_TID,
  OB_GLOBAL_STATUS_TID,
  OB_PARTITIONS_TID,
  OB_SESSION_STATUS_TID,
  OB_USER_TID,
  OB_DB_TID,
  OB_PROC_TID,
  OB_TENANT_VIRTUAL_COLLATION_TID,
  OB_TENANT_VIRTUAL_CHARSET_TID,
  OB_PARAMETERS_TID,
  OB_ALL_VIRTUAL_WEAK_READ_STAT_TID,
  OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TID,
  OB_REFERENTIAL_CONSTRAINTS_TID,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TID,
  OB_ALL_VIRTUAL_OPEN_CURSOR_TID,
  OB_ALL_VIRTUAL_FILES_TID,
  OB_FILES_TID,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID,
  OB_ALL_VIRTUAL_TABLE_AGENT_TID,
  OB_ALL_VIRTUAL_COLUMN_AGENT_TID,
  OB_ALL_VIRTUAL_DATABASE_AGENT_TID,
  OB_ALL_VIRTUAL_SEQUENCE_V2_AGENT_TID,
  OB_ALL_VIRTUAL_PART_AGENT_TID,
  OB_ALL_VIRTUAL_SUB_PART_AGENT_TID,
  OB_ALL_VIRTUAL_PACKAGE_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_META_TABLE_AGENT_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_ALL_VIRTUAL_SQL_AUDIT_I1_TID,
  OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_AGENT_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID,
  OB_ALL_VIRTUAL_SEQUENCE_VALUE_AGENT_TID,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_AGENT_TID,
  OB_ALL_VIRTUAL_USER_AGENT_TID,
  OB_ALL_VIRTUAL_SYNONYM_AGENT_TID,
  OB_ALL_VIRTUAL_FOREIGN_KEY_AGENT_TID,
  OB_ALL_VIRTUAL_COLUMN_STAT_AGENT_TID,
  OB_ALL_VIRTUAL_COLUMN_STATISTIC_AGENT_TID,
  OB_ALL_VIRTUAL_PARTITION_TABLE_AGENT_TID,
  OB_ALL_VIRTUAL_TABLE_STAT_AGENT_TID,
  OB_ALL_VIRTUAL_RECYCLEBIN_AGENT_TID,
  OB_TENANT_VIRTUAL_OUTLINE_AGENT_TID,
  OB_ALL_VIRTUAL_ROUTINE_AGENT_TID,
  OB_ALL_VIRTUAL_TABLEGROUP_AGENT_TID,
  OB_ALL_VIRTUAL_PRIVILEGE_AGENT_TID,
  OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TID,
  OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TID,
  OB_TENANT_VIRTUAL_CHARSET_AGENT_TID,
  OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID,
  OB_TENANT_VIRTUAL_COLLATION_AGENT_TID,
  OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_AGENT_TID,
  OB_ALL_VIRTUAL_SERVER_AGENT_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID,
  OB_ALL_VIRTUAL_PROCESSLIST_ORA_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_ORA_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_ORA_ALL_VIRTUAL_SESSION_WAIT_I1_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TID,
  OB_ALL_VIRTUAL_MEMORY_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_MEMSTORE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_AGENT_TID,
  OB_ALL_VIRTUAL_SESSTAT_ORA_TID,
  OB_ALL_VIRTUAL_SESSTAT_ORA_ALL_VIRTUAL_SESSTAT_I1_TID,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_TID,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_ALL_VIRTUAL_SYSSTAT_I1_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_AGENT_TID,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORA_TID,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORA_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORA_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_ORA_TID,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_ORA_TID,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TID,
  OB_ALL_VIRTUAL_TRACE_LOG_ORA_TID,
  OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TID,
  OB_ALL_VIRTUAL_CONSTRAINT_AGENT_TID,
  OB_ALL_VIRTUAL_TYPE_AGENT_TID,
  OB_ALL_VIRTUAL_TYPE_ATTR_AGENT_TID,
  OB_ALL_VIRTUAL_COLL_TYPE_AGENT_TID,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_AGENT_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_SSTABLE_CHECKSUM_AGENT_TID,
  OB_ALL_VIRTUAL_PARTITION_INFO_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_KEYSTORE_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TABLESPACE_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_PROFILE_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_AGENT_TID,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_AGENT_TID,
  OB_ALL_VIRTUAL_SECURITY_AUDIT_AGENT_TID,
  OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_AGENT_TID,
  OB_ALL_VIRTUAL_TRIGGER_AGENT_TID,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PS_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_AGENT_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_AGENT_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_AGENT_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_AGENT_TID,
  OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_AGENT_TID,
  OB_ALL_VIRTUAL_SYSAUTH_AGENT_TID,
  OB_ALL_VIRTUAL_SYSAUTH_HISTORY_AGENT_TID,
  OB_ALL_VIRTUAL_OBJAUTH_AGENT_TID,
  OB_ALL_VIRTUAL_OBJAUTH_HISTORY_AGENT_TID,
  OB_ALL_VIRTUAL_ERROR_AGENT_TID,
  OB_ALL_VIRTUAL_TABLE_MGR_AGENT_TID,
  OB_ALL_VIRTUAL_DEF_SUB_PART_AGENT_TID,
  OB_ALL_VIRTUAL_OBJECT_TYPE_AGENT_TID,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_AGENT_TID,
  OB_ALL_VIRTUAL_DBLINK_AGENT_TID,
  OB_ALL_VIRTUAL_DBLINK_HISTORY_AGENT_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_ORA_TID,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORA_TID,
  OB_ALL_VIRTUAL_OPEN_CURSOR_ORA_TID,
  OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_AGENT_TID,
  OB_ALL_VIRTUAL_DEPENDENCY_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_AGENT_TID,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_ORA_TID,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_TYPE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_GLOBAL_TRANSACTION_AGENT_TID,
  OB_ALL_VIRTUAL_ACQUIRED_SNAPSHOT_AGENT_TID,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SEQUENCE_V2_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_COLUMN_STATISTIC_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_STAT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SYSAUTH_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DBLINK_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TID,
  OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORA_TID,
  OB_GV_PLAN_CACHE_STAT_TID,
  OB_GV_PLAN_CACHE_PLAN_STAT_TID,
  OB_SCHEMATA_TID,
  OB_CHARACTER_SETS_TID,
  OB_GLOBAL_VARIABLES_TID,
  OB_STATISTICS_TID,
  OB_VIEWS_TID,
  OB_TABLES_TID,
  OB_COLLATIONS_TID,
  OB_COLLATION_CHARACTER_SET_APPLICABILITY_TID,
  OB_PROCESSLIST_TID,
  OB_KEY_COLUMN_USAGE_TID,
  OB_DBA_OUTLINES_TID,
  OB_ENGINES_TID,
  OB_ROUTINES_TID,
  OB_GV_SESSION_EVENT_TID,
  OB_GV_SESSION_WAIT_TID,
  OB_GV_SESSION_WAIT_HISTORY_TID,
  OB_GV_SYSTEM_EVENT_TID,
  OB_GV_SESSTAT_TID,
  OB_GV_SYSSTAT_TID,
  OB_V_STATNAME_TID,
  OB_V_EVENT_NAME_TID,
  OB_V_SESSION_EVENT_TID,
  OB_V_SESSION_WAIT_TID,
  OB_V_SESSION_WAIT_HISTORY_TID,
  OB_V_SESSTAT_TID,
  OB_V_SYSSTAT_TID,
  OB_V_SYSTEM_EVENT_TID,
  OB_GV_SQL_AUDIT_TID,
  OB_GV_LATCH_TID,
  OB_GV_MEMORY_TID,
  OB_V_MEMORY_TID,
  OB_GV_MEMSTORE_TID,
  OB_V_MEMSTORE_TID,
  OB_GV_MEMSTORE_INFO_TID,
  OB_V_MEMSTORE_INFO_TID,
  OB_V_PLAN_CACHE_STAT_TID,
  OB_V_PLAN_CACHE_PLAN_STAT_TID,
  OB_GV_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_V_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_V_SQL_AUDIT_TID,
  OB_V_LATCH_TID,
  OB_GV_OBRPC_OUTGOING_TID,
  OB_V_OBRPC_OUTGOING_TID,
  OB_GV_OBRPC_INCOMING_TID,
  OB_V_OBRPC_INCOMING_TID,
  OB_GV_SQL_TID,
  OB_V_SQL_TID,
  OB_GV_SQL_MONITOR_TID,
  OB_V_SQL_MONITOR_TID,
  OB_GV_SQL_PLAN_MONITOR_TID,
  OB_V_SQL_PLAN_MONITOR_TID,
  OB_USER_RECYCLEBIN_TID,
  OB_GV_OUTLINE_TID,
  OB_GV_CONCURRENT_LIMIT_SQL_TID,
  OB_GV_SQL_PLAN_STATISTICS_TID,
  OB_V_SQL_PLAN_STATISTICS_TID,
  OB_GV_SERVER_MEMSTORE_TID,
  OB_GV_UNIT_LOAD_BALANCE_EVENT_HISTORY_TID,
  OB_GV_TENANT_TID,
  OB_GV_DATABASE_TID,
  OB_GV_TABLE_TID,
  OB_GV_UNIT_TID,
  OB_V_UNIT_TID,
  OB_GV_PARTITION_TID,
  OB_V_PARTITION_TID,
  OB_GV_LOCK_WAIT_STAT_TID,
  OB_V_LOCK_WAIT_STAT_TID,
  OB_TIME_ZONE_TID,
  OB_TIME_ZONE_NAME_TID,
  OB_TIME_ZONE_TRANSITION_TID,
  OB_TIME_ZONE_TRANSITION_TYPE_TID,
  OB_GV_SESSION_LONGOPS_TID,
  OB_V_SESSION_LONGOPS_TID,
  OB_GV_TENANT_MEMSTORE_ALLOCATOR_INFO_TID,
  OB_V_TENANT_MEMSTORE_ALLOCATOR_INFO_TID,
  OB_GV_TENANT_SEQUENCE_OBJECT_TID,
  OB_COLUMNS_TID,
  OB_GV_MINOR_MERGE_INFO_TID,
  OB_GV_TENANT_PX_WORKER_STAT_TID,
  OB_V_TENANT_PX_WORKER_STAT_TID,
  OB_GV_PARTITION_AUDIT_TID,
  OB_V_PARTITION_AUDIT_TID,
  OB_GV_PS_STAT_TID,
  OB_V_PS_STAT_TID,
  OB_GV_PS_ITEM_INFO_TID,
  OB_V_PS_ITEM_INFO_TID,
  OB_GV_SQL_WORKAREA_TID,
  OB_V_SQL_WORKAREA_TID,
  OB_GV_SQL_WORKAREA_ACTIVE_TID,
  OB_V_SQL_WORKAREA_ACTIVE_TID,
  OB_GV_SQL_WORKAREA_HISTOGRAM_TID,
  OB_V_SQL_WORKAREA_HISTOGRAM_TID,
  OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_TID,
  OB_V_OB_SQL_WORKAREA_MEMORY_INFO_TID,
  OB_GV_PLAN_CACHE_REFERENCE_INFO_TID,
  OB_V_PLAN_CACHE_REFERENCE_INFO_TID,
  OB_V_OB_TIMESTAMP_SERVICE_TID,
  OB_GV_SSTABLE_TID,
  OB_V_SSTABLE_TID,
  OB_GV_SERVER_SCHEMA_INFO_TID,
  OB_V_SERVER_SCHEMA_INFO_TID,
  OB_CDB_CKPT_HISTORY_TID,
  OB_GV_OB_TRANS_TABLE_STATUS_TID,
  OB_V_OB_TRANS_TABLE_STATUS_TID,
  OB_V_SQL_MONITOR_STATNAME_TID,
  OB_GV_MERGE_INFO_TID,
  OB_V_MERGE_INFO_TID,
  OB_GV_LOCK_TID,
  OB_V_LOCK_TID,
  OB_V_RESTORE_POINT_TID,
  OB_COLUMN_PRIVILEGES_TID,
  OB_SHOW_RABBIT_TID,
  OB_DBA_SYNONYMS_TID,
  OB_DBA_OBJECTS_TID,
  OB_ALL_OBJECTS_TID,
  OB_USER_OBJECTS_TID,
  OB_DBA_SEQUENCES_TID,
  OB_ALL_SEQUENCES_TID,
  OB_USER_SEQUENCES_TID,
  OB_DBA_USERS_TID,
  OB_ALL_USERS_TID,
  OB_ALL_SYNONYMS_TID,
  OB_USER_SYNONYMS_TID,
  OB_DBA_IND_COLUMNS_TID,
  OB_ALL_IND_COLUMNS_TID,
  OB_USER_IND_COLUMNS_TID,
  OB_DBA_CONSTRAINTS_TID,
  OB_ALL_CONSTRAINTS_TID,
  OB_USER_CONSTRAINTS_TID,
  OB_ALL_TAB_COLS_V_TID,
  OB_DBA_TAB_COLS_V_TID,
  OB_USER_TAB_COLS_V_TID,
  OB_ALL_TAB_COLS_TID,
  OB_DBA_TAB_COLS_TID,
  OB_USER_TAB_COLS_TID,
  OB_ALL_TAB_COLUMNS_TID,
  OB_DBA_TAB_COLUMNS_TID,
  OB_USER_TAB_COLUMNS_TID,
  OB_ALL_TABLES_TID,
  OB_DBA_TABLES_TID,
  OB_USER_TABLES_TID,
  OB_DBA_TAB_COMMENTS_TID,
  OB_ALL_TAB_COMMENTS_TID,
  OB_USER_TAB_COMMENTS_TID,
  OB_DBA_COL_COMMENTS_TID,
  OB_ALL_COL_COMMENTS_TID,
  OB_USER_COL_COMMENTS_TID,
  OB_DBA_INDEXES_TID,
  OB_ALL_INDEXES_TID,
  OB_USER_INDEXES_TID,
  OB_DBA_CONS_COLUMNS_TID,
  OB_ALL_CONS_COLUMNS_TID,
  OB_USER_CONS_COLUMNS_TID,
  OB_USER_SEGMENTS_TID,
  OB_DBA_SEGMENTS_TID,
  OB_DBA_TYPES_TID,
  OB_ALL_TYPES_TID,
  OB_USER_TYPES_TID,
  OB_DBA_TYPE_ATTRS_TID,
  OB_ALL_TYPE_ATTRS_TID,
  OB_USER_TYPE_ATTRS_TID,
  OB_DBA_COLL_TYPES_TID,
  OB_ALL_COLL_TYPES_TID,
  OB_USER_COLL_TYPES_TID,
  OB_DBA_PROCEDURES_TID,
  OB_DBA_ARGUMENTS_TID,
  OB_DBA_SOURCE_TID,
  OB_ALL_PROCEDURES_TID,
  OB_ALL_ARGUMENTS_TID,
  OB_ALL_SOURCE_TID,
  OB_USER_PROCEDURES_TID,
  OB_USER_ARGUMENTS_TID,
  OB_USER_SOURCE_TID,
  OB_DBA_PART_KEY_COLUMNS_TID,
  OB_ALL_PART_KEY_COLUMNS_TID,
  OB_USER_PART_KEY_COLUMNS_TID,
  OB_DBA_SUBPART_KEY_COLUMNS_TID,
  OB_ALL_SUBPART_KEY_COLUMNS_TID,
  OB_USER_SUBPART_KEY_COLUMNS_TID,
  OB_DBA_VIEWS_TID,
  OB_ALL_VIEWS_TID,
  OB_USER_VIEWS_TID,
  OB_ALL_TAB_PARTITIONS_ORA_TID,
  OB_ALL_TAB_SUBPARTITIONS_ORA_TID,
  OB_ALL_PART_TABLES_ORA_TID,
  OB_DBA_PART_TABLES_ORA_TID,
  OB_USER_PART_TABLES_ORA_TID,
  OB_DBA_TAB_PARTITIONS_ORA_TID,
  OB_USER_TAB_PARTITIONS_ORA_TID,
  OB_DBA_TAB_SUBPARTITIONS_ORA_TID,
  OB_USER_TAB_SUBPARTITIONS_ORA_TID,
  OB_DBA_SUBPARTITION_TEMPLATES_ORA_TID,
  OB_ALL_SUBPARTITION_TEMPLATES_ORA_TID,
  OB_USER_SUBPARTITION_TEMPLATES_ORA_TID,
  OB_DBA_PART_INDEXES_TID,
  OB_ALL_PART_INDEXES_TID,
  OB_USER_PART_INDEXES_TID,
  OB_ALL_ALL_TABLES_ORA_TID,
  OB_DBA_ALL_TABLES_ORA_TID,
  OB_USER_ALL_TABLES_ORA_TID,
  OB_DBA_PROFILES_ORA_TID,
  OB_USER_PROFILES_ORA_TID,
  OB_ALL_PROFILES_ORA_TID,
  OB_ALL_MVIEW_COMMENTS_ORA_TID,
  OB_USER_MVIEW_COMMENTS_ORA_TID,
  OB_DBA_MVIEW_COMMENTS_ORA_TID,
  OB_ALL_SCHEDULER_PROGRAM_ARGS_ORA_TID,
  OB_DBA_SCHEDULER_PROGRAM_ARGS_ORA_TID,
  OB_USER_SCHEDULER_PROGRAM_ARGS_ORA_TID,
  OB_ALL_SCHEDULER_JOB_ARGS_ORA_TID,
  OB_DBA_SCHEDULER_JOB_ARGS_ORA_TID,
  OB_USER_SCHEDULER_JOB_ARGS_ORA_TID,
  OB_ALL_ERRORS_ORA_TID,
  OB_DBA_ERRORS_ORA_TID,
  OB_USER_ERRORS_ORA_TID,
  OB_ALL_TYPE_METHODS_ORA_TID,
  OB_DBA_TYPE_METHODS_ORA_TID,
  OB_USER_TYPE_METHODS_ORA_TID,
  OB_ALL_METHOD_PARAMS_ORA_TID,
  OB_DBA_METHOD_PARAMS_ORA_TID,
  OB_USER_METHOD_PARAMS_ORA_TID,
  OB_DBA_TABLESPACES_ORA_TID,
  OB_USER_TABLESPACES_ORA_TID,
  OB_DBA_IND_EXPRESSIONS_TID,
  OB_USER_IND_EXPRESSIONS_TID,
  OB_ALL_IND_EXPRESSIONS_TID,
  OB_ALL_IND_PARTITIONS_TID,
  OB_USER_IND_PARTITIONS_TID,
  OB_DBA_IND_PARTITIONS_TID,
  OB_DBA_IND_SUBPARTITIONS_TID,
  OB_ALL_IND_SUBPARTITIONS_TID,
  OB_USER_IND_SUBPARTITIONS_TID,
  OB_DBA_ROLES_TID,
  OB_DBA_ROLE_PRIVS_TID,
  OB_USER_ROLE_PRIVS_TID,
  OB_DBA_TAB_PRIVS_TID,
  OB_ALL_TAB_PRIVS_TID,
  OB_USER_TAB_PRIVS_TID,
  OB_DBA_SYS_PRIVS_TID,
  OB_USER_SYS_PRIVS_TID,
  OB_DBA_COL_PRIVS_ORA_TID,
  OB_USER_COL_PRIVS_ORA_TID,
  OB_ALL_COL_PRIVS_ORA_TID,
  OB_ROLE_TAB_PRIVS_ORA_TID,
  OB_ROLE_SYS_PRIVS_ORA_TID,
  OB_ROLE_ROLE_PRIVS_ORA_TID,
  OB_DICTIONARY_ORA_TID,
  OB_DICT_ORA_TID,
  OB_ALL_TRIGGERS_TID,
  OB_DBA_TRIGGERS_TID,
  OB_USER_TRIGGERS_TID,
  OB_ALL_DEPENDENCIES_ORA_TID,
  OB_DBA_DEPENDENCIES_ORA_TID,
  OB_USER_DEPENDENCIES_ORA_TID,
  OB_DBA_RSRC_PLANS_TID,
  OB_DBA_RSRC_PLAN_DIRECTIVES_TID,
  OB_DBA_RSRC_GROUP_MAPPINGS_TID,
  OB_DBA_RECYCLEBIN_ORA_TID,
  OB_USER_RECYCLEBIN_ORA_TID,
  OB_DBA_RSRC_CONSUMER_GROUPS_TID,
  OB_GV_OUTLINE_ORA_TID,
  OB_GV_SQL_AUDIT_ORA_TID,
  OB_V_SQL_AUDIT_ORA_TID,
  OB_GV_INSTANCE_TID,
  OB_V_INSTANCE_TID,
  OB_GV_PLAN_CACHE_PLAN_STAT_ORA_TID,
  OB_V_PLAN_CACHE_PLAN_STAT_ORA_TID,
  OB_GV_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID,
  OB_V_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID,
  OB_GV_SESSION_WAIT_ORA_TID,
  OB_V_SESSION_WAIT_ORA_TID,
  OB_GV_SESSION_WAIT_HISTORY_ORA_TID,
  OB_V_SESSION_WAIT_HISTORY_ORA_TID,
  OB_GV_MEMORY_ORA_TID,
  OB_V_MEMORY_ORA_TID,
  OB_GV_MEMSTORE_ORA_TID,
  OB_V_MEMSTORE_ORA_TID,
  OB_GV_MEMSTORE_INFO_ORA_TID,
  OB_V_MEMSTORE_INFO_ORA_TID,
  OB_GV_SERVER_MEMSTORE_ORA_TID,
  OB_GV_SESSTAT_ORA_TID,
  OB_V_SESSTAT_ORA_TID,
  OB_GV_SYSSTAT_ORA_TID,
  OB_V_SYSSTAT_ORA_TID,
  OB_GV_SYSTEM_EVENT_ORA_TID,
  OB_V_SYSTEM_EVENT_ORA_TID,
  OB_GV_TENANT_MEMSTORE_ALLOCATOR_INFO_ORA_TID,
  OB_V_TENANT_MEMSTORE_ALLOCATOR_INFO_ORA_TID,
  OB_GV_PLAN_CACHE_STAT_ORA_TID,
  OB_V_PLAN_CACHE_STAT_ORA_TID,
  OB_GV_CONCURRENT_LIMIT_SQL_ORA_TID,
  OB_NLS_SESSION_PARAMETERS_ORA_TID,
  OB_NLS_INSTANCE_PARAMETERS_ORA_TID,
  OB_NLS_DATABASE_PARAMETERS_ORA_TID,
  OB_V_NLS_PARAMETERS_ORA_TID,
  OB_V_VERSION_ORA_TID,
  OB_GV_TENANT_PX_WORKER_STAT_ORA_TID,
  OB_V_TENANT_PX_WORKER_STAT_ORA_TID,
  OB_GV_PS_STAT_ORA_TID,
  OB_V_PS_STAT_ORA_TID,
  OB_GV_PS_ITEM_INFO_ORA_TID,
  OB_V_PS_ITEM_INFO_ORA_TID,
  OB_GV_SQL_WORKAREA_ACTIVE_ORA_TID,
  OB_V_SQL_WORKAREA_ACTIVE_ORA_TID,
  OB_GV_SQL_WORKAREA_HISTOGRAM_ORA_TID,
  OB_V_SQL_WORKAREA_HISTOGRAM_ORA_TID,
  OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TID,
  OB_V_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TID,
  OB_GV_PLAN_CACHE_REFERENCE_INFO_ORA_TID,
  OB_V_PLAN_CACHE_REFERENCE_INFO_ORA_TID,
  OB_GV_SQL_WORKAREA_ORA_TID,
  OB_V_SQL_WORKAREA_ORA_TID,
  OB_GV_SSTABLE_ORA_TID,
  OB_V_SSTABLE_ORA_TID,
  OB_GV_SERVER_SCHEMA_INFO_ORA_TID,
  OB_V_SERVER_SCHEMA_INFO_ORA_TID,
  OB_GV_SQL_PLAN_MONITOR_ORA_TID,
  OB_V_SQL_PLAN_MONITOR_ORA_TID,
  OB_V_SQL_MONITOR_STATNAME_ORA_TID,
  OB_GV_LOCK_ORA_TID,
  OB_V_LOCK_ORA_TID,
  OB_GV_OPEN_CURSOR_ORA_TID,
  OB_V_OPEN_CURSOR_ORA_TID,
  OB_V_TIMEZONE_NAMES_ORA_TID,
  OB_GV_GLOBAL_TRANSACTION_ORA_TID,
  OB_V_GLOBAL_TRANSACTION_ORA_TID,
  OB_V_RESTORE_POINT_ORA_TID,
  OB_V_RSRC_PLAN_ORA_TID,  };

const uint64_t all_ora_mapping_virtual_table_org_tables [] = {
  OB_ALL_VIRTUAL_SQL_AUDIT_TID,
  OB_ALL_VIRTUAL_PLAN_STAT_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TID,
  OB_ALL_VIRTUAL_PROCESSLIST_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TID,
  OB_ALL_VIRTUAL_MEMORY_INFO_TID,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TID,
  OB_ALL_VIRTUAL_MEMSTORE_INFO_TID,
  OB_ALL_VIRTUAL_SESSTAT_TID,
  OB_ALL_VIRTUAL_SYSSTAT_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_TID,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_TID,
  OB_ALL_VIRTUAL_TRACE_LOG_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_TID,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_TID,
  OB_ALL_VIRTUAL_PS_STAT_TID,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TID,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TID,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TID,
  OB_ALL_VIRTUAL_OPEN_CURSOR_TID,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID,  };

const uint64_t all_ora_mapping_virtual_tables [] = {  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID
,  OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID
,  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID
,  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TID
,  OB_ALL_VIRTUAL_PROCESSLIST_ORA_TID
,  OB_ALL_VIRTUAL_SESSION_WAIT_ORA_TID
,  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_MEMORY_INFO_ORA_TID
,  OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_ORA_TID
,  OB_ALL_VIRTUAL_MEMSTORE_INFO_ORA_TID
,  OB_ALL_VIRTUAL_SESSTAT_ORA_TID
,  OB_ALL_VIRTUAL_SYSSTAT_ORA_TID
,  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TID
,  OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORA_TID
,  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORA_TID
,  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TID
,  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORA_TID
,  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_ORA_TID
,  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_ORA_TID
,  OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TID
,  OB_ALL_VIRTUAL_TRACE_LOG_ORA_TID
,  OB_ALL_VIRTUAL_DATA_TYPE_ORA_TID
,  OB_ALL_VIRTUAL_PX_WORKER_STAT_ORA_TID
,  OB_ALL_VIRTUAL_PS_STAT_ORA_TID
,  OB_ALL_VIRTUAL_PS_ITEM_INFO_ORA_TID
,  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TID
,  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TID
,  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_ORA_TID
,  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORA_TID
,  OB_ALL_VIRTUAL_OPEN_CURSOR_ORA_TID
,  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_ORA_TID
,  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TID
,  };

/* start/end_pos is start/end postition for column with tenant id */
struct VTMapping
{
   uint64_t mapping_tid_;
   bool is_real_vt_;
   int64_t start_pos_;
   int64_t end_pos_;
   bool use_real_tenant_id_;
};

// define all columns with tenant id
const char* const with_tenant_id_columns[] = {

  "COLL_TYPE_ID",
  "ELEM_TYPE_ID",
  "PACKAGE_ID",
  "TABLE_ID",
  "TABLE_ID",
  "TABLE_ID",
  "TABLE_ID",
  "DATABASE_ID",
  "DEFAULT_TABLEGROUP_ID",
  "DBLINK_ID",
  "OWNER_ID",
  "DBLINK_ID",
  "OWNER_ID",
  "TABLE_ID",
  "TABLESPACE_ID",
  "FOREIGN_KEY_ID",
  "FOREIGN_KEY_ID",
  "CHILD_TABLE_ID",
  "PARENT_TABLE_ID",
  "PACKAGE_ID",
  "DATABASE_ID",
  "OWNER_ID",
  "TABLE_ID",
  "TABLESPACE_ID",
  "DATABASE_ID",
  "TABLE_ID",
  "TABLEGROUP_ID",
  "ROUTINE_ID",
  "TYPE_OWNER",
  "ROUTINE_ID",
  "PACKAGE_ID",
  "DATABASE_ID",
  "OWNER_ID",
  "SEQUENCE_ID",
  "DATABASE_ID",
  "SEQUENCE_KEY",
  "SEQUENCE_ID",
  "TABLE_ID",
  "TABLESPACE_ID",
  "SYNONYM_ID",
  "DATABASE_ID",
  "OBJECT_DATABASE_ID",
  "TABLEGROUP_ID",
  "USER_ID",
  "TABLE_ID",
  "DATABASE_ID",
  "DATA_TABLE_ID",
  "TABLEGROUP_ID",
  "TABLESPACE_ID",
  "TABLE_ID",
  "TABLE_ID",
  "DEP_OBJ_ID",
  "REF_OBJ_ID",
  "DEP_OBJ_OWNER_ID",
  "OBJ_ID",
  "KEYSTORE_ID",
  "MASTER_KEY_ID",
  "OBJ_ID",
  "GRANTOR_ID",
  "GRANTEE_ID",
  "OBJ_ID",
  "GRANTOR_ID",
  "GRANTEE_ID",
  "DATABASE_ID",
  "OWNER_ID",
  "OBJECT_TYPE_ID",
  "PROFILE_ID",
  "GRANTEE_ID",
  "ROLE_ID",
  "AUDIT_ID",
  "OWNER_ID",
  "AUDIT_ID",
  "OWNER_ID",
  "USER_ID",
  "EFFECTIVE_USER_ID",
  "DB_ID",
  "CUR_DB_ID",
  "AUDIT_ID",
  "GRANTEE_ID",
  "GRANTEE_ID",
  "TABLESPACE_ID",
  "MASTER_KEY_ID",
  "TRIGGER_ID",
  "DATABASE_ID",
  "OWNER_ID",
  "BASE_OBJECT_ID",
  "TYPE_ID",
  "TYPE_ATTR_ID",
  "TYPE_ID",
  "DATABASE_ID",
  "SUPERTYPEID",
  "PACKAGE_ID",
  "USER_ID",
  "PROFILE_ID",
};

extern VTMapping vt_mappings[5000];

const char* const tenant_space_table_names [] = {
  OB_ALL_TABLE_TNAME,
  OB_ALL_COLUMN_TNAME,
  OB_ALL_DDL_OPERATION_TNAME,
  OB_ALL_TABLE_V2_TNAME,
  OB_ALL_USER_TNAME,
  OB_ALL_USER_HISTORY_TNAME,
  OB_ALL_DATABASE_TNAME,
  OB_ALL_DATABASE_HISTORY_TNAME,
  OB_ALL_TABLEGROUP_TNAME,
  OB_ALL_TABLEGROUP_HISTORY_TNAME,
  OB_ALL_TABLE_PRIVILEGE_TNAME,
  OB_ALL_TABLE_PRIVILEGE_HISTORY_TNAME,
  OB_ALL_DATABASE_PRIVILEGE_TNAME,
  OB_ALL_DATABASE_PRIVILEGE_HISTORY_TNAME,
  OB_ALL_TABLE_HISTORY_TNAME,
  OB_ALL_COLUMN_HISTORY_TNAME,
  OB_TENANT_PARAMETER_TNAME,
  OB_ALL_SYS_VARIABLE_TNAME,
  OB_ALL_SYS_STAT_TNAME,
  OB_ALL_COLUMN_STATISTIC_TNAME,
  OB_ALL_DUMMY_TNAME,
  OB_ALL_CLOG_HISTORY_INFO_V2_TNAME,
  OB_ALL_OUTLINE_TNAME,
  OB_ALL_OUTLINE_HISTORY_TNAME,
  OB_ALL_RECYCLEBIN_TNAME,
  OB_ALL_PART_TNAME,
  OB_ALL_PART_HISTORY_TNAME,
  OB_ALL_SUB_PART_TNAME,
  OB_ALL_SUB_PART_HISTORY_TNAME,
  OB_ALL_PART_INFO_TNAME,
  OB_ALL_PART_INFO_HISTORY_TNAME,
  OB_ALL_DEF_SUB_PART_TNAME,
  OB_ALL_DEF_SUB_PART_HISTORY_TNAME,
  OB_ALL_SYS_VARIABLE_HISTORY_TNAME,
  OB_ALL_DDL_ID_TNAME,
  OB_ALL_FOREIGN_KEY_TNAME,
  OB_ALL_FOREIGN_KEY_HISTORY_TNAME,
  OB_ALL_FOREIGN_KEY_COLUMN_TNAME,
  OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TNAME,
  OB_ALL_SYNONYM_TNAME,
  OB_ALL_SYNONYM_HISTORY_TNAME,
  OB_ALL_SEQUENCE_V2_TNAME,
  OB_ALL_TENANT_META_TABLE_TNAME,
  OB_ALL_ROUTINE_TNAME,
  OB_ALL_ROUTINE_HISTORY_TNAME,
  OB_ALL_ROUTINE_PARAM_TNAME,
  OB_ALL_ROUTINE_PARAM_HISTORY_TNAME,
  OB_ALL_TABLE_STAT_TNAME,
  OB_ALL_COLUMN_STAT_TNAME,
  OB_ALL_HISTOGRAM_STAT_TNAME,
  OB_ALL_PACKAGE_TNAME,
  OB_ALL_PACKAGE_HISTORY_TNAME,
  OB_ALL_TENANT_GC_PARTITION_INFO_TNAME,
  OB_ALL_CONSTRAINT_TNAME,
  OB_ALL_CONSTRAINT_HISTORY_TNAME,
  OB_ALL_ORI_SCHEMA_VERSION_TNAME,
  OB_ALL_FUNC_TNAME,
  OB_ALL_FUNC_HISTORY_TNAME,
  OB_ALL_TEMP_TABLE_TNAME,
  OB_ALL_SEQUENCE_OBJECT_TNAME,
  OB_ALL_SEQUENCE_OBJECT_HISTORY_TNAME,
  OB_ALL_SEQUENCE_VALUE_TNAME,
  OB_ALL_TENANT_PLAN_BASELINE_TNAME,
  OB_ALL_TENANT_PLAN_BASELINE_HISTORY_TNAME,
  OB_ALL_TYPE_TNAME,
  OB_ALL_TYPE_HISTORY_TNAME,
  OB_ALL_TYPE_ATTR_TNAME,
  OB_ALL_TYPE_ATTR_HISTORY_TNAME,
  OB_ALL_COLL_TYPE_TNAME,
  OB_ALL_COLL_TYPE_HISTORY_TNAME,
  OB_ALL_WEAK_READ_SERVICE_TNAME,
  OB_ALL_DBLINK_TNAME,
  OB_ALL_DBLINK_HISTORY_TNAME,
  OB_ALL_TENANT_PARTITION_META_TABLE_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TNAME,
  OB_ALL_TENANT_KEYSTORE_TNAME,
  OB_ALL_TENANT_KEYSTORE_HISTORY_TNAME,
  OB_ALL_TENANT_TABLESPACE_TNAME,
  OB_ALL_TENANT_TABLESPACE_HISTORY_TNAME,
  OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TNAME,
  OB_ALL_TENANT_PROFILE_TNAME,
  OB_ALL_TENANT_PROFILE_HISTORY_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TNAME,
  OB_ALL_TENANT_TRIGGER_TNAME,
  OB_ALL_TENANT_TRIGGER_HISTORY_TNAME,
  OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TNAME,
  OB_ALL_TENANT_SYSAUTH_TNAME,
  OB_ALL_TENANT_SYSAUTH_HISTORY_TNAME,
  OB_ALL_TENANT_OBJAUTH_TNAME,
  OB_ALL_TENANT_OBJAUTH_HISTORY_TNAME,
  OB_ALL_TENANT_BACKUP_INFO_TNAME,
  OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
  OB_ALL_TENANT_BACKUP_TASK_TNAME,
  OB_ALL_TENANT_PG_BACKUP_TASK_TNAME,
  OB_ALL_TENANT_ERROR_TNAME,
  OB_ALL_TENANT_BACKUP_CLEAN_INFO_TNAME,
  OB_ALL_TENANT_RESTORE_PG_INFO_TNAME,
  OB_ALL_TABLE_V2_HISTORY_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TNAME,
  OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME,
  OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TNAME,
  OB_ALL_TENANT_TIME_ZONE_NAME_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TNAME,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
  OB_ALL_TENANT_DEPENDENCY_TNAME,
  OB_ALL_TENANT_BACKUP_BACKUPSET_TASK_TNAME,
  OB_ALL_TENANT_PG_BACKUP_BACKUPSET_TASK_TNAME,
  OB_ALL_TENANT_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TNAME,
  OB_ALL_RES_MGR_PLAN_TNAME,
  OB_ALL_RES_MGR_DIRECTIVE_TNAME,
  OB_ALL_RES_MGR_MAPPING_RULE_TNAME,
  OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME,
  OB_ALL_TABLE_V2_HISTORY_IDX_DATA_TABLE_ID_TNAME,
  OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TNAME,
  OB_TENANT_VIRTUAL_ALL_TABLE_TNAME,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_TNAME,
  OB_TENANT_VIRTUAL_TABLE_INDEX_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TNAME,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_TNAME,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TNAME,
  OB_TENANT_VIRTUAL_WARNING_TNAME,
  OB_TENANT_VIRTUAL_CURRENT_TENANT_TNAME,
  OB_TENANT_VIRTUAL_DATABASE_STATUS_TNAME,
  OB_TENANT_VIRTUAL_TENANT_STATUS_TNAME,
  OB_TENANT_VIRTUAL_INTERM_RESULT_TNAME,
  OB_TENANT_VIRTUAL_PARTITION_STAT_TNAME,
  OB_TENANT_VIRTUAL_STATNAME_TNAME,
  OB_TENANT_VIRTUAL_EVENT_NAME_TNAME,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TNAME,
  OB_TENANT_VIRTUAL_SHOW_TABLES_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TNAME,
  OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TNAME,
  OB_ALL_VIRTUAL_DATA_TYPE_TNAME,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_TNAME,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TNAME,
  OB_ALL_VIRTUAL_TRACE_LOG_TNAME,
  OB_ALL_VIRTUAL_ENGINE_TNAME,
  OB_TENANT_VIRTUAL_OUTLINE_TNAME,
  OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TNAME,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TNAME,
  OB_SESSION_VARIABLES_TNAME,
  OB_TABLE_PRIVILEGES_TNAME,
  OB_USER_PRIVILEGES_TNAME,
  OB_SCHEMA_PRIVILEGES_TNAME,
  OB_TABLE_CONSTRAINTS_TNAME,
  OB_GLOBAL_STATUS_TNAME,
  OB_PARTITIONS_TNAME,
  OB_SESSION_STATUS_TNAME,
  OB_USER_TNAME,
  OB_DB_TNAME,
  OB_PROC_TNAME,
  OB_TENANT_VIRTUAL_COLLATION_TNAME,
  OB_TENANT_VIRTUAL_CHARSET_TNAME,
  OB_PARAMETERS_TNAME,
  OB_ALL_VIRTUAL_WEAK_READ_STAT_TNAME,
  OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TNAME,
  OB_REFERENTIAL_CONSTRAINTS_TNAME,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TNAME,
  OB_ALL_VIRTUAL_OPEN_CURSOR_TNAME,
  OB_ALL_VIRTUAL_FILES_TNAME,
  OB_FILES_TNAME,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TNAME,
  OB_ALL_VIRTUAL_TABLE_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLUMN_AGENT_TNAME,
  OB_ALL_VIRTUAL_DATABASE_AGENT_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_V2_AGENT_TNAME,
  OB_ALL_VIRTUAL_PART_AGENT_TNAME,
  OB_ALL_VIRTUAL_SUB_PART_AGENT_TNAME,
  OB_ALL_VIRTUAL_PACKAGE_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_META_TABLE_AGENT_TNAME,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_ALL_VIRTUAL_SQL_AUDIT_I1_TNAME,
  OB_ALL_VIRTUAL_PLAN_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_AGENT_TNAME,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_VALUE_AGENT_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_AGENT_TNAME,
  OB_ALL_VIRTUAL_USER_AGENT_TNAME,
  OB_ALL_VIRTUAL_SYNONYM_AGENT_TNAME,
  OB_ALL_VIRTUAL_FOREIGN_KEY_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLUMN_STAT_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLUMN_STATISTIC_AGENT_TNAME,
  OB_ALL_VIRTUAL_PARTITION_TABLE_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLE_STAT_AGENT_TNAME,
  OB_ALL_VIRTUAL_RECYCLEBIN_AGENT_TNAME,
  OB_TENANT_VIRTUAL_OUTLINE_AGENT_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLEGROUP_AGENT_TNAME,
  OB_ALL_VIRTUAL_PRIVILEGE_AGENT_TNAME,
  OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TNAME,
  OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TNAME,
  OB_TENANT_VIRTUAL_CHARSET_AGENT_TNAME,
  OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TNAME,
  OB_TENANT_VIRTUAL_COLLATION_AGENT_TNAME,
  OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_AGENT_TNAME,
  OB_ALL_VIRTUAL_SERVER_AGENT_TNAME,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TNAME,
  OB_ALL_VIRTUAL_PROCESSLIST_ORA_TNAME,
  OB_ALL_VIRTUAL_SESSION_WAIT_ORA_TNAME,
  OB_ALL_VIRTUAL_SESSION_WAIT_ORA_ALL_VIRTUAL_SESSION_WAIT_I1_TNAME,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TNAME,
  OB_ALL_VIRTUAL_MEMORY_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_MEMSTORE_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_AGENT_TNAME,
  OB_ALL_VIRTUAL_SESSTAT_ORA_TNAME,
  OB_ALL_VIRTUAL_SESSTAT_ORA_ALL_VIRTUAL_SESSTAT_I1_TNAME,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_TNAME,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_ALL_VIRTUAL_SYSSTAT_I1_TNAME,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_ALL_VIRTUAL_SYSTEM_EVENT_I1_TNAME,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_AGENT_TNAME,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORA_TNAME,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORA_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORA_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_ORA_TNAME,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_ORA_TNAME,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TNAME,
  OB_ALL_VIRTUAL_TRACE_LOG_ORA_TNAME,
  OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TNAME,
  OB_ALL_VIRTUAL_CONSTRAINT_AGENT_TNAME,
  OB_ALL_VIRTUAL_TYPE_AGENT_TNAME,
  OB_ALL_VIRTUAL_TYPE_ATTR_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLL_TYPE_AGENT_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_AGENT_TNAME,
  OB_ALL_VIRTUAL_DATA_TYPE_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_SSTABLE_CHECKSUM_AGENT_TNAME,
  OB_ALL_VIRTUAL_PARTITION_INFO_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_KEYSTORE_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TABLESPACE_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_PROFILE_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_AGENT_TNAME,
  OB_ALL_VIRTUAL_SECURITY_AUDIT_AGENT_TNAME,
  OB_ALL_VIRTUAL_SECURITY_AUDIT_HISTORY_AGENT_TNAME,
  OB_ALL_VIRTUAL_TRIGGER_AGENT_TNAME,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_PS_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_AGENT_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_AGENT_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_AGENT_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_AGENT_TNAME,
  OB_ALL_VIRTUAL_SECURITY_AUDIT_RECORD_AGENT_TNAME,
  OB_ALL_VIRTUAL_SYSAUTH_AGENT_TNAME,
  OB_ALL_VIRTUAL_SYSAUTH_HISTORY_AGENT_TNAME,
  OB_ALL_VIRTUAL_OBJAUTH_AGENT_TNAME,
  OB_ALL_VIRTUAL_OBJAUTH_HISTORY_AGENT_TNAME,
  OB_ALL_VIRTUAL_ERROR_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLE_MGR_AGENT_TNAME,
  OB_ALL_VIRTUAL_DEF_SUB_PART_AGENT_TNAME,
  OB_ALL_VIRTUAL_OBJECT_TYPE_AGENT_TNAME,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_AGENT_TNAME,
  OB_ALL_VIRTUAL_DBLINK_AGENT_TNAME,
  OB_ALL_VIRTUAL_DBLINK_HISTORY_AGENT_TNAME,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TNAME,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_ORA_TNAME,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_OPEN_CURSOR_ORA_TNAME,
  OB_ALL_VIRTUAL_CONSTRAINT_COLUMN_AGENT_TNAME,
  OB_ALL_VIRTUAL_DEPENDENCY_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_AGENT_TNAME,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_ORA_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_TYPE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_GLOBAL_TRANSACTION_AGENT_TNAME,
  OB_ALL_VIRTUAL_ACQUIRED_SNAPSHOT_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_V2_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_COLUMN_STATISTIC_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLE_STAT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_PARTITION_META_TABLE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SYSAUTH_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DBLINK_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORA_TNAME,
  OB_GV_PLAN_CACHE_STAT_TNAME,
  OB_GV_PLAN_CACHE_PLAN_STAT_TNAME,
  OB_SCHEMATA_TNAME,
  OB_CHARACTER_SETS_TNAME,
  OB_GLOBAL_VARIABLES_TNAME,
  OB_STATISTICS_TNAME,
  OB_VIEWS_TNAME,
  OB_TABLES_TNAME,
  OB_COLLATIONS_TNAME,
  OB_COLLATION_CHARACTER_SET_APPLICABILITY_TNAME,
  OB_PROCESSLIST_TNAME,
  OB_KEY_COLUMN_USAGE_TNAME,
  OB_DBA_OUTLINES_TNAME,
  OB_ENGINES_TNAME,
  OB_ROUTINES_TNAME,
  OB_GV_SESSION_EVENT_TNAME,
  OB_GV_SESSION_WAIT_TNAME,
  OB_GV_SESSION_WAIT_HISTORY_TNAME,
  OB_GV_SYSTEM_EVENT_TNAME,
  OB_GV_SESSTAT_TNAME,
  OB_GV_SYSSTAT_TNAME,
  OB_V_STATNAME_TNAME,
  OB_V_EVENT_NAME_TNAME,
  OB_V_SESSION_EVENT_TNAME,
  OB_V_SESSION_WAIT_TNAME,
  OB_V_SESSION_WAIT_HISTORY_TNAME,
  OB_V_SESSTAT_TNAME,
  OB_V_SYSSTAT_TNAME,
  OB_V_SYSTEM_EVENT_TNAME,
  OB_GV_SQL_AUDIT_TNAME,
  OB_GV_LATCH_TNAME,
  OB_GV_MEMORY_TNAME,
  OB_V_MEMORY_TNAME,
  OB_GV_MEMSTORE_TNAME,
  OB_V_MEMSTORE_TNAME,
  OB_GV_MEMSTORE_INFO_TNAME,
  OB_V_MEMSTORE_INFO_TNAME,
  OB_V_PLAN_CACHE_STAT_TNAME,
  OB_V_PLAN_CACHE_PLAN_STAT_TNAME,
  OB_GV_PLAN_CACHE_PLAN_EXPLAIN_TNAME,
  OB_V_PLAN_CACHE_PLAN_EXPLAIN_TNAME,
  OB_V_SQL_AUDIT_TNAME,
  OB_V_LATCH_TNAME,
  OB_GV_OBRPC_OUTGOING_TNAME,
  OB_V_OBRPC_OUTGOING_TNAME,
  OB_GV_OBRPC_INCOMING_TNAME,
  OB_V_OBRPC_INCOMING_TNAME,
  OB_GV_SQL_TNAME,
  OB_V_SQL_TNAME,
  OB_GV_SQL_MONITOR_TNAME,
  OB_V_SQL_MONITOR_TNAME,
  OB_GV_SQL_PLAN_MONITOR_TNAME,
  OB_V_SQL_PLAN_MONITOR_TNAME,
  OB_USER_RECYCLEBIN_TNAME,
  OB_GV_OUTLINE_TNAME,
  OB_GV_CONCURRENT_LIMIT_SQL_TNAME,
  OB_GV_SQL_PLAN_STATISTICS_TNAME,
  OB_V_SQL_PLAN_STATISTICS_TNAME,
  OB_GV_SERVER_MEMSTORE_TNAME,
  OB_GV_UNIT_LOAD_BALANCE_EVENT_HISTORY_TNAME,
  OB_GV_TENANT_TNAME,
  OB_GV_DATABASE_TNAME,
  OB_GV_TABLE_TNAME,
  OB_GV_UNIT_TNAME,
  OB_V_UNIT_TNAME,
  OB_GV_PARTITION_TNAME,
  OB_V_PARTITION_TNAME,
  OB_GV_LOCK_WAIT_STAT_TNAME,
  OB_V_LOCK_WAIT_STAT_TNAME,
  OB_TIME_ZONE_TNAME,
  OB_TIME_ZONE_NAME_TNAME,
  OB_TIME_ZONE_TRANSITION_TNAME,
  OB_TIME_ZONE_TRANSITION_TYPE_TNAME,
  OB_GV_SESSION_LONGOPS_TNAME,
  OB_V_SESSION_LONGOPS_TNAME,
  OB_GV_TENANT_MEMSTORE_ALLOCATOR_INFO_TNAME,
  OB_V_TENANT_MEMSTORE_ALLOCATOR_INFO_TNAME,
  OB_GV_TENANT_SEQUENCE_OBJECT_TNAME,
  OB_COLUMNS_TNAME,
  OB_GV_MINOR_MERGE_INFO_TNAME,
  OB_GV_TENANT_PX_WORKER_STAT_TNAME,
  OB_V_TENANT_PX_WORKER_STAT_TNAME,
  OB_GV_PARTITION_AUDIT_TNAME,
  OB_V_PARTITION_AUDIT_TNAME,
  OB_GV_PS_STAT_TNAME,
  OB_V_PS_STAT_TNAME,
  OB_GV_PS_ITEM_INFO_TNAME,
  OB_V_PS_ITEM_INFO_TNAME,
  OB_GV_SQL_WORKAREA_TNAME,
  OB_V_SQL_WORKAREA_TNAME,
  OB_GV_SQL_WORKAREA_ACTIVE_TNAME,
  OB_V_SQL_WORKAREA_ACTIVE_TNAME,
  OB_GV_SQL_WORKAREA_HISTOGRAM_TNAME,
  OB_V_SQL_WORKAREA_HISTOGRAM_TNAME,
  OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_TNAME,
  OB_V_OB_SQL_WORKAREA_MEMORY_INFO_TNAME,
  OB_GV_PLAN_CACHE_REFERENCE_INFO_TNAME,
  OB_V_PLAN_CACHE_REFERENCE_INFO_TNAME,
  OB_V_OB_TIMESTAMP_SERVICE_TNAME,
  OB_GV_SSTABLE_TNAME,
  OB_V_SSTABLE_TNAME,
  OB_GV_SERVER_SCHEMA_INFO_TNAME,
  OB_V_SERVER_SCHEMA_INFO_TNAME,
  OB_CDB_CKPT_HISTORY_TNAME,
  OB_GV_OB_TRANS_TABLE_STATUS_TNAME,
  OB_V_OB_TRANS_TABLE_STATUS_TNAME,
  OB_V_SQL_MONITOR_STATNAME_TNAME,
  OB_GV_MERGE_INFO_TNAME,
  OB_V_MERGE_INFO_TNAME,
  OB_GV_LOCK_TNAME,
  OB_V_LOCK_TNAME,
  OB_V_RESTORE_POINT_TNAME,
  OB_COLUMN_PRIVILEGES_TNAME,
  OB_SHOW_RABBIT_TNAME,
  OB_DBA_SYNONYMS_TNAME,
  OB_DBA_OBJECTS_TNAME,
  OB_ALL_OBJECTS_TNAME,
  OB_USER_OBJECTS_TNAME,
  OB_DBA_SEQUENCES_TNAME,
  OB_ALL_SEQUENCES_TNAME,
  OB_USER_SEQUENCES_TNAME,
  OB_DBA_USERS_TNAME,
  OB_ALL_USERS_TNAME,
  OB_ALL_SYNONYMS_TNAME,
  OB_USER_SYNONYMS_TNAME,
  OB_DBA_IND_COLUMNS_TNAME,
  OB_ALL_IND_COLUMNS_TNAME,
  OB_USER_IND_COLUMNS_TNAME,
  OB_DBA_CONSTRAINTS_TNAME,
  OB_ALL_CONSTRAINTS_TNAME,
  OB_USER_CONSTRAINTS_TNAME,
  OB_ALL_TAB_COLS_V_TNAME,
  OB_DBA_TAB_COLS_V_TNAME,
  OB_USER_TAB_COLS_V_TNAME,
  OB_ALL_TAB_COLS_TNAME,
  OB_DBA_TAB_COLS_TNAME,
  OB_USER_TAB_COLS_TNAME,
  OB_ALL_TAB_COLUMNS_TNAME,
  OB_DBA_TAB_COLUMNS_TNAME,
  OB_USER_TAB_COLUMNS_TNAME,
  OB_ALL_TABLES_TNAME,
  OB_DBA_TABLES_TNAME,
  OB_USER_TABLES_TNAME,
  OB_DBA_TAB_COMMENTS_TNAME,
  OB_ALL_TAB_COMMENTS_TNAME,
  OB_USER_TAB_COMMENTS_TNAME,
  OB_DBA_COL_COMMENTS_TNAME,
  OB_ALL_COL_COMMENTS_TNAME,
  OB_USER_COL_COMMENTS_TNAME,
  OB_DBA_INDEXES_TNAME,
  OB_ALL_INDEXES_TNAME,
  OB_USER_INDEXES_TNAME,
  OB_DBA_CONS_COLUMNS_TNAME,
  OB_ALL_CONS_COLUMNS_TNAME,
  OB_USER_CONS_COLUMNS_TNAME,
  OB_USER_SEGMENTS_TNAME,
  OB_DBA_SEGMENTS_TNAME,
  OB_DBA_TYPES_TNAME,
  OB_ALL_TYPES_TNAME,
  OB_USER_TYPES_TNAME,
  OB_DBA_TYPE_ATTRS_TNAME,
  OB_ALL_TYPE_ATTRS_TNAME,
  OB_USER_TYPE_ATTRS_TNAME,
  OB_DBA_COLL_TYPES_TNAME,
  OB_ALL_COLL_TYPES_TNAME,
  OB_USER_COLL_TYPES_TNAME,
  OB_DBA_PROCEDURES_TNAME,
  OB_DBA_ARGUMENTS_TNAME,
  OB_DBA_SOURCE_TNAME,
  OB_ALL_PROCEDURES_TNAME,
  OB_ALL_ARGUMENTS_TNAME,
  OB_ALL_SOURCE_TNAME,
  OB_USER_PROCEDURES_TNAME,
  OB_USER_ARGUMENTS_TNAME,
  OB_USER_SOURCE_TNAME,
  OB_DBA_PART_KEY_COLUMNS_TNAME,
  OB_ALL_PART_KEY_COLUMNS_TNAME,
  OB_USER_PART_KEY_COLUMNS_TNAME,
  OB_DBA_SUBPART_KEY_COLUMNS_TNAME,
  OB_ALL_SUBPART_KEY_COLUMNS_TNAME,
  OB_USER_SUBPART_KEY_COLUMNS_TNAME,
  OB_DBA_VIEWS_TNAME,
  OB_ALL_VIEWS_TNAME,
  OB_USER_VIEWS_TNAME,
  OB_ALL_TAB_PARTITIONS_ORA_TNAME,
  OB_ALL_TAB_SUBPARTITIONS_ORA_TNAME,
  OB_ALL_PART_TABLES_ORA_TNAME,
  OB_DBA_PART_TABLES_ORA_TNAME,
  OB_USER_PART_TABLES_ORA_TNAME,
  OB_DBA_TAB_PARTITIONS_ORA_TNAME,
  OB_USER_TAB_PARTITIONS_ORA_TNAME,
  OB_DBA_TAB_SUBPARTITIONS_ORA_TNAME,
  OB_USER_TAB_SUBPARTITIONS_ORA_TNAME,
  OB_DBA_SUBPARTITION_TEMPLATES_ORA_TNAME,
  OB_ALL_SUBPARTITION_TEMPLATES_ORA_TNAME,
  OB_USER_SUBPARTITION_TEMPLATES_ORA_TNAME,
  OB_DBA_PART_INDEXES_TNAME,
  OB_ALL_PART_INDEXES_TNAME,
  OB_USER_PART_INDEXES_TNAME,
  OB_ALL_ALL_TABLES_ORA_TNAME,
  OB_DBA_ALL_TABLES_ORA_TNAME,
  OB_USER_ALL_TABLES_ORA_TNAME,
  OB_DBA_PROFILES_ORA_TNAME,
  OB_USER_PROFILES_ORA_TNAME,
  OB_ALL_PROFILES_ORA_TNAME,
  OB_ALL_MVIEW_COMMENTS_ORA_TNAME,
  OB_USER_MVIEW_COMMENTS_ORA_TNAME,
  OB_DBA_MVIEW_COMMENTS_ORA_TNAME,
  OB_ALL_SCHEDULER_PROGRAM_ARGS_ORA_TNAME,
  OB_DBA_SCHEDULER_PROGRAM_ARGS_ORA_TNAME,
  OB_USER_SCHEDULER_PROGRAM_ARGS_ORA_TNAME,
  OB_ALL_SCHEDULER_JOB_ARGS_ORA_TNAME,
  OB_DBA_SCHEDULER_JOB_ARGS_ORA_TNAME,
  OB_USER_SCHEDULER_JOB_ARGS_ORA_TNAME,
  OB_ALL_ERRORS_ORA_TNAME,
  OB_DBA_ERRORS_ORA_TNAME,
  OB_USER_ERRORS_ORA_TNAME,
  OB_ALL_TYPE_METHODS_ORA_TNAME,
  OB_DBA_TYPE_METHODS_ORA_TNAME,
  OB_USER_TYPE_METHODS_ORA_TNAME,
  OB_ALL_METHOD_PARAMS_ORA_TNAME,
  OB_DBA_METHOD_PARAMS_ORA_TNAME,
  OB_USER_METHOD_PARAMS_ORA_TNAME,
  OB_DBA_TABLESPACES_ORA_TNAME,
  OB_USER_TABLESPACES_ORA_TNAME,
  OB_DBA_IND_EXPRESSIONS_TNAME,
  OB_USER_IND_EXPRESSIONS_TNAME,
  OB_ALL_IND_EXPRESSIONS_TNAME,
  OB_ALL_IND_PARTITIONS_TNAME,
  OB_USER_IND_PARTITIONS_TNAME,
  OB_DBA_IND_PARTITIONS_TNAME,
  OB_DBA_IND_SUBPARTITIONS_TNAME,
  OB_ALL_IND_SUBPARTITIONS_TNAME,
  OB_USER_IND_SUBPARTITIONS_TNAME,
  OB_DBA_ROLES_TNAME,
  OB_DBA_ROLE_PRIVS_TNAME,
  OB_USER_ROLE_PRIVS_TNAME,
  OB_DBA_TAB_PRIVS_TNAME,
  OB_ALL_TAB_PRIVS_TNAME,
  OB_USER_TAB_PRIVS_TNAME,
  OB_DBA_SYS_PRIVS_TNAME,
  OB_USER_SYS_PRIVS_TNAME,
  OB_DBA_COL_PRIVS_ORA_TNAME,
  OB_USER_COL_PRIVS_ORA_TNAME,
  OB_ALL_COL_PRIVS_ORA_TNAME,
  OB_ROLE_TAB_PRIVS_ORA_TNAME,
  OB_ROLE_SYS_PRIVS_ORA_TNAME,
  OB_ROLE_ROLE_PRIVS_ORA_TNAME,
  OB_DICTIONARY_ORA_TNAME,
  OB_DICT_ORA_TNAME,
  OB_ALL_TRIGGERS_TNAME,
  OB_DBA_TRIGGERS_TNAME,
  OB_USER_TRIGGERS_TNAME,
  OB_ALL_DEPENDENCIES_ORA_TNAME,
  OB_DBA_DEPENDENCIES_ORA_TNAME,
  OB_USER_DEPENDENCIES_ORA_TNAME,
  OB_DBA_RSRC_PLANS_TNAME,
  OB_DBA_RSRC_PLAN_DIRECTIVES_TNAME,
  OB_DBA_RSRC_GROUP_MAPPINGS_TNAME,
  OB_DBA_RECYCLEBIN_ORA_TNAME,
  OB_USER_RECYCLEBIN_ORA_TNAME,
  OB_DBA_RSRC_CONSUMER_GROUPS_TNAME,
  OB_GV_OUTLINE_ORA_TNAME,
  OB_GV_SQL_AUDIT_ORA_TNAME,
  OB_V_SQL_AUDIT_ORA_TNAME,
  OB_GV_INSTANCE_TNAME,
  OB_V_INSTANCE_TNAME,
  OB_GV_PLAN_CACHE_PLAN_STAT_ORA_TNAME,
  OB_V_PLAN_CACHE_PLAN_STAT_ORA_TNAME,
  OB_GV_PLAN_CACHE_PLAN_EXPLAIN_ORA_TNAME,
  OB_V_PLAN_CACHE_PLAN_EXPLAIN_ORA_TNAME,
  OB_GV_SESSION_WAIT_ORA_TNAME,
  OB_V_SESSION_WAIT_ORA_TNAME,
  OB_GV_SESSION_WAIT_HISTORY_ORA_TNAME,
  OB_V_SESSION_WAIT_HISTORY_ORA_TNAME,
  OB_GV_MEMORY_ORA_TNAME,
  OB_V_MEMORY_ORA_TNAME,
  OB_GV_MEMSTORE_ORA_TNAME,
  OB_V_MEMSTORE_ORA_TNAME,
  OB_GV_MEMSTORE_INFO_ORA_TNAME,
  OB_V_MEMSTORE_INFO_ORA_TNAME,
  OB_GV_SERVER_MEMSTORE_ORA_TNAME,
  OB_GV_SESSTAT_ORA_TNAME,
  OB_V_SESSTAT_ORA_TNAME,
  OB_GV_SYSSTAT_ORA_TNAME,
  OB_V_SYSSTAT_ORA_TNAME,
  OB_GV_SYSTEM_EVENT_ORA_TNAME,
  OB_V_SYSTEM_EVENT_ORA_TNAME,
  OB_GV_TENANT_MEMSTORE_ALLOCATOR_INFO_ORA_TNAME,
  OB_V_TENANT_MEMSTORE_ALLOCATOR_INFO_ORA_TNAME,
  OB_GV_PLAN_CACHE_STAT_ORA_TNAME,
  OB_V_PLAN_CACHE_STAT_ORA_TNAME,
  OB_GV_CONCURRENT_LIMIT_SQL_ORA_TNAME,
  OB_NLS_SESSION_PARAMETERS_ORA_TNAME,
  OB_NLS_INSTANCE_PARAMETERS_ORA_TNAME,
  OB_NLS_DATABASE_PARAMETERS_ORA_TNAME,
  OB_V_NLS_PARAMETERS_ORA_TNAME,
  OB_V_VERSION_ORA_TNAME,
  OB_GV_TENANT_PX_WORKER_STAT_ORA_TNAME,
  OB_V_TENANT_PX_WORKER_STAT_ORA_TNAME,
  OB_GV_PS_STAT_ORA_TNAME,
  OB_V_PS_STAT_ORA_TNAME,
  OB_GV_PS_ITEM_INFO_ORA_TNAME,
  OB_V_PS_ITEM_INFO_ORA_TNAME,
  OB_GV_SQL_WORKAREA_ACTIVE_ORA_TNAME,
  OB_V_SQL_WORKAREA_ACTIVE_ORA_TNAME,
  OB_GV_SQL_WORKAREA_HISTOGRAM_ORA_TNAME,
  OB_V_SQL_WORKAREA_HISTOGRAM_ORA_TNAME,
  OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TNAME,
  OB_V_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TNAME,
  OB_GV_PLAN_CACHE_REFERENCE_INFO_ORA_TNAME,
  OB_V_PLAN_CACHE_REFERENCE_INFO_ORA_TNAME,
  OB_GV_SQL_WORKAREA_ORA_TNAME,
  OB_V_SQL_WORKAREA_ORA_TNAME,
  OB_GV_SSTABLE_ORA_TNAME,
  OB_V_SSTABLE_ORA_TNAME,
  OB_GV_SERVER_SCHEMA_INFO_ORA_TNAME,
  OB_V_SERVER_SCHEMA_INFO_ORA_TNAME,
  OB_GV_SQL_PLAN_MONITOR_ORA_TNAME,
  OB_V_SQL_PLAN_MONITOR_ORA_TNAME,
  OB_V_SQL_MONITOR_STATNAME_ORA_TNAME,
  OB_GV_LOCK_ORA_TNAME,
  OB_V_LOCK_ORA_TNAME,
  OB_GV_OPEN_CURSOR_ORA_TNAME,
  OB_V_OPEN_CURSOR_ORA_TNAME,
  OB_V_TIMEZONE_NAMES_ORA_TNAME,
  OB_GV_GLOBAL_TRANSACTION_ORA_TNAME,
  OB_V_GLOBAL_TRANSACTION_ORA_TNAME,
  OB_V_RESTORE_POINT_ORA_TNAME,
  OB_V_RSRC_PLAN_ORA_TNAME,  };

const uint64_t only_rs_vtables [] = {
  OB_ALL_VIRTUAL_CORE_META_TABLE_TID,
  OB_ALL_VIRTUAL_SERVER_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_TASK_STAT_TID,
  OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID,
  OB_ALL_VIRTUAL_CORE_ROOT_TABLE_TID,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID,
  OB_ALL_VIRTUAL_TENANT_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_TENANT_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_UNIT_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_REPLICA_STAT_TID,
  OB_ALL_VIRTUAL_LEADER_STAT_TID,
  OB_ALL_VIRTUAL_ROOTSERVICE_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_MAP_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_MAP_ITEM_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_UNIT_MIGRATE_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_UNIT_DISTRIBUTION_STAT_TID,
  OB_ALL_VIRTUAL_PARTITION_TABLE_TID,
  OB_ALL_VIRTUAL_REPLICA_TASK_TID,
  OB_ALL_VIRTUAL_REPLICA_TASK_TID,
  OB_ALL_VIRTUAL_FREEZE_INFO_TID,
  OB_ALL_VIRTUAL_CLUSTER_TID,
  OB_ALL_VIRTUAL_CLUSTER_STATS_TID,
  OB_ALL_VIRTUAL_BACKUP_CLEAN_INFO_TID,
  OB_ALL_VIRTUAL_BACKUPSET_HISTORY_MGR_TID,
  OB_ALL_VIRTUAL_PARTITION_TABLE_AGENT_TID,  };

const uint64_t restrict_access_virtual_tables[] = {
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_ORA_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_MEMORY_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_MEMSTORE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SESSTAT_ORA_TID,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TID,
  OB_ALL_VIRTUAL_PS_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TID  };


static inline bool is_restrict_access_virtual_table(const uint64_t tid)
{
  bool found = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(restrict_access_virtual_tables) && !found; i++) {
    if (common::extract_pure_id(tid) == restrict_access_virtual_tables[i]) {
      found = true;
    }
  }
  return found;
}

static inline bool is_tenant_table(const uint64_t tid)
{
  bool in_tenant_space = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_space_tables); ++i) {
    if (common::extract_pure_id(tid) == tenant_space_tables[i]) {
      in_tenant_space = true;
      break;
    }
  }
  return in_tenant_space;
}

static inline bool is_tenant_table_name(const common::ObString &tname)
{
  bool in_tenant_space = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_space_table_names); ++i) {
    if (0 == tname.case_compare(tenant_space_table_names[i])) {
      in_tenant_space = true;
      break;
    }
  }
  return in_tenant_space;
}

static inline bool is_global_virtual_table(const uint64_t tid)
{
  return common::is_virtual_table(tid) && !is_tenant_table(tid);
}

static inline bool is_tenant_virtual_table(const uint64_t tid)
{
  return common::is_virtual_table(tid) && is_tenant_table(tid);
}

static inline uint64_t get_origin_tid_by_oracle_mapping_tid(const uint64_t tid)
{
  uint64_t org_tid = common::OB_INVALID_ID;
  uint64_t idx = common::OB_INVALID_ID;
  for (uint64_t i = 0; common::OB_INVALID_ID == idx && i < ARRAYSIZEOF(all_ora_mapping_virtual_tables); ++i) {
    if (common::extract_pure_id(tid) == all_ora_mapping_virtual_tables[i]) {
      idx = i;
    }
  }
  if (common::OB_INVALID_ID != idx) {
     org_tid = all_ora_mapping_virtual_table_org_tables[idx];
  }
  return org_tid;
}

static inline bool is_oracle_mapping_virtual_table(const uint64_t tid)
{
  bool is_ora_vt = false;
  for (uint64_t i = 0; i < ARRAYSIZEOF(all_ora_mapping_virtual_tables); ++i) {
    if (common::extract_pure_id(tid) == all_ora_mapping_virtual_tables[i]) {
      is_ora_vt = true;
    }
  }
  return is_ora_vt;
}

static inline uint64_t get_real_table_mappings_tid(const uint64_t tid)
{
  uint64_t org_tid = common::OB_INVALID_ID;
  uint64_t pure_id = common::extract_pure_id(tid);
  if (pure_id >= common::OB_MIN_VIRTUAL_TABLE_ID && pure_id < common::OB_MAX_VIRTUAL_TABLE_ID) {
    int64_t idx = pure_id - common::OB_MIN_VIRTUAL_TABLE_ID;
    VTMapping &tmp_vt_mapping = vt_mappings[idx];
    if (tmp_vt_mapping.is_real_vt_) {
      org_tid = tmp_vt_mapping.mapping_tid_;
    }
  }
  return org_tid;
}

static inline bool is_oracle_mapping_real_virtual_table(const uint64_t tid)
{
  return common::OB_INVALID_ID != get_real_table_mappings_tid(tid);
}

static inline void get_real_table_vt_mapping(const uint64_t tid, VTMapping *&vt_mapping)
{
  uint64_t pure_id = common::extract_pure_id(tid);
  vt_mapping = nullptr;
  if (pure_id >= common::OB_MIN_VIRTUAL_TABLE_ID && pure_id < common::OB_MAX_VIRTUAL_TABLE_ID) {
    int64_t idx = pure_id - common::OB_MIN_VIRTUAL_TABLE_ID;
    vt_mapping = &vt_mappings[idx];
  }
}

static inline bool is_only_rs_virtual_table(const uint64_t tid)
{
  bool only_rs = false;
  if (common::extract_pure_id(tid) == OB_ALL_VIRTUAL_ZONE_STAT_TID
      && GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_141) {
    only_rs = true;
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(only_rs_vtables); ++i) {
      if (common::extract_pure_id(tid) == only_rs_vtables[i]) {
        only_rs = true;
      }
    }
  }
  return only_rs;
}

const int64_t OB_CORE_TABLE_COUNT = 5;
const int64_t OB_SYS_TABLE_COUNT = 187;
const int64_t OB_VIRTUAL_TABLE_COUNT = 463;
const int64_t OB_SYS_VIEW_COUNT = 361;
const int64_t OB_SYS_TENANT_TABLE_COUNT = 1017;
const int64_t OB_CORE_SCHEMA_VERSION = 1;
const int64_t OB_BOOTSTRAP_SCHEMA_VERSION = 1020;

} // end namespace share
} // end namespace oceanbase
#endif /* _OB_INNER_TABLE_SCHEMA_H_ */
