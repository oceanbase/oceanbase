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
    CONFIGS,
    PLAN_HASH,
    FIRST_LOAD_TIME,
    SCHEMA_VERSION,
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
    IS_BATCHED_MULTI_STMT,
    OBJECT_STATUS,
    RULE_NAME,
    IS_IN_PC,
    ERASE_TIME,
    COMPILE_TIME
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
    EDIT_LEVEL,
    TENANT_ID,
    DEFAULT_VALUE,
    ISDEFAULT
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
    CONFIGS,
    PLAN_HASH,
    FIRST_LOAD_TIME,
    SCHEMA_VERSION,
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
    IS_BATCHED_MULTI_STMT,
    OBJECT_STATUS,
    RULE_NAME,
    IS_IN_PC,
    ERASE_TIME,
    COMPILE_TIME
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
    EDIT_LEVEL,
    TENANT_ID,
    DEFAULT_VALUE,
    ISDEFAULT
  };
};

class ObInnerTableSchema
{

public:
  static int all_core_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_operation_schema(share::schema::ObTableSchema &table_schema);
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
  static int all_unit_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_config_schema(share::schema::ObTableSchema &table_schema);
  static int all_resource_pool_schema(share::schema::ObTableSchema &table_schema);
  static int all_charset_schema(share::schema::ObTableSchema &table_schema);
  static int all_collation_schema(share::schema::ObTableSchema &table_schema);
  static int help_topic_schema(share::schema::ObTableSchema &table_schema);
  static int help_category_schema(share::schema::ObTableSchema &table_schema);
  static int help_keyword_schema(share::schema::ObTableSchema &table_schema);
  static int help_relation_schema(share::schema::ObTableSchema &table_schema);
  static int all_dummy_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_history_schema(share::schema::ObTableSchema &table_schema);
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
  static int all_sys_variable_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_auto_increment_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_param_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_param_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_acquired_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraint_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraint_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_ori_schema_version_schema(share::schema::ObTableSchema &table_schema);
  static int all_func_schema(share::schema::ObTableSchema &table_schema);
  static int all_func_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_temp_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_value_schema(share::schema::ObTableSchema &table_schema);
  static int all_freeze_schema_version_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attr_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attr_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_type_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_weak_read_service_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_policy_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_policy_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_component_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_component_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_label_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_label_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_user_level_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_user_level_history_schema(share::schema::ObTableSchema &table_schema);
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
  static int all_tenant_security_audit_record_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_sysauth_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_sysauth_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_error_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_transition_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_transition_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_constraint_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_constraint_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_global_transaction_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_dependency_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_plan_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_directive_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_mapping_rule_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_error_message_schema(share::schema::ObTableSchema &table_schema);
  static int all_space_usage_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_set_files_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_consumer_group_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_task_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_region_network_bandwidth_limit_schema(share::schema::ObTableSchema &table_schema);
  static int all_deadlock_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_usage_schema(share::schema::ObTableSchema &table_schema);
  static int all_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_job_log_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_directory_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_directory_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_histogram_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_monitor_modified_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_stat_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_stat_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_histogram_stat_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_optstat_global_prefs_schema(share::schema::ObTableSchema &table_schema);
  static int all_optstat_user_prefs_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_to_ls_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_piece_files_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_log_archive_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_storage_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_dam_last_arch_ts_schema(share::schema::ObTableSchema &table_schema);
  static int all_dam_cleanup_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_skipped_tablet_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_skipped_tablet_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_to_table_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_recovery_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_replica_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_replica_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_pending_transaction_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_group_ls_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_job_run_detail_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_program_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_program_argument_schema(share::schema::ObTableSchema &table_schema);
  static int all_context_schema(share::schema::ObTableSchema &table_schema);
  static int all_context_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_global_context_value_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_election_reference_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_ls_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_ls_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_zone_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_freeze_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_disk_io_calibration_schema(share::schema::ObTableSchema &table_schema);
  static int all_plan_baseline_schema(share::schema::ObTableSchema &table_schema);
  static int all_plan_baseline_item_schema(share::schema::ObTableSchema &table_schema);
  static int all_spm_config_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_dest_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_restore_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_restore_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_storage_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_policy_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_restore_source_schema(share::schema::ObTableSchema &table_schema);
  static int all_kv_ttl_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_kv_ttl_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_service_epoch_schema(share::schema::ObTableSchema &table_schema);
  static int all_spatial_reference_systems_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_checksum_error_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_mapping_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_mapping_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_arbitration_service_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_arb_replica_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_data_dictionary_in_log_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_arb_replica_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_policy_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_policy_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_security_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_security_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_group_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_group_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_context_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_context_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_attribute_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_attribute_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_rewrite_rules_schema(share::schema::ObTableSchema &table_schema);
  static int all_reserved_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int all_cluster_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_transfer_member_list_lock_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_external_table_file_schema(share::schema::ObTableSchema &table_schema);
  static int all_task_opt_stat_gather_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_opt_stat_gather_history_schema(share::schema::ObTableSchema &table_schema);
  static int wr_active_session_history_schema(share::schema::ObTableSchema &table_schema);
  static int wr_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int wr_statname_schema(share::schema::ObTableSchema &table_schema);
  static int wr_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_task_helper_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_ls_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_ls_replica_schema(share::schema::ObTableSchema &table_schema);
  static int all_mlog_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_sys_defaults_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_params_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_run_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_change_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stmt_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_dbms_lock_allocated_schema(share::schema::ObTableSchema &table_schema);
  static int wr_control_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_job_class_schema(share::schema::ObTableSchema &table_schema);
  static int all_recover_table_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_recover_table_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_storage_ha_error_diagnose_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_storage_ha_perf_diagnose_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_clone_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_clone_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_ncomp_dll_schema(share::schema::ObTableSchema &table_schema);
  static int all_aux_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_index_usage_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_detect_lock_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_client_to_server_session_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_partition_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_partition_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_trusted_root_certificate_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_ls_replica_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_role_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_role_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_dep_schema(share::schema::ObTableSchema &table_schema);
  static int all_scheduler_job_run_detail_v2_schema(share::schema::ObTableSchema &table_schema);
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
  static int tenant_virtual_statname_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_event_name_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_global_variable_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_tables_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_procedure_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mem_leak_checker_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_latch_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_kvcache_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_type_class_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_event_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_concurrency_object_pool_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_disk_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_upgrade_inspection_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_ctx_mgr_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_scheduler_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_all_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_column_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_parameter_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trace_span_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_engine_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_server_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_sys_variable_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_schema_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_plan_explain_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_obrpc_stat_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_outline_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_concurrent_limit_sql_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_sstable_macro_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_partition_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_partition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_sub_partition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_task_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_macro_block_marker_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_io_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_long_ops_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_object_pool_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_lock_stat_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_tablegroup_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_blacklist_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_parameter_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_schema_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memory_context_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dump_tenant_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_parameter_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_audit_operation_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_audit_action_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dag_warning_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_encrypt_info_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_restore_preview_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_master_key_version_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dag_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dag_scheduler_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_compaction_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_compaction_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_compaction_diagnose_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_compaction_suggestion_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_compaction_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_io_calibration_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_io_benchmark_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_io_quota_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_compaction_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ddl_sim_point_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ddl_sim_point_stat_schema(share::schema::ObTableSchema &table_schema);
  static int session_variables_schema(share::schema::ObTableSchema &table_schema);
  static int global_status_schema(share::schema::ObTableSchema &table_schema);
  static int session_status_schema(share::schema::ObTableSchema &table_schema);
  static int user_schema(share::schema::ObTableSchema &table_schema);
  static int db_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_lock_wait_stat_schema(share::schema::ObTableSchema &table_schema);
  static int proc_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_collation_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_charset_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memstore_allocator_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_mgr_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_freeze_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_bad_block_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_px_worker_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_auto_increment_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_value_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_store_stat_schema(share::schema::ObTableSchema &table_schema);
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
  static int all_virtual_recyclebin_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_object_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_object_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_raid_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dtl_channel_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dtl_memory_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_role_grantee_map_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_role_grantee_map_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_keystore_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_keystore_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_policy_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_policy_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_component_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_component_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_label_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_label_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_user_level_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_user_level_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_tablespace_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_tablespace_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_information_columns_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_user_failed_login_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_profile_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_profile_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_security_audit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_security_audit_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trigger_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trigger_history_schema(share::schema::ObTableSchema &table_schema);
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
  static int all_virtual_error_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_id_service_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_object_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_monitor_statname_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_open_cursor_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_time_zone_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_time_zone_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_time_zone_transition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_time_zone_transition_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_files_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dependency_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_object_definition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_global_transaction_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ddl_task_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_deadlock_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_usage_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ctx_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_job_log_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_directory_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_directory_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_histogram_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_trigger_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_px_target_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_monitor_modified_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_stat_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_stat_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_histogram_stat_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_optstat_global_prefs_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_optstat_user_prefs_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_archive_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_archive_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_archive_piece_files_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_log_archive_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_storage_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_to_ls_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_load_data_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dam_last_arch_ts_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dam_cleanup_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_ls_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_ls_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_ls_task_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_skipped_tablet_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_skipped_tablet_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_schedule_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_to_table_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_recovery_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_ls_task_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_replica_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ddl_checksum_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ddl_error_message_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_replica_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_pending_transaction_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_job_run_detail_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_program_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_program_argument_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_context_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_context_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_global_context_value_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_unit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_election_reference_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dtl_interm_result_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_archive_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_apply_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_replay_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_routine_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_ls_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_ls_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_obj_lock_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_zone_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tx_data_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transaction_freeze_checkpoint_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transaction_checkpoint_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_checkpoint_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_set_files_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_baseline_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_baseline_item_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_spm_config_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ash_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dml_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_archive_dest_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_restore_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_restore_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_restore_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_restore_progress_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_restore_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_storage_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_policy_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_ddl_kv_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_pointer_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_storage_meta_memory_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_kvcache_store_memblock_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mock_fk_parent_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mock_fk_parent_table_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mock_fk_parent_table_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mock_fk_parent_table_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_restore_source_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_query_response_time_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_kv_ttl_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_kv_ttl_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_checksum_error_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_compaction_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_replica_task_plan_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_schema_memory_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_schema_slot_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_minor_freeze_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_show_trace_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ha_diagnose_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_dictionary_in_log_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transfer_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transfer_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_policy_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_policy_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_security_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_security_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_group_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_group_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_context_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_context_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_attribute_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_attribute_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_mysql_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_malloc_sample_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_arb_replica_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_arb_replica_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_archive_dest_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_io_scheduler_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_external_table_file_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mds_node_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mds_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dup_ls_lease_mgr_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dup_ls_tablet_set_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dup_ls_tablets_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tx_data_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_task_opt_stat_gather_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_opt_stat_gather_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_opt_stat_gather_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_thread_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_arbitration_member_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_arbitration_service_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_active_session_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_statname_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_kv_connection_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_virtual_long_ops_status_mysql_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_transfer_member_list_lock_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_timestamp_service_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_resource_pool_mysql_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_px_p2p_datahub_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_group_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_storage_leak_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_log_restore_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_snapshot_ls_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_snapshot_ls_replica_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_buffer_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mlog_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_stats_sys_defaults_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_stats_params_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_run_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_change_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_stmt_stats_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_control_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_task_helper_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_group_ls_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_cgroup_config_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_flt_config_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_job_class_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recover_table_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recover_table_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_import_table_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_import_table_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_import_table_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_import_table_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_activity_metrics_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_group_mapping_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_group_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_group_mapping_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_storage_ha_error_diagnose_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_storage_ha_perf_diagnose_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_clone_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_clone_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_checkpoint_diagnose_memtable_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_checkpoint_diagnose_checkpoint_unit_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_checkpoint_diagnose_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_running_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_aux_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_detect_lock_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_client_to_server_session_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_variable_default_value_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transfer_partition_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transfer_partition_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_snapshot_job_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dbms_lock_allocated_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_index_usage_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_snapshot_ls_replica_history_schema(share::schema::ObTableSchema &table_schema);
  static int enabled_roles_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tracepoint_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_compatibility_control_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_proxy_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_proxy_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_proxy_role_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_proxy_role_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_resource_limit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_resource_limit_detail_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_nic_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_scheduler_job_run_detail_v2_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_spatial_reference_systems_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_plan_explain_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_outline_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_privilege_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_parameter_stat_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_table_index_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_charset_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_all_table_agent_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_collation_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_processlist_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memory_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memstore_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memstore_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_session_variable_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_global_variable_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_table_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_procedure_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_tablegroup_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_privilege_grant_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_table_column_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trace_span_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_concurrent_limit_sql_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_type_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_audit_operation_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_audit_action_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_px_worker_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ps_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ps_item_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_parameter_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_history_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_active_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_histogram_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_workarea_memory_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_mgr_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_schema_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_monitor_statname_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_lock_wait_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_open_cursor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_object_definition_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_param_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_attr_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_coll_type_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_package_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_trigger_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_global_transaction_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_auto_increment_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_part_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sub_part_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_package_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_value_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_object_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_synonym_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recyclebin_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablegroup_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_column_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_attr_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_coll_type_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_param_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_keystore_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_policy_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_component_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_label_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_user_level_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_tablespace_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_profile_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_role_grantee_map_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_privilege_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_security_audit_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_trigger_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_security_audit_record_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_sysauth_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_objauth_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_error_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_def_sub_part_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_object_type_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_constraint_column_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_dependency_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_name_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_transition_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_time_zone_transition_type_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_res_mgr_plan_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_res_mgr_directive_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_lock_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_res_mgr_mapping_rule_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_encrypt_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_res_mgr_consumer_group_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_usage_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_job_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_job_log_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_directory_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_stat_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_stat_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_histogram_stat_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memory_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_trigger_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_px_target_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_monitor_modified_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_stat_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_stat_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_histogram_stat_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_optstat_global_prefs_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_optstat_user_prefs_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dam_last_arch_ts_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dam_cleanup_jobs_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_job_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_program_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_context_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_global_context_value_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_unit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_kvcache_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_compaction_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_compaction_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_compaction_diagnose_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_compaction_suggestion_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_compaction_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_meta_table_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_to_ls_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_meta_table_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_all_table_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_column_table_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dtl_interm_result_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_schema_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_partition_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_partition_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_sub_partition_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_zone_merge_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_merge_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_object_type_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_job_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_task_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_set_files_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_baseline_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_baseline_item_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_spm_config_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_event_name_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ash_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dml_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_archive_dest_parameter_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_archive_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_archive_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_archive_piece_files_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_log_archive_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_parameter_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_restore_job_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_restore_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_restore_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_restore_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_restore_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_outline_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_outline_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_storage_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_storage_info_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_job_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_task_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_backup_delete_policy_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_deadlock_event_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_replay_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_apply_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_archive_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_status_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_recovery_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_election_reference_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_freeze_info_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_replica_task_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_replica_task_plan_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_show_trace_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_privilege_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_policy_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_security_column_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_group_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_context_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_attribute_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_rewrite_rules_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_scheduler_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_arb_replica_task_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_arb_replica_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_archive_dest_status_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_external_table_file_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_dictionary_in_log_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_task_opt_stat_gather_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_opt_stat_gather_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_opt_stat_gather_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_long_ops_status_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_thread_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_active_session_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_snapshot_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_statname_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_sysstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_arbitration_member_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_arbitration_service_status_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_obj_lock_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_log_restore_source_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_job_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_job_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_task_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_balance_task_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transfer_task_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transfer_task_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_resource_pool_sys_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_px_p2p_datahub_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_timestamp_service_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_log_restore_status_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_parameter_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mlog_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_stats_sys_defaults_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_stats_params_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_run_stats_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_stats_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_change_stats_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mview_refresh_stmt_stats_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dbms_lock_allocated_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_wr_control_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_event_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_flt_config_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_job_run_detail_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_scheduler_job_class_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recover_table_job_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recover_table_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_import_table_job_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_import_table_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_import_table_task_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_import_table_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_cgroup_config_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_aux_stat_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_variable_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_variable_default_value_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transfer_partition_task_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_transfer_partition_task_history_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ls_snapshot_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_index_usage_info_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tracepoint_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_proxy_info_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_proxy_role_info_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_resource_limit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_resource_limit_detail_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_nic_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_scheduler_job_run_detail_v2_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_spatial_reference_systems_real_agent_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_plan_cache_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_plan_cache_plan_stat_schema(share::schema::ObTableSchema &table_schema);
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
  static int engines_schema(share::schema::ObTableSchema &table_schema);
  static int routines_schema(share::schema::ObTableSchema &table_schema);
  static int profiling_schema(share::schema::ObTableSchema &table_schema);
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
  static int gv_ob_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int gv_latch_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_memory_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_memory_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_memstore_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_memstore_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_plan_cache_stat_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_plan_cache_plan_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_plan_cache_plan_explain_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_plan_cache_plan_explain_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int v_latch_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_rpc_outgoing_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_rpc_outgoing_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_rpc_incoming_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_rpc_incoming_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_plan_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_plan_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int dba_recyclebin_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_name_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_transition_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_transition_type_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_longops_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_longops_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_sequence_objects_schema(share::schema::ObTableSchema &table_schema);
  static int columns_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_px_worker_stat_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_px_worker_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_ps_stat_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ps_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_ps_item_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ps_item_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_active_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_active_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_histogram_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_histogram_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_sql_workarea_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sql_workarea_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_plan_cache_reference_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_plan_cache_reference_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_sstables_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sstables_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_restore_progress_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_restore_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_server_schema_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_server_schema_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_monitor_statname_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_encrypted_tables_schema(share::schema::ObTableSchema &table_schema);
  static int v_encrypted_tablespaces_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_archivelog_piece_files_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_set_files_schema(share::schema::ObTableSchema &table_schema);
  static int connection_control_failed_login_attempts_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tenant_memory_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tenant_memory_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_px_target_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_px_target_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int column_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int view_table_usage_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int files_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tenants_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_units_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_unit_configs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_resource_pools_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_servers_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_zones_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_rootservice_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tenant_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_unit_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_server_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_locations_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_ls_locations_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablet_to_ls_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_tablet_to_ls_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablet_replicas_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_tablet_replicas_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablegroups_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_tablegroups_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablegroup_partitions_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_tablegroup_partitions_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablegroup_subpartitions_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_tablegroup_subpartitions_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_databases_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_databases_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablegroup_tables_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_tablegroup_tables_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_zone_major_compaction_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_zone_major_compaction_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_major_compaction_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_major_compaction_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_objects_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_tables_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_tab_cols_v_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_tab_cols_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ind_columns_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_part_tables_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_tab_partitions_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_tab_subpartitions_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_subpartition_templates_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_part_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_subpart_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_part_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ind_partitions_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ind_subpartitions_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_tab_col_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int dba_objects_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_tables_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int dba_subpart_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_partitions_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_subpartitions_schema(share::schema::ObTableSchema &table_schema);
  static int dba_subpartition_templates_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ind_partitions_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ind_subpartitions_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_servers_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_servers_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_units_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_units_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_parameters_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_parameters_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_processlist_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_processlist_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_kvcache_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_kvcache_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_transaction_participants_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_transaction_participants_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_compaction_progress_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_compaction_progress_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tablet_compaction_progress_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tablet_compaction_progress_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tablet_compaction_history_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tablet_compaction_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_compaction_diagnose_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_compaction_diagnose_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_compaction_suggestions_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_compaction_suggestions_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_dtl_interm_result_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_dtl_interm_result_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_io_calibration_status_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_io_calibration_status_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_io_benchmark_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_io_benchmark_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_delete_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_delete_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_delete_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_delete_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_delete_policy_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_storage_info_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_col_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_col_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int dba_subpart_col_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_histograms_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_histograms_schema(share::schema::ObTableSchema &table_schema);
  static int dba_subpart_histograms_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_stats_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ind_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_set_files_schema(share::schema::ObTableSchema &table_schema);
  static int dba_sql_plan_baselines_schema(share::schema::ObTableSchema &table_schema);
  static int dba_sql_management_config_schema(share::schema::ObTableSchema &table_schema);
  static int gv_active_session_history_schema(share::schema::ObTableSchema &table_schema);
  static int v_active_session_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_dml_stats_schema(share::schema::ObTableSchema &table_schema);
  static int v_dml_stats_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_modifications_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_outline_concurrent_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_storage_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_storage_info_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_storage_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_policy_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_outlines_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_concurrent_limit_sql_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_restore_progress_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_restore_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_archive_dest_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_archivelog_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_archivelog_summary_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_archivelog_piece_files_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_archive_dest_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_archivelog_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_archivelog_summary_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_backup_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_deadlock_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_deadlock_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_sys_variables_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_kv_ttl_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_kv_ttl_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_log_stat_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_log_stat_schema(share::schema::ObTableSchema &table_schema);
  static int st_geometry_columns_schema(share::schema::ObTableSchema &table_schema);
  static int st_spatial_reference_systems_schema(share::schema::ObTableSchema &table_schema);
  static int query_response_time_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_kv_ttl_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_kv_ttl_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_plans_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_plan_directives_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_group_mappings_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_consumer_groups_schema(share::schema::ObTableSchema &table_schema);
  static int v_rsrc_plan_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_column_checksum_error_info_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_tablet_checksum_error_info_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_ls_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_table_locations_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_table_locations_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_server_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_freeze_info_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_freeze_info_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_replica_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_ls_replica_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ls_replica_task_plan_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_auto_increment_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_auto_increment_schema(share::schema::ObTableSchema &table_schema);
  static int dba_sequences_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_windows_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_users_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_users_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_database_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_database_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_user_defined_rules_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_sql_plan_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sql_plan_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_cluster_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int parameters_schema(share::schema::ObTableSchema &table_schema);
  static int table_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int user_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int schema_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int check_constraints_schema(share::schema::ObTableSchema &table_schema);
  static int referential_constraints_schema(share::schema::ObTableSchema &table_schema);
  static int table_constraints_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_transaction_schedulers_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_transaction_schedulers_schema(share::schema::ObTableSchema &table_schema);
  static int triggers_schema(share::schema::ObTableSchema &table_schema);
  static int partitions_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_arbitration_service_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_ls_arb_replica_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_arb_replica_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_ls_arb_replica_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_arb_replica_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_archive_dest_status_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_log_archive_progress_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_ls_log_archive_progress_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_rsrc_io_directives_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tablet_stats_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tablet_stats_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_access_point_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_access_point_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_data_dictionary_in_log_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_data_dictionary_in_log_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_opt_stat_gather_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_opt_stat_gather_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_task_opt_stat_gather_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_table_opt_stat_gather_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_thread_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_thread_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_arbitration_member_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_arbitration_member_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_arbitration_service_status_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_arbitration_service_status_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_active_session_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_wr_active_session_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_wr_snapshot_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_statname_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_wr_statname_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_wr_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_kv_connections_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_kv_connections_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_locks_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_locks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_log_restore_source_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_log_restore_source_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_timestamp_service_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_balance_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_balance_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_balance_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_balance_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_balance_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_balance_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_balance_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_balance_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_transfer_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_transfer_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_transfer_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_transfer_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_external_table_files_schema(share::schema::ObTableSchema &table_schema);
  static int all_ob_external_table_files_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_px_p2p_datahub_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_px_p2p_datahub_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_join_filter_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_join_filter_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_table_stat_stale_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ls_log_restore_status_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_external_table_files_schema(share::schema::ObTableSchema &table_schema);
  static int dba_db_links_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_control_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_wr_control_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_ls_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tenant_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_tenant_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_flt_trace_config_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_session_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_session_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_pl_cache_object_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_pl_cache_object_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_recover_table_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_recover_table_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_recover_table_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_recover_table_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_import_table_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_import_table_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_import_table_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_import_table_job_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_import_table_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_import_table_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_import_table_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_import_table_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tenant_runtime_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tenant_runtime_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_cgroup_config_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_cgroup_config_schema(share::schema::ObTableSchema &table_schema);
  static int procs_priv_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_aux_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_aux_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int dba_index_usage_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_sys_variables_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_transfer_partition_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_transfer_partition_tasks_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_transfer_partition_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_ob_transfer_partition_task_history_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_trusted_root_certificate_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_clone_progress_schema(share::schema::ObTableSchema &table_schema);
  static int role_edges_schema(share::schema::ObTableSchema &table_schema);
  static int default_roles_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_index_usage_schema(share::schema::ObTableSchema &table_schema);
  static int columns_priv_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_ls_snapshots_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ls_snapshots_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_clone_history_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_mview_logs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mview_logs_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_mviews_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mviews_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_mvref_stats_sys_defaults_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_stats_sys_defaults_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_mvref_stats_params_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_stats_params_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_mvref_run_stats_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_run_stats_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_mvref_stats_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_stats_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_mvref_change_stats_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_change_stats_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_mvref_stmt_stats_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_stmt_stats_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tracepoint_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tracepoint_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_compatibility_control_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tenant_resource_limit_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tenant_resource_limit_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tenant_resource_limit_detail_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tenant_resource_limit_detail_schema(share::schema::ObTableSchema &table_schema);
  static int tablespaces_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_buffer_page_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_buffer_page_lru_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_buffer_pool_stats_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_cmp_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_cmp_per_index_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_cmp_per_index_reset_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_cmp_reset_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_cmpmem_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_cmpmem_reset_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_sys_datafiles_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_sys_indexes_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_sys_tables_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_sys_tablespaces_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_sys_tablestats_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_sys_virtual_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_temp_table_info_schema(share::schema::ObTableSchema &table_schema);
  static int innodb_metrics_schema(share::schema::ObTableSchema &table_schema);
  static int events_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_nic_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_nic_info_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_job_run_details_schema(share::schema::ObTableSchema &table_schema);
  static int cdb_scheduler_job_run_details_schema(share::schema::ObTableSchema &table_schema);
  static int dba_synonyms_schema(share::schema::ObTableSchema &table_schema);
  static int dba_objects_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_objects_schema(share::schema::ObTableSchema &table_schema);
  static int user_objects_schema(share::schema::ObTableSchema &table_schema);
  static int dba_sequences_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequences_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_sequences_ora_schema(share::schema::ObTableSchema &table_schema);
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
  static int dba_part_key_columns_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int user_part_key_columns_schema(share::schema::ObTableSchema &table_schema);
  static int dba_subpart_key_columns_ora_schema(share::schema::ObTableSchema &table_schema);
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
  static int dba_part_indexes_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_indexes_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_part_indexes_ora_schema(share::schema::ObTableSchema &table_schema);
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
  static int dba_ind_partitions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ind_subpartitions_ora_schema(share::schema::ObTableSchema &table_schema);
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
  static int audit_actions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int stmt_audit_option_map_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_audit_opts_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_stmt_audit_opts_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_obj_audit_opts_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_audit_trail_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_audit_trail_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_audit_exists_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_audit_session_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_audit_session_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_audit_statement_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_audit_statement_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_audit_object_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_audit_object_ora_schema(share::schema::ObTableSchema &table_schema);
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
  static int dba_rsrc_plans_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_plan_directives_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_group_mappings_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_recyclebin_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_recyclebin_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_rsrc_consumer_groups_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_locations_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablet_to_ls_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablet_replicas_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablegroups_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablegroup_partitions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablegroup_subpartitions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_databases_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tablegroup_tables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_zone_major_compaction_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_major_compaction_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_ind_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ind_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_ind_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_jobs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_tasks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_set_files_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_modifications_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_modifications_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_modifications_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_storage_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_storage_info_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_policy_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_jobs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_tasks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_delete_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_restore_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_restore_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_archive_dest_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_archivelog_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_archivelog_summary_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_archivelog_piece_files_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_backup_parameter_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_freeze_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_replica_tasks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ls_replica_task_plan_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_scheduler_windows_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_windows_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_database_privilege_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tenants_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_policies_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_policies_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_policies_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_policy_groups_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_policy_groups_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_policy_groups_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_policy_contexts_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_policy_contexts_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_policy_contexts_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_sec_relevant_cols_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_sec_relevant_cols_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_sec_relevant_cols_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_arb_replica_tasks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_arb_replica_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_rsrc_io_directives_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_db_links_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_db_links_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_db_links_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_task_opt_stat_gather_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_table_opt_stat_gather_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_active_session_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_snapshot_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_statname_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_sysstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_log_restore_source_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_external_table_files_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_ob_external_table_files_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_balance_jobs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_balance_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_balance_tasks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_balance_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_transfer_tasks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_transfer_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_px_p2p_datahub_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_px_p2p_datahub_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_join_filter_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_join_filter_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_table_stat_stale_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dbms_lock_allocated_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_wr_control_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_tenant_event_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_job_run_details_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_job_classes_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_recover_table_jobs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_recover_table_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_import_table_jobs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_import_table_job_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_import_table_tasks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_import_table_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_transfer_partition_tasks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_transfer_partition_task_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_users_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mview_logs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_logs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_mview_logs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mviews_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_mviews_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_mviews_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_stats_sys_defaults_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_mvref_stats_sys_defaults_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_stats_params_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_mvref_stats_params_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_run_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_mvref_run_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_mvref_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_change_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_mvref_change_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_mvref_stmt_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_mvref_stmt_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int proxy_users_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_sql_audit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sql_audit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_instance_schema(share::schema::ObTableSchema &table_schema);
  static int v_instance_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_plan_cache_plan_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_plan_cache_plan_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_plan_cache_plan_explain_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_plan_cache_plan_explain_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_wait_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_wait_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_wait_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_wait_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_memory_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_memory_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_memstore_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_memstore_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_memstore_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_memstore_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sesstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sesstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sysstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sysstat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_system_event_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_system_event_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_plan_cache_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_plan_cache_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int nls_session_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int nls_instance_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int nls_database_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_nls_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_version_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_px_worker_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_px_worker_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_ps_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ps_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_ps_item_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ps_item_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_active_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_active_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_histogram_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_histogram_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_sql_workarea_memory_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sql_workarea_memory_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_plan_cache_reference_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_plan_cache_reference_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_workarea_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_workarea_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_sstables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sstables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_server_schema_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_server_schema_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_plan_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_plan_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_monitor_statname_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_open_cursor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_open_cursor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_timezone_names_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_global_transaction_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_global_transaction_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_rsrc_plan_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_encrypted_tables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_encrypted_tablespaces_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_col_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_col_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_col_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_col_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_col_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_part_col_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_subpart_col_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_subpart_col_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_subpart_col_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_histograms_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_histograms_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_histograms_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_histograms_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_part_histograms_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_part_histograms_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_subpart_histograms_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_subpart_histograms_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_subpart_histograms_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int user_jobs_schema(share::schema::ObTableSchema &table_schema);
  static int dba_jobs_running_schema(share::schema::ObTableSchema &table_schema);
  static int all_directories_schema(share::schema::ObTableSchema &table_schema);
  static int dba_directories_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tenant_memory_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tenant_memory_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_px_target_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_px_target_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_tab_stats_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_tab_stats_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int user_tab_stats_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_dblink_schema(share::schema::ObTableSchema &table_schema);
  static int v_dblink_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_jobs_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_scheduler_program_schema(share::schema::ObTableSchema &table_schema);
  static int dba_context_schema(share::schema::ObTableSchema &table_schema);
  static int v_globalcontext_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_units_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_units_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_parameters_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_processlist_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_processlist_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_kvcache_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_kvcache_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_transaction_participants_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_transaction_participants_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_compaction_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_compaction_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tablet_compaction_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tablet_compaction_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tablet_compaction_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tablet_compaction_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_compaction_diagnose_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_compaction_diagnose_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_compaction_suggestions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_compaction_suggestions_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_dtl_interm_result_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_dtl_interm_result_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_sql_plan_baselines_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_sql_management_config_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_event_name_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_active_session_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_active_session_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_dml_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_dml_stats_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_outline_concurrent_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_outlines_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_concurrent_limit_sql_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_deadlock_event_history_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_log_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_log_stat_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_global_transaction_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_global_transaction_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_table_locations_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_trigger_ordering_schema(share::schema::ObTableSchema &table_schema);
  static int dba_trigger_ordering_schema(share::schema::ObTableSchema &table_schema);
  static int user_trigger_ordering_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_transaction_schedulers_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_transaction_schedulers_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_user_defined_rules_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_sql_plan_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_sql_plan_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_archive_dest_status_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_ls_log_archive_progress_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_locks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_locks_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_access_point_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_data_dictionary_in_log_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_opt_stat_gather_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_opt_stat_gather_monitor_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_longops_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_longops_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_thread_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_thread_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_arbitration_member_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_arbitration_member_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_arbitration_service_status_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_arbitration_service_status_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_timestamp_service_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ls_log_restore_status_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_flt_trace_config_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_session_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_session_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_pl_cache_object_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_pl_cache_object_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_cgroup_config_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_cgroup_config_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_aux_statistics_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_sys_variables_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_index_usage_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_ls_snapshots_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_ls_snapshots_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tracepoint_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tracepoint_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tenant_resource_limit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tenant_resource_limit_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_tenant_resource_limit_detail_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_tenant_resource_limit_detail_ora_schema(share::schema::ObTableSchema &table_schema);
  static int gv_ob_nic_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int v_ob_nic_info_ora_schema(share::schema::ObTableSchema &table_schema);
  static int dba_ob_spatial_columns_ora_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_operation_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablegroup_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablegroup_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_privilege_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_privilege_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_zone_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_parameter_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_parameter_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_variable_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_stat_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_config_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_resource_pool_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_charset_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_collation_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int help_topic_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int help_category_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int help_keyword_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int help_relation_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_dummy_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_event_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_privilege_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_recyclebin_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_sub_part_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_sub_part_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_info_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_sub_part_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_sub_part_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_event_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_variable_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_job_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_id_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_column_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_column_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_auto_increment_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_checksum_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_param_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_param_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_acquired_snapshot_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraint_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraint_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ori_schema_version_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_func_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_func_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_temp_table_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_value_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_freeze_schema_version_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attr_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attr_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_type_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_type_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_weak_read_service_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_policy_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_policy_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_component_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_component_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_label_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_label_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_user_level_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_user_level_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_tablespace_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_tablespace_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_user_failed_login_stat_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_profile_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_profile_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_seed_parameter_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_record_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_sysauth_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_sysauth_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_error_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_progress_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_name_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_transition_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_transition_type_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_constraint_column_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_constraint_column_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_global_transaction_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_dependency_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_plan_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_directive_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_mapping_rule_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_error_message_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_space_usage_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_set_files_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_consumer_group_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_task_status_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_region_network_bandwidth_limit_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_deadlock_event_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_usage_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_job_log_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_directory_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_directory_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_stat_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_stat_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_histogram_stat_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_monitor_modified_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_stat_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_stat_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_histogram_stat_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_optstat_global_prefs_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_optstat_user_prefs_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_meta_table_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_to_ls_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_meta_table_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_status_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_progress_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_piece_files_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_log_archive_progress_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_storage_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_dam_last_arch_ts_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_dam_cleanup_jobs_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_job_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_skipped_tablet_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_skipped_tablet_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_to_table_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_recovery_stat_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_info_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_replica_checksum_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_checksum_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_replica_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_pending_transaction_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_group_ls_stat_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_job_run_detail_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_program_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_program_argument_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_context_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_context_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_global_context_value_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_election_reference_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_job_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_ls_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_ls_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_zone_merge_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_merge_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_freeze_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_disk_io_calibration_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_plan_baseline_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_plan_baseline_item_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_spm_config_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_dest_parameter_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_parameter_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_restore_progress_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_restore_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_storage_info_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_policy_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_column_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_column_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_restore_source_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_kv_ttl_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_kv_ttl_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_service_epoch_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_spatial_reference_systems_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_checksum_error_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_mapping_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_mapping_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_job_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_arbitration_service_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_arb_replica_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_data_dictionary_in_log_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_arb_replica_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_policy_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_policy_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_security_column_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_security_column_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_group_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_group_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_context_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_context_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_attribute_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_attribute_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_rewrite_rules_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_reserved_snapshot_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_cluster_event_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_transfer_member_list_lock_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_external_table_file_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_task_opt_stat_gather_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_opt_stat_gather_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int wr_active_session_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int wr_snapshot_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int wr_statname_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int wr_sysstat_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_task_helper_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_ls_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_ls_replica_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mlog_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_sys_defaults_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_params_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_run_stats_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_change_stats_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stmt_stats_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_dbms_lock_allocated_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int wr_control_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_event_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_job_class_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_recover_table_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_recover_table_job_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_job_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_storage_ha_error_diagnose_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_storage_ha_perf_diagnose_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_clone_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_clone_job_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_privilege_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_privilege_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_ncomp_dll_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_aux_stat_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_index_usage_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_detect_lock_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_client_to_server_session_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_partition_task_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_partition_task_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_job_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_trusted_root_certificate_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_privilege_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_privilege_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_ls_replica_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_info_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_role_info_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_role_info_history_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_dep_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_scheduler_job_run_detail_v2_aux_lob_meta_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_operation_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablegroup_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablegroup_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_privilege_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_privilege_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_zone_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_parameter_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_parameter_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_variable_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_stat_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_config_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_resource_pool_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_charset_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_collation_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int help_topic_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int help_category_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int help_keyword_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int help_relation_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_dummy_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_event_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_privilege_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_recyclebin_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_sub_part_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_sub_part_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_info_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_sub_part_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_sub_part_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_event_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_variable_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_job_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_id_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_column_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_column_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_auto_increment_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_checksum_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_param_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_param_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_acquired_snapshot_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraint_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraint_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ori_schema_version_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_func_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_func_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_temp_table_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_value_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_freeze_schema_version_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attr_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attr_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_type_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_type_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_weak_read_service_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_policy_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_policy_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_component_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_component_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_label_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_label_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_user_level_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_user_level_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_tablespace_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_tablespace_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_user_failed_login_stat_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_profile_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_profile_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_seed_parameter_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_record_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_sysauth_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_sysauth_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_error_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_restore_progress_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_name_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_transition_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_time_zone_transition_type_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_constraint_column_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_constraint_column_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_global_transaction_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_dependency_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_plan_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_directive_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_mapping_rule_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_error_message_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_space_usage_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_set_files_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_res_mgr_consumer_group_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_task_status_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_region_network_bandwidth_limit_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_deadlock_event_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_usage_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_job_log_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_directory_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_directory_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_stat_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_stat_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_histogram_stat_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_monitor_modified_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_stat_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_stat_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_histogram_stat_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_optstat_global_prefs_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_optstat_user_prefs_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_meta_table_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_to_ls_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_meta_table_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_status_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_progress_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_piece_files_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_log_archive_progress_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_storage_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_dam_last_arch_ts_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_dam_cleanup_jobs_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_job_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_skipped_tablet_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_skipped_tablet_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_to_table_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_recovery_stat_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_ls_task_info_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_replica_checksum_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_checksum_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_replica_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_pending_transaction_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_group_ls_stat_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_job_run_detail_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_program_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_program_argument_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_context_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_context_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_global_context_value_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_election_reference_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_job_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_ls_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_ls_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_zone_merge_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_merge_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_freeze_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_disk_io_calibration_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_plan_baseline_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_plan_baseline_item_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_spm_config_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_dest_parameter_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_parameter_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_restore_progress_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_restore_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_storage_info_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_delete_policy_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_column_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mock_fk_parent_table_column_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_restore_source_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_kv_ttl_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_kv_ttl_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_service_epoch_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_spatial_reference_systems_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_checksum_error_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_mapping_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_group_mapping_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_job_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_arbitration_service_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_arb_replica_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_data_dictionary_in_log_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_arb_replica_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_policy_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_policy_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_security_column_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_security_column_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_group_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_group_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_context_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_context_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_attribute_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_attribute_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_rewrite_rules_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_reserved_snapshot_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_cluster_event_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ls_transfer_member_list_lock_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_external_table_file_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_task_opt_stat_gather_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_opt_stat_gather_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int wr_active_session_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int wr_snapshot_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int wr_statname_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int wr_sysstat_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_balance_task_helper_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_ls_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_ls_replica_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mlog_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_sys_defaults_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_params_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_run_stats_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_change_stats_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stmt_stats_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_dbms_lock_allocated_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int wr_control_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_event_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_scheduler_job_class_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_recover_table_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_recover_table_job_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_job_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_import_table_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_storage_ha_error_diagnose_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_storage_ha_perf_diagnose_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_clone_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_clone_job_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_privilege_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_privilege_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_ncomp_dll_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_aux_stat_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_index_usage_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_detect_lock_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_client_to_server_session_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_partition_task_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_partition_task_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_job_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_trusted_root_certificate_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_privilege_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_privilege_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_ls_replica_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_info_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_role_info_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_role_info_history_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_dep_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_scheduler_job_run_detail_v2_aux_lob_piece_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ash_all_virtual_ash_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_monitor_all_virtual_sql_plan_monitor_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_all_virtual_sql_audit_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_all_virtual_sysstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_all_virtual_sesstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_all_virtual_system_event_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_all_virtual_session_wait_history_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_all_virtual_session_wait_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_event_all_virtual_session_event_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_all_virtual_plan_cache_stat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_real_agent_ora_idx_data_table_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_real_agent_ora_idx_db_tb_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_real_agent_ora_idx_tb_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_real_agent_ora_idx_tb_column_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_real_agent_ora_idx_column_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_real_agent_ora_idx_ur_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_real_agent_ora_idx_db_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablegroup_real_agent_ora_idx_tg_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recyclebin_real_agent_ora_idx_recyclebin_db_type_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_part_real_agent_ora_idx_part_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sub_part_real_agent_ora_idx_sub_part_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_def_sub_part_real_agent_ora_idx_def_sub_part_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_real_agent_ora_idx_fk_child_tid_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_real_agent_ora_idx_fk_parent_tid_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_foreign_key_real_agent_ora_idx_fk_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_synonym_real_agent_ora_idx_db_synonym_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_synonym_real_agent_ora_idx_synonym_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_real_agent_ora_idx_db_routine_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_real_agent_ora_idx_routine_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_real_agent_ora_idx_routine_pkg_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_routine_param_real_agent_ora_idx_routine_param_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_package_real_agent_ora_idx_db_pkg_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_package_real_agent_ora_idx_pkg_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_constraint_real_agent_ora_idx_cst_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_real_agent_ora_idx_db_type_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_real_agent_ora_idx_type_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_type_attr_real_agent_ora_idx_type_attr_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_coll_type_real_agent_ora_idx_coll_name_type_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_real_agent_ora_idx_owner_dblink_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dblink_real_agent_ora_idx_dblink_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_role_grantee_map_real_agent_ora_idx_grantee_role_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_keystore_real_agent_ora_idx_keystore_master_key_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_policy_real_agent_ora_idx_ols_policy_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_policy_real_agent_ora_idx_ols_policy_col_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_component_real_agent_ora_idx_ols_com_policy_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_label_real_agent_ora_idx_ols_lab_policy_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_label_real_agent_ora_idx_ols_lab_tag_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_label_real_agent_ora_idx_ols_lab_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_user_level_real_agent_ora_idx_ols_level_uid_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_ols_user_level_real_agent_ora_idx_ols_level_policy_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_profile_real_agent_ora_idx_profile_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_security_audit_real_agent_ora_idx_audit_type_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_trigger_real_agent_ora_idx_trigger_base_obj_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_trigger_real_agent_ora_idx_db_trigger_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_trigger_real_agent_ora_idx_trigger_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_objauth_real_agent_ora_idx_objauth_grantor_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_objauth_real_agent_ora_idx_objauth_grantee_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_object_type_real_agent_ora_idx_obj_type_db_obj_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_object_type_real_agent_ora_idx_obj_type_obj_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_dependency_real_agent_ora_idx_dependency_ref_obj_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_stat_history_real_agent_ora_idx_table_stat_his_savtime_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_column_stat_history_real_agent_ora_idx_column_stat_his_savtime_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_histogram_stat_history_real_agent_ora_idx_histogram_stat_his_savtime_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_to_ls_real_agent_ora_idx_tablet_to_ls_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tablet_to_ls_real_agent_ora_idx_tablet_to_table_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_context_real_agent_ora_idx_ctx_namespace_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_baseline_item_real_agent_ora_idx_spm_item_sql_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_baseline_item_real_agent_ora_idx_spm_item_value_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_directory_real_agent_ora_idx_directory_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_job_real_agent_ora_idx_job_powner_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_object_real_agent_ora_idx_seq_obj_db_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sequence_object_real_agent_ora_idx_seq_obj_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_recyclebin_real_agent_ora_idx_recyclebin_ori_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_privilege_real_agent_ora_idx_tb_priv_db_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_table_privilege_real_agent_ora_idx_tb_priv_tb_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_database_privilege_real_agent_ora_idx_db_priv_db_name_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_policy_real_agent_ora_idx_rls_policy_table_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_policy_real_agent_ora_idx_rls_policy_group_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_group_real_agent_ora_idx_rls_group_table_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rls_context_real_agent_ora_idx_rls_context_table_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dbms_lock_allocated_real_agent_ora_idx_dbms_lock_allocated_lockhandle_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_dbms_lock_allocated_real_agent_ora_idx_dbms_lock_allocated_expiration_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_user_proxy_info_real_agent_ora_idx_user_proxy_info_proxy_user_id_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_scheduler_job_run_detail_v2_real_agent_ora_idx_scheduler_job_run_detail_v2_time_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_scheduler_job_run_detail_v2_real_agent_ora_idx_scheduler_job_run_detail_v2_job_class_time_real_agent_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_ash_ora_all_virtual_ash_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_monitor_ora_all_virtual_sql_plan_monitor_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_ora_all_virtual_system_event_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_ora_all_virtual_sysstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_ora_all_virtual_sesstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_ora_all_virtual_session_wait_history_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_ora_all_virtual_session_wait_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_ora_all_virtual_plan_cache_stat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_ora_all_virtual_sql_audit_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_idx_data_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_idx_db_tb_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_idx_tb_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_idx_tb_column_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_idx_column_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_operation_idx_ddl_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_history_idx_data_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_log_archive_piece_files_idx_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_backup_set_files_idx_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_task_status_idx_task_key_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_idx_ur_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_idx_db_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablegroup_idx_tg_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_history_idx_tenant_deleted_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_event_history_idx_rs_module_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_event_history_idx_rs_event_schema(share::schema::ObTableSchema &table_schema);
  static int all_recyclebin_idx_recyclebin_db_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_idx_part_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_sub_part_idx_sub_part_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_sub_part_idx_def_sub_part_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_event_history_idx_server_module_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_event_history_idx_server_event_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_job_idx_rs_job_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_idx_fk_child_tid_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_idx_fk_parent_tid_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_idx_fk_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_history_idx_fk_his_child_tid_schema(share::schema::ObTableSchema &table_schema);
  static int all_foreign_key_history_idx_fk_his_parent_tid_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_idx_db_synonym_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_synonym_idx_synonym_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_checksum_idx_ddl_checksum_task_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_idx_db_routine_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_idx_routine_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_idx_routine_pkg_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_routine_param_idx_routine_param_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_idx_db_pkg_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_package_idx_pkg_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_acquired_snapshot_idx_snapshot_tablet_schema(share::schema::ObTableSchema &table_schema);
  static int all_constraint_idx_cst_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_idx_db_type_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_idx_type_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_type_attr_idx_type_attr_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_coll_type_idx_coll_name_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_idx_owner_dblink_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_dblink_idx_dblink_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_idx_grantee_role_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_role_grantee_map_history_idx_grantee_his_role_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_idx_keystore_master_key_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_keystore_history_idx_keystore_his_master_key_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_policy_idx_ols_policy_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_policy_idx_ols_policy_col_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_component_idx_ols_com_policy_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_label_idx_ols_lab_policy_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_label_idx_ols_lab_tag_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_label_idx_ols_lab_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_user_level_idx_ols_level_uid_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_ols_user_level_idx_ols_level_policy_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_profile_idx_profile_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_security_audit_idx_audit_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_idx_trigger_base_obj_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_idx_db_trigger_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_idx_trigger_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_trigger_history_idx_trigger_his_base_obj_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_idx_objauth_grantor_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_objauth_idx_objauth_grantee_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_idx_obj_type_db_obj_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_object_type_idx_obj_type_obj_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_global_transaction_idx_xa_trans_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_dependency_idx_dependency_ref_obj_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_error_message_idx_ddl_error_object_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_stat_history_idx_table_stat_his_savtime_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_stat_history_idx_column_stat_his_savtime_schema(share::schema::ObTableSchema &table_schema);
  static int all_histogram_stat_history_idx_histogram_stat_his_savtime_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_to_ls_idx_tablet_to_ls_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablet_to_ls_idx_tablet_to_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_pending_transaction_idx_pending_tx_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_context_idx_ctx_namespace_schema(share::schema::ObTableSchema &table_schema);
  static int all_plan_baseline_item_idx_spm_item_sql_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_plan_baseline_item_idx_spm_item_value_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_directory_idx_directory_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_job_idx_job_powner_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_idx_seq_obj_db_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_object_idx_seq_obj_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_recyclebin_idx_recyclebin_ori_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_idx_tb_priv_db_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_idx_tb_priv_tb_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_privilege_idx_db_priv_db_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_policy_idx_rls_policy_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_policy_idx_rls_policy_group_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_policy_history_idx_rls_policy_his_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_group_idx_rls_group_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_group_history_idx_rls_group_his_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_context_idx_rls_context_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_rls_context_history_idx_rls_context_his_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_snapshot_idx_tenant_snapshot_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_dbms_lock_allocated_idx_dbms_lock_allocated_lockhandle_schema(share::schema::ObTableSchema &table_schema);
  static int all_dbms_lock_allocated_idx_dbms_lock_allocated_expiration_schema(share::schema::ObTableSchema &table_schema);
  static int all_kv_ttl_task_idx_kv_ttl_task_table_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_kv_ttl_task_history_idx_kv_ttl_task_history_upd_time_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_run_stats_idx_mview_refresh_run_stats_num_mvs_current_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_idx_mview_refresh_stats_end_time_schema(share::schema::ObTableSchema &table_schema);
  static int all_mview_refresh_stats_idx_mview_refresh_stats_mview_end_time_schema(share::schema::ObTableSchema &table_schema);
  static int all_transfer_partition_task_idx_transfer_partition_key_schema(share::schema::ObTableSchema &table_schema);
  static int all_client_to_server_session_info_idx_client_to_server_session_info_client_session_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_privilege_idx_column_privilege_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_info_idx_user_proxy_info_proxy_user_id_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_info_history_idx_user_proxy_info_proxy_user_id_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_proxy_role_info_history_idx_user_proxy_role_info_proxy_user_id_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_scheduler_job_run_detail_v2_idx_scheduler_job_run_detail_v2_time_schema(share::schema::ObTableSchema &table_schema);
  static int all_scheduler_job_run_detail_v2_idx_scheduler_job_run_detail_v2_job_class_time_schema(share::schema::ObTableSchema &table_schema);

private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerTableSchema);
};

typedef int (*schema_create_func)(share::schema::ObTableSchema &table_schema);

const schema_create_func all_core_table_schema_creator [] = {
  ObInnerTableSchema::all_core_table_schema,
  NULL,};

const schema_create_func core_table_schema_creators [] = {
  ObInnerTableSchema::all_table_schema,
  ObInnerTableSchema::all_column_schema,
  ObInnerTableSchema::all_ddl_operation_schema,
  NULL,};

const schema_create_func sys_table_schema_creators [] = {
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
  ObInnerTableSchema::all_unit_schema,
  ObInnerTableSchema::all_unit_config_schema,
  ObInnerTableSchema::all_resource_pool_schema,
  ObInnerTableSchema::all_charset_schema,
  ObInnerTableSchema::all_collation_schema,
  ObInnerTableSchema::help_topic_schema,
  ObInnerTableSchema::help_category_schema,
  ObInnerTableSchema::help_keyword_schema,
  ObInnerTableSchema::help_relation_schema,
  ObInnerTableSchema::all_dummy_schema,
  ObInnerTableSchema::all_rootservice_event_history_schema,
  ObInnerTableSchema::all_privilege_schema,
  ObInnerTableSchema::all_outline_schema,
  ObInnerTableSchema::all_outline_history_schema,
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
  ObInnerTableSchema::all_sys_variable_history_schema,
  ObInnerTableSchema::all_restore_job_schema,
  ObInnerTableSchema::all_restore_job_history_schema,
  ObInnerTableSchema::all_ddl_id_schema,
  ObInnerTableSchema::all_foreign_key_schema,
  ObInnerTableSchema::all_foreign_key_history_schema,
  ObInnerTableSchema::all_foreign_key_column_schema,
  ObInnerTableSchema::all_foreign_key_column_history_schema,
  ObInnerTableSchema::all_synonym_schema,
  ObInnerTableSchema::all_synonym_history_schema,
  ObInnerTableSchema::all_auto_increment_schema,
  ObInnerTableSchema::all_ddl_checksum_schema,
  ObInnerTableSchema::all_routine_schema,
  ObInnerTableSchema::all_routine_history_schema,
  ObInnerTableSchema::all_routine_param_schema,
  ObInnerTableSchema::all_routine_param_history_schema,
  ObInnerTableSchema::all_package_schema,
  ObInnerTableSchema::all_package_history_schema,
  ObInnerTableSchema::all_acquired_snapshot_schema,
  ObInnerTableSchema::all_constraint_schema,
  ObInnerTableSchema::all_constraint_history_schema,
  ObInnerTableSchema::all_ori_schema_version_schema,
  ObInnerTableSchema::all_func_schema,
  ObInnerTableSchema::all_func_history_schema,
  ObInnerTableSchema::all_temp_table_schema,
  ObInnerTableSchema::all_sequence_object_schema,
  ObInnerTableSchema::all_sequence_object_history_schema,
  ObInnerTableSchema::all_sequence_value_schema,
  ObInnerTableSchema::all_freeze_schema_version_schema,
  ObInnerTableSchema::all_type_schema,
  ObInnerTableSchema::all_type_history_schema,
  ObInnerTableSchema::all_type_attr_schema,
  ObInnerTableSchema::all_type_attr_history_schema,
  ObInnerTableSchema::all_coll_type_schema,
  ObInnerTableSchema::all_coll_type_history_schema,
  ObInnerTableSchema::all_weak_read_service_schema,
  ObInnerTableSchema::all_dblink_schema,
  ObInnerTableSchema::all_dblink_history_schema,
  ObInnerTableSchema::all_tenant_role_grantee_map_schema,
  ObInnerTableSchema::all_tenant_role_grantee_map_history_schema,
  ObInnerTableSchema::all_tenant_keystore_schema,
  ObInnerTableSchema::all_tenant_keystore_history_schema,
  ObInnerTableSchema::all_tenant_ols_policy_schema,
  ObInnerTableSchema::all_tenant_ols_policy_history_schema,
  ObInnerTableSchema::all_tenant_ols_component_schema,
  ObInnerTableSchema::all_tenant_ols_component_history_schema,
  ObInnerTableSchema::all_tenant_ols_label_schema,
  ObInnerTableSchema::all_tenant_ols_label_history_schema,
  ObInnerTableSchema::all_tenant_ols_user_level_schema,
  ObInnerTableSchema::all_tenant_ols_user_level_history_schema,
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
  ObInnerTableSchema::all_tenant_security_audit_record_schema,
  ObInnerTableSchema::all_tenant_sysauth_schema,
  ObInnerTableSchema::all_tenant_sysauth_history_schema,
  ObInnerTableSchema::all_tenant_objauth_schema,
  ObInnerTableSchema::all_tenant_objauth_history_schema,
  ObInnerTableSchema::all_restore_info_schema,
  ObInnerTableSchema::all_tenant_error_schema,
  ObInnerTableSchema::all_restore_progress_schema,
  ObInnerTableSchema::all_tenant_object_type_schema,
  ObInnerTableSchema::all_tenant_object_type_history_schema,
  ObInnerTableSchema::all_tenant_time_zone_schema,
  ObInnerTableSchema::all_tenant_time_zone_name_schema,
  ObInnerTableSchema::all_tenant_time_zone_transition_schema,
  ObInnerTableSchema::all_tenant_time_zone_transition_type_schema,
  ObInnerTableSchema::all_tenant_constraint_column_schema,
  ObInnerTableSchema::all_tenant_constraint_column_history_schema,
  ObInnerTableSchema::all_tenant_global_transaction_schema,
  ObInnerTableSchema::all_tenant_dependency_schema,
  ObInnerTableSchema::all_res_mgr_plan_schema,
  ObInnerTableSchema::all_res_mgr_directive_schema,
  ObInnerTableSchema::all_res_mgr_mapping_rule_schema,
  ObInnerTableSchema::all_ddl_error_message_schema,
  ObInnerTableSchema::all_space_usage_schema,
  ObInnerTableSchema::all_backup_set_files_schema,
  ObInnerTableSchema::all_res_mgr_consumer_group_schema,
  ObInnerTableSchema::all_backup_info_schema,
  ObInnerTableSchema::all_ddl_task_status_schema,
  ObInnerTableSchema::all_region_network_bandwidth_limit_schema,
  ObInnerTableSchema::all_deadlock_event_history_schema,
  ObInnerTableSchema::all_column_usage_schema,
  ObInnerTableSchema::all_job_schema,
  ObInnerTableSchema::all_job_log_schema,
  ObInnerTableSchema::all_tenant_directory_schema,
  ObInnerTableSchema::all_tenant_directory_history_schema,
  ObInnerTableSchema::all_table_stat_schema,
  ObInnerTableSchema::all_column_stat_schema,
  ObInnerTableSchema::all_histogram_stat_schema,
  ObInnerTableSchema::all_monitor_modified_schema,
  ObInnerTableSchema::all_table_stat_history_schema,
  ObInnerTableSchema::all_column_stat_history_schema,
  ObInnerTableSchema::all_histogram_stat_history_schema,
  ObInnerTableSchema::all_optstat_global_prefs_schema,
  ObInnerTableSchema::all_optstat_user_prefs_schema,
  ObInnerTableSchema::all_ls_meta_table_schema,
  ObInnerTableSchema::all_tablet_to_ls_schema,
  ObInnerTableSchema::all_tablet_meta_table_schema,
  ObInnerTableSchema::all_ls_status_schema,
  ObInnerTableSchema::all_log_archive_progress_schema,
  ObInnerTableSchema::all_log_archive_history_schema,
  ObInnerTableSchema::all_log_archive_piece_files_schema,
  ObInnerTableSchema::all_ls_log_archive_progress_schema,
  ObInnerTableSchema::all_ls_schema,
  ObInnerTableSchema::all_backup_storage_info_schema,
  ObInnerTableSchema::all_dam_last_arch_ts_schema,
  ObInnerTableSchema::all_dam_cleanup_jobs_schema,
  ObInnerTableSchema::all_backup_job_schema,
  ObInnerTableSchema::all_backup_job_history_schema,
  ObInnerTableSchema::all_backup_task_schema,
  ObInnerTableSchema::all_backup_task_history_schema,
  ObInnerTableSchema::all_backup_ls_task_schema,
  ObInnerTableSchema::all_backup_ls_task_history_schema,
  ObInnerTableSchema::all_backup_ls_task_info_schema,
  ObInnerTableSchema::all_backup_skipped_tablet_schema,
  ObInnerTableSchema::all_backup_skipped_tablet_history_schema,
  ObInnerTableSchema::all_tenant_info_schema,
  ObInnerTableSchema::all_tablet_to_table_history_schema,
  ObInnerTableSchema::all_ls_recovery_stat_schema,
  ObInnerTableSchema::all_backup_ls_task_info_history_schema,
  ObInnerTableSchema::all_tablet_replica_checksum_schema,
  ObInnerTableSchema::all_tablet_checksum_schema,
  ObInnerTableSchema::all_ls_replica_task_schema,
  ObInnerTableSchema::all_pending_transaction_schema,
  ObInnerTableSchema::all_balance_group_ls_stat_schema,
  ObInnerTableSchema::all_tenant_scheduler_job_schema,
  ObInnerTableSchema::all_tenant_scheduler_job_run_detail_schema,
  ObInnerTableSchema::all_tenant_scheduler_program_schema,
  ObInnerTableSchema::all_tenant_scheduler_program_argument_schema,
  ObInnerTableSchema::all_context_schema,
  ObInnerTableSchema::all_context_history_schema,
  ObInnerTableSchema::all_global_context_value_schema,
  ObInnerTableSchema::all_ls_election_reference_info_schema,
  ObInnerTableSchema::all_backup_delete_job_schema,
  ObInnerTableSchema::all_backup_delete_job_history_schema,
  ObInnerTableSchema::all_backup_delete_task_schema,
  ObInnerTableSchema::all_backup_delete_task_history_schema,
  ObInnerTableSchema::all_backup_delete_ls_task_schema,
  ObInnerTableSchema::all_backup_delete_ls_task_history_schema,
  ObInnerTableSchema::all_zone_merge_info_schema,
  ObInnerTableSchema::all_merge_info_schema,
  ObInnerTableSchema::all_freeze_info_schema,
  ObInnerTableSchema::all_disk_io_calibration_schema,
  ObInnerTableSchema::all_plan_baseline_schema,
  ObInnerTableSchema::all_plan_baseline_item_schema,
  ObInnerTableSchema::all_spm_config_schema,
  ObInnerTableSchema::all_log_archive_dest_parameter_schema,
  ObInnerTableSchema::all_backup_parameter_schema,
  ObInnerTableSchema::all_ls_restore_progress_schema,
  ObInnerTableSchema::all_ls_restore_history_schema,
  ObInnerTableSchema::all_backup_storage_info_history_schema,
  ObInnerTableSchema::all_backup_delete_policy_schema,
  ObInnerTableSchema::all_mock_fk_parent_table_schema,
  ObInnerTableSchema::all_mock_fk_parent_table_history_schema,
  ObInnerTableSchema::all_mock_fk_parent_table_column_schema,
  ObInnerTableSchema::all_mock_fk_parent_table_column_history_schema,
  ObInnerTableSchema::all_log_restore_source_schema,
  ObInnerTableSchema::all_kv_ttl_task_schema,
  ObInnerTableSchema::all_kv_ttl_task_history_schema,
  ObInnerTableSchema::all_service_epoch_schema,
  ObInnerTableSchema::all_spatial_reference_systems_schema,
  ObInnerTableSchema::all_column_checksum_error_info_schema,
  ObInnerTableSchema::all_column_group_schema,
  ObInnerTableSchema::all_column_group_history_schema,
  ObInnerTableSchema::all_column_group_mapping_schema,
  ObInnerTableSchema::all_column_group_mapping_history_schema,
  ObInnerTableSchema::all_transfer_task_schema,
  ObInnerTableSchema::all_transfer_task_history_schema,
  ObInnerTableSchema::all_balance_job_schema,
  ObInnerTableSchema::all_balance_job_history_schema,
  ObInnerTableSchema::all_balance_task_schema,
  ObInnerTableSchema::all_balance_task_history_schema,
  ObInnerTableSchema::all_arbitration_service_schema,
  ObInnerTableSchema::all_ls_arb_replica_task_schema,
  ObInnerTableSchema::all_data_dictionary_in_log_schema,
  ObInnerTableSchema::all_ls_arb_replica_task_history_schema,
  ObInnerTableSchema::all_rls_policy_schema,
  ObInnerTableSchema::all_rls_policy_history_schema,
  ObInnerTableSchema::all_rls_security_column_schema,
  ObInnerTableSchema::all_rls_security_column_history_schema,
  ObInnerTableSchema::all_rls_group_schema,
  ObInnerTableSchema::all_rls_group_history_schema,
  ObInnerTableSchema::all_rls_context_schema,
  ObInnerTableSchema::all_rls_context_history_schema,
  ObInnerTableSchema::all_rls_attribute_schema,
  ObInnerTableSchema::all_rls_attribute_history_schema,
  ObInnerTableSchema::all_tenant_rewrite_rules_schema,
  ObInnerTableSchema::all_reserved_snapshot_schema,
  ObInnerTableSchema::all_cluster_event_history_schema,
  ObInnerTableSchema::all_ls_transfer_member_list_lock_info_schema,
  ObInnerTableSchema::all_external_table_file_schema,
  ObInnerTableSchema::all_task_opt_stat_gather_history_schema,
  ObInnerTableSchema::all_table_opt_stat_gather_history_schema,
  ObInnerTableSchema::wr_active_session_history_schema,
  ObInnerTableSchema::wr_snapshot_schema,
  ObInnerTableSchema::wr_statname_schema,
  ObInnerTableSchema::wr_sysstat_schema,
  ObInnerTableSchema::all_balance_task_helper_schema,
  ObInnerTableSchema::all_tenant_snapshot_schema,
  ObInnerTableSchema::all_tenant_snapshot_ls_schema,
  ObInnerTableSchema::all_tenant_snapshot_ls_replica_schema,
  ObInnerTableSchema::all_mlog_schema,
  ObInnerTableSchema::all_mview_schema,
  ObInnerTableSchema::all_mview_refresh_stats_sys_defaults_schema,
  ObInnerTableSchema::all_mview_refresh_stats_params_schema,
  ObInnerTableSchema::all_mview_refresh_run_stats_schema,
  ObInnerTableSchema::all_mview_refresh_stats_schema,
  ObInnerTableSchema::all_mview_refresh_change_stats_schema,
  ObInnerTableSchema::all_mview_refresh_stmt_stats_schema,
  ObInnerTableSchema::all_dbms_lock_allocated_schema,
  ObInnerTableSchema::wr_control_schema,
  ObInnerTableSchema::all_tenant_event_history_schema,
  ObInnerTableSchema::all_tenant_scheduler_job_class_schema,
  ObInnerTableSchema::all_recover_table_job_schema,
  ObInnerTableSchema::all_recover_table_job_history_schema,
  ObInnerTableSchema::all_import_table_job_schema,
  ObInnerTableSchema::all_import_table_job_history_schema,
  ObInnerTableSchema::all_import_table_task_schema,
  ObInnerTableSchema::all_import_table_task_history_schema,
  ObInnerTableSchema::all_storage_ha_error_diagnose_history_schema,
  ObInnerTableSchema::all_storage_ha_perf_diagnose_history_schema,
  ObInnerTableSchema::all_clone_job_schema,
  ObInnerTableSchema::all_clone_job_history_schema,
  ObInnerTableSchema::all_routine_privilege_schema,
  ObInnerTableSchema::all_routine_privilege_history_schema,
  ObInnerTableSchema::all_ncomp_dll_schema,
  ObInnerTableSchema::all_aux_stat_schema,
  ObInnerTableSchema::all_index_usage_info_schema,
  ObInnerTableSchema::all_detect_lock_info_schema,
  ObInnerTableSchema::all_client_to_server_session_info_schema,
  ObInnerTableSchema::all_transfer_partition_task_schema,
  ObInnerTableSchema::all_transfer_partition_task_history_schema,
  ObInnerTableSchema::all_tenant_snapshot_job_schema,
  ObInnerTableSchema::all_trusted_root_certificate_schema,
  ObInnerTableSchema::all_column_privilege_schema,
  ObInnerTableSchema::all_column_privilege_history_schema,
  ObInnerTableSchema::all_tenant_snapshot_ls_replica_history_schema,
  ObInnerTableSchema::all_user_proxy_info_schema,
  ObInnerTableSchema::all_user_proxy_info_history_schema,
  ObInnerTableSchema::all_user_proxy_role_info_schema,
  ObInnerTableSchema::all_user_proxy_role_info_history_schema,
  ObInnerTableSchema::all_mview_dep_schema,
  ObInnerTableSchema::all_scheduler_job_run_detail_v2_schema,
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
  ObInnerTableSchema::tenant_virtual_statname_schema,
  ObInnerTableSchema::tenant_virtual_event_name_schema,
  ObInnerTableSchema::tenant_virtual_global_variable_schema,
  ObInnerTableSchema::tenant_virtual_show_tables_schema,
  ObInnerTableSchema::tenant_virtual_show_create_procedure_schema,
  ObInnerTableSchema::all_virtual_core_meta_table_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_schema,
  ObInnerTableSchema::all_virtual_plan_stat_schema,
  ObInnerTableSchema::all_virtual_mem_leak_checker_info_schema,
  ObInnerTableSchema::all_virtual_latch_schema,
  ObInnerTableSchema::all_virtual_kvcache_info_schema,
  ObInnerTableSchema::all_virtual_data_type_class_schema,
  ObInnerTableSchema::all_virtual_data_type_schema,
  ObInnerTableSchema::all_virtual_session_event_schema,
  ObInnerTableSchema::all_virtual_session_wait_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_schema,
  ObInnerTableSchema::all_virtual_system_event_schema,
  ObInnerTableSchema::all_virtual_tenant_memstore_info_schema,
  ObInnerTableSchema::all_virtual_concurrency_object_pool_schema,
  ObInnerTableSchema::all_virtual_sesstat_schema,
  ObInnerTableSchema::all_virtual_sysstat_schema,
  ObInnerTableSchema::all_virtual_disk_stat_schema,
  ObInnerTableSchema::all_virtual_memstore_info_schema,
  ObInnerTableSchema::all_virtual_upgrade_inspection_schema,
  ObInnerTableSchema::all_virtual_trans_stat_schema,
  ObInnerTableSchema::all_virtual_trans_ctx_mgr_stat_schema,
  ObInnerTableSchema::all_virtual_trans_scheduler_schema,
  ObInnerTableSchema::all_virtual_sql_audit_schema,
  ObInnerTableSchema::all_virtual_core_all_table_schema,
  ObInnerTableSchema::all_virtual_core_column_table_schema,
  ObInnerTableSchema::all_virtual_memory_info_schema,
  ObInnerTableSchema::all_virtual_sys_parameter_stat_schema,
  ObInnerTableSchema::all_virtual_trace_span_info_schema,
  ObInnerTableSchema::all_virtual_engine_schema,
  ObInnerTableSchema::all_virtual_proxy_server_stat_schema,
  ObInnerTableSchema::all_virtual_proxy_sys_variable_schema,
  ObInnerTableSchema::all_virtual_proxy_schema_schema,
  ObInnerTableSchema::all_virtual_plan_cache_plan_explain_schema,
  ObInnerTableSchema::all_virtual_obrpc_stat_schema,
  ObInnerTableSchema::tenant_virtual_outline_schema,
  ObInnerTableSchema::tenant_virtual_concurrent_limit_sql_schema,
  ObInnerTableSchema::all_virtual_tablet_sstable_macro_info_schema,
  ObInnerTableSchema::all_virtual_proxy_partition_info_schema,
  ObInnerTableSchema::all_virtual_proxy_partition_schema,
  ObInnerTableSchema::all_virtual_proxy_sub_partition_schema,
  ObInnerTableSchema::all_virtual_sys_task_status_schema,
  ObInnerTableSchema::all_virtual_macro_block_marker_status_schema,
  ObInnerTableSchema::all_virtual_io_stat_schema,
  ObInnerTableSchema::all_virtual_long_ops_status_schema,
  ObInnerTableSchema::all_virtual_server_object_pool_schema,
  ObInnerTableSchema::all_virtual_trans_lock_stat_schema,
  ObInnerTableSchema::tenant_virtual_show_create_tablegroup_schema,
  ObInnerTableSchema::all_virtual_server_blacklist_schema,
  ObInnerTableSchema::all_virtual_tenant_parameter_stat_schema,
  ObInnerTableSchema::all_virtual_server_schema_info_schema,
  ObInnerTableSchema::all_virtual_memory_context_stat_schema,
  ObInnerTableSchema::all_virtual_dump_tenant_info_schema,
  ObInnerTableSchema::all_virtual_tenant_parameter_info_schema,
  ObInnerTableSchema::all_virtual_audit_operation_schema,
  ObInnerTableSchema::all_virtual_audit_action_schema,
  ObInnerTableSchema::all_virtual_dag_warning_history_schema,
  ObInnerTableSchema::all_virtual_tablet_encrypt_info_schema,
  ObInnerTableSchema::tenant_virtual_show_restore_preview_schema,
  ObInnerTableSchema::all_virtual_master_key_version_info_schema,
  ObInnerTableSchema::all_virtual_dag_schema,
  ObInnerTableSchema::all_virtual_dag_scheduler_schema,
  ObInnerTableSchema::all_virtual_server_compaction_progress_schema,
  ObInnerTableSchema::all_virtual_tablet_compaction_progress_schema,
  ObInnerTableSchema::all_virtual_compaction_diagnose_info_schema,
  ObInnerTableSchema::all_virtual_compaction_suggestion_schema,
  ObInnerTableSchema::all_virtual_session_info_schema,
  ObInnerTableSchema::all_virtual_tablet_compaction_history_schema,
  ObInnerTableSchema::all_virtual_io_calibration_status_schema,
  ObInnerTableSchema::all_virtual_io_benchmark_schema,
  ObInnerTableSchema::all_virtual_io_quota_schema,
  ObInnerTableSchema::all_virtual_server_compaction_event_history_schema,
  ObInnerTableSchema::all_virtual_tablet_stat_schema,
  ObInnerTableSchema::all_virtual_ddl_sim_point_schema,
  ObInnerTableSchema::all_virtual_ddl_sim_point_stat_schema,
  ObInnerTableSchema::session_variables_schema,
  ObInnerTableSchema::global_status_schema,
  ObInnerTableSchema::session_status_schema,
  ObInnerTableSchema::user_schema,
  ObInnerTableSchema::db_schema,
  ObInnerTableSchema::all_virtual_lock_wait_stat_schema,
  ObInnerTableSchema::proc_schema,
  ObInnerTableSchema::tenant_virtual_collation_schema,
  ObInnerTableSchema::tenant_virtual_charset_schema,
  ObInnerTableSchema::all_virtual_tenant_memstore_allocator_info_schema,
  ObInnerTableSchema::all_virtual_table_mgr_schema,
  ObInnerTableSchema::all_virtual_freeze_info_schema,
  ObInnerTableSchema::all_virtual_bad_block_table_schema,
  ObInnerTableSchema::all_virtual_px_worker_stat_schema,
  ObInnerTableSchema::all_virtual_auto_increment_schema,
  ObInnerTableSchema::all_virtual_sequence_value_schema,
  ObInnerTableSchema::all_virtual_tablet_store_stat_schema,
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
  ObInnerTableSchema::all_virtual_recyclebin_schema,
  ObInnerTableSchema::all_virtual_sequence_object_schema,
  ObInnerTableSchema::all_virtual_sequence_object_history_schema,
  ObInnerTableSchema::all_virtual_raid_stat_schema,
  ObInnerTableSchema::all_virtual_dtl_channel_schema,
  ObInnerTableSchema::all_virtual_dtl_memory_schema,
  ObInnerTableSchema::all_virtual_dblink_schema,
  ObInnerTableSchema::all_virtual_dblink_history_schema,
  ObInnerTableSchema::all_virtual_tenant_role_grantee_map_schema,
  ObInnerTableSchema::all_virtual_tenant_role_grantee_map_history_schema,
  ObInnerTableSchema::all_virtual_tenant_keystore_schema,
  ObInnerTableSchema::all_virtual_tenant_keystore_history_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_policy_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_policy_history_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_component_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_component_history_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_label_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_label_history_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_user_level_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_user_level_history_schema,
  ObInnerTableSchema::all_virtual_tenant_tablespace_schema,
  ObInnerTableSchema::all_virtual_tenant_tablespace_history_schema,
  ObInnerTableSchema::all_virtual_information_columns_schema,
  ObInnerTableSchema::all_virtual_tenant_user_failed_login_stat_schema,
  ObInnerTableSchema::all_virtual_tenant_profile_schema,
  ObInnerTableSchema::all_virtual_tenant_profile_history_schema,
  ObInnerTableSchema::all_virtual_security_audit_schema,
  ObInnerTableSchema::all_virtual_security_audit_history_schema,
  ObInnerTableSchema::all_virtual_trigger_schema,
  ObInnerTableSchema::all_virtual_trigger_history_schema,
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
  ObInnerTableSchema::all_virtual_error_schema,
  ObInnerTableSchema::all_virtual_id_service_schema,
  ObInnerTableSchema::all_virtual_object_type_schema,
  ObInnerTableSchema::all_virtual_sql_plan_monitor_schema,
  ObInnerTableSchema::all_virtual_sql_monitor_statname_schema,
  ObInnerTableSchema::all_virtual_open_cursor_schema,
  ObInnerTableSchema::all_virtual_time_zone_schema,
  ObInnerTableSchema::all_virtual_time_zone_name_schema,
  ObInnerTableSchema::all_virtual_time_zone_transition_schema,
  ObInnerTableSchema::all_virtual_time_zone_transition_type_schema,
  ObInnerTableSchema::all_virtual_constraint_column_schema,
  ObInnerTableSchema::all_virtual_constraint_column_history_schema,
  ObInnerTableSchema::all_virtual_files_schema,
  ObInnerTableSchema::all_virtual_dependency_schema,
  ObInnerTableSchema::tenant_virtual_object_definition_schema,
  ObInnerTableSchema::all_virtual_global_transaction_schema,
  ObInnerTableSchema::all_virtual_ddl_task_status_schema,
  ObInnerTableSchema::all_virtual_deadlock_event_history_schema,
  ObInnerTableSchema::all_virtual_column_usage_schema,
  ObInnerTableSchema::all_virtual_tenant_ctx_memory_info_schema,
  ObInnerTableSchema::all_virtual_job_schema,
  ObInnerTableSchema::all_virtual_job_log_schema,
  ObInnerTableSchema::all_virtual_tenant_directory_schema,
  ObInnerTableSchema::all_virtual_tenant_directory_history_schema,
  ObInnerTableSchema::all_virtual_table_stat_schema,
  ObInnerTableSchema::all_virtual_column_stat_schema,
  ObInnerTableSchema::all_virtual_histogram_stat_schema,
  ObInnerTableSchema::all_virtual_tenant_memory_info_schema,
  ObInnerTableSchema::tenant_virtual_show_create_trigger_schema,
  ObInnerTableSchema::all_virtual_px_target_monitor_schema,
  ObInnerTableSchema::all_virtual_monitor_modified_schema,
  ObInnerTableSchema::all_virtual_table_stat_history_schema,
  ObInnerTableSchema::all_virtual_column_stat_history_schema,
  ObInnerTableSchema::all_virtual_histogram_stat_history_schema,
  ObInnerTableSchema::all_virtual_optstat_global_prefs_schema,
  ObInnerTableSchema::all_virtual_optstat_user_prefs_schema,
  ObInnerTableSchema::all_virtual_dblink_info_schema,
  ObInnerTableSchema::all_virtual_log_archive_progress_schema,
  ObInnerTableSchema::all_virtual_log_archive_history_schema,
  ObInnerTableSchema::all_virtual_log_archive_piece_files_schema,
  ObInnerTableSchema::all_virtual_ls_log_archive_progress_schema,
  ObInnerTableSchema::all_virtual_backup_storage_info_schema,
  ObInnerTableSchema::all_virtual_ls_status_schema,
  ObInnerTableSchema::all_virtual_ls_schema,
  ObInnerTableSchema::all_virtual_ls_meta_table_schema,
  ObInnerTableSchema::all_virtual_tablet_meta_table_schema,
  ObInnerTableSchema::all_virtual_tablet_to_ls_schema,
  ObInnerTableSchema::all_virtual_load_data_stat_schema,
  ObInnerTableSchema::all_virtual_dam_last_arch_ts_schema,
  ObInnerTableSchema::all_virtual_dam_cleanup_jobs_schema,
  ObInnerTableSchema::all_virtual_backup_task_schema,
  ObInnerTableSchema::all_virtual_backup_task_history_schema,
  ObInnerTableSchema::all_virtual_backup_ls_task_schema,
  ObInnerTableSchema::all_virtual_backup_ls_task_history_schema,
  ObInnerTableSchema::all_virtual_backup_ls_task_info_schema,
  ObInnerTableSchema::all_virtual_backup_skipped_tablet_schema,
  ObInnerTableSchema::all_virtual_backup_skipped_tablet_history_schema,
  ObInnerTableSchema::all_virtual_backup_schedule_task_schema,
  ObInnerTableSchema::all_virtual_tablet_to_table_history_schema,
  ObInnerTableSchema::all_virtual_log_stat_schema,
  ObInnerTableSchema::all_virtual_tenant_info_schema,
  ObInnerTableSchema::all_virtual_ls_recovery_stat_schema,
  ObInnerTableSchema::all_virtual_backup_ls_task_info_history_schema,
  ObInnerTableSchema::all_virtual_tablet_replica_checksum_schema,
  ObInnerTableSchema::all_virtual_ddl_checksum_schema,
  ObInnerTableSchema::all_virtual_ddl_error_message_schema,
  ObInnerTableSchema::all_virtual_ls_replica_task_schema,
  ObInnerTableSchema::all_virtual_pending_transaction_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_job_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_job_run_detail_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_program_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_program_argument_schema,
  ObInnerTableSchema::all_virtual_tenant_context_schema,
  ObInnerTableSchema::all_virtual_tenant_context_history_schema,
  ObInnerTableSchema::all_virtual_global_context_value_schema,
  ObInnerTableSchema::all_virtual_unit_schema,
  ObInnerTableSchema::all_virtual_server_schema,
  ObInnerTableSchema::all_virtual_ls_election_reference_info_schema,
  ObInnerTableSchema::all_virtual_dtl_interm_result_monitor_schema,
  ObInnerTableSchema::all_virtual_archive_stat_schema,
  ObInnerTableSchema::all_virtual_apply_stat_schema,
  ObInnerTableSchema::all_virtual_replay_stat_schema,
  ObInnerTableSchema::all_virtual_proxy_routine_schema,
  ObInnerTableSchema::all_virtual_backup_delete_task_schema,
  ObInnerTableSchema::all_virtual_backup_delete_task_history_schema,
  ObInnerTableSchema::all_virtual_backup_delete_ls_task_schema,
  ObInnerTableSchema::all_virtual_backup_delete_ls_task_history_schema,
  ObInnerTableSchema::all_virtual_ls_info_schema,
  ObInnerTableSchema::all_virtual_tablet_info_schema,
  ObInnerTableSchema::all_virtual_obj_lock_schema,
  ObInnerTableSchema::all_virtual_zone_merge_info_schema,
  ObInnerTableSchema::all_virtual_merge_info_schema,
  ObInnerTableSchema::all_virtual_tx_data_table_schema,
  ObInnerTableSchema::all_virtual_transaction_freeze_checkpoint_schema,
  ObInnerTableSchema::all_virtual_transaction_checkpoint_schema,
  ObInnerTableSchema::all_virtual_checkpoint_schema,
  ObInnerTableSchema::all_virtual_backup_set_files_schema,
  ObInnerTableSchema::all_virtual_backup_job_schema,
  ObInnerTableSchema::all_virtual_backup_job_history_schema,
  ObInnerTableSchema::all_virtual_plan_baseline_schema,
  ObInnerTableSchema::all_virtual_plan_baseline_item_schema,
  ObInnerTableSchema::all_virtual_spm_config_schema,
  ObInnerTableSchema::all_virtual_ash_schema,
  ObInnerTableSchema::all_virtual_dml_stats_schema,
  ObInnerTableSchema::all_virtual_log_archive_dest_parameter_schema,
  ObInnerTableSchema::all_virtual_backup_parameter_schema,
  ObInnerTableSchema::all_virtual_restore_job_schema,
  ObInnerTableSchema::all_virtual_restore_job_history_schema,
  ObInnerTableSchema::all_virtual_restore_progress_schema,
  ObInnerTableSchema::all_virtual_ls_restore_progress_schema,
  ObInnerTableSchema::all_virtual_ls_restore_history_schema,
  ObInnerTableSchema::all_virtual_backup_storage_info_history_schema,
  ObInnerTableSchema::all_virtual_backup_delete_job_schema,
  ObInnerTableSchema::all_virtual_backup_delete_job_history_schema,
  ObInnerTableSchema::all_virtual_backup_delete_policy_schema,
  ObInnerTableSchema::all_virtual_tablet_ddl_kv_info_schema,
  ObInnerTableSchema::all_virtual_privilege_schema,
  ObInnerTableSchema::all_virtual_tablet_pointer_status_schema,
  ObInnerTableSchema::all_virtual_storage_meta_memory_status_schema,
  ObInnerTableSchema::all_virtual_kvcache_store_memblock_schema,
  ObInnerTableSchema::all_virtual_mock_fk_parent_table_schema,
  ObInnerTableSchema::all_virtual_mock_fk_parent_table_history_schema,
  ObInnerTableSchema::all_virtual_mock_fk_parent_table_column_schema,
  ObInnerTableSchema::all_virtual_mock_fk_parent_table_column_history_schema,
  ObInnerTableSchema::all_virtual_log_restore_source_schema,
  ObInnerTableSchema::all_virtual_query_response_time_schema,
  ObInnerTableSchema::all_virtual_kv_ttl_task_schema,
  ObInnerTableSchema::all_virtual_kv_ttl_task_history_schema,
  ObInnerTableSchema::all_virtual_column_checksum_error_info_schema,
  ObInnerTableSchema::all_virtual_tablet_compaction_info_schema,
  ObInnerTableSchema::all_virtual_ls_replica_task_plan_schema,
  ObInnerTableSchema::all_virtual_schema_memory_schema,
  ObInnerTableSchema::all_virtual_schema_slot_schema,
  ObInnerTableSchema::all_virtual_minor_freeze_info_schema,
  ObInnerTableSchema::all_virtual_show_trace_schema,
  ObInnerTableSchema::all_virtual_ha_diagnose_schema,
  ObInnerTableSchema::all_virtual_data_dictionary_in_log_schema,
  ObInnerTableSchema::all_virtual_transfer_task_schema,
  ObInnerTableSchema::all_virtual_transfer_task_history_schema,
  ObInnerTableSchema::all_virtual_balance_job_schema,
  ObInnerTableSchema::all_virtual_balance_job_history_schema,
  ObInnerTableSchema::all_virtual_balance_task_schema,
  ObInnerTableSchema::all_virtual_balance_task_history_schema,
  ObInnerTableSchema::all_virtual_rls_policy_schema,
  ObInnerTableSchema::all_virtual_rls_policy_history_schema,
  ObInnerTableSchema::all_virtual_rls_security_column_schema,
  ObInnerTableSchema::all_virtual_rls_security_column_history_schema,
  ObInnerTableSchema::all_virtual_rls_group_schema,
  ObInnerTableSchema::all_virtual_rls_group_history_schema,
  ObInnerTableSchema::all_virtual_rls_context_schema,
  ObInnerTableSchema::all_virtual_rls_context_history_schema,
  ObInnerTableSchema::all_virtual_rls_attribute_schema,
  ObInnerTableSchema::all_virtual_rls_attribute_history_schema,
  ObInnerTableSchema::all_virtual_tenant_mysql_sys_agent_schema,
  ObInnerTableSchema::all_virtual_sql_plan_schema,
  ObInnerTableSchema::all_virtual_core_table_schema,
  ObInnerTableSchema::all_virtual_malloc_sample_info_schema,
  ObInnerTableSchema::all_virtual_ls_arb_replica_task_schema,
  ObInnerTableSchema::all_virtual_ls_arb_replica_task_history_schema,
  ObInnerTableSchema::all_virtual_archive_dest_status_schema,
  ObInnerTableSchema::all_virtual_io_scheduler_schema,
  ObInnerTableSchema::all_virtual_external_table_file_schema,
  ObInnerTableSchema::all_virtual_mds_node_stat_schema,
  ObInnerTableSchema::all_virtual_mds_event_history_schema,
  ObInnerTableSchema::all_virtual_dup_ls_lease_mgr_schema,
  ObInnerTableSchema::all_virtual_dup_ls_tablet_set_schema,
  ObInnerTableSchema::all_virtual_dup_ls_tablets_schema,
  ObInnerTableSchema::all_virtual_tx_data_schema,
  ObInnerTableSchema::all_virtual_task_opt_stat_gather_history_schema,
  ObInnerTableSchema::all_virtual_table_opt_stat_gather_history_schema,
  ObInnerTableSchema::all_virtual_opt_stat_gather_monitor_schema,
  ObInnerTableSchema::all_virtual_thread_schema,
  ObInnerTableSchema::all_virtual_arbitration_member_info_schema,
  ObInnerTableSchema::all_virtual_arbitration_service_status_schema,
  ObInnerTableSchema::all_virtual_wr_active_session_history_schema,
  ObInnerTableSchema::all_virtual_wr_snapshot_schema,
  ObInnerTableSchema::all_virtual_wr_statname_schema,
  ObInnerTableSchema::all_virtual_wr_sysstat_schema,
  ObInnerTableSchema::all_virtual_kv_connection_schema,
  ObInnerTableSchema::all_virtual_virtual_long_ops_status_mysql_sys_agent_schema,
  ObInnerTableSchema::all_virtual_ls_transfer_member_list_lock_info_schema,
  ObInnerTableSchema::all_virtual_timestamp_service_schema,
  ObInnerTableSchema::all_virtual_resource_pool_mysql_sys_agent_schema,
  ObInnerTableSchema::all_virtual_px_p2p_datahub_schema,
  ObInnerTableSchema::all_virtual_column_group_schema,
  ObInnerTableSchema::all_virtual_storage_leak_info_schema,
  ObInnerTableSchema::all_virtual_ls_log_restore_status_schema,
  ObInnerTableSchema::all_virtual_tenant_parameter_schema,
  ObInnerTableSchema::all_virtual_tenant_snapshot_schema,
  ObInnerTableSchema::all_virtual_tenant_snapshot_ls_schema,
  ObInnerTableSchema::all_virtual_tenant_snapshot_ls_replica_schema,
  ObInnerTableSchema::all_virtual_tablet_buffer_info_schema,
  ObInnerTableSchema::all_virtual_mlog_schema,
  ObInnerTableSchema::all_virtual_mview_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_stats_sys_defaults_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_stats_params_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_run_stats_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_stats_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_change_stats_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_stmt_stats_schema,
  ObInnerTableSchema::all_virtual_wr_control_schema,
  ObInnerTableSchema::all_virtual_tenant_event_history_schema,
  ObInnerTableSchema::all_virtual_balance_task_helper_schema,
  ObInnerTableSchema::all_virtual_balance_group_ls_stat_schema,
  ObInnerTableSchema::all_virtual_cgroup_config_schema,
  ObInnerTableSchema::all_virtual_flt_config_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_job_class_schema,
  ObInnerTableSchema::all_virtual_recover_table_job_schema,
  ObInnerTableSchema::all_virtual_recover_table_job_history_schema,
  ObInnerTableSchema::all_virtual_import_table_job_schema,
  ObInnerTableSchema::all_virtual_import_table_job_history_schema,
  ObInnerTableSchema::all_virtual_import_table_task_schema,
  ObInnerTableSchema::all_virtual_import_table_task_history_schema,
  ObInnerTableSchema::all_virtual_data_activity_metrics_schema,
  ObInnerTableSchema::all_virtual_column_group_mapping_schema,
  ObInnerTableSchema::all_virtual_column_group_history_schema,
  ObInnerTableSchema::all_virtual_column_group_mapping_history_schema,
  ObInnerTableSchema::all_virtual_storage_ha_error_diagnose_schema,
  ObInnerTableSchema::all_virtual_storage_ha_perf_diagnose_schema,
  ObInnerTableSchema::all_virtual_clone_job_schema,
  ObInnerTableSchema::all_virtual_clone_job_history_schema,
  ObInnerTableSchema::all_virtual_checkpoint_diagnose_memtable_info_schema,
  ObInnerTableSchema::all_virtual_checkpoint_diagnose_checkpoint_unit_info_schema,
  ObInnerTableSchema::all_virtual_checkpoint_diagnose_info_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_running_job_schema,
  ObInnerTableSchema::all_virtual_routine_privilege_schema,
  ObInnerTableSchema::all_virtual_routine_privilege_history_schema,
  ObInnerTableSchema::all_virtual_aux_stat_schema,
  ObInnerTableSchema::all_virtual_detect_lock_info_schema,
  ObInnerTableSchema::all_virtual_client_to_server_session_info_schema,
  ObInnerTableSchema::all_virtual_sys_variable_default_value_schema,
  ObInnerTableSchema::all_virtual_transfer_partition_task_schema,
  ObInnerTableSchema::all_virtual_transfer_partition_task_history_schema,
  ObInnerTableSchema::all_virtual_tenant_snapshot_job_schema,
  ObInnerTableSchema::all_virtual_dbms_lock_allocated_schema,
  ObInnerTableSchema::all_virtual_ls_snapshot_schema,
  ObInnerTableSchema::all_virtual_index_usage_info_schema,
  ObInnerTableSchema::all_virtual_column_privilege_schema,
  ObInnerTableSchema::all_virtual_column_privilege_history_schema,
  ObInnerTableSchema::all_virtual_tenant_snapshot_ls_replica_history_schema,
  ObInnerTableSchema::enabled_roles_schema,
  ObInnerTableSchema::all_virtual_tracepoint_info_schema,
  ObInnerTableSchema::all_virtual_compatibility_control_schema,
  ObInnerTableSchema::all_virtual_user_proxy_info_schema,
  ObInnerTableSchema::all_virtual_user_proxy_info_history_schema,
  ObInnerTableSchema::all_virtual_user_proxy_role_info_schema,
  ObInnerTableSchema::all_virtual_user_proxy_role_info_history_schema,
  ObInnerTableSchema::all_virtual_tenant_resource_limit_schema,
  ObInnerTableSchema::all_virtual_tenant_resource_limit_detail_schema,
  ObInnerTableSchema::all_virtual_nic_info_schema,
  ObInnerTableSchema::all_virtual_scheduler_job_run_detail_v2_schema,
  ObInnerTableSchema::all_virtual_spatial_reference_systems_schema,
  ObInnerTableSchema::all_virtual_ash_all_virtual_ash_i1_schema,
  ObInnerTableSchema::all_virtual_sql_plan_monitor_all_virtual_sql_plan_monitor_i1_schema,
  ObInnerTableSchema::all_virtual_sql_audit_all_virtual_sql_audit_i1_schema,
  ObInnerTableSchema::all_virtual_sysstat_all_virtual_sysstat_i1_schema,
  ObInnerTableSchema::all_virtual_sesstat_all_virtual_sesstat_i1_schema,
  ObInnerTableSchema::all_virtual_system_event_all_virtual_system_event_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_all_virtual_session_wait_history_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_all_virtual_session_wait_i1_schema,
  ObInnerTableSchema::all_virtual_session_event_all_virtual_session_event_i1_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_all_virtual_plan_cache_stat_i1_schema,
  ObInnerTableSchema::all_virtual_sql_audit_ora_schema,
  ObInnerTableSchema::all_virtual_plan_stat_ora_schema,
  ObInnerTableSchema::all_virtual_plan_cache_plan_explain_ora_schema,
  ObInnerTableSchema::tenant_virtual_outline_agent_schema,
  ObInnerTableSchema::all_virtual_privilege_ora_schema,
  ObInnerTableSchema::all_virtual_sys_parameter_stat_agent_schema,
  ObInnerTableSchema::tenant_virtual_table_index_agent_schema,
  ObInnerTableSchema::tenant_virtual_charset_agent_schema,
  ObInnerTableSchema::tenant_virtual_all_table_agent_schema,
  ObInnerTableSchema::tenant_virtual_collation_ora_schema,
  ObInnerTableSchema::all_virtual_server_agent_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_ora_schema,
  ObInnerTableSchema::all_virtual_processlist_ora_schema,
  ObInnerTableSchema::all_virtual_session_wait_ora_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_ora_schema,
  ObInnerTableSchema::all_virtual_memory_info_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_memstore_info_ora_schema,
  ObInnerTableSchema::all_virtual_memstore_info_ora_schema,
  ObInnerTableSchema::all_virtual_sesstat_ora_schema,
  ObInnerTableSchema::all_virtual_sysstat_ora_schema,
  ObInnerTableSchema::all_virtual_system_event_ora_schema,
  ObInnerTableSchema::tenant_virtual_session_variable_ora_schema,
  ObInnerTableSchema::tenant_virtual_global_variable_ora_schema,
  ObInnerTableSchema::tenant_virtual_show_create_table_ora_schema,
  ObInnerTableSchema::tenant_virtual_show_create_procedure_ora_schema,
  ObInnerTableSchema::tenant_virtual_show_create_tablegroup_ora_schema,
  ObInnerTableSchema::tenant_virtual_privilege_grant_ora_schema,
  ObInnerTableSchema::tenant_virtual_table_column_ora_schema,
  ObInnerTableSchema::all_virtual_trace_span_info_ora_schema,
  ObInnerTableSchema::tenant_virtual_concurrent_limit_sql_agent_schema,
  ObInnerTableSchema::all_virtual_data_type_ora_schema,
  ObInnerTableSchema::all_virtual_audit_operation_ora_schema,
  ObInnerTableSchema::all_virtual_audit_action_ora_schema,
  ObInnerTableSchema::all_virtual_px_worker_stat_ora_schema,
  ObInnerTableSchema::all_virtual_ps_stat_ora_schema,
  ObInnerTableSchema::all_virtual_ps_item_info_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_parameter_stat_ora_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_history_stat_ora_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_active_ora_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_histogram_ora_schema,
  ObInnerTableSchema::all_virtual_sql_workarea_memory_info_ora_schema,
  ObInnerTableSchema::all_virtual_table_mgr_ora_schema,
  ObInnerTableSchema::all_virtual_server_schema_info_ora_schema,
  ObInnerTableSchema::all_virtual_sql_plan_monitor_ora_schema,
  ObInnerTableSchema::all_virtual_sql_monitor_statname_ora_schema,
  ObInnerTableSchema::all_virtual_lock_wait_stat_ora_schema,
  ObInnerTableSchema::all_virtual_open_cursor_ora_schema,
  ObInnerTableSchema::tenant_virtual_object_definition_ora_schema,
  ObInnerTableSchema::all_virtual_routine_param_sys_agent_schema,
  ObInnerTableSchema::all_virtual_type_sys_agent_schema,
  ObInnerTableSchema::all_virtual_type_attr_sys_agent_schema,
  ObInnerTableSchema::all_virtual_coll_type_sys_agent_schema,
  ObInnerTableSchema::all_virtual_package_sys_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_trigger_sys_agent_schema,
  ObInnerTableSchema::all_virtual_routine_sys_agent_schema,
  ObInnerTableSchema::all_virtual_global_transaction_ora_schema,
  ObInnerTableSchema::all_virtual_table_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_column_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_database_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_auto_increment_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_part_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_sub_part_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_package_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_sequence_value_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_sequence_object_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_user_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_synonym_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_foreign_key_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_recyclebin_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_routine_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tablegroup_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_foreign_key_column_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_constraint_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_type_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_type_attr_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_coll_type_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_routine_param_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_keystore_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_policy_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_component_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_label_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_user_level_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_tablespace_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_profile_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_role_grantee_map_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_table_privilege_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_security_audit_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_trigger_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_security_audit_record_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_sysauth_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_objauth_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_error_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_def_sub_part_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_object_type_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_dblink_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_constraint_column_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_dependency_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_name_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_transition_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_time_zone_transition_type_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_res_mgr_plan_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_res_mgr_directive_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_trans_lock_stat_ora_schema,
  ObInnerTableSchema::all_virtual_res_mgr_mapping_rule_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tablet_encrypt_info_ora_schema,
  ObInnerTableSchema::all_virtual_res_mgr_consumer_group_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_column_usage_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_job_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_job_log_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_directory_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_table_stat_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_column_stat_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_histogram_stat_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_memory_info_ora_schema,
  ObInnerTableSchema::tenant_virtual_show_create_trigger_ora_schema,
  ObInnerTableSchema::all_virtual_px_target_monitor_ora_schema,
  ObInnerTableSchema::all_virtual_monitor_modified_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_table_stat_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_column_stat_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_histogram_stat_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_optstat_global_prefs_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_optstat_user_prefs_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_dblink_info_ora_schema,
  ObInnerTableSchema::all_virtual_dam_last_arch_ts_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_dam_cleanup_jobs_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_job_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_program_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_context_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_global_context_value_ora_schema,
  ObInnerTableSchema::all_virtual_trans_stat_ora_schema,
  ObInnerTableSchema::all_virtual_unit_ora_schema,
  ObInnerTableSchema::all_virtual_kvcache_info_ora_schema,
  ObInnerTableSchema::all_virtual_server_compaction_progress_ora_schema,
  ObInnerTableSchema::all_virtual_tablet_compaction_progress_ora_schema,
  ObInnerTableSchema::all_virtual_compaction_diagnose_info_ora_schema,
  ObInnerTableSchema::all_virtual_compaction_suggestion_ora_schema,
  ObInnerTableSchema::all_virtual_tablet_compaction_history_ora_schema,
  ObInnerTableSchema::all_virtual_ls_meta_table_ora_schema,
  ObInnerTableSchema::all_virtual_tablet_to_ls_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tablet_meta_table_ora_schema,
  ObInnerTableSchema::all_virtual_core_all_table_ora_schema,
  ObInnerTableSchema::all_virtual_core_column_table_ora_schema,
  ObInnerTableSchema::all_virtual_dtl_interm_result_monitor_ora_schema,
  ObInnerTableSchema::all_virtual_proxy_schema_ora_schema,
  ObInnerTableSchema::all_virtual_proxy_partition_ora_schema,
  ObInnerTableSchema::all_virtual_proxy_partition_info_ora_schema,
  ObInnerTableSchema::all_virtual_proxy_sub_partition_ora_schema,
  ObInnerTableSchema::all_virtual_zone_merge_info_ora_schema,
  ObInnerTableSchema::all_virtual_merge_info_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_object_type_sys_agent_schema,
  ObInnerTableSchema::all_virtual_backup_job_ora_schema,
  ObInnerTableSchema::all_virtual_backup_job_history_ora_schema,
  ObInnerTableSchema::all_virtual_backup_task_ora_schema,
  ObInnerTableSchema::all_virtual_backup_task_history_ora_schema,
  ObInnerTableSchema::all_virtual_backup_set_files_ora_schema,
  ObInnerTableSchema::all_virtual_plan_baseline_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_plan_baseline_item_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_spm_config_real_agent_ora_schema,
  ObInnerTableSchema::tenant_virtual_event_name_ora_schema,
  ObInnerTableSchema::all_virtual_ash_ora_schema,
  ObInnerTableSchema::all_virtual_dml_stats_ora_schema,
  ObInnerTableSchema::all_virtual_log_archive_dest_parameter_ora_schema,
  ObInnerTableSchema::all_virtual_log_archive_progress_ora_schema,
  ObInnerTableSchema::all_virtual_log_archive_history_ora_schema,
  ObInnerTableSchema::all_virtual_log_archive_piece_files_ora_schema,
  ObInnerTableSchema::all_virtual_ls_log_archive_progress_ora_schema,
  ObInnerTableSchema::all_virtual_backup_parameter_ora_schema,
  ObInnerTableSchema::all_virtual_restore_job_ora_schema,
  ObInnerTableSchema::all_virtual_restore_job_history_ora_schema,
  ObInnerTableSchema::all_virtual_restore_progress_ora_schema,
  ObInnerTableSchema::all_virtual_ls_restore_progress_ora_schema,
  ObInnerTableSchema::all_virtual_ls_restore_history_ora_schema,
  ObInnerTableSchema::all_virtual_outline_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_outline_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_backup_storage_info_ora_schema,
  ObInnerTableSchema::all_virtual_backup_storage_info_history_ora_schema,
  ObInnerTableSchema::all_virtual_backup_delete_job_ora_schema,
  ObInnerTableSchema::all_virtual_backup_delete_job_history_ora_schema,
  ObInnerTableSchema::all_virtual_backup_delete_task_ora_schema,
  ObInnerTableSchema::all_virtual_backup_delete_task_history_ora_schema,
  ObInnerTableSchema::all_virtual_backup_delete_policy_ora_schema,
  ObInnerTableSchema::all_virtual_deadlock_event_history_ora_schema,
  ObInnerTableSchema::all_virtual_log_stat_ora_schema,
  ObInnerTableSchema::all_virtual_replay_stat_ora_schema,
  ObInnerTableSchema::all_virtual_apply_stat_ora_schema,
  ObInnerTableSchema::all_virtual_archive_stat_ora_schema,
  ObInnerTableSchema::all_virtual_ls_status_ora_schema,
  ObInnerTableSchema::all_virtual_ls_recovery_stat_ora_schema,
  ObInnerTableSchema::all_virtual_ls_election_reference_info_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_info_ora_schema,
  ObInnerTableSchema::all_virtual_freeze_info_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_ls_replica_task_ora_schema,
  ObInnerTableSchema::all_virtual_ls_replica_task_plan_ora_schema,
  ObInnerTableSchema::all_virtual_show_trace_ora_schema,
  ObInnerTableSchema::all_virtual_database_privilege_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_rls_policy_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_rls_security_column_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_rls_group_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_rls_context_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_rls_attribute_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_rewrite_rules_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_sys_agent_schema,
  ObInnerTableSchema::all_virtual_sql_plan_ora_schema,
  ObInnerTableSchema::all_virtual_trans_scheduler_ora_schema,
  ObInnerTableSchema::all_virtual_ls_arb_replica_task_ora_schema,
  ObInnerTableSchema::all_virtual_ls_arb_replica_task_history_ora_schema,
  ObInnerTableSchema::all_virtual_archive_dest_status_ora_schema,
  ObInnerTableSchema::all_virtual_external_table_file_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_data_dictionary_in_log_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_task_opt_stat_gather_history_ora_schema,
  ObInnerTableSchema::all_virtual_table_opt_stat_gather_history_ora_schema,
  ObInnerTableSchema::all_virtual_opt_stat_gather_monitor_ora_schema,
  ObInnerTableSchema::all_virtual_long_ops_status_sys_agent_schema,
  ObInnerTableSchema::all_virtual_thread_ora_schema,
  ObInnerTableSchema::all_virtual_wr_active_session_history_ora_schema,
  ObInnerTableSchema::all_virtual_wr_snapshot_ora_schema,
  ObInnerTableSchema::all_virtual_wr_statname_ora_schema,
  ObInnerTableSchema::all_virtual_wr_sysstat_ora_schema,
  ObInnerTableSchema::all_virtual_arbitration_member_info_ora_schema,
  ObInnerTableSchema::all_virtual_arbitration_service_status_ora_schema,
  ObInnerTableSchema::all_virtual_obj_lock_ora_schema,
  ObInnerTableSchema::all_virtual_log_restore_source_ora_schema,
  ObInnerTableSchema::all_virtual_balance_job_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_balance_job_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_balance_task_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_balance_task_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_transfer_task_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_transfer_task_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_resource_pool_sys_agent_schema,
  ObInnerTableSchema::all_virtual_px_p2p_datahub_ora_schema,
  ObInnerTableSchema::all_virtual_timestamp_service_ora_schema,
  ObInnerTableSchema::all_virtual_ls_log_restore_status_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_parameter_ora_schema,
  ObInnerTableSchema::all_virtual_mlog_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_mview_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_stats_sys_defaults_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_stats_params_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_run_stats_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_stats_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_change_stats_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_mview_refresh_stmt_stats_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_dbms_lock_allocated_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_wr_control_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_event_history_ora_schema,
  ObInnerTableSchema::all_virtual_ls_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_flt_config_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_job_run_detail_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_session_info_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_scheduler_job_class_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_recover_table_job_ora_schema,
  ObInnerTableSchema::all_virtual_recover_table_job_history_ora_schema,
  ObInnerTableSchema::all_virtual_import_table_job_ora_schema,
  ObInnerTableSchema::all_virtual_import_table_job_history_ora_schema,
  ObInnerTableSchema::all_virtual_import_table_task_ora_schema,
  ObInnerTableSchema::all_virtual_import_table_task_history_ora_schema,
  ObInnerTableSchema::all_virtual_ls_info_ora_schema,
  ObInnerTableSchema::all_virtual_cgroup_config_ora_schema,
  ObInnerTableSchema::all_virtual_aux_stat_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_sys_variable_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_sys_variable_default_value_ora_schema,
  ObInnerTableSchema::all_virtual_transfer_partition_task_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_transfer_partition_task_history_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_ls_snapshot_ora_schema,
  ObInnerTableSchema::all_virtual_index_usage_info_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tracepoint_info_ora_schema,
  ObInnerTableSchema::all_virtual_user_proxy_info_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_user_proxy_role_info_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_resource_limit_ora_schema,
  ObInnerTableSchema::all_virtual_tenant_resource_limit_detail_ora_schema,
  ObInnerTableSchema::all_virtual_nic_info_ora_schema,
  ObInnerTableSchema::all_virtual_scheduler_job_run_detail_v2_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_spatial_reference_systems_real_agent_ora_schema,
  ObInnerTableSchema::all_virtual_table_real_agent_ora_idx_data_table_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_table_real_agent_ora_idx_db_tb_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_table_real_agent_ora_idx_tb_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_column_real_agent_ora_idx_tb_column_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_column_real_agent_ora_idx_column_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_user_real_agent_ora_idx_ur_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_database_real_agent_ora_idx_db_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_tablegroup_real_agent_ora_idx_tg_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_recyclebin_real_agent_ora_idx_recyclebin_db_type_real_agent_schema,
  ObInnerTableSchema::all_virtual_part_real_agent_ora_idx_part_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_sub_part_real_agent_ora_idx_sub_part_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_def_sub_part_real_agent_ora_idx_def_sub_part_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_foreign_key_real_agent_ora_idx_fk_child_tid_real_agent_schema,
  ObInnerTableSchema::all_virtual_foreign_key_real_agent_ora_idx_fk_parent_tid_real_agent_schema,
  ObInnerTableSchema::all_virtual_foreign_key_real_agent_ora_idx_fk_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_synonym_real_agent_ora_idx_db_synonym_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_synonym_real_agent_ora_idx_synonym_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_routine_real_agent_ora_idx_db_routine_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_routine_real_agent_ora_idx_routine_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_routine_real_agent_ora_idx_routine_pkg_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_routine_param_real_agent_ora_idx_routine_param_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_package_real_agent_ora_idx_db_pkg_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_package_real_agent_ora_idx_pkg_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_constraint_real_agent_ora_idx_cst_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_type_real_agent_ora_idx_db_type_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_type_real_agent_ora_idx_type_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_type_attr_real_agent_ora_idx_type_attr_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_coll_type_real_agent_ora_idx_coll_name_type_real_agent_schema,
  ObInnerTableSchema::all_virtual_dblink_real_agent_ora_idx_owner_dblink_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_dblink_real_agent_ora_idx_dblink_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_role_grantee_map_real_agent_ora_idx_grantee_role_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_keystore_real_agent_ora_idx_keystore_master_key_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_policy_real_agent_ora_idx_ols_policy_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_policy_real_agent_ora_idx_ols_policy_col_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_component_real_agent_ora_idx_ols_com_policy_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_label_real_agent_ora_idx_ols_lab_policy_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_label_real_agent_ora_idx_ols_lab_tag_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_label_real_agent_ora_idx_ols_lab_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_user_level_real_agent_ora_idx_ols_level_uid_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_ols_user_level_real_agent_ora_idx_ols_level_policy_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_profile_real_agent_ora_idx_profile_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_security_audit_real_agent_ora_idx_audit_type_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_trigger_real_agent_ora_idx_trigger_base_obj_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_trigger_real_agent_ora_idx_db_trigger_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_trigger_real_agent_ora_idx_trigger_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_objauth_real_agent_ora_idx_objauth_grantor_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_objauth_real_agent_ora_idx_objauth_grantee_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_object_type_real_agent_ora_idx_obj_type_db_obj_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_object_type_real_agent_ora_idx_obj_type_obj_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_dependency_real_agent_ora_idx_dependency_ref_obj_real_agent_schema,
  ObInnerTableSchema::all_virtual_table_stat_history_real_agent_ora_idx_table_stat_his_savtime_real_agent_schema,
  ObInnerTableSchema::all_virtual_column_stat_history_real_agent_ora_idx_column_stat_his_savtime_real_agent_schema,
  ObInnerTableSchema::all_virtual_histogram_stat_history_real_agent_ora_idx_histogram_stat_his_savtime_real_agent_schema,
  ObInnerTableSchema::all_virtual_tablet_to_ls_real_agent_ora_idx_tablet_to_ls_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_tablet_to_ls_real_agent_ora_idx_tablet_to_table_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_context_real_agent_ora_idx_ctx_namespace_real_agent_schema,
  ObInnerTableSchema::all_virtual_plan_baseline_item_real_agent_ora_idx_spm_item_sql_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_plan_baseline_item_real_agent_ora_idx_spm_item_value_real_agent_schema,
  ObInnerTableSchema::all_virtual_tenant_directory_real_agent_ora_idx_directory_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_job_real_agent_ora_idx_job_powner_real_agent_schema,
  ObInnerTableSchema::all_virtual_sequence_object_real_agent_ora_idx_seq_obj_db_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_sequence_object_real_agent_ora_idx_seq_obj_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_recyclebin_real_agent_ora_idx_recyclebin_ori_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_table_privilege_real_agent_ora_idx_tb_priv_db_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_table_privilege_real_agent_ora_idx_tb_priv_tb_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_database_privilege_real_agent_ora_idx_db_priv_db_name_real_agent_schema,
  ObInnerTableSchema::all_virtual_rls_policy_real_agent_ora_idx_rls_policy_table_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_rls_policy_real_agent_ora_idx_rls_policy_group_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_rls_group_real_agent_ora_idx_rls_group_table_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_rls_context_real_agent_ora_idx_rls_context_table_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_dbms_lock_allocated_real_agent_ora_idx_dbms_lock_allocated_lockhandle_real_agent_schema,
  ObInnerTableSchema::all_virtual_dbms_lock_allocated_real_agent_ora_idx_dbms_lock_allocated_expiration_real_agent_schema,
  ObInnerTableSchema::all_virtual_user_proxy_info_real_agent_ora_idx_user_proxy_info_proxy_user_id_real_agent_schema,
  ObInnerTableSchema::all_virtual_scheduler_job_run_detail_v2_real_agent_ora_idx_scheduler_job_run_detail_v2_time_real_agent_schema,
  ObInnerTableSchema::all_virtual_scheduler_job_run_detail_v2_real_agent_ora_idx_scheduler_job_run_detail_v2_job_class_time_real_agent_schema,
  ObInnerTableSchema::all_virtual_ash_ora_all_virtual_ash_i1_schema,
  ObInnerTableSchema::all_virtual_sql_plan_monitor_ora_all_virtual_sql_plan_monitor_i1_schema,
  ObInnerTableSchema::all_virtual_system_event_ora_all_virtual_system_event_i1_schema,
  ObInnerTableSchema::all_virtual_sysstat_ora_all_virtual_sysstat_i1_schema,
  ObInnerTableSchema::all_virtual_sesstat_ora_all_virtual_sesstat_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_ora_all_virtual_session_wait_history_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_ora_all_virtual_session_wait_i1_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_ora_all_virtual_plan_cache_stat_i1_schema,
  ObInnerTableSchema::all_virtual_sql_audit_ora_all_virtual_sql_audit_i1_schema,
  NULL,};

const schema_create_func sys_view_schema_creators [] = {
  ObInnerTableSchema::gv_ob_plan_cache_stat_schema,
  ObInnerTableSchema::gv_ob_plan_cache_plan_stat_schema,
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
  ObInnerTableSchema::engines_schema,
  ObInnerTableSchema::routines_schema,
  ObInnerTableSchema::profiling_schema,
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
  ObInnerTableSchema::gv_ob_sql_audit_schema,
  ObInnerTableSchema::gv_latch_schema,
  ObInnerTableSchema::gv_ob_memory_schema,
  ObInnerTableSchema::v_ob_memory_schema,
  ObInnerTableSchema::gv_ob_memstore_schema,
  ObInnerTableSchema::v_ob_memstore_schema,
  ObInnerTableSchema::gv_ob_memstore_info_schema,
  ObInnerTableSchema::v_ob_memstore_info_schema,
  ObInnerTableSchema::v_ob_plan_cache_stat_schema,
  ObInnerTableSchema::v_ob_plan_cache_plan_stat_schema,
  ObInnerTableSchema::gv_ob_plan_cache_plan_explain_schema,
  ObInnerTableSchema::v_ob_plan_cache_plan_explain_schema,
  ObInnerTableSchema::v_ob_sql_audit_schema,
  ObInnerTableSchema::v_latch_schema,
  ObInnerTableSchema::gv_ob_rpc_outgoing_schema,
  ObInnerTableSchema::v_ob_rpc_outgoing_schema,
  ObInnerTableSchema::gv_ob_rpc_incoming_schema,
  ObInnerTableSchema::v_ob_rpc_incoming_schema,
  ObInnerTableSchema::gv_sql_plan_monitor_schema,
  ObInnerTableSchema::v_sql_plan_monitor_schema,
  ObInnerTableSchema::dba_recyclebin_schema,
  ObInnerTableSchema::time_zone_schema,
  ObInnerTableSchema::time_zone_name_schema,
  ObInnerTableSchema::time_zone_transition_schema,
  ObInnerTableSchema::time_zone_transition_type_schema,
  ObInnerTableSchema::gv_session_longops_schema,
  ObInnerTableSchema::v_session_longops_schema,
  ObInnerTableSchema::dba_ob_sequence_objects_schema,
  ObInnerTableSchema::columns_schema,
  ObInnerTableSchema::gv_ob_px_worker_stat_schema,
  ObInnerTableSchema::v_ob_px_worker_stat_schema,
  ObInnerTableSchema::gv_ob_ps_stat_schema,
  ObInnerTableSchema::v_ob_ps_stat_schema,
  ObInnerTableSchema::gv_ob_ps_item_info_schema,
  ObInnerTableSchema::v_ob_ps_item_info_schema,
  ObInnerTableSchema::gv_sql_workarea_schema,
  ObInnerTableSchema::v_sql_workarea_schema,
  ObInnerTableSchema::gv_sql_workarea_active_schema,
  ObInnerTableSchema::v_sql_workarea_active_schema,
  ObInnerTableSchema::gv_sql_workarea_histogram_schema,
  ObInnerTableSchema::v_sql_workarea_histogram_schema,
  ObInnerTableSchema::gv_ob_sql_workarea_memory_info_schema,
  ObInnerTableSchema::v_ob_sql_workarea_memory_info_schema,
  ObInnerTableSchema::gv_ob_plan_cache_reference_info_schema,
  ObInnerTableSchema::v_ob_plan_cache_reference_info_schema,
  ObInnerTableSchema::gv_ob_sstables_schema,
  ObInnerTableSchema::v_ob_sstables_schema,
  ObInnerTableSchema::cdb_ob_restore_progress_schema,
  ObInnerTableSchema::cdb_ob_restore_history_schema,
  ObInnerTableSchema::gv_ob_server_schema_info_schema,
  ObInnerTableSchema::v_ob_server_schema_info_schema,
  ObInnerTableSchema::v_sql_monitor_statname_schema,
  ObInnerTableSchema::gv_ob_merge_info_schema,
  ObInnerTableSchema::v_ob_merge_info_schema,
  ObInnerTableSchema::v_ob_encrypted_tables_schema,
  ObInnerTableSchema::v_encrypted_tablespaces_schema,
  ObInnerTableSchema::cdb_ob_archivelog_piece_files_schema,
  ObInnerTableSchema::cdb_ob_backup_set_files_schema,
  ObInnerTableSchema::connection_control_failed_login_attempts_schema,
  ObInnerTableSchema::gv_ob_tenant_memory_schema,
  ObInnerTableSchema::v_ob_tenant_memory_schema,
  ObInnerTableSchema::gv_ob_px_target_monitor_schema,
  ObInnerTableSchema::v_ob_px_target_monitor_schema,
  ObInnerTableSchema::column_privileges_schema,
  ObInnerTableSchema::view_table_usage_schema,
  ObInnerTableSchema::cdb_ob_backup_jobs_schema,
  ObInnerTableSchema::cdb_ob_backup_job_history_schema,
  ObInnerTableSchema::cdb_ob_backup_tasks_schema,
  ObInnerTableSchema::cdb_ob_backup_task_history_schema,
  ObInnerTableSchema::files_schema,
  ObInnerTableSchema::dba_ob_tenants_schema,
  ObInnerTableSchema::dba_ob_units_schema,
  ObInnerTableSchema::dba_ob_unit_configs_schema,
  ObInnerTableSchema::dba_ob_resource_pools_schema,
  ObInnerTableSchema::dba_ob_servers_schema,
  ObInnerTableSchema::dba_ob_zones_schema,
  ObInnerTableSchema::dba_ob_rootservice_event_history_schema,
  ObInnerTableSchema::dba_ob_tenant_jobs_schema,
  ObInnerTableSchema::dba_ob_unit_jobs_schema,
  ObInnerTableSchema::dba_ob_server_jobs_schema,
  ObInnerTableSchema::dba_ob_ls_locations_schema,
  ObInnerTableSchema::cdb_ob_ls_locations_schema,
  ObInnerTableSchema::dba_ob_tablet_to_ls_schema,
  ObInnerTableSchema::cdb_ob_tablet_to_ls_schema,
  ObInnerTableSchema::dba_ob_tablet_replicas_schema,
  ObInnerTableSchema::cdb_ob_tablet_replicas_schema,
  ObInnerTableSchema::dba_ob_tablegroups_schema,
  ObInnerTableSchema::cdb_ob_tablegroups_schema,
  ObInnerTableSchema::dba_ob_tablegroup_partitions_schema,
  ObInnerTableSchema::cdb_ob_tablegroup_partitions_schema,
  ObInnerTableSchema::dba_ob_tablegroup_subpartitions_schema,
  ObInnerTableSchema::cdb_ob_tablegroup_subpartitions_schema,
  ObInnerTableSchema::dba_ob_databases_schema,
  ObInnerTableSchema::cdb_ob_databases_schema,
  ObInnerTableSchema::dba_ob_tablegroup_tables_schema,
  ObInnerTableSchema::cdb_ob_tablegroup_tables_schema,
  ObInnerTableSchema::dba_ob_zone_major_compaction_schema,
  ObInnerTableSchema::cdb_ob_zone_major_compaction_schema,
  ObInnerTableSchema::dba_ob_major_compaction_schema,
  ObInnerTableSchema::cdb_ob_major_compaction_schema,
  ObInnerTableSchema::cdb_objects_schema,
  ObInnerTableSchema::cdb_tables_schema,
  ObInnerTableSchema::cdb_tab_cols_v_schema,
  ObInnerTableSchema::cdb_tab_cols_schema,
  ObInnerTableSchema::cdb_indexes_schema,
  ObInnerTableSchema::cdb_ind_columns_schema,
  ObInnerTableSchema::cdb_part_tables_schema,
  ObInnerTableSchema::cdb_tab_partitions_schema,
  ObInnerTableSchema::cdb_tab_subpartitions_schema,
  ObInnerTableSchema::cdb_subpartition_templates_schema,
  ObInnerTableSchema::cdb_part_key_columns_schema,
  ObInnerTableSchema::cdb_subpart_key_columns_schema,
  ObInnerTableSchema::cdb_part_indexes_schema,
  ObInnerTableSchema::cdb_ind_partitions_schema,
  ObInnerTableSchema::cdb_ind_subpartitions_schema,
  ObInnerTableSchema::cdb_tab_col_statistics_schema,
  ObInnerTableSchema::dba_objects_schema,
  ObInnerTableSchema::dba_part_tables_schema,
  ObInnerTableSchema::dba_part_key_columns_schema,
  ObInnerTableSchema::dba_subpart_key_columns_schema,
  ObInnerTableSchema::dba_tab_partitions_schema,
  ObInnerTableSchema::dba_tab_subpartitions_schema,
  ObInnerTableSchema::dba_subpartition_templates_schema,
  ObInnerTableSchema::dba_part_indexes_schema,
  ObInnerTableSchema::dba_ind_partitions_schema,
  ObInnerTableSchema::dba_ind_subpartitions_schema,
  ObInnerTableSchema::gv_ob_servers_schema,
  ObInnerTableSchema::v_ob_servers_schema,
  ObInnerTableSchema::gv_ob_units_schema,
  ObInnerTableSchema::v_ob_units_schema,
  ObInnerTableSchema::gv_ob_parameters_schema,
  ObInnerTableSchema::v_ob_parameters_schema,
  ObInnerTableSchema::gv_ob_processlist_schema,
  ObInnerTableSchema::v_ob_processlist_schema,
  ObInnerTableSchema::gv_ob_kvcache_schema,
  ObInnerTableSchema::v_ob_kvcache_schema,
  ObInnerTableSchema::gv_ob_transaction_participants_schema,
  ObInnerTableSchema::v_ob_transaction_participants_schema,
  ObInnerTableSchema::gv_ob_compaction_progress_schema,
  ObInnerTableSchema::v_ob_compaction_progress_schema,
  ObInnerTableSchema::gv_ob_tablet_compaction_progress_schema,
  ObInnerTableSchema::v_ob_tablet_compaction_progress_schema,
  ObInnerTableSchema::gv_ob_tablet_compaction_history_schema,
  ObInnerTableSchema::v_ob_tablet_compaction_history_schema,
  ObInnerTableSchema::gv_ob_compaction_diagnose_info_schema,
  ObInnerTableSchema::v_ob_compaction_diagnose_info_schema,
  ObInnerTableSchema::gv_ob_compaction_suggestions_schema,
  ObInnerTableSchema::v_ob_compaction_suggestions_schema,
  ObInnerTableSchema::gv_ob_dtl_interm_result_monitor_schema,
  ObInnerTableSchema::v_ob_dtl_interm_result_monitor_schema,
  ObInnerTableSchema::gv_ob_io_calibration_status_schema,
  ObInnerTableSchema::v_ob_io_calibration_status_schema,
  ObInnerTableSchema::gv_ob_io_benchmark_schema,
  ObInnerTableSchema::v_ob_io_benchmark_schema,
  ObInnerTableSchema::cdb_ob_backup_delete_jobs_schema,
  ObInnerTableSchema::cdb_ob_backup_delete_job_history_schema,
  ObInnerTableSchema::cdb_ob_backup_delete_tasks_schema,
  ObInnerTableSchema::cdb_ob_backup_delete_task_history_schema,
  ObInnerTableSchema::cdb_ob_backup_delete_policy_schema,
  ObInnerTableSchema::cdb_ob_backup_storage_info_schema,
  ObInnerTableSchema::dba_tab_statistics_schema,
  ObInnerTableSchema::dba_tab_col_statistics_schema,
  ObInnerTableSchema::dba_part_col_statistics_schema,
  ObInnerTableSchema::dba_subpart_col_statistics_schema,
  ObInnerTableSchema::dba_tab_histograms_schema,
  ObInnerTableSchema::dba_part_histograms_schema,
  ObInnerTableSchema::dba_subpart_histograms_schema,
  ObInnerTableSchema::dba_tab_stats_history_schema,
  ObInnerTableSchema::dba_ind_statistics_schema,
  ObInnerTableSchema::dba_ob_backup_jobs_schema,
  ObInnerTableSchema::dba_ob_backup_job_history_schema,
  ObInnerTableSchema::dba_ob_backup_tasks_schema,
  ObInnerTableSchema::dba_ob_backup_task_history_schema,
  ObInnerTableSchema::dba_ob_backup_set_files_schema,
  ObInnerTableSchema::dba_sql_plan_baselines_schema,
  ObInnerTableSchema::dba_sql_management_config_schema,
  ObInnerTableSchema::gv_active_session_history_schema,
  ObInnerTableSchema::v_active_session_history_schema,
  ObInnerTableSchema::gv_dml_stats_schema,
  ObInnerTableSchema::v_dml_stats_schema,
  ObInnerTableSchema::dba_tab_modifications_schema,
  ObInnerTableSchema::dba_scheduler_jobs_schema,
  ObInnerTableSchema::dba_ob_outline_concurrent_history_schema,
  ObInnerTableSchema::cdb_ob_backup_storage_info_history_schema,
  ObInnerTableSchema::dba_ob_backup_storage_info_schema,
  ObInnerTableSchema::dba_ob_backup_storage_info_history_schema,
  ObInnerTableSchema::dba_ob_backup_delete_policy_schema,
  ObInnerTableSchema::dba_ob_backup_delete_jobs_schema,
  ObInnerTableSchema::dba_ob_backup_delete_job_history_schema,
  ObInnerTableSchema::dba_ob_backup_delete_tasks_schema,
  ObInnerTableSchema::dba_ob_backup_delete_task_history_schema,
  ObInnerTableSchema::dba_ob_outlines_schema,
  ObInnerTableSchema::dba_ob_concurrent_limit_sql_schema,
  ObInnerTableSchema::dba_ob_restore_progress_schema,
  ObInnerTableSchema::dba_ob_restore_history_schema,
  ObInnerTableSchema::dba_ob_archive_dest_schema,
  ObInnerTableSchema::dba_ob_archivelog_schema,
  ObInnerTableSchema::dba_ob_archivelog_summary_schema,
  ObInnerTableSchema::dba_ob_archivelog_piece_files_schema,
  ObInnerTableSchema::dba_ob_backup_parameter_schema,
  ObInnerTableSchema::cdb_ob_archive_dest_schema,
  ObInnerTableSchema::cdb_ob_archivelog_schema,
  ObInnerTableSchema::cdb_ob_archivelog_summary_schema,
  ObInnerTableSchema::cdb_ob_backup_parameter_schema,
  ObInnerTableSchema::dba_ob_deadlock_event_history_schema,
  ObInnerTableSchema::cdb_ob_deadlock_event_history_schema,
  ObInnerTableSchema::cdb_ob_sys_variables_schema,
  ObInnerTableSchema::dba_ob_kv_ttl_tasks_schema,
  ObInnerTableSchema::dba_ob_kv_ttl_task_history_schema,
  ObInnerTableSchema::gv_ob_log_stat_schema,
  ObInnerTableSchema::v_ob_log_stat_schema,
  ObInnerTableSchema::st_geometry_columns_schema,
  ObInnerTableSchema::st_spatial_reference_systems_schema,
  ObInnerTableSchema::query_response_time_schema,
  ObInnerTableSchema::cdb_ob_kv_ttl_tasks_schema,
  ObInnerTableSchema::cdb_ob_kv_ttl_task_history_schema,
  ObInnerTableSchema::dba_rsrc_plans_schema,
  ObInnerTableSchema::dba_rsrc_plan_directives_schema,
  ObInnerTableSchema::dba_rsrc_group_mappings_schema,
  ObInnerTableSchema::dba_rsrc_consumer_groups_schema,
  ObInnerTableSchema::v_rsrc_plan_schema,
  ObInnerTableSchema::cdb_ob_column_checksum_error_info_schema,
  ObInnerTableSchema::cdb_ob_tablet_checksum_error_info_schema,
  ObInnerTableSchema::dba_ob_ls_schema,
  ObInnerTableSchema::cdb_ob_ls_schema,
  ObInnerTableSchema::dba_ob_table_locations_schema,
  ObInnerTableSchema::cdb_ob_table_locations_schema,
  ObInnerTableSchema::dba_ob_server_event_history_schema,
  ObInnerTableSchema::cdb_ob_freeze_info_schema,
  ObInnerTableSchema::dba_ob_freeze_info_schema,
  ObInnerTableSchema::dba_ob_ls_replica_tasks_schema,
  ObInnerTableSchema::cdb_ob_ls_replica_tasks_schema,
  ObInnerTableSchema::v_ob_ls_replica_task_plan_schema,
  ObInnerTableSchema::dba_ob_auto_increment_schema,
  ObInnerTableSchema::cdb_ob_auto_increment_schema,
  ObInnerTableSchema::dba_sequences_schema,
  ObInnerTableSchema::dba_scheduler_windows_schema,
  ObInnerTableSchema::dba_ob_users_schema,
  ObInnerTableSchema::cdb_ob_users_schema,
  ObInnerTableSchema::dba_ob_database_privilege_schema,
  ObInnerTableSchema::cdb_ob_database_privilege_schema,
  ObInnerTableSchema::dba_ob_user_defined_rules_schema,
  ObInnerTableSchema::gv_ob_sql_plan_schema,
  ObInnerTableSchema::v_ob_sql_plan_schema,
  ObInnerTableSchema::dba_ob_cluster_event_history_schema,
  ObInnerTableSchema::parameters_schema,
  ObInnerTableSchema::table_privileges_schema,
  ObInnerTableSchema::user_privileges_schema,
  ObInnerTableSchema::schema_privileges_schema,
  ObInnerTableSchema::check_constraints_schema,
  ObInnerTableSchema::referential_constraints_schema,
  ObInnerTableSchema::table_constraints_schema,
  ObInnerTableSchema::gv_ob_transaction_schedulers_schema,
  ObInnerTableSchema::v_ob_transaction_schedulers_schema,
  ObInnerTableSchema::triggers_schema,
  ObInnerTableSchema::partitions_schema,
  ObInnerTableSchema::dba_ob_arbitration_service_schema,
  ObInnerTableSchema::cdb_ob_ls_arb_replica_tasks_schema,
  ObInnerTableSchema::dba_ob_ls_arb_replica_tasks_schema,
  ObInnerTableSchema::cdb_ob_ls_arb_replica_task_history_schema,
  ObInnerTableSchema::dba_ob_ls_arb_replica_task_history_schema,
  ObInnerTableSchema::v_ob_archive_dest_status_schema,
  ObInnerTableSchema::dba_ob_ls_log_archive_progress_schema,
  ObInnerTableSchema::cdb_ob_ls_log_archive_progress_schema,
  ObInnerTableSchema::dba_ob_rsrc_io_directives_schema,
  ObInnerTableSchema::gv_ob_tablet_stats_schema,
  ObInnerTableSchema::v_ob_tablet_stats_schema,
  ObInnerTableSchema::dba_ob_access_point_schema,
  ObInnerTableSchema::cdb_ob_access_point_schema,
  ObInnerTableSchema::cdb_ob_data_dictionary_in_log_schema,
  ObInnerTableSchema::dba_ob_data_dictionary_in_log_schema,
  ObInnerTableSchema::gv_ob_opt_stat_gather_monitor_schema,
  ObInnerTableSchema::v_ob_opt_stat_gather_monitor_schema,
  ObInnerTableSchema::dba_ob_task_opt_stat_gather_history_schema,
  ObInnerTableSchema::dba_ob_table_opt_stat_gather_history_schema,
  ObInnerTableSchema::gv_ob_thread_schema,
  ObInnerTableSchema::v_ob_thread_schema,
  ObInnerTableSchema::gv_ob_arbitration_member_info_schema,
  ObInnerTableSchema::v_ob_arbitration_member_info_schema,
  ObInnerTableSchema::gv_ob_arbitration_service_status_schema,
  ObInnerTableSchema::v_ob_arbitration_service_status_schema,
  ObInnerTableSchema::dba_wr_active_session_history_schema,
  ObInnerTableSchema::cdb_wr_active_session_history_schema,
  ObInnerTableSchema::dba_wr_snapshot_schema,
  ObInnerTableSchema::cdb_wr_snapshot_schema,
  ObInnerTableSchema::dba_wr_statname_schema,
  ObInnerTableSchema::cdb_wr_statname_schema,
  ObInnerTableSchema::dba_wr_sysstat_schema,
  ObInnerTableSchema::cdb_wr_sysstat_schema,
  ObInnerTableSchema::gv_ob_kv_connections_schema,
  ObInnerTableSchema::v_ob_kv_connections_schema,
  ObInnerTableSchema::gv_ob_locks_schema,
  ObInnerTableSchema::v_ob_locks_schema,
  ObInnerTableSchema::cdb_ob_log_restore_source_schema,
  ObInnerTableSchema::dba_ob_log_restore_source_schema,
  ObInnerTableSchema::v_ob_timestamp_service_schema,
  ObInnerTableSchema::dba_ob_balance_jobs_schema,
  ObInnerTableSchema::cdb_ob_balance_jobs_schema,
  ObInnerTableSchema::dba_ob_balance_job_history_schema,
  ObInnerTableSchema::cdb_ob_balance_job_history_schema,
  ObInnerTableSchema::dba_ob_balance_tasks_schema,
  ObInnerTableSchema::cdb_ob_balance_tasks_schema,
  ObInnerTableSchema::dba_ob_balance_task_history_schema,
  ObInnerTableSchema::cdb_ob_balance_task_history_schema,
  ObInnerTableSchema::dba_ob_transfer_tasks_schema,
  ObInnerTableSchema::cdb_ob_transfer_tasks_schema,
  ObInnerTableSchema::dba_ob_transfer_task_history_schema,
  ObInnerTableSchema::cdb_ob_transfer_task_history_schema,
  ObInnerTableSchema::dba_ob_external_table_files_schema,
  ObInnerTableSchema::all_ob_external_table_files_schema,
  ObInnerTableSchema::gv_ob_px_p2p_datahub_schema,
  ObInnerTableSchema::v_ob_px_p2p_datahub_schema,
  ObInnerTableSchema::gv_sql_join_filter_schema,
  ObInnerTableSchema::v_sql_join_filter_schema,
  ObInnerTableSchema::dba_ob_table_stat_stale_info_schema,
  ObInnerTableSchema::v_ob_ls_log_restore_status_schema,
  ObInnerTableSchema::cdb_ob_external_table_files_schema,
  ObInnerTableSchema::dba_db_links_schema,
  ObInnerTableSchema::dba_wr_control_schema,
  ObInnerTableSchema::cdb_wr_control_schema,
  ObInnerTableSchema::dba_ob_ls_history_schema,
  ObInnerTableSchema::cdb_ob_ls_history_schema,
  ObInnerTableSchema::dba_ob_tenant_event_history_schema,
  ObInnerTableSchema::cdb_ob_tenant_event_history_schema,
  ObInnerTableSchema::gv_ob_flt_trace_config_schema,
  ObInnerTableSchema::gv_ob_session_schema,
  ObInnerTableSchema::v_ob_session_schema,
  ObInnerTableSchema::gv_ob_pl_cache_object_schema,
  ObInnerTableSchema::v_ob_pl_cache_object_schema,
  ObInnerTableSchema::cdb_ob_recover_table_jobs_schema,
  ObInnerTableSchema::dba_ob_recover_table_jobs_schema,
  ObInnerTableSchema::cdb_ob_recover_table_job_history_schema,
  ObInnerTableSchema::dba_ob_recover_table_job_history_schema,
  ObInnerTableSchema::cdb_ob_import_table_jobs_schema,
  ObInnerTableSchema::dba_ob_import_table_jobs_schema,
  ObInnerTableSchema::cdb_ob_import_table_job_history_schema,
  ObInnerTableSchema::dba_ob_import_table_job_history_schema,
  ObInnerTableSchema::cdb_ob_import_table_tasks_schema,
  ObInnerTableSchema::dba_ob_import_table_tasks_schema,
  ObInnerTableSchema::cdb_ob_import_table_task_history_schema,
  ObInnerTableSchema::dba_ob_import_table_task_history_schema,
  ObInnerTableSchema::gv_ob_tenant_runtime_info_schema,
  ObInnerTableSchema::v_ob_tenant_runtime_info_schema,
  ObInnerTableSchema::gv_ob_cgroup_config_schema,
  ObInnerTableSchema::v_ob_cgroup_config_schema,
  ObInnerTableSchema::procs_priv_schema,
  ObInnerTableSchema::dba_ob_aux_statistics_schema,
  ObInnerTableSchema::cdb_ob_aux_statistics_schema,
  ObInnerTableSchema::dba_index_usage_schema,
  ObInnerTableSchema::dba_ob_sys_variables_schema,
  ObInnerTableSchema::dba_ob_transfer_partition_tasks_schema,
  ObInnerTableSchema::cdb_ob_transfer_partition_tasks_schema,
  ObInnerTableSchema::dba_ob_transfer_partition_task_history_schema,
  ObInnerTableSchema::cdb_ob_transfer_partition_task_history_schema,
  ObInnerTableSchema::dba_ob_trusted_root_certificate_schema,
  ObInnerTableSchema::dba_ob_clone_progress_schema,
  ObInnerTableSchema::role_edges_schema,
  ObInnerTableSchema::default_roles_schema,
  ObInnerTableSchema::cdb_index_usage_schema,
  ObInnerTableSchema::columns_priv_schema,
  ObInnerTableSchema::gv_ob_ls_snapshots_schema,
  ObInnerTableSchema::v_ob_ls_snapshots_schema,
  ObInnerTableSchema::dba_ob_clone_history_schema,
  ObInnerTableSchema::cdb_mview_logs_schema,
  ObInnerTableSchema::dba_mview_logs_schema,
  ObInnerTableSchema::cdb_mviews_schema,
  ObInnerTableSchema::dba_mviews_schema,
  ObInnerTableSchema::cdb_mvref_stats_sys_defaults_schema,
  ObInnerTableSchema::dba_mvref_stats_sys_defaults_schema,
  ObInnerTableSchema::cdb_mvref_stats_params_schema,
  ObInnerTableSchema::dba_mvref_stats_params_schema,
  ObInnerTableSchema::cdb_mvref_run_stats_schema,
  ObInnerTableSchema::dba_mvref_run_stats_schema,
  ObInnerTableSchema::cdb_mvref_stats_schema,
  ObInnerTableSchema::dba_mvref_stats_schema,
  ObInnerTableSchema::cdb_mvref_change_stats_schema,
  ObInnerTableSchema::dba_mvref_change_stats_schema,
  ObInnerTableSchema::cdb_mvref_stmt_stats_schema,
  ObInnerTableSchema::dba_mvref_stmt_stats_schema,
  ObInnerTableSchema::gv_ob_tracepoint_info_schema,
  ObInnerTableSchema::v_ob_tracepoint_info_schema,
  ObInnerTableSchema::v_ob_compatibility_control_schema,
  ObInnerTableSchema::gv_ob_tenant_resource_limit_schema,
  ObInnerTableSchema::v_ob_tenant_resource_limit_schema,
  ObInnerTableSchema::gv_ob_tenant_resource_limit_detail_schema,
  ObInnerTableSchema::v_ob_tenant_resource_limit_detail_schema,
  ObInnerTableSchema::tablespaces_schema,
  ObInnerTableSchema::innodb_buffer_page_schema,
  ObInnerTableSchema::innodb_buffer_page_lru_schema,
  ObInnerTableSchema::innodb_buffer_pool_stats_schema,
  ObInnerTableSchema::innodb_cmp_schema,
  ObInnerTableSchema::innodb_cmp_per_index_schema,
  ObInnerTableSchema::innodb_cmp_per_index_reset_schema,
  ObInnerTableSchema::innodb_cmp_reset_schema,
  ObInnerTableSchema::innodb_cmpmem_schema,
  ObInnerTableSchema::innodb_cmpmem_reset_schema,
  ObInnerTableSchema::innodb_sys_datafiles_schema,
  ObInnerTableSchema::innodb_sys_indexes_schema,
  ObInnerTableSchema::innodb_sys_tables_schema,
  ObInnerTableSchema::innodb_sys_tablespaces_schema,
  ObInnerTableSchema::innodb_sys_tablestats_schema,
  ObInnerTableSchema::innodb_sys_virtual_schema,
  ObInnerTableSchema::innodb_temp_table_info_schema,
  ObInnerTableSchema::innodb_metrics_schema,
  ObInnerTableSchema::events_schema,
  ObInnerTableSchema::v_ob_nic_info_schema,
  ObInnerTableSchema::gv_ob_nic_info_schema,
  ObInnerTableSchema::dba_scheduler_job_run_details_schema,
  ObInnerTableSchema::cdb_scheduler_job_run_details_schema,
  ObInnerTableSchema::dba_synonyms_schema,
  ObInnerTableSchema::dba_objects_ora_schema,
  ObInnerTableSchema::all_objects_schema,
  ObInnerTableSchema::user_objects_schema,
  ObInnerTableSchema::dba_sequences_ora_schema,
  ObInnerTableSchema::all_sequences_ora_schema,
  ObInnerTableSchema::user_sequences_ora_schema,
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
  ObInnerTableSchema::dba_part_key_columns_ora_schema,
  ObInnerTableSchema::all_part_key_columns_schema,
  ObInnerTableSchema::user_part_key_columns_schema,
  ObInnerTableSchema::dba_subpart_key_columns_ora_schema,
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
  ObInnerTableSchema::dba_part_indexes_ora_schema,
  ObInnerTableSchema::all_part_indexes_ora_schema,
  ObInnerTableSchema::user_part_indexes_ora_schema,
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
  ObInnerTableSchema::dba_ind_partitions_ora_schema,
  ObInnerTableSchema::dba_ind_subpartitions_ora_schema,
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
  ObInnerTableSchema::audit_actions_ora_schema,
  ObInnerTableSchema::stmt_audit_option_map_ora_schema,
  ObInnerTableSchema::all_def_audit_opts_ora_schema,
  ObInnerTableSchema::dba_stmt_audit_opts_ora_schema,
  ObInnerTableSchema::dba_obj_audit_opts_ora_schema,
  ObInnerTableSchema::dba_audit_trail_ora_schema,
  ObInnerTableSchema::user_audit_trail_ora_schema,
  ObInnerTableSchema::dba_audit_exists_ora_schema,
  ObInnerTableSchema::dba_audit_session_ora_schema,
  ObInnerTableSchema::user_audit_session_ora_schema,
  ObInnerTableSchema::dba_audit_statement_ora_schema,
  ObInnerTableSchema::user_audit_statement_ora_schema,
  ObInnerTableSchema::dba_audit_object_ora_schema,
  ObInnerTableSchema::user_audit_object_ora_schema,
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
  ObInnerTableSchema::dba_rsrc_plans_ora_schema,
  ObInnerTableSchema::dba_rsrc_plan_directives_ora_schema,
  ObInnerTableSchema::dba_rsrc_group_mappings_ora_schema,
  ObInnerTableSchema::dba_recyclebin_ora_schema,
  ObInnerTableSchema::user_recyclebin_ora_schema,
  ObInnerTableSchema::dba_rsrc_consumer_groups_ora_schema,
  ObInnerTableSchema::dba_ob_ls_locations_ora_schema,
  ObInnerTableSchema::dba_ob_tablet_to_ls_ora_schema,
  ObInnerTableSchema::dba_ob_tablet_replicas_ora_schema,
  ObInnerTableSchema::dba_ob_tablegroups_ora_schema,
  ObInnerTableSchema::dba_ob_tablegroup_partitions_ora_schema,
  ObInnerTableSchema::dba_ob_tablegroup_subpartitions_ora_schema,
  ObInnerTableSchema::dba_ob_databases_ora_schema,
  ObInnerTableSchema::dba_ob_tablegroup_tables_ora_schema,
  ObInnerTableSchema::dba_ob_zone_major_compaction_ora_schema,
  ObInnerTableSchema::dba_ob_major_compaction_ora_schema,
  ObInnerTableSchema::all_ind_statistics_ora_schema,
  ObInnerTableSchema::dba_ind_statistics_ora_schema,
  ObInnerTableSchema::user_ind_statistics_ora_schema,
  ObInnerTableSchema::dba_ob_backup_jobs_ora_schema,
  ObInnerTableSchema::dba_ob_backup_job_history_ora_schema,
  ObInnerTableSchema::dba_ob_backup_tasks_ora_schema,
  ObInnerTableSchema::dba_ob_backup_task_history_ora_schema,
  ObInnerTableSchema::dba_ob_backup_set_files_ora_schema,
  ObInnerTableSchema::all_tab_modifications_ora_schema,
  ObInnerTableSchema::dba_tab_modifications_ora_schema,
  ObInnerTableSchema::user_tab_modifications_ora_schema,
  ObInnerTableSchema::dba_ob_backup_storage_info_ora_schema,
  ObInnerTableSchema::dba_ob_backup_storage_info_history_ora_schema,
  ObInnerTableSchema::dba_ob_backup_delete_policy_ora_schema,
  ObInnerTableSchema::dba_ob_backup_delete_jobs_ora_schema,
  ObInnerTableSchema::dba_ob_backup_delete_job_history_ora_schema,
  ObInnerTableSchema::dba_ob_backup_delete_tasks_ora_schema,
  ObInnerTableSchema::dba_ob_backup_delete_task_history_ora_schema,
  ObInnerTableSchema::dba_ob_restore_progress_ora_schema,
  ObInnerTableSchema::dba_ob_restore_history_ora_schema,
  ObInnerTableSchema::dba_ob_archive_dest_ora_schema,
  ObInnerTableSchema::dba_ob_archivelog_ora_schema,
  ObInnerTableSchema::dba_ob_archivelog_summary_ora_schema,
  ObInnerTableSchema::dba_ob_archivelog_piece_files_ora_schema,
  ObInnerTableSchema::dba_ob_backup_parameter_ora_schema,
  ObInnerTableSchema::dba_ob_freeze_info_ora_schema,
  ObInnerTableSchema::dba_ob_ls_replica_tasks_ora_schema,
  ObInnerTableSchema::v_ob_ls_replica_task_plan_ora_schema,
  ObInnerTableSchema::all_scheduler_windows_ora_schema,
  ObInnerTableSchema::dba_scheduler_windows_ora_schema,
  ObInnerTableSchema::dba_ob_database_privilege_ora_schema,
  ObInnerTableSchema::dba_ob_tenants_ora_schema,
  ObInnerTableSchema::dba_policies_ora_schema,
  ObInnerTableSchema::all_policies_ora_schema,
  ObInnerTableSchema::user_policies_ora_schema,
  ObInnerTableSchema::dba_policy_groups_ora_schema,
  ObInnerTableSchema::all_policy_groups_ora_schema,
  ObInnerTableSchema::user_policy_groups_ora_schema,
  ObInnerTableSchema::dba_policy_contexts_ora_schema,
  ObInnerTableSchema::all_policy_contexts_ora_schema,
  ObInnerTableSchema::user_policy_contexts_ora_schema,
  ObInnerTableSchema::dba_sec_relevant_cols_ora_schema,
  ObInnerTableSchema::all_sec_relevant_cols_ora_schema,
  ObInnerTableSchema::user_sec_relevant_cols_ora_schema,
  ObInnerTableSchema::dba_ob_ls_arb_replica_tasks_ora_schema,
  ObInnerTableSchema::dba_ob_ls_arb_replica_task_history_ora_schema,
  ObInnerTableSchema::dba_ob_rsrc_io_directives_ora_schema,
  ObInnerTableSchema::all_db_links_ora_schema,
  ObInnerTableSchema::dba_db_links_ora_schema,
  ObInnerTableSchema::user_db_links_ora_schema,
  ObInnerTableSchema::dba_ob_task_opt_stat_gather_history_ora_schema,
  ObInnerTableSchema::dba_ob_table_opt_stat_gather_history_ora_schema,
  ObInnerTableSchema::dba_wr_active_session_history_ora_schema,
  ObInnerTableSchema::dba_wr_snapshot_ora_schema,
  ObInnerTableSchema::dba_wr_statname_ora_schema,
  ObInnerTableSchema::dba_wr_sysstat_ora_schema,
  ObInnerTableSchema::dba_ob_log_restore_source_ora_schema,
  ObInnerTableSchema::dba_ob_external_table_files_ora_schema,
  ObInnerTableSchema::all_ob_external_table_files_ora_schema,
  ObInnerTableSchema::dba_ob_balance_jobs_ora_schema,
  ObInnerTableSchema::dba_ob_balance_job_history_ora_schema,
  ObInnerTableSchema::dba_ob_balance_tasks_ora_schema,
  ObInnerTableSchema::dba_ob_balance_task_history_ora_schema,
  ObInnerTableSchema::dba_ob_transfer_tasks_ora_schema,
  ObInnerTableSchema::dba_ob_transfer_task_history_ora_schema,
  ObInnerTableSchema::gv_ob_px_p2p_datahub_ora_schema,
  ObInnerTableSchema::v_ob_px_p2p_datahub_ora_schema,
  ObInnerTableSchema::gv_sql_join_filter_ora_schema,
  ObInnerTableSchema::v_sql_join_filter_ora_schema,
  ObInnerTableSchema::dba_ob_table_stat_stale_info_ora_schema,
  ObInnerTableSchema::dbms_lock_allocated_ora_schema,
  ObInnerTableSchema::dba_wr_control_ora_schema,
  ObInnerTableSchema::dba_ob_ls_history_ora_schema,
  ObInnerTableSchema::dba_ob_tenant_event_history_ora_schema,
  ObInnerTableSchema::dba_scheduler_job_run_details_ora_schema,
  ObInnerTableSchema::dba_scheduler_job_classes_schema,
  ObInnerTableSchema::dba_ob_recover_table_jobs_ora_schema,
  ObInnerTableSchema::dba_ob_recover_table_job_history_ora_schema,
  ObInnerTableSchema::dba_ob_import_table_jobs_ora_schema,
  ObInnerTableSchema::dba_ob_import_table_job_history_ora_schema,
  ObInnerTableSchema::dba_ob_import_table_tasks_ora_schema,
  ObInnerTableSchema::dba_ob_import_table_task_history_ora_schema,
  ObInnerTableSchema::dba_ob_transfer_partition_tasks_ora_schema,
  ObInnerTableSchema::dba_ob_transfer_partition_task_history_ora_schema,
  ObInnerTableSchema::user_users_schema,
  ObInnerTableSchema::dba_mview_logs_ora_schema,
  ObInnerTableSchema::all_mview_logs_ora_schema,
  ObInnerTableSchema::user_mview_logs_ora_schema,
  ObInnerTableSchema::dba_mviews_ora_schema,
  ObInnerTableSchema::all_mviews_ora_schema,
  ObInnerTableSchema::user_mviews_ora_schema,
  ObInnerTableSchema::dba_mvref_stats_sys_defaults_ora_schema,
  ObInnerTableSchema::user_mvref_stats_sys_defaults_ora_schema,
  ObInnerTableSchema::dba_mvref_stats_params_ora_schema,
  ObInnerTableSchema::user_mvref_stats_params_ora_schema,
  ObInnerTableSchema::dba_mvref_run_stats_ora_schema,
  ObInnerTableSchema::user_mvref_run_stats_ora_schema,
  ObInnerTableSchema::dba_mvref_stats_ora_schema,
  ObInnerTableSchema::user_mvref_stats_ora_schema,
  ObInnerTableSchema::dba_mvref_change_stats_ora_schema,
  ObInnerTableSchema::user_mvref_change_stats_ora_schema,
  ObInnerTableSchema::dba_mvref_stmt_stats_ora_schema,
  ObInnerTableSchema::user_mvref_stmt_stats_ora_schema,
  ObInnerTableSchema::proxy_users_schema,
  ObInnerTableSchema::gv_ob_sql_audit_ora_schema,
  ObInnerTableSchema::v_ob_sql_audit_ora_schema,
  ObInnerTableSchema::gv_instance_schema,
  ObInnerTableSchema::v_instance_schema,
  ObInnerTableSchema::gv_ob_plan_cache_plan_stat_ora_schema,
  ObInnerTableSchema::v_ob_plan_cache_plan_stat_ora_schema,
  ObInnerTableSchema::gv_ob_plan_cache_plan_explain_ora_schema,
  ObInnerTableSchema::v_ob_plan_cache_plan_explain_ora_schema,
  ObInnerTableSchema::gv_session_wait_ora_schema,
  ObInnerTableSchema::v_session_wait_ora_schema,
  ObInnerTableSchema::gv_session_wait_history_ora_schema,
  ObInnerTableSchema::v_session_wait_history_ora_schema,
  ObInnerTableSchema::gv_ob_memory_ora_schema,
  ObInnerTableSchema::v_ob_memory_ora_schema,
  ObInnerTableSchema::gv_ob_memstore_ora_schema,
  ObInnerTableSchema::v_ob_memstore_ora_schema,
  ObInnerTableSchema::gv_ob_memstore_info_ora_schema,
  ObInnerTableSchema::v_ob_memstore_info_ora_schema,
  ObInnerTableSchema::gv_sesstat_ora_schema,
  ObInnerTableSchema::v_sesstat_ora_schema,
  ObInnerTableSchema::gv_sysstat_ora_schema,
  ObInnerTableSchema::v_sysstat_ora_schema,
  ObInnerTableSchema::gv_system_event_ora_schema,
  ObInnerTableSchema::v_system_event_ora_schema,
  ObInnerTableSchema::gv_ob_plan_cache_stat_ora_schema,
  ObInnerTableSchema::v_ob_plan_cache_stat_ora_schema,
  ObInnerTableSchema::nls_session_parameters_ora_schema,
  ObInnerTableSchema::nls_instance_parameters_ora_schema,
  ObInnerTableSchema::nls_database_parameters_ora_schema,
  ObInnerTableSchema::v_nls_parameters_ora_schema,
  ObInnerTableSchema::v_version_ora_schema,
  ObInnerTableSchema::gv_ob_px_worker_stat_ora_schema,
  ObInnerTableSchema::v_ob_px_worker_stat_ora_schema,
  ObInnerTableSchema::gv_ob_ps_stat_ora_schema,
  ObInnerTableSchema::v_ob_ps_stat_ora_schema,
  ObInnerTableSchema::gv_ob_ps_item_info_ora_schema,
  ObInnerTableSchema::v_ob_ps_item_info_ora_schema,
  ObInnerTableSchema::gv_sql_workarea_active_ora_schema,
  ObInnerTableSchema::v_sql_workarea_active_ora_schema,
  ObInnerTableSchema::gv_sql_workarea_histogram_ora_schema,
  ObInnerTableSchema::v_sql_workarea_histogram_ora_schema,
  ObInnerTableSchema::gv_ob_sql_workarea_memory_info_ora_schema,
  ObInnerTableSchema::v_ob_sql_workarea_memory_info_ora_schema,
  ObInnerTableSchema::gv_ob_plan_cache_reference_info_ora_schema,
  ObInnerTableSchema::v_ob_plan_cache_reference_info_ora_schema,
  ObInnerTableSchema::gv_sql_workarea_ora_schema,
  ObInnerTableSchema::v_sql_workarea_ora_schema,
  ObInnerTableSchema::gv_ob_sstables_ora_schema,
  ObInnerTableSchema::v_ob_sstables_ora_schema,
  ObInnerTableSchema::gv_ob_server_schema_info_ora_schema,
  ObInnerTableSchema::v_ob_server_schema_info_ora_schema,
  ObInnerTableSchema::gv_sql_plan_monitor_ora_schema,
  ObInnerTableSchema::v_sql_plan_monitor_ora_schema,
  ObInnerTableSchema::v_sql_monitor_statname_ora_schema,
  ObInnerTableSchema::gv_open_cursor_ora_schema,
  ObInnerTableSchema::v_open_cursor_ora_schema,
  ObInnerTableSchema::v_timezone_names_ora_schema,
  ObInnerTableSchema::gv_global_transaction_ora_schema,
  ObInnerTableSchema::v_global_transaction_ora_schema,
  ObInnerTableSchema::v_rsrc_plan_ora_schema,
  ObInnerTableSchema::v_ob_encrypted_tables_ora_schema,
  ObInnerTableSchema::v_encrypted_tablespaces_ora_schema,
  ObInnerTableSchema::all_tab_col_statistics_ora_schema,
  ObInnerTableSchema::dba_tab_col_statistics_ora_schema,
  ObInnerTableSchema::user_tab_col_statistics_ora_schema,
  ObInnerTableSchema::all_part_col_statistics_ora_schema,
  ObInnerTableSchema::dba_part_col_statistics_ora_schema,
  ObInnerTableSchema::user_part_col_statistics_ora_schema,
  ObInnerTableSchema::all_subpart_col_statistics_ora_schema,
  ObInnerTableSchema::dba_subpart_col_statistics_ora_schema,
  ObInnerTableSchema::user_subpart_col_statistics_ora_schema,
  ObInnerTableSchema::all_tab_histograms_ora_schema,
  ObInnerTableSchema::dba_tab_histograms_ora_schema,
  ObInnerTableSchema::user_tab_histograms_ora_schema,
  ObInnerTableSchema::all_part_histograms_ora_schema,
  ObInnerTableSchema::dba_part_histograms_ora_schema,
  ObInnerTableSchema::user_part_histograms_ora_schema,
  ObInnerTableSchema::all_subpart_histograms_ora_schema,
  ObInnerTableSchema::dba_subpart_histograms_ora_schema,
  ObInnerTableSchema::user_subpart_histograms_ora_schema,
  ObInnerTableSchema::all_tab_statistics_ora_schema,
  ObInnerTableSchema::dba_tab_statistics_ora_schema,
  ObInnerTableSchema::user_tab_statistics_ora_schema,
  ObInnerTableSchema::dba_jobs_schema,
  ObInnerTableSchema::user_jobs_schema,
  ObInnerTableSchema::dba_jobs_running_schema,
  ObInnerTableSchema::all_directories_schema,
  ObInnerTableSchema::dba_directories_schema,
  ObInnerTableSchema::gv_ob_tenant_memory_ora_schema,
  ObInnerTableSchema::v_ob_tenant_memory_ora_schema,
  ObInnerTableSchema::gv_ob_px_target_monitor_ora_schema,
  ObInnerTableSchema::v_ob_px_target_monitor_ora_schema,
  ObInnerTableSchema::all_tab_stats_history_ora_schema,
  ObInnerTableSchema::dba_tab_stats_history_ora_schema,
  ObInnerTableSchema::user_tab_stats_history_ora_schema,
  ObInnerTableSchema::gv_dblink_schema,
  ObInnerTableSchema::v_dblink_schema,
  ObInnerTableSchema::dba_scheduler_jobs_ora_schema,
  ObInnerTableSchema::dba_scheduler_program_schema,
  ObInnerTableSchema::dba_context_schema,
  ObInnerTableSchema::v_globalcontext_schema,
  ObInnerTableSchema::gv_ob_units_ora_schema,
  ObInnerTableSchema::v_ob_units_ora_schema,
  ObInnerTableSchema::gv_ob_parameters_ora_schema,
  ObInnerTableSchema::v_ob_parameters_ora_schema,
  ObInnerTableSchema::gv_ob_processlist_ora_schema,
  ObInnerTableSchema::v_ob_processlist_ora_schema,
  ObInnerTableSchema::gv_ob_kvcache_ora_schema,
  ObInnerTableSchema::v_ob_kvcache_ora_schema,
  ObInnerTableSchema::gv_ob_transaction_participants_ora_schema,
  ObInnerTableSchema::v_ob_transaction_participants_ora_schema,
  ObInnerTableSchema::gv_ob_compaction_progress_ora_schema,
  ObInnerTableSchema::v_ob_compaction_progress_ora_schema,
  ObInnerTableSchema::gv_ob_tablet_compaction_progress_ora_schema,
  ObInnerTableSchema::v_ob_tablet_compaction_progress_ora_schema,
  ObInnerTableSchema::gv_ob_tablet_compaction_history_ora_schema,
  ObInnerTableSchema::v_ob_tablet_compaction_history_ora_schema,
  ObInnerTableSchema::gv_ob_compaction_diagnose_info_ora_schema,
  ObInnerTableSchema::v_ob_compaction_diagnose_info_ora_schema,
  ObInnerTableSchema::gv_ob_compaction_suggestions_ora_schema,
  ObInnerTableSchema::v_ob_compaction_suggestions_ora_schema,
  ObInnerTableSchema::gv_ob_dtl_interm_result_monitor_ora_schema,
  ObInnerTableSchema::v_ob_dtl_interm_result_monitor_ora_schema,
  ObInnerTableSchema::dba_sql_plan_baselines_ora_schema,
  ObInnerTableSchema::dba_sql_management_config_ora_schema,
  ObInnerTableSchema::v_event_name_ora_schema,
  ObInnerTableSchema::gv_active_session_history_ora_schema,
  ObInnerTableSchema::v_active_session_history_ora_schema,
  ObInnerTableSchema::gv_dml_stats_ora_schema,
  ObInnerTableSchema::v_dml_stats_ora_schema,
  ObInnerTableSchema::dba_ob_outline_concurrent_history_ora_schema,
  ObInnerTableSchema::dba_ob_outlines_ora_schema,
  ObInnerTableSchema::dba_ob_concurrent_limit_sql_ora_schema,
  ObInnerTableSchema::dba_ob_deadlock_event_history_ora_schema,
  ObInnerTableSchema::gv_ob_log_stat_ora_schema,
  ObInnerTableSchema::v_ob_log_stat_ora_schema,
  ObInnerTableSchema::gv_ob_global_transaction_ora_schema,
  ObInnerTableSchema::v_ob_global_transaction_ora_schema,
  ObInnerTableSchema::dba_ob_ls_ora_schema,
  ObInnerTableSchema::dba_ob_table_locations_ora_schema,
  ObInnerTableSchema::all_trigger_ordering_schema,
  ObInnerTableSchema::dba_trigger_ordering_schema,
  ObInnerTableSchema::user_trigger_ordering_schema,
  ObInnerTableSchema::gv_ob_transaction_schedulers_ora_schema,
  ObInnerTableSchema::v_ob_transaction_schedulers_ora_schema,
  ObInnerTableSchema::dba_ob_user_defined_rules_ora_schema,
  ObInnerTableSchema::gv_ob_sql_plan_ora_schema,
  ObInnerTableSchema::v_ob_sql_plan_ora_schema,
  ObInnerTableSchema::v_ob_archive_dest_status_ora_schema,
  ObInnerTableSchema::dba_ob_ls_log_archive_progress_ora_schema,
  ObInnerTableSchema::gv_ob_locks_ora_schema,
  ObInnerTableSchema::v_ob_locks_ora_schema,
  ObInnerTableSchema::dba_ob_access_point_ora_schema,
  ObInnerTableSchema::dba_ob_data_dictionary_in_log_ora_schema,
  ObInnerTableSchema::gv_ob_opt_stat_gather_monitor_ora_schema,
  ObInnerTableSchema::v_ob_opt_stat_gather_monitor_ora_schema,
  ObInnerTableSchema::gv_session_longops_ora_schema,
  ObInnerTableSchema::v_session_longops_ora_schema,
  ObInnerTableSchema::gv_ob_thread_ora_schema,
  ObInnerTableSchema::v_ob_thread_ora_schema,
  ObInnerTableSchema::gv_ob_arbitration_member_info_ora_schema,
  ObInnerTableSchema::v_ob_arbitration_member_info_ora_schema,
  ObInnerTableSchema::gv_ob_arbitration_service_status_ora_schema,
  ObInnerTableSchema::v_ob_arbitration_service_status_ora_schema,
  ObInnerTableSchema::v_ob_timestamp_service_ora_schema,
  ObInnerTableSchema::v_ob_ls_log_restore_status_ora_schema,
  ObInnerTableSchema::gv_ob_flt_trace_config_ora_schema,
  ObInnerTableSchema::gv_ob_session_ora_schema,
  ObInnerTableSchema::v_ob_session_ora_schema,
  ObInnerTableSchema::gv_ob_pl_cache_object_ora_schema,
  ObInnerTableSchema::v_ob_pl_cache_object_ora_schema,
  ObInnerTableSchema::gv_ob_cgroup_config_ora_schema,
  ObInnerTableSchema::v_ob_cgroup_config_ora_schema,
  ObInnerTableSchema::dba_ob_aux_statistics_ora_schema,
  ObInnerTableSchema::dba_ob_sys_variables_ora_schema,
  ObInnerTableSchema::dba_index_usage_ora_schema,
  ObInnerTableSchema::gv_ob_ls_snapshots_ora_schema,
  ObInnerTableSchema::v_ob_ls_snapshots_ora_schema,
  ObInnerTableSchema::gv_ob_tracepoint_info_ora_schema,
  ObInnerTableSchema::v_ob_tracepoint_info_ora_schema,
  ObInnerTableSchema::gv_ob_tenant_resource_limit_ora_schema,
  ObInnerTableSchema::v_ob_tenant_resource_limit_ora_schema,
  ObInnerTableSchema::gv_ob_tenant_resource_limit_detail_ora_schema,
  ObInnerTableSchema::v_ob_tenant_resource_limit_detail_ora_schema,
  ObInnerTableSchema::gv_ob_nic_info_ora_schema,
  ObInnerTableSchema::v_ob_nic_info_ora_schema,
  ObInnerTableSchema::dba_ob_spatial_columns_ora_schema,
  NULL,};

const schema_create_func core_index_table_schema_creators [] = {
  ObInnerTableSchema::all_table_idx_data_table_id_schema,
  ObInnerTableSchema::all_table_idx_db_tb_name_schema,
  ObInnerTableSchema::all_table_idx_tb_name_schema,
  ObInnerTableSchema::all_column_idx_tb_column_name_schema,
  ObInnerTableSchema::all_column_idx_column_name_schema,
  ObInnerTableSchema::all_ddl_operation_idx_ddl_type_schema,
  NULL,};

const schema_create_func sys_index_table_schema_creators [] = {
  ObInnerTableSchema::all_table_history_idx_data_table_id_schema,
  ObInnerTableSchema::all_log_archive_piece_files_idx_status_schema,
  ObInnerTableSchema::all_backup_set_files_idx_status_schema,
  ObInnerTableSchema::all_ddl_task_status_idx_task_key_schema,
  ObInnerTableSchema::all_user_idx_ur_name_schema,
  ObInnerTableSchema::all_database_idx_db_name_schema,
  ObInnerTableSchema::all_tablegroup_idx_tg_name_schema,
  ObInnerTableSchema::all_tenant_history_idx_tenant_deleted_schema,
  ObInnerTableSchema::all_rootservice_event_history_idx_rs_module_schema,
  ObInnerTableSchema::all_rootservice_event_history_idx_rs_event_schema,
  ObInnerTableSchema::all_recyclebin_idx_recyclebin_db_type_schema,
  ObInnerTableSchema::all_part_idx_part_name_schema,
  ObInnerTableSchema::all_sub_part_idx_sub_part_name_schema,
  ObInnerTableSchema::all_def_sub_part_idx_def_sub_part_name_schema,
  ObInnerTableSchema::all_server_event_history_idx_server_module_schema,
  ObInnerTableSchema::all_server_event_history_idx_server_event_schema,
  ObInnerTableSchema::all_rootservice_job_idx_rs_job_type_schema,
  ObInnerTableSchema::all_foreign_key_idx_fk_child_tid_schema,
  ObInnerTableSchema::all_foreign_key_idx_fk_parent_tid_schema,
  ObInnerTableSchema::all_foreign_key_idx_fk_name_schema,
  ObInnerTableSchema::all_foreign_key_history_idx_fk_his_child_tid_schema,
  ObInnerTableSchema::all_foreign_key_history_idx_fk_his_parent_tid_schema,
  ObInnerTableSchema::all_synonym_idx_db_synonym_name_schema,
  ObInnerTableSchema::all_synonym_idx_synonym_name_schema,
  ObInnerTableSchema::all_ddl_checksum_idx_ddl_checksum_task_schema,
  ObInnerTableSchema::all_routine_idx_db_routine_name_schema,
  ObInnerTableSchema::all_routine_idx_routine_name_schema,
  ObInnerTableSchema::all_routine_idx_routine_pkg_id_schema,
  ObInnerTableSchema::all_routine_param_idx_routine_param_name_schema,
  ObInnerTableSchema::all_package_idx_db_pkg_name_schema,
  ObInnerTableSchema::all_package_idx_pkg_name_schema,
  ObInnerTableSchema::all_acquired_snapshot_idx_snapshot_tablet_schema,
  ObInnerTableSchema::all_constraint_idx_cst_name_schema,
  ObInnerTableSchema::all_type_idx_db_type_name_schema,
  ObInnerTableSchema::all_type_idx_type_name_schema,
  ObInnerTableSchema::all_type_attr_idx_type_attr_name_schema,
  ObInnerTableSchema::all_coll_type_idx_coll_name_type_schema,
  ObInnerTableSchema::all_dblink_idx_owner_dblink_name_schema,
  ObInnerTableSchema::all_dblink_idx_dblink_name_schema,
  ObInnerTableSchema::all_tenant_role_grantee_map_idx_grantee_role_id_schema,
  ObInnerTableSchema::all_tenant_role_grantee_map_history_idx_grantee_his_role_id_schema,
  ObInnerTableSchema::all_tenant_keystore_idx_keystore_master_key_id_schema,
  ObInnerTableSchema::all_tenant_keystore_history_idx_keystore_his_master_key_id_schema,
  ObInnerTableSchema::all_tenant_ols_policy_idx_ols_policy_name_schema,
  ObInnerTableSchema::all_tenant_ols_policy_idx_ols_policy_col_name_schema,
  ObInnerTableSchema::all_tenant_ols_component_idx_ols_com_policy_id_schema,
  ObInnerTableSchema::all_tenant_ols_label_idx_ols_lab_policy_id_schema,
  ObInnerTableSchema::all_tenant_ols_label_idx_ols_lab_tag_schema,
  ObInnerTableSchema::all_tenant_ols_label_idx_ols_lab_schema,
  ObInnerTableSchema::all_tenant_ols_user_level_idx_ols_level_uid_schema,
  ObInnerTableSchema::all_tenant_ols_user_level_idx_ols_level_policy_id_schema,
  ObInnerTableSchema::all_tenant_profile_idx_profile_name_schema,
  ObInnerTableSchema::all_tenant_security_audit_idx_audit_type_schema,
  ObInnerTableSchema::all_tenant_trigger_idx_trigger_base_obj_id_schema,
  ObInnerTableSchema::all_tenant_trigger_idx_db_trigger_name_schema,
  ObInnerTableSchema::all_tenant_trigger_idx_trigger_name_schema,
  ObInnerTableSchema::all_tenant_trigger_history_idx_trigger_his_base_obj_id_schema,
  ObInnerTableSchema::all_tenant_objauth_idx_objauth_grantor_schema,
  ObInnerTableSchema::all_tenant_objauth_idx_objauth_grantee_schema,
  ObInnerTableSchema::all_tenant_object_type_idx_obj_type_db_obj_name_schema,
  ObInnerTableSchema::all_tenant_object_type_idx_obj_type_obj_name_schema,
  ObInnerTableSchema::all_tenant_global_transaction_idx_xa_trans_id_schema,
  ObInnerTableSchema::all_tenant_dependency_idx_dependency_ref_obj_schema,
  ObInnerTableSchema::all_ddl_error_message_idx_ddl_error_object_schema,
  ObInnerTableSchema::all_table_stat_history_idx_table_stat_his_savtime_schema,
  ObInnerTableSchema::all_column_stat_history_idx_column_stat_his_savtime_schema,
  ObInnerTableSchema::all_histogram_stat_history_idx_histogram_stat_his_savtime_schema,
  ObInnerTableSchema::all_tablet_to_ls_idx_tablet_to_ls_id_schema,
  ObInnerTableSchema::all_tablet_to_ls_idx_tablet_to_table_id_schema,
  ObInnerTableSchema::all_pending_transaction_idx_pending_tx_id_schema,
  ObInnerTableSchema::all_context_idx_ctx_namespace_schema,
  ObInnerTableSchema::all_plan_baseline_item_idx_spm_item_sql_id_schema,
  ObInnerTableSchema::all_plan_baseline_item_idx_spm_item_value_schema,
  ObInnerTableSchema::all_tenant_directory_idx_directory_name_schema,
  ObInnerTableSchema::all_job_idx_job_powner_schema,
  ObInnerTableSchema::all_sequence_object_idx_seq_obj_db_name_schema,
  ObInnerTableSchema::all_sequence_object_idx_seq_obj_name_schema,
  ObInnerTableSchema::all_recyclebin_idx_recyclebin_ori_name_schema,
  ObInnerTableSchema::all_table_privilege_idx_tb_priv_db_name_schema,
  ObInnerTableSchema::all_table_privilege_idx_tb_priv_tb_name_schema,
  ObInnerTableSchema::all_database_privilege_idx_db_priv_db_name_schema,
  ObInnerTableSchema::all_rls_policy_idx_rls_policy_table_id_schema,
  ObInnerTableSchema::all_rls_policy_idx_rls_policy_group_id_schema,
  ObInnerTableSchema::all_rls_policy_history_idx_rls_policy_his_table_id_schema,
  ObInnerTableSchema::all_rls_group_idx_rls_group_table_id_schema,
  ObInnerTableSchema::all_rls_group_history_idx_rls_group_his_table_id_schema,
  ObInnerTableSchema::all_rls_context_idx_rls_context_table_id_schema,
  ObInnerTableSchema::all_rls_context_history_idx_rls_context_his_table_id_schema,
  ObInnerTableSchema::all_tenant_snapshot_idx_tenant_snapshot_name_schema,
  ObInnerTableSchema::all_dbms_lock_allocated_idx_dbms_lock_allocated_lockhandle_schema,
  ObInnerTableSchema::all_dbms_lock_allocated_idx_dbms_lock_allocated_expiration_schema,
  ObInnerTableSchema::all_kv_ttl_task_idx_kv_ttl_task_table_id_schema,
  ObInnerTableSchema::all_kv_ttl_task_history_idx_kv_ttl_task_history_upd_time_schema,
  ObInnerTableSchema::all_mview_refresh_run_stats_idx_mview_refresh_run_stats_num_mvs_current_schema,
  ObInnerTableSchema::all_mview_refresh_stats_idx_mview_refresh_stats_end_time_schema,
  ObInnerTableSchema::all_mview_refresh_stats_idx_mview_refresh_stats_mview_end_time_schema,
  ObInnerTableSchema::all_transfer_partition_task_idx_transfer_partition_key_schema,
  ObInnerTableSchema::all_client_to_server_session_info_idx_client_to_server_session_info_client_session_id_schema,
  ObInnerTableSchema::all_column_privilege_idx_column_privilege_name_schema,
  ObInnerTableSchema::all_user_proxy_info_idx_user_proxy_info_proxy_user_id_schema,
  ObInnerTableSchema::all_user_proxy_info_history_idx_user_proxy_info_proxy_user_id_history_schema,
  ObInnerTableSchema::all_user_proxy_role_info_history_idx_user_proxy_role_info_proxy_user_id_history_schema,
  ObInnerTableSchema::all_scheduler_job_run_detail_v2_idx_scheduler_job_run_detail_v2_time_schema,
  ObInnerTableSchema::all_scheduler_job_run_detail_v2_idx_scheduler_job_run_detail_v2_job_class_time_schema,
  NULL,};

const schema_create_func information_schema_table_schema_creators[] = {
  NULL,};

const schema_create_func mysql_table_schema_creators[] = {
  NULL,};

const uint64_t tenant_space_tables [] = {
  OB_ALL_CORE_TABLE_TID,
  OB_ALL_TABLE_TID,
  OB_ALL_COLUMN_TID,
  OB_ALL_DDL_OPERATION_TID,
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
  OB_HELP_TOPIC_TID,
  OB_HELP_CATEGORY_TID,
  OB_HELP_KEYWORD_TID,
  OB_HELP_RELATION_TID,
  OB_ALL_DUMMY_TID,
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
  OB_ALL_RESTORE_JOB_TID,
  OB_ALL_RESTORE_JOB_HISTORY_TID,
  OB_ALL_DDL_ID_TID,
  OB_ALL_FOREIGN_KEY_TID,
  OB_ALL_FOREIGN_KEY_HISTORY_TID,
  OB_ALL_FOREIGN_KEY_COLUMN_TID,
  OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID,
  OB_ALL_SYNONYM_TID,
  OB_ALL_SYNONYM_HISTORY_TID,
  OB_ALL_AUTO_INCREMENT_TID,
  OB_ALL_DDL_CHECKSUM_TID,
  OB_ALL_ROUTINE_TID,
  OB_ALL_ROUTINE_HISTORY_TID,
  OB_ALL_ROUTINE_PARAM_TID,
  OB_ALL_ROUTINE_PARAM_HISTORY_TID,
  OB_ALL_PACKAGE_TID,
  OB_ALL_PACKAGE_HISTORY_TID,
  OB_ALL_ACQUIRED_SNAPSHOT_TID,
  OB_ALL_CONSTRAINT_TID,
  OB_ALL_CONSTRAINT_HISTORY_TID,
  OB_ALL_ORI_SCHEMA_VERSION_TID,
  OB_ALL_FUNC_TID,
  OB_ALL_FUNC_HISTORY_TID,
  OB_ALL_TEMP_TABLE_TID,
  OB_ALL_SEQUENCE_OBJECT_TID,
  OB_ALL_SEQUENCE_OBJECT_HISTORY_TID,
  OB_ALL_SEQUENCE_VALUE_TID,
  OB_ALL_TYPE_TID,
  OB_ALL_TYPE_HISTORY_TID,
  OB_ALL_TYPE_ATTR_TID,
  OB_ALL_TYPE_ATTR_HISTORY_TID,
  OB_ALL_COLL_TYPE_TID,
  OB_ALL_COLL_TYPE_HISTORY_TID,
  OB_ALL_WEAK_READ_SERVICE_TID,
  OB_ALL_DBLINK_TID,
  OB_ALL_DBLINK_HISTORY_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID,
  OB_ALL_TENANT_KEYSTORE_TID,
  OB_ALL_TENANT_KEYSTORE_HISTORY_TID,
  OB_ALL_TENANT_OLS_POLICY_TID,
  OB_ALL_TENANT_OLS_POLICY_HISTORY_TID,
  OB_ALL_TENANT_OLS_COMPONENT_TID,
  OB_ALL_TENANT_OLS_COMPONENT_HISTORY_TID,
  OB_ALL_TENANT_OLS_LABEL_TID,
  OB_ALL_TENANT_OLS_LABEL_HISTORY_TID,
  OB_ALL_TENANT_OLS_USER_LEVEL_TID,
  OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_TID,
  OB_ALL_TENANT_TABLESPACE_TID,
  OB_ALL_TENANT_TABLESPACE_HISTORY_TID,
  OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID,
  OB_ALL_TENANT_PROFILE_TID,
  OB_ALL_TENANT_PROFILE_HISTORY_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TID,
  OB_ALL_TENANT_TRIGGER_TID,
  OB_ALL_TENANT_TRIGGER_HISTORY_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TID,
  OB_ALL_TENANT_SYSAUTH_TID,
  OB_ALL_TENANT_SYSAUTH_HISTORY_TID,
  OB_ALL_TENANT_OBJAUTH_TID,
  OB_ALL_TENANT_OBJAUTH_HISTORY_TID,
  OB_ALL_RESTORE_INFO_TID,
  OB_ALL_TENANT_ERROR_TID,
  OB_ALL_RESTORE_PROGRESS_TID,
  OB_ALL_TENANT_OBJECT_TYPE_TID,
  OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TID,
  OB_ALL_TENANT_TIME_ZONE_TID,
  OB_ALL_TENANT_TIME_ZONE_NAME_TID,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_TID,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TID,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_TID,
  OB_ALL_TENANT_DEPENDENCY_TID,
  OB_ALL_RES_MGR_PLAN_TID,
  OB_ALL_RES_MGR_DIRECTIVE_TID,
  OB_ALL_RES_MGR_MAPPING_RULE_TID,
  OB_ALL_DDL_ERROR_MESSAGE_TID,
  OB_ALL_BACKUP_SET_FILES_TID,
  OB_ALL_RES_MGR_CONSUMER_GROUP_TID,
  OB_ALL_BACKUP_INFO_TID,
  OB_ALL_DDL_TASK_STATUS_TID,
  OB_ALL_DEADLOCK_EVENT_HISTORY_TID,
  OB_ALL_COLUMN_USAGE_TID,
  OB_ALL_JOB_TID,
  OB_ALL_JOB_LOG_TID,
  OB_ALL_TENANT_DIRECTORY_TID,
  OB_ALL_TENANT_DIRECTORY_HISTORY_TID,
  OB_ALL_TABLE_STAT_TID,
  OB_ALL_COLUMN_STAT_TID,
  OB_ALL_HISTOGRAM_STAT_TID,
  OB_ALL_MONITOR_MODIFIED_TID,
  OB_ALL_TABLE_STAT_HISTORY_TID,
  OB_ALL_COLUMN_STAT_HISTORY_TID,
  OB_ALL_HISTOGRAM_STAT_HISTORY_TID,
  OB_ALL_OPTSTAT_GLOBAL_PREFS_TID,
  OB_ALL_OPTSTAT_USER_PREFS_TID,
  OB_ALL_LS_META_TABLE_TID,
  OB_ALL_TABLET_TO_LS_TID,
  OB_ALL_TABLET_META_TABLE_TID,
  OB_ALL_LS_STATUS_TID,
  OB_ALL_LOG_ARCHIVE_PROGRESS_TID,
  OB_ALL_LOG_ARCHIVE_HISTORY_TID,
  OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID,
  OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TID,
  OB_ALL_LS_TID,
  OB_ALL_BACKUP_STORAGE_INFO_TID,
  OB_ALL_DAM_LAST_ARCH_TS_TID,
  OB_ALL_DAM_CLEANUP_JOBS_TID,
  OB_ALL_BACKUP_JOB_TID,
  OB_ALL_BACKUP_JOB_HISTORY_TID,
  OB_ALL_BACKUP_TASK_TID,
  OB_ALL_BACKUP_TASK_HISTORY_TID,
  OB_ALL_BACKUP_LS_TASK_TID,
  OB_ALL_BACKUP_LS_TASK_HISTORY_TID,
  OB_ALL_BACKUP_LS_TASK_INFO_TID,
  OB_ALL_BACKUP_SKIPPED_TABLET_TID,
  OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_TID,
  OB_ALL_TENANT_INFO_TID,
  OB_ALL_TABLET_TO_TABLE_HISTORY_TID,
  OB_ALL_LS_RECOVERY_STAT_TID,
  OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_TID,
  OB_ALL_TABLET_REPLICA_CHECKSUM_TID,
  OB_ALL_TABLET_CHECKSUM_TID,
  OB_ALL_LS_REPLICA_TASK_TID,
  OB_ALL_PENDING_TRANSACTION_TID,
  OB_ALL_BALANCE_GROUP_LS_STAT_TID,
  OB_ALL_TENANT_SCHEDULER_JOB_TID,
  OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TID,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_TID,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TID,
  OB_ALL_CONTEXT_TID,
  OB_ALL_CONTEXT_HISTORY_TID,
  OB_ALL_GLOBAL_CONTEXT_VALUE_TID,
  OB_ALL_LS_ELECTION_REFERENCE_INFO_TID,
  OB_ALL_BACKUP_DELETE_JOB_TID,
  OB_ALL_BACKUP_DELETE_JOB_HISTORY_TID,
  OB_ALL_BACKUP_DELETE_TASK_TID,
  OB_ALL_BACKUP_DELETE_TASK_HISTORY_TID,
  OB_ALL_BACKUP_DELETE_LS_TASK_TID,
  OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_TID,
  OB_ALL_ZONE_MERGE_INFO_TID,
  OB_ALL_MERGE_INFO_TID,
  OB_ALL_FREEZE_INFO_TID,
  OB_ALL_PLAN_BASELINE_TID,
  OB_ALL_PLAN_BASELINE_ITEM_TID,
  OB_ALL_SPM_CONFIG_TID,
  OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TID,
  OB_ALL_BACKUP_PARAMETER_TID,
  OB_ALL_LS_RESTORE_PROGRESS_TID,
  OB_ALL_LS_RESTORE_HISTORY_TID,
  OB_ALL_BACKUP_STORAGE_INFO_HISTORY_TID,
  OB_ALL_BACKUP_DELETE_POLICY_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TID,
  OB_ALL_LOG_RESTORE_SOURCE_TID,
  OB_ALL_KV_TTL_TASK_TID,
  OB_ALL_KV_TTL_TASK_HISTORY_TID,
  OB_ALL_SERVICE_EPOCH_TID,
  OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TID,
  OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TID,
  OB_ALL_COLUMN_GROUP_TID,
  OB_ALL_COLUMN_GROUP_HISTORY_TID,
  OB_ALL_COLUMN_GROUP_MAPPING_TID,
  OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TID,
  OB_ALL_TRANSFER_TASK_TID,
  OB_ALL_TRANSFER_TASK_HISTORY_TID,
  OB_ALL_BALANCE_JOB_TID,
  OB_ALL_BALANCE_JOB_HISTORY_TID,
  OB_ALL_BALANCE_TASK_TID,
  OB_ALL_BALANCE_TASK_HISTORY_TID,
  OB_ALL_LS_ARB_REPLICA_TASK_TID,
  OB_ALL_DATA_DICTIONARY_IN_LOG_TID,
  OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_TID,
  OB_ALL_RLS_POLICY_TID,
  OB_ALL_RLS_POLICY_HISTORY_TID,
  OB_ALL_RLS_SECURITY_COLUMN_TID,
  OB_ALL_RLS_SECURITY_COLUMN_HISTORY_TID,
  OB_ALL_RLS_GROUP_TID,
  OB_ALL_RLS_GROUP_HISTORY_TID,
  OB_ALL_RLS_CONTEXT_TID,
  OB_ALL_RLS_CONTEXT_HISTORY_TID,
  OB_ALL_RLS_ATTRIBUTE_TID,
  OB_ALL_RLS_ATTRIBUTE_HISTORY_TID,
  OB_ALL_TENANT_REWRITE_RULES_TID,
  OB_ALL_RESERVED_SNAPSHOT_TID,
  OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TID,
  OB_ALL_EXTERNAL_TABLE_FILE_TID,
  OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_TID,
  OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TID,
  OB_WR_ACTIVE_SESSION_HISTORY_TID,
  OB_WR_SNAPSHOT_TID,
  OB_WR_STATNAME_TID,
  OB_WR_SYSSTAT_TID,
  OB_ALL_BALANCE_TASK_HELPER_TID,
  OB_ALL_TENANT_SNAPSHOT_TID,
  OB_ALL_TENANT_SNAPSHOT_LS_TID,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TID,
  OB_ALL_MLOG_TID,
  OB_ALL_MVIEW_TID,
  OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TID,
  OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TID,
  OB_ALL_MVIEW_REFRESH_RUN_STATS_TID,
  OB_ALL_MVIEW_REFRESH_STATS_TID,
  OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TID,
  OB_ALL_MVIEW_REFRESH_STMT_STATS_TID,
  OB_ALL_DBMS_LOCK_ALLOCATED_TID,
  OB_WR_CONTROL_TID,
  OB_ALL_TENANT_EVENT_HISTORY_TID,
  OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TID,
  OB_ALL_RECOVER_TABLE_JOB_TID,
  OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID,
  OB_ALL_IMPORT_TABLE_JOB_TID,
  OB_ALL_IMPORT_TABLE_JOB_HISTORY_TID,
  OB_ALL_IMPORT_TABLE_TASK_TID,
  OB_ALL_IMPORT_TABLE_TASK_HISTORY_TID,
  OB_ALL_CLONE_JOB_TID,
  OB_ALL_CLONE_JOB_HISTORY_TID,
  OB_ALL_ROUTINE_PRIVILEGE_TID,
  OB_ALL_ROUTINE_PRIVILEGE_HISTORY_TID,
  OB_ALL_NCOMP_DLL_TID,
  OB_ALL_AUX_STAT_TID,
  OB_ALL_INDEX_USAGE_INFO_TID,
  OB_ALL_DETECT_LOCK_INFO_TID,
  OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TID,
  OB_ALL_TRANSFER_PARTITION_TASK_TID,
  OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_TID,
  OB_ALL_TENANT_SNAPSHOT_JOB_TID,
  OB_ALL_COLUMN_PRIVILEGE_TID,
  OB_ALL_COLUMN_PRIVILEGE_HISTORY_TID,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TID,
  OB_ALL_USER_PROXY_INFO_TID,
  OB_ALL_USER_PROXY_INFO_HISTORY_TID,
  OB_ALL_USER_PROXY_ROLE_INFO_TID,
  OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TID,
  OB_ALL_MVIEW_DEP_TID,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TID,
  OB_TENANT_VIRTUAL_ALL_TABLE_TID,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_TID,
  OB_TENANT_VIRTUAL_TABLE_INDEX_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID,
  OB_ALL_VIRTUAL_PROCESSLIST_TID,
  OB_TENANT_VIRTUAL_WARNING_TID,
  OB_TENANT_VIRTUAL_CURRENT_TENANT_TID,
  OB_TENANT_VIRTUAL_DATABASE_STATUS_TID,
  OB_TENANT_VIRTUAL_TENANT_STATUS_TID,
  OB_TENANT_VIRTUAL_STATNAME_TID,
  OB_TENANT_VIRTUAL_EVENT_NAME_TID,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID,
  OB_TENANT_VIRTUAL_SHOW_TABLES_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TID,
  OB_ALL_VIRTUAL_CORE_META_TABLE_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID,
  OB_ALL_VIRTUAL_PLAN_STAT_TID,
  OB_ALL_VIRTUAL_LATCH_TID,
  OB_ALL_VIRTUAL_KVCACHE_INFO_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_TID,
  OB_ALL_VIRTUAL_SESSION_EVENT_TID,
  OB_ALL_VIRTUAL_SESSION_EVENT_ALL_VIRTUAL_SESSION_EVENT_I1_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_ALL_VIRTUAL_SESSION_WAIT_I1_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TID,
  OB_ALL_VIRTUAL_SESSTAT_TID,
  OB_ALL_VIRTUAL_SESSTAT_ALL_VIRTUAL_SESSTAT_I1_TID,
  OB_ALL_VIRTUAL_SYSSTAT_TID,
  OB_ALL_VIRTUAL_SYSSTAT_ALL_VIRTUAL_SYSSTAT_I1_TID,
  OB_ALL_VIRTUAL_MEMSTORE_INFO_TID,
  OB_ALL_VIRTUAL_TRANS_STAT_TID,
  OB_ALL_VIRTUAL_TRANS_SCHEDULER_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_ALL_VIRTUAL_SQL_AUDIT_I1_TID,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID,
  OB_ALL_VIRTUAL_MEMORY_INFO_TID,
  OB_ALL_VIRTUAL_TRACE_SPAN_INFO_TID,
  OB_ALL_VIRTUAL_ENGINE_TID,
  OB_ALL_VIRTUAL_PROXY_SCHEMA_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_ALL_VIRTUAL_OBRPC_STAT_TID,
  OB_TENANT_VIRTUAL_OUTLINE_TID,
  OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID,
  OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID,
  OB_ALL_VIRTUAL_PROXY_PARTITION_TID,
  OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TID,
  OB_ALL_VIRTUAL_AUDIT_OPERATION_TID,
  OB_ALL_VIRTUAL_AUDIT_ACTION_TID,
  OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_TID,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_TID,
  OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_TID,
  OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_TID,
  OB_ALL_VIRTUAL_SESSION_INFO_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_TID,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_EVENT_HISTORY_TID,
  OB_ALL_VIRTUAL_TABLET_STAT_TID,
  OB_SESSION_VARIABLES_TID,
  OB_GLOBAL_STATUS_TID,
  OB_SESSION_STATUS_TID,
  OB_USER_TID,
  OB_DB_TID,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TID,
  OB_PROC_TID,
  OB_TENANT_VIRTUAL_COLLATION_TID,
  OB_TENANT_VIRTUAL_CHARSET_TID,
  OB_ALL_VIRTUAL_TABLE_MGR_TID,
  OB_ALL_VIRTUAL_FREEZE_INFO_TID,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_TID,
  OB_ALL_VIRTUAL_TABLE_TID,
  OB_ALL_VIRTUAL_USER_TID,
  OB_ALL_VIRTUAL_TENANT_TABLESPACE_TID,
  OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TID,
  OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_TID,
  OB_ALL_VIRTUAL_PS_STAT_TID,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_TID,
  OB_ALL_VIRTUAL_BACKUP_INFO_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TID,
  OB_ALL_VIRTUAL_OPEN_CURSOR_TID,
  OB_ALL_VIRTUAL_FILES_TID,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID,
  OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TID,
  OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_TID,
  OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_TID,
  OB_ALL_VIRTUAL_PX_TARGET_MONITOR_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_TID,
  OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_TID,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_TID,
  OB_ALL_VIRTUAL_LS_STATUS_TID,
  OB_ALL_VIRTUAL_LS_META_TABLE_TID,
  OB_ALL_VIRTUAL_TABLET_META_TABLE_TID,
  OB_ALL_VIRTUAL_BACKUP_TASK_TID,
  OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_LOG_STAT_TID,
  OB_ALL_VIRTUAL_TENANT_INFO_TID,
  OB_ALL_VIRTUAL_LS_RECOVERY_STAT_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_TID,
  OB_ALL_VIRTUAL_UNIT_TID,
  OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_TID,
  OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_TID,
  OB_ALL_VIRTUAL_ARCHIVE_STAT_TID,
  OB_ALL_VIRTUAL_APPLY_STAT_TID,
  OB_ALL_VIRTUAL_REPLAY_STAT_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_LS_INFO_TID,
  OB_ALL_VIRTUAL_OBJ_LOCK_TID,
  OB_ALL_VIRTUAL_ZONE_MERGE_INFO_TID,
  OB_ALL_VIRTUAL_MERGE_INFO_TID,
  OB_ALL_VIRTUAL_TRANSACTION_FREEZE_CHECKPOINT_TID,
  OB_ALL_VIRTUAL_TRANSACTION_CHECKPOINT_TID,
  OB_ALL_VIRTUAL_CHECKPOINT_TID,
  OB_ALL_VIRTUAL_BACKUP_SET_FILES_TID,
  OB_ALL_VIRTUAL_BACKUP_JOB_TID,
  OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_ASH_TID,
  OB_ALL_VIRTUAL_ASH_ALL_VIRTUAL_ASH_I1_TID,
  OB_ALL_VIRTUAL_DML_STATS_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_TID,
  OB_ALL_VIRTUAL_BACKUP_PARAMETER_TID,
  OB_ALL_VIRTUAL_RESTORE_JOB_TID,
  OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_RESTORE_PROGRESS_TID,
  OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_TID,
  OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_TID,
  OB_ALL_VIRTUAL_PRIVILEGE_TID,
  OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_TID,
  OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_TID,
  OB_ALL_VIRTUAL_KV_TTL_TASK_TID,
  OB_ALL_VIRTUAL_KV_TTL_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_INFO_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_TID,
  OB_ALL_VIRTUAL_SHOW_TRACE_TID,
  OB_ALL_VIRTUAL_TENANT_MYSQL_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_TID,
  OB_ALL_VIRTUAL_MALLOC_SAMPLE_INFO_TID,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_TID,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_TID,
  OB_ALL_VIRTUAL_MDS_NODE_STAT_TID,
  OB_ALL_VIRTUAL_MDS_EVENT_HISTORY_TID,
  OB_ALL_VIRTUAL_DUP_LS_LEASE_MGR_TID,
  OB_ALL_VIRTUAL_DUP_LS_TABLET_SET_TID,
  OB_ALL_VIRTUAL_DUP_LS_TABLETS_TID,
  OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_TID,
  OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_TID,
  OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_TID,
  OB_ALL_VIRTUAL_THREAD_TID,
  OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_TID,
  OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_TID,
  OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_TID,
  OB_ALL_VIRTUAL_WR_SNAPSHOT_TID,
  OB_ALL_VIRTUAL_WR_STATNAME_TID,
  OB_ALL_VIRTUAL_WR_SYSSTAT_TID,
  OB_ALL_VIRTUAL_KV_CONNECTION_TID,
  OB_ALL_VIRTUAL_VIRTUAL_LONG_OPS_STATUS_MYSQL_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TID,
  OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_TID,
  OB_ALL_VIRTUAL_RESOURCE_POOL_MYSQL_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_PX_P2P_DATAHUB_TID,
  OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_TID,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_TID,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_TID,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_TID,
  OB_ALL_VIRTUAL_MLOG_TID,
  OB_ALL_VIRTUAL_MVIEW_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_TID,
  OB_ALL_VIRTUAL_WR_CONTROL_TID,
  OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_TID,
  OB_ALL_VIRTUAL_CGROUP_CONFIG_TID,
  OB_ALL_VIRTUAL_FLT_CONFIG_TID,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_TID,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_TID,
  OB_ALL_VIRTUAL_COLUMN_GROUP_HISTORY_TID,
  OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_HISTORY_TID,
  OB_ALL_VIRTUAL_CLONE_JOB_TID,
  OB_ALL_VIRTUAL_CLONE_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_MEMTABLE_INFO_TID,
  OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_CHECKPOINT_UNIT_INFO_TID,
  OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_INFO_TID,
  OB_ALL_VIRTUAL_DETECT_LOCK_INFO_TID,
  OB_ALL_VIRTUAL_CLIENT_TO_SERVER_SESSION_INFO_TID,
  OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_TID,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_JOB_TID,
  OB_ALL_VIRTUAL_LS_SNAPSHOT_TID,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TID,
  OB_ENABLED_ROLES_TID,
  OB_ALL_VIRTUAL_TRACEPOINT_INFO_TID,
  OB_ALL_VIRTUAL_COMPATIBILITY_CONTROL_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_TID,
  OB_ALL_VIRTUAL_NIC_INFO_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_ALL_VIRTUAL_SQL_AUDIT_I1_TID,
  OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID,
  OB_TENANT_VIRTUAL_OUTLINE_AGENT_TID,
  OB_ALL_VIRTUAL_PRIVILEGE_ORA_TID,
  OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TID,
  OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TID,
  OB_TENANT_VIRTUAL_CHARSET_AGENT_TID,
  OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID,
  OB_TENANT_VIRTUAL_COLLATION_ORA_TID,
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
  OB_ALL_VIRTUAL_SESSTAT_ORA_TID,
  OB_ALL_VIRTUAL_SESSTAT_ORA_ALL_VIRTUAL_SESSTAT_I1_TID,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_TID,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_ALL_VIRTUAL_SYSSTAT_I1_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORA_TID,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORA_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORA_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_ORA_TID,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_ORA_TID,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TID,
  OB_ALL_VIRTUAL_TRACE_SPAN_INFO_ORA_TID,
  OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_ORA_TID,
  OB_ALL_VIRTUAL_AUDIT_OPERATION_ORA_TID,
  OB_ALL_VIRTUAL_AUDIT_ACTION_ORA_TID,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PS_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_ORA_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_ORA_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_MGR_ORA_TID,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_ORA_TID,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORA_TID,
  OB_ALL_VIRTUAL_OPEN_CURSOR_ORA_TID,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_ORA_TID,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_TYPE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_AUTO_INCREMENT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TID,
  OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_ORA_TID,
  OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_COLUMN_USAGE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_JOB_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_JOB_LOG_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_STAT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_ORA_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_ORA_TID,
  OB_ALL_VIRTUAL_PX_TARGET_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_MONITOR_MODIFIED_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_OPTSTAT_USER_PREFS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DBLINK_INFO_ORA_TID,
  OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_CONTEXT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_ORA_TID,
  OB_ALL_VIRTUAL_TRANS_STAT_ORA_TID,
  OB_ALL_VIRTUAL_UNIT_ORA_TID,
  OB_ALL_VIRTUAL_KVCACHE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_LS_META_TABLE_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_META_TABLE_ORA_TID,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_ORA_TID,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_ORA_TID,
  OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_PROXY_SCHEMA_ORA_TID,
  OB_ALL_VIRTUAL_PROXY_PARTITION_ORA_TID,
  OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_ORA_TID,
  OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_ORA_TID,
  OB_ALL_VIRTUAL_ZONE_MERGE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_MERGE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_BACKUP_JOB_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_TASK_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_SET_FILES_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_BASELINE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SPM_CONFIG_REAL_AGENT_ORA_TID,
  OB_TENANT_VIRTUAL_EVENT_NAME_ORA_TID,
  OB_ALL_VIRTUAL_ASH_ORA_TID,
  OB_ALL_VIRTUAL_ASH_ORA_ALL_VIRTUAL_ASH_I1_TID,
  OB_ALL_VIRTUAL_DML_STATS_ORA_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_ORA_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_ORA_TID,
  OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_PARAMETER_ORA_TID,
  OB_ALL_VIRTUAL_RESTORE_JOB_ORA_TID,
  OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_RESTORE_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_OUTLINE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_OUTLINE_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_ORA_TID,
  OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_LOG_STAT_ORA_TID,
  OB_ALL_VIRTUAL_REPLAY_STAT_ORA_TID,
  OB_ALL_VIRTUAL_APPLY_STAT_ORA_TID,
  OB_ALL_VIRTUAL_ARCHIVE_STAT_ORA_TID,
  OB_ALL_VIRTUAL_LS_STATUS_ORA_TID,
  OB_ALL_VIRTUAL_LS_RECOVERY_STAT_ORA_TID,
  OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_INFO_ORA_TID,
  OB_ALL_VIRTUAL_FREEZE_INFO_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_ORA_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_ORA_TID,
  OB_ALL_VIRTUAL_SHOW_TRACE_ORA_TID,
  OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RLS_ATTRIBUTE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_REWRITE_RULES_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_ORA_TID,
  OB_ALL_VIRTUAL_TRANS_SCHEDULER_ORA_TID,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_ORA_TID,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_ORA_TID,
  OB_ALL_VIRTUAL_EXTERNAL_TABLE_FILE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_LONG_OPS_STATUS_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_THREAD_ORA_TID,
  OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_WR_SNAPSHOT_ORA_TID,
  OB_ALL_VIRTUAL_WR_STATNAME_ORA_TID,
  OB_ALL_VIRTUAL_WR_SYSSTAT_ORA_TID,
  OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_ORA_TID,
  OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_ORA_TID,
  OB_ALL_VIRTUAL_OBJ_LOCK_ORA_TID,
  OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_ORA_TID,
  OB_ALL_VIRTUAL_BALANCE_JOB_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_BALANCE_TASK_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRANSFER_TASK_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RESOURCE_POOL_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_PX_P2P_DATAHUB_ORA_TID,
  OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_ORA_TID,
  OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_ORA_TID,
  OB_ALL_VIRTUAL_MLOG_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_MVIEW_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_WR_CONTROL_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_LS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_FLT_CONFIG_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SESSION_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_ORA_TID,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_ORA_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_ORA_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_LS_INFO_ORA_TID,
  OB_ALL_VIRTUAL_CGROUP_CONFIG_ORA_TID,
  OB_ALL_VIRTUAL_AUX_STAT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SYS_VARIABLE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_ORA_TID,
  OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_LS_SNAPSHOT_ORA_TID,
  OB_ALL_VIRTUAL_INDEX_USAGE_INFO_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRACEPOINT_INFO_ORA_TID,
  OB_ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID,
  OB_ALL_VIRTUAL_NIC_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_REAL_AGENT_ORA_TID,
  OB_GV_OB_PLAN_CACHE_STAT_TID,
  OB_GV_OB_PLAN_CACHE_PLAN_STAT_TID,
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
  OB_ENGINES_TID,
  OB_ROUTINES_TID,
  OB_PROFILING_TID,
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
  OB_GV_OB_SQL_AUDIT_TID,
  OB_GV_LATCH_TID,
  OB_GV_OB_MEMORY_TID,
  OB_V_OB_MEMORY_TID,
  OB_GV_OB_MEMSTORE_TID,
  OB_V_OB_MEMSTORE_TID,
  OB_GV_OB_MEMSTORE_INFO_TID,
  OB_V_OB_MEMSTORE_INFO_TID,
  OB_V_OB_PLAN_CACHE_STAT_TID,
  OB_V_OB_PLAN_CACHE_PLAN_STAT_TID,
  OB_GV_OB_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_V_OB_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_V_OB_SQL_AUDIT_TID,
  OB_V_LATCH_TID,
  OB_GV_OB_RPC_OUTGOING_TID,
  OB_V_OB_RPC_OUTGOING_TID,
  OB_GV_OB_RPC_INCOMING_TID,
  OB_V_OB_RPC_INCOMING_TID,
  OB_GV_SQL_PLAN_MONITOR_TID,
  OB_V_SQL_PLAN_MONITOR_TID,
  OB_DBA_RECYCLEBIN_TID,
  OB_TIME_ZONE_TID,
  OB_TIME_ZONE_NAME_TID,
  OB_TIME_ZONE_TRANSITION_TID,
  OB_TIME_ZONE_TRANSITION_TYPE_TID,
  OB_GV_SESSION_LONGOPS_TID,
  OB_V_SESSION_LONGOPS_TID,
  OB_DBA_OB_SEQUENCE_OBJECTS_TID,
  OB_COLUMNS_TID,
  OB_GV_OB_PX_WORKER_STAT_TID,
  OB_V_OB_PX_WORKER_STAT_TID,
  OB_GV_OB_PS_STAT_TID,
  OB_V_OB_PS_STAT_TID,
  OB_GV_OB_PS_ITEM_INFO_TID,
  OB_V_OB_PS_ITEM_INFO_TID,
  OB_GV_SQL_WORKAREA_TID,
  OB_V_SQL_WORKAREA_TID,
  OB_GV_SQL_WORKAREA_ACTIVE_TID,
  OB_V_SQL_WORKAREA_ACTIVE_TID,
  OB_GV_SQL_WORKAREA_HISTOGRAM_TID,
  OB_V_SQL_WORKAREA_HISTOGRAM_TID,
  OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_TID,
  OB_V_OB_SQL_WORKAREA_MEMORY_INFO_TID,
  OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_TID,
  OB_V_OB_PLAN_CACHE_REFERENCE_INFO_TID,
  OB_GV_OB_SSTABLES_TID,
  OB_V_OB_SSTABLES_TID,
  OB_GV_OB_SERVER_SCHEMA_INFO_TID,
  OB_V_OB_SERVER_SCHEMA_INFO_TID,
  OB_V_SQL_MONITOR_STATNAME_TID,
  OB_GV_OB_MERGE_INFO_TID,
  OB_V_OB_MERGE_INFO_TID,
  OB_V_OB_ENCRYPTED_TABLES_TID,
  OB_V_ENCRYPTED_TABLESPACES_TID,
  OB_CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS_TID,
  OB_GV_OB_TENANT_MEMORY_TID,
  OB_V_OB_TENANT_MEMORY_TID,
  OB_GV_OB_PX_TARGET_MONITOR_TID,
  OB_V_OB_PX_TARGET_MONITOR_TID,
  OB_COLUMN_PRIVILEGES_TID,
  OB_VIEW_TABLE_USAGE_TID,
  OB_FILES_TID,
  OB_DBA_OB_TENANTS_TID,
  OB_DBA_OB_LS_LOCATIONS_TID,
  OB_DBA_OB_TABLET_TO_LS_TID,
  OB_DBA_OB_TABLET_REPLICAS_TID,
  OB_DBA_OB_TABLEGROUPS_TID,
  OB_DBA_OB_TABLEGROUP_PARTITIONS_TID,
  OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_TID,
  OB_DBA_OB_DATABASES_TID,
  OB_DBA_OB_TABLEGROUP_TABLES_TID,
  OB_DBA_OB_ZONE_MAJOR_COMPACTION_TID,
  OB_DBA_OB_MAJOR_COMPACTION_TID,
  OB_DBA_OBJECTS_TID,
  OB_DBA_PART_TABLES_TID,
  OB_DBA_PART_KEY_COLUMNS_TID,
  OB_DBA_SUBPART_KEY_COLUMNS_TID,
  OB_DBA_TAB_PARTITIONS_TID,
  OB_DBA_TAB_SUBPARTITIONS_TID,
  OB_DBA_SUBPARTITION_TEMPLATES_TID,
  OB_DBA_PART_INDEXES_TID,
  OB_DBA_IND_PARTITIONS_TID,
  OB_DBA_IND_SUBPARTITIONS_TID,
  OB_GV_OB_UNITS_TID,
  OB_V_OB_UNITS_TID,
  OB_GV_OB_PARAMETERS_TID,
  OB_V_OB_PARAMETERS_TID,
  OB_GV_OB_PROCESSLIST_TID,
  OB_V_OB_PROCESSLIST_TID,
  OB_GV_OB_KVCACHE_TID,
  OB_V_OB_KVCACHE_TID,
  OB_GV_OB_TRANSACTION_PARTICIPANTS_TID,
  OB_V_OB_TRANSACTION_PARTICIPANTS_TID,
  OB_GV_OB_COMPACTION_PROGRESS_TID,
  OB_V_OB_COMPACTION_PROGRESS_TID,
  OB_GV_OB_TABLET_COMPACTION_PROGRESS_TID,
  OB_V_OB_TABLET_COMPACTION_PROGRESS_TID,
  OB_GV_OB_TABLET_COMPACTION_HISTORY_TID,
  OB_V_OB_TABLET_COMPACTION_HISTORY_TID,
  OB_GV_OB_COMPACTION_DIAGNOSE_INFO_TID,
  OB_V_OB_COMPACTION_DIAGNOSE_INFO_TID,
  OB_GV_OB_COMPACTION_SUGGESTIONS_TID,
  OB_V_OB_COMPACTION_SUGGESTIONS_TID,
  OB_GV_OB_DTL_INTERM_RESULT_MONITOR_TID,
  OB_V_OB_DTL_INTERM_RESULT_MONITOR_TID,
  OB_DBA_TAB_STATISTICS_TID,
  OB_DBA_TAB_COL_STATISTICS_TID,
  OB_DBA_PART_COL_STATISTICS_TID,
  OB_DBA_SUBPART_COL_STATISTICS_TID,
  OB_DBA_TAB_HISTOGRAMS_TID,
  OB_DBA_PART_HISTOGRAMS_TID,
  OB_DBA_SUBPART_HISTOGRAMS_TID,
  OB_DBA_TAB_STATS_HISTORY_TID,
  OB_DBA_IND_STATISTICS_TID,
  OB_DBA_OB_BACKUP_JOBS_TID,
  OB_DBA_OB_BACKUP_JOB_HISTORY_TID,
  OB_DBA_OB_BACKUP_TASKS_TID,
  OB_DBA_OB_BACKUP_TASK_HISTORY_TID,
  OB_DBA_OB_BACKUP_SET_FILES_TID,
  OB_DBA_SQL_PLAN_BASELINES_TID,
  OB_DBA_SQL_MANAGEMENT_CONFIG_TID,
  OB_GV_ACTIVE_SESSION_HISTORY_TID,
  OB_V_ACTIVE_SESSION_HISTORY_TID,
  OB_GV_DML_STATS_TID,
  OB_V_DML_STATS_TID,
  OB_DBA_TAB_MODIFICATIONS_TID,
  OB_DBA_SCHEDULER_JOBS_TID,
  OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_TID,
  OB_DBA_OB_BACKUP_STORAGE_INFO_TID,
  OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_TID,
  OB_DBA_OB_BACKUP_DELETE_POLICY_TID,
  OB_DBA_OB_BACKUP_DELETE_JOBS_TID,
  OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_TID,
  OB_DBA_OB_BACKUP_DELETE_TASKS_TID,
  OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_TID,
  OB_DBA_OB_OUTLINES_TID,
  OB_DBA_OB_CONCURRENT_LIMIT_SQL_TID,
  OB_DBA_OB_RESTORE_PROGRESS_TID,
  OB_DBA_OB_RESTORE_HISTORY_TID,
  OB_DBA_OB_ARCHIVE_DEST_TID,
  OB_DBA_OB_ARCHIVELOG_TID,
  OB_DBA_OB_ARCHIVELOG_SUMMARY_TID,
  OB_DBA_OB_ARCHIVELOG_PIECE_FILES_TID,
  OB_DBA_OB_BACKUP_PARAMETER_TID,
  OB_DBA_OB_DEADLOCK_EVENT_HISTORY_TID,
  OB_DBA_OB_KV_TTL_TASKS_TID,
  OB_DBA_OB_KV_TTL_TASK_HISTORY_TID,
  OB_GV_OB_LOG_STAT_TID,
  OB_V_OB_LOG_STAT_TID,
  OB_ST_GEOMETRY_COLUMNS_TID,
  OB_ST_SPATIAL_REFERENCE_SYSTEMS_TID,
  OB_QUERY_RESPONSE_TIME_TID,
  OB_DBA_RSRC_PLANS_TID,
  OB_DBA_RSRC_PLAN_DIRECTIVES_TID,
  OB_DBA_RSRC_GROUP_MAPPINGS_TID,
  OB_DBA_RSRC_CONSUMER_GROUPS_TID,
  OB_V_RSRC_PLAN_TID,
  OB_DBA_OB_LS_TID,
  OB_DBA_OB_TABLE_LOCATIONS_TID,
  OB_DBA_OB_FREEZE_INFO_TID,
  OB_DBA_OB_LS_REPLICA_TASKS_TID,
  OB_V_OB_LS_REPLICA_TASK_PLAN_TID,
  OB_DBA_OB_AUTO_INCREMENT_TID,
  OB_DBA_SEQUENCES_TID,
  OB_DBA_SCHEDULER_WINDOWS_TID,
  OB_DBA_OB_USERS_TID,
  OB_DBA_OB_DATABASE_PRIVILEGE_TID,
  OB_DBA_OB_USER_DEFINED_RULES_TID,
  OB_GV_OB_SQL_PLAN_TID,
  OB_V_OB_SQL_PLAN_TID,
  OB_PARAMETERS_TID,
  OB_TABLE_PRIVILEGES_TID,
  OB_USER_PRIVILEGES_TID,
  OB_SCHEMA_PRIVILEGES_TID,
  OB_CHECK_CONSTRAINTS_TID,
  OB_REFERENTIAL_CONSTRAINTS_TID,
  OB_TABLE_CONSTRAINTS_TID,
  OB_GV_OB_TRANSACTION_SCHEDULERS_TID,
  OB_V_OB_TRANSACTION_SCHEDULERS_TID,
  OB_TRIGGERS_TID,
  OB_PARTITIONS_TID,
  OB_DBA_OB_LS_ARB_REPLICA_TASKS_TID,
  OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_TID,
  OB_V_OB_ARCHIVE_DEST_STATUS_TID,
  OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_TID,
  OB_DBA_OB_RSRC_IO_DIRECTIVES_TID,
  OB_GV_OB_TABLET_STATS_TID,
  OB_V_OB_TABLET_STATS_TID,
  OB_DBA_OB_ACCESS_POINT_TID,
  OB_DBA_OB_DATA_DICTIONARY_IN_LOG_TID,
  OB_GV_OB_OPT_STAT_GATHER_MONITOR_TID,
  OB_V_OB_OPT_STAT_GATHER_MONITOR_TID,
  OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_TID,
  OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_TID,
  OB_GV_OB_THREAD_TID,
  OB_V_OB_THREAD_TID,
  OB_GV_OB_ARBITRATION_MEMBER_INFO_TID,
  OB_V_OB_ARBITRATION_MEMBER_INFO_TID,
  OB_GV_OB_ARBITRATION_SERVICE_STATUS_TID,
  OB_V_OB_ARBITRATION_SERVICE_STATUS_TID,
  OB_DBA_WR_ACTIVE_SESSION_HISTORY_TID,
  OB_DBA_WR_SNAPSHOT_TID,
  OB_DBA_WR_STATNAME_TID,
  OB_DBA_WR_SYSSTAT_TID,
  OB_GV_OB_KV_CONNECTIONS_TID,
  OB_V_OB_KV_CONNECTIONS_TID,
  OB_GV_OB_LOCKS_TID,
  OB_V_OB_LOCKS_TID,
  OB_DBA_OB_LOG_RESTORE_SOURCE_TID,
  OB_V_OB_TIMESTAMP_SERVICE_TID,
  OB_DBA_OB_BALANCE_JOBS_TID,
  OB_DBA_OB_BALANCE_JOB_HISTORY_TID,
  OB_DBA_OB_BALANCE_TASKS_TID,
  OB_DBA_OB_BALANCE_TASK_HISTORY_TID,
  OB_DBA_OB_TRANSFER_TASKS_TID,
  OB_DBA_OB_TRANSFER_TASK_HISTORY_TID,
  OB_DBA_OB_EXTERNAL_TABLE_FILES_TID,
  OB_ALL_OB_EXTERNAL_TABLE_FILES_TID,
  OB_GV_OB_PX_P2P_DATAHUB_TID,
  OB_V_OB_PX_P2P_DATAHUB_TID,
  OB_GV_SQL_JOIN_FILTER_TID,
  OB_V_SQL_JOIN_FILTER_TID,
  OB_DBA_OB_TABLE_STAT_STALE_INFO_TID,
  OB_V_OB_LS_LOG_RESTORE_STATUS_TID,
  OB_DBA_DB_LINKS_TID,
  OB_DBA_WR_CONTROL_TID,
  OB_DBA_OB_LS_HISTORY_TID,
  OB_DBA_OB_TENANT_EVENT_HISTORY_TID,
  OB_GV_OB_FLT_TRACE_CONFIG_TID,
  OB_GV_OB_SESSION_TID,
  OB_V_OB_SESSION_TID,
  OB_GV_OB_PL_CACHE_OBJECT_TID,
  OB_V_OB_PL_CACHE_OBJECT_TID,
  OB_DBA_OB_RECOVER_TABLE_JOBS_TID,
  OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_TID,
  OB_DBA_OB_IMPORT_TABLE_JOBS_TID,
  OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_TID,
  OB_DBA_OB_IMPORT_TABLE_TASKS_TID,
  OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_TID,
  OB_GV_OB_CGROUP_CONFIG_TID,
  OB_V_OB_CGROUP_CONFIG_TID,
  OB_PROCS_PRIV_TID,
  OB_DBA_OB_AUX_STATISTICS_TID,
  OB_DBA_INDEX_USAGE_TID,
  OB_DBA_OB_SYS_VARIABLES_TID,
  OB_DBA_OB_TRANSFER_PARTITION_TASKS_TID,
  OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_TID,
  OB_ROLE_EDGES_TID,
  OB_DEFAULT_ROLES_TID,
  OB_COLUMNS_PRIV_TID,
  OB_GV_OB_LS_SNAPSHOTS_TID,
  OB_V_OB_LS_SNAPSHOTS_TID,
  OB_DBA_MVIEW_LOGS_TID,
  OB_DBA_MVIEWS_TID,
  OB_DBA_MVREF_STATS_SYS_DEFAULTS_TID,
  OB_DBA_MVREF_STATS_PARAMS_TID,
  OB_DBA_MVREF_RUN_STATS_TID,
  OB_DBA_MVREF_STATS_TID,
  OB_DBA_MVREF_CHANGE_STATS_TID,
  OB_DBA_MVREF_STMT_STATS_TID,
  OB_GV_OB_TRACEPOINT_INFO_TID,
  OB_V_OB_TRACEPOINT_INFO_TID,
  OB_V_OB_COMPATIBILITY_CONTROL_TID,
  OB_GV_OB_TENANT_RESOURCE_LIMIT_TID,
  OB_V_OB_TENANT_RESOURCE_LIMIT_TID,
  OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_TID,
  OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_TID,
  OB_TABLESPACES_TID,
  OB_INNODB_BUFFER_PAGE_TID,
  OB_INNODB_BUFFER_PAGE_LRU_TID,
  OB_INNODB_BUFFER_POOL_STATS_TID,
  OB_INNODB_CMP_TID,
  OB_INNODB_CMP_PER_INDEX_TID,
  OB_INNODB_CMP_PER_INDEX_RESET_TID,
  OB_INNODB_CMP_RESET_TID,
  OB_INNODB_CMPMEM_TID,
  OB_INNODB_CMPMEM_RESET_TID,
  OB_INNODB_SYS_DATAFILES_TID,
  OB_INNODB_SYS_INDEXES_TID,
  OB_INNODB_SYS_TABLES_TID,
  OB_INNODB_SYS_TABLESPACES_TID,
  OB_INNODB_SYS_TABLESTATS_TID,
  OB_INNODB_SYS_VIRTUAL_TID,
  OB_INNODB_TEMP_TABLE_INFO_TID,
  OB_INNODB_METRICS_TID,
  OB_EVENTS_TID,
  OB_V_OB_NIC_INFO_TID,
  OB_GV_OB_NIC_INFO_TID,
  OB_DBA_SCHEDULER_JOB_RUN_DETAILS_TID,
  OB_DBA_SYNONYMS_TID,
  OB_DBA_OBJECTS_ORA_TID,
  OB_ALL_OBJECTS_TID,
  OB_USER_OBJECTS_TID,
  OB_DBA_SEQUENCES_ORA_TID,
  OB_ALL_SEQUENCES_ORA_TID,
  OB_USER_SEQUENCES_ORA_TID,
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
  OB_DBA_PART_KEY_COLUMNS_ORA_TID,
  OB_ALL_PART_KEY_COLUMNS_TID,
  OB_USER_PART_KEY_COLUMNS_TID,
  OB_DBA_SUBPART_KEY_COLUMNS_ORA_TID,
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
  OB_DBA_PART_INDEXES_ORA_TID,
  OB_ALL_PART_INDEXES_ORA_TID,
  OB_USER_PART_INDEXES_ORA_TID,
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
  OB_DBA_IND_PARTITIONS_ORA_TID,
  OB_DBA_IND_SUBPARTITIONS_ORA_TID,
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
  OB_AUDIT_ACTIONS_ORA_TID,
  OB_STMT_AUDIT_OPTION_MAP_ORA_TID,
  OB_ALL_DEF_AUDIT_OPTS_ORA_TID,
  OB_DBA_STMT_AUDIT_OPTS_ORA_TID,
  OB_DBA_OBJ_AUDIT_OPTS_ORA_TID,
  OB_DBA_AUDIT_TRAIL_ORA_TID,
  OB_USER_AUDIT_TRAIL_ORA_TID,
  OB_DBA_AUDIT_EXISTS_ORA_TID,
  OB_DBA_AUDIT_SESSION_ORA_TID,
  OB_USER_AUDIT_SESSION_ORA_TID,
  OB_DBA_AUDIT_STATEMENT_ORA_TID,
  OB_USER_AUDIT_STATEMENT_ORA_TID,
  OB_DBA_AUDIT_OBJECT_ORA_TID,
  OB_USER_AUDIT_OBJECT_ORA_TID,
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
  OB_DBA_RSRC_PLANS_ORA_TID,
  OB_DBA_RSRC_PLAN_DIRECTIVES_ORA_TID,
  OB_DBA_RSRC_GROUP_MAPPINGS_ORA_TID,
  OB_DBA_RECYCLEBIN_ORA_TID,
  OB_USER_RECYCLEBIN_ORA_TID,
  OB_DBA_RSRC_CONSUMER_GROUPS_ORA_TID,
  OB_DBA_OB_LS_LOCATIONS_ORA_TID,
  OB_DBA_OB_TABLET_TO_LS_ORA_TID,
  OB_DBA_OB_TABLET_REPLICAS_ORA_TID,
  OB_DBA_OB_TABLEGROUPS_ORA_TID,
  OB_DBA_OB_TABLEGROUP_PARTITIONS_ORA_TID,
  OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_ORA_TID,
  OB_DBA_OB_DATABASES_ORA_TID,
  OB_DBA_OB_TABLEGROUP_TABLES_ORA_TID,
  OB_DBA_OB_ZONE_MAJOR_COMPACTION_ORA_TID,
  OB_DBA_OB_MAJOR_COMPACTION_ORA_TID,
  OB_ALL_IND_STATISTICS_ORA_TID,
  OB_DBA_IND_STATISTICS_ORA_TID,
  OB_USER_IND_STATISTICS_ORA_TID,
  OB_DBA_OB_BACKUP_JOBS_ORA_TID,
  OB_DBA_OB_BACKUP_JOB_HISTORY_ORA_TID,
  OB_DBA_OB_BACKUP_TASKS_ORA_TID,
  OB_DBA_OB_BACKUP_TASK_HISTORY_ORA_TID,
  OB_DBA_OB_BACKUP_SET_FILES_ORA_TID,
  OB_ALL_TAB_MODIFICATIONS_ORA_TID,
  OB_DBA_TAB_MODIFICATIONS_ORA_TID,
  OB_USER_TAB_MODIFICATIONS_ORA_TID,
  OB_DBA_OB_BACKUP_STORAGE_INFO_ORA_TID,
  OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_ORA_TID,
  OB_DBA_OB_BACKUP_DELETE_POLICY_ORA_TID,
  OB_DBA_OB_BACKUP_DELETE_JOBS_ORA_TID,
  OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_ORA_TID,
  OB_DBA_OB_BACKUP_DELETE_TASKS_ORA_TID,
  OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_ORA_TID,
  OB_DBA_OB_RESTORE_PROGRESS_ORA_TID,
  OB_DBA_OB_RESTORE_HISTORY_ORA_TID,
  OB_DBA_OB_ARCHIVE_DEST_ORA_TID,
  OB_DBA_OB_ARCHIVELOG_ORA_TID,
  OB_DBA_OB_ARCHIVELOG_SUMMARY_ORA_TID,
  OB_DBA_OB_ARCHIVELOG_PIECE_FILES_ORA_TID,
  OB_DBA_OB_BACKUP_PARAMETER_ORA_TID,
  OB_DBA_OB_FREEZE_INFO_ORA_TID,
  OB_DBA_OB_LS_REPLICA_TASKS_ORA_TID,
  OB_V_OB_LS_REPLICA_TASK_PLAN_ORA_TID,
  OB_ALL_SCHEDULER_WINDOWS_ORA_TID,
  OB_DBA_SCHEDULER_WINDOWS_ORA_TID,
  OB_DBA_OB_DATABASE_PRIVILEGE_ORA_TID,
  OB_DBA_OB_TENANTS_ORA_TID,
  OB_DBA_POLICIES_ORA_TID,
  OB_ALL_POLICIES_ORA_TID,
  OB_USER_POLICIES_ORA_TID,
  OB_DBA_POLICY_GROUPS_ORA_TID,
  OB_ALL_POLICY_GROUPS_ORA_TID,
  OB_USER_POLICY_GROUPS_ORA_TID,
  OB_DBA_POLICY_CONTEXTS_ORA_TID,
  OB_ALL_POLICY_CONTEXTS_ORA_TID,
  OB_USER_POLICY_CONTEXTS_ORA_TID,
  OB_DBA_SEC_RELEVANT_COLS_ORA_TID,
  OB_ALL_SEC_RELEVANT_COLS_ORA_TID,
  OB_USER_SEC_RELEVANT_COLS_ORA_TID,
  OB_DBA_OB_LS_ARB_REPLICA_TASKS_ORA_TID,
  OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_ORA_TID,
  OB_DBA_OB_RSRC_IO_DIRECTIVES_ORA_TID,
  OB_ALL_DB_LINKS_ORA_TID,
  OB_DBA_DB_LINKS_ORA_TID,
  OB_USER_DB_LINKS_ORA_TID,
  OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_ORA_TID,
  OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TID,
  OB_DBA_WR_ACTIVE_SESSION_HISTORY_ORA_TID,
  OB_DBA_WR_SNAPSHOT_ORA_TID,
  OB_DBA_WR_STATNAME_ORA_TID,
  OB_DBA_WR_SYSSTAT_ORA_TID,
  OB_DBA_OB_LOG_RESTORE_SOURCE_ORA_TID,
  OB_DBA_OB_EXTERNAL_TABLE_FILES_ORA_TID,
  OB_ALL_OB_EXTERNAL_TABLE_FILES_ORA_TID,
  OB_DBA_OB_BALANCE_JOBS_ORA_TID,
  OB_DBA_OB_BALANCE_JOB_HISTORY_ORA_TID,
  OB_DBA_OB_BALANCE_TASKS_ORA_TID,
  OB_DBA_OB_BALANCE_TASK_HISTORY_ORA_TID,
  OB_DBA_OB_TRANSFER_TASKS_ORA_TID,
  OB_DBA_OB_TRANSFER_TASK_HISTORY_ORA_TID,
  OB_GV_OB_PX_P2P_DATAHUB_ORA_TID,
  OB_V_OB_PX_P2P_DATAHUB_ORA_TID,
  OB_GV_SQL_JOIN_FILTER_ORA_TID,
  OB_V_SQL_JOIN_FILTER_ORA_TID,
  OB_DBA_OB_TABLE_STAT_STALE_INFO_ORA_TID,
  OB_DBMS_LOCK_ALLOCATED_ORA_TID,
  OB_DBA_WR_CONTROL_ORA_TID,
  OB_DBA_OB_LS_HISTORY_ORA_TID,
  OB_DBA_OB_TENANT_EVENT_HISTORY_ORA_TID,
  OB_DBA_SCHEDULER_JOB_RUN_DETAILS_ORA_TID,
  OB_DBA_SCHEDULER_JOB_CLASSES_TID,
  OB_DBA_OB_RECOVER_TABLE_JOBS_ORA_TID,
  OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_ORA_TID,
  OB_DBA_OB_IMPORT_TABLE_JOBS_ORA_TID,
  OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_ORA_TID,
  OB_DBA_OB_IMPORT_TABLE_TASKS_ORA_TID,
  OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_ORA_TID,
  OB_DBA_OB_TRANSFER_PARTITION_TASKS_ORA_TID,
  OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_ORA_TID,
  OB_USER_USERS_TID,
  OB_DBA_MVIEW_LOGS_ORA_TID,
  OB_ALL_MVIEW_LOGS_ORA_TID,
  OB_USER_MVIEW_LOGS_ORA_TID,
  OB_DBA_MVIEWS_ORA_TID,
  OB_ALL_MVIEWS_ORA_TID,
  OB_USER_MVIEWS_ORA_TID,
  OB_DBA_MVREF_STATS_SYS_DEFAULTS_ORA_TID,
  OB_USER_MVREF_STATS_SYS_DEFAULTS_ORA_TID,
  OB_DBA_MVREF_STATS_PARAMS_ORA_TID,
  OB_USER_MVREF_STATS_PARAMS_ORA_TID,
  OB_DBA_MVREF_RUN_STATS_ORA_TID,
  OB_USER_MVREF_RUN_STATS_ORA_TID,
  OB_DBA_MVREF_STATS_ORA_TID,
  OB_USER_MVREF_STATS_ORA_TID,
  OB_DBA_MVREF_CHANGE_STATS_ORA_TID,
  OB_USER_MVREF_CHANGE_STATS_ORA_TID,
  OB_DBA_MVREF_STMT_STATS_ORA_TID,
  OB_USER_MVREF_STMT_STATS_ORA_TID,
  OB_PROXY_USERS_TID,
  OB_GV_OB_SQL_AUDIT_ORA_TID,
  OB_V_OB_SQL_AUDIT_ORA_TID,
  OB_GV_INSTANCE_TID,
  OB_V_INSTANCE_TID,
  OB_GV_OB_PLAN_CACHE_PLAN_STAT_ORA_TID,
  OB_V_OB_PLAN_CACHE_PLAN_STAT_ORA_TID,
  OB_GV_OB_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID,
  OB_V_OB_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID,
  OB_GV_SESSION_WAIT_ORA_TID,
  OB_V_SESSION_WAIT_ORA_TID,
  OB_GV_SESSION_WAIT_HISTORY_ORA_TID,
  OB_V_SESSION_WAIT_HISTORY_ORA_TID,
  OB_GV_OB_MEMORY_ORA_TID,
  OB_V_OB_MEMORY_ORA_TID,
  OB_GV_OB_MEMSTORE_ORA_TID,
  OB_V_OB_MEMSTORE_ORA_TID,
  OB_GV_OB_MEMSTORE_INFO_ORA_TID,
  OB_V_OB_MEMSTORE_INFO_ORA_TID,
  OB_GV_SESSTAT_ORA_TID,
  OB_V_SESSTAT_ORA_TID,
  OB_GV_SYSSTAT_ORA_TID,
  OB_V_SYSSTAT_ORA_TID,
  OB_GV_SYSTEM_EVENT_ORA_TID,
  OB_V_SYSTEM_EVENT_ORA_TID,
  OB_GV_OB_PLAN_CACHE_STAT_ORA_TID,
  OB_V_OB_PLAN_CACHE_STAT_ORA_TID,
  OB_NLS_SESSION_PARAMETERS_ORA_TID,
  OB_NLS_INSTANCE_PARAMETERS_ORA_TID,
  OB_NLS_DATABASE_PARAMETERS_ORA_TID,
  OB_V_NLS_PARAMETERS_ORA_TID,
  OB_V_VERSION_ORA_TID,
  OB_GV_OB_PX_WORKER_STAT_ORA_TID,
  OB_V_OB_PX_WORKER_STAT_ORA_TID,
  OB_GV_OB_PS_STAT_ORA_TID,
  OB_V_OB_PS_STAT_ORA_TID,
  OB_GV_OB_PS_ITEM_INFO_ORA_TID,
  OB_V_OB_PS_ITEM_INFO_ORA_TID,
  OB_GV_SQL_WORKAREA_ACTIVE_ORA_TID,
  OB_V_SQL_WORKAREA_ACTIVE_ORA_TID,
  OB_GV_SQL_WORKAREA_HISTOGRAM_ORA_TID,
  OB_V_SQL_WORKAREA_HISTOGRAM_ORA_TID,
  OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TID,
  OB_V_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TID,
  OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TID,
  OB_V_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TID,
  OB_GV_SQL_WORKAREA_ORA_TID,
  OB_V_SQL_WORKAREA_ORA_TID,
  OB_GV_OB_SSTABLES_ORA_TID,
  OB_V_OB_SSTABLES_ORA_TID,
  OB_GV_OB_SERVER_SCHEMA_INFO_ORA_TID,
  OB_V_OB_SERVER_SCHEMA_INFO_ORA_TID,
  OB_GV_SQL_PLAN_MONITOR_ORA_TID,
  OB_V_SQL_PLAN_MONITOR_ORA_TID,
  OB_V_SQL_MONITOR_STATNAME_ORA_TID,
  OB_GV_OPEN_CURSOR_ORA_TID,
  OB_V_OPEN_CURSOR_ORA_TID,
  OB_V_TIMEZONE_NAMES_ORA_TID,
  OB_GV_GLOBAL_TRANSACTION_ORA_TID,
  OB_V_GLOBAL_TRANSACTION_ORA_TID,
  OB_V_RSRC_PLAN_ORA_TID,
  OB_V_OB_ENCRYPTED_TABLES_ORA_TID,
  OB_V_ENCRYPTED_TABLESPACES_ORA_TID,
  OB_ALL_TAB_COL_STATISTICS_ORA_TID,
  OB_DBA_TAB_COL_STATISTICS_ORA_TID,
  OB_USER_TAB_COL_STATISTICS_ORA_TID,
  OB_ALL_PART_COL_STATISTICS_ORA_TID,
  OB_DBA_PART_COL_STATISTICS_ORA_TID,
  OB_USER_PART_COL_STATISTICS_ORA_TID,
  OB_ALL_SUBPART_COL_STATISTICS_ORA_TID,
  OB_DBA_SUBPART_COL_STATISTICS_ORA_TID,
  OB_USER_SUBPART_COL_STATISTICS_ORA_TID,
  OB_ALL_TAB_HISTOGRAMS_ORA_TID,
  OB_DBA_TAB_HISTOGRAMS_ORA_TID,
  OB_USER_TAB_HISTOGRAMS_ORA_TID,
  OB_ALL_PART_HISTOGRAMS_ORA_TID,
  OB_DBA_PART_HISTOGRAMS_ORA_TID,
  OB_USER_PART_HISTOGRAMS_ORA_TID,
  OB_ALL_SUBPART_HISTOGRAMS_ORA_TID,
  OB_DBA_SUBPART_HISTOGRAMS_ORA_TID,
  OB_USER_SUBPART_HISTOGRAMS_ORA_TID,
  OB_ALL_TAB_STATISTICS_ORA_TID,
  OB_DBA_TAB_STATISTICS_ORA_TID,
  OB_USER_TAB_STATISTICS_ORA_TID,
  OB_DBA_JOBS_TID,
  OB_USER_JOBS_TID,
  OB_DBA_JOBS_RUNNING_TID,
  OB_ALL_DIRECTORIES_TID,
  OB_DBA_DIRECTORIES_TID,
  OB_GV_OB_TENANT_MEMORY_ORA_TID,
  OB_V_OB_TENANT_MEMORY_ORA_TID,
  OB_GV_OB_PX_TARGET_MONITOR_ORA_TID,
  OB_V_OB_PX_TARGET_MONITOR_ORA_TID,
  OB_ALL_TAB_STATS_HISTORY_ORA_TID,
  OB_DBA_TAB_STATS_HISTORY_ORA_TID,
  OB_USER_TAB_STATS_HISTORY_ORA_TID,
  OB_GV_DBLINK_TID,
  OB_V_DBLINK_TID,
  OB_DBA_SCHEDULER_JOBS_ORA_TID,
  OB_DBA_SCHEDULER_PROGRAM_TID,
  OB_DBA_CONTEXT_TID,
  OB_V_GLOBALCONTEXT_TID,
  OB_GV_OB_UNITS_ORA_TID,
  OB_V_OB_UNITS_ORA_TID,
  OB_GV_OB_PARAMETERS_ORA_TID,
  OB_V_OB_PARAMETERS_ORA_TID,
  OB_GV_OB_PROCESSLIST_ORA_TID,
  OB_V_OB_PROCESSLIST_ORA_TID,
  OB_GV_OB_KVCACHE_ORA_TID,
  OB_V_OB_KVCACHE_ORA_TID,
  OB_GV_OB_TRANSACTION_PARTICIPANTS_ORA_TID,
  OB_V_OB_TRANSACTION_PARTICIPANTS_ORA_TID,
  OB_GV_OB_COMPACTION_PROGRESS_ORA_TID,
  OB_V_OB_COMPACTION_PROGRESS_ORA_TID,
  OB_GV_OB_TABLET_COMPACTION_PROGRESS_ORA_TID,
  OB_V_OB_TABLET_COMPACTION_PROGRESS_ORA_TID,
  OB_GV_OB_TABLET_COMPACTION_HISTORY_ORA_TID,
  OB_V_OB_TABLET_COMPACTION_HISTORY_ORA_TID,
  OB_GV_OB_COMPACTION_DIAGNOSE_INFO_ORA_TID,
  OB_V_OB_COMPACTION_DIAGNOSE_INFO_ORA_TID,
  OB_GV_OB_COMPACTION_SUGGESTIONS_ORA_TID,
  OB_V_OB_COMPACTION_SUGGESTIONS_ORA_TID,
  OB_GV_OB_DTL_INTERM_RESULT_MONITOR_ORA_TID,
  OB_V_OB_DTL_INTERM_RESULT_MONITOR_ORA_TID,
  OB_DBA_SQL_PLAN_BASELINES_ORA_TID,
  OB_DBA_SQL_MANAGEMENT_CONFIG_ORA_TID,
  OB_V_EVENT_NAME_ORA_TID,
  OB_GV_ACTIVE_SESSION_HISTORY_ORA_TID,
  OB_V_ACTIVE_SESSION_HISTORY_ORA_TID,
  OB_GV_DML_STATS_ORA_TID,
  OB_V_DML_STATS_ORA_TID,
  OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_ORA_TID,
  OB_DBA_OB_OUTLINES_ORA_TID,
  OB_DBA_OB_CONCURRENT_LIMIT_SQL_ORA_TID,
  OB_DBA_OB_DEADLOCK_EVENT_HISTORY_ORA_TID,
  OB_GV_OB_LOG_STAT_ORA_TID,
  OB_V_OB_LOG_STAT_ORA_TID,
  OB_GV_OB_GLOBAL_TRANSACTION_ORA_TID,
  OB_V_OB_GLOBAL_TRANSACTION_ORA_TID,
  OB_DBA_OB_LS_ORA_TID,
  OB_DBA_OB_TABLE_LOCATIONS_ORA_TID,
  OB_ALL_TRIGGER_ORDERING_TID,
  OB_DBA_TRIGGER_ORDERING_TID,
  OB_USER_TRIGGER_ORDERING_TID,
  OB_GV_OB_TRANSACTION_SCHEDULERS_ORA_TID,
  OB_V_OB_TRANSACTION_SCHEDULERS_ORA_TID,
  OB_DBA_OB_USER_DEFINED_RULES_ORA_TID,
  OB_GV_OB_SQL_PLAN_ORA_TID,
  OB_V_OB_SQL_PLAN_ORA_TID,
  OB_V_OB_ARCHIVE_DEST_STATUS_ORA_TID,
  OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_ORA_TID,
  OB_GV_OB_LOCKS_ORA_TID,
  OB_V_OB_LOCKS_ORA_TID,
  OB_DBA_OB_ACCESS_POINT_ORA_TID,
  OB_DBA_OB_DATA_DICTIONARY_IN_LOG_ORA_TID,
  OB_GV_OB_OPT_STAT_GATHER_MONITOR_ORA_TID,
  OB_V_OB_OPT_STAT_GATHER_MONITOR_ORA_TID,
  OB_GV_SESSION_LONGOPS_ORA_TID,
  OB_V_SESSION_LONGOPS_ORA_TID,
  OB_GV_OB_THREAD_ORA_TID,
  OB_V_OB_THREAD_ORA_TID,
  OB_GV_OB_ARBITRATION_MEMBER_INFO_ORA_TID,
  OB_V_OB_ARBITRATION_MEMBER_INFO_ORA_TID,
  OB_GV_OB_ARBITRATION_SERVICE_STATUS_ORA_TID,
  OB_V_OB_ARBITRATION_SERVICE_STATUS_ORA_TID,
  OB_V_OB_TIMESTAMP_SERVICE_ORA_TID,
  OB_V_OB_LS_LOG_RESTORE_STATUS_ORA_TID,
  OB_GV_OB_FLT_TRACE_CONFIG_ORA_TID,
  OB_GV_OB_SESSION_ORA_TID,
  OB_V_OB_SESSION_ORA_TID,
  OB_GV_OB_PL_CACHE_OBJECT_ORA_TID,
  OB_V_OB_PL_CACHE_OBJECT_ORA_TID,
  OB_GV_OB_CGROUP_CONFIG_ORA_TID,
  OB_V_OB_CGROUP_CONFIG_ORA_TID,
  OB_DBA_OB_AUX_STATISTICS_ORA_TID,
  OB_DBA_OB_SYS_VARIABLES_ORA_TID,
  OB_DBA_INDEX_USAGE_ORA_TID,
  OB_GV_OB_LS_SNAPSHOTS_ORA_TID,
  OB_V_OB_LS_SNAPSHOTS_ORA_TID,
  OB_GV_OB_TRACEPOINT_INFO_ORA_TID,
  OB_V_OB_TRACEPOINT_INFO_ORA_TID,
  OB_GV_OB_TENANT_RESOURCE_LIMIT_ORA_TID,
  OB_V_OB_TENANT_RESOURCE_LIMIT_ORA_TID,
  OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID,
  OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID,
  OB_GV_OB_NIC_INFO_ORA_TID,
  OB_V_OB_NIC_INFO_ORA_TID,
  OB_DBA_OB_SPATIAL_COLUMNS_ORA_TID,
  OB_ALL_TABLE_IDX_DATA_TABLE_ID_TID,
  OB_ALL_TABLE_IDX_DB_TB_NAME_TID,
  OB_ALL_TABLE_IDX_TB_NAME_TID,
  OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TID,
  OB_ALL_COLUMN_IDX_COLUMN_NAME_TID,
  OB_ALL_DDL_OPERATION_IDX_DDL_TYPE_TID,
  OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID,
  OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TID,
  OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TID,
  OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TID,
  OB_ALL_USER_IDX_UR_NAME_TID,
  OB_ALL_DATABASE_IDX_DB_NAME_TID,
  OB_ALL_TABLEGROUP_IDX_TG_NAME_TID,
  OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TID,
  OB_ALL_PART_IDX_PART_NAME_TID,
  OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TID,
  OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TID,
  OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TID,
  OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TID,
  OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TID,
  OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TID,
  OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TID,
  OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TID,
  OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TID,
  OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TID,
  OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TID,
  OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TID,
  OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TID,
  OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TID,
  OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TID,
  OB_ALL_PACKAGE_IDX_PKG_NAME_TID,
  OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TID,
  OB_ALL_CONSTRAINT_IDX_CST_NAME_TID,
  OB_ALL_TYPE_IDX_DB_TYPE_NAME_TID,
  OB_ALL_TYPE_IDX_TYPE_NAME_TID,
  OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TID,
  OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TID,
  OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TID,
  OB_ALL_DBLINK_IDX_DBLINK_NAME_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TID,
  OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TID,
  OB_ALL_TENANT_KEYSTORE_HISTORY_IDX_KEYSTORE_HIS_MASTER_KEY_ID_TID,
  OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TID,
  OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TID,
  OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TID,
  OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TID,
  OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TID,
  OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TID,
  OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TID,
  OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TID,
  OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TID,
  OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TID,
  OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TID,
  OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TID,
  OB_ALL_TENANT_TRIGGER_HISTORY_IDX_TRIGGER_HIS_BASE_OBJ_ID_TID,
  OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TID,
  OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TID,
  OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TID,
  OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TID,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TID,
  OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TID,
  OB_ALL_DDL_ERROR_MESSAGE_IDX_DDL_ERROR_OBJECT_TID,
  OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TID,
  OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TID,
  OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TID,
  OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TID,
  OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TID,
  OB_ALL_PENDING_TRANSACTION_IDX_PENDING_TX_ID_TID,
  OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TID,
  OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TID,
  OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TID,
  OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TID,
  OB_ALL_JOB_IDX_JOB_POWNER_TID,
  OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TID,
  OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TID,
  OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TID,
  OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TID,
  OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TID,
  OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TID,
  OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TID,
  OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TID,
  OB_ALL_RLS_POLICY_HISTORY_IDX_RLS_POLICY_HIS_TABLE_ID_TID,
  OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TID,
  OB_ALL_RLS_GROUP_HISTORY_IDX_RLS_GROUP_HIS_TABLE_ID_TID,
  OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TID,
  OB_ALL_RLS_CONTEXT_HISTORY_IDX_RLS_CONTEXT_HIS_TABLE_ID_TID,
  OB_ALL_TENANT_SNAPSHOT_IDX_TENANT_SNAPSHOT_NAME_TID,
  OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_TID,
  OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_TID,
  OB_ALL_KV_TTL_TASK_IDX_KV_TTL_TASK_TABLE_ID_TID,
  OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TID,
  OB_ALL_MVIEW_REFRESH_RUN_STATS_IDX_MVIEW_REFRESH_RUN_STATS_NUM_MVS_CURRENT_TID,
  OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_END_TIME_TID,
  OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_MVIEW_END_TIME_TID,
  OB_ALL_TRANSFER_PARTITION_TASK_IDX_TRANSFER_PARTITION_KEY_TID,
  OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_IDX_CLIENT_TO_SERVER_SESSION_INFO_CLIENT_SESSION_ID_TID,
  OB_ALL_COLUMN_PRIVILEGE_IDX_COLUMN_PRIVILEGE_NAME_TID,
  OB_ALL_USER_PROXY_INFO_IDX_USER_PROXY_INFO_PROXY_USER_ID_TID,
  OB_ALL_USER_PROXY_INFO_HISTORY_IDX_USER_PROXY_INFO_PROXY_USER_ID_HISTORY_TID,
  OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_IDX_USER_PROXY_ROLE_INFO_PROXY_USER_ID_HISTORY_TID,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_TID,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_TID,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_DATA_TABLE_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_DB_TB_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_TB_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_IDX_TB_COLUMN_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_IDX_COLUMN_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_IDX_UR_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_IDX_DB_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_IDX_TG_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_IDX_RECYCLEBIN_DB_TYPE_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_IDX_PART_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_IDX_SUB_PART_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_IDX_DEF_SUB_PART_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_CHILD_TID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_PARENT_TID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_IDX_DB_SYNONYM_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_IDX_SYNONYM_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_DB_ROUTINE_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_ROUTINE_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_ROUTINE_PKG_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_IDX_ROUTINE_PARAM_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_IDX_DB_PKG_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_IDX_PKG_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_IDX_CST_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_IDX_DB_TYPE_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_IDX_TYPE_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_IDX_TYPE_ATTR_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_IDX_COLL_NAME_TYPE_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_IDX_OWNER_DBLINK_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_IDX_DBLINK_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_IDX_GRANTEE_ROLE_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_IDX_KEYSTORE_MASTER_KEY_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_IDX_OLS_POLICY_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_IDX_OLS_POLICY_COL_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_REAL_AGENT_ORA_IDX_OLS_COM_POLICY_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_POLICY_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_TAG_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_IDX_OLS_LEVEL_UID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_IDX_OLS_LEVEL_POLICY_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_IDX_PROFILE_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_IDX_AUDIT_TYPE_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_TRIGGER_BASE_OBJ_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_DB_TRIGGER_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_TRIGGER_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_IDX_OBJAUTH_GRANTOR_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_IDX_OBJAUTH_GRANTEE_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_IDX_OBJ_TYPE_DB_OBJ_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_IDX_OBJ_TYPE_OBJ_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_IDX_DEPENDENCY_REF_OBJ_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_REAL_AGENT_ORA_IDX_TABLE_STAT_HIS_SAVTIME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_REAL_AGENT_ORA_IDX_COLUMN_STAT_HIS_SAVTIME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_REAL_AGENT_ORA_IDX_HISTOGRAM_STAT_HIS_SAVTIME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_IDX_TABLET_TO_LS_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_IDX_TABLET_TO_TABLE_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_CONTEXT_REAL_AGENT_ORA_IDX_CTX_NAMESPACE_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_IDX_SPM_ITEM_SQL_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_IDX_SPM_ITEM_VALUE_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT_ORA_IDX_DIRECTORY_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_JOB_REAL_AGENT_ORA_IDX_JOB_POWNER_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_IDX_SEQ_OBJ_DB_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_IDX_SEQ_OBJ_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_IDX_RECYCLEBIN_ORI_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_IDX_TB_PRIV_DB_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_IDX_TB_PRIV_TB_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT_ORA_IDX_DB_PRIV_DB_NAME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_IDX_RLS_POLICY_TABLE_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_IDX_RLS_POLICY_GROUP_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORA_IDX_RLS_GROUP_TABLE_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORA_IDX_RLS_CONTEXT_TABLE_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT_ORA_IDX_USER_PROXY_INFO_PROXY_USER_ID_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORA_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_REAL_AGENT_TID,
  OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORA_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_REAL_AGENT_TID,
  OB_ALL_TABLE_AUX_LOB_META_TID,
  OB_ALL_COLUMN_AUX_LOB_META_TID,
  OB_ALL_DDL_OPERATION_AUX_LOB_META_TID,
  OB_ALL_USER_AUX_LOB_META_TID,
  OB_ALL_USER_HISTORY_AUX_LOB_META_TID,
  OB_ALL_DATABASE_AUX_LOB_META_TID,
  OB_ALL_DATABASE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TABLEGROUP_AUX_LOB_META_TID,
  OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TABLE_PRIVILEGE_AUX_LOB_META_TID,
  OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_META_TID,
  OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TABLE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_COLUMN_HISTORY_AUX_LOB_META_TID,
  OB_TENANT_PARAMETER_AUX_LOB_META_TID,
  OB_ALL_SYS_VARIABLE_AUX_LOB_META_TID,
  OB_ALL_SYS_STAT_AUX_LOB_META_TID,
  OB_HELP_TOPIC_AUX_LOB_META_TID,
  OB_HELP_CATEGORY_AUX_LOB_META_TID,
  OB_HELP_KEYWORD_AUX_LOB_META_TID,
  OB_HELP_RELATION_AUX_LOB_META_TID,
  OB_ALL_DUMMY_AUX_LOB_META_TID,
  OB_ALL_OUTLINE_AUX_LOB_META_TID,
  OB_ALL_OUTLINE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_RECYCLEBIN_AUX_LOB_META_TID,
  OB_ALL_PART_AUX_LOB_META_TID,
  OB_ALL_PART_HISTORY_AUX_LOB_META_TID,
  OB_ALL_SUB_PART_AUX_LOB_META_TID,
  OB_ALL_SUB_PART_HISTORY_AUX_LOB_META_TID,
  OB_ALL_PART_INFO_AUX_LOB_META_TID,
  OB_ALL_PART_INFO_HISTORY_AUX_LOB_META_TID,
  OB_ALL_DEF_SUB_PART_AUX_LOB_META_TID,
  OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_META_TID,
  OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_RESTORE_JOB_AUX_LOB_META_TID,
  OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_META_TID,
  OB_ALL_DDL_ID_AUX_LOB_META_TID,
  OB_ALL_FOREIGN_KEY_AUX_LOB_META_TID,
  OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_META_TID,
  OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_META_TID,
  OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_META_TID,
  OB_ALL_SYNONYM_AUX_LOB_META_TID,
  OB_ALL_SYNONYM_HISTORY_AUX_LOB_META_TID,
  OB_ALL_AUTO_INCREMENT_AUX_LOB_META_TID,
  OB_ALL_DDL_CHECKSUM_AUX_LOB_META_TID,
  OB_ALL_ROUTINE_AUX_LOB_META_TID,
  OB_ALL_ROUTINE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_ROUTINE_PARAM_AUX_LOB_META_TID,
  OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_META_TID,
  OB_ALL_PACKAGE_AUX_LOB_META_TID,
  OB_ALL_PACKAGE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_META_TID,
  OB_ALL_CONSTRAINT_AUX_LOB_META_TID,
  OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_META_TID,
  OB_ALL_FUNC_AUX_LOB_META_TID,
  OB_ALL_FUNC_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TEMP_TABLE_AUX_LOB_META_TID,
  OB_ALL_SEQUENCE_OBJECT_AUX_LOB_META_TID,
  OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_SEQUENCE_VALUE_AUX_LOB_META_TID,
  OB_ALL_TYPE_AUX_LOB_META_TID,
  OB_ALL_TYPE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TYPE_ATTR_AUX_LOB_META_TID,
  OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_META_TID,
  OB_ALL_COLL_TYPE_AUX_LOB_META_TID,
  OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_WEAK_READ_SERVICE_AUX_LOB_META_TID,
  OB_ALL_DBLINK_AUX_LOB_META_TID,
  OB_ALL_DBLINK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_META_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_KEYSTORE_AUX_LOB_META_TID,
  OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_OLS_POLICY_AUX_LOB_META_TID,
  OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_META_TID,
  OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_OLS_LABEL_AUX_LOB_META_TID,
  OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_META_TID,
  OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_TABLESPACE_AUX_LOB_META_TID,
  OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_META_TID,
  OB_ALL_TENANT_PROFILE_AUX_LOB_META_TID,
  OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_META_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_TRIGGER_AUX_LOB_META_TID,
  OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_META_TID,
  OB_ALL_TENANT_SYSAUTH_AUX_LOB_META_TID,
  OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_OBJAUTH_AUX_LOB_META_TID,
  OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_META_TID,
  OB_ALL_RESTORE_INFO_AUX_LOB_META_TID,
  OB_ALL_TENANT_ERROR_AUX_LOB_META_TID,
  OB_ALL_RESTORE_PROGRESS_AUX_LOB_META_TID,
  OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_META_TID,
  OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_TIME_ZONE_AUX_LOB_META_TID,
  OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_META_TID,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_META_TID,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_META_TID,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_META_TID,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_META_TID,
  OB_ALL_TENANT_DEPENDENCY_AUX_LOB_META_TID,
  OB_ALL_RES_MGR_PLAN_AUX_LOB_META_TID,
  OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_META_TID,
  OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_META_TID,
  OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_META_TID,
  OB_ALL_BACKUP_SET_FILES_AUX_LOB_META_TID,
  OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_META_TID,
  OB_ALL_BACKUP_INFO_AUX_LOB_META_TID,
  OB_ALL_DDL_TASK_STATUS_AUX_LOB_META_TID,
  OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_COLUMN_USAGE_AUX_LOB_META_TID,
  OB_ALL_JOB_AUX_LOB_META_TID,
  OB_ALL_JOB_LOG_AUX_LOB_META_TID,
  OB_ALL_TENANT_DIRECTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TABLE_STAT_AUX_LOB_META_TID,
  OB_ALL_COLUMN_STAT_AUX_LOB_META_TID,
  OB_ALL_HISTOGRAM_STAT_AUX_LOB_META_TID,
  OB_ALL_MONITOR_MODIFIED_AUX_LOB_META_TID,
  OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_META_TID,
  OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_META_TID,
  OB_ALL_LS_META_TABLE_AUX_LOB_META_TID,
  OB_ALL_TABLET_TO_LS_AUX_LOB_META_TID,
  OB_ALL_TABLET_META_TABLE_AUX_LOB_META_TID,
  OB_ALL_LS_STATUS_AUX_LOB_META_TID,
  OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TID,
  OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_META_TID,
  OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TID,
  OB_ALL_LS_AUX_LOB_META_TID,
  OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_META_TID,
  OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_META_TID,
  OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_META_TID,
  OB_ALL_BACKUP_JOB_AUX_LOB_META_TID,
  OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_META_TID,
  OB_ALL_BACKUP_TASK_AUX_LOB_META_TID,
  OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_BACKUP_LS_TASK_AUX_LOB_META_TID,
  OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_META_TID,
  OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_META_TID,
  OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_INFO_AUX_LOB_META_TID,
  OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_LS_RECOVERY_STAT_AUX_LOB_META_TID,
  OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_META_TID,
  OB_ALL_TABLET_CHECKSUM_AUX_LOB_META_TID,
  OB_ALL_LS_REPLICA_TASK_AUX_LOB_META_TID,
  OB_ALL_PENDING_TRANSACTION_AUX_LOB_META_TID,
  OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_META_TID,
  OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_META_TID,
  OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_META_TID,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_META_TID,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_META_TID,
  OB_ALL_CONTEXT_AUX_LOB_META_TID,
  OB_ALL_CONTEXT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_META_TID,
  OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_META_TID,
  OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_META_TID,
  OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_META_TID,
  OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_META_TID,
  OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_META_TID,
  OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_ZONE_MERGE_INFO_AUX_LOB_META_TID,
  OB_ALL_MERGE_INFO_AUX_LOB_META_TID,
  OB_ALL_FREEZE_INFO_AUX_LOB_META_TID,
  OB_ALL_PLAN_BASELINE_AUX_LOB_META_TID,
  OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_META_TID,
  OB_ALL_SPM_CONFIG_AUX_LOB_META_TID,
  OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_META_TID,
  OB_ALL_BACKUP_PARAMETER_AUX_LOB_META_TID,
  OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_META_TID,
  OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_META_TID,
  OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_META_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_META_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_META_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_META_TID,
  OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_META_TID,
  OB_ALL_KV_TTL_TASK_AUX_LOB_META_TID,
  OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_SERVICE_EPOCH_AUX_LOB_META_TID,
  OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_META_TID,
  OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_META_TID,
  OB_ALL_COLUMN_GROUP_AUX_LOB_META_TID,
  OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_META_TID,
  OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_META_TID,
  OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TRANSFER_TASK_AUX_LOB_META_TID,
  OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_BALANCE_JOB_AUX_LOB_META_TID,
  OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_META_TID,
  OB_ALL_BALANCE_TASK_AUX_LOB_META_TID,
  OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_META_TID,
  OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_META_TID,
  OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_RLS_POLICY_AUX_LOB_META_TID,
  OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_META_TID,
  OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_META_TID,
  OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_META_TID,
  OB_ALL_RLS_GROUP_AUX_LOB_META_TID,
  OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_META_TID,
  OB_ALL_RLS_CONTEXT_AUX_LOB_META_TID,
  OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_RLS_ATTRIBUTE_AUX_LOB_META_TID,
  OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_META_TID,
  OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_META_TID,
  OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_META_TID,
  OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_META_TID,
  OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TID,
  OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_META_TID,
  OB_WR_SNAPSHOT_AUX_LOB_META_TID,
  OB_WR_STATNAME_AUX_LOB_META_TID,
  OB_WR_SYSSTAT_AUX_LOB_META_TID,
  OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_META_TID,
  OB_ALL_TENANT_SNAPSHOT_AUX_LOB_META_TID,
  OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_META_TID,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_META_TID,
  OB_ALL_MLOG_AUX_LOB_META_TID,
  OB_ALL_MVIEW_AUX_LOB_META_TID,
  OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_META_TID,
  OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_META_TID,
  OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_META_TID,
  OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_META_TID,
  OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_META_TID,
  OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_META_TID,
  OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_META_TID,
  OB_WR_CONTROL_AUX_LOB_META_TID,
  OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_META_TID,
  OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_META_TID,
  OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_META_TID,
  OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_META_TID,
  OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_META_TID,
  OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_META_TID,
  OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_CLONE_JOB_AUX_LOB_META_TID,
  OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_META_TID,
  OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_META_TID,
  OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_NCOMP_DLL_AUX_LOB_META_TID,
  OB_ALL_AUX_STAT_AUX_LOB_META_TID,
  OB_ALL_INDEX_USAGE_INFO_AUX_LOB_META_TID,
  OB_ALL_DETECT_LOCK_INFO_AUX_LOB_META_TID,
  OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_META_TID,
  OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_META_TID,
  OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_META_TID,
  OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_META_TID,
  OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_META_TID,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_META_TID,
  OB_ALL_USER_PROXY_INFO_AUX_LOB_META_TID,
  OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_META_TID,
  OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_META_TID,
  OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_META_TID,
  OB_ALL_MVIEW_DEP_AUX_LOB_META_TID,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_META_TID,
  OB_ALL_TABLE_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_AUX_LOB_PIECE_TID,
  OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_TID,
  OB_ALL_USER_AUX_LOB_PIECE_TID,
  OB_ALL_USER_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_DATABASE_AUX_LOB_PIECE_TID,
  OB_ALL_DATABASE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TABLEGROUP_AUX_LOB_PIECE_TID,
  OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TABLE_PRIVILEGE_AUX_LOB_PIECE_TID,
  OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_PIECE_TID,
  OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TABLE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
  OB_TENANT_PARAMETER_AUX_LOB_PIECE_TID,
  OB_ALL_SYS_VARIABLE_AUX_LOB_PIECE_TID,
  OB_ALL_SYS_STAT_AUX_LOB_PIECE_TID,
  OB_HELP_TOPIC_AUX_LOB_PIECE_TID,
  OB_HELP_CATEGORY_AUX_LOB_PIECE_TID,
  OB_HELP_KEYWORD_AUX_LOB_PIECE_TID,
  OB_HELP_RELATION_AUX_LOB_PIECE_TID,
  OB_ALL_DUMMY_AUX_LOB_PIECE_TID,
  OB_ALL_OUTLINE_AUX_LOB_PIECE_TID,
  OB_ALL_OUTLINE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_RECYCLEBIN_AUX_LOB_PIECE_TID,
  OB_ALL_PART_AUX_LOB_PIECE_TID,
  OB_ALL_PART_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_SUB_PART_AUX_LOB_PIECE_TID,
  OB_ALL_SUB_PART_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_PART_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_PART_INFO_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_DEF_SUB_PART_AUX_LOB_PIECE_TID,
  OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_RESTORE_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_DDL_ID_AUX_LOB_PIECE_TID,
  OB_ALL_FOREIGN_KEY_AUX_LOB_PIECE_TID,
  OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_PIECE_TID,
  OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_SYNONYM_AUX_LOB_PIECE_TID,
  OB_ALL_SYNONYM_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_AUTO_INCREMENT_AUX_LOB_PIECE_TID,
  OB_ALL_DDL_CHECKSUM_AUX_LOB_PIECE_TID,
  OB_ALL_ROUTINE_AUX_LOB_PIECE_TID,
  OB_ALL_ROUTINE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_ROUTINE_PARAM_AUX_LOB_PIECE_TID,
  OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_PACKAGE_AUX_LOB_PIECE_TID,
  OB_ALL_PACKAGE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_PIECE_TID,
  OB_ALL_CONSTRAINT_AUX_LOB_PIECE_TID,
  OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_PIECE_TID,
  OB_ALL_FUNC_AUX_LOB_PIECE_TID,
  OB_ALL_FUNC_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TEMP_TABLE_AUX_LOB_PIECE_TID,
  OB_ALL_SEQUENCE_OBJECT_AUX_LOB_PIECE_TID,
  OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_SEQUENCE_VALUE_AUX_LOB_PIECE_TID,
  OB_ALL_TYPE_AUX_LOB_PIECE_TID,
  OB_ALL_TYPE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TYPE_ATTR_AUX_LOB_PIECE_TID,
  OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_COLL_TYPE_AUX_LOB_PIECE_TID,
  OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_WEAK_READ_SERVICE_AUX_LOB_PIECE_TID,
  OB_ALL_DBLINK_AUX_LOB_PIECE_TID,
  OB_ALL_DBLINK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_KEYSTORE_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OLS_POLICY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OLS_LABEL_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_TABLESPACE_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_PROFILE_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_TRIGGER_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SYSAUTH_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OBJAUTH_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_RESTORE_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_ERROR_AUX_LOB_PIECE_TID,
  OB_ALL_RESTORE_PROGRESS_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_TIME_ZONE_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_DEPENDENCY_AUX_LOB_PIECE_TID,
  OB_ALL_RES_MGR_PLAN_AUX_LOB_PIECE_TID,
  OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_PIECE_TID,
  OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_PIECE_TID,
  OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_SET_FILES_AUX_LOB_PIECE_TID,
  OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_DDL_TASK_STATUS_AUX_LOB_PIECE_TID,
  OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_USAGE_AUX_LOB_PIECE_TID,
  OB_ALL_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_JOB_LOG_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_DIRECTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TABLE_STAT_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_STAT_AUX_LOB_PIECE_TID,
  OB_ALL_HISTOGRAM_STAT_AUX_LOB_PIECE_TID,
  OB_ALL_MONITOR_MODIFIED_AUX_LOB_PIECE_TID,
  OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_PIECE_TID,
  OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_PIECE_TID,
  OB_ALL_LS_META_TABLE_AUX_LOB_PIECE_TID,
  OB_ALL_TABLET_TO_LS_AUX_LOB_PIECE_TID,
  OB_ALL_TABLET_META_TABLE_AUX_LOB_PIECE_TID,
  OB_ALL_LS_STATUS_AUX_LOB_PIECE_TID,
  OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TID,
  OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_PIECE_TID,
  OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TID,
  OB_ALL_LS_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_PIECE_TID,
  OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_LS_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_LS_RECOVERY_STAT_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_PIECE_TID,
  OB_ALL_TABLET_CHECKSUM_AUX_LOB_PIECE_TID,
  OB_ALL_LS_REPLICA_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_PENDING_TRANSACTION_AUX_LOB_PIECE_TID,
  OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_PIECE_TID,
  OB_ALL_CONTEXT_AUX_LOB_PIECE_TID,
  OB_ALL_CONTEXT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_PIECE_TID,
  OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_ZONE_MERGE_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_MERGE_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_FREEZE_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_PLAN_BASELINE_AUX_LOB_PIECE_TID,
  OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_PIECE_TID,
  OB_ALL_SPM_CONFIG_AUX_LOB_PIECE_TID,
  OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_PARAMETER_AUX_LOB_PIECE_TID,
  OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_PIECE_TID,
  OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_PIECE_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_PIECE_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_PIECE_TID,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_PIECE_TID,
  OB_ALL_KV_TTL_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_SERVICE_EPOCH_AUX_LOB_PIECE_TID,
  OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_GROUP_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TRANSFER_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_BALANCE_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_BALANCE_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_PIECE_TID,
  OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_POLICY_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_GROUP_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_CONTEXT_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_ATTRIBUTE_AUX_LOB_PIECE_TID,
  OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_PIECE_TID,
  OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_PIECE_TID,
  OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_PIECE_TID,
  OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TID,
  OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_PIECE_TID,
  OB_WR_SNAPSHOT_AUX_LOB_PIECE_TID,
  OB_WR_STATNAME_AUX_LOB_PIECE_TID,
  OB_WR_SYSSTAT_AUX_LOB_PIECE_TID,
  OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SNAPSHOT_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_PIECE_TID,
  OB_ALL_MLOG_AUX_LOB_PIECE_TID,
  OB_ALL_MVIEW_AUX_LOB_PIECE_TID,
  OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_PIECE_TID,
  OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_PIECE_TID,
  OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_PIECE_TID,
  OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_PIECE_TID,
  OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_PIECE_TID,
  OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_PIECE_TID,
  OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_PIECE_TID,
  OB_WR_CONTROL_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_PIECE_TID,
  OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_CLONE_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_PIECE_TID,
  OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_NCOMP_DLL_AUX_LOB_PIECE_TID,
  OB_ALL_AUX_STAT_AUX_LOB_PIECE_TID,
  OB_ALL_INDEX_USAGE_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_DETECT_LOCK_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_PIECE_TID,
  OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_PIECE_TID,
  OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_USER_PROXY_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_PIECE_TID,
  OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_PIECE_TID,
  OB_ALL_MVIEW_DEP_AUX_LOB_PIECE_TID,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_PIECE_TID,  };

const uint64_t all_ora_mapping_virtual_table_org_tables [] = {
  OB_ALL_VIRTUAL_SQL_AUDIT_TID,
  OB_ALL_VIRTUAL_PLAN_STAT_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_ALL_VIRTUAL_PRIVILEGE_TID,
  OB_TENANT_VIRTUAL_COLLATION_TID,
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
  OB_ALL_VIRTUAL_TRACE_SPAN_INFO_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_TID,
  OB_ALL_VIRTUAL_AUDIT_OPERATION_TID,
  OB_ALL_VIRTUAL_AUDIT_ACTION_TID,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_TID,
  OB_ALL_VIRTUAL_PS_STAT_TID,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_TID,
  OB_ALL_VIRTUAL_TABLE_MGR_TID,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TID,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TID,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TID,
  OB_ALL_VIRTUAL_OPEN_CURSOR_TID,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TID,
  OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TID,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID,
  OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_TID,
  OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_TID,
  OB_ALL_VIRTUAL_PX_TARGET_MONITOR_TID,
  OB_ALL_VIRTUAL_DBLINK_INFO_TID,
  OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_TID,
  OB_ALL_VIRTUAL_TRANS_STAT_TID,
  OB_ALL_VIRTUAL_UNIT_TID,
  OB_ALL_VIRTUAL_KVCACHE_INFO_TID,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_TID,
  OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_TID,
  OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_TID,
  OB_ALL_VIRTUAL_LS_META_TABLE_TID,
  OB_ALL_VIRTUAL_TABLET_META_TABLE_TID,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID,
  OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_TID,
  OB_ALL_VIRTUAL_PROXY_SCHEMA_TID,
  OB_ALL_VIRTUAL_PROXY_PARTITION_TID,
  OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID,
  OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID,
  OB_ALL_VIRTUAL_ZONE_MERGE_INFO_TID,
  OB_ALL_VIRTUAL_MERGE_INFO_TID,
  OB_ALL_VIRTUAL_BACKUP_JOB_TID,
  OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_TASK_TID,
  OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_SET_FILES_TID,
  OB_TENANT_VIRTUAL_EVENT_NAME_TID,
  OB_ALL_VIRTUAL_ASH_TID,
  OB_ALL_VIRTUAL_DML_STATS_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_TID,
  OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_TID,
  OB_ALL_VIRTUAL_BACKUP_PARAMETER_TID,
  OB_ALL_VIRTUAL_RESTORE_JOB_TID,
  OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_RESTORE_PROGRESS_TID,
  OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_TID,
  OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_TID,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_TID,
  OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_TID,
  OB_ALL_VIRTUAL_LOG_STAT_TID,
  OB_ALL_VIRTUAL_REPLAY_STAT_TID,
  OB_ALL_VIRTUAL_APPLY_STAT_TID,
  OB_ALL_VIRTUAL_ARCHIVE_STAT_TID,
  OB_ALL_VIRTUAL_LS_STATUS_TID,
  OB_ALL_VIRTUAL_LS_RECOVERY_STAT_TID,
  OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_TID,
  OB_ALL_VIRTUAL_TENANT_INFO_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_TID,
  OB_ALL_VIRTUAL_SHOW_TRACE_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_TID,
  OB_ALL_VIRTUAL_TRANS_SCHEDULER_TID,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_TID,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_TID,
  OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_TID,
  OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_TID,
  OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_TID,
  OB_ALL_VIRTUAL_THREAD_TID,
  OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_TID,
  OB_ALL_VIRTUAL_WR_SNAPSHOT_TID,
  OB_ALL_VIRTUAL_WR_STATNAME_TID,
  OB_ALL_VIRTUAL_WR_SYSSTAT_TID,
  OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_TID,
  OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_TID,
  OB_ALL_VIRTUAL_OBJ_LOCK_TID,
  OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_TID,
  OB_ALL_VIRTUAL_PX_P2P_DATAHUB_TID,
  OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_TID,
  OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_TID,
  OB_ALL_VIRTUAL_WR_CONTROL_TID,
  OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_TID,
  OB_ALL_VIRTUAL_FLT_CONFIG_TID,
  OB_ALL_VIRTUAL_SESSION_INFO_TID,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_TID,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_TID,
  OB_ALL_VIRTUAL_LS_INFO_TID,
  OB_ALL_VIRTUAL_CGROUP_CONFIG_TID,
  OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_TID,
  OB_ALL_VIRTUAL_LS_SNAPSHOT_TID,
  OB_ALL_VIRTUAL_TRACEPOINT_INFO_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_TID,
  OB_ALL_VIRTUAL_NIC_INFO_TID,  };

const uint64_t all_ora_mapping_virtual_tables [] = {  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID
,  OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID
,  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID
,  OB_ALL_VIRTUAL_PRIVILEGE_ORA_TID
,  OB_TENANT_VIRTUAL_COLLATION_ORA_TID
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
,  OB_ALL_VIRTUAL_TRACE_SPAN_INFO_ORA_TID
,  OB_ALL_VIRTUAL_DATA_TYPE_ORA_TID
,  OB_ALL_VIRTUAL_AUDIT_OPERATION_ORA_TID
,  OB_ALL_VIRTUAL_AUDIT_ACTION_ORA_TID
,  OB_ALL_VIRTUAL_PX_WORKER_STAT_ORA_TID
,  OB_ALL_VIRTUAL_PS_STAT_ORA_TID
,  OB_ALL_VIRTUAL_PS_ITEM_INFO_ORA_TID
,  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TID
,  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_ORA_TID
,  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_ORA_TID
,  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_ORA_TID
,  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_ORA_TID
,  OB_ALL_VIRTUAL_TABLE_MGR_ORA_TID
,  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_ORA_TID
,  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TID
,  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_ORA_TID
,  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORA_TID
,  OB_ALL_VIRTUAL_OPEN_CURSOR_ORA_TID
,  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_ORA_TID
,  OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_ORA_TID
,  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TID
,  OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_ORA_TID
,  OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_ORA_TID
,  OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_ORA_TID
,  OB_ALL_VIRTUAL_PX_TARGET_MONITOR_ORA_TID
,  OB_ALL_VIRTUAL_DBLINK_INFO_ORA_TID
,  OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_ORA_TID
,  OB_ALL_VIRTUAL_TRANS_STAT_ORA_TID
,  OB_ALL_VIRTUAL_UNIT_ORA_TID
,  OB_ALL_VIRTUAL_KVCACHE_INFO_ORA_TID
,  OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_ORA_TID
,  OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_ORA_TID
,  OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_ORA_TID
,  OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_ORA_TID
,  OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_LS_META_TABLE_ORA_TID
,  OB_ALL_VIRTUAL_TABLET_META_TABLE_ORA_TID
,  OB_ALL_VIRTUAL_CORE_ALL_TABLE_ORA_TID
,  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_ORA_TID
,  OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_ORA_TID
,  OB_ALL_VIRTUAL_PROXY_SCHEMA_ORA_TID
,  OB_ALL_VIRTUAL_PROXY_PARTITION_ORA_TID
,  OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_ORA_TID
,  OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_ORA_TID
,  OB_ALL_VIRTUAL_ZONE_MERGE_INFO_ORA_TID
,  OB_ALL_VIRTUAL_MERGE_INFO_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_JOB_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_TASK_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_SET_FILES_ORA_TID
,  OB_TENANT_VIRTUAL_EVENT_NAME_ORA_TID
,  OB_ALL_VIRTUAL_ASH_ORA_TID
,  OB_ALL_VIRTUAL_DML_STATS_ORA_TID
,  OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_ORA_TID
,  OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_ORA_TID
,  OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_ORA_TID
,  OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_PARAMETER_ORA_TID
,  OB_ALL_VIRTUAL_RESTORE_JOB_ORA_TID
,  OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_RESTORE_PROGRESS_ORA_TID
,  OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_ORA_TID
,  OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_ORA_TID
,  OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_LOG_STAT_ORA_TID
,  OB_ALL_VIRTUAL_REPLAY_STAT_ORA_TID
,  OB_ALL_VIRTUAL_APPLY_STAT_ORA_TID
,  OB_ALL_VIRTUAL_ARCHIVE_STAT_ORA_TID
,  OB_ALL_VIRTUAL_LS_STATUS_ORA_TID
,  OB_ALL_VIRTUAL_LS_RECOVERY_STAT_ORA_TID
,  OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_ORA_TID
,  OB_ALL_VIRTUAL_TENANT_INFO_ORA_TID
,  OB_ALL_VIRTUAL_LS_REPLICA_TASK_ORA_TID
,  OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_ORA_TID
,  OB_ALL_VIRTUAL_SHOW_TRACE_ORA_TID
,  OB_ALL_VIRTUAL_SQL_PLAN_ORA_TID
,  OB_ALL_VIRTUAL_TRANS_SCHEDULER_ORA_TID
,  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_ORA_TID
,  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_ORA_TID
,  OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_ORA_TID
,  OB_ALL_VIRTUAL_THREAD_ORA_TID
,  OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_WR_SNAPSHOT_ORA_TID
,  OB_ALL_VIRTUAL_WR_STATNAME_ORA_TID
,  OB_ALL_VIRTUAL_WR_SYSSTAT_ORA_TID
,  OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_ORA_TID
,  OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_ORA_TID
,  OB_ALL_VIRTUAL_OBJ_LOCK_ORA_TID
,  OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_ORA_TID
,  OB_ALL_VIRTUAL_PX_P2P_DATAHUB_ORA_TID
,  OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_ORA_TID
,  OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_ORA_TID
,  OB_ALL_VIRTUAL_TENANT_PARAMETER_ORA_TID
,  OB_ALL_VIRTUAL_WR_CONTROL_ORA_TID
,  OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_FLT_CONFIG_ORA_TID
,  OB_ALL_VIRTUAL_SESSION_INFO_ORA_TID
,  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_ORA_TID
,  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_ORA_TID
,  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_ORA_TID
,  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_ORA_TID
,  OB_ALL_VIRTUAL_LS_INFO_ORA_TID
,  OB_ALL_VIRTUAL_CGROUP_CONFIG_ORA_TID
,  OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_ORA_TID
,  OB_ALL_VIRTUAL_LS_SNAPSHOT_ORA_TID
,  OB_ALL_VIRTUAL_TRACEPOINT_INFO_ORA_TID
,  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_ORA_TID
,  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID
,  OB_ALL_VIRTUAL_NIC_INFO_ORA_TID
,  };

/* start/end_pos is start/end postition for column with tenant id */
struct VTMapping
{
   uint64_t mapping_tid_;
   bool is_real_vt_;
   int64_t start_pos_;
   int64_t end_pos_;
};

extern VTMapping vt_mappings[5000];

const char* const tenant_space_table_names [] = {
  OB_ALL_CORE_TABLE_TNAME,
  OB_ALL_TABLE_TNAME,
  OB_ALL_COLUMN_TNAME,
  OB_ALL_DDL_OPERATION_TNAME,
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
  OB_HELP_TOPIC_TNAME,
  OB_HELP_CATEGORY_TNAME,
  OB_HELP_KEYWORD_TNAME,
  OB_HELP_RELATION_TNAME,
  OB_ALL_DUMMY_TNAME,
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
  OB_ALL_RESTORE_JOB_TNAME,
  OB_ALL_RESTORE_JOB_HISTORY_TNAME,
  OB_ALL_DDL_ID_TNAME,
  OB_ALL_FOREIGN_KEY_TNAME,
  OB_ALL_FOREIGN_KEY_HISTORY_TNAME,
  OB_ALL_FOREIGN_KEY_COLUMN_TNAME,
  OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TNAME,
  OB_ALL_SYNONYM_TNAME,
  OB_ALL_SYNONYM_HISTORY_TNAME,
  OB_ALL_AUTO_INCREMENT_TNAME,
  OB_ALL_DDL_CHECKSUM_TNAME,
  OB_ALL_ROUTINE_TNAME,
  OB_ALL_ROUTINE_HISTORY_TNAME,
  OB_ALL_ROUTINE_PARAM_TNAME,
  OB_ALL_ROUTINE_PARAM_HISTORY_TNAME,
  OB_ALL_PACKAGE_TNAME,
  OB_ALL_PACKAGE_HISTORY_TNAME,
  OB_ALL_ACQUIRED_SNAPSHOT_TNAME,
  OB_ALL_CONSTRAINT_TNAME,
  OB_ALL_CONSTRAINT_HISTORY_TNAME,
  OB_ALL_ORI_SCHEMA_VERSION_TNAME,
  OB_ALL_FUNC_TNAME,
  OB_ALL_FUNC_HISTORY_TNAME,
  OB_ALL_TEMP_TABLE_TNAME,
  OB_ALL_SEQUENCE_OBJECT_TNAME,
  OB_ALL_SEQUENCE_OBJECT_HISTORY_TNAME,
  OB_ALL_SEQUENCE_VALUE_TNAME,
  OB_ALL_TYPE_TNAME,
  OB_ALL_TYPE_HISTORY_TNAME,
  OB_ALL_TYPE_ATTR_TNAME,
  OB_ALL_TYPE_ATTR_HISTORY_TNAME,
  OB_ALL_COLL_TYPE_TNAME,
  OB_ALL_COLL_TYPE_HISTORY_TNAME,
  OB_ALL_WEAK_READ_SERVICE_TNAME,
  OB_ALL_DBLINK_TNAME,
  OB_ALL_DBLINK_HISTORY_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TNAME,
  OB_ALL_TENANT_KEYSTORE_TNAME,
  OB_ALL_TENANT_KEYSTORE_HISTORY_TNAME,
  OB_ALL_TENANT_OLS_POLICY_TNAME,
  OB_ALL_TENANT_OLS_POLICY_HISTORY_TNAME,
  OB_ALL_TENANT_OLS_COMPONENT_TNAME,
  OB_ALL_TENANT_OLS_COMPONENT_HISTORY_TNAME,
  OB_ALL_TENANT_OLS_LABEL_TNAME,
  OB_ALL_TENANT_OLS_LABEL_HISTORY_TNAME,
  OB_ALL_TENANT_OLS_USER_LEVEL_TNAME,
  OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_TNAME,
  OB_ALL_TENANT_TABLESPACE_TNAME,
  OB_ALL_TENANT_TABLESPACE_HISTORY_TNAME,
  OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TNAME,
  OB_ALL_TENANT_PROFILE_TNAME,
  OB_ALL_TENANT_PROFILE_HISTORY_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TNAME,
  OB_ALL_TENANT_TRIGGER_TNAME,
  OB_ALL_TENANT_TRIGGER_HISTORY_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TNAME,
  OB_ALL_TENANT_SYSAUTH_TNAME,
  OB_ALL_TENANT_SYSAUTH_HISTORY_TNAME,
  OB_ALL_TENANT_OBJAUTH_TNAME,
  OB_ALL_TENANT_OBJAUTH_HISTORY_TNAME,
  OB_ALL_RESTORE_INFO_TNAME,
  OB_ALL_TENANT_ERROR_TNAME,
  OB_ALL_RESTORE_PROGRESS_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TNAME,
  OB_ALL_TENANT_TIME_ZONE_NAME_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TNAME,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
  OB_ALL_TENANT_DEPENDENCY_TNAME,
  OB_ALL_RES_MGR_PLAN_TNAME,
  OB_ALL_RES_MGR_DIRECTIVE_TNAME,
  OB_ALL_RES_MGR_MAPPING_RULE_TNAME,
  OB_ALL_DDL_ERROR_MESSAGE_TNAME,
  OB_ALL_BACKUP_SET_FILES_TNAME,
  OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME,
  OB_ALL_BACKUP_INFO_TNAME,
  OB_ALL_DDL_TASK_STATUS_TNAME,
  OB_ALL_DEADLOCK_EVENT_HISTORY_TNAME,
  OB_ALL_COLUMN_USAGE_TNAME,
  OB_ALL_JOB_TNAME,
  OB_ALL_JOB_LOG_TNAME,
  OB_ALL_TENANT_DIRECTORY_TNAME,
  OB_ALL_TENANT_DIRECTORY_HISTORY_TNAME,
  OB_ALL_TABLE_STAT_TNAME,
  OB_ALL_COLUMN_STAT_TNAME,
  OB_ALL_HISTOGRAM_STAT_TNAME,
  OB_ALL_MONITOR_MODIFIED_TNAME,
  OB_ALL_TABLE_STAT_HISTORY_TNAME,
  OB_ALL_COLUMN_STAT_HISTORY_TNAME,
  OB_ALL_HISTOGRAM_STAT_HISTORY_TNAME,
  OB_ALL_OPTSTAT_GLOBAL_PREFS_TNAME,
  OB_ALL_OPTSTAT_USER_PREFS_TNAME,
  OB_ALL_LS_META_TABLE_TNAME,
  OB_ALL_TABLET_TO_LS_TNAME,
  OB_ALL_TABLET_META_TABLE_TNAME,
  OB_ALL_LS_STATUS_TNAME,
  OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME,
  OB_ALL_LOG_ARCHIVE_HISTORY_TNAME,
  OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME,
  OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
  OB_ALL_LS_TNAME,
  OB_ALL_BACKUP_STORAGE_INFO_TNAME,
  OB_ALL_DAM_LAST_ARCH_TS_TNAME,
  OB_ALL_DAM_CLEANUP_JOBS_TNAME,
  OB_ALL_BACKUP_JOB_TNAME,
  OB_ALL_BACKUP_JOB_HISTORY_TNAME,
  OB_ALL_BACKUP_TASK_TNAME,
  OB_ALL_BACKUP_TASK_HISTORY_TNAME,
  OB_ALL_BACKUP_LS_TASK_TNAME,
  OB_ALL_BACKUP_LS_TASK_HISTORY_TNAME,
  OB_ALL_BACKUP_LS_TASK_INFO_TNAME,
  OB_ALL_BACKUP_SKIPPED_TABLET_TNAME,
  OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_TNAME,
  OB_ALL_TENANT_INFO_TNAME,
  OB_ALL_TABLET_TO_TABLE_HISTORY_TNAME,
  OB_ALL_LS_RECOVERY_STAT_TNAME,
  OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_TNAME,
  OB_ALL_TABLET_REPLICA_CHECKSUM_TNAME,
  OB_ALL_TABLET_CHECKSUM_TNAME,
  OB_ALL_LS_REPLICA_TASK_TNAME,
  OB_ALL_PENDING_TRANSACTION_TNAME,
  OB_ALL_BALANCE_GROUP_LS_STAT_TNAME,
  OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
  OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TNAME,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_TNAME,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TNAME,
  OB_ALL_CONTEXT_TNAME,
  OB_ALL_CONTEXT_HISTORY_TNAME,
  OB_ALL_GLOBAL_CONTEXT_VALUE_TNAME,
  OB_ALL_LS_ELECTION_REFERENCE_INFO_TNAME,
  OB_ALL_BACKUP_DELETE_JOB_TNAME,
  OB_ALL_BACKUP_DELETE_JOB_HISTORY_TNAME,
  OB_ALL_BACKUP_DELETE_TASK_TNAME,
  OB_ALL_BACKUP_DELETE_TASK_HISTORY_TNAME,
  OB_ALL_BACKUP_DELETE_LS_TASK_TNAME,
  OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_TNAME,
  OB_ALL_ZONE_MERGE_INFO_TNAME,
  OB_ALL_MERGE_INFO_TNAME,
  OB_ALL_FREEZE_INFO_TNAME,
  OB_ALL_PLAN_BASELINE_TNAME,
  OB_ALL_PLAN_BASELINE_ITEM_TNAME,
  OB_ALL_SPM_CONFIG_TNAME,
  OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TNAME,
  OB_ALL_BACKUP_PARAMETER_TNAME,
  OB_ALL_LS_RESTORE_PROGRESS_TNAME,
  OB_ALL_LS_RESTORE_HISTORY_TNAME,
  OB_ALL_BACKUP_STORAGE_INFO_HISTORY_TNAME,
  OB_ALL_BACKUP_DELETE_POLICY_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TNAME,
  OB_ALL_LOG_RESTORE_SOURCE_TNAME,
  OB_ALL_KV_TTL_TASK_TNAME,
  OB_ALL_KV_TTL_TASK_HISTORY_TNAME,
  OB_ALL_SERVICE_EPOCH_TNAME,
  OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TNAME,
  OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TNAME,
  OB_ALL_COLUMN_GROUP_TNAME,
  OB_ALL_COLUMN_GROUP_HISTORY_TNAME,
  OB_ALL_COLUMN_GROUP_MAPPING_TNAME,
  OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TNAME,
  OB_ALL_TRANSFER_TASK_TNAME,
  OB_ALL_TRANSFER_TASK_HISTORY_TNAME,
  OB_ALL_BALANCE_JOB_TNAME,
  OB_ALL_BALANCE_JOB_HISTORY_TNAME,
  OB_ALL_BALANCE_TASK_TNAME,
  OB_ALL_BALANCE_TASK_HISTORY_TNAME,
  OB_ALL_LS_ARB_REPLICA_TASK_TNAME,
  OB_ALL_DATA_DICTIONARY_IN_LOG_TNAME,
  OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_TNAME,
  OB_ALL_RLS_POLICY_TNAME,
  OB_ALL_RLS_POLICY_HISTORY_TNAME,
  OB_ALL_RLS_SECURITY_COLUMN_TNAME,
  OB_ALL_RLS_SECURITY_COLUMN_HISTORY_TNAME,
  OB_ALL_RLS_GROUP_TNAME,
  OB_ALL_RLS_GROUP_HISTORY_TNAME,
  OB_ALL_RLS_CONTEXT_TNAME,
  OB_ALL_RLS_CONTEXT_HISTORY_TNAME,
  OB_ALL_RLS_ATTRIBUTE_TNAME,
  OB_ALL_RLS_ATTRIBUTE_HISTORY_TNAME,
  OB_ALL_TENANT_REWRITE_RULES_TNAME,
  OB_ALL_RESERVED_SNAPSHOT_TNAME,
  OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TNAME,
  OB_ALL_EXTERNAL_TABLE_FILE_TNAME,
  OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_TNAME,
  OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TNAME,
  OB_WR_ACTIVE_SESSION_HISTORY_TNAME,
  OB_WR_SNAPSHOT_TNAME,
  OB_WR_STATNAME_TNAME,
  OB_WR_SYSSTAT_TNAME,
  OB_ALL_BALANCE_TASK_HELPER_TNAME,
  OB_ALL_TENANT_SNAPSHOT_TNAME,
  OB_ALL_TENANT_SNAPSHOT_LS_TNAME,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
  OB_ALL_MLOG_TNAME,
  OB_ALL_MVIEW_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TNAME,
  OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_TNAME,
  OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TNAME,
  OB_ALL_MVIEW_REFRESH_STMT_STATS_TNAME,
  OB_ALL_DBMS_LOCK_ALLOCATED_TNAME,
  OB_WR_CONTROL_TNAME,
  OB_ALL_TENANT_EVENT_HISTORY_TNAME,
  OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME,
  OB_ALL_RECOVER_TABLE_JOB_TNAME,
  OB_ALL_RECOVER_TABLE_JOB_HISTORY_TNAME,
  OB_ALL_IMPORT_TABLE_JOB_TNAME,
  OB_ALL_IMPORT_TABLE_JOB_HISTORY_TNAME,
  OB_ALL_IMPORT_TABLE_TASK_TNAME,
  OB_ALL_IMPORT_TABLE_TASK_HISTORY_TNAME,
  OB_ALL_CLONE_JOB_TNAME,
  OB_ALL_CLONE_JOB_HISTORY_TNAME,
  OB_ALL_ROUTINE_PRIVILEGE_TNAME,
  OB_ALL_ROUTINE_PRIVILEGE_HISTORY_TNAME,
  OB_ALL_NCOMP_DLL_TNAME,
  OB_ALL_AUX_STAT_TNAME,
  OB_ALL_INDEX_USAGE_INFO_TNAME,
  OB_ALL_DETECT_LOCK_INFO_TNAME,
  OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TNAME,
  OB_ALL_TRANSFER_PARTITION_TASK_TNAME,
  OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_TNAME,
  OB_ALL_TENANT_SNAPSHOT_JOB_TNAME,
  OB_ALL_COLUMN_PRIVILEGE_TNAME,
  OB_ALL_COLUMN_PRIVILEGE_HISTORY_TNAME,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TNAME,
  OB_ALL_USER_PROXY_INFO_TNAME,
  OB_ALL_USER_PROXY_INFO_HISTORY_TNAME,
  OB_ALL_USER_PROXY_ROLE_INFO_TNAME,
  OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TNAME,
  OB_ALL_MVIEW_DEP_TNAME,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TNAME,
  OB_TENANT_VIRTUAL_ALL_TABLE_TNAME,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_TNAME,
  OB_TENANT_VIRTUAL_TABLE_INDEX_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TNAME,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_TNAME,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TNAME,
  OB_ALL_VIRTUAL_PROCESSLIST_TNAME,
  OB_TENANT_VIRTUAL_WARNING_TNAME,
  OB_TENANT_VIRTUAL_CURRENT_TENANT_TNAME,
  OB_TENANT_VIRTUAL_DATABASE_STATUS_TNAME,
  OB_TENANT_VIRTUAL_TENANT_STATUS_TNAME,
  OB_TENANT_VIRTUAL_STATNAME_TNAME,
  OB_TENANT_VIRTUAL_EVENT_NAME_TNAME,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TNAME,
  OB_TENANT_VIRTUAL_SHOW_TABLES_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TNAME,
  OB_ALL_VIRTUAL_CORE_META_TABLE_TNAME,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TNAME,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TNAME,
  OB_ALL_VIRTUAL_PLAN_STAT_TNAME,
  OB_ALL_VIRTUAL_LATCH_TNAME,
  OB_ALL_VIRTUAL_KVCACHE_INFO_TNAME,
  OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TNAME,
  OB_ALL_VIRTUAL_DATA_TYPE_TNAME,
  OB_ALL_VIRTUAL_SESSION_EVENT_TNAME,
  OB_ALL_VIRTUAL_SESSION_EVENT_ALL_VIRTUAL_SESSION_EVENT_I1_TNAME,
  OB_ALL_VIRTUAL_SESSION_WAIT_TNAME,
  OB_ALL_VIRTUAL_SESSION_WAIT_ALL_VIRTUAL_SESSION_WAIT_I1_TNAME,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TNAME,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TNAME,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_TNAME,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ALL_VIRTUAL_SYSTEM_EVENT_I1_TNAME,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TNAME,
  OB_ALL_VIRTUAL_SESSTAT_TNAME,
  OB_ALL_VIRTUAL_SESSTAT_ALL_VIRTUAL_SESSTAT_I1_TNAME,
  OB_ALL_VIRTUAL_SYSSTAT_TNAME,
  OB_ALL_VIRTUAL_SYSSTAT_ALL_VIRTUAL_SYSSTAT_I1_TNAME,
  OB_ALL_VIRTUAL_MEMSTORE_INFO_TNAME,
  OB_ALL_VIRTUAL_TRANS_STAT_TNAME,
  OB_ALL_VIRTUAL_TRANS_SCHEDULER_TNAME,
  OB_ALL_VIRTUAL_SQL_AUDIT_TNAME,
  OB_ALL_VIRTUAL_SQL_AUDIT_ALL_VIRTUAL_SQL_AUDIT_I1_TNAME,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_TNAME,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TNAME,
  OB_ALL_VIRTUAL_MEMORY_INFO_TNAME,
  OB_ALL_VIRTUAL_TRACE_SPAN_INFO_TNAME,
  OB_ALL_VIRTUAL_ENGINE_TNAME,
  OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TNAME,
  OB_ALL_VIRTUAL_OBRPC_STAT_TNAME,
  OB_TENANT_VIRTUAL_OUTLINE_TNAME,
  OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TNAME,
  OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TNAME,
  OB_ALL_VIRTUAL_PROXY_PARTITION_TNAME,
  OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TNAME,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TNAME,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TNAME,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TNAME,
  OB_ALL_VIRTUAL_AUDIT_OPERATION_TNAME,
  OB_ALL_VIRTUAL_AUDIT_ACTION_TNAME,
  OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_TNAME,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_TNAME,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_TNAME,
  OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_TNAME,
  OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_TNAME,
  OB_ALL_VIRTUAL_SESSION_INFO_TNAME,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_TNAME,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_EVENT_HISTORY_TNAME,
  OB_ALL_VIRTUAL_TABLET_STAT_TNAME,
  OB_SESSION_VARIABLES_TNAME,
  OB_GLOBAL_STATUS_TNAME,
  OB_SESSION_STATUS_TNAME,
  OB_USER_TNAME,
  OB_DB_TNAME,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TNAME,
  OB_PROC_TNAME,
  OB_TENANT_VIRTUAL_COLLATION_TNAME,
  OB_TENANT_VIRTUAL_CHARSET_TNAME,
  OB_ALL_VIRTUAL_TABLE_MGR_TNAME,
  OB_ALL_VIRTUAL_FREEZE_INFO_TNAME,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_TNAME,
  OB_ALL_VIRTUAL_TABLE_TNAME,
  OB_ALL_VIRTUAL_USER_TNAME,
  OB_ALL_VIRTUAL_TENANT_TABLESPACE_TNAME,
  OB_ALL_VIRTUAL_INFORMATION_COLUMNS_TNAME,
  OB_ALL_VIRTUAL_TENANT_USER_FAILED_LOGIN_STAT_TNAME,
  OB_ALL_VIRTUAL_PS_STAT_TNAME,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_TNAME,
  OB_ALL_VIRTUAL_BACKUP_INFO_TNAME,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TNAME,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TNAME,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_TNAME,
  OB_ALL_VIRTUAL_OPEN_CURSOR_TNAME,
  OB_ALL_VIRTUAL_FILES_TNAME,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_TNAME,
  OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TNAME,
  OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_TNAME,
  OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_TNAME,
  OB_ALL_VIRTUAL_PX_TARGET_MONITOR_TNAME,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_TNAME,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_TNAME,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_TNAME,
  OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_TNAME,
  OB_ALL_VIRTUAL_LS_STATUS_TNAME,
  OB_ALL_VIRTUAL_LS_META_TABLE_TNAME,
  OB_ALL_VIRTUAL_TABLET_META_TABLE_TNAME,
  OB_ALL_VIRTUAL_BACKUP_TASK_TNAME,
  OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_TNAME,
  OB_ALL_VIRTUAL_LOG_STAT_TNAME,
  OB_ALL_VIRTUAL_TENANT_INFO_TNAME,
  OB_ALL_VIRTUAL_LS_RECOVERY_STAT_TNAME,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_TNAME,
  OB_ALL_VIRTUAL_UNIT_TNAME,
  OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_TNAME,
  OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_TNAME,
  OB_ALL_VIRTUAL_ARCHIVE_STAT_TNAME,
  OB_ALL_VIRTUAL_APPLY_STAT_TNAME,
  OB_ALL_VIRTUAL_REPLAY_STAT_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_LS_TASK_HISTORY_TNAME,
  OB_ALL_VIRTUAL_LS_INFO_TNAME,
  OB_ALL_VIRTUAL_OBJ_LOCK_TNAME,
  OB_ALL_VIRTUAL_ZONE_MERGE_INFO_TNAME,
  OB_ALL_VIRTUAL_MERGE_INFO_TNAME,
  OB_ALL_VIRTUAL_TRANSACTION_FREEZE_CHECKPOINT_TNAME,
  OB_ALL_VIRTUAL_TRANSACTION_CHECKPOINT_TNAME,
  OB_ALL_VIRTUAL_CHECKPOINT_TNAME,
  OB_ALL_VIRTUAL_BACKUP_SET_FILES_TNAME,
  OB_ALL_VIRTUAL_BACKUP_JOB_TNAME,
  OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_TNAME,
  OB_ALL_VIRTUAL_ASH_TNAME,
  OB_ALL_VIRTUAL_ASH_ALL_VIRTUAL_ASH_I1_TNAME,
  OB_ALL_VIRTUAL_DML_STATS_TNAME,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_TNAME,
  OB_ALL_VIRTUAL_BACKUP_PARAMETER_TNAME,
  OB_ALL_VIRTUAL_RESTORE_JOB_TNAME,
  OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_TNAME,
  OB_ALL_VIRTUAL_RESTORE_PROGRESS_TNAME,
  OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_TNAME,
  OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_TNAME,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_TNAME,
  OB_ALL_VIRTUAL_PRIVILEGE_TNAME,
  OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_TNAME,
  OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_TNAME,
  OB_ALL_VIRTUAL_KV_TTL_TASK_TNAME,
  OB_ALL_VIRTUAL_KV_TTL_TASK_HISTORY_TNAME,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_INFO_TNAME,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_TNAME,
  OB_ALL_VIRTUAL_SHOW_TRACE_TNAME,
  OB_ALL_VIRTUAL_TENANT_MYSQL_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_SQL_PLAN_TNAME,
  OB_ALL_VIRTUAL_MALLOC_SAMPLE_INFO_TNAME,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_TNAME,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_TNAME,
  OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_TNAME,
  OB_ALL_VIRTUAL_MDS_NODE_STAT_TNAME,
  OB_ALL_VIRTUAL_MDS_EVENT_HISTORY_TNAME,
  OB_ALL_VIRTUAL_DUP_LS_LEASE_MGR_TNAME,
  OB_ALL_VIRTUAL_DUP_LS_TABLET_SET_TNAME,
  OB_ALL_VIRTUAL_DUP_LS_TABLETS_TNAME,
  OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_TNAME,
  OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_TNAME,
  OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_TNAME,
  OB_ALL_VIRTUAL_THREAD_TNAME,
  OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_TNAME,
  OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_TNAME,
  OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_TNAME,
  OB_ALL_VIRTUAL_WR_SNAPSHOT_TNAME,
  OB_ALL_VIRTUAL_WR_STATNAME_TNAME,
  OB_ALL_VIRTUAL_WR_SYSSTAT_TNAME,
  OB_ALL_VIRTUAL_KV_CONNECTION_TNAME,
  OB_ALL_VIRTUAL_VIRTUAL_LONG_OPS_STATUS_MYSQL_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TNAME,
  OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_TNAME,
  OB_ALL_VIRTUAL_RESOURCE_POOL_MYSQL_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_PX_P2P_DATAHUB_TNAME,
  OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_TNAME,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_TNAME,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_TNAME,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_TNAME,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_TNAME,
  OB_ALL_VIRTUAL_MLOG_TNAME,
  OB_ALL_VIRTUAL_MVIEW_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_TNAME,
  OB_ALL_VIRTUAL_WR_CONTROL_TNAME,
  OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_TNAME,
  OB_ALL_VIRTUAL_CGROUP_CONFIG_TNAME,
  OB_ALL_VIRTUAL_FLT_CONFIG_TNAME,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_TNAME,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_TNAME,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_TNAME,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_TNAME,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_TNAME,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_TNAME,
  OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_TNAME,
  OB_ALL_VIRTUAL_COLUMN_GROUP_HISTORY_TNAME,
  OB_ALL_VIRTUAL_COLUMN_GROUP_MAPPING_HISTORY_TNAME,
  OB_ALL_VIRTUAL_CLONE_JOB_TNAME,
  OB_ALL_VIRTUAL_CLONE_JOB_HISTORY_TNAME,
  OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_MEMTABLE_INFO_TNAME,
  OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_CHECKPOINT_UNIT_INFO_TNAME,
  OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_INFO_TNAME,
  OB_ALL_VIRTUAL_DETECT_LOCK_INFO_TNAME,
  OB_ALL_VIRTUAL_CLIENT_TO_SERVER_SESSION_INFO_TNAME,
  OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_TNAME,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_JOB_TNAME,
  OB_ALL_VIRTUAL_LS_SNAPSHOT_TNAME,
  OB_ALL_VIRTUAL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TNAME,
  OB_ENABLED_ROLES_TNAME,
  OB_ALL_VIRTUAL_TRACEPOINT_INFO_TNAME,
  OB_ALL_VIRTUAL_COMPATIBILITY_CONTROL_TNAME,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_TNAME,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_TNAME,
  OB_ALL_VIRTUAL_NIC_INFO_TNAME,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_ALL_VIRTUAL_SQL_AUDIT_I1_TNAME,
  OB_ALL_VIRTUAL_PLAN_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TNAME,
  OB_TENANT_VIRTUAL_OUTLINE_AGENT_TNAME,
  OB_ALL_VIRTUAL_PRIVILEGE_ORA_TNAME,
  OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TNAME,
  OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TNAME,
  OB_TENANT_VIRTUAL_CHARSET_AGENT_TNAME,
  OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TNAME,
  OB_TENANT_VIRTUAL_COLLATION_ORA_TNAME,
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
  OB_ALL_VIRTUAL_SESSTAT_ORA_TNAME,
  OB_ALL_VIRTUAL_SESSTAT_ORA_ALL_VIRTUAL_SESSTAT_I1_TNAME,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_TNAME,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_ALL_VIRTUAL_SYSSTAT_I1_TNAME,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_ALL_VIRTUAL_SYSTEM_EVENT_I1_TNAME,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORA_TNAME,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORA_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORA_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_ORA_TNAME,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_ORA_TNAME,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TNAME,
  OB_ALL_VIRTUAL_TRACE_SPAN_INFO_ORA_TNAME,
  OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_AGENT_TNAME,
  OB_ALL_VIRTUAL_DATA_TYPE_ORA_TNAME,
  OB_ALL_VIRTUAL_AUDIT_OPERATION_ORA_TNAME,
  OB_ALL_VIRTUAL_AUDIT_ACTION_ORA_TNAME,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_PS_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLE_MGR_ORA_TNAME,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TNAME,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TNAME,
  OB_ALL_VIRTUAL_SQL_MONITOR_STATNAME_ORA_TNAME,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_OPEN_CURSOR_ORA_TNAME,
  OB_TENANT_VIRTUAL_OBJECT_DEFINITION_ORA_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_TYPE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_TYPE_ATTR_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLL_TYPE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_PACKAGE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_AUTO_INCREMENT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_VALUE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_COLUMN_USAGE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_JOB_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_JOB_LOG_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLE_STAT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_COLUMN_STAT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_HISTOGRAM_STAT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_ORA_TNAME,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TRIGGER_ORA_TNAME,
  OB_ALL_VIRTUAL_PX_TARGET_MONITOR_ORA_TNAME,
  OB_ALL_VIRTUAL_MONITOR_MODIFIED_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_OPTSTAT_GLOBAL_PREFS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_OPTSTAT_USER_PREFS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DBLINK_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_DAM_LAST_ARCH_TS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DAM_CLEANUP_JOBS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SCHEDULER_PROGRAM_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_CONTEXT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_ORA_TNAME,
  OB_ALL_VIRTUAL_TRANS_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_UNIT_ORA_TNAME,
  OB_ALL_VIRTUAL_KVCACHE_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_ORA_TNAME,
  OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_META_TABLE_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLET_META_TABLE_ORA_TNAME,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_ORA_TNAME,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_ORA_TNAME,
  OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_ORA_TNAME,
  OB_ALL_VIRTUAL_PROXY_SCHEMA_ORA_TNAME,
  OB_ALL_VIRTUAL_PROXY_PARTITION_ORA_TNAME,
  OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_ORA_TNAME,
  OB_ALL_VIRTUAL_ZONE_MERGE_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_MERGE_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_BACKUP_JOB_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_TASK_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_SET_FILES_ORA_TNAME,
  OB_ALL_VIRTUAL_PLAN_BASELINE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SPM_CONFIG_REAL_AGENT_ORA_TNAME,
  OB_TENANT_VIRTUAL_EVENT_NAME_ORA_TNAME,
  OB_ALL_VIRTUAL_ASH_ORA_TNAME,
  OB_ALL_VIRTUAL_ASH_ORA_ALL_VIRTUAL_ASH_I1_TNAME,
  OB_ALL_VIRTUAL_DML_STATS_ORA_TNAME,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_ORA_TNAME,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_ORA_TNAME,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_LOG_ARCHIVE_PROGRESS_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_PARAMETER_ORA_TNAME,
  OB_ALL_VIRTUAL_RESTORE_JOB_ORA_TNAME,
  OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_RESTORE_PROGRESS_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_RESTORE_PROGRESS_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_RESTORE_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_OUTLINE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_OUTLINE_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_ORA_TNAME,
  OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_LOG_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_REPLAY_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_APPLY_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_ARCHIVE_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_STATUS_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_RECOVERY_STAT_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_ELECTION_REFERENCE_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_FREEZE_INFO_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_ORA_TNAME,
  OB_ALL_VIRTUAL_SHOW_TRACE_ORA_TNAME,
  OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RLS_ATTRIBUTE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_REWRITE_RULES_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_SQL_PLAN_ORA_TNAME,
  OB_ALL_VIRTUAL_TRANS_SCHEDULER_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_ORA_TNAME,
  OB_ALL_VIRTUAL_EXTERNAL_TABLE_FILE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_ORA_TNAME,
  OB_ALL_VIRTUAL_LONG_OPS_STATUS_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_THREAD_ORA_TNAME,
  OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_WR_SNAPSHOT_ORA_TNAME,
  OB_ALL_VIRTUAL_WR_STATNAME_ORA_TNAME,
  OB_ALL_VIRTUAL_WR_SYSSTAT_ORA_TNAME,
  OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_ORA_TNAME,
  OB_ALL_VIRTUAL_OBJ_LOCK_ORA_TNAME,
  OB_ALL_VIRTUAL_LOG_RESTORE_SOURCE_ORA_TNAME,
  OB_ALL_VIRTUAL_BALANCE_JOB_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_BALANCE_TASK_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TRANSFER_TASK_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RESOURCE_POOL_SYS_AGENT_TNAME,
  OB_ALL_VIRTUAL_PX_P2P_DATAHUB_ORA_TNAME,
  OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_ORA_TNAME,
  OB_ALL_VIRTUAL_MLOG_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_PARAMS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_RUN_STATS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STATS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_CHANGE_STATS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_MVIEW_REFRESH_STMT_STATS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_WR_CONTROL_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_EVENT_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_FLT_CONFIG_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_RUN_DETAIL_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SESSION_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_SCHEDULER_JOB_CLASS_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_ORA_TNAME,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_ORA_TNAME,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_ORA_TNAME,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_CGROUP_CONFIG_ORA_TNAME,
  OB_ALL_VIRTUAL_AUX_STAT_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SYS_VARIABLE_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_ORA_TNAME,
  OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_LS_SNAPSHOT_ORA_TNAME,
  OB_ALL_VIRTUAL_INDEX_USAGE_INFO_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TRACEPOINT_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_ORA_TNAME,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TNAME,
  OB_ALL_VIRTUAL_NIC_INFO_ORA_TNAME,
  OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORA_TNAME,
  OB_ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_REAL_AGENT_ORA_TNAME,
  OB_GV_OB_PLAN_CACHE_STAT_TNAME,
  OB_GV_OB_PLAN_CACHE_PLAN_STAT_TNAME,
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
  OB_ENGINES_TNAME,
  OB_ROUTINES_TNAME,
  OB_PROFILING_TNAME,
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
  OB_GV_OB_SQL_AUDIT_TNAME,
  OB_GV_LATCH_TNAME,
  OB_GV_OB_MEMORY_TNAME,
  OB_V_OB_MEMORY_TNAME,
  OB_GV_OB_MEMSTORE_TNAME,
  OB_V_OB_MEMSTORE_TNAME,
  OB_GV_OB_MEMSTORE_INFO_TNAME,
  OB_V_OB_MEMSTORE_INFO_TNAME,
  OB_V_OB_PLAN_CACHE_STAT_TNAME,
  OB_V_OB_PLAN_CACHE_PLAN_STAT_TNAME,
  OB_GV_OB_PLAN_CACHE_PLAN_EXPLAIN_TNAME,
  OB_V_OB_PLAN_CACHE_PLAN_EXPLAIN_TNAME,
  OB_V_OB_SQL_AUDIT_TNAME,
  OB_V_LATCH_TNAME,
  OB_GV_OB_RPC_OUTGOING_TNAME,
  OB_V_OB_RPC_OUTGOING_TNAME,
  OB_GV_OB_RPC_INCOMING_TNAME,
  OB_V_OB_RPC_INCOMING_TNAME,
  OB_GV_SQL_PLAN_MONITOR_TNAME,
  OB_V_SQL_PLAN_MONITOR_TNAME,
  OB_DBA_RECYCLEBIN_TNAME,
  OB_TIME_ZONE_TNAME,
  OB_TIME_ZONE_NAME_TNAME,
  OB_TIME_ZONE_TRANSITION_TNAME,
  OB_TIME_ZONE_TRANSITION_TYPE_TNAME,
  OB_GV_SESSION_LONGOPS_TNAME,
  OB_V_SESSION_LONGOPS_TNAME,
  OB_DBA_OB_SEQUENCE_OBJECTS_TNAME,
  OB_COLUMNS_TNAME,
  OB_GV_OB_PX_WORKER_STAT_TNAME,
  OB_V_OB_PX_WORKER_STAT_TNAME,
  OB_GV_OB_PS_STAT_TNAME,
  OB_V_OB_PS_STAT_TNAME,
  OB_GV_OB_PS_ITEM_INFO_TNAME,
  OB_V_OB_PS_ITEM_INFO_TNAME,
  OB_GV_SQL_WORKAREA_TNAME,
  OB_V_SQL_WORKAREA_TNAME,
  OB_GV_SQL_WORKAREA_ACTIVE_TNAME,
  OB_V_SQL_WORKAREA_ACTIVE_TNAME,
  OB_GV_SQL_WORKAREA_HISTOGRAM_TNAME,
  OB_V_SQL_WORKAREA_HISTOGRAM_TNAME,
  OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_TNAME,
  OB_V_OB_SQL_WORKAREA_MEMORY_INFO_TNAME,
  OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_TNAME,
  OB_V_OB_PLAN_CACHE_REFERENCE_INFO_TNAME,
  OB_GV_OB_SSTABLES_TNAME,
  OB_V_OB_SSTABLES_TNAME,
  OB_GV_OB_SERVER_SCHEMA_INFO_TNAME,
  OB_V_OB_SERVER_SCHEMA_INFO_TNAME,
  OB_V_SQL_MONITOR_STATNAME_TNAME,
  OB_GV_OB_MERGE_INFO_TNAME,
  OB_V_OB_MERGE_INFO_TNAME,
  OB_V_OB_ENCRYPTED_TABLES_TNAME,
  OB_V_ENCRYPTED_TABLESPACES_TNAME,
  OB_CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS_TNAME,
  OB_GV_OB_TENANT_MEMORY_TNAME,
  OB_V_OB_TENANT_MEMORY_TNAME,
  OB_GV_OB_PX_TARGET_MONITOR_TNAME,
  OB_V_OB_PX_TARGET_MONITOR_TNAME,
  OB_COLUMN_PRIVILEGES_TNAME,
  OB_VIEW_TABLE_USAGE_TNAME,
  OB_FILES_TNAME,
  OB_DBA_OB_TENANTS_TNAME,
  OB_DBA_OB_LS_LOCATIONS_TNAME,
  OB_DBA_OB_TABLET_TO_LS_TNAME,
  OB_DBA_OB_TABLET_REPLICAS_TNAME,
  OB_DBA_OB_TABLEGROUPS_TNAME,
  OB_DBA_OB_TABLEGROUP_PARTITIONS_TNAME,
  OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_TNAME,
  OB_DBA_OB_DATABASES_TNAME,
  OB_DBA_OB_TABLEGROUP_TABLES_TNAME,
  OB_DBA_OB_ZONE_MAJOR_COMPACTION_TNAME,
  OB_DBA_OB_MAJOR_COMPACTION_TNAME,
  OB_DBA_OBJECTS_TNAME,
  OB_DBA_PART_TABLES_TNAME,
  OB_DBA_PART_KEY_COLUMNS_TNAME,
  OB_DBA_SUBPART_KEY_COLUMNS_TNAME,
  OB_DBA_TAB_PARTITIONS_TNAME,
  OB_DBA_TAB_SUBPARTITIONS_TNAME,
  OB_DBA_SUBPARTITION_TEMPLATES_TNAME,
  OB_DBA_PART_INDEXES_TNAME,
  OB_DBA_IND_PARTITIONS_TNAME,
  OB_DBA_IND_SUBPARTITIONS_TNAME,
  OB_GV_OB_UNITS_TNAME,
  OB_V_OB_UNITS_TNAME,
  OB_GV_OB_PARAMETERS_TNAME,
  OB_V_OB_PARAMETERS_TNAME,
  OB_GV_OB_PROCESSLIST_TNAME,
  OB_V_OB_PROCESSLIST_TNAME,
  OB_GV_OB_KVCACHE_TNAME,
  OB_V_OB_KVCACHE_TNAME,
  OB_GV_OB_TRANSACTION_PARTICIPANTS_TNAME,
  OB_V_OB_TRANSACTION_PARTICIPANTS_TNAME,
  OB_GV_OB_COMPACTION_PROGRESS_TNAME,
  OB_V_OB_COMPACTION_PROGRESS_TNAME,
  OB_GV_OB_TABLET_COMPACTION_PROGRESS_TNAME,
  OB_V_OB_TABLET_COMPACTION_PROGRESS_TNAME,
  OB_GV_OB_TABLET_COMPACTION_HISTORY_TNAME,
  OB_V_OB_TABLET_COMPACTION_HISTORY_TNAME,
  OB_GV_OB_COMPACTION_DIAGNOSE_INFO_TNAME,
  OB_V_OB_COMPACTION_DIAGNOSE_INFO_TNAME,
  OB_GV_OB_COMPACTION_SUGGESTIONS_TNAME,
  OB_V_OB_COMPACTION_SUGGESTIONS_TNAME,
  OB_GV_OB_DTL_INTERM_RESULT_MONITOR_TNAME,
  OB_V_OB_DTL_INTERM_RESULT_MONITOR_TNAME,
  OB_DBA_TAB_STATISTICS_TNAME,
  OB_DBA_TAB_COL_STATISTICS_TNAME,
  OB_DBA_PART_COL_STATISTICS_TNAME,
  OB_DBA_SUBPART_COL_STATISTICS_TNAME,
  OB_DBA_TAB_HISTOGRAMS_TNAME,
  OB_DBA_PART_HISTOGRAMS_TNAME,
  OB_DBA_SUBPART_HISTOGRAMS_TNAME,
  OB_DBA_TAB_STATS_HISTORY_TNAME,
  OB_DBA_IND_STATISTICS_TNAME,
  OB_DBA_OB_BACKUP_JOBS_TNAME,
  OB_DBA_OB_BACKUP_JOB_HISTORY_TNAME,
  OB_DBA_OB_BACKUP_TASKS_TNAME,
  OB_DBA_OB_BACKUP_TASK_HISTORY_TNAME,
  OB_DBA_OB_BACKUP_SET_FILES_TNAME,
  OB_DBA_SQL_PLAN_BASELINES_TNAME,
  OB_DBA_SQL_MANAGEMENT_CONFIG_TNAME,
  OB_GV_ACTIVE_SESSION_HISTORY_TNAME,
  OB_V_ACTIVE_SESSION_HISTORY_TNAME,
  OB_GV_DML_STATS_TNAME,
  OB_V_DML_STATS_TNAME,
  OB_DBA_TAB_MODIFICATIONS_TNAME,
  OB_DBA_SCHEDULER_JOBS_TNAME,
  OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_TNAME,
  OB_DBA_OB_BACKUP_STORAGE_INFO_TNAME,
  OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_TNAME,
  OB_DBA_OB_BACKUP_DELETE_POLICY_TNAME,
  OB_DBA_OB_BACKUP_DELETE_JOBS_TNAME,
  OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_TNAME,
  OB_DBA_OB_BACKUP_DELETE_TASKS_TNAME,
  OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_TNAME,
  OB_DBA_OB_OUTLINES_TNAME,
  OB_DBA_OB_CONCURRENT_LIMIT_SQL_TNAME,
  OB_DBA_OB_RESTORE_PROGRESS_TNAME,
  OB_DBA_OB_RESTORE_HISTORY_TNAME,
  OB_DBA_OB_ARCHIVE_DEST_TNAME,
  OB_DBA_OB_ARCHIVELOG_TNAME,
  OB_DBA_OB_ARCHIVELOG_SUMMARY_TNAME,
  OB_DBA_OB_ARCHIVELOG_PIECE_FILES_TNAME,
  OB_DBA_OB_BACKUP_PARAMETER_TNAME,
  OB_DBA_OB_DEADLOCK_EVENT_HISTORY_TNAME,
  OB_DBA_OB_KV_TTL_TASKS_TNAME,
  OB_DBA_OB_KV_TTL_TASK_HISTORY_TNAME,
  OB_GV_OB_LOG_STAT_TNAME,
  OB_V_OB_LOG_STAT_TNAME,
  OB_ST_GEOMETRY_COLUMNS_TNAME,
  OB_ST_SPATIAL_REFERENCE_SYSTEMS_TNAME,
  OB_QUERY_RESPONSE_TIME_TNAME,
  OB_DBA_RSRC_PLANS_TNAME,
  OB_DBA_RSRC_PLAN_DIRECTIVES_TNAME,
  OB_DBA_RSRC_GROUP_MAPPINGS_TNAME,
  OB_DBA_RSRC_CONSUMER_GROUPS_TNAME,
  OB_V_RSRC_PLAN_TNAME,
  OB_DBA_OB_LS_TNAME,
  OB_DBA_OB_TABLE_LOCATIONS_TNAME,
  OB_DBA_OB_FREEZE_INFO_TNAME,
  OB_DBA_OB_LS_REPLICA_TASKS_TNAME,
  OB_V_OB_LS_REPLICA_TASK_PLAN_TNAME,
  OB_DBA_OB_AUTO_INCREMENT_TNAME,
  OB_DBA_SEQUENCES_TNAME,
  OB_DBA_SCHEDULER_WINDOWS_TNAME,
  OB_DBA_OB_USERS_TNAME,
  OB_DBA_OB_DATABASE_PRIVILEGE_TNAME,
  OB_DBA_OB_USER_DEFINED_RULES_TNAME,
  OB_GV_OB_SQL_PLAN_TNAME,
  OB_V_OB_SQL_PLAN_TNAME,
  OB_PARAMETERS_TNAME,
  OB_TABLE_PRIVILEGES_TNAME,
  OB_USER_PRIVILEGES_TNAME,
  OB_SCHEMA_PRIVILEGES_TNAME,
  OB_CHECK_CONSTRAINTS_TNAME,
  OB_REFERENTIAL_CONSTRAINTS_TNAME,
  OB_TABLE_CONSTRAINTS_TNAME,
  OB_GV_OB_TRANSACTION_SCHEDULERS_TNAME,
  OB_V_OB_TRANSACTION_SCHEDULERS_TNAME,
  OB_TRIGGERS_TNAME,
  OB_PARTITIONS_TNAME,
  OB_DBA_OB_LS_ARB_REPLICA_TASKS_TNAME,
  OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_TNAME,
  OB_V_OB_ARCHIVE_DEST_STATUS_TNAME,
  OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_TNAME,
  OB_DBA_OB_RSRC_IO_DIRECTIVES_TNAME,
  OB_GV_OB_TABLET_STATS_TNAME,
  OB_V_OB_TABLET_STATS_TNAME,
  OB_DBA_OB_ACCESS_POINT_TNAME,
  OB_DBA_OB_DATA_DICTIONARY_IN_LOG_TNAME,
  OB_GV_OB_OPT_STAT_GATHER_MONITOR_TNAME,
  OB_V_OB_OPT_STAT_GATHER_MONITOR_TNAME,
  OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_TNAME,
  OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_TNAME,
  OB_GV_OB_THREAD_TNAME,
  OB_V_OB_THREAD_TNAME,
  OB_GV_OB_ARBITRATION_MEMBER_INFO_TNAME,
  OB_V_OB_ARBITRATION_MEMBER_INFO_TNAME,
  OB_GV_OB_ARBITRATION_SERVICE_STATUS_TNAME,
  OB_V_OB_ARBITRATION_SERVICE_STATUS_TNAME,
  OB_DBA_WR_ACTIVE_SESSION_HISTORY_TNAME,
  OB_DBA_WR_SNAPSHOT_TNAME,
  OB_DBA_WR_STATNAME_TNAME,
  OB_DBA_WR_SYSSTAT_TNAME,
  OB_GV_OB_KV_CONNECTIONS_TNAME,
  OB_V_OB_KV_CONNECTIONS_TNAME,
  OB_GV_OB_LOCKS_TNAME,
  OB_V_OB_LOCKS_TNAME,
  OB_DBA_OB_LOG_RESTORE_SOURCE_TNAME,
  OB_V_OB_TIMESTAMP_SERVICE_TNAME,
  OB_DBA_OB_BALANCE_JOBS_TNAME,
  OB_DBA_OB_BALANCE_JOB_HISTORY_TNAME,
  OB_DBA_OB_BALANCE_TASKS_TNAME,
  OB_DBA_OB_BALANCE_TASK_HISTORY_TNAME,
  OB_DBA_OB_TRANSFER_TASKS_TNAME,
  OB_DBA_OB_TRANSFER_TASK_HISTORY_TNAME,
  OB_DBA_OB_EXTERNAL_TABLE_FILES_TNAME,
  OB_ALL_OB_EXTERNAL_TABLE_FILES_TNAME,
  OB_GV_OB_PX_P2P_DATAHUB_TNAME,
  OB_V_OB_PX_P2P_DATAHUB_TNAME,
  OB_GV_SQL_JOIN_FILTER_TNAME,
  OB_V_SQL_JOIN_FILTER_TNAME,
  OB_DBA_OB_TABLE_STAT_STALE_INFO_TNAME,
  OB_V_OB_LS_LOG_RESTORE_STATUS_TNAME,
  OB_DBA_DB_LINKS_TNAME,
  OB_DBA_WR_CONTROL_TNAME,
  OB_DBA_OB_LS_HISTORY_TNAME,
  OB_DBA_OB_TENANT_EVENT_HISTORY_TNAME,
  OB_GV_OB_FLT_TRACE_CONFIG_TNAME,
  OB_GV_OB_SESSION_TNAME,
  OB_V_OB_SESSION_TNAME,
  OB_GV_OB_PL_CACHE_OBJECT_TNAME,
  OB_V_OB_PL_CACHE_OBJECT_TNAME,
  OB_DBA_OB_RECOVER_TABLE_JOBS_TNAME,
  OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_TNAME,
  OB_DBA_OB_IMPORT_TABLE_JOBS_TNAME,
  OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_TNAME,
  OB_DBA_OB_IMPORT_TABLE_TASKS_TNAME,
  OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_TNAME,
  OB_GV_OB_CGROUP_CONFIG_TNAME,
  OB_V_OB_CGROUP_CONFIG_TNAME,
  OB_PROCS_PRIV_TNAME,
  OB_DBA_OB_AUX_STATISTICS_TNAME,
  OB_DBA_INDEX_USAGE_TNAME,
  OB_DBA_OB_SYS_VARIABLES_TNAME,
  OB_DBA_OB_TRANSFER_PARTITION_TASKS_TNAME,
  OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_TNAME,
  OB_ROLE_EDGES_TNAME,
  OB_DEFAULT_ROLES_TNAME,
  OB_COLUMNS_PRIV_TNAME,
  OB_GV_OB_LS_SNAPSHOTS_TNAME,
  OB_V_OB_LS_SNAPSHOTS_TNAME,
  OB_DBA_MVIEW_LOGS_TNAME,
  OB_DBA_MVIEWS_TNAME,
  OB_DBA_MVREF_STATS_SYS_DEFAULTS_TNAME,
  OB_DBA_MVREF_STATS_PARAMS_TNAME,
  OB_DBA_MVREF_RUN_STATS_TNAME,
  OB_DBA_MVREF_STATS_TNAME,
  OB_DBA_MVREF_CHANGE_STATS_TNAME,
  OB_DBA_MVREF_STMT_STATS_TNAME,
  OB_GV_OB_TRACEPOINT_INFO_TNAME,
  OB_V_OB_TRACEPOINT_INFO_TNAME,
  OB_V_OB_COMPATIBILITY_CONTROL_TNAME,
  OB_GV_OB_TENANT_RESOURCE_LIMIT_TNAME,
  OB_V_OB_TENANT_RESOURCE_LIMIT_TNAME,
  OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_TNAME,
  OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_TNAME,
  OB_TABLESPACES_TNAME,
  OB_INNODB_BUFFER_PAGE_TNAME,
  OB_INNODB_BUFFER_PAGE_LRU_TNAME,
  OB_INNODB_BUFFER_POOL_STATS_TNAME,
  OB_INNODB_CMP_TNAME,
  OB_INNODB_CMP_PER_INDEX_TNAME,
  OB_INNODB_CMP_PER_INDEX_RESET_TNAME,
  OB_INNODB_CMP_RESET_TNAME,
  OB_INNODB_CMPMEM_TNAME,
  OB_INNODB_CMPMEM_RESET_TNAME,
  OB_INNODB_SYS_DATAFILES_TNAME,
  OB_INNODB_SYS_INDEXES_TNAME,
  OB_INNODB_SYS_TABLES_TNAME,
  OB_INNODB_SYS_TABLESPACES_TNAME,
  OB_INNODB_SYS_TABLESTATS_TNAME,
  OB_INNODB_SYS_VIRTUAL_TNAME,
  OB_INNODB_TEMP_TABLE_INFO_TNAME,
  OB_INNODB_METRICS_TNAME,
  OB_EVENTS_TNAME,
  OB_V_OB_NIC_INFO_TNAME,
  OB_GV_OB_NIC_INFO_TNAME,
  OB_DBA_SCHEDULER_JOB_RUN_DETAILS_TNAME,
  OB_DBA_SYNONYMS_TNAME,
  OB_DBA_OBJECTS_ORA_TNAME,
  OB_ALL_OBJECTS_TNAME,
  OB_USER_OBJECTS_TNAME,
  OB_DBA_SEQUENCES_ORA_TNAME,
  OB_ALL_SEQUENCES_ORA_TNAME,
  OB_USER_SEQUENCES_ORA_TNAME,
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
  OB_DBA_PART_KEY_COLUMNS_ORA_TNAME,
  OB_ALL_PART_KEY_COLUMNS_TNAME,
  OB_USER_PART_KEY_COLUMNS_TNAME,
  OB_DBA_SUBPART_KEY_COLUMNS_ORA_TNAME,
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
  OB_DBA_PART_INDEXES_ORA_TNAME,
  OB_ALL_PART_INDEXES_ORA_TNAME,
  OB_USER_PART_INDEXES_ORA_TNAME,
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
  OB_DBA_IND_PARTITIONS_ORA_TNAME,
  OB_DBA_IND_SUBPARTITIONS_ORA_TNAME,
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
  OB_AUDIT_ACTIONS_ORA_TNAME,
  OB_STMT_AUDIT_OPTION_MAP_ORA_TNAME,
  OB_ALL_DEF_AUDIT_OPTS_ORA_TNAME,
  OB_DBA_STMT_AUDIT_OPTS_ORA_TNAME,
  OB_DBA_OBJ_AUDIT_OPTS_ORA_TNAME,
  OB_DBA_AUDIT_TRAIL_ORA_TNAME,
  OB_USER_AUDIT_TRAIL_ORA_TNAME,
  OB_DBA_AUDIT_EXISTS_ORA_TNAME,
  OB_DBA_AUDIT_SESSION_ORA_TNAME,
  OB_USER_AUDIT_SESSION_ORA_TNAME,
  OB_DBA_AUDIT_STATEMENT_ORA_TNAME,
  OB_USER_AUDIT_STATEMENT_ORA_TNAME,
  OB_DBA_AUDIT_OBJECT_ORA_TNAME,
  OB_USER_AUDIT_OBJECT_ORA_TNAME,
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
  OB_DBA_RSRC_PLANS_ORA_TNAME,
  OB_DBA_RSRC_PLAN_DIRECTIVES_ORA_TNAME,
  OB_DBA_RSRC_GROUP_MAPPINGS_ORA_TNAME,
  OB_DBA_RECYCLEBIN_ORA_TNAME,
  OB_USER_RECYCLEBIN_ORA_TNAME,
  OB_DBA_RSRC_CONSUMER_GROUPS_ORA_TNAME,
  OB_DBA_OB_LS_LOCATIONS_ORA_TNAME,
  OB_DBA_OB_TABLET_TO_LS_ORA_TNAME,
  OB_DBA_OB_TABLET_REPLICAS_ORA_TNAME,
  OB_DBA_OB_TABLEGROUPS_ORA_TNAME,
  OB_DBA_OB_TABLEGROUP_PARTITIONS_ORA_TNAME,
  OB_DBA_OB_TABLEGROUP_SUBPARTITIONS_ORA_TNAME,
  OB_DBA_OB_DATABASES_ORA_TNAME,
  OB_DBA_OB_TABLEGROUP_TABLES_ORA_TNAME,
  OB_DBA_OB_ZONE_MAJOR_COMPACTION_ORA_TNAME,
  OB_DBA_OB_MAJOR_COMPACTION_ORA_TNAME,
  OB_ALL_IND_STATISTICS_ORA_TNAME,
  OB_DBA_IND_STATISTICS_ORA_TNAME,
  OB_USER_IND_STATISTICS_ORA_TNAME,
  OB_DBA_OB_BACKUP_JOBS_ORA_TNAME,
  OB_DBA_OB_BACKUP_JOB_HISTORY_ORA_TNAME,
  OB_DBA_OB_BACKUP_TASKS_ORA_TNAME,
  OB_DBA_OB_BACKUP_TASK_HISTORY_ORA_TNAME,
  OB_DBA_OB_BACKUP_SET_FILES_ORA_TNAME,
  OB_ALL_TAB_MODIFICATIONS_ORA_TNAME,
  OB_DBA_TAB_MODIFICATIONS_ORA_TNAME,
  OB_USER_TAB_MODIFICATIONS_ORA_TNAME,
  OB_DBA_OB_BACKUP_STORAGE_INFO_ORA_TNAME,
  OB_DBA_OB_BACKUP_STORAGE_INFO_HISTORY_ORA_TNAME,
  OB_DBA_OB_BACKUP_DELETE_POLICY_ORA_TNAME,
  OB_DBA_OB_BACKUP_DELETE_JOBS_ORA_TNAME,
  OB_DBA_OB_BACKUP_DELETE_JOB_HISTORY_ORA_TNAME,
  OB_DBA_OB_BACKUP_DELETE_TASKS_ORA_TNAME,
  OB_DBA_OB_BACKUP_DELETE_TASK_HISTORY_ORA_TNAME,
  OB_DBA_OB_RESTORE_PROGRESS_ORA_TNAME,
  OB_DBA_OB_RESTORE_HISTORY_ORA_TNAME,
  OB_DBA_OB_ARCHIVE_DEST_ORA_TNAME,
  OB_DBA_OB_ARCHIVELOG_ORA_TNAME,
  OB_DBA_OB_ARCHIVELOG_SUMMARY_ORA_TNAME,
  OB_DBA_OB_ARCHIVELOG_PIECE_FILES_ORA_TNAME,
  OB_DBA_OB_BACKUP_PARAMETER_ORA_TNAME,
  OB_DBA_OB_FREEZE_INFO_ORA_TNAME,
  OB_DBA_OB_LS_REPLICA_TASKS_ORA_TNAME,
  OB_V_OB_LS_REPLICA_TASK_PLAN_ORA_TNAME,
  OB_ALL_SCHEDULER_WINDOWS_ORA_TNAME,
  OB_DBA_SCHEDULER_WINDOWS_ORA_TNAME,
  OB_DBA_OB_DATABASE_PRIVILEGE_ORA_TNAME,
  OB_DBA_OB_TENANTS_ORA_TNAME,
  OB_DBA_POLICIES_ORA_TNAME,
  OB_ALL_POLICIES_ORA_TNAME,
  OB_USER_POLICIES_ORA_TNAME,
  OB_DBA_POLICY_GROUPS_ORA_TNAME,
  OB_ALL_POLICY_GROUPS_ORA_TNAME,
  OB_USER_POLICY_GROUPS_ORA_TNAME,
  OB_DBA_POLICY_CONTEXTS_ORA_TNAME,
  OB_ALL_POLICY_CONTEXTS_ORA_TNAME,
  OB_USER_POLICY_CONTEXTS_ORA_TNAME,
  OB_DBA_SEC_RELEVANT_COLS_ORA_TNAME,
  OB_ALL_SEC_RELEVANT_COLS_ORA_TNAME,
  OB_USER_SEC_RELEVANT_COLS_ORA_TNAME,
  OB_DBA_OB_LS_ARB_REPLICA_TASKS_ORA_TNAME,
  OB_DBA_OB_LS_ARB_REPLICA_TASK_HISTORY_ORA_TNAME,
  OB_DBA_OB_RSRC_IO_DIRECTIVES_ORA_TNAME,
  OB_ALL_DB_LINKS_ORA_TNAME,
  OB_DBA_DB_LINKS_ORA_TNAME,
  OB_USER_DB_LINKS_ORA_TNAME,
  OB_DBA_OB_TASK_OPT_STAT_GATHER_HISTORY_ORA_TNAME,
  OB_DBA_OB_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TNAME,
  OB_DBA_WR_ACTIVE_SESSION_HISTORY_ORA_TNAME,
  OB_DBA_WR_SNAPSHOT_ORA_TNAME,
  OB_DBA_WR_STATNAME_ORA_TNAME,
  OB_DBA_WR_SYSSTAT_ORA_TNAME,
  OB_DBA_OB_LOG_RESTORE_SOURCE_ORA_TNAME,
  OB_DBA_OB_EXTERNAL_TABLE_FILES_ORA_TNAME,
  OB_ALL_OB_EXTERNAL_TABLE_FILES_ORA_TNAME,
  OB_DBA_OB_BALANCE_JOBS_ORA_TNAME,
  OB_DBA_OB_BALANCE_JOB_HISTORY_ORA_TNAME,
  OB_DBA_OB_BALANCE_TASKS_ORA_TNAME,
  OB_DBA_OB_BALANCE_TASK_HISTORY_ORA_TNAME,
  OB_DBA_OB_TRANSFER_TASKS_ORA_TNAME,
  OB_DBA_OB_TRANSFER_TASK_HISTORY_ORA_TNAME,
  OB_GV_OB_PX_P2P_DATAHUB_ORA_TNAME,
  OB_V_OB_PX_P2P_DATAHUB_ORA_TNAME,
  OB_GV_SQL_JOIN_FILTER_ORA_TNAME,
  OB_V_SQL_JOIN_FILTER_ORA_TNAME,
  OB_DBA_OB_TABLE_STAT_STALE_INFO_ORA_TNAME,
  OB_DBMS_LOCK_ALLOCATED_ORA_TNAME,
  OB_DBA_WR_CONTROL_ORA_TNAME,
  OB_DBA_OB_LS_HISTORY_ORA_TNAME,
  OB_DBA_OB_TENANT_EVENT_HISTORY_ORA_TNAME,
  OB_DBA_SCHEDULER_JOB_RUN_DETAILS_ORA_TNAME,
  OB_DBA_SCHEDULER_JOB_CLASSES_TNAME,
  OB_DBA_OB_RECOVER_TABLE_JOBS_ORA_TNAME,
  OB_DBA_OB_RECOVER_TABLE_JOB_HISTORY_ORA_TNAME,
  OB_DBA_OB_IMPORT_TABLE_JOBS_ORA_TNAME,
  OB_DBA_OB_IMPORT_TABLE_JOB_HISTORY_ORA_TNAME,
  OB_DBA_OB_IMPORT_TABLE_TASKS_ORA_TNAME,
  OB_DBA_OB_IMPORT_TABLE_TASK_HISTORY_ORA_TNAME,
  OB_DBA_OB_TRANSFER_PARTITION_TASKS_ORA_TNAME,
  OB_DBA_OB_TRANSFER_PARTITION_TASK_HISTORY_ORA_TNAME,
  OB_USER_USERS_TNAME,
  OB_DBA_MVIEW_LOGS_ORA_TNAME,
  OB_ALL_MVIEW_LOGS_ORA_TNAME,
  OB_USER_MVIEW_LOGS_ORA_TNAME,
  OB_DBA_MVIEWS_ORA_TNAME,
  OB_ALL_MVIEWS_ORA_TNAME,
  OB_USER_MVIEWS_ORA_TNAME,
  OB_DBA_MVREF_STATS_SYS_DEFAULTS_ORA_TNAME,
  OB_USER_MVREF_STATS_SYS_DEFAULTS_ORA_TNAME,
  OB_DBA_MVREF_STATS_PARAMS_ORA_TNAME,
  OB_USER_MVREF_STATS_PARAMS_ORA_TNAME,
  OB_DBA_MVREF_RUN_STATS_ORA_TNAME,
  OB_USER_MVREF_RUN_STATS_ORA_TNAME,
  OB_DBA_MVREF_STATS_ORA_TNAME,
  OB_USER_MVREF_STATS_ORA_TNAME,
  OB_DBA_MVREF_CHANGE_STATS_ORA_TNAME,
  OB_USER_MVREF_CHANGE_STATS_ORA_TNAME,
  OB_DBA_MVREF_STMT_STATS_ORA_TNAME,
  OB_USER_MVREF_STMT_STATS_ORA_TNAME,
  OB_PROXY_USERS_TNAME,
  OB_GV_OB_SQL_AUDIT_ORA_TNAME,
  OB_V_OB_SQL_AUDIT_ORA_TNAME,
  OB_GV_INSTANCE_TNAME,
  OB_V_INSTANCE_TNAME,
  OB_GV_OB_PLAN_CACHE_PLAN_STAT_ORA_TNAME,
  OB_V_OB_PLAN_CACHE_PLAN_STAT_ORA_TNAME,
  OB_GV_OB_PLAN_CACHE_PLAN_EXPLAIN_ORA_TNAME,
  OB_V_OB_PLAN_CACHE_PLAN_EXPLAIN_ORA_TNAME,
  OB_GV_SESSION_WAIT_ORA_TNAME,
  OB_V_SESSION_WAIT_ORA_TNAME,
  OB_GV_SESSION_WAIT_HISTORY_ORA_TNAME,
  OB_V_SESSION_WAIT_HISTORY_ORA_TNAME,
  OB_GV_OB_MEMORY_ORA_TNAME,
  OB_V_OB_MEMORY_ORA_TNAME,
  OB_GV_OB_MEMSTORE_ORA_TNAME,
  OB_V_OB_MEMSTORE_ORA_TNAME,
  OB_GV_OB_MEMSTORE_INFO_ORA_TNAME,
  OB_V_OB_MEMSTORE_INFO_ORA_TNAME,
  OB_GV_SESSTAT_ORA_TNAME,
  OB_V_SESSTAT_ORA_TNAME,
  OB_GV_SYSSTAT_ORA_TNAME,
  OB_V_SYSSTAT_ORA_TNAME,
  OB_GV_SYSTEM_EVENT_ORA_TNAME,
  OB_V_SYSTEM_EVENT_ORA_TNAME,
  OB_GV_OB_PLAN_CACHE_STAT_ORA_TNAME,
  OB_V_OB_PLAN_CACHE_STAT_ORA_TNAME,
  OB_NLS_SESSION_PARAMETERS_ORA_TNAME,
  OB_NLS_INSTANCE_PARAMETERS_ORA_TNAME,
  OB_NLS_DATABASE_PARAMETERS_ORA_TNAME,
  OB_V_NLS_PARAMETERS_ORA_TNAME,
  OB_V_VERSION_ORA_TNAME,
  OB_GV_OB_PX_WORKER_STAT_ORA_TNAME,
  OB_V_OB_PX_WORKER_STAT_ORA_TNAME,
  OB_GV_OB_PS_STAT_ORA_TNAME,
  OB_V_OB_PS_STAT_ORA_TNAME,
  OB_GV_OB_PS_ITEM_INFO_ORA_TNAME,
  OB_V_OB_PS_ITEM_INFO_ORA_TNAME,
  OB_GV_SQL_WORKAREA_ACTIVE_ORA_TNAME,
  OB_V_SQL_WORKAREA_ACTIVE_ORA_TNAME,
  OB_GV_SQL_WORKAREA_HISTOGRAM_ORA_TNAME,
  OB_V_SQL_WORKAREA_HISTOGRAM_ORA_TNAME,
  OB_GV_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TNAME,
  OB_V_OB_SQL_WORKAREA_MEMORY_INFO_ORA_TNAME,
  OB_GV_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TNAME,
  OB_V_OB_PLAN_CACHE_REFERENCE_INFO_ORA_TNAME,
  OB_GV_SQL_WORKAREA_ORA_TNAME,
  OB_V_SQL_WORKAREA_ORA_TNAME,
  OB_GV_OB_SSTABLES_ORA_TNAME,
  OB_V_OB_SSTABLES_ORA_TNAME,
  OB_GV_OB_SERVER_SCHEMA_INFO_ORA_TNAME,
  OB_V_OB_SERVER_SCHEMA_INFO_ORA_TNAME,
  OB_GV_SQL_PLAN_MONITOR_ORA_TNAME,
  OB_V_SQL_PLAN_MONITOR_ORA_TNAME,
  OB_V_SQL_MONITOR_STATNAME_ORA_TNAME,
  OB_GV_OPEN_CURSOR_ORA_TNAME,
  OB_V_OPEN_CURSOR_ORA_TNAME,
  OB_V_TIMEZONE_NAMES_ORA_TNAME,
  OB_GV_GLOBAL_TRANSACTION_ORA_TNAME,
  OB_V_GLOBAL_TRANSACTION_ORA_TNAME,
  OB_V_RSRC_PLAN_ORA_TNAME,
  OB_V_OB_ENCRYPTED_TABLES_ORA_TNAME,
  OB_V_ENCRYPTED_TABLESPACES_ORA_TNAME,
  OB_ALL_TAB_COL_STATISTICS_ORA_TNAME,
  OB_DBA_TAB_COL_STATISTICS_ORA_TNAME,
  OB_USER_TAB_COL_STATISTICS_ORA_TNAME,
  OB_ALL_PART_COL_STATISTICS_ORA_TNAME,
  OB_DBA_PART_COL_STATISTICS_ORA_TNAME,
  OB_USER_PART_COL_STATISTICS_ORA_TNAME,
  OB_ALL_SUBPART_COL_STATISTICS_ORA_TNAME,
  OB_DBA_SUBPART_COL_STATISTICS_ORA_TNAME,
  OB_USER_SUBPART_COL_STATISTICS_ORA_TNAME,
  OB_ALL_TAB_HISTOGRAMS_ORA_TNAME,
  OB_DBA_TAB_HISTOGRAMS_ORA_TNAME,
  OB_USER_TAB_HISTOGRAMS_ORA_TNAME,
  OB_ALL_PART_HISTOGRAMS_ORA_TNAME,
  OB_DBA_PART_HISTOGRAMS_ORA_TNAME,
  OB_USER_PART_HISTOGRAMS_ORA_TNAME,
  OB_ALL_SUBPART_HISTOGRAMS_ORA_TNAME,
  OB_DBA_SUBPART_HISTOGRAMS_ORA_TNAME,
  OB_USER_SUBPART_HISTOGRAMS_ORA_TNAME,
  OB_ALL_TAB_STATISTICS_ORA_TNAME,
  OB_DBA_TAB_STATISTICS_ORA_TNAME,
  OB_USER_TAB_STATISTICS_ORA_TNAME,
  OB_DBA_JOBS_TNAME,
  OB_USER_JOBS_TNAME,
  OB_DBA_JOBS_RUNNING_TNAME,
  OB_ALL_DIRECTORIES_TNAME,
  OB_DBA_DIRECTORIES_TNAME,
  OB_GV_OB_TENANT_MEMORY_ORA_TNAME,
  OB_V_OB_TENANT_MEMORY_ORA_TNAME,
  OB_GV_OB_PX_TARGET_MONITOR_ORA_TNAME,
  OB_V_OB_PX_TARGET_MONITOR_ORA_TNAME,
  OB_ALL_TAB_STATS_HISTORY_ORA_TNAME,
  OB_DBA_TAB_STATS_HISTORY_ORA_TNAME,
  OB_USER_TAB_STATS_HISTORY_ORA_TNAME,
  OB_GV_DBLINK_TNAME,
  OB_V_DBLINK_TNAME,
  OB_DBA_SCHEDULER_JOBS_ORA_TNAME,
  OB_DBA_SCHEDULER_PROGRAM_TNAME,
  OB_DBA_CONTEXT_TNAME,
  OB_V_GLOBALCONTEXT_TNAME,
  OB_GV_OB_UNITS_ORA_TNAME,
  OB_V_OB_UNITS_ORA_TNAME,
  OB_GV_OB_PARAMETERS_ORA_TNAME,
  OB_V_OB_PARAMETERS_ORA_TNAME,
  OB_GV_OB_PROCESSLIST_ORA_TNAME,
  OB_V_OB_PROCESSLIST_ORA_TNAME,
  OB_GV_OB_KVCACHE_ORA_TNAME,
  OB_V_OB_KVCACHE_ORA_TNAME,
  OB_GV_OB_TRANSACTION_PARTICIPANTS_ORA_TNAME,
  OB_V_OB_TRANSACTION_PARTICIPANTS_ORA_TNAME,
  OB_GV_OB_COMPACTION_PROGRESS_ORA_TNAME,
  OB_V_OB_COMPACTION_PROGRESS_ORA_TNAME,
  OB_GV_OB_TABLET_COMPACTION_PROGRESS_ORA_TNAME,
  OB_V_OB_TABLET_COMPACTION_PROGRESS_ORA_TNAME,
  OB_GV_OB_TABLET_COMPACTION_HISTORY_ORA_TNAME,
  OB_V_OB_TABLET_COMPACTION_HISTORY_ORA_TNAME,
  OB_GV_OB_COMPACTION_DIAGNOSE_INFO_ORA_TNAME,
  OB_V_OB_COMPACTION_DIAGNOSE_INFO_ORA_TNAME,
  OB_GV_OB_COMPACTION_SUGGESTIONS_ORA_TNAME,
  OB_V_OB_COMPACTION_SUGGESTIONS_ORA_TNAME,
  OB_GV_OB_DTL_INTERM_RESULT_MONITOR_ORA_TNAME,
  OB_V_OB_DTL_INTERM_RESULT_MONITOR_ORA_TNAME,
  OB_DBA_SQL_PLAN_BASELINES_ORA_TNAME,
  OB_DBA_SQL_MANAGEMENT_CONFIG_ORA_TNAME,
  OB_V_EVENT_NAME_ORA_TNAME,
  OB_GV_ACTIVE_SESSION_HISTORY_ORA_TNAME,
  OB_V_ACTIVE_SESSION_HISTORY_ORA_TNAME,
  OB_GV_DML_STATS_ORA_TNAME,
  OB_V_DML_STATS_ORA_TNAME,
  OB_DBA_OB_OUTLINE_CONCURRENT_HISTORY_ORA_TNAME,
  OB_DBA_OB_OUTLINES_ORA_TNAME,
  OB_DBA_OB_CONCURRENT_LIMIT_SQL_ORA_TNAME,
  OB_DBA_OB_DEADLOCK_EVENT_HISTORY_ORA_TNAME,
  OB_GV_OB_LOG_STAT_ORA_TNAME,
  OB_V_OB_LOG_STAT_ORA_TNAME,
  OB_GV_OB_GLOBAL_TRANSACTION_ORA_TNAME,
  OB_V_OB_GLOBAL_TRANSACTION_ORA_TNAME,
  OB_DBA_OB_LS_ORA_TNAME,
  OB_DBA_OB_TABLE_LOCATIONS_ORA_TNAME,
  OB_ALL_TRIGGER_ORDERING_TNAME,
  OB_DBA_TRIGGER_ORDERING_TNAME,
  OB_USER_TRIGGER_ORDERING_TNAME,
  OB_GV_OB_TRANSACTION_SCHEDULERS_ORA_TNAME,
  OB_V_OB_TRANSACTION_SCHEDULERS_ORA_TNAME,
  OB_DBA_OB_USER_DEFINED_RULES_ORA_TNAME,
  OB_GV_OB_SQL_PLAN_ORA_TNAME,
  OB_V_OB_SQL_PLAN_ORA_TNAME,
  OB_V_OB_ARCHIVE_DEST_STATUS_ORA_TNAME,
  OB_DBA_OB_LS_LOG_ARCHIVE_PROGRESS_ORA_TNAME,
  OB_GV_OB_LOCKS_ORA_TNAME,
  OB_V_OB_LOCKS_ORA_TNAME,
  OB_DBA_OB_ACCESS_POINT_ORA_TNAME,
  OB_DBA_OB_DATA_DICTIONARY_IN_LOG_ORA_TNAME,
  OB_GV_OB_OPT_STAT_GATHER_MONITOR_ORA_TNAME,
  OB_V_OB_OPT_STAT_GATHER_MONITOR_ORA_TNAME,
  OB_GV_SESSION_LONGOPS_ORA_TNAME,
  OB_V_SESSION_LONGOPS_ORA_TNAME,
  OB_GV_OB_THREAD_ORA_TNAME,
  OB_V_OB_THREAD_ORA_TNAME,
  OB_GV_OB_ARBITRATION_MEMBER_INFO_ORA_TNAME,
  OB_V_OB_ARBITRATION_MEMBER_INFO_ORA_TNAME,
  OB_GV_OB_ARBITRATION_SERVICE_STATUS_ORA_TNAME,
  OB_V_OB_ARBITRATION_SERVICE_STATUS_ORA_TNAME,
  OB_V_OB_TIMESTAMP_SERVICE_ORA_TNAME,
  OB_V_OB_LS_LOG_RESTORE_STATUS_ORA_TNAME,
  OB_GV_OB_FLT_TRACE_CONFIG_ORA_TNAME,
  OB_GV_OB_SESSION_ORA_TNAME,
  OB_V_OB_SESSION_ORA_TNAME,
  OB_GV_OB_PL_CACHE_OBJECT_ORA_TNAME,
  OB_V_OB_PL_CACHE_OBJECT_ORA_TNAME,
  OB_GV_OB_CGROUP_CONFIG_ORA_TNAME,
  OB_V_OB_CGROUP_CONFIG_ORA_TNAME,
  OB_DBA_OB_AUX_STATISTICS_ORA_TNAME,
  OB_DBA_OB_SYS_VARIABLES_ORA_TNAME,
  OB_DBA_INDEX_USAGE_ORA_TNAME,
  OB_GV_OB_LS_SNAPSHOTS_ORA_TNAME,
  OB_V_OB_LS_SNAPSHOTS_ORA_TNAME,
  OB_GV_OB_TRACEPOINT_INFO_ORA_TNAME,
  OB_V_OB_TRACEPOINT_INFO_ORA_TNAME,
  OB_GV_OB_TENANT_RESOURCE_LIMIT_ORA_TNAME,
  OB_V_OB_TENANT_RESOURCE_LIMIT_ORA_TNAME,
  OB_GV_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TNAME,
  OB_V_OB_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TNAME,
  OB_GV_OB_NIC_INFO_ORA_TNAME,
  OB_V_OB_NIC_INFO_ORA_TNAME,
  OB_DBA_OB_SPATIAL_COLUMNS_ORA_TNAME,
  OB_ALL_TABLE_IDX_DATA_TABLE_ID_TNAME,
  OB_ALL_TABLE_IDX_DB_TB_NAME_TNAME,
  OB_ALL_TABLE_IDX_TB_NAME_TNAME,
  OB_ALL_COLUMN_IDX_TB_COLUMN_NAME_TNAME,
  OB_ALL_COLUMN_IDX_COLUMN_NAME_TNAME,
  OB_ALL_DDL_OPERATION_IDX_DDL_TYPE_TNAME,
  OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TNAME,
  OB_ALL_LOG_ARCHIVE_PIECE_FILES_IDX_STATUS_TNAME,
  OB_ALL_BACKUP_SET_FILES_IDX_STATUS_TNAME,
  OB_ALL_DDL_TASK_STATUS_IDX_TASK_KEY_TNAME,
  OB_ALL_USER_IDX_UR_NAME_TNAME,
  OB_ALL_DATABASE_IDX_DB_NAME_TNAME,
  OB_ALL_TABLEGROUP_IDX_TG_NAME_TNAME,
  OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_DB_TYPE_TNAME,
  OB_ALL_PART_IDX_PART_NAME_TNAME,
  OB_ALL_SUB_PART_IDX_SUB_PART_NAME_TNAME,
  OB_ALL_DEF_SUB_PART_IDX_DEF_SUB_PART_NAME_TNAME,
  OB_ALL_FOREIGN_KEY_IDX_FK_CHILD_TID_TNAME,
  OB_ALL_FOREIGN_KEY_IDX_FK_PARENT_TID_TNAME,
  OB_ALL_FOREIGN_KEY_IDX_FK_NAME_TNAME,
  OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_CHILD_TID_TNAME,
  OB_ALL_FOREIGN_KEY_HISTORY_IDX_FK_HIS_PARENT_TID_TNAME,
  OB_ALL_SYNONYM_IDX_DB_SYNONYM_NAME_TNAME,
  OB_ALL_SYNONYM_IDX_SYNONYM_NAME_TNAME,
  OB_ALL_DDL_CHECKSUM_IDX_DDL_CHECKSUM_TASK_TNAME,
  OB_ALL_ROUTINE_IDX_DB_ROUTINE_NAME_TNAME,
  OB_ALL_ROUTINE_IDX_ROUTINE_NAME_TNAME,
  OB_ALL_ROUTINE_IDX_ROUTINE_PKG_ID_TNAME,
  OB_ALL_ROUTINE_PARAM_IDX_ROUTINE_PARAM_NAME_TNAME,
  OB_ALL_PACKAGE_IDX_DB_PKG_NAME_TNAME,
  OB_ALL_PACKAGE_IDX_PKG_NAME_TNAME,
  OB_ALL_ACQUIRED_SNAPSHOT_IDX_SNAPSHOT_TABLET_TNAME,
  OB_ALL_CONSTRAINT_IDX_CST_NAME_TNAME,
  OB_ALL_TYPE_IDX_DB_TYPE_NAME_TNAME,
  OB_ALL_TYPE_IDX_TYPE_NAME_TNAME,
  OB_ALL_TYPE_ATTR_IDX_TYPE_ATTR_NAME_TNAME,
  OB_ALL_COLL_TYPE_IDX_COLL_NAME_TYPE_TNAME,
  OB_ALL_DBLINK_IDX_OWNER_DBLINK_NAME_TNAME,
  OB_ALL_DBLINK_IDX_DBLINK_NAME_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_IDX_GRANTEE_ROLE_ID_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_IDX_GRANTEE_HIS_ROLE_ID_TNAME,
  OB_ALL_TENANT_KEYSTORE_IDX_KEYSTORE_MASTER_KEY_ID_TNAME,
  OB_ALL_TENANT_KEYSTORE_HISTORY_IDX_KEYSTORE_HIS_MASTER_KEY_ID_TNAME,
  OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_NAME_TNAME,
  OB_ALL_TENANT_OLS_POLICY_IDX_OLS_POLICY_COL_NAME_TNAME,
  OB_ALL_TENANT_OLS_COMPONENT_IDX_OLS_COM_POLICY_ID_TNAME,
  OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_POLICY_ID_TNAME,
  OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TAG_TNAME,
  OB_ALL_TENANT_OLS_LABEL_IDX_OLS_LAB_TNAME,
  OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_UID_TNAME,
  OB_ALL_TENANT_OLS_USER_LEVEL_IDX_OLS_LEVEL_POLICY_ID_TNAME,
  OB_ALL_TENANT_PROFILE_IDX_PROFILE_NAME_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_IDX_AUDIT_TYPE_TNAME,
  OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_BASE_OBJ_ID_TNAME,
  OB_ALL_TENANT_TRIGGER_IDX_DB_TRIGGER_NAME_TNAME,
  OB_ALL_TENANT_TRIGGER_IDX_TRIGGER_NAME_TNAME,
  OB_ALL_TENANT_TRIGGER_HISTORY_IDX_TRIGGER_HIS_BASE_OBJ_ID_TNAME,
  OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTOR_TNAME,
  OB_ALL_TENANT_OBJAUTH_IDX_OBJAUTH_GRANTEE_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_DB_OBJ_NAME_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_IDX_OBJ_TYPE_OBJ_NAME_TNAME,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_IDX_XA_TRANS_ID_TNAME,
  OB_ALL_TENANT_DEPENDENCY_IDX_DEPENDENCY_REF_OBJ_TNAME,
  OB_ALL_DDL_ERROR_MESSAGE_IDX_DDL_ERROR_OBJECT_TNAME,
  OB_ALL_TABLE_STAT_HISTORY_IDX_TABLE_STAT_HIS_SAVTIME_TNAME,
  OB_ALL_COLUMN_STAT_HISTORY_IDX_COLUMN_STAT_HIS_SAVTIME_TNAME,
  OB_ALL_HISTOGRAM_STAT_HISTORY_IDX_HISTOGRAM_STAT_HIS_SAVTIME_TNAME,
  OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_LS_ID_TNAME,
  OB_ALL_TABLET_TO_LS_IDX_TABLET_TO_TABLE_ID_TNAME,
  OB_ALL_PENDING_TRANSACTION_IDX_PENDING_TX_ID_TNAME,
  OB_ALL_CONTEXT_IDX_CTX_NAMESPACE_TNAME,
  OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_SQL_ID_TNAME,
  OB_ALL_PLAN_BASELINE_ITEM_IDX_SPM_ITEM_VALUE_TNAME,
  OB_ALL_TENANT_DIRECTORY_IDX_DIRECTORY_NAME_TNAME,
  OB_ALL_JOB_IDX_JOB_POWNER_TNAME,
  OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_DB_NAME_TNAME,
  OB_ALL_SEQUENCE_OBJECT_IDX_SEQ_OBJ_NAME_TNAME,
  OB_ALL_RECYCLEBIN_IDX_RECYCLEBIN_ORI_NAME_TNAME,
  OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_DB_NAME_TNAME,
  OB_ALL_TABLE_PRIVILEGE_IDX_TB_PRIV_TB_NAME_TNAME,
  OB_ALL_DATABASE_PRIVILEGE_IDX_DB_PRIV_DB_NAME_TNAME,
  OB_ALL_RLS_POLICY_IDX_RLS_POLICY_TABLE_ID_TNAME,
  OB_ALL_RLS_POLICY_IDX_RLS_POLICY_GROUP_ID_TNAME,
  OB_ALL_RLS_POLICY_HISTORY_IDX_RLS_POLICY_HIS_TABLE_ID_TNAME,
  OB_ALL_RLS_GROUP_IDX_RLS_GROUP_TABLE_ID_TNAME,
  OB_ALL_RLS_GROUP_HISTORY_IDX_RLS_GROUP_HIS_TABLE_ID_TNAME,
  OB_ALL_RLS_CONTEXT_IDX_RLS_CONTEXT_TABLE_ID_TNAME,
  OB_ALL_RLS_CONTEXT_HISTORY_IDX_RLS_CONTEXT_HIS_TABLE_ID_TNAME,
  OB_ALL_TENANT_SNAPSHOT_IDX_TENANT_SNAPSHOT_NAME_TNAME,
  OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_TNAME,
  OB_ALL_DBMS_LOCK_ALLOCATED_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_TNAME,
  OB_ALL_KV_TTL_TASK_IDX_KV_TTL_TASK_TABLE_ID_TNAME,
  OB_ALL_KV_TTL_TASK_HISTORY_IDX_KV_TTL_TASK_HISTORY_UPD_TIME_TNAME,
  OB_ALL_MVIEW_REFRESH_RUN_STATS_IDX_MVIEW_REFRESH_RUN_STATS_NUM_MVS_CURRENT_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_END_TIME_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_IDX_MVIEW_REFRESH_STATS_MVIEW_END_TIME_TNAME,
  OB_ALL_TRANSFER_PARTITION_TASK_IDX_TRANSFER_PARTITION_KEY_TNAME,
  OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_IDX_CLIENT_TO_SERVER_SESSION_INFO_CLIENT_SESSION_ID_TNAME,
  OB_ALL_COLUMN_PRIVILEGE_IDX_COLUMN_PRIVILEGE_NAME_TNAME,
  OB_ALL_USER_PROXY_INFO_IDX_USER_PROXY_INFO_PROXY_USER_ID_TNAME,
  OB_ALL_USER_PROXY_INFO_HISTORY_IDX_USER_PROXY_INFO_PROXY_USER_ID_HISTORY_TNAME,
  OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_IDX_USER_PROXY_ROLE_INFO_PROXY_USER_ID_HISTORY_TNAME,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_TNAME,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_TNAME,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_DATA_TABLE_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_DB_TB_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLE_REAL_AGENT_ORA_IDX_TB_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_IDX_TB_COLUMN_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLUMN_REAL_AGENT_ORA_IDX_COLUMN_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_USER_REAL_AGENT_ORA_IDX_UR_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_DATABASE_REAL_AGENT_ORA_IDX_DB_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLEGROUP_REAL_AGENT_ORA_IDX_TG_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_IDX_RECYCLEBIN_DB_TYPE_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_PART_REAL_AGENT_ORA_IDX_PART_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_SUB_PART_REAL_AGENT_ORA_IDX_SUB_PART_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_IDX_DEF_SUB_PART_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_CHILD_TID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_PARENT_TID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_FOREIGN_KEY_REAL_AGENT_ORA_IDX_FK_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_IDX_DB_SYNONYM_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_SYNONYM_REAL_AGENT_ORA_IDX_SYNONYM_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_DB_ROUTINE_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_ROUTINE_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_REAL_AGENT_ORA_IDX_ROUTINE_PKG_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_ROUTINE_PARAM_REAL_AGENT_ORA_IDX_ROUTINE_PARAM_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_IDX_DB_PKG_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_PACKAGE_REAL_AGENT_ORA_IDX_PKG_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_CONSTRAINT_REAL_AGENT_ORA_IDX_CST_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_IDX_DB_TYPE_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TYPE_REAL_AGENT_ORA_IDX_TYPE_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TYPE_ATTR_REAL_AGENT_ORA_IDX_TYPE_ATTR_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLL_TYPE_REAL_AGENT_ORA_IDX_COLL_NAME_TYPE_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_IDX_OWNER_DBLINK_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_IDX_DBLINK_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_IDX_GRANTEE_ROLE_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_KEYSTORE_REAL_AGENT_ORA_IDX_KEYSTORE_MASTER_KEY_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_IDX_OLS_POLICY_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_POLICY_REAL_AGENT_ORA_IDX_OLS_POLICY_COL_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_COMPONENT_REAL_AGENT_ORA_IDX_OLS_COM_POLICY_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_POLICY_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_TAG_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_LABEL_REAL_AGENT_ORA_IDX_OLS_LAB_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_IDX_OLS_LEVEL_UID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OLS_USER_LEVEL_REAL_AGENT_ORA_IDX_OLS_LEVEL_POLICY_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_IDX_PROFILE_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_IDX_AUDIT_TYPE_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_TRIGGER_BASE_OBJ_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_DB_TRIGGER_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_IDX_TRIGGER_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_IDX_OBJAUTH_GRANTOR_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_IDX_OBJAUTH_GRANTEE_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_IDX_OBJ_TYPE_DB_OBJ_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_IDX_OBJ_TYPE_OBJ_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_IDX_DEPENDENCY_REF_OBJ_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLE_STAT_HISTORY_REAL_AGENT_ORA_IDX_TABLE_STAT_HIS_SAVTIME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_COLUMN_STAT_HISTORY_REAL_AGENT_ORA_IDX_COLUMN_STAT_HIS_SAVTIME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_HISTOGRAM_STAT_HISTORY_REAL_AGENT_ORA_IDX_HISTOGRAM_STAT_HIS_SAVTIME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_IDX_TABLET_TO_LS_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_IDX_TABLET_TO_TABLE_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_CONTEXT_REAL_AGENT_ORA_IDX_CTX_NAMESPACE_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_IDX_SPM_ITEM_SQL_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_PLAN_BASELINE_ITEM_REAL_AGENT_ORA_IDX_SPM_ITEM_VALUE_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TENANT_DIRECTORY_REAL_AGENT_ORA_IDX_DIRECTORY_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_JOB_REAL_AGENT_ORA_IDX_JOB_POWNER_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_IDX_SEQ_OBJ_DB_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_SEQUENCE_OBJECT_REAL_AGENT_ORA_IDX_SEQ_OBJ_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_RECYCLEBIN_REAL_AGENT_ORA_IDX_RECYCLEBIN_ORI_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_IDX_TB_PRIV_DB_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_IDX_TB_PRIV_TB_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_DATABASE_PRIVILEGE_REAL_AGENT_ORA_IDX_DB_PRIV_DB_NAME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_IDX_RLS_POLICY_TABLE_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_IDX_RLS_POLICY_GROUP_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORA_IDX_RLS_GROUP_TABLE_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORA_IDX_RLS_CONTEXT_TABLE_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_IDX_DBMS_LOCK_ALLOCATED_LOCKHANDLE_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_IDX_DBMS_LOCK_ALLOCATED_EXPIRATION_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT_ORA_IDX_USER_PROXY_INFO_PROXY_USER_ID_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORA_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_TIME_REAL_AGENT_TNAME,
  OB_ALL_VIRTUAL_SCHEDULER_JOB_RUN_DETAIL_V2_REAL_AGENT_ORA_IDX_SCHEDULER_JOB_RUN_DETAIL_V2_JOB_CLASS_TIME_REAL_AGENT_TNAME,
  OB_ALL_TABLE_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_AUX_LOB_META_TNAME,
  OB_ALL_DDL_OPERATION_AUX_LOB_META_TNAME,
  OB_ALL_USER_AUX_LOB_META_TNAME,
  OB_ALL_USER_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_DATABASE_AUX_LOB_META_TNAME,
  OB_ALL_DATABASE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TABLEGROUP_AUX_LOB_META_TNAME,
  OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TABLE_PRIVILEGE_AUX_LOB_META_TNAME,
  OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_META_TNAME,
  OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TABLE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_HISTORY_AUX_LOB_META_TNAME,
  OB_TENANT_PARAMETER_AUX_LOB_META_TNAME,
  OB_ALL_SYS_VARIABLE_AUX_LOB_META_TNAME,
  OB_ALL_SYS_STAT_AUX_LOB_META_TNAME,
  OB_HELP_TOPIC_AUX_LOB_META_TNAME,
  OB_HELP_CATEGORY_AUX_LOB_META_TNAME,
  OB_HELP_KEYWORD_AUX_LOB_META_TNAME,
  OB_HELP_RELATION_AUX_LOB_META_TNAME,
  OB_ALL_DUMMY_AUX_LOB_META_TNAME,
  OB_ALL_OUTLINE_AUX_LOB_META_TNAME,
  OB_ALL_OUTLINE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_RECYCLEBIN_AUX_LOB_META_TNAME,
  OB_ALL_PART_AUX_LOB_META_TNAME,
  OB_ALL_PART_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_SUB_PART_AUX_LOB_META_TNAME,
  OB_ALL_SUB_PART_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_PART_INFO_AUX_LOB_META_TNAME,
  OB_ALL_PART_INFO_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_DEF_SUB_PART_AUX_LOB_META_TNAME,
  OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_RESTORE_JOB_AUX_LOB_META_TNAME,
  OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_DDL_ID_AUX_LOB_META_TNAME,
  OB_ALL_FOREIGN_KEY_AUX_LOB_META_TNAME,
  OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_META_TNAME,
  OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_SYNONYM_AUX_LOB_META_TNAME,
  OB_ALL_SYNONYM_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_AUTO_INCREMENT_AUX_LOB_META_TNAME,
  OB_ALL_DDL_CHECKSUM_AUX_LOB_META_TNAME,
  OB_ALL_ROUTINE_AUX_LOB_META_TNAME,
  OB_ALL_ROUTINE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_ROUTINE_PARAM_AUX_LOB_META_TNAME,
  OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_PACKAGE_AUX_LOB_META_TNAME,
  OB_ALL_PACKAGE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_META_TNAME,
  OB_ALL_CONSTRAINT_AUX_LOB_META_TNAME,
  OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_META_TNAME,
  OB_ALL_FUNC_AUX_LOB_META_TNAME,
  OB_ALL_FUNC_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TEMP_TABLE_AUX_LOB_META_TNAME,
  OB_ALL_SEQUENCE_OBJECT_AUX_LOB_META_TNAME,
  OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_SEQUENCE_VALUE_AUX_LOB_META_TNAME,
  OB_ALL_TYPE_AUX_LOB_META_TNAME,
  OB_ALL_TYPE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TYPE_ATTR_AUX_LOB_META_TNAME,
  OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_COLL_TYPE_AUX_LOB_META_TNAME,
  OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_WEAK_READ_SERVICE_AUX_LOB_META_TNAME,
  OB_ALL_DBLINK_AUX_LOB_META_TNAME,
  OB_ALL_DBLINK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_KEYSTORE_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OLS_POLICY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OLS_LABEL_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_TABLESPACE_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_PROFILE_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_TRIGGER_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SYSAUTH_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OBJAUTH_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_RESTORE_INFO_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_ERROR_AUX_LOB_META_TNAME,
  OB_ALL_RESTORE_PROGRESS_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_TIME_ZONE_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_DEPENDENCY_AUX_LOB_META_TNAME,
  OB_ALL_RES_MGR_PLAN_AUX_LOB_META_TNAME,
  OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_META_TNAME,
  OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_META_TNAME,
  OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_SET_FILES_AUX_LOB_META_TNAME,
  OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_INFO_AUX_LOB_META_TNAME,
  OB_ALL_DDL_TASK_STATUS_AUX_LOB_META_TNAME,
  OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_USAGE_AUX_LOB_META_TNAME,
  OB_ALL_JOB_AUX_LOB_META_TNAME,
  OB_ALL_JOB_LOG_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_DIRECTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TABLE_STAT_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_STAT_AUX_LOB_META_TNAME,
  OB_ALL_HISTOGRAM_STAT_AUX_LOB_META_TNAME,
  OB_ALL_MONITOR_MODIFIED_AUX_LOB_META_TNAME,
  OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_META_TNAME,
  OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_META_TNAME,
  OB_ALL_LS_META_TABLE_AUX_LOB_META_TNAME,
  OB_ALL_TABLET_TO_LS_AUX_LOB_META_TNAME,
  OB_ALL_TABLET_META_TABLE_AUX_LOB_META_TNAME,
  OB_ALL_LS_STATUS_AUX_LOB_META_TNAME,
  OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TNAME,
  OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_META_TNAME,
  OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TNAME,
  OB_ALL_LS_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_META_TNAME,
  OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_META_TNAME,
  OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_JOB_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_TASK_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_LS_TASK_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_INFO_AUX_LOB_META_TNAME,
  OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_LS_RECOVERY_STAT_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_META_TNAME,
  OB_ALL_TABLET_CHECKSUM_AUX_LOB_META_TNAME,
  OB_ALL_LS_REPLICA_TASK_AUX_LOB_META_TNAME,
  OB_ALL_PENDING_TRANSACTION_AUX_LOB_META_TNAME,
  OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_META_TNAME,
  OB_ALL_CONTEXT_AUX_LOB_META_TNAME,
  OB_ALL_CONTEXT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_META_TNAME,
  OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_ZONE_MERGE_INFO_AUX_LOB_META_TNAME,
  OB_ALL_MERGE_INFO_AUX_LOB_META_TNAME,
  OB_ALL_FREEZE_INFO_AUX_LOB_META_TNAME,
  OB_ALL_PLAN_BASELINE_AUX_LOB_META_TNAME,
  OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_META_TNAME,
  OB_ALL_SPM_CONFIG_AUX_LOB_META_TNAME,
  OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_PARAMETER_AUX_LOB_META_TNAME,
  OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_META_TNAME,
  OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_META_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_META_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_META_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_META_TNAME,
  OB_ALL_KV_TTL_TASK_AUX_LOB_META_TNAME,
  OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_SERVICE_EPOCH_AUX_LOB_META_TNAME,
  OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_GROUP_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TRANSFER_TASK_AUX_LOB_META_TNAME,
  OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_BALANCE_JOB_AUX_LOB_META_TNAME,
  OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_BALANCE_TASK_AUX_LOB_META_TNAME,
  OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_META_TNAME,
  OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_META_TNAME,
  OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_RLS_POLICY_AUX_LOB_META_TNAME,
  OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_META_TNAME,
  OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_RLS_GROUP_AUX_LOB_META_TNAME,
  OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_RLS_CONTEXT_AUX_LOB_META_TNAME,
  OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_RLS_ATTRIBUTE_AUX_LOB_META_TNAME,
  OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_META_TNAME,
  OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_META_TNAME,
  OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_META_TNAME,
  OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_META_TNAME,
  OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TNAME,
  OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_META_TNAME,
  OB_WR_SNAPSHOT_AUX_LOB_META_TNAME,
  OB_WR_STATNAME_AUX_LOB_META_TNAME,
  OB_WR_SYSSTAT_AUX_LOB_META_TNAME,
  OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SNAPSHOT_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_META_TNAME,
  OB_ALL_MLOG_AUX_LOB_META_TNAME,
  OB_ALL_MVIEW_AUX_LOB_META_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_META_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_META_TNAME,
  OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_META_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_META_TNAME,
  OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_META_TNAME,
  OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_META_TNAME,
  OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_META_TNAME,
  OB_WR_CONTROL_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_META_TNAME,
  OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_META_TNAME,
  OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_META_TNAME,
  OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_META_TNAME,
  OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_CLONE_JOB_AUX_LOB_META_TNAME,
  OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_META_TNAME,
  OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_NCOMP_DLL_AUX_LOB_META_TNAME,
  OB_ALL_AUX_STAT_AUX_LOB_META_TNAME,
  OB_ALL_INDEX_USAGE_INFO_AUX_LOB_META_TNAME,
  OB_ALL_DETECT_LOCK_INFO_AUX_LOB_META_TNAME,
  OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_META_TNAME,
  OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_META_TNAME,
  OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_META_TNAME,
  OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_USER_PROXY_INFO_AUX_LOB_META_TNAME,
  OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_META_TNAME,
  OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_META_TNAME,
  OB_ALL_MVIEW_DEP_AUX_LOB_META_TNAME,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_META_TNAME,
  OB_ALL_TABLE_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_AUX_LOB_PIECE_TNAME,
  OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_TNAME,
  OB_ALL_USER_AUX_LOB_PIECE_TNAME,
  OB_ALL_USER_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_DATABASE_AUX_LOB_PIECE_TNAME,
  OB_ALL_DATABASE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLEGROUP_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLE_PRIVILEGE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_PIECE_TNAME,
  OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_TENANT_PARAMETER_AUX_LOB_PIECE_TNAME,
  OB_ALL_SYS_VARIABLE_AUX_LOB_PIECE_TNAME,
  OB_ALL_SYS_STAT_AUX_LOB_PIECE_TNAME,
  OB_HELP_TOPIC_AUX_LOB_PIECE_TNAME,
  OB_HELP_CATEGORY_AUX_LOB_PIECE_TNAME,
  OB_HELP_KEYWORD_AUX_LOB_PIECE_TNAME,
  OB_HELP_RELATION_AUX_LOB_PIECE_TNAME,
  OB_ALL_DUMMY_AUX_LOB_PIECE_TNAME,
  OB_ALL_OUTLINE_AUX_LOB_PIECE_TNAME,
  OB_ALL_OUTLINE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RECYCLEBIN_AUX_LOB_PIECE_TNAME,
  OB_ALL_PART_AUX_LOB_PIECE_TNAME,
  OB_ALL_PART_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_SUB_PART_AUX_LOB_PIECE_TNAME,
  OB_ALL_SUB_PART_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_PART_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_PART_INFO_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_DEF_SUB_PART_AUX_LOB_PIECE_TNAME,
  OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RESTORE_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_DDL_ID_AUX_LOB_PIECE_TNAME,
  OB_ALL_FOREIGN_KEY_AUX_LOB_PIECE_TNAME,
  OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_PIECE_TNAME,
  OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_SYNONYM_AUX_LOB_PIECE_TNAME,
  OB_ALL_SYNONYM_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_AUTO_INCREMENT_AUX_LOB_PIECE_TNAME,
  OB_ALL_DDL_CHECKSUM_AUX_LOB_PIECE_TNAME,
  OB_ALL_ROUTINE_AUX_LOB_PIECE_TNAME,
  OB_ALL_ROUTINE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_ROUTINE_PARAM_AUX_LOB_PIECE_TNAME,
  OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_PACKAGE_AUX_LOB_PIECE_TNAME,
  OB_ALL_PACKAGE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_PIECE_TNAME,
  OB_ALL_CONSTRAINT_AUX_LOB_PIECE_TNAME,
  OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_PIECE_TNAME,
  OB_ALL_FUNC_AUX_LOB_PIECE_TNAME,
  OB_ALL_FUNC_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TEMP_TABLE_AUX_LOB_PIECE_TNAME,
  OB_ALL_SEQUENCE_OBJECT_AUX_LOB_PIECE_TNAME,
  OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_SEQUENCE_VALUE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TYPE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TYPE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TYPE_ATTR_AUX_LOB_PIECE_TNAME,
  OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLL_TYPE_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_WEAK_READ_SERVICE_AUX_LOB_PIECE_TNAME,
  OB_ALL_DBLINK_AUX_LOB_PIECE_TNAME,
  OB_ALL_DBLINK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_KEYSTORE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OLS_POLICY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OLS_LABEL_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_TABLESPACE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_PROFILE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_TRIGGER_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SYSAUTH_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OBJAUTH_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RESTORE_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_ERROR_AUX_LOB_PIECE_TNAME,
  OB_ALL_RESTORE_PROGRESS_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_TIME_ZONE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_DEPENDENCY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RES_MGR_PLAN_AUX_LOB_PIECE_TNAME,
  OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_PIECE_TNAME,
  OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_PIECE_TNAME,
  OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_SET_FILES_AUX_LOB_PIECE_TNAME,
  OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_DDL_TASK_STATUS_AUX_LOB_PIECE_TNAME,
  OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_USAGE_AUX_LOB_PIECE_TNAME,
  OB_ALL_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_JOB_LOG_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_DIRECTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLE_STAT_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_STAT_AUX_LOB_PIECE_TNAME,
  OB_ALL_HISTOGRAM_STAT_AUX_LOB_PIECE_TNAME,
  OB_ALL_MONITOR_MODIFIED_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_PIECE_TNAME,
  OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_META_TABLE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLET_TO_LS_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLET_META_TABLE_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_STATUS_AUX_LOB_PIECE_TNAME,
  OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TNAME,
  OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_PIECE_TNAME,
  OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_LS_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_RECOVERY_STAT_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLET_CHECKSUM_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_REPLICA_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_PENDING_TRANSACTION_AUX_LOB_PIECE_TNAME,
  OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_PIECE_TNAME,
  OB_ALL_CONTEXT_AUX_LOB_PIECE_TNAME,
  OB_ALL_CONTEXT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_ZONE_MERGE_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_MERGE_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_FREEZE_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_PLAN_BASELINE_AUX_LOB_PIECE_TNAME,
  OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_PIECE_TNAME,
  OB_ALL_SPM_CONFIG_AUX_LOB_PIECE_TNAME,
  OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_PARAMETER_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_PIECE_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_PIECE_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_PIECE_TNAME,
  OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_PIECE_TNAME,
  OB_ALL_KV_TTL_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_SERVICE_EPOCH_AUX_LOB_PIECE_TNAME,
  OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_GROUP_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TRANSFER_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_BALANCE_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_BALANCE_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_POLICY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_GROUP_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_CONTEXT_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_ATTRIBUTE_AUX_LOB_PIECE_TNAME,
  OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_PIECE_TNAME,
  OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_PIECE_TNAME,
  OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_PIECE_TNAME,
  OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_WR_SNAPSHOT_AUX_LOB_PIECE_TNAME,
  OB_WR_STATNAME_AUX_LOB_PIECE_TNAME,
  OB_WR_SYSSTAT_AUX_LOB_PIECE_TNAME,
  OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SNAPSHOT_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_PIECE_TNAME,
  OB_ALL_MLOG_AUX_LOB_PIECE_TNAME,
  OB_ALL_MVIEW_AUX_LOB_PIECE_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_PIECE_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_PIECE_TNAME,
  OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_PIECE_TNAME,
  OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_PIECE_TNAME,
  OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_PIECE_TNAME,
  OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_PIECE_TNAME,
  OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_PIECE_TNAME,
  OB_WR_CONTROL_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_PIECE_TNAME,
  OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_CLONE_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_PIECE_TNAME,
  OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_NCOMP_DLL_AUX_LOB_PIECE_TNAME,
  OB_ALL_AUX_STAT_AUX_LOB_PIECE_TNAME,
  OB_ALL_INDEX_USAGE_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_DETECT_LOCK_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_PIECE_TNAME,
  OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_PIECE_TNAME,
  OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_USER_PROXY_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_PIECE_TNAME,
  OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_PIECE_TNAME,
  OB_ALL_MVIEW_DEP_AUX_LOB_PIECE_TNAME,
  OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_PIECE_TNAME,  };

const uint64_t only_rs_vtables [] = {
  OB_ALL_VIRTUAL_CORE_META_TABLE_TID,
  OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID,
  OB_ALL_VIRTUAL_LONG_OPS_STATUS_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_ORA_TID,  };

const uint64_t cluster_distributed_vtables [] = {
  OB_ALL_VIRTUAL_MEM_LEAK_CHECKER_INFO_TID,
  OB_ALL_VIRTUAL_CONCURRENCY_OBJECT_POOL_TID,
  OB_ALL_VIRTUAL_DISK_STAT_TID,
  OB_ALL_VIRTUAL_TRANS_CTX_MGR_STAT_TID,
  OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TID,
  OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_TID,
  OB_ALL_VIRTUAL_SYS_TASK_STATUS_TID,
  OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_TID,
  OB_ALL_VIRTUAL_IO_STAT_TID,
  OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_TID,
  OB_ALL_VIRTUAL_SERVER_BLACKLIST_TID,
  OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_TID,
  OB_ALL_VIRTUAL_DUMP_TENANT_INFO_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_TID,
  OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_TID,
  OB_ALL_VIRTUAL_MASTER_KEY_VERSION_INFO_TID,
  OB_ALL_VIRTUAL_DAG_TID,
  OB_ALL_VIRTUAL_DAG_SCHEDULER_TID,
  OB_ALL_VIRTUAL_IO_CALIBRATION_STATUS_TID,
  OB_ALL_VIRTUAL_IO_BENCHMARK_TID,
  OB_ALL_VIRTUAL_IO_QUOTA_TID,
  OB_ALL_VIRTUAL_DDL_SIM_POINT_STAT_TID,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_TID,
  OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_TID,
  OB_ALL_VIRTUAL_TABLET_STORE_STAT_TID,
  OB_ALL_VIRTUAL_RAID_STAT_TID,
  OB_ALL_VIRTUAL_DTL_CHANNEL_TID,
  OB_ALL_VIRTUAL_DTL_MEMORY_TID,
  OB_ALL_VIRTUAL_ID_SERVICE_TID,
  OB_ALL_VIRTUAL_TENANT_CTX_MEMORY_INFO_TID,
  OB_ALL_VIRTUAL_DBLINK_INFO_TID,
  OB_ALL_VIRTUAL_LOAD_DATA_STAT_TID,
  OB_ALL_VIRTUAL_BACKUP_SCHEDULE_TASK_TID,
  OB_ALL_VIRTUAL_SERVER_TID,
  OB_ALL_VIRTUAL_TABLET_INFO_TID,
  OB_ALL_VIRTUAL_TX_DATA_TABLE_TID,
  OB_ALL_VIRTUAL_TABLET_DDL_KV_INFO_TID,
  OB_ALL_VIRTUAL_TABLET_POINTER_STATUS_TID,
  OB_ALL_VIRTUAL_STORAGE_META_MEMORY_STATUS_TID,
  OB_ALL_VIRTUAL_KVCACHE_STORE_MEMBLOCK_TID,
  OB_ALL_VIRTUAL_SCHEMA_MEMORY_TID,
  OB_ALL_VIRTUAL_SCHEMA_SLOT_TID,
  OB_ALL_VIRTUAL_MINOR_FREEZE_INFO_TID,
  OB_ALL_VIRTUAL_HA_DIAGNOSE_TID,
  OB_ALL_VIRTUAL_IO_SCHEDULER_TID,
  OB_ALL_VIRTUAL_TX_DATA_TID,
  OB_ALL_VIRTUAL_STORAGE_LEAK_INFO_TID,
  OB_ALL_VIRTUAL_STORAGE_HA_ERROR_DIAGNOSE_TID,
  OB_ALL_VIRTUAL_STORAGE_HA_PERF_DIAGNOSE_TID,
  OB_ALL_VIRTUAL_TENANT_SCHEDULER_RUNNING_JOB_TID,  };

const uint64_t tenant_distributed_vtables [] = {
  OB_ALL_VIRTUAL_PROCESSLIST_TID,
  OB_TENANT_VIRTUAL_DATABASE_STATUS_TID,
  OB_TENANT_VIRTUAL_TENANT_STATUS_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID,
  OB_ALL_VIRTUAL_PLAN_STAT_TID,
  OB_ALL_VIRTUAL_LATCH_TID,
  OB_ALL_VIRTUAL_KVCACHE_INFO_TID,
  OB_ALL_VIRTUAL_SESSION_EVENT_TID,
  OB_ALL_VIRTUAL_SESSION_EVENT_ALL_VIRTUAL_SESSION_EVENT_I1_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_ALL_VIRTUAL_SESSION_WAIT_I1_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TID,
  OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID,
  OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TID,
  OB_ALL_VIRTUAL_SESSTAT_TID,
  OB_ALL_VIRTUAL_SESSTAT_ALL_VIRTUAL_SESSTAT_I1_TID,
  OB_ALL_VIRTUAL_SYSSTAT_TID,
  OB_ALL_VIRTUAL_SYSSTAT_ALL_VIRTUAL_SYSSTAT_I1_TID,
  OB_ALL_VIRTUAL_MEMSTORE_INFO_TID,
  OB_ALL_VIRTUAL_TRANS_STAT_TID,
  OB_ALL_VIRTUAL_TRANS_SCHEDULER_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_ALL_VIRTUAL_SQL_AUDIT_I1_TID,
  OB_ALL_VIRTUAL_MEMORY_INFO_TID,
  OB_ALL_VIRTUAL_TRACE_SPAN_INFO_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_ALL_VIRTUAL_OBRPC_STAT_TID,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TID,
  OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_TID,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_TID,
  OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_TID,
  OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_TID,
  OB_ALL_VIRTUAL_SESSION_INFO_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_TID,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_EVENT_HISTORY_TID,
  OB_ALL_VIRTUAL_TABLET_STAT_TID,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TID,
  OB_ALL_VIRTUAL_TABLE_MGR_TID,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_TID,
  OB_ALL_VIRTUAL_PS_STAT_TID,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID,
  OB_ALL_VIRTUAL_OPEN_CURSOR_TID,
  OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_TID,
  OB_ALL_VIRTUAL_PX_TARGET_MONITOR_TID,
  OB_ALL_VIRTUAL_LOG_STAT_TID,
  OB_ALL_VIRTUAL_UNIT_TID,
  OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_TID,
  OB_ALL_VIRTUAL_ARCHIVE_STAT_TID,
  OB_ALL_VIRTUAL_APPLY_STAT_TID,
  OB_ALL_VIRTUAL_REPLAY_STAT_TID,
  OB_ALL_VIRTUAL_LS_INFO_TID,
  OB_ALL_VIRTUAL_OBJ_LOCK_TID,
  OB_ALL_VIRTUAL_TRANSACTION_FREEZE_CHECKPOINT_TID,
  OB_ALL_VIRTUAL_TRANSACTION_CHECKPOINT_TID,
  OB_ALL_VIRTUAL_CHECKPOINT_TID,
  OB_ALL_VIRTUAL_ASH_TID,
  OB_ALL_VIRTUAL_ASH_ALL_VIRTUAL_ASH_I1_TID,
  OB_ALL_VIRTUAL_DML_STATS_TID,
  OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_INFO_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_TID,
  OB_ALL_VIRTUAL_MALLOC_SAMPLE_INFO_TID,
  OB_ALL_VIRTUAL_MDS_NODE_STAT_TID,
  OB_ALL_VIRTUAL_MDS_EVENT_HISTORY_TID,
  OB_ALL_VIRTUAL_DUP_LS_LEASE_MGR_TID,
  OB_ALL_VIRTUAL_DUP_LS_TABLET_SET_TID,
  OB_ALL_VIRTUAL_DUP_LS_TABLETS_TID,
  OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_TID,
  OB_ALL_VIRTUAL_THREAD_TID,
  OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_TID,
  OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_TID,
  OB_ALL_VIRTUAL_KV_CONNECTION_TID,
  OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_TID,
  OB_ALL_VIRTUAL_PX_P2P_DATAHUB_TID,
  OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_TID,
  OB_ALL_VIRTUAL_CGROUP_CONFIG_TID,
  OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_MEMTABLE_INFO_TID,
  OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_CHECKPOINT_UNIT_INFO_TID,
  OB_ALL_VIRTUAL_CHECKPOINT_DIAGNOSE_INFO_TID,
  OB_ALL_VIRTUAL_LS_SNAPSHOT_TID,
  OB_ALL_VIRTUAL_TRACEPOINT_INFO_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_TID,
  OB_ALL_VIRTUAL_NIC_INFO_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_ALL_VIRTUAL_SQL_AUDIT_I1_TID,
  OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID,
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
  OB_ALL_VIRTUAL_SESSTAT_ORA_TID,
  OB_ALL_VIRTUAL_SESSTAT_ORA_ALL_VIRTUAL_SESSTAT_I1_TID,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_TID,
  OB_ALL_VIRTUAL_SYSSTAT_ORA_ALL_VIRTUAL_SYSSTAT_I1_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TID,
  OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID,
  OB_ALL_VIRTUAL_TRACE_SPAN_INFO_ORA_TID,
  OB_ALL_VIRTUAL_PX_WORKER_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PS_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PS_ITEM_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTORY_STAT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_ACTIVE_ORA_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_ORA_TID,
  OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_MGR_ORA_TID,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_ALL_VIRTUAL_SQL_PLAN_MONITOR_I1_TID,
  OB_ALL_VIRTUAL_LOCK_WAIT_STAT_ORA_TID,
  OB_ALL_VIRTUAL_OPEN_CURSOR_ORA_TID,
  OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_ORA_TID,
  OB_ALL_VIRTUAL_PX_TARGET_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_DBLINK_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TRANS_STAT_ORA_TID,
  OB_ALL_VIRTUAL_UNIT_ORA_TID,
  OB_ALL_VIRTUAL_KVCACHE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_ASH_ORA_TID,
  OB_ALL_VIRTUAL_ASH_ORA_ALL_VIRTUAL_ASH_I1_TID,
  OB_ALL_VIRTUAL_DML_STATS_ORA_TID,
  OB_ALL_VIRTUAL_LOG_STAT_ORA_TID,
  OB_ALL_VIRTUAL_REPLAY_STAT_ORA_TID,
  OB_ALL_VIRTUAL_APPLY_STAT_ORA_TID,
  OB_ALL_VIRTUAL_ARCHIVE_STAT_ORA_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_ORA_TID,
  OB_ALL_VIRTUAL_TRANS_SCHEDULER_ORA_TID,
  OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_THREAD_ORA_TID,
  OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_ORA_TID,
  OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_ORA_TID,
  OB_ALL_VIRTUAL_OBJ_LOCK_ORA_TID,
  OB_ALL_VIRTUAL_PX_P2P_DATAHUB_ORA_TID,
  OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_ORA_TID,
  OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_ORA_TID,
  OB_ALL_VIRTUAL_SESSION_INFO_ORA_TID,
  OB_ALL_VIRTUAL_LS_INFO_ORA_TID,
  OB_ALL_VIRTUAL_CGROUP_CONFIG_ORA_TID,
  OB_ALL_VIRTUAL_LS_SNAPSHOT_ORA_TID,
  OB_ALL_VIRTUAL_TRACEPOINT_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID,
  OB_ALL_VIRTUAL_NIC_INFO_ORA_TID,  };

const uint64_t restrict_access_virtual_tables[] = {
  OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID,
  OB_ALL_VIRTUAL_PRIVILEGE_ORA_TID,
  OB_TENANT_VIRTUAL_COLLATION_ORA_TID,
  OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TID,
  OB_ALL_VIRTUAL_PROCESSLIST_ORA_TID,
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
  OB_ALL_VIRTUAL_TABLE_MGR_ORA_TID,
  OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_ENCRYPT_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_MEMORY_INFO_ORA_TID,
  OB_ALL_VIRTUAL_PX_TARGET_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_GLOBAL_CONTEXT_VALUE_ORA_TID,
  OB_ALL_VIRTUAL_TRANS_STAT_ORA_TID,
  OB_ALL_VIRTUAL_UNIT_ORA_TID,
  OB_ALL_VIRTUAL_KVCACHE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_COMPACTION_SUGGESTION_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_LS_META_TABLE_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_TO_LS_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TABLET_META_TABLE_ORA_TID,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_ORA_TID,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_ORA_TID,
  OB_ALL_VIRTUAL_DTL_INTERM_RESULT_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_ZONE_MERGE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_MERGE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_BACKUP_JOB_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_TASK_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_TASK_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_SET_FILES_ORA_TID,
  OB_TENANT_VIRTUAL_EVENT_NAME_ORA_TID,
  OB_ALL_VIRTUAL_ASH_ORA_TID,
  OB_ALL_VIRTUAL_DML_STATS_ORA_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_DEST_PARAMETER_ORA_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_LOG_ARCHIVE_PIECE_FILES_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_PARAMETER_ORA_TID,
  OB_ALL_VIRTUAL_RESTORE_JOB_ORA_TID,
  OB_ALL_VIRTUAL_RESTORE_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_RESTORE_PROGRESS_ORA_TID,
  OB_ALL_VIRTUAL_OUTLINE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_OUTLINE_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_STORAGE_INFO_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_TASK_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_BACKUP_DELETE_POLICY_ORA_TID,
  OB_ALL_VIRTUAL_DEADLOCK_EVENT_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_LOG_STAT_ORA_TID,
  OB_ALL_VIRTUAL_REPLAY_STAT_ORA_TID,
  OB_ALL_VIRTUAL_APPLY_STAT_ORA_TID,
  OB_ALL_VIRTUAL_ARCHIVE_STAT_ORA_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_ORA_TID,
  OB_ALL_VIRTUAL_LS_REPLICA_TASK_PLAN_ORA_TID,
  OB_ALL_VIRTUAL_RLS_POLICY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RLS_SECURITY_COLUMN_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RLS_GROUP_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RLS_CONTEXT_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RLS_ATTRIBUTE_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_SQL_PLAN_ORA_TID,
  OB_ALL_VIRTUAL_TRANS_SCHEDULER_ORA_TID,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_ORA_TID,
  OB_ALL_VIRTUAL_LS_ARB_REPLICA_TASK_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_ORA_TID,
  OB_ALL_VIRTUAL_DATA_DICTIONARY_IN_LOG_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TASK_OPT_STAT_GATHER_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_TABLE_OPT_STAT_GATHER_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_ORA_TID,
  OB_ALL_VIRTUAL_THREAD_ORA_TID,
  OB_ALL_VIRTUAL_WR_ACTIVE_SESSION_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_WR_SNAPSHOT_ORA_TID,
  OB_ALL_VIRTUAL_WR_STATNAME_ORA_TID,
  OB_ALL_VIRTUAL_WR_SYSSTAT_ORA_TID,
  OB_ALL_VIRTUAL_ARBITRATION_MEMBER_INFO_ORA_TID,
  OB_ALL_VIRTUAL_ARBITRATION_SERVICE_STATUS_ORA_TID,
  OB_ALL_VIRTUAL_BALANCE_JOB_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_BALANCE_JOB_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_BALANCE_TASK_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_BALANCE_TASK_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRANSFER_TASK_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRANSFER_TASK_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_RESOURCE_POOL_SYS_AGENT_TID,
  OB_ALL_VIRTUAL_PX_P2P_DATAHUB_ORA_TID,
  OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_ORA_TID,
  OB_ALL_VIRTUAL_LS_LOG_RESTORE_STATUS_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_PARAMETER_ORA_TID,
  OB_ALL_VIRTUAL_DBMS_LOCK_ALLOCATED_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_WR_CONTROL_ORA_TID,
  OB_ALL_VIRTUAL_FLT_CONFIG_ORA_TID,
  OB_ALL_VIRTUAL_SESSION_INFO_ORA_TID,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_ORA_TID,
  OB_ALL_VIRTUAL_RECOVER_TABLE_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_ORA_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_JOB_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_ORA_TID,
  OB_ALL_VIRTUAL_IMPORT_TABLE_TASK_HISTORY_ORA_TID,
  OB_ALL_VIRTUAL_CGROUP_CONFIG_ORA_TID,
  OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRANSFER_PARTITION_TASK_HISTORY_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_LS_SNAPSHOT_ORA_TID,
  OB_ALL_VIRTUAL_INDEX_USAGE_INFO_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TRACEPOINT_INFO_ORA_TID,
  OB_ALL_VIRTUAL_USER_PROXY_INFO_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_USER_PROXY_ROLE_INFO_REAL_AGENT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_ORA_TID,
  OB_ALL_VIRTUAL_TENANT_RESOURCE_LIMIT_DETAIL_ORA_TID,
  OB_ALL_VIRTUAL_NIC_INFO_ORA_TID,
  OB_ALL_VIRTUAL_SPATIAL_REFERENCE_SYSTEMS_REAL_AGENT_ORA_TID  };


static inline bool is_restrict_access_virtual_table(const uint64_t tid)
{
  bool found = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(restrict_access_virtual_tables) && !found; i++) {
    if (tid == restrict_access_virtual_tables[i]) {
      found = true;
    }
  }
  return found;
}

static inline bool is_tenant_table(const uint64_t tid)
{
  bool in_tenant_space = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_space_tables); ++i) {
    if (tid == tenant_space_tables[i]) {
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
    if (tid == all_ora_mapping_virtual_tables[i]) {
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
    if (tid == all_ora_mapping_virtual_tables[i]) {
      is_ora_vt = true;
    }
  }
  return is_ora_vt;
}

static inline uint64_t get_real_table_mappings_tid(const uint64_t tid)
{
  uint64_t org_tid = common::OB_INVALID_ID;
  uint64_t pure_id = tid;
  if (pure_id > common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID && pure_id < common::OB_MAX_VIRTUAL_TABLE_ID) {
    int64_t idx = pure_id - common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID - 1;
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
  uint64_t pure_id = tid;
  vt_mapping = nullptr;
  if (pure_id > common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID && pure_id < common::OB_MAX_VIRTUAL_TABLE_ID) {
    int64_t idx = pure_id - common::OB_MAX_MYSQL_VIRTUAL_TABLE_ID - 1;
    vt_mapping = &vt_mappings[idx];
  }
}

static inline bool is_only_rs_virtual_table(const uint64_t tid)
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < ARRAYSIZEOF(only_rs_vtables); ++i) {
    if (tid == only_rs_vtables[i]) {
      bret = true;
    }
  }
  return bret;
}

static inline bool is_cluster_distributed_vtables(const uint64_t tid)
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < ARRAYSIZEOF(cluster_distributed_vtables); ++i) {
    if (tid == cluster_distributed_vtables[i]) {
      bret = true;
    }
  }
  return bret;
}

static inline bool is_tenant_distributed_vtables(const uint64_t tid)
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < ARRAYSIZEOF(tenant_distributed_vtables); ++i) {
    if (tid == tenant_distributed_vtables[i]) {
      bret = true;
    }
  }
  return bret;
}

/* lob aux table mapping for sys table */
struct LOBMapping
{
  uint64_t data_table_tid_;
  uint64_t lob_meta_tid_;
  uint64_t lob_piece_tid_;
  schema_create_func lob_meta_func_;
  schema_create_func lob_piece_func_;
};

LOBMapping const lob_aux_table_mappings [] = {
  {
    OB_ALL_TABLE_TID,
    OB_ALL_TABLE_AUX_LOB_META_TID,
    OB_ALL_TABLE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_table_aux_lob_meta_schema,
    ObInnerTableSchema::all_table_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_TID,
    OB_ALL_COLUMN_AUX_LOB_META_TID,
    OB_ALL_COLUMN_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_aux_lob_piece_schema
  },

  {
    OB_ALL_DDL_OPERATION_TID,
    OB_ALL_DDL_OPERATION_AUX_LOB_META_TID,
    OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ddl_operation_aux_lob_meta_schema,
    ObInnerTableSchema::all_ddl_operation_aux_lob_piece_schema
  },

  {
    OB_ALL_USER_TID,
    OB_ALL_USER_AUX_LOB_META_TID,
    OB_ALL_USER_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_user_aux_lob_meta_schema,
    ObInnerTableSchema::all_user_aux_lob_piece_schema
  },

  {
    OB_ALL_USER_HISTORY_TID,
    OB_ALL_USER_HISTORY_AUX_LOB_META_TID,
    OB_ALL_USER_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_user_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_user_history_aux_lob_piece_schema
  },

  {
    OB_ALL_DATABASE_TID,
    OB_ALL_DATABASE_AUX_LOB_META_TID,
    OB_ALL_DATABASE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_database_aux_lob_meta_schema,
    ObInnerTableSchema::all_database_aux_lob_piece_schema
  },

  {
    OB_ALL_DATABASE_HISTORY_TID,
    OB_ALL_DATABASE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_DATABASE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_database_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_database_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLEGROUP_TID,
    OB_ALL_TABLEGROUP_AUX_LOB_META_TID,
    OB_ALL_TABLEGROUP_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tablegroup_aux_lob_meta_schema,
    ObInnerTableSchema::all_tablegroup_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLEGROUP_HISTORY_TID,
    OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TABLEGROUP_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tablegroup_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tablegroup_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_TID,
    OB_ALL_TENANT_AUX_LOB_META_TID,
    OB_ALL_TENANT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_HISTORY_TID,
    OB_ALL_TENANT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLE_PRIVILEGE_TID,
    OB_ALL_TABLE_PRIVILEGE_AUX_LOB_META_TID,
    OB_ALL_TABLE_PRIVILEGE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_table_privilege_aux_lob_meta_schema,
    ObInnerTableSchema::all_table_privilege_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLE_PRIVILEGE_HISTORY_TID,
    OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TABLE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_table_privilege_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_table_privilege_history_aux_lob_piece_schema
  },

  {
    OB_ALL_DATABASE_PRIVILEGE_TID,
    OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_META_TID,
    OB_ALL_DATABASE_PRIVILEGE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_database_privilege_aux_lob_meta_schema,
    ObInnerTableSchema::all_database_privilege_aux_lob_piece_schema
  },

  {
    OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID,
    OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_DATABASE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_database_privilege_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_database_privilege_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLE_HISTORY_TID,
    OB_ALL_TABLE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TABLE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_table_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_table_history_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_HISTORY_TID,
    OB_ALL_COLUMN_HISTORY_AUX_LOB_META_TID,
    OB_ALL_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_history_aux_lob_piece_schema
  },

  {
    OB_ALL_ZONE_TID,
    OB_ALL_ZONE_AUX_LOB_META_TID,
    OB_ALL_ZONE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_zone_aux_lob_meta_schema,
    ObInnerTableSchema::all_zone_aux_lob_piece_schema
  },

  {
    OB_ALL_SERVER_TID,
    OB_ALL_SERVER_AUX_LOB_META_TID,
    OB_ALL_SERVER_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_server_aux_lob_meta_schema,
    ObInnerTableSchema::all_server_aux_lob_piece_schema
  },

  {
    OB_ALL_SYS_PARAMETER_TID,
    OB_ALL_SYS_PARAMETER_AUX_LOB_META_TID,
    OB_ALL_SYS_PARAMETER_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_sys_parameter_aux_lob_meta_schema,
    ObInnerTableSchema::all_sys_parameter_aux_lob_piece_schema
  },

  {
    OB_TENANT_PARAMETER_TID,
    OB_TENANT_PARAMETER_AUX_LOB_META_TID,
    OB_TENANT_PARAMETER_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::tenant_parameter_aux_lob_meta_schema,
    ObInnerTableSchema::tenant_parameter_aux_lob_piece_schema
  },

  {
    OB_ALL_SYS_VARIABLE_TID,
    OB_ALL_SYS_VARIABLE_AUX_LOB_META_TID,
    OB_ALL_SYS_VARIABLE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_sys_variable_aux_lob_meta_schema,
    ObInnerTableSchema::all_sys_variable_aux_lob_piece_schema
  },

  {
    OB_ALL_SYS_STAT_TID,
    OB_ALL_SYS_STAT_AUX_LOB_META_TID,
    OB_ALL_SYS_STAT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_sys_stat_aux_lob_meta_schema,
    ObInnerTableSchema::all_sys_stat_aux_lob_piece_schema
  },

  {
    OB_ALL_UNIT_TID,
    OB_ALL_UNIT_AUX_LOB_META_TID,
    OB_ALL_UNIT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_unit_aux_lob_meta_schema,
    ObInnerTableSchema::all_unit_aux_lob_piece_schema
  },

  {
    OB_ALL_UNIT_CONFIG_TID,
    OB_ALL_UNIT_CONFIG_AUX_LOB_META_TID,
    OB_ALL_UNIT_CONFIG_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_unit_config_aux_lob_meta_schema,
    ObInnerTableSchema::all_unit_config_aux_lob_piece_schema
  },

  {
    OB_ALL_RESOURCE_POOL_TID,
    OB_ALL_RESOURCE_POOL_AUX_LOB_META_TID,
    OB_ALL_RESOURCE_POOL_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_resource_pool_aux_lob_meta_schema,
    ObInnerTableSchema::all_resource_pool_aux_lob_piece_schema
  },

  {
    OB_ALL_CHARSET_TID,
    OB_ALL_CHARSET_AUX_LOB_META_TID,
    OB_ALL_CHARSET_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_charset_aux_lob_meta_schema,
    ObInnerTableSchema::all_charset_aux_lob_piece_schema
  },

  {
    OB_ALL_COLLATION_TID,
    OB_ALL_COLLATION_AUX_LOB_META_TID,
    OB_ALL_COLLATION_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_collation_aux_lob_meta_schema,
    ObInnerTableSchema::all_collation_aux_lob_piece_schema
  },

  {
    OB_HELP_TOPIC_TID,
    OB_HELP_TOPIC_AUX_LOB_META_TID,
    OB_HELP_TOPIC_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::help_topic_aux_lob_meta_schema,
    ObInnerTableSchema::help_topic_aux_lob_piece_schema
  },

  {
    OB_HELP_CATEGORY_TID,
    OB_HELP_CATEGORY_AUX_LOB_META_TID,
    OB_HELP_CATEGORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::help_category_aux_lob_meta_schema,
    ObInnerTableSchema::help_category_aux_lob_piece_schema
  },

  {
    OB_HELP_KEYWORD_TID,
    OB_HELP_KEYWORD_AUX_LOB_META_TID,
    OB_HELP_KEYWORD_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::help_keyword_aux_lob_meta_schema,
    ObInnerTableSchema::help_keyword_aux_lob_piece_schema
  },

  {
    OB_HELP_RELATION_TID,
    OB_HELP_RELATION_AUX_LOB_META_TID,
    OB_HELP_RELATION_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::help_relation_aux_lob_meta_schema,
    ObInnerTableSchema::help_relation_aux_lob_piece_schema
  },

  {
    OB_ALL_DUMMY_TID,
    OB_ALL_DUMMY_AUX_LOB_META_TID,
    OB_ALL_DUMMY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_dummy_aux_lob_meta_schema,
    ObInnerTableSchema::all_dummy_aux_lob_piece_schema
  },

  {
    OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID,
    OB_ALL_ROOTSERVICE_EVENT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_ROOTSERVICE_EVENT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rootservice_event_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_rootservice_event_history_aux_lob_piece_schema
  },

  {
    OB_ALL_PRIVILEGE_TID,
    OB_ALL_PRIVILEGE_AUX_LOB_META_TID,
    OB_ALL_PRIVILEGE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_privilege_aux_lob_meta_schema,
    ObInnerTableSchema::all_privilege_aux_lob_piece_schema
  },

  {
    OB_ALL_OUTLINE_TID,
    OB_ALL_OUTLINE_AUX_LOB_META_TID,
    OB_ALL_OUTLINE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_outline_aux_lob_meta_schema,
    ObInnerTableSchema::all_outline_aux_lob_piece_schema
  },

  {
    OB_ALL_OUTLINE_HISTORY_TID,
    OB_ALL_OUTLINE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_OUTLINE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_outline_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_outline_history_aux_lob_piece_schema
  },

  {
    OB_ALL_RECYCLEBIN_TID,
    OB_ALL_RECYCLEBIN_AUX_LOB_META_TID,
    OB_ALL_RECYCLEBIN_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_recyclebin_aux_lob_meta_schema,
    ObInnerTableSchema::all_recyclebin_aux_lob_piece_schema
  },

  {
    OB_ALL_PART_TID,
    OB_ALL_PART_AUX_LOB_META_TID,
    OB_ALL_PART_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_part_aux_lob_meta_schema,
    ObInnerTableSchema::all_part_aux_lob_piece_schema
  },

  {
    OB_ALL_PART_HISTORY_TID,
    OB_ALL_PART_HISTORY_AUX_LOB_META_TID,
    OB_ALL_PART_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_part_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_part_history_aux_lob_piece_schema
  },

  {
    OB_ALL_SUB_PART_TID,
    OB_ALL_SUB_PART_AUX_LOB_META_TID,
    OB_ALL_SUB_PART_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_sub_part_aux_lob_meta_schema,
    ObInnerTableSchema::all_sub_part_aux_lob_piece_schema
  },

  {
    OB_ALL_SUB_PART_HISTORY_TID,
    OB_ALL_SUB_PART_HISTORY_AUX_LOB_META_TID,
    OB_ALL_SUB_PART_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_sub_part_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_sub_part_history_aux_lob_piece_schema
  },

  {
    OB_ALL_PART_INFO_TID,
    OB_ALL_PART_INFO_AUX_LOB_META_TID,
    OB_ALL_PART_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_part_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_part_info_aux_lob_piece_schema
  },

  {
    OB_ALL_PART_INFO_HISTORY_TID,
    OB_ALL_PART_INFO_HISTORY_AUX_LOB_META_TID,
    OB_ALL_PART_INFO_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_part_info_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_part_info_history_aux_lob_piece_schema
  },

  {
    OB_ALL_DEF_SUB_PART_TID,
    OB_ALL_DEF_SUB_PART_AUX_LOB_META_TID,
    OB_ALL_DEF_SUB_PART_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_def_sub_part_aux_lob_meta_schema,
    ObInnerTableSchema::all_def_sub_part_aux_lob_piece_schema
  },

  {
    OB_ALL_DEF_SUB_PART_HISTORY_TID,
    OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_META_TID,
    OB_ALL_DEF_SUB_PART_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_def_sub_part_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_def_sub_part_history_aux_lob_piece_schema
  },

  {
    OB_ALL_SERVER_EVENT_HISTORY_TID,
    OB_ALL_SERVER_EVENT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_SERVER_EVENT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_server_event_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_server_event_history_aux_lob_piece_schema
  },

  {
    OB_ALL_ROOTSERVICE_JOB_TID,
    OB_ALL_ROOTSERVICE_JOB_AUX_LOB_META_TID,
    OB_ALL_ROOTSERVICE_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rootservice_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_rootservice_job_aux_lob_piece_schema
  },

  {
    OB_ALL_SYS_VARIABLE_HISTORY_TID,
    OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_SYS_VARIABLE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_sys_variable_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_sys_variable_history_aux_lob_piece_schema
  },

  {
    OB_ALL_RESTORE_JOB_TID,
    OB_ALL_RESTORE_JOB_AUX_LOB_META_TID,
    OB_ALL_RESTORE_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_restore_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_restore_job_aux_lob_piece_schema
  },

  {
    OB_ALL_RESTORE_JOB_HISTORY_TID,
    OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_META_TID,
    OB_ALL_RESTORE_JOB_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_restore_job_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_restore_job_history_aux_lob_piece_schema
  },

  {
    OB_ALL_DDL_ID_TID,
    OB_ALL_DDL_ID_AUX_LOB_META_TID,
    OB_ALL_DDL_ID_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ddl_id_aux_lob_meta_schema,
    ObInnerTableSchema::all_ddl_id_aux_lob_piece_schema
  },

  {
    OB_ALL_FOREIGN_KEY_TID,
    OB_ALL_FOREIGN_KEY_AUX_LOB_META_TID,
    OB_ALL_FOREIGN_KEY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_foreign_key_aux_lob_meta_schema,
    ObInnerTableSchema::all_foreign_key_aux_lob_piece_schema
  },

  {
    OB_ALL_FOREIGN_KEY_HISTORY_TID,
    OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_META_TID,
    OB_ALL_FOREIGN_KEY_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_foreign_key_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_foreign_key_history_aux_lob_piece_schema
  },

  {
    OB_ALL_FOREIGN_KEY_COLUMN_TID,
    OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_META_TID,
    OB_ALL_FOREIGN_KEY_COLUMN_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_foreign_key_column_aux_lob_meta_schema,
    ObInnerTableSchema::all_foreign_key_column_aux_lob_piece_schema
  },

  {
    OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID,
    OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_META_TID,
    OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_foreign_key_column_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_foreign_key_column_history_aux_lob_piece_schema
  },

  {
    OB_ALL_SYNONYM_TID,
    OB_ALL_SYNONYM_AUX_LOB_META_TID,
    OB_ALL_SYNONYM_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_synonym_aux_lob_meta_schema,
    ObInnerTableSchema::all_synonym_aux_lob_piece_schema
  },

  {
    OB_ALL_SYNONYM_HISTORY_TID,
    OB_ALL_SYNONYM_HISTORY_AUX_LOB_META_TID,
    OB_ALL_SYNONYM_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_synonym_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_synonym_history_aux_lob_piece_schema
  },

  {
    OB_ALL_AUTO_INCREMENT_TID,
    OB_ALL_AUTO_INCREMENT_AUX_LOB_META_TID,
    OB_ALL_AUTO_INCREMENT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_auto_increment_aux_lob_meta_schema,
    ObInnerTableSchema::all_auto_increment_aux_lob_piece_schema
  },

  {
    OB_ALL_DDL_CHECKSUM_TID,
    OB_ALL_DDL_CHECKSUM_AUX_LOB_META_TID,
    OB_ALL_DDL_CHECKSUM_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ddl_checksum_aux_lob_meta_schema,
    ObInnerTableSchema::all_ddl_checksum_aux_lob_piece_schema
  },

  {
    OB_ALL_ROUTINE_TID,
    OB_ALL_ROUTINE_AUX_LOB_META_TID,
    OB_ALL_ROUTINE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_routine_aux_lob_meta_schema,
    ObInnerTableSchema::all_routine_aux_lob_piece_schema
  },

  {
    OB_ALL_ROUTINE_HISTORY_TID,
    OB_ALL_ROUTINE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_ROUTINE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_routine_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_routine_history_aux_lob_piece_schema
  },

  {
    OB_ALL_ROUTINE_PARAM_TID,
    OB_ALL_ROUTINE_PARAM_AUX_LOB_META_TID,
    OB_ALL_ROUTINE_PARAM_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_routine_param_aux_lob_meta_schema,
    ObInnerTableSchema::all_routine_param_aux_lob_piece_schema
  },

  {
    OB_ALL_ROUTINE_PARAM_HISTORY_TID,
    OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_META_TID,
    OB_ALL_ROUTINE_PARAM_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_routine_param_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_routine_param_history_aux_lob_piece_schema
  },

  {
    OB_ALL_PACKAGE_TID,
    OB_ALL_PACKAGE_AUX_LOB_META_TID,
    OB_ALL_PACKAGE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_package_aux_lob_meta_schema,
    ObInnerTableSchema::all_package_aux_lob_piece_schema
  },

  {
    OB_ALL_PACKAGE_HISTORY_TID,
    OB_ALL_PACKAGE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_PACKAGE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_package_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_package_history_aux_lob_piece_schema
  },

  {
    OB_ALL_ACQUIRED_SNAPSHOT_TID,
    OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_META_TID,
    OB_ALL_ACQUIRED_SNAPSHOT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_acquired_snapshot_aux_lob_meta_schema,
    ObInnerTableSchema::all_acquired_snapshot_aux_lob_piece_schema
  },

  {
    OB_ALL_CONSTRAINT_TID,
    OB_ALL_CONSTRAINT_AUX_LOB_META_TID,
    OB_ALL_CONSTRAINT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_constraint_aux_lob_meta_schema,
    ObInnerTableSchema::all_constraint_aux_lob_piece_schema
  },

  {
    OB_ALL_CONSTRAINT_HISTORY_TID,
    OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_CONSTRAINT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_constraint_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_constraint_history_aux_lob_piece_schema
  },

  {
    OB_ALL_ORI_SCHEMA_VERSION_TID,
    OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_META_TID,
    OB_ALL_ORI_SCHEMA_VERSION_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ori_schema_version_aux_lob_meta_schema,
    ObInnerTableSchema::all_ori_schema_version_aux_lob_piece_schema
  },

  {
    OB_ALL_FUNC_TID,
    OB_ALL_FUNC_AUX_LOB_META_TID,
    OB_ALL_FUNC_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_func_aux_lob_meta_schema,
    ObInnerTableSchema::all_func_aux_lob_piece_schema
  },

  {
    OB_ALL_FUNC_HISTORY_TID,
    OB_ALL_FUNC_HISTORY_AUX_LOB_META_TID,
    OB_ALL_FUNC_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_func_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_func_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TEMP_TABLE_TID,
    OB_ALL_TEMP_TABLE_AUX_LOB_META_TID,
    OB_ALL_TEMP_TABLE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_temp_table_aux_lob_meta_schema,
    ObInnerTableSchema::all_temp_table_aux_lob_piece_schema
  },

  {
    OB_ALL_SEQUENCE_OBJECT_TID,
    OB_ALL_SEQUENCE_OBJECT_AUX_LOB_META_TID,
    OB_ALL_SEQUENCE_OBJECT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_sequence_object_aux_lob_meta_schema,
    ObInnerTableSchema::all_sequence_object_aux_lob_piece_schema
  },

  {
    OB_ALL_SEQUENCE_OBJECT_HISTORY_TID,
    OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_SEQUENCE_OBJECT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_sequence_object_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_sequence_object_history_aux_lob_piece_schema
  },

  {
    OB_ALL_SEQUENCE_VALUE_TID,
    OB_ALL_SEQUENCE_VALUE_AUX_LOB_META_TID,
    OB_ALL_SEQUENCE_VALUE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_sequence_value_aux_lob_meta_schema,
    ObInnerTableSchema::all_sequence_value_aux_lob_piece_schema
  },

  {
    OB_ALL_FREEZE_SCHEMA_VERSION_TID,
    OB_ALL_FREEZE_SCHEMA_VERSION_AUX_LOB_META_TID,
    OB_ALL_FREEZE_SCHEMA_VERSION_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_freeze_schema_version_aux_lob_meta_schema,
    ObInnerTableSchema::all_freeze_schema_version_aux_lob_piece_schema
  },

  {
    OB_ALL_TYPE_TID,
    OB_ALL_TYPE_AUX_LOB_META_TID,
    OB_ALL_TYPE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_type_aux_lob_meta_schema,
    ObInnerTableSchema::all_type_aux_lob_piece_schema
  },

  {
    OB_ALL_TYPE_HISTORY_TID,
    OB_ALL_TYPE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TYPE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_type_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_type_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TYPE_ATTR_TID,
    OB_ALL_TYPE_ATTR_AUX_LOB_META_TID,
    OB_ALL_TYPE_ATTR_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_type_attr_aux_lob_meta_schema,
    ObInnerTableSchema::all_type_attr_aux_lob_piece_schema
  },

  {
    OB_ALL_TYPE_ATTR_HISTORY_TID,
    OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TYPE_ATTR_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_type_attr_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_type_attr_history_aux_lob_piece_schema
  },

  {
    OB_ALL_COLL_TYPE_TID,
    OB_ALL_COLL_TYPE_AUX_LOB_META_TID,
    OB_ALL_COLL_TYPE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_coll_type_aux_lob_meta_schema,
    ObInnerTableSchema::all_coll_type_aux_lob_piece_schema
  },

  {
    OB_ALL_COLL_TYPE_HISTORY_TID,
    OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_COLL_TYPE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_coll_type_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_coll_type_history_aux_lob_piece_schema
  },

  {
    OB_ALL_WEAK_READ_SERVICE_TID,
    OB_ALL_WEAK_READ_SERVICE_AUX_LOB_META_TID,
    OB_ALL_WEAK_READ_SERVICE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_weak_read_service_aux_lob_meta_schema,
    ObInnerTableSchema::all_weak_read_service_aux_lob_piece_schema
  },

  {
    OB_ALL_DBLINK_TID,
    OB_ALL_DBLINK_AUX_LOB_META_TID,
    OB_ALL_DBLINK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_dblink_aux_lob_meta_schema,
    ObInnerTableSchema::all_dblink_aux_lob_piece_schema
  },

  {
    OB_ALL_DBLINK_HISTORY_TID,
    OB_ALL_DBLINK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_DBLINK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_dblink_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_dblink_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_ROLE_GRANTEE_MAP_TID,
    OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_META_TID,
    OB_ALL_TENANT_ROLE_GRANTEE_MAP_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_role_grantee_map_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_role_grantee_map_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_TID,
    OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_ROLE_GRANTEE_MAP_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_role_grantee_map_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_role_grantee_map_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_KEYSTORE_TID,
    OB_ALL_TENANT_KEYSTORE_AUX_LOB_META_TID,
    OB_ALL_TENANT_KEYSTORE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_keystore_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_keystore_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_KEYSTORE_HISTORY_TID,
    OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_KEYSTORE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_keystore_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_keystore_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OLS_POLICY_TID,
    OB_ALL_TENANT_OLS_POLICY_AUX_LOB_META_TID,
    OB_ALL_TENANT_OLS_POLICY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_ols_policy_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_ols_policy_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OLS_POLICY_HISTORY_TID,
    OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_OLS_POLICY_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_ols_policy_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_ols_policy_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OLS_COMPONENT_TID,
    OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_META_TID,
    OB_ALL_TENANT_OLS_COMPONENT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_ols_component_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_ols_component_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OLS_COMPONENT_HISTORY_TID,
    OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_OLS_COMPONENT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_ols_component_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_ols_component_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OLS_LABEL_TID,
    OB_ALL_TENANT_OLS_LABEL_AUX_LOB_META_TID,
    OB_ALL_TENANT_OLS_LABEL_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_ols_label_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_ols_label_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OLS_LABEL_HISTORY_TID,
    OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_OLS_LABEL_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_ols_label_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_ols_label_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OLS_USER_LEVEL_TID,
    OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_META_TID,
    OB_ALL_TENANT_OLS_USER_LEVEL_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_ols_user_level_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_ols_user_level_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_TID,
    OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_OLS_USER_LEVEL_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_ols_user_level_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_ols_user_level_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_TABLESPACE_TID,
    OB_ALL_TENANT_TABLESPACE_AUX_LOB_META_TID,
    OB_ALL_TENANT_TABLESPACE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_tablespace_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_tablespace_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_TABLESPACE_HISTORY_TID,
    OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_TABLESPACE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_tablespace_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_tablespace_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TID,
    OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_META_TID,
    OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_user_failed_login_stat_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_user_failed_login_stat_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_PROFILE_TID,
    OB_ALL_TENANT_PROFILE_AUX_LOB_META_TID,
    OB_ALL_TENANT_PROFILE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_profile_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_profile_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_PROFILE_HISTORY_TID,
    OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_PROFILE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_profile_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_profile_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SECURITY_AUDIT_TID,
    OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_META_TID,
    OB_ALL_TENANT_SECURITY_AUDIT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_security_audit_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_security_audit_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TID,
    OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_security_audit_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_security_audit_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_TRIGGER_TID,
    OB_ALL_TENANT_TRIGGER_AUX_LOB_META_TID,
    OB_ALL_TENANT_TRIGGER_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_trigger_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_trigger_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_TRIGGER_HISTORY_TID,
    OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_TRIGGER_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_trigger_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_trigger_history_aux_lob_piece_schema
  },

  {
    OB_ALL_SEED_PARAMETER_TID,
    OB_ALL_SEED_PARAMETER_AUX_LOB_META_TID,
    OB_ALL_SEED_PARAMETER_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_seed_parameter_aux_lob_meta_schema,
    ObInnerTableSchema::all_seed_parameter_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TID,
    OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_META_TID,
    OB_ALL_TENANT_SECURITY_AUDIT_RECORD_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_security_audit_record_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_security_audit_record_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SYSAUTH_TID,
    OB_ALL_TENANT_SYSAUTH_AUX_LOB_META_TID,
    OB_ALL_TENANT_SYSAUTH_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_sysauth_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_sysauth_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SYSAUTH_HISTORY_TID,
    OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_SYSAUTH_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_sysauth_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_sysauth_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OBJAUTH_TID,
    OB_ALL_TENANT_OBJAUTH_AUX_LOB_META_TID,
    OB_ALL_TENANT_OBJAUTH_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_objauth_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_objauth_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OBJAUTH_HISTORY_TID,
    OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_OBJAUTH_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_objauth_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_objauth_history_aux_lob_piece_schema
  },

  {
    OB_ALL_RESTORE_INFO_TID,
    OB_ALL_RESTORE_INFO_AUX_LOB_META_TID,
    OB_ALL_RESTORE_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_restore_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_restore_info_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_ERROR_TID,
    OB_ALL_TENANT_ERROR_AUX_LOB_META_TID,
    OB_ALL_TENANT_ERROR_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_error_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_error_aux_lob_piece_schema
  },

  {
    OB_ALL_RESTORE_PROGRESS_TID,
    OB_ALL_RESTORE_PROGRESS_AUX_LOB_META_TID,
    OB_ALL_RESTORE_PROGRESS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_restore_progress_aux_lob_meta_schema,
    ObInnerTableSchema::all_restore_progress_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OBJECT_TYPE_TID,
    OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_META_TID,
    OB_ALL_TENANT_OBJECT_TYPE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_object_type_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_object_type_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TID,
    OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_OBJECT_TYPE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_object_type_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_object_type_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_TIME_ZONE_TID,
    OB_ALL_TENANT_TIME_ZONE_AUX_LOB_META_TID,
    OB_ALL_TENANT_TIME_ZONE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_time_zone_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_time_zone_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_TIME_ZONE_NAME_TID,
    OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_META_TID,
    OB_ALL_TENANT_TIME_ZONE_NAME_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_time_zone_name_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_time_zone_name_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID,
    OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_META_TID,
    OB_ALL_TENANT_TIME_ZONE_TRANSITION_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_time_zone_transition_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_time_zone_transition_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID,
    OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_META_TID,
    OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_time_zone_transition_type_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_time_zone_transition_type_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_CONSTRAINT_COLUMN_TID,
    OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_META_TID,
    OB_ALL_TENANT_CONSTRAINT_COLUMN_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_constraint_column_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_constraint_column_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TID,
    OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_constraint_column_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_constraint_column_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_GLOBAL_TRANSACTION_TID,
    OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_META_TID,
    OB_ALL_TENANT_GLOBAL_TRANSACTION_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_global_transaction_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_global_transaction_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_DEPENDENCY_TID,
    OB_ALL_TENANT_DEPENDENCY_AUX_LOB_META_TID,
    OB_ALL_TENANT_DEPENDENCY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_dependency_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_dependency_aux_lob_piece_schema
  },

  {
    OB_ALL_RES_MGR_PLAN_TID,
    OB_ALL_RES_MGR_PLAN_AUX_LOB_META_TID,
    OB_ALL_RES_MGR_PLAN_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_res_mgr_plan_aux_lob_meta_schema,
    ObInnerTableSchema::all_res_mgr_plan_aux_lob_piece_schema
  },

  {
    OB_ALL_RES_MGR_DIRECTIVE_TID,
    OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_META_TID,
    OB_ALL_RES_MGR_DIRECTIVE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_res_mgr_directive_aux_lob_meta_schema,
    ObInnerTableSchema::all_res_mgr_directive_aux_lob_piece_schema
  },

  {
    OB_ALL_RES_MGR_MAPPING_RULE_TID,
    OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_META_TID,
    OB_ALL_RES_MGR_MAPPING_RULE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_res_mgr_mapping_rule_aux_lob_meta_schema,
    ObInnerTableSchema::all_res_mgr_mapping_rule_aux_lob_piece_schema
  },

  {
    OB_ALL_DDL_ERROR_MESSAGE_TID,
    OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_META_TID,
    OB_ALL_DDL_ERROR_MESSAGE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ddl_error_message_aux_lob_meta_schema,
    ObInnerTableSchema::all_ddl_error_message_aux_lob_piece_schema
  },

  {
    OB_ALL_SPACE_USAGE_TID,
    OB_ALL_SPACE_USAGE_AUX_LOB_META_TID,
    OB_ALL_SPACE_USAGE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_space_usage_aux_lob_meta_schema,
    ObInnerTableSchema::all_space_usage_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_SET_FILES_TID,
    OB_ALL_BACKUP_SET_FILES_AUX_LOB_META_TID,
    OB_ALL_BACKUP_SET_FILES_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_set_files_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_set_files_aux_lob_piece_schema
  },

  {
    OB_ALL_RES_MGR_CONSUMER_GROUP_TID,
    OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_META_TID,
    OB_ALL_RES_MGR_CONSUMER_GROUP_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_res_mgr_consumer_group_aux_lob_meta_schema,
    ObInnerTableSchema::all_res_mgr_consumer_group_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_INFO_TID,
    OB_ALL_BACKUP_INFO_AUX_LOB_META_TID,
    OB_ALL_BACKUP_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_info_aux_lob_piece_schema
  },

  {
    OB_ALL_DDL_TASK_STATUS_TID,
    OB_ALL_DDL_TASK_STATUS_AUX_LOB_META_TID,
    OB_ALL_DDL_TASK_STATUS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ddl_task_status_aux_lob_meta_schema,
    ObInnerTableSchema::all_ddl_task_status_aux_lob_piece_schema
  },

  {
    OB_ALL_REGION_NETWORK_BANDWIDTH_LIMIT_TID,
    OB_ALL_REGION_NETWORK_BANDWIDTH_LIMIT_AUX_LOB_META_TID,
    OB_ALL_REGION_NETWORK_BANDWIDTH_LIMIT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_region_network_bandwidth_limit_aux_lob_meta_schema,
    ObInnerTableSchema::all_region_network_bandwidth_limit_aux_lob_piece_schema
  },

  {
    OB_ALL_DEADLOCK_EVENT_HISTORY_TID,
    OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_DEADLOCK_EVENT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_deadlock_event_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_deadlock_event_history_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_USAGE_TID,
    OB_ALL_COLUMN_USAGE_AUX_LOB_META_TID,
    OB_ALL_COLUMN_USAGE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_usage_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_usage_aux_lob_piece_schema
  },

  {
    OB_ALL_JOB_TID,
    OB_ALL_JOB_AUX_LOB_META_TID,
    OB_ALL_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_job_aux_lob_piece_schema
  },

  {
    OB_ALL_JOB_LOG_TID,
    OB_ALL_JOB_LOG_AUX_LOB_META_TID,
    OB_ALL_JOB_LOG_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_job_log_aux_lob_meta_schema,
    ObInnerTableSchema::all_job_log_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_DIRECTORY_TID,
    OB_ALL_TENANT_DIRECTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_DIRECTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_directory_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_directory_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_DIRECTORY_HISTORY_TID,
    OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_DIRECTORY_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_directory_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_directory_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLE_STAT_TID,
    OB_ALL_TABLE_STAT_AUX_LOB_META_TID,
    OB_ALL_TABLE_STAT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_table_stat_aux_lob_meta_schema,
    ObInnerTableSchema::all_table_stat_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_STAT_TID,
    OB_ALL_COLUMN_STAT_AUX_LOB_META_TID,
    OB_ALL_COLUMN_STAT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_stat_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_stat_aux_lob_piece_schema
  },

  {
    OB_ALL_HISTOGRAM_STAT_TID,
    OB_ALL_HISTOGRAM_STAT_AUX_LOB_META_TID,
    OB_ALL_HISTOGRAM_STAT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_histogram_stat_aux_lob_meta_schema,
    ObInnerTableSchema::all_histogram_stat_aux_lob_piece_schema
  },

  {
    OB_ALL_MONITOR_MODIFIED_TID,
    OB_ALL_MONITOR_MODIFIED_AUX_LOB_META_TID,
    OB_ALL_MONITOR_MODIFIED_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_monitor_modified_aux_lob_meta_schema,
    ObInnerTableSchema::all_monitor_modified_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLE_STAT_HISTORY_TID,
    OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TABLE_STAT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_table_stat_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_table_stat_history_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_STAT_HISTORY_TID,
    OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_COLUMN_STAT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_stat_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_stat_history_aux_lob_piece_schema
  },

  {
    OB_ALL_HISTOGRAM_STAT_HISTORY_TID,
    OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_HISTOGRAM_STAT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_histogram_stat_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_histogram_stat_history_aux_lob_piece_schema
  },

  {
    OB_ALL_OPTSTAT_GLOBAL_PREFS_TID,
    OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_META_TID,
    OB_ALL_OPTSTAT_GLOBAL_PREFS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_optstat_global_prefs_aux_lob_meta_schema,
    ObInnerTableSchema::all_optstat_global_prefs_aux_lob_piece_schema
  },

  {
    OB_ALL_OPTSTAT_USER_PREFS_TID,
    OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_META_TID,
    OB_ALL_OPTSTAT_USER_PREFS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_optstat_user_prefs_aux_lob_meta_schema,
    ObInnerTableSchema::all_optstat_user_prefs_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_META_TABLE_TID,
    OB_ALL_LS_META_TABLE_AUX_LOB_META_TID,
    OB_ALL_LS_META_TABLE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_meta_table_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_meta_table_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLET_TO_LS_TID,
    OB_ALL_TABLET_TO_LS_AUX_LOB_META_TID,
    OB_ALL_TABLET_TO_LS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tablet_to_ls_aux_lob_meta_schema,
    ObInnerTableSchema::all_tablet_to_ls_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLET_META_TABLE_TID,
    OB_ALL_TABLET_META_TABLE_AUX_LOB_META_TID,
    OB_ALL_TABLET_META_TABLE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tablet_meta_table_aux_lob_meta_schema,
    ObInnerTableSchema::all_tablet_meta_table_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_STATUS_TID,
    OB_ALL_LS_STATUS_AUX_LOB_META_TID,
    OB_ALL_LS_STATUS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_status_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_status_aux_lob_piece_schema
  },

  {
    OB_ALL_LOG_ARCHIVE_PROGRESS_TID,
    OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TID,
    OB_ALL_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_log_archive_progress_aux_lob_meta_schema,
    ObInnerTableSchema::all_log_archive_progress_aux_lob_piece_schema
  },

  {
    OB_ALL_LOG_ARCHIVE_HISTORY_TID,
    OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_LOG_ARCHIVE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_log_archive_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_log_archive_history_aux_lob_piece_schema
  },

  {
    OB_ALL_LOG_ARCHIVE_PIECE_FILES_TID,
    OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_META_TID,
    OB_ALL_LOG_ARCHIVE_PIECE_FILES_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_log_archive_piece_files_aux_lob_meta_schema,
    ObInnerTableSchema::all_log_archive_piece_files_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TID,
    OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_META_TID,
    OB_ALL_LS_LOG_ARCHIVE_PROGRESS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_log_archive_progress_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_log_archive_progress_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_TID,
    OB_ALL_LS_AUX_LOB_META_TID,
    OB_ALL_LS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_STORAGE_INFO_TID,
    OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_META_TID,
    OB_ALL_BACKUP_STORAGE_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_storage_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_storage_info_aux_lob_piece_schema
  },

  {
    OB_ALL_DAM_LAST_ARCH_TS_TID,
    OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_META_TID,
    OB_ALL_DAM_LAST_ARCH_TS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_dam_last_arch_ts_aux_lob_meta_schema,
    ObInnerTableSchema::all_dam_last_arch_ts_aux_lob_piece_schema
  },

  {
    OB_ALL_DAM_CLEANUP_JOBS_TID,
    OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_META_TID,
    OB_ALL_DAM_CLEANUP_JOBS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_dam_cleanup_jobs_aux_lob_meta_schema,
    ObInnerTableSchema::all_dam_cleanup_jobs_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_JOB_TID,
    OB_ALL_BACKUP_JOB_AUX_LOB_META_TID,
    OB_ALL_BACKUP_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_job_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_JOB_HISTORY_TID,
    OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_JOB_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_job_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_job_history_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_TASK_TID,
    OB_ALL_BACKUP_TASK_AUX_LOB_META_TID,
    OB_ALL_BACKUP_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_task_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_TASK_HISTORY_TID,
    OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_LS_TASK_TID,
    OB_ALL_BACKUP_LS_TASK_AUX_LOB_META_TID,
    OB_ALL_BACKUP_LS_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_ls_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_ls_task_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_LS_TASK_HISTORY_TID,
    OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_LS_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_ls_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_ls_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_LS_TASK_INFO_TID,
    OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_META_TID,
    OB_ALL_BACKUP_LS_TASK_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_ls_task_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_ls_task_info_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_SKIPPED_TABLET_TID,
    OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_META_TID,
    OB_ALL_BACKUP_SKIPPED_TABLET_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_skipped_tablet_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_skipped_tablet_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_TID,
    OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_skipped_tablet_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_skipped_tablet_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_INFO_TID,
    OB_ALL_TENANT_INFO_AUX_LOB_META_TID,
    OB_ALL_TENANT_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_info_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLET_TO_TABLE_HISTORY_TID,
    OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TABLET_TO_TABLE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tablet_to_table_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tablet_to_table_history_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_RECOVERY_STAT_TID,
    OB_ALL_LS_RECOVERY_STAT_AUX_LOB_META_TID,
    OB_ALL_LS_RECOVERY_STAT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_recovery_stat_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_recovery_stat_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_TID,
    OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_LS_TASK_INFO_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_ls_task_info_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_ls_task_info_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLET_REPLICA_CHECKSUM_TID,
    OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_META_TID,
    OB_ALL_TABLET_REPLICA_CHECKSUM_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tablet_replica_checksum_aux_lob_meta_schema,
    ObInnerTableSchema::all_tablet_replica_checksum_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLET_CHECKSUM_TID,
    OB_ALL_TABLET_CHECKSUM_AUX_LOB_META_TID,
    OB_ALL_TABLET_CHECKSUM_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tablet_checksum_aux_lob_meta_schema,
    ObInnerTableSchema::all_tablet_checksum_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_REPLICA_TASK_TID,
    OB_ALL_LS_REPLICA_TASK_AUX_LOB_META_TID,
    OB_ALL_LS_REPLICA_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_replica_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_replica_task_aux_lob_piece_schema
  },

  {
    OB_ALL_PENDING_TRANSACTION_TID,
    OB_ALL_PENDING_TRANSACTION_AUX_LOB_META_TID,
    OB_ALL_PENDING_TRANSACTION_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_pending_transaction_aux_lob_meta_schema,
    ObInnerTableSchema::all_pending_transaction_aux_lob_piece_schema
  },

  {
    OB_ALL_BALANCE_GROUP_LS_STAT_TID,
    OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_META_TID,
    OB_ALL_BALANCE_GROUP_LS_STAT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_balance_group_ls_stat_aux_lob_meta_schema,
    ObInnerTableSchema::all_balance_group_ls_stat_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SCHEDULER_JOB_TID,
    OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_META_TID,
    OB_ALL_TENANT_SCHEDULER_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_scheduler_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_scheduler_job_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_TID,
    OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_META_TID,
    OB_ALL_TENANT_SCHEDULER_JOB_RUN_DETAIL_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_scheduler_job_run_detail_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_scheduler_job_run_detail_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SCHEDULER_PROGRAM_TID,
    OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_META_TID,
    OB_ALL_TENANT_SCHEDULER_PROGRAM_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_scheduler_program_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_scheduler_program_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_TID,
    OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_META_TID,
    OB_ALL_TENANT_SCHEDULER_PROGRAM_ARGUMENT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_scheduler_program_argument_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_scheduler_program_argument_aux_lob_piece_schema
  },

  {
    OB_ALL_CONTEXT_TID,
    OB_ALL_CONTEXT_AUX_LOB_META_TID,
    OB_ALL_CONTEXT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_context_aux_lob_meta_schema,
    ObInnerTableSchema::all_context_aux_lob_piece_schema
  },

  {
    OB_ALL_CONTEXT_HISTORY_TID,
    OB_ALL_CONTEXT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_CONTEXT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_context_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_context_history_aux_lob_piece_schema
  },

  {
    OB_ALL_GLOBAL_CONTEXT_VALUE_TID,
    OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_META_TID,
    OB_ALL_GLOBAL_CONTEXT_VALUE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_global_context_value_aux_lob_meta_schema,
    ObInnerTableSchema::all_global_context_value_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_ELECTION_REFERENCE_INFO_TID,
    OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_META_TID,
    OB_ALL_LS_ELECTION_REFERENCE_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_election_reference_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_election_reference_info_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_DELETE_JOB_TID,
    OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_META_TID,
    OB_ALL_BACKUP_DELETE_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_delete_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_delete_job_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_DELETE_JOB_HISTORY_TID,
    OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_DELETE_JOB_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_delete_job_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_delete_job_history_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_DELETE_TASK_TID,
    OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_META_TID,
    OB_ALL_BACKUP_DELETE_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_delete_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_delete_task_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_DELETE_TASK_HISTORY_TID,
    OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_DELETE_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_delete_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_delete_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_DELETE_LS_TASK_TID,
    OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_META_TID,
    OB_ALL_BACKUP_DELETE_LS_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_delete_ls_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_delete_ls_task_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_TID,
    OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_DELETE_LS_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_delete_ls_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_delete_ls_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_ZONE_MERGE_INFO_TID,
    OB_ALL_ZONE_MERGE_INFO_AUX_LOB_META_TID,
    OB_ALL_ZONE_MERGE_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_zone_merge_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_zone_merge_info_aux_lob_piece_schema
  },

  {
    OB_ALL_MERGE_INFO_TID,
    OB_ALL_MERGE_INFO_AUX_LOB_META_TID,
    OB_ALL_MERGE_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_merge_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_merge_info_aux_lob_piece_schema
  },

  {
    OB_ALL_FREEZE_INFO_TID,
    OB_ALL_FREEZE_INFO_AUX_LOB_META_TID,
    OB_ALL_FREEZE_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_freeze_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_freeze_info_aux_lob_piece_schema
  },

  {
    OB_ALL_DISK_IO_CALIBRATION_TID,
    OB_ALL_DISK_IO_CALIBRATION_AUX_LOB_META_TID,
    OB_ALL_DISK_IO_CALIBRATION_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_disk_io_calibration_aux_lob_meta_schema,
    ObInnerTableSchema::all_disk_io_calibration_aux_lob_piece_schema
  },

  {
    OB_ALL_PLAN_BASELINE_TID,
    OB_ALL_PLAN_BASELINE_AUX_LOB_META_TID,
    OB_ALL_PLAN_BASELINE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_plan_baseline_aux_lob_meta_schema,
    ObInnerTableSchema::all_plan_baseline_aux_lob_piece_schema
  },

  {
    OB_ALL_PLAN_BASELINE_ITEM_TID,
    OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_META_TID,
    OB_ALL_PLAN_BASELINE_ITEM_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_plan_baseline_item_aux_lob_meta_schema,
    ObInnerTableSchema::all_plan_baseline_item_aux_lob_piece_schema
  },

  {
    OB_ALL_SPM_CONFIG_TID,
    OB_ALL_SPM_CONFIG_AUX_LOB_META_TID,
    OB_ALL_SPM_CONFIG_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_spm_config_aux_lob_meta_schema,
    ObInnerTableSchema::all_spm_config_aux_lob_piece_schema
  },

  {
    OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TID,
    OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_META_TID,
    OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_log_archive_dest_parameter_aux_lob_meta_schema,
    ObInnerTableSchema::all_log_archive_dest_parameter_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_PARAMETER_TID,
    OB_ALL_BACKUP_PARAMETER_AUX_LOB_META_TID,
    OB_ALL_BACKUP_PARAMETER_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_parameter_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_parameter_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_RESTORE_PROGRESS_TID,
    OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_META_TID,
    OB_ALL_LS_RESTORE_PROGRESS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_restore_progress_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_restore_progress_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_RESTORE_HISTORY_TID,
    OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_LS_RESTORE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_restore_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_restore_history_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_STORAGE_INFO_HISTORY_TID,
    OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_STORAGE_INFO_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_storage_info_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_storage_info_history_aux_lob_piece_schema
  },

  {
    OB_ALL_BACKUP_DELETE_POLICY_TID,
    OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_META_TID,
    OB_ALL_BACKUP_DELETE_POLICY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_backup_delete_policy_aux_lob_meta_schema,
    ObInnerTableSchema::all_backup_delete_policy_aux_lob_piece_schema
  },

  {
    OB_ALL_MOCK_FK_PARENT_TABLE_TID,
    OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_META_TID,
    OB_ALL_MOCK_FK_PARENT_TABLE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mock_fk_parent_table_aux_lob_meta_schema,
    ObInnerTableSchema::all_mock_fk_parent_table_aux_lob_piece_schema
  },

  {
    OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_TID,
    OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_MOCK_FK_PARENT_TABLE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mock_fk_parent_table_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_mock_fk_parent_table_history_aux_lob_piece_schema
  },

  {
    OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_TID,
    OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_META_TID,
    OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mock_fk_parent_table_column_aux_lob_meta_schema,
    ObInnerTableSchema::all_mock_fk_parent_table_column_aux_lob_piece_schema
  },

  {
    OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_TID,
    OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_META_TID,
    OB_ALL_MOCK_FK_PARENT_TABLE_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mock_fk_parent_table_column_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_mock_fk_parent_table_column_history_aux_lob_piece_schema
  },

  {
    OB_ALL_LOG_RESTORE_SOURCE_TID,
    OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_META_TID,
    OB_ALL_LOG_RESTORE_SOURCE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_log_restore_source_aux_lob_meta_schema,
    ObInnerTableSchema::all_log_restore_source_aux_lob_piece_schema
  },

  {
    OB_ALL_KV_TTL_TASK_TID,
    OB_ALL_KV_TTL_TASK_AUX_LOB_META_TID,
    OB_ALL_KV_TTL_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_kv_ttl_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_kv_ttl_task_aux_lob_piece_schema
  },

  {
    OB_ALL_KV_TTL_TASK_HISTORY_TID,
    OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_KV_TTL_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_kv_ttl_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_kv_ttl_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_SERVICE_EPOCH_TID,
    OB_ALL_SERVICE_EPOCH_AUX_LOB_META_TID,
    OB_ALL_SERVICE_EPOCH_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_service_epoch_aux_lob_meta_schema,
    ObInnerTableSchema::all_service_epoch_aux_lob_piece_schema
  },

  {
    OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TID,
    OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_META_TID,
    OB_ALL_SPATIAL_REFERENCE_SYSTEMS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_spatial_reference_systems_aux_lob_meta_schema,
    ObInnerTableSchema::all_spatial_reference_systems_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TID,
    OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_META_TID,
    OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_checksum_error_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_checksum_error_info_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_GROUP_TID,
    OB_ALL_COLUMN_GROUP_AUX_LOB_META_TID,
    OB_ALL_COLUMN_GROUP_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_group_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_group_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_GROUP_HISTORY_TID,
    OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_META_TID,
    OB_ALL_COLUMN_GROUP_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_group_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_group_history_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_GROUP_MAPPING_TID,
    OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_META_TID,
    OB_ALL_COLUMN_GROUP_MAPPING_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_group_mapping_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_group_mapping_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TID,
    OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_META_TID,
    OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_group_mapping_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_group_mapping_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TRANSFER_TASK_TID,
    OB_ALL_TRANSFER_TASK_AUX_LOB_META_TID,
    OB_ALL_TRANSFER_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_transfer_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_transfer_task_aux_lob_piece_schema
  },

  {
    OB_ALL_TRANSFER_TASK_HISTORY_TID,
    OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TRANSFER_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_transfer_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_transfer_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_BALANCE_JOB_TID,
    OB_ALL_BALANCE_JOB_AUX_LOB_META_TID,
    OB_ALL_BALANCE_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_balance_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_balance_job_aux_lob_piece_schema
  },

  {
    OB_ALL_BALANCE_JOB_HISTORY_TID,
    OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BALANCE_JOB_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_balance_job_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_balance_job_history_aux_lob_piece_schema
  },

  {
    OB_ALL_BALANCE_TASK_TID,
    OB_ALL_BALANCE_TASK_AUX_LOB_META_TID,
    OB_ALL_BALANCE_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_balance_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_balance_task_aux_lob_piece_schema
  },

  {
    OB_ALL_BALANCE_TASK_HISTORY_TID,
    OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_BALANCE_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_balance_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_balance_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_ARBITRATION_SERVICE_TID,
    OB_ALL_ARBITRATION_SERVICE_AUX_LOB_META_TID,
    OB_ALL_ARBITRATION_SERVICE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_arbitration_service_aux_lob_meta_schema,
    ObInnerTableSchema::all_arbitration_service_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_ARB_REPLICA_TASK_TID,
    OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_META_TID,
    OB_ALL_LS_ARB_REPLICA_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_arb_replica_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_arb_replica_task_aux_lob_piece_schema
  },

  {
    OB_ALL_DATA_DICTIONARY_IN_LOG_TID,
    OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_META_TID,
    OB_ALL_DATA_DICTIONARY_IN_LOG_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_data_dictionary_in_log_aux_lob_meta_schema,
    ObInnerTableSchema::all_data_dictionary_in_log_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_TID,
    OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_arb_replica_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_arb_replica_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_POLICY_TID,
    OB_ALL_RLS_POLICY_AUX_LOB_META_TID,
    OB_ALL_RLS_POLICY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_policy_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_policy_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_POLICY_HISTORY_TID,
    OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_META_TID,
    OB_ALL_RLS_POLICY_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_policy_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_policy_history_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_SECURITY_COLUMN_TID,
    OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_META_TID,
    OB_ALL_RLS_SECURITY_COLUMN_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_security_column_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_security_column_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_SECURITY_COLUMN_HISTORY_TID,
    OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_META_TID,
    OB_ALL_RLS_SECURITY_COLUMN_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_security_column_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_security_column_history_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_GROUP_TID,
    OB_ALL_RLS_GROUP_AUX_LOB_META_TID,
    OB_ALL_RLS_GROUP_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_group_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_group_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_GROUP_HISTORY_TID,
    OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_META_TID,
    OB_ALL_RLS_GROUP_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_group_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_group_history_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_CONTEXT_TID,
    OB_ALL_RLS_CONTEXT_AUX_LOB_META_TID,
    OB_ALL_RLS_CONTEXT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_context_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_context_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_CONTEXT_HISTORY_TID,
    OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_RLS_CONTEXT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_context_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_context_history_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_ATTRIBUTE_TID,
    OB_ALL_RLS_ATTRIBUTE_AUX_LOB_META_TID,
    OB_ALL_RLS_ATTRIBUTE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_attribute_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_attribute_aux_lob_piece_schema
  },

  {
    OB_ALL_RLS_ATTRIBUTE_HISTORY_TID,
    OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_RLS_ATTRIBUTE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_rls_attribute_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_rls_attribute_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_REWRITE_RULES_TID,
    OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_META_TID,
    OB_ALL_TENANT_REWRITE_RULES_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_rewrite_rules_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_rewrite_rules_aux_lob_piece_schema
  },

  {
    OB_ALL_RESERVED_SNAPSHOT_TID,
    OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_META_TID,
    OB_ALL_RESERVED_SNAPSHOT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_reserved_snapshot_aux_lob_meta_schema,
    ObInnerTableSchema::all_reserved_snapshot_aux_lob_piece_schema
  },

  {
    OB_ALL_CLUSTER_EVENT_HISTORY_TID,
    OB_ALL_CLUSTER_EVENT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_CLUSTER_EVENT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_cluster_event_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_cluster_event_history_aux_lob_piece_schema
  },

  {
    OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TID,
    OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_META_TID,
    OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ls_transfer_member_list_lock_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_ls_transfer_member_list_lock_info_aux_lob_piece_schema
  },

  {
    OB_ALL_EXTERNAL_TABLE_FILE_TID,
    OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_META_TID,
    OB_ALL_EXTERNAL_TABLE_FILE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_external_table_file_aux_lob_meta_schema,
    ObInnerTableSchema::all_external_table_file_aux_lob_piece_schema
  },

  {
    OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_TID,
    OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TASK_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_task_opt_stat_gather_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_task_opt_stat_gather_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_TID,
    OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TABLE_OPT_STAT_GATHER_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_table_opt_stat_gather_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_table_opt_stat_gather_history_aux_lob_piece_schema
  },

  {
    OB_WR_ACTIVE_SESSION_HISTORY_TID,
    OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_META_TID,
    OB_WR_ACTIVE_SESSION_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::wr_active_session_history_aux_lob_meta_schema,
    ObInnerTableSchema::wr_active_session_history_aux_lob_piece_schema
  },

  {
    OB_WR_SNAPSHOT_TID,
    OB_WR_SNAPSHOT_AUX_LOB_META_TID,
    OB_WR_SNAPSHOT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::wr_snapshot_aux_lob_meta_schema,
    ObInnerTableSchema::wr_snapshot_aux_lob_piece_schema
  },

  {
    OB_WR_STATNAME_TID,
    OB_WR_STATNAME_AUX_LOB_META_TID,
    OB_WR_STATNAME_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::wr_statname_aux_lob_meta_schema,
    ObInnerTableSchema::wr_statname_aux_lob_piece_schema
  },

  {
    OB_WR_SYSSTAT_TID,
    OB_WR_SYSSTAT_AUX_LOB_META_TID,
    OB_WR_SYSSTAT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::wr_sysstat_aux_lob_meta_schema,
    ObInnerTableSchema::wr_sysstat_aux_lob_piece_schema
  },

  {
    OB_ALL_BALANCE_TASK_HELPER_TID,
    OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_META_TID,
    OB_ALL_BALANCE_TASK_HELPER_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_balance_task_helper_aux_lob_meta_schema,
    ObInnerTableSchema::all_balance_task_helper_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SNAPSHOT_TID,
    OB_ALL_TENANT_SNAPSHOT_AUX_LOB_META_TID,
    OB_ALL_TENANT_SNAPSHOT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_snapshot_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_snapshot_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SNAPSHOT_LS_TID,
    OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_META_TID,
    OB_ALL_TENANT_SNAPSHOT_LS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_snapshot_ls_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_snapshot_ls_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_TID,
    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_META_TID,
    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_snapshot_ls_replica_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_snapshot_ls_replica_aux_lob_piece_schema
  },

  {
    OB_ALL_MLOG_TID,
    OB_ALL_MLOG_AUX_LOB_META_TID,
    OB_ALL_MLOG_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mlog_aux_lob_meta_schema,
    ObInnerTableSchema::all_mlog_aux_lob_piece_schema
  },

  {
    OB_ALL_MVIEW_TID,
    OB_ALL_MVIEW_AUX_LOB_META_TID,
    OB_ALL_MVIEW_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mview_aux_lob_meta_schema,
    ObInnerTableSchema::all_mview_aux_lob_piece_schema
  },

  {
    OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_TID,
    OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_META_TID,
    OB_ALL_MVIEW_REFRESH_STATS_SYS_DEFAULTS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mview_refresh_stats_sys_defaults_aux_lob_meta_schema,
    ObInnerTableSchema::all_mview_refresh_stats_sys_defaults_aux_lob_piece_schema
  },

  {
    OB_ALL_MVIEW_REFRESH_STATS_PARAMS_TID,
    OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_META_TID,
    OB_ALL_MVIEW_REFRESH_STATS_PARAMS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mview_refresh_stats_params_aux_lob_meta_schema,
    ObInnerTableSchema::all_mview_refresh_stats_params_aux_lob_piece_schema
  },

  {
    OB_ALL_MVIEW_REFRESH_RUN_STATS_TID,
    OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_META_TID,
    OB_ALL_MVIEW_REFRESH_RUN_STATS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mview_refresh_run_stats_aux_lob_meta_schema,
    ObInnerTableSchema::all_mview_refresh_run_stats_aux_lob_piece_schema
  },

  {
    OB_ALL_MVIEW_REFRESH_STATS_TID,
    OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_META_TID,
    OB_ALL_MVIEW_REFRESH_STATS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mview_refresh_stats_aux_lob_meta_schema,
    ObInnerTableSchema::all_mview_refresh_stats_aux_lob_piece_schema
  },

  {
    OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TID,
    OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_META_TID,
    OB_ALL_MVIEW_REFRESH_CHANGE_STATS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mview_refresh_change_stats_aux_lob_meta_schema,
    ObInnerTableSchema::all_mview_refresh_change_stats_aux_lob_piece_schema
  },

  {
    OB_ALL_MVIEW_REFRESH_STMT_STATS_TID,
    OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_META_TID,
    OB_ALL_MVIEW_REFRESH_STMT_STATS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mview_refresh_stmt_stats_aux_lob_meta_schema,
    ObInnerTableSchema::all_mview_refresh_stmt_stats_aux_lob_piece_schema
  },

  {
    OB_ALL_DBMS_LOCK_ALLOCATED_TID,
    OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_META_TID,
    OB_ALL_DBMS_LOCK_ALLOCATED_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_dbms_lock_allocated_aux_lob_meta_schema,
    ObInnerTableSchema::all_dbms_lock_allocated_aux_lob_piece_schema
  },

  {
    OB_WR_CONTROL_TID,
    OB_WR_CONTROL_AUX_LOB_META_TID,
    OB_WR_CONTROL_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::wr_control_aux_lob_meta_schema,
    ObInnerTableSchema::wr_control_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_EVENT_HISTORY_TID,
    OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_EVENT_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_event_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_event_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TID,
    OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_META_TID,
    OB_ALL_TENANT_SCHEDULER_JOB_CLASS_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_scheduler_job_class_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_scheduler_job_class_aux_lob_piece_schema
  },

  {
    OB_ALL_RECOVER_TABLE_JOB_TID,
    OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_META_TID,
    OB_ALL_RECOVER_TABLE_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_recover_table_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_recover_table_job_aux_lob_piece_schema
  },

  {
    OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID,
    OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_META_TID,
    OB_ALL_RECOVER_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_recover_table_job_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_recover_table_job_history_aux_lob_piece_schema
  },

  {
    OB_ALL_IMPORT_TABLE_JOB_TID,
    OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_META_TID,
    OB_ALL_IMPORT_TABLE_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_import_table_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_import_table_job_aux_lob_piece_schema
  },

  {
    OB_ALL_IMPORT_TABLE_JOB_HISTORY_TID,
    OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_META_TID,
    OB_ALL_IMPORT_TABLE_JOB_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_import_table_job_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_import_table_job_history_aux_lob_piece_schema
  },

  {
    OB_ALL_IMPORT_TABLE_TASK_TID,
    OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_META_TID,
    OB_ALL_IMPORT_TABLE_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_import_table_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_import_table_task_aux_lob_piece_schema
  },

  {
    OB_ALL_IMPORT_TABLE_TASK_HISTORY_TID,
    OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_IMPORT_TABLE_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_import_table_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_import_table_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_TID,
    OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_STORAGE_HA_ERROR_DIAGNOSE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_storage_ha_error_diagnose_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_storage_ha_error_diagnose_history_aux_lob_piece_schema
  },

  {
    OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_TID,
    OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_STORAGE_HA_PERF_DIAGNOSE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_storage_ha_perf_diagnose_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_storage_ha_perf_diagnose_history_aux_lob_piece_schema
  },

  {
    OB_ALL_CLONE_JOB_TID,
    OB_ALL_CLONE_JOB_AUX_LOB_META_TID,
    OB_ALL_CLONE_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_clone_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_clone_job_aux_lob_piece_schema
  },

  {
    OB_ALL_CLONE_JOB_HISTORY_TID,
    OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_META_TID,
    OB_ALL_CLONE_JOB_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_clone_job_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_clone_job_history_aux_lob_piece_schema
  },

  {
    OB_ALL_ROUTINE_PRIVILEGE_TID,
    OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_META_TID,
    OB_ALL_ROUTINE_PRIVILEGE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_routine_privilege_aux_lob_meta_schema,
    ObInnerTableSchema::all_routine_privilege_aux_lob_piece_schema
  },

  {
    OB_ALL_ROUTINE_PRIVILEGE_HISTORY_TID,
    OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_ROUTINE_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_routine_privilege_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_routine_privilege_history_aux_lob_piece_schema
  },

  {
    OB_ALL_NCOMP_DLL_TID,
    OB_ALL_NCOMP_DLL_AUX_LOB_META_TID,
    OB_ALL_NCOMP_DLL_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_ncomp_dll_aux_lob_meta_schema,
    ObInnerTableSchema::all_ncomp_dll_aux_lob_piece_schema
  },

  {
    OB_ALL_AUX_STAT_TID,
    OB_ALL_AUX_STAT_AUX_LOB_META_TID,
    OB_ALL_AUX_STAT_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_aux_stat_aux_lob_meta_schema,
    ObInnerTableSchema::all_aux_stat_aux_lob_piece_schema
  },

  {
    OB_ALL_INDEX_USAGE_INFO_TID,
    OB_ALL_INDEX_USAGE_INFO_AUX_LOB_META_TID,
    OB_ALL_INDEX_USAGE_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_index_usage_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_index_usage_info_aux_lob_piece_schema
  },

  {
    OB_ALL_DETECT_LOCK_INFO_TID,
    OB_ALL_DETECT_LOCK_INFO_AUX_LOB_META_TID,
    OB_ALL_DETECT_LOCK_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_detect_lock_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_detect_lock_info_aux_lob_piece_schema
  },

  {
    OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TID,
    OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_META_TID,
    OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_client_to_server_session_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_client_to_server_session_info_aux_lob_piece_schema
  },

  {
    OB_ALL_TRANSFER_PARTITION_TASK_TID,
    OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_META_TID,
    OB_ALL_TRANSFER_PARTITION_TASK_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_transfer_partition_task_aux_lob_meta_schema,
    ObInnerTableSchema::all_transfer_partition_task_aux_lob_piece_schema
  },

  {
    OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_TID,
    OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TRANSFER_PARTITION_TASK_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_transfer_partition_task_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_transfer_partition_task_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SNAPSHOT_JOB_TID,
    OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_META_TID,
    OB_ALL_TENANT_SNAPSHOT_JOB_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_snapshot_job_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_snapshot_job_aux_lob_piece_schema
  },

  {
    OB_ALL_TRUSTED_ROOT_CERTIFICATE_TID,
    OB_ALL_TRUSTED_ROOT_CERTIFICATE_AUX_LOB_META_TID,
    OB_ALL_TRUSTED_ROOT_CERTIFICATE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_trusted_root_certificate_aux_lob_meta_schema,
    ObInnerTableSchema::all_trusted_root_certificate_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_PRIVILEGE_TID,
    OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_META_TID,
    OB_ALL_COLUMN_PRIVILEGE_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_privilege_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_privilege_aux_lob_piece_schema
  },

  {
    OB_ALL_COLUMN_PRIVILEGE_HISTORY_TID,
    OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_META_TID,
    OB_ALL_COLUMN_PRIVILEGE_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_column_privilege_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_column_privilege_history_aux_lob_piece_schema
  },

  {
    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_TID,
    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_META_TID,
    OB_ALL_TENANT_SNAPSHOT_LS_REPLICA_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_tenant_snapshot_ls_replica_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_tenant_snapshot_ls_replica_history_aux_lob_piece_schema
  },

  {
    OB_ALL_USER_PROXY_INFO_TID,
    OB_ALL_USER_PROXY_INFO_AUX_LOB_META_TID,
    OB_ALL_USER_PROXY_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_user_proxy_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_user_proxy_info_aux_lob_piece_schema
  },

  {
    OB_ALL_USER_PROXY_INFO_HISTORY_TID,
    OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_META_TID,
    OB_ALL_USER_PROXY_INFO_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_user_proxy_info_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_user_proxy_info_history_aux_lob_piece_schema
  },

  {
    OB_ALL_USER_PROXY_ROLE_INFO_TID,
    OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_META_TID,
    OB_ALL_USER_PROXY_ROLE_INFO_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_user_proxy_role_info_aux_lob_meta_schema,
    ObInnerTableSchema::all_user_proxy_role_info_aux_lob_piece_schema
  },

  {
    OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_TID,
    OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_META_TID,
    OB_ALL_USER_PROXY_ROLE_INFO_HISTORY_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_user_proxy_role_info_history_aux_lob_meta_schema,
    ObInnerTableSchema::all_user_proxy_role_info_history_aux_lob_piece_schema
  },

  {
    OB_ALL_MVIEW_DEP_TID,
    OB_ALL_MVIEW_DEP_AUX_LOB_META_TID,
    OB_ALL_MVIEW_DEP_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_mview_dep_aux_lob_meta_schema,
    ObInnerTableSchema::all_mview_dep_aux_lob_piece_schema
  },

  {
    OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_TID,
    OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_META_TID,
    OB_ALL_SCHEDULER_JOB_RUN_DETAIL_V2_AUX_LOB_PIECE_TID,
    ObInnerTableSchema::all_scheduler_job_run_detail_v2_aux_lob_meta_schema,
    ObInnerTableSchema::all_scheduler_job_run_detail_v2_aux_lob_piece_schema
  },

};

static inline bool get_sys_table_lob_aux_table_id(const uint64_t tid, uint64_t& meta_tid, uint64_t& piece_tid)
{
  bool bret = false;
  meta_tid = OB_INVALID_ID;
  piece_tid = OB_INVALID_ID;
  if (OB_ALL_CORE_TABLE_TID == tid) {
    // __all_core_table do not need lob aux table, return false
  } else if (is_system_table(tid)) {
    bret = true;
    meta_tid = tid + OB_MIN_SYS_LOB_META_TABLE_ID;
    piece_tid = tid + OB_MIN_SYS_LOB_PIECE_TABLE_ID;
  }
  return bret;
}

typedef common::hash::ObHashMap<uint64_t, LOBMapping> inner_lob_map_t;
extern inner_lob_map_t inner_lob_map;
extern bool inited_lob;
static inline int get_sys_table_lob_aux_schema(const uint64_t tid,
                                               share::schema::ObTableSchema& meta_schema,
                                               share::schema::ObTableSchema& piece_schema)
{
  int ret = OB_SUCCESS;
  LOBMapping item;
  if (OB_FAIL(inner_lob_map.get_refactored(tid, item))) {
    SERVER_LOG(WARN, "fail to get lob mapping item", K(ret), K(tid), K(inited_lob));
  } else if (OB_FAIL(item.lob_meta_func_(meta_schema))) {
    SERVER_LOG(WARN, "fail to build lob meta schema", K(ret), K(tid));
  } else if (OB_FAIL(item.lob_piece_func_(piece_schema))) {
    SERVER_LOG(WARN, "fail to build lob piece schema", K(ret), K(tid));
  }
  return ret;
}

const int64_t OB_CORE_TABLE_COUNT = 4;
const int64_t OB_SYS_TABLE_COUNT = 296;
const int64_t OB_VIRTUAL_TABLE_COUNT = 821;
const int64_t OB_SYS_VIEW_COUNT = 901;
const int64_t OB_SYS_TENANT_TABLE_COUNT = 2023;
const int64_t OB_CORE_SCHEMA_VERSION = 1;
const int64_t OB_BOOTSTRAP_SCHEMA_VERSION = 2026;

} // end namespace share
} // end namespace oceanbase
#endif /* _OB_INNER_TABLE_SCHEMA_H_ */
