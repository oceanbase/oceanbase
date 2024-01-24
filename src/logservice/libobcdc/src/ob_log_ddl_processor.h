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
 *
 * DDL type task parser, work thread pool
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_DDL_PROCESSOR_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_DDL_PROCESSOR_H__

#ifndef OB_USE_DRCMSG
#include "ob_cdc_msg_convert.h"
#else
#include <drcmsg/BR.h>                            // IBinlogRecord
#endif
#include "lib/utility/ob_macro_utils.h"           // DISALLOW_COPY_AND_ASSIGN
#include "ob_log_utils.h"                         // _SEC_

namespace oceanbase
{
namespace libobcdc
{
///////////////////////////////// ObLogDDLProcessor /////////////////////////////////

class IObLogSchemaGetter;
class IStmtTask;
class PartTransTask;
class DdlStmtTask;
class ObLogSchemaGuard;
class IObLogTenantMgr;
class ObLogTenant;
class ObLogTenantGuard;
class TenantSchemaInfo;
class DBSchemaInfo;
class ObLogDDLProcessor
{
  enum
  {
    DATA_OP_TIMEOUT = 1 * _SEC_,
  };

public:
  ObLogDDLProcessor();
  virtual ~ObLogDDLProcessor();

public:
  int init(
      IObLogSchemaGetter *schema_getter,
      const bool skip_reversed_schema_version,
      const bool enable_white_black_list);
  void destroy();
  int handle_ddl_trans(
      PartTransTask &task,
      ObLogTenant &tenant,
      const bool need_update_tic,
      volatile bool &stop_flag);
private:
  void mark_stmt_binlog_record_invalid_(DdlStmtTask &stmt_task);
  void mark_all_binlog_records_invalid_(PartTransTask &task);

  // TODO schema
  int get_old_schema_version_(
      const uint64_t tenant_id,
      PartTransTask &task,
      const int64_t tenant_ddl_cur_schema_version,
      int64_t &old_schema_version,
      volatile bool &stop_flag);
  int get_schema_version_by_timestamp_util_succ_(
      const uint64_t tenant_id,
      const int64_t ddl_schema_version,
      int64_t &old_schema_version,
      volatile bool &stop_flag);
  // schema
  int filter_ddl_stmt_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      IObLogTenantMgr &tenant_mgr,
      bool &chosen,
      const bool is_filter_by_tenant_id = false);
  int handle_tenant_ddl_task_(
      PartTransTask &task,
      ObLogTenant &tenant,
      const bool need_update_tic,
      volatile bool &stop_flag);
  int handle_ddl_stmt_(
      ObLogTenant &tenant,
      PartTransTask &task,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema_version,
      const int64_t new_schema_version,
      volatile bool &stop_flag);
  int handle_ddl_stmt_update_tic_(
      ObLogTenant &tenant,
      PartTransTask &task,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema_version,
      const int64_t new_schema_version,
      volatile bool &stop_flag);
  int handle_ddl_stmt_direct_output_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t schema_version,
      volatile bool &stop_flag,
      const bool format_with_new_schema = false);
  int commit_ddl_stmt_(ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t schema_version,
      volatile bool &stop_flag,
      const char *tenant_name = NULL,
      const char *db_name = NULL,
      const bool format_with_new_schema = false,
      const bool filter_ddl_stmt = false);
  // TODO schema
  int get_schemas_for_ddl_stmt_(
      const uint64_t ddl_tenant_id,
      DdlStmtTask &ddl_stmt,
      const int64_t schema_version,
      const bool format_with_new_schema,
      const char *&tenant_name,
      const char *&db_name,
      volatile bool &stop_flag);
  int get_schema_info_with_online_schema_(
      const uint64_t ddl_tenant_id,
      const int64_t schema_version,
      DdlStmtTask &ddl_stmt,
      TenantSchemaInfo &tenant_schema_info,
      DBSchemaInfo &db_schema_info,
      volatile bool &stop_flag);
  int get_schema_info_with_data_dict_(
      const uint64_t ddl_tenant_id,
      const bool format_with_new_schema,
      DdlStmtTask &ddl_stmt,
      TenantSchemaInfo &tenant_schema_info,
      DBSchemaInfo &db_schema_info);
  int get_lazy_schema_guard_(
      const uint64_t tenant_id,
      const int64_t version,
      ObLogSchemaGuard &schema_guard,
      volatile bool &stop_flag);
  int decide_ddl_stmt_database_id_(
      const uint64_t tenant_id,
      DdlStmtTask &ddl_stmt,
      const int64_t schema_version,
      ObLogSchemaGuard &schema_guard,
      uint64_t &db_id,
      volatile bool &stop_flag);
  int decide_ddl_stmt_database_id_with_data_dict_(
      const uint64_t tenant_id,
      const bool use_new_schema,
      DdlStmtTask &ddl_stmt,
      uint64_t &db_id);
  bool is_use_new_schema_version_(
      DdlStmtTask &ddl_stmt,
      const int64_t schema_version);
  // schema

  int set_binlog_record_db_name_(IBinlogRecord &br,
      const int64_t ddl_operation_type,
      const char * const tenant_name,
      const char * const db_name);

  /*************** Table related DDL ***************/
  int handle_ddl_stmt_drop_table_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      const bool need_update_tic,
      volatile bool &stop_flag);
  int handle_ddl_stmt_drop_table_to_recyclebin_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      volatile bool &stop_flag);
  // alter table DDL
  // No longer maintain support for updating a table's db_id, to avoid db_id dependency
  // Currently two ways to modify table's db_id, move a table to another db
  // 1. alter table
  // 2. rename table
  int handle_ddl_stmt_alter_table_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      const bool need_update_tic,
      volatile bool &stop_flag);
  int handle_ddl_stmt_create_table_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema,
      const bool need_update_tic,
      volatile bool &stop_flag);
  int handle_ddl_stmt_rename_table_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      const bool need_update_tic,
      volatile bool &stop_flag);

  /*************** Index related DDL ***************/
  // Support global index, unique index
  int handle_ddl_stmt_create_index_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema,
      volatile bool &stop_flag);
  // Support global index, unique index
  int handle_ddl_stmt_drop_index_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      volatile bool &stop_flag);
  int handle_ddl_stmt_drop_index_to_recyclebin_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      volatile bool &stop_flag);

  /*************** TRUNCATE related DDL ***************/
  int handle_ddl_stmt_truncate_table_drop_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      volatile bool &stop_flag);
  int handle_ddl_stmt_truncate_drop_table_to_recyclebin_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema);
  int handle_ddl_stmt_truncate_table_create_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema,
      volatile bool &stop_flag);

  /*************** Tenant related DDL ***************/
  int handle_ddl_stmt_add_tenant_start_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema,
      volatile bool &stop_flag);
  int handle_ddl_stmt_add_tenant_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t new_schema,
      volatile bool &stop_flag);
  int handle_ddl_stmt_drop_tenant_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      const bool is_del_tenant_start_op,
      volatile bool &stop_flag);
  int handle_ddl_stmt_alter_tenant_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema_version,
      const int64_t new_schema_version,
      volatile bool &stop_flag);
  int handle_ddl_stmt_rename_tenant_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema_version,
      const int64_t new_schema_version,
      volatile bool &stop_flag);
  int handle_ddl_stmt_drop_tenant_to_recyclebin_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      volatile bool &stop_flag);

  /*************** Database related DDL ***************/
  int handle_ddl_stmt_alter_database_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      volatile bool &stop_flag);
  int handle_ddl_stmt_drop_database_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      const bool need_update_tic,
      volatile bool &stop_flag);
  int handle_ddl_stmt_drop_database_to_recyclebin_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      volatile bool &stop_flag);
  int handle_ddl_stmt_rename_database_(
      ObLogTenant &tenant,
      DdlStmtTask &ddl_stmt,
      const int64_t old_schema,
      const int64_t new_schema,
      volatile bool &stop_flag);

  int64_t get_start_serve_timestamp_(
      const int64_t new_schema_version,
      const int64_t commit_log_timestamp);

  // Parsing ddl stmt at the moment of recovery completion of backup recovery tenant
  // format: schema_version=${schema_version};teannt_gts=${tenant_gts}
  // tenant can be treated created from restore if ddl_stmt of create_tenant_end contains key_schema_version(schema_version) and key_tenant_gts(tenant_gts)
  // if error while parsing ddl_stmt_str, this method will return OB_ERR_UNEXPECTED, otherwise return is is_create_tenant_by_restore_ddl
  // if not create by restore, value of is_create_tenant_by_restore_ddl and tenant_gts_value won't change
  int parse_tenant_ddl_stmt_for_restore_(
      DdlStmtTask &ddl_stmt,
      int64_t &tenant_schema_version,
      int64_t &tenant_gts_value,
      bool &is_create_tenant_by_restore_ddl);

private:
  bool                is_inited_;
  IObLogSchemaGetter  *schema_getter_;
  bool                skip_reversed_schema_version_;
  bool                enable_white_black_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDDLProcessor);
};

}
}

#endif
