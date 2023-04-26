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

#include "lib/string/ob_string.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/sys_package/ob_dbms_upgrade.h"
#include "pl/sys_package/ob_dbms_stats.h"
#include "pl/sys_package/ob_dbms_scheduler_mysql.h"
#include "pl/sys_package/ob_dbms_application.h"
#include "pl/sys_package/ob_dbms_session.h"
#include "pl/sys_package/ob_dbms_monitor.h"
#include "pl/sys_package/ob_dbms_sql.h"
#include "pl/sys_package/ob_dbms_user_define_rule.h"
#include "pl/sys_package/ob_dbms_session.h"

#ifdef INTERFACE_DEF
  INTERFACE_DEF(INTERFACE_START, "TEST", (ObPLInterfaceImpl::call))

  /*************************.. add interface here ..*****************************/
  // start of __dbms_upgrade
  INTERFACE_DEF(INTERFACE_DBMS_UPGRADE_SINGLE, "UPGRADE_SINGLE", (ObDBMSUpgrade::upgrade_single))
  INTERFACE_DEF(INTERFACE_DBMS_UPGRADE_ALL, "UPGRADE_ALL", (ObDBMSUpgrade::upgrade_all))
  // end of __dbms_upgrade


  // start of dbms_application_info
  INTERFACE_DEF(INTERFACE_DBMS_READ_CLIENT_INFO, "READ_CLIENT_INFO", (ObDBMSAppInfo::read_client_info))
  INTERFACE_DEF(INTERFACE_DBMS_READ_MODULE, "READ_MODULE", (ObDBMSAppInfo::read_module))
  INTERFACE_DEF(INTERFACE_DBMS_SET_ACTION, "SET_ACTION", (ObDBMSAppInfo::set_action))
  INTERFACE_DEF(INTERFACE_DBMS_SET_CLIENT_INFO, "SET_CLIENT_INFO", (ObDBMSAppInfo::set_client_info))
  INTERFACE_DEF(INTERFACE_DBMS_SET_MODULE, "SET_MODULE", (ObDBMSAppInfo::set_module))
  // end of dbms_application_info

  // start of dbms_monitor
  INTERFACE_DEF(INTERFACE_DBMS_SESSION_TRACE_ENABLE, "OB_SESSION_TRACE_ENABLE", (ObDBMSMonitor::session_trace_enable))
  INTERFACE_DEF(INTERFACE_DBMS_SESSION_TRACE_DISABLE, "OB_SESSION_TRACE_DISABLE", (ObDBMSMonitor::session_trace_disable))
  INTERFACE_DEF(INTERFACE_DBMS_CLIENT_ID_TRACE_ENABLE, "OB_CLIENT_ID_TRACE_ENABLE", (ObDBMSMonitor::client_id_trace_enable))
  INTERFACE_DEF(INTERFACE_DBMS_CLIENT_ID_TRACE_DISABLE, "OB_CLIENT_ID_TRACE_DISABLE", (ObDBMSMonitor::client_id_trace_disable))
  INTERFACE_DEF(INTERFACE_DBMS_MOD_ACT_TRACE_ENABLE, "OB_MOD_ACT_TRACE_ENABLE", (ObDBMSMonitor::mod_act_trace_enable))
  INTERFACE_DEF(INTERFACE_DBMS_MOD_ACT_TRACE_DISABLE, "OB_MOD_ACT_TRACE_DISABLE", (ObDBMSMonitor::mod_act_trace_disable))
  INTERFACE_DEF(INTERFACE_DBMS_TENANT_TRACE_ENABLE, "OB_TENANT_TRACE_ENABLE", (ObDBMSMonitor::tenant_trace_enable))
  INTERFACE_DEF(INTERFACE_DBMS_TENANT_TRACE_DISABLE, "OB_TENANT_TRACE_DISABLE", (ObDBMSMonitor::tenant_trace_disable))
  // end of dbms_monitor

  //start of dbms_stat
  INTERFACE_DEF(INTERFACE_DBMS_STATS_GATHER_TABLE_STATS, "GATHER_TABLE_STATS", (ObDbmsStats::gather_table_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_GATHER_SCHEMA_STATS, "GATHER_SCHEMA_STATS", (ObDbmsStats::gather_schema_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_SET_TABLE_STATS, "SET_TABLE_STATS", (ObDbmsStats::set_table_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_SET_COLUMN_STATS, "SET_COLUMN_STATS", (ObDbmsStats::set_column_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_DELETE_TABLE_STATS, "DELETE_TABLE_STATS", (ObDbmsStats::delete_table_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_DELETE_COLUMN_STATS, "DELETE_COLUMN_STATS", (ObDbmsStats::delete_column_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_DELETE_SCHEMA_STATS, "DELETE_SCHEMA_STATS", (ObDbmsStats::delete_schema_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_FLUSH_DATABASE_MONITORING_INFO, "FLUSH_DATABASE_MONITORING_INFO", (ObDbmsStats::flush_database_monitoring_info))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_CREATE_STAT_TABLE, "CREATE_STAT_TABLE", (ObDbmsStats::create_stat_table))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_DROP_STAT_TABLE, "DROP_STAT_TABLE", (ObDbmsStats::drop_stat_table))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_EXPORT_TABLE_STATS, "EXPORT_TABLE_STATS", (ObDbmsStats::export_table_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_EXPORT_COLUMN_STATS, "EXPORT_COLUMN_STATS", (ObDbmsStats::export_column_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_EXPORT_SCHEMA_STATS, "EXPORT_SCHEMA_STATS", (ObDbmsStats::export_schema_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_IMPORT_TABLE_STATS, "IMPORT_TABLE_STATS", (ObDbmsStats::import_table_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_IMPORT_COLUMN_STATS, "IMPORT_COLUMN_STATS", (ObDbmsStats::import_column_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_IMPORT_SCHEMA_STATS, "IMPORT_SCHEMA_STATS", (ObDbmsStats::import_schema_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_LOCK_TABLE_STATS, "LOCK_TABLE_STATS", (ObDbmsStats::lock_table_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_LOCK_PARTITION_STATS, "LOCK_PARTITION_STATS", (ObDbmsStats::lock_partition_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_LOCK_SCHEMA_STATS, "LOCK_SCHEMA_STATS", (ObDbmsStats::lock_schema_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_UNLOCK_TABLE_STATS, "UNLOCK_TABLE_STATS", (ObDbmsStats::unlock_table_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_UNLOCK_PARTITION_STATS, "UNLOCK_PARTITION_STATS", (ObDbmsStats::unlock_partition_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_UNLOCK_SCHEMA_STATS, "UNLOCK_SCHEMA_STATS", (ObDbmsStats::unlock_schema_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_GATHER_DATABASE_STATS_JOB_PROC, "GATHER_DATABASE_STATS_JOB_PROC", (ObDbmsStats::gather_database_stats_job_proc))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_RESTORE_TABLE_STATS, "RESTORE_TABLE_STATS", (ObDbmsStats::restore_table_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_RESTORE_SCHEMA_STATS, "RESTORE_SCHEMA_STATS", (ObDbmsStats::restore_schema_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_PURGE_STATS, "PURGE_STATS", (ObDbmsStats::purge_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_ALTER_STATS_HISTORY_RETENTION, "ALTER_STATS_HISTORY_RETENTION", (ObDbmsStats::alter_stats_history_retention))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_GET_STATS_HISTORY_AVAILABILITY, "GET_STATS_HISTORY_AVAILABILITY", (ObDbmsStats::get_stats_history_availability))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_GET_STATS_HISTORY_RETENTION, "GET_STATS_HISTORY_RETENTION", (ObDbmsStats::get_stats_history_retention))
  INTERFACE_DEF(INTERFACE_DBMS_RESET_GLOBAL_PREF_DEFAULTS, "RESET_GLOBAL_PREF_DEFAULTS", (ObDbmsStats::reset_global_pref_defaults))
  INTERFACE_DEF(INTERFACE_DBMS_SET_GLOBAL_PREFS, "SET_GLOBAL_PREFS", (ObDbmsStats::set_global_prefs))
  INTERFACE_DEF(INTERFACE_DBMS_SET_SCHEMA_PREFS, "SET_SCHEMA_PREFS", (ObDbmsStats::set_schema_prefs))
  INTERFACE_DEF(INTERFACE_DBMS_SET_TABLE_PREFS, "SET_TABLE_PREFS", (ObDbmsStats::set_table_prefs))
  INTERFACE_DEF(INTERFACE_DBMS_GET_PREFS, "GET_PREFS", (ObDbmsStats::get_prefs))
  INTERFACE_DEF(INTERFACE_DBMS_DELETE_SCHEMA_PREFS, "DELETE_SCHEMA_PREFS", (ObDbmsStats::delete_schema_prefs))
  INTERFACE_DEF(INTERFACE_DBMS_DELETE_TABLE_PREFS, "DELETE_TABLE_PREFS", (ObDbmsStats::delete_table_prefs))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_GATHER_INDEX_STATS, "GATHER_INDEX_STATS", (ObDbmsStats::gather_index_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_DELETE_INDEX_STATS, "DELETE_INDEX_STATS", (ObDbmsStats::delete_index_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_SET_INDEX_STATS, "SET_INDEX_STATS", (ObDbmsStats::set_index_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_EXPORT_INDEX_STATS, "EXPORT_INDEX_STATS", (ObDbmsStats::export_index_stats))
  INTERFACE_DEF(INTERFACE_DBMS_STATS_IMPORT_INDEX_STATS, "IMPORT_INDEX_STATS", (ObDbmsStats::import_index_stats))
  //end of dbms_stat


  //start of dbms_scheduler_mysql
#define DEFINE_DBMS_SCHEDULER_MYSQL_INTERFACE(symbol, func) \
  INTERFACE_DEF(INTERFACE_##symbol, #symbol, (func))

  DEFINE_DBMS_SCHEDULER_MYSQL_INTERFACE(DBMS_SCHEDULER_MYSQL_CREATE_JOB, ObDBMSSchedulerMysql::create_job)
  DEFINE_DBMS_SCHEDULER_MYSQL_INTERFACE(DBMS_SCHEDULER_MYSQL_DISABLE, ObDBMSSchedulerMysql::disable)
  DEFINE_DBMS_SCHEDULER_MYSQL_INTERFACE(DBMS_SCHEDULER_MYSQL_ENABLE, ObDBMSSchedulerMysql::enable)
  DEFINE_DBMS_SCHEDULER_MYSQL_INTERFACE(DBMS_SCHEDULER_MYSQL_SET_ATTRIBUTE, ObDBMSSchedulerMysql::set_attribute)

#undef DEFINE_DBMS_SCHEDULER_MYSQL_INTERFACE
  //end of dbms_scheduler_mysql


  // start of dbms_session
  INTERFACE_DEF(INTERFACE_DBMS_SESSION_CLEAR_ALL_CONTEXT, "CLEAR_ALL_CONTEXT", (ObDBMSSession::clear_all_context))
  INTERFACE_DEF(INTERFACE_DBMS_SESSION_CLEAR_CONTEXT, "CLEAR_CONTEXT", (ObDBMSSession::clear_context))
  INTERFACE_DEF(INTERFACE_DBMS_SESSION_CLEAR_IDENTIFIER, "CLEAR_IDENTIFIER", (ObDBMSSession::clear_identifier))
  INTERFACE_DEF(INTERFACE_DBMS_SESSION_SET_CONTEXT, "SET_CONTEXT", (ObDBMSSession::set_context))
  INTERFACE_DEF(INTERFACE_DBMS_SESSION_SET_IDENTIFIER, "SET_IDENTIFIER", (ObDBMSSession::set_identifier))
  // end of dbms_session

  // start of dbms_udr
  INTERFACE_DEF(INTERFACE_DBMS_UDR_CREATE_RULE, "CREATE_RULE", (ObDBMSUserDefineRule::create_rule))
  INTERFACE_DEF(INTERFACE_DBMS_UDR_REMOVE_RULE, "REMOVE_RULE", (ObDBMSUserDefineRule::remove_rule))
  INTERFACE_DEF(INTERFACE_DBMS_UDR_ENABLE_RULE, "ENABLE_RULE", (ObDBMSUserDefineRule::enable_rule))
  INTERFACE_DEF(INTERFACE_DBMS_UDR_DISABLE_RULE, "DISABLE_RULE", (ObDBMSUserDefineRule::disable_rule))
  // end of dbms_udr
  /****************************************************************************/

  INTERFACE_DEF(INTERFACE_END, "INVALID", (nullptr))
#endif

#ifndef OCEANBASE_SRC_PL_OB_PL_INTERFACE_PRAGMA_H_
#define OCEANBASE_SRC_PL_OB_PL_INTERFACE_PRAGMA_H_

namespace oceanbase
{
namespace pl
{

typedef int(*PL_C_INTERFACE_t)(ObPLExecCtx&, ParamStore&, ObObj&);

enum ObPLInterfaceType
{
#define INTERFACE_DEF(type, name, entry) type,
#include "pl/ob_pl_interface_pragma.h"
#undef INTERFACE_DEF
};

class ObPLInterfaceService
{
public:
  ObPLInterfaceService() {}
  virtual ~ObPLInterfaceService() {}

  PL_C_INTERFACE_t get_entry(common::ObString &name) const;
  int init();

private:

  ObPLInterfaceType get_type(common::ObString &name) const;

private:
  typedef common::hash::ObHashMap<common::ObString, ObPLInterfaceType,
      common::hash::NoPthreadDefendMode> InterfaceMap;
  InterfaceMap interface_map_;
};

class ObPLInterfaceImpl
{
public:
  ObPLInterfaceImpl() {}
  virtual ~ObPLInterfaceImpl() {}

public:
  static int call(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  virtual int check_params() = 0;

};

}
}

#endif /* OCEANBASE_SRC_PL_OB_PL_INTERFACE_PRAGMA_H_ */
