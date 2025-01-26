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

#ifndef OB_DBMS_STATS_MAINTENANCE_WINDOW_H
#define OB_DBMS_STATS_MAINTENANCE_WINDOW_H

#include "lib/string/ob_sql_string.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_dml_sql_splicer.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"

#define DAY_OF_WEEK 7
#define HOUR_OF_DAY  24
#define USEC_OF_HOUR (60 * 60 * 1000000LL)
#define MAX_OF_WORK_DAY 5
#define DEFAULT_DAY_INTERVAL_USEC (1 * 24 * 60 * 60 * 1000000LL)
#define DEFAULT_WEEK_INTERVAL_USEC (7 * DEFAULT_DAY_INTERVAL_USEC)
#define DEFAULT_WORKING_DAY_START_HOHR 22
#define DEFAULT_WORKING_DAY_DURATION_SEC (4 * 60 * 60)
#define DEFAULT_WORKING_DAY_DURATION_USEC (4 * 60 * 60 * 1000000LL)
#define DEFAULT_NON_WORKING_DAY_START_HOHR 6
#define DEFAULT_NON_WORKING_DAY_DURATION_SEC (20 * 60 * 60)
#define DEFAULT_NON_WORKING_DAY_DURATION_USEC (20 * 60 * 60 * 1000000LL)
#define DEFAULT_HISTORY_MANAGER_DURATION_SEC (12 * 60 * 60)
#define DEFAULT_ASYNC_GATHER_STATS_DURATION_SEC (10 * 60)
#define DEFAULT_ASYNC_GATHER_STATS_INTERVAL_USEC (15 * 60 * 1000000LL)

namespace oceanbase {

namespace common {

class ObDbmsStatsMaintenanceWindow
{
public:
  ObDbmsStatsMaintenanceWindow();

  static int get_stats_maintenance_window_jobs_sql(const share::schema::ObSysVariableSchema &sys_variable,
                                                   const uint64_t tenant_id,
                                                   common::ObISQLClient &sql_client);

  static int is_stats_maintenance_window_attr(const sql::ObSQLSessionInfo *session,
                                              const ObString &job_name,
                                              const ObString &attr_name,
                                              const ObString &val_name,
                                              bool &is_window_attr,
                                              share::ObDMLSqlSplicer &dml);

  static bool is_stats_job(const ObString &job_name);

  static int get_async_gather_stats_job_for_upgrade(common::ObMySQLProxy *sql_proxy,
                                                    const uint64_t tenant_id);

  static int check_job_exists(common::ObMySQLProxy *sql_proxy,
                              const uint64_t tenant_id,
                              const char* job_name,
                              bool &is_join_exists);

  static int get_spm_stats_upgrade_jobs_sql(common::ObMySQLProxy *sql_proxy,
                                            const ObSysVariableSchema &sys_variable,
                                            const uint64_t tenant_id,
                                            const bool is_oracle_mode);

  static int get_time_zone_offset(const share::schema::ObSysVariableSchema &sys_variable,
                                  const uint64_t tenant_id,
                                  int32_t &offset_sec);

private:
  static int get_window_job_info(const int64_t current_time,
                                 const int64_t nth_window,
                                 const int64_t offset_sec,
                                 int64_t &start_usec,
                                 ObSqlString &job_action);

  static int get_stat_window_job_info(const bool is_oracle_mode,
                                     const uint64_t tenant_id,
                                     const int64_t job_id,
                                     const char *job_name,
                                     const ObString &exec_env,
                                     const int64_t start_usec,
                                     ObSqlString &job_action,
                                     dbms_scheduler::ObDBMSSchedJobInfo &job_info);

  static int get_stats_history_manager_job_info(const bool is_oracle_mode,
                                               const uint64_t tenant_id,
                                               const int64_t job_id,
                                               const ObString &exec_env,
                                               dbms_scheduler::ObDBMSSchedJobInfo &job_info);
  static int get_spm_stats_job_info(const bool is_oracle_mode,
                                   const uint64_t tenant_id,
                                   const int64_t job_id,
                                   const int64_t offset_sec,
                                   const ObString &exec_env,
                                   dbms_scheduler::ObDBMSSchedJobInfo &job_info);

  static bool is_work_day(int64_t now_wday) { return now_wday >= 1 && now_wday <= MAX_OF_WORK_DAY; }

  static int check_date_validate(const ObString &job_name,
                                 const int64_t specify_time,
                                 const int64_t current_time,
                                 bool &is_valid);
  static int get_async_gather_stats_job_info(const bool is_oracle_mode,
                                            const uint64_t tenant_id,
                                            const int64_t job_id,
                                            const ObString &exec_env,
                                            dbms_scheduler::ObDBMSSchedJobInfo &job_info);
  static int get_next_job_id_and_exec_env(common::ObMySQLProxy *sql_proxy,
                                          ObIAllocator &allocator,
                                          const uint64_t tenant_id,
                                          int64_t &job_id,
                                          ObString &exec_env);

};

}
}

#endif // OB_DBMS_STATS_MAINTENANCE_WINDOW_H
