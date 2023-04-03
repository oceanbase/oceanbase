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
#define DEFAULT_DML_STATS_INTERVAL_USEC (15*60*1000000LL)

namespace oceanbase {

namespace common {

class ObDbmsStatsMaintenanceWindow
{
public:
  ObDbmsStatsMaintenanceWindow();

  static int get_stats_maintenance_window_jobs_sql(const share::schema::ObSysVariableSchema &sys_variable,
                                                   const uint64_t tenant_id,
                                                   ObSqlString &raw_sql,
                                                   int64_t &expected_affected_rows);

  static int is_stats_maintenance_window_attr(const sql::ObSQLSessionInfo *session,
                                              const ObString &job_name,
                                              const ObString &attr_name,
                                              const ObString &val_name,
                                              bool &is_window_attr,
                                              share::ObDMLSqlSplicer &dml);

  static bool is_stats_job(const ObString &job_name);

private:
  static int get_window_job_info(const int64_t current_time,
                                 const int64_t nth_window,
                                 const int64_t offset_sec,
                                 int64_t &start_usec,
                                 ObSqlString &job_action);

  static int get_stat_window_job_sql(const bool is_oracle_mode,
                                     const uint64_t tenant_id,
                                     const int64_t job_id,
                                     const char *job_name,
                                     const ObString &exec_env,
                                     const int64_t start_usec,
                                     ObSqlString &job_action,
                                     ObSqlString &raw_sql);

  static int get_stats_history_manager_job_sql(const bool is_oracle_mode,
                                               const uint64_t tenant_id,
                                               const int64_t job_id,
                                               const ObString &exec_env,
                                               ObSqlString &raw_sql);

  static int get_dummy_guard_job_sql(const uint64_t tenant_id,
                                     const int64_t job_id,
                                     ObSqlString &raw_sql);

  static bool is_work_day(int64_t now_wday) { return now_wday >= 1 && now_wday <= MAX_OF_WORK_DAY; }

  static int get_time_zone_offset(const share::schema::ObSysVariableSchema &sys_variable,
                                  const uint64_t tenant_id,
                                  int32_t &offset_sec);
  static int check_date_validate(const ObString &job_name,
                                 const int64_t specify_time,
                                 const int64_t current_time,
                                 bool &is_valid);

};

}
}

#endif // OB_DBMS_STATS_MAINTENANCE_WINDOW_H
