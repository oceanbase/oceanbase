/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_WORKLOAD_REPOSITORY_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_WORKLOAD_REPOSITORY_H_

#include "pl/ob_pl.h"
namespace oceanbase
{
namespace pl
{

class ObDbmsWorkloadRepository
{
public:
  struct AshReportParams
  {
    int64_t ash_begin_time;
    int64_t ash_end_time;
    ObString sql_id;
    ObString trace_id;
    ObString wait_class;
  };

public:
  static int create_snapshot(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int drop_snapshot_range(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int modify_snapshot_settings(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static const int64_t WR_USER_CREATE_SNAP_TASK_TIMEOUT = 18 * 1000 * 1000L;  // 18 s
  static const int64_t WR_USER_CREATE_SNAP_RPC_TIMEOUT = 20 * 1000 * 1000L;   // 20 s
  static const int64_t WR_USER_DEL_TASK_TIMEOUT = 8 * 60 * 1000 * 1000L;      // 8 min
  static const int64_t WR_USER_DEL_RPC_TIMEOUT = 9 * 60 * 1000 * 1000L;       // 9 min

  static int generate_ash_report_text(
      ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

  static int lpad(const char *src, const int64_t size, const char *pad, ObStringBuffer &buff);
  static int format_row(const int64_t column_size, const char *column_contents[],
      const int64_t column_widths[], const char *pad, const char *sep, ObStringBuffer &buff);
  static bool phase_cmp_func(
      const std::pair<const char *, int64_t> &a, const std::pair<const char *, int64_t> &b)
  {
    return a.second > b.second;  // Sort in descending order based on the value of int64_t.
  }
  static int usec_to_string(const int64_t usec, char *buf, int64_t buf_len, int64_t &pos);
  static int append_fmt_ash_view_sql(
      const AshReportParams &ash_report_params, ObSqlString &sql_string);
  static int get_ash_begin_and_end_time(
      const AshReportParams &ash_report_params, int64_t &ash_begin_time, int64_t &ash_end_time);
  static int get_ash_num_samples_and_events(
      const AshReportParams &ash_report_params, int64_t &num_samples, int64_t &num_events);
  static int print_ash_summary_info(const AshReportParams &ash_report_params, const int64_t l_btime,
      const int64_t l_etime, int64_t &dur_elapsed_time, int64_t &num_samples, int64_t &num_events,
      ObStringBuffer &buff, bool &no_data);
  static int print_ash_top_user_event_info(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_top_events_and_value(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_top_exec_phase(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t dur_elapsed_time, ObStringBuffer &buff);
  static int print_ash_top_sql_with_event(
      const AshReportParams &ash_report_params, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_top_sql_with_blocking_event(
      const AshReportParams &ash_report_params, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_sql_text_list(
      const AshReportParams &ash_report_params, ObStringBuffer &buff);
  static int print_ash_top_session_info(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, const int64_t dur_elapsed_time,
      ObStringBuffer &buff);
  static int print_ash_top_blocking_session_info(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, const int64_t dur_elapsed_time,
      ObStringBuffer &buff);
  static int print_ash_top_latches_info(
      const AshReportParams &ash_report_params, const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_node_load(
      const AshReportParams &ash_report_params, ObStringBuffer &buff);
private:
  static int check_snapshot_task_success_for_snap_id(int64_t snap_id, bool &is_all_success);
  static int check_drop_task_success_for_snap_id_range(const int64_t low_snap_id, const int64_t high_snap_id, bool &is_all_success);
};

} // end pl
} // end oceanbase
#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_WORKLOAD_REPOSITORY_H_