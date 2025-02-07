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
    AshReportParams(const common::ObTimeZoneInfo *tz_info)
        : ash_begin_time(0),
          ash_end_time(0),
          sql_id(),
          trace_id(),
          wait_class(),
          svr_ip(),
          port(-1),
          tenant_id(0),
          tz_info(tz_info),
          is_html(false),
          user_input_ash_begin_time(0),
          user_input_ash_end_time(0),
          section_cnt_(0)
    {}
    int64_t ash_begin_time;
    int64_t ash_end_time;
    ObString sql_id;
    ObString trace_id;
    ObString wait_class;
    ObString svr_ip;
    int64_t port;
    int64_t tenant_id;
    const common::ObTimeZoneInfo *tz_info;
    bool is_html;
    int64_t user_input_ash_begin_time;
    int64_t user_input_ash_end_time;
    mutable int64_t section_cnt_;
    TO_STRING_KV(K(ash_begin_time), K(ash_end_time), K(sql_id), K(trace_id), K(wait_class), K(svr_ip), K(port),
    K(tenant_id), K(is_html));
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
  static int print_text_table_frame(const int64_t column_size, const int64_t column_widths[], ObStringBuffer &buff);
  static bool phase_cmp_func(
      const std::pair<const char *, int64_t> &a, const std::pair<const char *, int64_t> &b)
  {
    return a.second > b.second;  // Sort in descending order based on the value of int64_t.
  }
  static int usec_to_string(const common::ObTimeZoneInfo *tz_info,
    const int64_t usec, char *buf, int64_t buf_len, int64_t &pos);
  static int append_fmt_ash_view_sql(
      const AshReportParams &ash_report_params, ObSqlString &sql_string);
  static bool is_single_identifier(const char *sql_str);
  static int append_time_model_view_sql(common::ObSqlString &sql_string,
                                        const char *select_lists,
                                        const common::ObArrayWrap<const char*> &timemodel_columns,
                                        const common::ObArrayWrap<int32_t> &timemodel_fields,
                                        const char *source_table,
                                        bool with_sum);
  static int unpivot_time_model_column_sql(common::ObSqlString &sql_string,
                                           const char *select_lists,
                                           const common::ObArrayWrap<const char*> &timemodel_columns,
                                           const char *source_table);
  static int get_ash_begin_and_end_time(
      const AshReportParams &ash_report_params, int64_t &ash_begin_time, int64_t &ash_end_time);
  static int get_ash_num_samples_and_events(
      const AshReportParams &ash_report_params, int64_t &num_samples, int64_t &num_events);
  static int print_ash_summary_info(const AshReportParams &ash_report_params, const int64_t l_btime,
      const int64_t l_etime, int64_t &dur_elapsed_time, int64_t &num_samples, int64_t &num_events,
      ObStringBuffer &buff, bool &no_data);
  static int print_ash_top_active_tenants(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_top_node_load(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_foreground_db_time(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_top_execution_phase(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_background_db_time(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_top_sessions(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_top_group(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_top_latches(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_activity_over_time(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_top_sql_with_top_db_time(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff)
      __attribute__((deprecated));
  static int print_top_sql_with_top_wait_events(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_top_sql_command_type(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_top_sql_with_top_operator(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_top_plsql(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_top_sql_text(const AshReportParams &ash_report_params,
      const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_top_blocking_session(const AshReportParams &ash_report_params,
    const int64_t num_samples, const int64_t num_events, ObStringBuffer &buff);
  static int print_ash_report_header(const AshReportParams &ash_report_params, ObStringBuffer &buff);
  static int print_section_header(const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *str);
  static int print_subsection_header(const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *str);
  static int print_section_column_header(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const int64_t column_size, const char *column_contents[],
      const int64_t column_widths[], bool need_merge_table = false);
  static int print_section_column_row(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const int64_t column_size, const char *column_contents[],
      const int64_t column_widths[], bool with_color = true);
  static int print_sql_section_column_row(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const int64_t column_size, const char *column_contents[],
      const int64_t column_widths[], bool with_color, int sql_id_column, int query_column);
  static int print_sqltext_section_column_row(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const int64_t column_size, const char *column_contents[],
      const int64_t column_widths[], bool with_color = true);
  static int print_section_column_end(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const int64_t column_size, const int64_t column_widths[]);
  static int print_ash_report_end(const AshReportParams &ash_report_params, ObStringBuffer &buff);
  static int print_section_explaination_begin(const AshReportParams &ash_report_params, ObStringBuffer &buff);
  static int print_section_explaination_end(const AshReportParams &ash_report_params, ObStringBuffer &buff);
  static int insert_section_explaination_line(const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *str);
private:
  static int check_snapshot_task_success_for_snap_id(int64_t snap_id, bool &is_all_success);
  static int check_drop_task_success_for_snap_id_range(const int64_t low_snap_id, const int64_t high_snap_id, bool &is_all_success);
  static int process_ash_report_params(const uint64_t data_version, const sql::ParamStore &params, AshReportParams &ash_report_params);
};

} // end pl
} // end oceanbase
#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_WORKLOAD_REPOSITORY_H_