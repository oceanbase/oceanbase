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
  class SqlIdArray
  {
  public:
    SqlIdArray(const char *str);
    SqlIdArray();
    SqlIdArray(const SqlIdArray &other);
    bool operator==(const SqlIdArray& other) const;
    const char *get_str() const { return str_; }
    TO_STRING_KV(K_(str));
  private:
    char str_[common::OB_MAX_SQL_ID_LENGTH + 1];
  };
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
          wr_begin_time(0),
          wr_end_time(0),
          ash_num_samples(0),
          wr_num_samples(0),
          cur_tenant_id(0),
          section_name_(nullptr),
          table_cnt_(0),
          top_sql_ids_()
    { }
    int64_t get_elapsed_time() const
    {
      return ash_end_time - ash_begin_time + wr_end_time - wr_begin_time;
    }
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
    int64_t wr_begin_time;
    int64_t wr_end_time;
    int64_t ash_num_samples;
    int64_t wr_num_samples;
    int64_t cur_tenant_id;
    mutable const char *section_name_;
    mutable int64_t table_cnt_; //table count in each section
    mutable common::ObArray<SqlIdArray> top_sql_ids_;
  };

  struct AshColumnItem
  {
    AshColumnItem(uint64_t foreign_key,
                  int64_t column_width,
                  const char *column_content,
                  int64_t flags)
      : foreign_section_name_(nullptr),
        foreign_table_index_(0),
        foreign_key_(foreign_key),
        column_width_(column_width),
        column_content_(column_content),
        flags_(flags)
    { }
    AshColumnItem(uint64_t finding_key, const char *column_content)
      : AshColumnItem(finding_key, 0, column_content, 0)
    { }
    AshColumnItem(const char *column_content)
      : AshColumnItem(0, 0, column_content, 0)
    { }
    AshColumnItem()
      : AshColumnItem(0, 0, nullptr, 0)
    { }
    const char *foreign_section_name_; //Associated section name
    int64_t foreign_table_index_; //Indicates the number of the table within the section.
    uint64_t foreign_key_; //column finding key
    int64_t column_width_;
    const char *column_content_;
    union {
      /**
       * used to mark some status related to column attribute,
       * and reserve some expansion bits for subsequent needs
       */
      uint64_t flags_;
      struct {
        uint64_t is_hidden_                       : 1; //hidden all elements in this column, used in html
        uint64_t href_sql_id_                     : 1; //build a href with SQL_ID column, can jump to sql text section
        uint64_t merge_cell_                      : 1; //used in html column header attribute, to merge the empty cell.
        uint64_t reserved_                        : 61;
      };
    };
  };

  struct AshColumnHeader
  {
    AshColumnHeader(int64_t column_size, AshColumnItem *columns, bool merge_columns = false)
      : column_size_(column_size),
        columns_(columns),
        flags_(0)
    { 
      merge_columns_ = merge_columns;
    }
    AshColumnHeader(int64_t column_size, AshColumnItem *columns, const int64_t column_widths[], bool merge_columns = false)
      : AshColumnHeader(column_size, columns, merge_columns)
    {
      for (int i = 0; i < column_size; ++i) {
        columns[i].column_width_ = column_widths[i];
      }
    }
    int64_t column_size_;
    AshColumnItem *columns_;
    union {
      /**
       * used to mark some status related to column attribute,
       * and reserve some expansion bits for subsequent needs
       */
      uint64_t flags_;
      struct {
        uint64_t merge_columns_                   : 1; //merge all empty cells upward in the column
        uint64_t hide_table_                      : 1; //hide this table
        uint64_t reserved_                        : 62;
      };
    };
  };

  struct AshRowItem
  {
    AshRowItem(int64_t column_size,
               uint64_t data_row_id,
               AshColumnItem *columns,
               bool with_color)
      : column_size_(column_size),
        data_row_id_(data_row_id),
        columns_(columns),
        with_color_(with_color)
    { }
    AshRowItem(int64_t column_size, AshColumnItem *columns, bool with_color)
      : AshRowItem(column_size, 0, columns, with_color)
    { }
    AshRowItem(int64_t column_size,
               uint64_t data_row_id,
               AshColumnItem *columns,
               const int64_t column_widths[],
               bool with_color)
      : AshRowItem(column_size, data_row_id, columns, with_color)
    {
      for (int i = 0; i < column_size; ++i) {
        columns[i].column_width_ = column_widths[i];
      }
    }
    AshRowItem(int64_t column_size,
               AshColumnItem *columns,
               const int64_t column_widths[],
               bool with_color)
      : AshRowItem(column_size, 0, columns, column_widths, with_color)
    { }

    int64_t column_size_;
    uint64_t data_row_id_;
    AshColumnItem *columns_;
    bool with_color_;
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
  static int format_row(const AshReportParams &ash_report_params,
                        const AshRowItem &ash_row,
                        const char *pad,
                        const char *sep,
                        ObStringBuffer &buff);
  static int print_text_table_frame(const AshColumnHeader &column_header, ObStringBuffer &buff);
  static bool phase_cmp_func(
      const std::pair<const char *, int64_t> &a, const std::pair<const char *, int64_t> &b)
  {
    return a.second > b.second;  // Sort in descending order based on the value of int64_t.
  }
  static int usec_to_string(const common::ObTimeZoneInfo *tz_info,
    const int64_t usec, char *buf, int64_t buf_len, int64_t &pos);
  static int get_ash_bound_sql_time(const AshReportParams &ash_report_params,
                                    char *start_time_buf, int64_t start_buf_len,
                                    char *end_time_buf, int64_t end_buf_len);
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
  static int append_fmt_wr_view_sql(
    const AshReportParams &ash_report_params, ObSqlString &sql_string);
  static int get_ash_begin_and_end_time(AshReportParams &ash_report_params);
  static int get_wr_begin_and_end_time(AshReportParams &ash_report_params);
  static int get_ash_num_samples(AshReportParams &ash_report_params, int64_t &num_samples);
  static int get_wr_num_samples(
      AshReportParams &ash_report_params, int64_t &wr_num_samples);
  static int print_ash_summary_info(const AshReportParams &ash_report_params, ObStringBuffer &buff, bool &no_data);
  static int print_ash_top_active_tenants(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_top_node_load(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_foreground_db_time(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_top_execution_phase(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_background_db_time(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_top_sessions(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_top_group(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_top_io_bandwidth(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_top_io_event(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff);
  static int print_action_activity_over_time(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_top_latches(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_activity_over_time(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_top_sql_with_top_db_time(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff)
      __attribute__((deprecated));
  static int print_top_sql_with_top_wait_events(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_top_sql_command_type(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_top_sql_with_top_operator(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_top_plsql(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_top_sql_text(const AshReportParams &ash_report_params,
      const int64_t num_samples, ObStringBuffer &buff);
  static int print_top_blocking_session(const AshReportParams &ash_report_params,
    const int64_t num_samples, ObStringBuffer &buff);
  static int print_ash_report_header(const AshReportParams &ash_report_params, ObStringBuffer &buff);
  static int print_section_header(const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *str);
  static int print_subsection_header(const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *str);
  static int print_section_column_header(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const AshColumnHeader &column_header);
  static int print_section_column_row(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const AshRowItem &ash_row);
  static int print_sqltext_section_column_row(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const AshRowItem &ash_row);
  static int print_section_column_end(const AshReportParams &ash_report_params,
      ObStringBuffer &buff, const AshColumnHeader &column_header);
  static int print_ash_report_end(const AshReportParams &ash_report_params, ObStringBuffer &buff);
  static int print_section_explaination_begin(const AshReportParams &ash_report_params, ObStringBuffer &buff);
  static int print_section_explaination_end(const AshReportParams &ash_report_params, ObStringBuffer &buff);
  static int insert_section_explaination_line(const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *str);
  static int print_top_db_object(const AshReportParams &ash_report_params, const int64_t num_samples, ObStringBuffer &buff);
private:
  static int check_snapshot_task_success_for_snap_id(int64_t snap_id, bool &is_all_success);
  static int check_drop_task_success_for_snap_id_range(const int64_t low_snap_id, const int64_t high_snap_id, bool &is_all_success);
  static int process_ash_report_params(const uint64_t data_version, const sql::ParamStore &params, AshReportParams &ash_report_params);
  static int append_fmt_ash_wr_view_sql(const AshReportParams &ash_report_params, ObSqlString &sql_string);
  static int print_section_header_and_explaination(const AshReportParams &ash_report_params, ObStringBuffer &buff, const char *content[], int64_t content_size);
  static uint64_t get_section_table_id(const char *section_name, int64_t table_index);
  static int32_t get_active_time_window(int64_t elapsed_time);
  static int get_table_name_by_id(uint64_t tenant_id, uint64_t object_id, char *object_name,const size_t object_name_len);
};

} // end pl
} // end oceanbase
#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_WORKLOAD_REPOSITORY_H_
