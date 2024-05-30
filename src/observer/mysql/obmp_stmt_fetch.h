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

#ifndef OCEANBASE_SRC_OBSERVER_MYSQL_OBMP_STMT_FETCH_H_
#define OCEANBASE_SRC_OBSERVER_MYSQL_OBMP_STMT_FETCH_H_

#include "sql/ob_sql_context.h"
#include "observer/mysql/obmp_base.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
namespace oceanbase
{
namespace observer
{
#define FETCH_PACKET_SIZE_WITHOUT_OFFSET 9
#define FETCH_PACKET_SIZE_WITH_OFFSET 15
#define FETCH_PACKET_SIZE_WITH_OFFSET_OK 19

class ObMPStmtFetch : public ObMPBase
{
public:
  enum OffsetType {
    OB_OCI_DEFAULT = 0,
    OB_OCI_FETCH_CURRENT = 1,  
    OB_OCI_FETCH_NEXT = 2,
    OB_OCI_FETCH_FIRST = 4,
    OB_OCI_FETCH_LAST = 8,
    OB_OCI_FETCH_PRIOR = 16,
    OB_OCI_FETCH_ABSOLUTE = 32,
    OB_OCI_FETCH_RELATIVE = 64
  };
  enum ExtendFlag {
    OB_OCI_INVALID = 0,
    OB_OCI_NEED_EXTRA_OK_PACKET = 1,
    OB_OCI_GET_PIECE_INFO = 2
  };
  static const obmysql::ObMySQLCmd COM = obmysql::COM_STMT_FETCH;
  explicit ObMPStmtFetch(const ObGlobalContext &gctx);
  virtual ~ObMPStmtFetch() {}
  int64_t get_single_process_timestamp() const { return single_process_timestamp_; }
  int64_t get_exec_start_timestamp() const { return exec_start_timestamp_; }
  int64_t get_exec_end_timestamp() const { return exec_end_timestamp_; }
  int64_t get_send_timestamp() const { return get_receive_timestamp(); }
  virtual int flush_buffer(const bool is_last) override
  {
    return ObMPBase::flush_buffer(is_last);
  }
  inline bool has_ok_packet() { return extend_flag_ & OB_OCI_NEED_EXTRA_OK_PACKET; }
  inline bool has_long_data() { return extend_flag_ & OB_OCI_GET_PIECE_INFO; }
  int response_row(sql::ObSQLSessionInfo &session, 
                   common::ObNewRow &row,
                   const ColumnsFieldArray *fields,
                   char *column_map,
                   int32_t stmt_id,
                   bool first_time,
                   bool is_packed,
                   ObSchemaGetterGuard *schema_guard);
  int response_row(sql::ObSQLSessionInfo &session,
                   common::ObNewRow &row,
                   const ColumnsFieldArray *fields,
                   bool is_packed,
                   sql::ObExecContext *exec_ctx = NULL,
                   ObSchemaGetterGuard *schema_guard = NULL) {
    return ObMPBase::response_row(session, row, fields, is_packed, exec_ctx, true, schema_guard);
  }
  bool need_close_cursor() { return need_close_cursor_; }
  void set_close_cursor() { need_close_cursor_ = true; }
  void reset_close_cursor() { need_close_cursor_ = false; }
  
protected:
  virtual int deserialize()  { return common::OB_SUCCESS; }
  virtual int process();
private:
  int do_process(sql::ObSQLSessionInfo &session, bool &need_response_error);
  int set_session_active(sql::ObSQLSessionInfo &session) const;
  int process_fetch_stmt(sql::ObSQLSessionInfo &session, bool &need_response_error);
  int response_result(pl::ObPLCursorInfo &cursor,
                      sql::ObSQLSessionInfo &session,
                      int64_t fetch_limit,
                      int64_t &row_num);
  int response_query_header(sql::ObSQLSessionInfo &session, const ColumnsFieldArray *fields);
  virtual int before_process();
  void record_stat(const sql::stmt::StmtType type, const int64_t end_time) const;
  //重载response，在response中不去调用flush_buffer(true)；flush_buffer(true)在需要回包时显示调用
private:
  int64_t cursor_id_;
  int64_t fetch_rows_;
  int64_t single_process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  int16_t offset_type_;
  int32_t offset_;
  int32_t extend_flag_;
  char    *column_flag_;
  bool    need_close_cursor_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMPStmtFetch);
}; //end of class
} //end of namespace observer
} //end of namespace oceanbase



#endif /* OCEANBASE_SRC_OBSERVER_MYSQL_OBMP_STMT_FETCH_H_ */
