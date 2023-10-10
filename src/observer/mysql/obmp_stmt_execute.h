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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_EXECUTE_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_EXECUTE_H_


#include "lib/container/ob_2d_array.h"
#include "sql/ob_sql_context.h"
#include "observer/mysql/obmp_base.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"

namespace oceanbase
{
namespace observer
{

struct ObSavedException
{
  int pos_;
  int32_t error_code_;
  common::ObString error_msg_;

  TO_STRING_KV(K(pos_), K(error_code_), K(error_msg_));
};

typedef common::ParamStore ParamStore;

enum ObPSCursorType
{
  ObNormalType,
  ObExecutePsCursorType,
  ObPrexecutePsCursorType
};

class ObPSAnalysisChecker
{
public:
  ObPSAnalysisChecker()
    : pos_(nullptr), begin_pos_(nullptr), end_pos_(nullptr),
      data_len_(0), need_check_(true)
  {}
  void init(const char*& pos, const int64_t len)
  {
    pos_ = &pos;
    begin_pos_ = pos;
    end_pos_ = pos + len;
    data_len_ = len;
    need_check_ = true;
  }
  int detection(const int64_t len);
  inline int64_t remain_len()
  { return end_pos_ - (*pos_); }

public:
  const char** pos_;
  const char* begin_pos_;
  const char* end_pos_;
  int64_t data_len_;
  bool need_check_;
};

#define PS_DEFENSE_CHECK(len)                              \
  if (OB_FAIL(ret)) {                                      \
  } else if (OB_FAIL(analysis_checker_.detection(len))) {  \
    LOG_WARN("memory access out of bounds", K(ret));       \
  } else

#define PS_STATIC_DEFENSE_CHECK(checker, len)              \
  if (OB_FAIL(ret)) {                                      \
  } else if (nullptr != checker                            \
    && OB_FAIL(checker->detection(len))) {                 \
    LOG_WARN("memory access out of bounds", K(ret));       \
  } else

class ObMPStmtExecute : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_STMT_EXECUTE;
  const uint32_t DEFAULT_ITERATION_COUNT = 1;

  explicit ObMPStmtExecute(const ObGlobalContext &gctx);
  virtual ~ObMPStmtExecute() {}

  // Parse basic param value, no MYSQL_TYPE_COMPLEX or MYSQL_TYPE_CURSOR.
  // see parse_param_value()
  static int parse_basic_param_value(ObIAllocator &allocator,
                                    const uint32_t type,
                                    const ObCharsetType charset,
                                    const ObCharsetType ncharset,
                                    const ObCollationType cs_type,
                                    const ObCollationType ncs_type,
                                    const char *& data,
                                    const common::ObTimeZoneInfo *tz_info,
                                    ObObj &param,
                                    bool is_complex_element = false,
                                    ObPSAnalysisChecker *checker = nullptr,
                                    bool is_unsigned = false);
  static int parse_mysql_timestamp_value(const obmysql::EMySQLFieldType field_type,
                                    const char *&data,
                                    ObObj &param,
                                    const common::ObTimeZoneInfo *tz_info,
                                    ObPSAnalysisChecker *checker = nullptr);
  static int parse_integer_value(const uint32_t type,
                                 const char *&data,
                                 ObObj &param,
                                 ObIAllocator &allocator,
                                 bool is_complex_element = false,
                                 ObPSAnalysisChecker *checker = nullptr,
                                 bool is_unsigned = false);
  static int parse_oracle_timestamp_value(const obmysql::EMySQLFieldType field_type, const char *&data,
                                    const ObTimeConvertCtx &cvrt_ctx, ObObj &param,
                                    ObPSAnalysisChecker *checker = nullptr);
  static int parse_mysql_time_value(const char *&data, ObObj &param, ObPSAnalysisChecker *checker = nullptr);
  static int parse_oracle_interval_ym_value(const char *&data, ObObj &param, ObPSAnalysisChecker *checker = nullptr);
  static int parse_oracle_interval_ds_value(const char *&data, ObObj &param, ObPSAnalysisChecker *checker = nullptr);
  int64_t get_single_process_timestamp() const { return single_process_timestamp_; }
  int64_t get_exec_start_timestamp() const { return exec_start_timestamp_; }
  int64_t get_exec_end_timestamp() const { return exec_end_timestamp_; }
  int64_t get_send_timestamp() const { return get_receive_timestamp(); }
  virtual int flush_buffer(const bool is_last) override
  {
    return ObMPBase::flush_buffer(is_last);
  }
  int init_for_arraybinding(ObIAllocator &alloc);
  int init_arraybinding_paramstore(ObIAllocator &alloc);
  int init_arraybinding_fields_and_row(ObMySQLResultSet &result);
  int set_session_active(sql::ObSQLSessionInfo &session) const;
  int after_do_process_for_arraybinding(ObMySQLResultSet &result);
  inline void set_arraybounding(bool is_arraybinding) { is_arraybinding_ = is_arraybinding; }
  inline bool get_arraybounding() { return is_arraybinding_; }
  inline void set_save_exception(bool is_save_exception) { is_save_exception_ = is_save_exception; }
  inline bool get_save_exception() { return is_save_exception_; }
  // response need send long data
  virtual bool is_send_long_data() { return false;}
  inline bool support_send_long_data(const uint32_t type) {
    bool is_support = false;
    switch (type) {
      case obmysql::MYSQL_TYPE_OB_NVARCHAR2:
      case obmysql::MYSQL_TYPE_OB_NCHAR:
      case obmysql::MYSQL_TYPE_OB_RAW:
      case obmysql::MYSQL_TYPE_TINY_BLOB:
      case obmysql::MYSQL_TYPE_MEDIUM_BLOB:
      case obmysql::MYSQL_TYPE_LONG_BLOB:
      case obmysql::MYSQL_TYPE_BLOB:
      case obmysql::MYSQL_TYPE_STRING:
      case obmysql::MYSQL_TYPE_VARCHAR:
      case obmysql::MYSQL_TYPE_VAR_STRING:
      case obmysql::MYSQL_TYPE_OB_NUMBER_FLOAT:
      case obmysql::MYSQL_TYPE_NEWDECIMAL:
      case obmysql::MYSQL_TYPE_OB_UROWID:
      case obmysql::MYSQL_TYPE_ORA_BLOB:
      case obmysql::MYSQL_TYPE_ORA_CLOB:
      case obmysql::MYSQL_TYPE_JSON:
      case obmysql::MYSQL_TYPE_GEOMETRY:
        is_support = true;
        break;
      case obmysql::MYSQL_TYPE_COMPLEX:
        is_support = lib::is_oracle_mode() ? true : false;
        break;
      default :
        is_support = false;
    }
    return is_support;
  }
  inline int32_t get_param_num() { return params_num_; }
  inline void set_param_num(int32_t num) { params_num_ = num; }
  static int store_params_value_to_str(ObIAllocator &alloc,
                                       sql::ObSQLSessionInfo &session,
                                       ParamStore *params,
                                       char *&params_value,
                                       int64_t &params_value_len);

protected:
  virtual int deserialize()  { return common::OB_SUCCESS; }
  virtual int process();
  int response_result(ObMySQLResultSet &result,
                      sql::ObSQLSessionInfo &session,
                      bool force_sync_resp,
                      bool &async_resp_used);
  inline void set_single_process_timestamp(int64_t single_process_timestamp)
  {
    single_process_timestamp_ = single_process_timestamp;
  }
  inline void set_exec_start_timestamp(int64_t time) { exec_start_timestamp_ = time; }
  ParamStore *get_params() { return params_; }
  inline void set_param(ParamStore *params) { params_ = params; }
  sql::ObSqlCtx &get_ctx() { return ctx_; }
  ObQueryRetryCtrl &get_retry_ctrl() { return retry_ctrl_; }
  void record_stat(const sql::stmt::StmtType type, const int64_t end_time) const;
  int request_params(sql::ObSQLSessionInfo *session,
                     const char* &pos,
                     uint32_t ps_stmt_checksum,
                     ObIAllocator &alloc,
                     int32_t all_param_num);
  int parse_request_type(const char* &pos,
                         int64_t num_of_params,
                         int8_t new_param_bound_flag,
                         ObCollationType cs_type,
                         ObCollationType ncs_type,
                         sql::ParamTypeArray &param_types,
                         sql::ParamTypeInfoArray &param_type_infos
                         /*ParamCastArray param_cast_infos*/);
  int parse_request_param_value(ObIAllocator &alloc,
                                sql::ObSQLSessionInfo *session,
                                const char* &pos,
                                int64_t idx,
                                obmysql::EMySQLFieldType &param_type,
                                sql::TypeInfo &param_type_info,
                                ObObjParam &param,
                                const char *bitmap);
  int store_params_value_to_str(ObIAllocator &alloc, sql::ObSQLSessionInfo &session);
  int execute_response(sql::ObSQLSessionInfo &session,
                        ObMySQLResultSet &result,
                        const bool enable_perf_event,
                        bool &need_response_error,
                        bool &is_diagnostics_stmt,
                        int64_t &execution_id,
                        const bool force_sync_resp,
                        bool &async_resp_used,
                        ObPsStmtId &inner_stmt_id);
  virtual bool is_prexecute() const { return false; }
  inline bool is_execute_ps_cursor() { return ObExecutePsCursorType == ps_cursor_type_; }
  inline bool is_prexecute_ps_cursor() { return ObPrexecutePsCursorType == ps_cursor_type_; }
  inline bool is_ps_cursor() { return ObNormalType != ps_cursor_type_; }
  inline void set_ps_cursor_type(ObPSCursorType type) { ps_cursor_type_ = type; }
  inline bool is_pl_stmt(sql::stmt::StmtType stmt_type) const
  {
    return (sql::stmt::T_CALL_PROCEDURE  == stmt_type || sql::stmt::T_ANONYMOUS_BLOCK == stmt_type);
  }
  void set_curr_sql_idx(int64_t curr_sql_idx) { curr_sql_idx_ = curr_sql_idx; }
  int64_t get_curr_sql_idx() { return curr_sql_idx_; }

private:
  // for arraybinding
  int init_arraybinding_field(int64_t column_field_cnt, const ColumnsFieldIArray *column_fields);

  int init_row_for_arraybinding(ObIAllocator &alloc, int64_t array_binding_row_num);
  int check_param_type_for_arraybinding(
    sql::ObSQLSessionInfo *session_info, sql::ParamTypeInfoArray &param_type_infos);
  int check_param_value_for_arraybinding(ObObjParam &param);
  int construct_execute_param_for_arraybinding(int64_t pos);
  void reset_complex_param_memory(ParamStore *params, sql::ObSQLSessionInfo &session_info);
  int save_exception_for_arraybinding(
    int64_t pos, int error_code, ObIArray<ObSavedException> &exception_array);
  //int after_do_process_for_arraybinding(ObMySQLResultSet &result);
  int response_result_for_arraybinding(
      sql::ObSQLSessionInfo &session_info, ObIArray<ObSavedException> &exception_array);
  int send_eof_packet_for_arraybinding(sql::ObSQLSessionInfo &session_info);

  int do_process_single(sql::ObSQLSessionInfo &session,
                        ParamStore *param_store,
                        const bool has_more_result,
                        const bool force_sync_resp,
                        bool &async_resp_used);
  int do_process(sql::ObSQLSessionInfo &session,
                 ParamStore *param_store,
                 const bool has_more_result,
                 const bool force_sync_resp,
                 bool &async_resp_used);
  int process_retry(sql::ObSQLSessionInfo &session,
                    ParamStore *param_store,
                    bool has_more_result,
                    bool force_sync_resp,
                    bool &async_resp_used);

  int process_execute_stmt(const sql::ObMultiStmtItem &multi_stmt_item,
                           sql::ObSQLSessionInfo &session,
                           bool has_more_result,
                           bool fore_sync_resp,
                           bool &async_resp_used);

  int try_batch_multi_stmt_optimization(sql::ObSQLSessionInfo &session,
                                        bool has_more_result,
                                        bool force_sync_resp,
                                        bool &async_resp_used,
                                        bool &optimization_done);

  int is_arraybinding_returning(sql::ObSQLSessionInfo &session, bool &is_ab_return);


  //
  // %charset is current charset of data, %cs_type and %ncs_type is destination collation.
  //
  // %charset: connection charset
  // %cs_type: collation for char/varchar type
  // %ncs_type: collation for nchar/nvarcahr type
  //
  // in mysql: %charset is charset of %cs_type, %ncs_type is not used
  // in oracle: %cs_type is server collation whose charset may differ with %charset
  int parse_param_value(ObIAllocator& allocator,
                        const uint32_t type,
                        const ObCharsetType charset,
                        const ObCharsetType ncharset,
                        const ObCollationType cs_type,
                        const ObCollationType ncs_type,
                        const char *&data,
                        const common::ObTimeZoneInfo *tz_info,
                        sql::TypeInfo *type_info,
                        ObObjParam &param,
                        const char *bitmap,
                        int64_t param_id);
  int parse_complex_param_value(ObIAllocator &allocator,
                        const ObCharsetType charset,
                        const ObCollationType cs_type,
                        const ObCollationType ncs_type,
                        const char *&data,
                        const common::ObTimeZoneInfo *tz_info,
                        sql::TypeInfo *type_info,
                        ObObjParam &param);
  int decode_type_info(const char*& buf, sql::TypeInfo &type_info);
  int get_udt_by_name(ObString relation_name,
                        ObString type_name,
                        const share::schema::ObUDTTypeInfo *&udt_info);
  int get_package_type_by_name(ObIAllocator &allocator,
                        const sql::TypeInfo *type_info,
                        const pl::ObUserDefinedType *&pl_type);
  int get_pl_type_by_type_info(ObIAllocator &allocator,
                        const sql::TypeInfo *type_info,
                        const pl::ObUserDefinedType *&pl_type);
  bool is_contain_complex_element(const sql::ParamTypeArray &param_types) const;

  virtual int before_process();
  int response_query_header(sql::ObSQLSessionInfo &session, pl::ObDbmsCursorInfo &cursor);
  //重载response，在response中不去调用flush_buffer(true)；flush_buffer(true)在需要回包时显示调用


  // copy or convert string, resove %extra_buf_len before result string.
  static int copy_or_convert_str(common::ObIAllocator &allocator,
                                 const common::ObCollationType src_type,
                                 const common::ObCollationType dst_type,
                                 const common::ObString &src,
                                 common::ObString &out,
                                 int64_t extra_buf_len = 0);
protected:
  ObQueryRetryCtrl retry_ctrl_;
  sql::ObSqlCtx ctx_;
  int64_t stmt_id_;
  sql::stmt::StmtType stmt_type_;
  ParamStore *params_;

  ParamStore *arraybinding_params_;
  ColumnsFieldArray *arraybinding_columns_;
  ObNewRow *arraybinding_row_;

  bool is_arraybinding_;
  bool is_save_exception_;
  int64_t arraybinding_size_;
  int64_t arraybinding_rowcnt_;

  ObPSCursorType ps_cursor_type_;   // cursor read only 类型的语句

  int64_t single_process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  bool prepare_packet_sent_;
  uint64_t params_num_; 
  int64_t params_value_len_;
  char *params_value_;
  int64_t curr_sql_idx_; // only for arraybinding
  ObPSAnalysisChecker analysis_checker_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMPStmtExecute);

}; //end of class

} //end of namespace observer
} //end of namespace oceanbase

#endif //OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_EXECUTE_H__
