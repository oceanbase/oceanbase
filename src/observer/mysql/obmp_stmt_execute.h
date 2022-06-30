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

namespace oceanbase {
namespace observer {

struct ObSavedException {
  int pos_;
  int32_t error_code_;
  common::ObString error_msg_;

  TO_STRING_KV(K(pos_), K(error_code_), K(error_msg_));
};

typedef common::Ob2DArray<common::ObObjParam, common::OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator, false> ParamStore;

class ObMPStmtExecute : public ObMPBase, public ObIMPPacketSender {
public:
  static const obmysql::ObMySQLCmd COM = obmysql::OB_MYSQL_COM_STMT_EXECUTE;

  explicit ObMPStmtExecute(const ObGlobalContext& gctx);
  virtual ~ObMPStmtExecute()
  {}

  // Parse basic param value, no MYSQL_TYPE_COMPLEX or MYSQL_TYPE_CURSOR.
  // see parse_param_value()
  static int parse_basic_param_value(ObIAllocator &allocator, const uint32_t type, const ObCharsetType charset,
      const ObCollationType cs_type, const ObCollationType ncs_type, const char *&data,
      const common::ObTimeZoneInfo *tz_info, ObObj &param);
  static int parse_mysql_timestamp_value(const obmysql::EMySQLFieldType field_type, const char *&data, ObObj &param,
      const common::ObTimeZoneInfo *tz_info);
  static int parse_oracle_timestamp_value(
      const obmysql::EMySQLFieldType field_type, const char*& data, const ObTimeConvertCtx& cvrt_ctx, ObObj& param);
  static int parse_mysql_time_value(const char*& data, ObObj& param);
  static int parse_oracle_interval_ym_value(const char*& data, ObObj& param);
  static int parse_oracle_interval_ds_value(const char*& data, ObObj& param);
  int64_t get_single_process_timestamp() const
  {
    return single_process_timestamp_;
  }
  int64_t get_exec_start_timestamp() const
  {
    return exec_start_timestamp_;
  }
  int64_t get_exec_end_timestamp() const
  {
    return exec_end_timestamp_;
  }
  int64_t get_send_timestamp() const
  {
    return get_receive_timestamp();
  }
  virtual int flush_buffer(const bool is_last) override
  {
    return ObMPBase::flush_buffer(is_last);
  }
  inline bool support_send_long_data(const uint32_t type)
  {
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
        is_support = true;
        break;
      case obmysql::MYSQL_TYPE_COMPLEX:
        is_support = share::is_oracle_mode() ? true : false;
        break;
      default:
        is_support = false;
    }
    return is_support;
  }
  inline int32_t get_param_num()
  {
    return params_num_;
  }
  inline void set_param_num(int32_t num)
  {
    params_num_ = num;
  }

protected:
  virtual int deserialize() override
  {
    return common::OB_SUCCESS;
  }
  virtual int process() override;
  virtual void disconnect() override
  {
    ObMPBase::disconnect();
  }
  virtual void update_last_pkt_pos() override
  {
    if (NULL != ez_buf_) {
      comp_context_.update_last_pkt_pos(ez_buf_->last);
    }
  }
  virtual int send_error_packet(int err, const char* errmsg, bool is_partition_hit = true, void* extra_err_info = NULL) override
  {
    return ObMPBase::send_error_packet(err, errmsg, is_partition_hit, extra_err_info);
  }
  virtual int send_ok_packet(sql::ObSQLSessionInfo& session, ObOKPParam& ok_param) override
  {
    return ObMPBase::send_ok_packet(session, ok_param);
  }
  virtual int send_eof_packet(const sql::ObSQLSessionInfo& session, const ObMySQLResultSet& result) override
  {
    return ObMPBase::send_eof_packet(session, result);
  }
  virtual bool need_send_extra_ok_packet() override
  {
    return OB_NOT_NULL(get_conn()) && get_conn()->need_send_extra_ok_packet();
  }
  virtual int response_packet(obmysql::ObMySQLPacket& pkt) override
  {
    return ObMPBase::response_packet(pkt);
  }
  virtual int after_process() override
  {
    return ObMPBase::after_process();
  }

private:
  // for arraybinding
  int init_field_for_arraybinding();
  int init_row_for_arraybinding(ObIAllocator& alloc);
  int init_for_arraybinding(ObIAllocator& alloc);
  int check_param_type_for_arraybinding(sql::ObSQLSessionInfo* session_info, sql::ParamTypeInfoArray& param_type_infos);
  int check_param_value_for_arraybinding(ObObjParam& param);
  int construct_execute_param_for_arraybinding(int64_t pos);
  void reset_collection_param_for_arraybinding();
  int save_exception_for_arraybinding(int64_t pos, int error_code, ObIArray<ObSavedException>& exception_array);
  int after_do_process_for_arraybinding(ObMySQLResultSet& result);
  int response_result_for_arraybinding(
      sql::ObSQLSessionInfo& session_info, ObIArray<ObSavedException>& exception_array);
  int send_eof_packet_for_arraybinding(sql::ObSQLSessionInfo& session_info);

  int do_process_single(
      sql::ObSQLSessionInfo& session, const bool has_more_result, const bool force_sync_resp, bool& async_resp_used);
  int do_process(
      sql::ObSQLSessionInfo& session, const bool has_more_result, const bool force_sync_resp, bool& async_resp_used);
  int set_session_active(sql::ObSQLSessionInfo& session) const;

  int process_execute_stmt(const sql::ObMultiStmtItem& multi_stmt_item, sql::ObSQLSessionInfo& session,
      bool has_more_result, bool fore_sync_resp, bool& async_resp_used);
  int response_result(
      ObMySQLResultSet& result, sql::ObSQLSessionInfo& session, bool force_sync_resp, bool& async_resp_used);

  //
  // %charset is current charset of data, %cs_type and %ncs_type is destination collation.
  //
  // %charset: connection charset
  // %cs_type: collation for char/varchar type
  // %ncs_type: collation for nchar/nvarcahr type
  //
  // in mysql: %charset is charset of %cs_type, %ncs_type is not used
  // in oracle: %cs_type is server collation whose charset may differ with %charset
  int parse_param_value(ObIAllocator& allocator, const uint32_t type, const ObCharsetType charset,
      const ObCollationType cs_type, const ObCollationType ncs_type, const char*& data,
      const common::ObTimeZoneInfo* tz_info, sql::TypeInfo* type_info, sql::TypeInfo* dst_type_info, 
      ObObjParam& param, int16_t param_id);
  int decode_type_info(const char*& buf, sql::TypeInfo& type_info);

  virtual int before_response() override
  {
    return OB_SUCCESS;
  }
  virtual int before_process() override;
  void record_stat(const sql::stmt::StmtType type, const int64_t end_time) const;

  // copy or convert string, resove %extra_buf_len before result string.
  static int copy_or_convert_str(common::ObIAllocator& allocator, const common::ObCollationType src_type,
      const common::ObCollationType dst_type, const common::ObString& src, common::ObString& out,
      int64_t extra_buf_len = 0);

private:
  ObQueryRetryCtrl retry_ctrl_;
  sql::ObSqlCtx ctx_;
  int64_t stmt_id_;
  sql::stmt::StmtType stmt_type_;
  ParamStore* params_;

  ParamStore* arraybinding_params_;
  ColumnsFieldArray* arraybinding_columns_;
  ObNewRow* arraybinding_row_;

  bool is_arraybinding_;
  bool is_save_exception_;
  int64_t arraybinding_size_;
  int64_t arraybinding_rowcnt_;

  bool is_cursor_readonly_;  // cursor read only

  int64_t single_process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  uint64_t params_num_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPStmtExecute);

};  // end of class

}  // end of namespace observer
}  // end of namespace oceanbase

#endif  // OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_EXECUTE_H__
