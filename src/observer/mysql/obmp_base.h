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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_BASE_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_BASE_H_

#include "io/easy_io.h"
#include "rpc/frame/ob_req_processor.h"
#include "rpc/obmysql/ob_2_0_protocol_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "observer/mysql/obsm_struct.h"

namespace oceanbase {

namespace obmysql {
class ObMySQLPacket;
}  // end of namespace obmysql

namespace share {
class ObFeedbackRerouteInfo;
}
namespace observer {

class ObSMConnection;

struct ObOKPParam {
  ObOKPParam()
      : affected_rows_(0),
        message_(NULL),
        is_on_connect_(false),
        is_partition_hit_(true),
        has_more_result_(false),
        take_trace_id_to_client_(false),
        warnings_count_(0),
        lii_(0),
        reroute_info_(NULL)
  {}
  void reset();
  int64_t to_string(char* buf, const int64_t buf_len) const;
  uint64_t affected_rows_;
  char* message_;
  bool is_on_connect_;
  bool is_partition_hit_;
  bool has_more_result_;
  // current, when return error packet or query fly time >= trace_log_slow_query_watermark(100ms default),
  // will take trace id to client for easy debugging;
  bool take_trace_id_to_client_;
  uint16_t warnings_count_;
  // According to mysql behavior, when last_insert_id was changed by insert,
  // update or replace, no matter auto generated or manual specified, mysql
  // should send last_insert_id through ok packet to mysql client.
  // And if last_insert_id was changed by set @@last_insert_id=xxx, mysql
  // will not send changed last_insert_id to client.
  //
  // if last insert id has changed, send lii to client;
  // Attention, in this case, lii is in ok packet's last insert id field
  // (encode by int<lenenc>), not in session track field;
  uint64_t lii_;

  share::ObFeedbackRerouteInfo* reroute_info_;
};

class ObIMPPacketSender {
public:
  virtual ~ObIMPPacketSender()
  {}
  virtual void disconnect() = 0;
  virtual void update_last_pkt_pos() = 0;
  virtual int response_packet(obmysql::ObMySQLPacket& pkt) = 0;
  virtual int send_error_packet(
      int err, const char* errmsg, bool is_partition_hit = true, void* extra_err_info = NULL) = 0;
  virtual int send_ok_packet(sql::ObSQLSessionInfo& session, ObOKPParam& ok_param) = 0;
  virtual int send_eof_packet(const sql::ObSQLSessionInfo& session, const ObMySQLResultSet& result) = 0;
  virtual bool need_send_extra_ok_packet() = 0;
  virtual int flush_buffer(const bool is_last) = 0;
};

class ObMPPacketSender : public ObIMPPacketSender {
public:
  ObMPPacketSender();
  virtual ~ObMPPacketSender();

  virtual void disconnect() override;
  virtual void update_last_pkt_pos() override
  {
    if (NULL != ez_buf_) {
      comp_context_.update_last_pkt_pos(ez_buf_->last);
    }
  }
  virtual int response_packet(obmysql::ObMySQLPacket& pkt) override;
  virtual int send_error_packet(
      int err, const char* errmsg, bool is_partition_hit = true, void* extra_err_info = NULL) override;
  virtual int send_ok_packet(sql::ObSQLSessionInfo& session, ObOKPParam& ok_param) override;
  virtual int send_eof_packet(const sql::ObSQLSessionInfo& session, const ObMySQLResultSet& result) override;
  virtual bool need_send_extra_ok_packet() override
  {
    return OB_NOT_NULL(get_conn()) && get_conn()->need_send_extra_ok_packet();
  }
  virtual int flush_buffer(const bool is_last);
  int init(rpc::ObRequest* req, sql::ObSQLSessionInfo* sess_info, uint8_t packet_seq, bool conn_status,
      bool req_has_wokenup, int64_t query_receive_ts, bool io_thread_mark);

private:
  static const int64_t MAX_TRY_STEPS = 5;
  static int64_t TRY_EZ_BUF_SIZES[MAX_TRY_STEPS];

  int alloc_ezbuf();
  ObSMConnection* get_conn() const;
  int try_encode_with(obmysql::ObMySQLPacket& pkt, int64_t current_size, int64_t& seri_size, int64_t try_steps);
  int build_encode_param(obmysql::ObProtoEncodeParam& param, obmysql::ObMySQLPacket* pkt, const bool is_last);
  bool need_flush_buffer() const;
  int resize_ezbuf(const int64_t size);
  int get_conn_id(uint32_t& sessid) const;
  int get_session(sql::ObSQLSessionInfo*& sess_info);
  int revert_session(sql::ObSQLSessionInfo* sess_info);

protected:
  rpc::ObRequest* req_;
  sql::ObSQLSessionInfo* sess_info_;
  uint8_t seq_;
  obmysql::ObCompressionContext comp_context_;
  obmysql::ObProto20Context proto20_context_;
  easy_buf_t* ez_buf_;
  bool conn_valid_;
  uint32_t sessid_;
  bool req_has_wokenup_;
  int64_t query_receive_ts_;
  bool io_thread_mark_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPPacketSender);
};

class ObMPBase : public rpc::frame::ObReqProcessor {
public:
  explicit ObMPBase(const ObGlobalContext& gctx);
  virtual ~ObMPBase();

  int64_t get_process_timestamp() const
  {
    return process_timestamp_;
  };

protected:
  virtual int serialize();
  virtual int before_process();
  virtual int before_response();
  virtual int response(const int retcode) final;
  virtual int after_process();
  int load_system_variables(
      const share::schema::ObSysVariableSchema& sys_variable_schema, sql::ObSQLSessionInfo& session) const;
  void disconnect();

  // Response a packet to client peer.
  //
  // It'll wake up responding IO thread and then wait for that thread
  // write the data back to this fd. After data has been sent out, IO
  // thread will wake up this thread in turn.
  //
  int response_packet(obmysql::ObMySQLPacket& pkt);

  /// send a error packet to client
  int send_error_packet(int err, const char* errmsg, bool is_partition_hit = true, void* extra_err_info = NULL);

  // send a ok packet to client
  int send_ok_packet(sql::ObSQLSessionInfo& session, ObOKPParam& ok_param);
  int send_eof_packet(const sql::ObSQLSessionInfo& session, const ObMySQLResultSet& result);

  int send_null_packet(sql::ObSQLSessionInfo& session, ObOKPParam& ok_param);

  int send_switch_packet(common::ObString& auth_name, common::ObString& auth_data);
  ObSMConnection* get_conn() const;
  int get_conn_id(uint32_t& sessid) const;
  int create_session(ObSMConnection* conn, sql::ObSQLSessionInfo*& sess_info);
  int free_session();
  int get_session(sql::ObSQLSessionInfo*& sess_info);
  int revert_session(sql::ObSQLSessionInfo* sess_info);
  int try_encode_with(obmysql::ObMySQLPacket& pkt, int64_t current_size, int64_t& seri_size, int64_t try_steps);
  int resize_ezbuf(const int64_t size);
  int flush_buffer(const bool is_last);
  int init_process_var(sql::ObSqlCtx& ctx, const sql::ObMultiStmtItem& multi_stmt_item, sql::ObSQLSessionInfo& session,
      ObVirtualTableIteratorFactory& vt_iter_fty, bool& use_trace_log) const;
  int do_after_process(
      sql::ObSQLSessionInfo& session, bool use_trace_log, sql::ObSqlCtx& ctx, bool async_resp_used) const;
  // reset warning buffer err msg, for inner retry
  void setup_wb(sql::ObSQLSessionInfo& session);
  void clear_wb_content(sql::ObSQLSessionInfo& session);
  int set_session_active(const ObString& sql, sql::ObSQLSessionInfo& session, const int64_t last_active_time_ts,
      obmysql::ObMySQLCmd cmd = obmysql::ObMySQLCmd::OB_MYSQL_COM_QUERY) const;
  int check_and_refresh_schema(
      uint64_t login_tenant_id, uint64_t effective_tenant_id, sql::ObSQLSessionInfo* session = NULL);
  bool need_flush_buffer() const;
  int update_transmission_checksum_flag(const sql::ObSQLSessionInfo& session);
  int update_proxy_sys_vars(sql::ObSQLSessionInfo& session);

  int build_encode_param(obmysql::ObProtoEncodeParam& param, obmysql::ObMySQLPacket* pkt, const bool is_last);
  // Calculate and set the cgroup that the current user belongs to for resource isolation.
  // If not set, the default cgroup id is 0
  int setup_user_resource_group(ObSMConnection& conn, const uint64_t tenant_id, const uint64_t user_id);

protected:
  static const int64_t MAX_TRY_STEPS = 5;
  static int64_t TRY_EZ_BUF_SIZES[MAX_TRY_STEPS];

  const ObGlobalContext& gctx_;
  uint8_t seq_;
  obmysql::ObCompressionContext comp_context_;
  obmysql::ObProto20Context proto20_context_;

  // easy buffer used to cache mysql response packets to client
  easy_buf_t* ez_buf_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPBase);
  int64_t process_timestamp_;
};  // end of class ObMPBase

inline void ObMPBase::setup_wb(sql::ObSQLSessionInfo& session)
{
  // just in case, not really need to call this
  session.reset_warnings_buf();
  // Set the wb of the current session to the thread var,
  // so that you can write warnings into wb everywhere in the code
  ob_setup_tsi_warning_buffer(&session.get_warnings_buffer());
}

inline void ObMPBase::clear_wb_content(sql::ObSQLSessionInfo& session)
{
  session.reset_warnings_buf();
}
}  // end of namespace observer
}  // namespace oceanbase

#endif  // OCEANBASE_OBSERVER_MYSQL_OBMP_BASE_H_
