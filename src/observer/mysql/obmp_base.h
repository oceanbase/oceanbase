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
#include "rpc/frame/ob_sql_processor.h"
#include "rpc/obmysql/ob_2_0_protocol_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "observer/mysql/obmp_packet_sender.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/alloc/ob_malloc_callback.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace observer
{

class ObMPBase : public rpc::frame::ObSqlProcessor, public ObIMPPacketSender
{
public:
  explicit ObMPBase(const ObGlobalContext &gctx);
  virtual ~ObMPBase();

  int64_t get_process_timestamp() const { return process_timestamp_; };
  inline void set_proxy_version(uint64_t v) { proxy_version_ = v; }
  inline uint64_t get_proxy_version() { return proxy_version_; }
protected:
  virtual void cleanup() final; // please don't overload cleanup in child class, mark as final
  virtual int setup_packet_sender();
  virtual int before_process();
  virtual int response(const int retcode) final;
  virtual int after_process(int error_code);
  int load_system_variables(const share::schema::ObSysVariableSchema &sys_variable_schema,
                            sql::ObSQLSessionInfo &session) const;
  void disconnect();
  void force_disconnect();

  bool is_conn_valid() const { return packet_sender_.is_conn_valid(); }

  virtual int update_last_pkt_pos() { return packet_sender_.update_last_pkt_pos(); }
  virtual bool need_send_extra_ok_packet() { return packet_sender_.need_send_extra_ok_packet(); }

  virtual int read_packet(obmysql::ObICSMemPool& mem_pool, obmysql::ObMySQLPacket*& pkt) override;
  virtual int release_packet(obmysql::ObMySQLPacket* pkt) override;


  // Response a packet to client peer.
  //
  // It'll wake up responding IO thread and then wait for that thread
  // write the data back to this fd. After data has been sent out, IO
  // thread will wake up this thread in turn.
  //
  OB_INLINE int response_packet(obmysql::ObMySQLPacket &pkt, sql::ObSQLSessionInfo* session)
  {
    return packet_sender_.response_packet(pkt, session);
  }

  /// send a error packet to client
  int send_error_packet(int err,
                        const char* errmsg,
                        bool is_partition_hit = true,
                        void *extra_err_info = NULL);

  // send a ok packet to client
  int send_ok_packet(sql::ObSQLSessionInfo &session, ObOKPParam &ok_param, obmysql::ObMySQLPacket* pkt=NULL);
  int send_eof_packet(const sql::ObSQLSessionInfo &session, const ObMySQLResultSet &result, ObOKPParam *ok_param = NULL);

  int send_null_packet(sql::ObSQLSessionInfo &session, ObOKPParam &ok_param);

  int send_switch_packet(common::ObString &auth_name, common::ObString& auth_data);
  ObSMConnection *get_conn() const;
  int get_conn_id(uint32_t &sessid) const;
  int create_session(ObSMConnection *conn, sql::ObSQLSessionInfo *&sess_info);
  int free_session();
  int get_session(sql::ObSQLSessionInfo *&sess_info);
  int revert_session(sql::ObSQLSessionInfo *sess_info);
  int flush_buffer(const bool is_last);
  int clean_buffer();
  int init_process_var(sql::ObSqlCtx &ctx,
                       const sql::ObMultiStmtItem &multi_stmt_item,
                       sql::ObSQLSessionInfo &session) const;
  int do_after_process(sql::ObSQLSessionInfo &session,
                       sql::ObSqlCtx &ctx,
                       bool async_resp_used) const;
  int record_flt_trace(sql::ObSQLSessionInfo &session) const;
  // reset warning buffer err msg, for inner retry
  void setup_wb(sql::ObSQLSessionInfo &session);
  void clear_wb_content(sql::ObSQLSessionInfo &session);
  int set_session_active(const ObString &sql,
                         sql::ObSQLSessionInfo &session,
                         const int64_t last_active_time_ts,
                         obmysql::ObMySQLCmd cmd = obmysql::ObMySQLCmd::COM_QUERY) const
  {
    return session.set_session_active(sql, get_receive_timestamp(), last_active_time_ts, cmd);
  }
  int check_and_refresh_schema(uint64_t login_tenant_id,
                               uint64_t effective_tenant_id,
                               sql::ObSQLSessionInfo *session = NULL);
  bool need_flush_buffer() const;
  int update_transmission_checksum_flag(const sql::ObSQLSessionInfo &session);
  int update_proxy_sys_vars(sql::ObSQLSessionInfo &session);
  int update_charset_sys_vars(ObSMConnection &conn, sql::ObSQLSessionInfo &sess_info);

  int build_encode_param_(obmysql::ObProtoEncodeParam &param,
                          obmysql::ObMySQLPacket *pkt, const bool is_last);
  // 计算并设置当前用户所属 cgroup，用于资源隔离。如未设置，默认 cgroup id 为 0
  int setup_user_resource_group(
      ObSMConnection &conn,
      const uint64_t tenant_id,
      sql::ObSQLSessionInfo *session);
  int response_row(sql::ObSQLSessionInfo &session,
                   common::ObNewRow &row,
                   const ColumnsFieldIArray *fields,
                   bool is_packed,
                   sql::ObExecContext *exec_ctx = NULL,
                   bool is_ps_protocol = true,
                   ObSchemaGetterGuard *schema_guard = NULL);
  int process_extra_info(sql::ObSQLSessionInfo &session, const obmysql::ObMySQLRawPacket &pkt,
                                bool &need_response_error);
  int process_kill_client_session(sql::ObSQLSessionInfo &session, bool is_connect = false);
  int load_privilege_info_for_change_user(sql::ObSQLSessionInfo *session);
protected:
  static const int64_t MAX_TRY_STEPS = 5;
  static int64_t TRY_EZ_BUF_SIZES[MAX_TRY_STEPS];

  const ObGlobalContext &gctx_;
  ObMPPacketSender packet_sender_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMPBase);
  int64_t process_timestamp_;
  uint64_t proxy_version_; // 控制prepare statement返回stmt id的策略
}; // end of class ObMPBase

inline void ObMPBase::setup_wb(sql::ObSQLSessionInfo &session)
{
  // just in case, not really need to call this
  session.reset_warnings_buf();
  // 将当前session的wb设置到线程局部，以便于代码中随处可以写warning进wb
  ob_setup_tsi_warning_buffer(&session.get_warnings_buffer());
}

inline void ObMPBase::clear_wb_content(sql::ObSQLSessionInfo &session)
{
  session.reset_warnings_buf();
}

class ObProcessMallocCallback final : public lib::ObMallocCallback
{
public:
  ObProcessMallocCallback(int64_t cur_used, int64_t &max_used)
    : cur_used_(cur_used), max_used_(max_used) {
      max_used_ = cur_used_ > max_used_ ? cur_used_ : max_used_;
  }
  virtual ~ObProcessMallocCallback() {}

  virtual void operator()(const ObMemAttr &attr, int64_t add_size) override
  {
    //You can use:
    //alter system set_tp tp_no=405, error_code=label_high64, frequency=1;
    //alter system set_tp tp_no=406, error_code=label_low64, frequency=1;
    //to inject a monitored ObLabel.
    //When this injection takes effect,
    //the maximum memory usage will only be counted for the specified label.
    //tp_no=405 and tp_no=406 need to be used at the same time

    //To obtain the label_high64 and label_low64 values of an ObLabel,
    //you can use the tool './label2int64 LabelName' to easily retrieve them.
    //If you don't have access to this tool,
    //you can map a string that conforms to the ObLabel format into two int64_t integer values,
    //ensuring consistency with the endianness of the target machine.
    int64_t label_high64 = - EVENT_CODE(EventTable::EN_SQL_MEMORY_LABEL_HIGH64);
    if (OB_UNLIKELY(ObLabel("SqlDtlBuf") == attr.label_
                    || ObCtxIds::MEMSTORE_CTX_ID == attr.ctx_id_)) {
      // do nothing
    } else if (label_high64 != 0) {
      int64_t label_low64 = - EVENT_CODE(EventTable::EN_SQL_MEMORY_LABEL_LOW64);
      char trace_label[16] = {'\0'};
      MEMSET(trace_label, 0, sizeof(trace_label));
      MEMCPY(trace_label, &label_high64, sizeof(int64_t));
      MEMCPY(trace_label + 8, &label_low64, sizeof(int64_t));
      if (ObLabel(trace_label) == attr.label_) {
        cur_used_ += add_size;
        max_used_ = cur_used_ > max_used_ ? cur_used_ : max_used_;
#ifdef ERRSIM
        int64_t dynamic_leak_size = - EVENT_CODE(EventTable::EN_SQL_MEMORY_DYNAMIC_LEAK_SIZE);
        if (dynamic_leak_size > 0 && max_used_ >= dynamic_leak_size) {
          abort();
        }
#endif //end of ERRSIM
      }
    } else {
      cur_used_ += add_size;
      max_used_ = cur_used_ > max_used_ ? cur_used_ : max_used_;
    }
  }
private:
  int64_t cur_used_;
  int64_t &max_used_;
}; // end of class ObProcessMallocCallback

} // end of namespace observer
} // end of namespace oceanbsae

#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_BASE_H_
