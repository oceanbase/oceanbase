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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_PREPARE_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_PREPARE_H_

#include "sql/ob_sql_context.h"
#include "observer/mysql/obmp_base.h"
#include "observer/mysql/ob_query_retry_ctrl.h"

namespace oceanbase {
namespace sql {
class ObMultiStmtItem;
}
namespace observer {
struct ObGlobalContext;

class ObMPStmtPrepare : public ObMPBase, public ObIMPPacketSender {
public:
  static const obmysql::ObMySQLCmd COM = obmysql::OB_MYSQL_COM_STMT_PREPARE;

  explicit ObMPStmtPrepare(const ObGlobalContext& gctx);
  virtual ~ObMPStmtPrepare()
  {}
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
  void set_proxy_version(uint64_t v)
  {
    proxy_version_ = v;
  }
  uint64_t get_proxy_version()
  {
    return proxy_version_;
  }

protected:
  virtual int deserialize();
  virtual int before_process() override;
  virtual int process();

  virtual void disconnect()
  {
    ObMPBase::disconnect();
  }
  virtual void update_last_pkt_pos()
  {
    if (NULL != ez_buf_) {
      comp_context_.update_last_pkt_pos(ez_buf_->last);
    }
  }
  virtual int send_error_packet(int err, const char* errmsg, bool is_partition_hit = true, void* extra_err_info = NULL)
  {
    return ObMPBase::send_error_packet(err, errmsg, is_partition_hit, extra_err_info);
  }
  virtual int send_ok_packet(sql::ObSQLSessionInfo& session, ObOKPParam& ok_param)
  {
    return ObMPBase::send_ok_packet(session, ok_param);
  }
  virtual int send_eof_packet(const sql::ObSQLSessionInfo& session, const ObMySQLResultSet& result)
  {
    return ObMPBase::send_eof_packet(session, result);
  }
  virtual int response_packet(obmysql::ObMySQLPacket& pkt)
  {
    return ObMPBase::response_packet(pkt);
  }
  virtual bool need_send_extra_ok_packet()
  {
    return OB_NOT_NULL(get_conn()) && get_conn()->need_send_extra_ok_packet();
  }

private:
  int do_process(
      sql::ObSQLSessionInfo& session, const bool has_more_result, const bool force_sync_resp, bool& async_resp_used);
  int process_prepare_stmt(const sql::ObMultiStmtItem& multi_stmt_item, sql::ObSQLSessionInfo& session,
      bool has_more_result, bool fore_sync_resp, bool& async_resp_used);
  int check_and_refresh_schema(uint64_t login_tenant_id, uint64_t effective_tenant_id);
  int response_result(
      ObMySQLResultSet& result, sql::ObSQLSessionInfo& session, bool force_sync_resp, bool& async_resp_used);

  int send_prepare_packet(const ObMySQLResultSet& result);
  int send_column_packet(const sql::ObSQLSessionInfo& session, ObMySQLResultSet& result);
  int send_param_packet(const sql::ObSQLSessionInfo& session, ObMySQLResultSet& result);

private:
  ObQueryRetryCtrl retry_ctrl_;
  sql::ObSqlCtx ctx_;
  common::ObString sql_;
  int64_t sql_len_;
  int64_t single_process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  uint64_t proxy_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPStmtPrepare);

};  // end of class ObMPStmtPrepare

}  // end of namespace observer
}  // end of namespace oceanbase

#endif  // OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_PREPARE_H_
