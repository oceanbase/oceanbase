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

#ifndef _OBMP_QUERY_H_
#define _OBMP_QUERY_H_

#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/ob_sql_context.h"
#include "observer/mysql/obmp_base.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "observer/mysql/ob_mysql_request_manager.h"
namespace oceanbase {
namespace sql {
class ObMonitorInfoManager;
class ObPhyPlanMonitorInfo;
class ObQueryExecCtx;
class ObMPParseStat;
}  // namespace sql
namespace share {
namespace schema {
class ObTableSchema;
}
class ObPartitionLocation;
class ObFBPartitionParam;
}  // namespace share
namespace observer {
class ObMPQuery : public ObMPBase, public ObIMPPacketSender {
public:
  static const obmysql::ObMySQLCmd COM = obmysql::OB_MYSQL_COM_QUERY;

public:
  explicit ObMPQuery(const ObGlobalContext& gctx);
  virtual ~ObMPQuery();

public:
  virtual void disconnect();
  virtual void update_last_pkt_pos()
  {
    if (NULL != ez_buf_) {
      comp_context_.update_last_pkt_pos(ez_buf_->last);
    }
  }
  virtual int response_packet(obmysql::ObMySQLPacket& pkt)
  {
    return ObMPBase::response_packet(pkt);
  }
  virtual int flush_buffer(const bool is_last) override
  {
    return ObMPBase::flush_buffer(is_last);
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
  virtual bool need_send_extra_ok_packet()
  {
    return OB_NOT_NULL(get_conn()) && get_conn()->need_send_extra_ok_packet();
  }

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
  void set_is_com_filed_list()
  {
    is_com_filed_list_ = true;
  }
  bool get_is_com_filed_list() const
  {
    return is_com_filed_list_;
  }

protected:
  int process();
  int deserialize();
  int check_readonly_stmt(ObMySQLResultSet& result);
  int is_readonly_stmt(ObMySQLResultSet& result, bool& is_readonly);
  virtual int after_process();

private:
  int register_callback_with_async(ObQueryExecCtx& query_ctx);
  int response_result(ObQueryExecCtx& query_ctx, bool force_sync_resp, bool& async_resp_used);
  int get_tenant_schema_info_(const uint64_t tenant_id, ObTenantCachedSchemaGuardInfo* cache_info,
      share::schema::ObSchemaGetterGuard*& schema_guard, int64_t& tenant_version, int64_t& sys_version);
  int do_process(sql::ObSQLSessionInfo& session, bool has_more_result, bool force_sync_resp, bool& async_resp_used,
      bool& need_disconnect);
  int process_single_stmt(const sql::ObMultiStmtItem& multi_stmt_item, sql::ObSQLSessionInfo& session,
      bool has_more_result, bool force_sync_resp, bool& async_resp_used, bool& need_disconnect);

  virtual int before_response()
  {
    return OB_SUCCESS;
  }

  void record_stat(const sql::stmt::StmtType type, const int64_t end_time) const;
  void update_audit_info(const ObWaitEventStat& total_wait_desc, ObAuditRecordData& record);
  int fill_feedback_session_info(ObMySQLResultSet& result, sql::ObSQLSessionInfo& session);
  int build_fb_partition_param(const share::schema::ObTableSchema& table_schema,
      const share::ObPartitionLocation& partition_loc, share::ObFBPartitionParam& param);
  int try_batched_multi_stmt_optimization(sql::ObSQLSessionInfo& session, common::ObIArray<ObString>& queries,
      const ObMPParseStat& parse_stat, bool& optimization_done, bool& async_resp_used, bool& need_disconnect);
  int deserialize_com_field_list();

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPQuery);

private:
  sql::ObSqlCtx ctx_;
  ObQueryRetryCtrl retry_ctrl_;
  common::ObString sql_;
  char sql_buf_[OB_MAX_SQL_LENGTH + 2];  // for audit
  int64_t sql_len_;
  int64_t single_process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  bool is_com_filed_list_;
  common::ObString wild_str_;  // used to save wildware string in OB_MYSQL_COM_FIELD_LIST
};                             // end of class ObMPQuery
}  // end of namespace observer
}  // end of namespace oceanbase
#endif /* _OBMP_QUERY_H_ */
