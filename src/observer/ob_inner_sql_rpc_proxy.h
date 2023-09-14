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

#ifndef OBDEV_SRC_OBSERVER_INNER_SQL_RPC_PROXY_H_
#define OBDEV_SRC_OBSERVER_INNER_SQL_RPC_PROXY_H_
#include "lib/container/ob_array.h"
#include "share/ob_define.h"
#include "share/ob_scanner.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "sql/resolver/ob_stmt_type.h"

namespace oceanbase
{
namespace obrpc
{

class ObInnerSQLTransmitArg
{
  OB_UNIS_VERSION(1);
public:

  enum InnerSQLOperationType
  {
    OPERATION_TYPE_INVALID = 0,
    OPERATION_TYPE_START_TRANSACTION = 1,
    OPERATION_TYPE_ROLLBACK = 2,
    OPERATION_TYPE_COMMIT = 3,
    OPERATION_TYPE_EXECUTE_READ = 4,
    OPERATION_TYPE_EXECUTE_WRITE = 5,
    OPERATION_TYPE_REGISTER_MDS = 6,
    OPERATION_TYPE_LOCK_TABLE = 7,
    OPERATION_TYPE_LOCK_TABLET = 8,
    OPERATION_TYPE_UNLOCK_TABLE = 9,
    OPERATION_TYPE_UNLOCK_TABLET = 10,
    OPERATION_TYPE_LOCK_PART = 11,
    OPERATION_TYPE_UNLOCK_PART = 12,
    OPERATION_TYPE_LOCK_OBJ = 13,
    OPERATION_TYPE_UNLOCK_OBJ = 14,
    OPERATION_TYPE_LOCK_SUBPART = 15,
    OPERATION_TYPE_UNLOCK_SUBPART = 16,
    OPERATION_TYPE_LOCK_ALONE_TABLET = 17,
    OPERATION_TYPE_UNLOCK_ALONE_TABLET = 18,
    OPERATION_TYPE_LOCK_OBJS = 19,
    OPERATION_TYPE_UNLOCK_OBJS = 20,
    OPERATION_TYPE_MAX = 100
  };

  ObInnerSQLTransmitArg() : ctrl_svr_(), runner_svr_(), tenant_id_(OB_INVALID_ID),
      conn_id_(OB_INVALID_ID), inner_sql_(NULL), operation_type_(OPERATION_TYPE_INVALID),
      is_oracle_mode_(false), source_cluster_id_(OB_INVALID_CLUSTER_ID),
      worker_timeout_(OB_DEFAULT_SESSION_TIMEOUT),
      query_timeout_(OB_DEFAULT_SESSION_TIMEOUT), trx_timeout_(OB_DEFAULT_SESSION_TIMEOUT),
      sql_mode_(0), tz_info_wrap_(), ddl_info_(), is_load_data_exec_(false), nls_formats_{},
      use_external_session_(false), consumer_group_id_(0) {};
  ObInnerSQLTransmitArg(common::ObAddr ctrl_svr, common::ObAddr runner_svr,
                        uint64_t tenant_id, uint64_t conn_id, common::ObString inner_sql,
                        InnerSQLOperationType operation_type, bool is_oracle_mode,
                        const int64_t source_cluster_id, const int64_t worker_timeout,
                        const int64_t query_timeout, const int64_t trx_timeout,
                        ObSQLMode sql_mode, ObSessionDDLInfo ddl_info, const bool is_load_data_exec,
                        const bool use_external_session, const int64_t consumer_group_id = 0)
        : ctrl_svr_(ctrl_svr), runner_svr_(runner_svr),
          tenant_id_(tenant_id), conn_id_(conn_id), inner_sql_(inner_sql),
          operation_type_(operation_type), is_oracle_mode_(is_oracle_mode),
          source_cluster_id_(source_cluster_id), worker_timeout_(worker_timeout),
          query_timeout_(query_timeout), trx_timeout_(trx_timeout), sql_mode_(sql_mode),
          tz_info_wrap_(), ddl_info_(ddl_info), is_load_data_exec_(is_load_data_exec), nls_formats_{},
          use_external_session_(use_external_session), consumer_group_id_(consumer_group_id) {}
  ~ObInnerSQLTransmitArg() {}

  const common::ObAddr &get_ctrl_svr() const { return ctrl_svr_; }
  void set_ctrl_svr(const common::ObAddr &ctrl_svr) { ctrl_svr_ = ctrl_svr; }

  const common::ObAddr &get_runner_svr() const { return runner_svr_; }
  void set_runner_svr(const common::ObAddr &runner_svr) { runner_svr_ = runner_svr; }

  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

  uint64_t get_conn_id() const { return conn_id_; }
  void set_conn_id(const uint64_t conn_id) { conn_id_ = conn_id; }

  const common::ObString &get_inner_sql() const { return inner_sql_; }
  void set_inner_sql(const common::ObString &inner_sql) { inner_sql_ = inner_sql; }

  InnerSQLOperationType get_operation_type() const { return operation_type_; }
  void set_operation_type(const InnerSQLOperationType operation_type)
  { operation_type_ = operation_type; }

  bool get_is_oracle_mode() const { return is_oracle_mode_; }
  void set_is_oracle_mode(const bool is_oracle_mode) { is_oracle_mode_ = is_oracle_mode; }
  void set_source_cluster_id(const int64_t source_cluster_id) {
    source_cluster_id_ = source_cluster_id;
  }
  int64_t get_source_cluster_id() const {
    return source_cluster_id_;
  }

  void set_worker_timeout(const int64_t worker_timeout) {
    worker_timeout_ = worker_timeout;
  }
  int64_t get_worker_timeout() const {
    return worker_timeout_;
  }
  void set_query_timeout(const int64_t query_timeout) {
    query_timeout_ = query_timeout;
  }
  int64_t get_query_timeout() const {
    return query_timeout_;
  }
  void set_trx_timeout(const int64_t trx_timeout) {
    trx_timeout_ = trx_timeout;
  }
  int64_t get_trx_timeout() const {
    return trx_timeout_;
  }
  void set_consumer_group_id(const int64_t consumer_group_id) {
    consumer_group_id_ = consumer_group_id;
  }
  int64_t get_consumer_group_id() const {
    return consumer_group_id_;
  }
  inline int set_tz_info_wrap(const ObTimeZoneInfoWrap &other) { return tz_info_wrap_.deep_copy(other); }
  void set_nls_formats(const common::ObString &nls_date_format,
                       const common::ObString &nls_timestamp_format,
                       const common::ObString &nls_timestamp_tz_format)
  {
    nls_formats_[0] = nls_date_format;
    nls_formats_[1] = nls_timestamp_format;
    nls_formats_[2] = nls_timestamp_tz_format;
  }
  
  const common::ObTimeZoneInfoWrap &get_tz_info_wrap() const { return tz_info_wrap_; }
  const ObSessionDDLInfo &get_ddl_info() const { return ddl_info_; }
  ObSQLMode get_sql_mode() const { return sql_mode_; }
  bool get_is_load_data_exec() const { return is_load_data_exec_; }
  const ObString *get_nls_formats() const { return nls_formats_; }
  bool get_use_external_session() const { return use_external_session_; }

  TO_STRING_KV(K_(ctrl_svr),
               K_(runner_svr),
               K_(tenant_id),
               K_(conn_id),
               K_(inner_sql),
               K_(operation_type),
               K_(is_oracle_mode),
               K_(source_cluster_id),
               K_(worker_timeout),
               K_(query_timeout),
               K_(trx_timeout),
               K_(sql_mode),
               K_(tz_info_wrap),
               K_(ddl_info),
               K_(is_load_data_exec),
               K_(nls_formats),
               K_(use_external_session),
               K_(consumer_group_id));

private:
  common::ObAddr ctrl_svr_;
  common::ObAddr runner_svr_;
  uint64_t tenant_id_;
  uint64_t conn_id_;
  common::ObString inner_sql_;
  InnerSQLOperationType operation_type_;
  bool is_oracle_mode_;
  int64_t source_cluster_id_;
  int64_t worker_timeout_;
  int64_t query_timeout_;
  int64_t trx_timeout_;
  ObSQLMode sql_mode_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  ObSessionDDLInfo ddl_info_;
  bool is_load_data_exec_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
  bool use_external_session_;
  int64_t consumer_group_id_;
};

class ObInnerSQLTransmitResult
{
  OB_UNIS_VERSION(1);
public:
  ObInnerSQLTransmitResult() :
    res_code_(), conn_id_(OB_INVALID_ID), affected_rows_(-1),
    stmt_type_(sql::stmt::T_NONE), scanner_(), field_columns_(), allocator_("InnerSQL") {};
  ~ObInnerSQLTransmitResult() { allocator_.clear(); }

  int init_scanner() { return scanner_.init(); };
  void reset_scanner() { scanner_.reset(); }
  bool is_scanner_inited() { return scanner_.is_inited(); }

  const obrpc::ObRpcResultCode &get_res_code() const { return res_code_; }
  void set_err_code(int err_code) { res_code_.rcode_ = err_code; }
  int get_err_code() const { return res_code_.rcode_; }

  void set_conn_id(int64_t conn_id) { conn_id_ = conn_id; }
  int64_t get_conn_id() const { return conn_id_; }

  void set_stmt_type(sql::stmt::StmtType stmt_type) { stmt_type_ = stmt_type; }
  sql::stmt::StmtType get_stmt_type() const { return stmt_type_; }

  void set_affected_rows(int64_t affected_rows) { affected_rows_ = affected_rows; }
  int64_t get_affected_rows() const { return affected_rows_; }

  void set_scanner(const common::ObScanner scanner) { scanner_.assign(scanner); }
  common::ObScanner &get_scanner() { return scanner_; }

  int copy_field_columns(const common::ObIArray<common::ObField> &field_columns);
  void reset_field_columns() { field_columns_.reset(); } ;
  const common::ObSArray<common::ObField> &get_field_columns() { return field_columns_; };
  void set_tenant_id(uint64_t tenant_id)
  {
    scanner_.set_tenant_id(tenant_id);
    allocator_.set_tenant_id(tenant_id);
  }

  void reset()
  {
    res_code_.reset();
    conn_id_ = OB_INVALID_ID;
    affected_rows_ = -1;
    stmt_type_ = sql::stmt::T_NONE;
    reset_scanner();
    reset_field_columns();
  }

  TO_STRING_KV(K_(res_code),
               K_(conn_id),
               K_(affected_rows),
               K_(stmt_type),
               K_(scanner),
               K(field_columns_.count()));

private:
  obrpc::ObRpcResultCode res_code_;
  int64_t conn_id_;
  int64_t affected_rows_;
  sql::stmt::StmtType stmt_type_;
  common::ObScanner scanner_;
  common::ObSArray<common::ObField> field_columns_;
  common::ObArenaAllocator allocator_;
};

class ObInnerSQLRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObInnerSQLRpcProxy);
  virtual ~ObInnerSQLRpcProxy() {}
  // stream rpc interface
  RPC_SS(@PR5 inner_sql_sync_transmit, obrpc::OB_INNER_SQL_SYNC_TRANSMIT, (obrpc::ObInnerSQLTransmitArg), obrpc::ObInnerSQLTransmitResult);
};

class ObInnerSqlRpcStreamHandle
{
public:
  typedef obrpc::ObInnerSQLRpcProxy::SSHandle<obrpc::OB_INNER_SQL_SYNC_TRANSMIT> InnerSQLSSHandle;
  explicit ObInnerSqlRpcStreamHandle()
    : result_()
  {
    (void)reset_and_init_scanner();
  }
  virtual ~ObInnerSqlRpcStreamHandle() {}
  void reset() { result_.reset(); }

  const common::ObAddr &get_dst_addr() const { return handle_.get_dst_addr(); }
  InnerSQLSSHandle &get_handle() { return handle_; }

  int reset_and_init_scanner()
  {
    int ret = common::OB_SUCCESS;
    result_.reset_scanner();
    if (!result_.is_scanner_inited() && OB_FAIL(result_.init_scanner())) {
      SQL_EXE_LOG(WARN,"fail to init result", K(ret));
    }
    return ret;
  }

  ObInnerSQLTransmitResult *get_result()
  {
    ObInnerSQLTransmitResult *ret_result = NULL;
    if (!result_.is_scanner_inited()) {
      SQL_EXE_LOG_RET(ERROR, common::OB_NOT_INIT, "result_ is not inited");
    } else {
      ret_result = &result_;
    }
    return ret_result;
  }

  void reuse_scanner() { result_.get_scanner().reuse(); }
  int abort() { return handle_.abort(); }
  bool has_more() { return handle_.has_more(); }
  int get_more(ObInnerSQLTransmitResult &result) { return get_handle().get_more(result); }

private:
  InnerSQLSSHandle handle_;
  ObInnerSQLTransmitResult result_;
};

}  // namespace obrpc
}  // namespace oceanbase
#endif /* OBDEV_SRC_OBSERVER_INNER_SQL_RPC_PROXY_H_ */
