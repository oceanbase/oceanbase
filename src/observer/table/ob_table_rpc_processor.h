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

#ifndef _OB_TABLE_RPC_PROCESSOR_H
#define _OB_TABLE_RPC_PROCESSOR_H 1

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "ob_table_service.h"
#include "sql/monitor/ob_exec_stat.h"
#include "share/table/ob_table.h"
#include "ob_htable_lock_mgr.h"
namespace oceanbase
{
namespace table
{
class ObTableAPITransCb;
} // end namespace table
namespace observer
{
using oceanbase::table::ObTableConsistencyLevel;

struct ObGlobalContext;
class ObTableService;

/// @see RPC_S(PR5 login, obrpc::OB_TABLE_API_LOGIN, (table::ObTableLoginRequest), table::ObTableLoginResult);
class ObTableLoginP: public obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOGIN> >
{
  typedef obrpc::ObRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LOGIN> > ParentType;
public:
  explicit ObTableLoginP(const ObGlobalContext &gctx)
      :gctx_(gctx)
  {}
  virtual ~ObTableLoginP() = default;
  virtual int process() override;
private:
  int get_ids();
  int verify_password(const ObString &tenant, const ObString &user, const ObString &pass_secret,
                      const ObString &pass_scramble, const ObString &database, uint64_t &user_token);
  int generate_credential(uint64_t tenant_id, uint64_t user_id, uint64_t database,
                          int64_t ttl_us, uint64_t user_token, ObString &credential);
private:
  static const int64_t CREDENTIAL_BUF_SIZE = 256;
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
  char credential_buf_[CREDENTIAL_BUF_SIZE];
};

class ObTableRetryPolicy
{
public:
  ObTableRetryPolicy()
  : allow_retry_(true),
    allow_rpc_retry_(true),
    local_retry_interval_us_(10),
    max_local_retry_count_(5)
  {}
  virtual ~ObTableRetryPolicy() {}
  bool allow_retry() const { return allow_retry_; }
  // rpc retry will receate the processor,
  // so there is no retry count limit for now.
  bool allow_rpc_retry() const { return allow_retry_ && allow_rpc_retry_; }
public:
  bool allow_retry_;
  bool allow_rpc_retry_;
  int64_t local_retry_interval_us_;
  int64_t max_local_retry_count_;
};

class ObTableApiUtils
{
public:
  static int check_user_access(const common::ObString &credential_str, const ObGlobalContext &gctx, table::ObTableApiCredential &credential);
};

/*
 * Normally, the rpc process flow is:
 * 1. deserialize
 * 2. before_process
 * 3. process
 * 4. before_response
 * 5. response
 * 6. after_process
 * 7. cleanup
 *
 * Attention:
 * After response or async_commit_trans,
 * all buffer related to the request (such as req_) may recycled by the network frame.
 * DO NOT access these memory in after_process() and cleanup().
 */


/// Base class of all table api processor
class ObTableApiProcessorBase
{
public:
  explicit ObTableApiProcessorBase(const ObGlobalContext &gctx);
  virtual ~ObTableApiProcessorBase() = default;
public:
  static int init_session();
  int check_user_access(const ObString &credential_str);
  // transaction control
  int start_trans(bool is_readonly, const sql::stmt::StmtType stmt_type,
                  const table::ObTableConsistencyLevel consistency_level, uint64_t table_id,
                  const share::ObLSID &ls_id, int64_t timeout_ts);
  int end_trans(bool is_rollback, rpc::ObRequest *req, int64_t timeout_ts,
                bool use_sync = false, table::ObHTableLockHandle *lock_handle = nullptr);
  static int start_trans_(bool is_readonly, transaction::ObTxDesc*& trans_desc,
                          transaction::ObTxReadSnapshot &tx_snapshot,
                          const ObTableConsistencyLevel consistency_level,
                          sql::TransState *trans_state_ptr,
                          uint64_t table_id, const share::ObLSID &ls_id, int64_t timeout_ts);
  int sync_end_trans(bool is_rollback, int64_t timeout_ts, table::ObHTableLockHandle *lock_handle = nullptr);
  static int sync_end_trans_(bool is_rollback,
                             transaction::ObTxDesc *&trans_desc,
                             int64_t timeout_ts,
                             table::ObHTableLockHandle *lock_handle = nullptr,
                             const ObString *trace_info = nullptr);

  // for get
  int init_read_trans(const table::ObTableConsistencyLevel  consistency_level,
                      const share::ObLSID &ls_id,
                      int64_t timeout_ts);
  void release_read_trans();
  inline transaction::ObTxDesc *get_trans_desc() { return trans_desc_; }
  int get_tablet_by_rowkey(uint64_t table_id, const ObIArray<ObRowkey> &rowkeys,
                           ObIArray<ObTabletID> &tablet_ids);
  inline transaction::ObTxReadSnapshot &get_tx_snapshot() { return tx_snapshot_; }
  inline bool had_do_response() const { return had_do_response_; }
  int get_table_id(const ObString &table_name, const uint64_t arg_table_id, uint64_t &real_table_id) const;
protected:
  virtual int check_arg() = 0;
  virtual int try_process() = 0;
  virtual table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) = 0;
  virtual void set_req_has_wokenup() = 0;
  virtual void reset_ctx();
  int get_ls_id(const ObTabletID &tablet_id, share::ObLSID &ls_id);
  int process_with_retry(const ObString &credential, const int64_t timeout_ts);

  // audit
  bool need_audit() const;
  void start_audit(const rpc::ObRequest *req);
  void end_audit();
  virtual void audit_on_finish() {}
  virtual void save_request_string() = 0;
  virtual void generate_sql_id() = 0;
  virtual int check_table_index_supported(uint64_t table_id, bool &is_supported);

private:
  int async_commit_trans(rpc::ObRequest *req, int64_t timeout_ts, table::ObHTableLockHandle *lock_handle = nullptr);
  static int setup_tx_snapshot_(transaction::ObTxDesc &trans_desc,
                               transaction::ObTxReadSnapshot &tx_snapshot,
                               const bool strong_read,
                               const share::ObLSID &ls_id,
                               const int64_t timeout_ts);
protected:
  const ObGlobalContext &gctx_;
  ObTableService *table_service_;
  storage::ObAccessService *access_service_;
  share::ObLocationService *location_service_;
  table::ObTableApiCredential credential_;
  int32_t stat_event_type_;
  int64_t audit_row_count_;
  bool need_audit_;
  const char *request_string_;
  int64_t request_string_len_;
  sql::ObAuditRecordData audit_record_;
  ObArenaAllocator audit_allocator_;
  ObTableRetryPolicy retry_policy_;
  bool need_retry_in_queue_;
  int32_t retry_count_;
  uint64_t table_id_;
  ObTabletID tablet_id_;
protected:
  // trans control
  sql::TransState trans_state_;
  transaction::ObTxDesc *trans_desc_;
  bool had_do_response_; // asynchronous transactions return packet in advance
  sql::TransState *trans_state_ptr_;
  transaction::ObTxReadSnapshot tx_snapshot_;
  static const ObString OBKV_TRACE_INFO;
  ObAddr user_client_addr_;
};

template<class T>
class ObTableRpcProcessor: public obrpc::ObRpcProcessor<T>, public ObTableApiProcessorBase
{
  typedef obrpc::ObRpcProcessor<T> RpcProcessor;
public:
  explicit ObTableRpcProcessor(const ObGlobalContext &gctx) : ObTableApiProcessorBase(gctx) {}
  virtual ~ObTableRpcProcessor() = default;
  virtual int deserialize() override;
  virtual int before_process() override;
  virtual int process() override;
  virtual int before_response(int error_code) override;
  virtual int response(const int retcode) override;
  virtual int after_process(int error_code) override;

protected:
  virtual void set_req_has_wokenup() override;
  int64_t get_timeout_ts() const;
  virtual void save_request_string() override;
  virtual void generate_sql_id() override;
  virtual uint64_t get_request_checksum() = 0;
};

template<class T>
int64_t ObTableRpcProcessor<T>::get_timeout_ts() const
{
  int64_t ts = 0;
  if (NULL != RpcProcessor::rpc_pkt_) {
    ts = RpcProcessor::get_receive_timestamp() + RpcProcessor::rpc_pkt_->get_timeout();
  }
  return ts;
}
} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_PROCESSOR_H */
