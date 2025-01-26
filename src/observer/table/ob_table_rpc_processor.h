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
#include "ob_table_schema_cache.h"
#include "observer/ob_req_time_service.h"
#include "ob_table_trans_utils.h"
#include "ob_table_audit.h"

namespace oceanbase
{
namespace table
{
class ObTableAPITransCb;
} // end namespace table
namespace observer
{
using oceanbase::table::ObTableConsistencyLevel;

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
  bool can_use_redis_v2();
private:
  const ObGlobalContext &gctx_;
  table::ObTableApiCredential credential_;
  char credential_buf_[table::ObTableApiCredential::CREDENTIAL_BUF_SIZE];
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
  int check_mode();
  // transaction control
  int start_trans(bool is_readonly,
                  const table::ObTableConsistencyLevel consistency_level,
                  const share::ObLSID &ls_id,
                  int64_t timeout_ts,
                  bool need_global_snapshot);
  int end_trans(bool is_rollback,
                rpc::ObRequest *req,
                table::ObTableCreateCbFunctor *functor,
                bool use_sync = false);
  int init_read_trans(const ObTableConsistencyLevel consistency_level,
                      const ObLSID &ls_id,
                      int64_t timeout_ts,
                      bool need_global_snapshot);

  // for get
  inline transaction::ObTxDesc *get_trans_desc() { return trans_param_.trans_desc_; }

  int get_idx_by_table_tablet_id(uint64_t arg_table_id, ObTabletID arg_tablet_id,
                                int64_t &part_idx, int64_t &subpart_idx);
  int get_tablet_by_idx(uint64_t table_id, int64_t part_idx, int64_t subpart_idx, ObTabletID &tablet_ids);
  inline transaction::ObTxReadSnapshot &get_tx_snapshot() { return trans_param_.tx_snapshot_; }
  inline bool had_do_response() const { return trans_param_.had_do_response_; }
  int get_table_id(const ObString &table_name, const uint64_t arg_table_id, uint64_t &real_table_id) const;
protected:
  virtual int check_arg() = 0;
  virtual int try_process() = 0;
  virtual table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) { return nullptr; }
  virtual void set_req_has_wokenup() = 0;
  virtual void reset_ctx();
  int get_ls_id(const ObTabletID &tablet_id, share::ObLSID &ls_id);
  virtual table::ObTableEntityType get_entity_type() = 0;
  virtual bool is_kv_processor() = 0;
  int process_with_retry(const ObString &credential, const int64_t timeout_ts);
  // init schema guard for tablegroup
  int init_tablegroup_schema(const ObString &arg_tablegroup_name);
  // init schema guard
  virtual int init_schema_info(const ObString &arg_table_name, uint64_t arg_table_id);
  virtual int init_schema_info(uint64_t table_id, const ObString &arg_table_name);
  virtual int init_schema_info(const ObString &arg_table_name);
  int check_table_has_global_index(bool &exists, table::ObKvSchemaCacheGuard& schema_cache_guard);
  int get_tablet_id(const share::schema::ObSimpleTableSchemaV2 * simple_table_schema,
                    const ObTabletID &arg_tablet_id,
                    const uint64_t table_id,
                    ObTabletID &tablet_id);
  ObTableProccessType get_stat_process_type(bool is_readonly,
                                            bool is_same_type,
                                            bool is_same_properties_names,
                                            table::ObTableOperationType::Type op_type);
protected:
  const ObGlobalContext &gctx_;
  ObTableService *table_service_;
  storage::ObAccessService *access_service_;
  share::ObLocationService *location_service_;
  table::ObTableApiCredential credential_;
  table::ObTableApiSessGuard sess_guard_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  const share::schema::ObSimpleTableSchemaV2 *simple_table_schema_;
  observer::ObReqTimeGuard req_timeinfo_guard_; // 引用cache资源必须加ObReqTimeGuard
  table::ObKvSchemaCacheGuard schema_cache_guard_;
  int32_t stat_process_type_;
  bool enable_query_response_time_stats_;
  int64_t stat_row_count_;
  ObTableRetryPolicy retry_policy_;
  bool need_retry_in_queue_;
  bool is_tablegroup_req_; // is table name a tablegroup name
  int32_t retry_count_;
  uint64_t table_id_;
  ObTabletID tablet_id_;
protected:
  // trans control
  table::ObTableTransParam trans_param_;
  transaction::ObTxReadSnapshot tx_snapshot_;
  common::ObAddr user_client_addr_;
  table::ObTableAuditCtx audit_ctx_;
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
  int64_t get_timeout() const;
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

template<class T>
int64_t ObTableRpcProcessor<T>::get_timeout() const
{
  int64_t timeout = 0;
  if (NULL != RpcProcessor::rpc_pkt_) {
    timeout = RpcProcessor::rpc_pkt_->get_timeout();
  }
  return timeout;
}

struct ObTableInfoBase {
  explicit ObTableInfoBase()
                          : table_id_(OB_INVALID_ID),
                            simple_schema_(nullptr),
                            schema_cache_guard_(),
                            schema_version_(OB_INVALID_VERSION) {}

  virtual ~ObTableInfoBase() {}

  int64_t get_table_id() const {
    return table_id_;
  }

  void set_table_id(int64_t table_id) {
    table_id_ = table_id;
  }

  const ObString& get_real_table_name() const {
    return real_table_name_;
  }

  void set_real_table_name(const ObString& real_table_name) {
    real_table_name_ = real_table_name;
  }

  const share::schema::ObSimpleTableSchemaV2* get_simple_schema() {
    return simple_schema_;
  }

  void set_simple_schema(const share::schema::ObSimpleTableSchemaV2* simple_schema) {
    simple_schema_ = simple_schema;
  }

  table::ObKvSchemaCacheGuard& get_schema_cache_guard() {
    return schema_cache_guard_;
  }

  void set_schema_cache_guard(const table::ObKvSchemaCacheGuard& schema_cache_guard) {
    schema_cache_guard_ = schema_cache_guard;
  }

  int64_t get_schema_version() const {
    return schema_version_;
  }

  void set_schema_version(int64_t schema_version) {
    schema_version_ = schema_version;
  }

  TO_STRING_KV(K(table_id_),
               KP(simple_schema_),
               K(schema_cache_guard_),
               K(schema_version_));

  int64_t table_id_;
  ObString real_table_name_;
  const share::schema::ObSimpleTableSchemaV2* simple_schema_;
  table::ObKvSchemaCacheGuard schema_cache_guard_;
  int64_t schema_version_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_PROCESSOR_H */
