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
namespace oceanbase
{
namespace table
{
class ObTableAPITransCb;
} // end namespace table
namespace observer
{
using namespace oceanbase::table;

class ObGlobalContext;
class ObTableService;

struct ObTableApiCredential final
{
  OB_UNIS_VERSION(1);
public:
  ObTableApiCredential();
  ~ObTableApiCredential();
public:
  int64_t cluster_id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  int64_t expire_ts_;
  uint64_t hash_val_;
public:
  uint64_t hash(uint64_t seed = 0) const;
  TO_STRING_KV(K_(cluster_id),
               K_(tenant_id),
               K_(user_id),
               K_(database_id),
               K_(expire_ts),
               K_(hash_val));
};

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
                  const ObTableConsistencyLevel consistency_level, uint64_t table_id,
                  const common::ObIArray<int64_t> &part_ids, int64_t timeout_ts);
  int start_trans(bool is_readonly, const sql::stmt::StmtType stmt_type, uint64_t table_id,
                  const common::ObIArray<int64_t> &part_ids, int64_t timeout_ts);
  int end_trans(bool is_rollback, rpc::ObRequest *req, int64_t timeout_ts, bool use_sync = false);
  inline bool did_async_end_trans() const { return did_async_end_trans_; }
  inline transaction::ObTransDesc& get_trans_desc() { return *trans_desc_ptr_; }
  int get_partition_by_rowkey(uint64_t table_id, const ObIArray<common::ObRowkey> &rowkeys,
                              common::ObIArray<int64_t> &part_ids,
                              common::ObIArray<sql::RowkeyArray> &rowkeys_per_part);

  int get_table_id(const ObString &table_name, const uint64_t arg_table_id, uint64_t &real_table_id) const;
protected:
  virtual int check_arg() = 0;
  virtual int try_process() = 0;
  virtual table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) = 0;
  virtual void set_req_has_wokenup() = 0;
  virtual void reset_ctx();
  int process_with_retry(const ObString &credential, const int64_t timeout_ts);

  // audit
  bool need_audit() const;
  void start_audit(const rpc::ObRequest *req);
  void end_audit();
  virtual void audit_on_finish() {}
  virtual void save_request_string() = 0;
  virtual void generate_sql_id() = 0;
  virtual int check_table_index_supported(uint64_t table_id, bool &is_supported);

  // set trans consistency level
  void set_consistency_level(const ObTableConsistencyLevel consistency_level) { consistency_level_ = consistency_level; }
  ObTableConsistencyLevel consistency_level() const { return consistency_level_; }

private:
  int get_participants(uint64_t table_id, const common::ObIArray<int64_t> &part_ids,
                       common::ObPartitionLeaderArray &partition_leaders);
  int get_participants_from_lc(uint64_t table_id, const common::ObIArray<int64_t> &part_ids,
                               common::ObPartitionLeaderArray &partition_leaders);
  int get_participants_optimistic(uint64_t table_id, const common::ObIArray<int64_t> &part_ids,
                                  common::ObPartitionLeaderArray &partition_leaders);

  int async_commit_trans(rpc::ObRequest *req, int64_t timeout_ts);
  int sync_end_trans(bool is_rollback, int64_t timeout_ts);
  int generate_schema_info_arr(const uint64_t table_id,
                               const common::ObPartitionArray &participants,
                               transaction::ObPartitionSchemaInfoArray &schema_info_arr);
  //@}
protected:
  const ObGlobalContext &gctx_;
  storage::ObPartitionService *part_service_;
  ObTableService *table_service_;
  ObTableApiCredential credential_;
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
protected:
  // trans control
  ObPartitionLeaderArray participants_;
  sql::TransState trans_state_;
  transaction::ObTransDesc trans_desc_;
  //part_epoch_list_ record the epoch id of response_partitions_
  //when start_participants executed in the leader replica
  transaction::ObPartitionEpochArray part_epoch_list_;
  bool did_async_end_trans_;
  ObTableConsistencyLevel consistency_level_;
  ObPartitionLeaderArray *participants_ptr_;
  sql::TransState *trans_state_ptr_;
  transaction::ObTransDesc *trans_desc_ptr_;
  transaction::ObPartitionEpochArray *part_epoch_list_ptr_;
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
  virtual int before_response() override;
  virtual int response(const int retcode) override;
  virtual int after_process() override;

protected:
  virtual void set_req_has_wokenup() override;
  virtual int64_t get_timeout_ts() const;
  virtual void save_request_string() override;
  virtual void generate_sql_id() override;
  virtual uint64_t get_request_checksum() = 0;
};


class ObHTableDeleteExecutor final
{
public:
  ObHTableDeleteExecutor(common::ObArenaAllocator &alloc,
                         uint64_t table_id,
                         uint64_t partition_id,
                         int64_t timeout_ts,
                         ObTableApiProcessorBase *processor,
                         ObTableService *table_service,
                         storage::ObPartitionService *part_service);
  ~ObHTableDeleteExecutor() {}
  // @param affected_rows [out] deleted number of htable cells
  int htable_delete(const table::ObTableBatchOperation &delete_op, int64_t &affected_rows);
private:
  int execute_query(const table::ObTableQuery &query,
                    table::ObTableQueryResultIterator *&result_iterator);
  int generate_delete_cells(
      table::ObTableQueryResult &one_row,
      table::ObTableEntityFactory<table::ObTableEntity> &entity_factory,
      table::ObTableBatchOperation &mutations_out);
  int execute_mutation(const table::ObTableBatchOperation &mutations,
                       table::ObTableBatchOperationResult &mutations_result);
private:
  ObTableService *table_service_;
  storage::ObPartitionService *part_service_;
  ObTableServiceQueryCtx query_ctx_;
  table::ObTableQuery query_;
  table::ObTableQueryResult one_result_;
  table::ObTableEntityFactory<table::ObTableEntity> entity_factory_;
  table::ObTableBatchOperation mutations_;
  table::ObTableBatchOperationResult mutations_result_;
  ObTableServiceGetCtx mutate_ctx_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHTableDeleteExecutor);
};

class ObHTablePutExecutor final
{
public:
  ObHTablePutExecutor(common::ObArenaAllocator &alloc,
                      uint64_t table_id,
                      uint64_t partition_id,
                      int64_t timeout_ts,
                      ObTableApiProcessorBase *processor,
                      ObTableService *table_service,
                      storage::ObPartitionService *part_service);
  ~ObHTablePutExecutor() {}

  int htable_put(const ObTableBatchOperation &put_op, int64_t &affected_rows, int64_t now_ms = 0);
private:
  ObTableService *table_service_;
  storage::ObPartitionService *part_service_;
  table::ObTableEntityFactory<table::ObTableEntity> entity_factory_;
  table::ObTableBatchOperationResult mutations_result_;
  ObTableServiceGetCtx mutate_ctx_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHTablePutExecutor);
};

// executor of Increment and Append
class ObHTableIncrementExecutor final
{
public:
  ObHTableIncrementExecutor(table::ObTableOperationType::Type type,
                            common::ObArenaAllocator &alloc,
                            uint64_t table_id,
                            uint64_t partition_id,
                            int64_t timeout_ts,
                            ObTableApiProcessorBase *processor,
                            ObTableService *table_service,
                            storage::ObPartitionService *part_service);
  ~ObHTableIncrementExecutor() {}

  int htable_increment(ObTableQueryResult &row_cells,
                       const table::ObTableBatchOperation &increment_op,
                       int64_t &affected_rows,
                       table::ObTableQueryResult *results);
private:
  typedef std::pair<common::ObString, int32_t> ColumnIdx;
  class ColumnIdxComparator;
  int sort_qualifier(const table::ObTableBatchOperation &increment);
  int execute_mutation(const table::ObTableBatchOperation &mutations,
                       table::ObTableBatchOperationResult &mutations_result);
  static int add_to_results(table::ObTableQueryResult &results, const ObObj &rk, const ObObj &cq,
                            const ObObj &ts, const ObObj &value);
private:
  table::ObTableOperationType::Type type_;
  ObTableService *table_service_;
  storage::ObPartitionService *part_service_;
  table::ObTableEntityFactory<table::ObTableEntity> entity_factory_;
  table::ObTableBatchOperation mutations_;
  table::ObTableBatchOperationResult mutations_result_;
  ObTableServiceGetCtx mutate_ctx_;
  common::ObSEArray<ColumnIdx, OB_DEFAULT_SE_ARRAY_COUNT> columns_;
  common::ObArenaAllocator allocator_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHTableIncrementExecutor);
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

}  // end namespace observer
}  // end namespace oceanbase

#endif /* _OB_TABLE_RPC_PROCESSOR_H */
