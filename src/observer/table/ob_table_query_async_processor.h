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

#ifndef _OB_TABLE_QUERY_ASYNC_PROCESSOR_H
#define _OB_TABLE_QUERY_ASYNC_PROCESSOR_H 1
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "ob_table_context.h"
#include "ob_table_cache.h"
#include "ob_table_scan_executor.h"
#include "ob_table_query_common.h"
#include "ob_table_merge_filter.h"
#include "ob_table_batch_service.h"

namespace oceanbase
{

namespace storage
{
class ObPartitionService;
}
namespace observer
{

struct ObTableSingleQueryInfo : public ObTableInfoBase {
  explicit ObTableSingleQueryInfo(common::ObIAllocator &allocator):
    tb_ctx_(allocator),
    row_iter_(),
    result_(),
    query_(),
    table_cache_guard_() {}

  ~ObTableSingleQueryInfo()
  {
    // TODO: It's unsuitable to get and destruct executor row iter and destruct
    // the scan executor should be released after row iter destructed
    table::ObTableApiScanExecutor *scan_exexcutor = row_iter_.get_scan_executor();
    row_iter_.close();
    if (OB_NOT_NULL(scan_exexcutor)) {
      scan_exexcutor->~ObTableApiScanExecutor();
    }
  }

  int64_t to_string(char *buf, const int64_t len) const {
    return OB_SUCCESS;
  }
  table::ObTableCtx tb_ctx_;
  table::ObTableApiScanRowIterator row_iter_;
  table::ObTableQueryIterableResult result_;
  table::ObTableQuery query_;
  table::ObTableApiCacheGuard table_cache_guard_;
};

class ObTableMergeFilterCompare {
public:
  ObTableMergeFilterCompare() = default;
  virtual ~ObTableMergeFilterCompare() = default;

  virtual int compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const = 0;
  virtual bool operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs) = 0;

  int get_error_code() const noexcept { return result_code_; }

protected:
  int result_code_ = OB_SUCCESS;
};

/**
 * ---------------------------------------- ObTableQueryAsyncCtx ----------------------------------------
 */
struct ObTableQueryAsyncCtx : public table::ObTableQueryBatchCtx
{
  explicit ObTableQueryAsyncCtx(common::ObIAllocator &allocator)
      : table::ObTableQueryBatchCtx(),
        part_idx_(OB_INVALID_ID),
        subpart_idx_(OB_INVALID_ID),
        schema_version_(OB_INVALID_SCHEMA_VERSION),
        sess_guard_(nullptr),
        expr_frame_info_(allocator),
        spec_(nullptr),
        executor_(nullptr)
  {}
  virtual ~ObTableQueryAsyncCtx()
  {
    row_iter_.close();
    if (OB_NOT_NULL(spec_) && OB_NOT_NULL(executor_)) {
      spec_->destroy_executor(executor_);
    }
    for (int i = 0; i < multi_cf_infos_.count(); i++) {
      if (OB_NOT_NULL(multi_cf_infos_.at(i))) {
        multi_cf_infos_.at(i)->~ObTableSingleQueryInfo();
      }
    }
  }

  // Computes the real tablet ID on the server side during multi-table queries
  int64_t part_idx_;
  int64_t subpart_idx_;
  int64_t schema_version_;
  table::ObTableApiSessGuard *sess_guard_;
  ObExprFrameInfo expr_frame_info_;
  table::ObTableApiSpec *spec_;
  table::ObTableApiScanExecutor *executor_;
  table::ObTableApiScanRowIterator row_iter_;
  common::ObArray<ObTableSingleQueryInfo*> multi_cf_infos_;
  // record table_id and tablet_id pass by client, especially for global index:
  // - index_table_id is the id of index table
  // - index_tablet_id is the query index tablet
  uint64_t index_table_id_;
  ObTabletID index_tablet_id_;
};

/**
 * ---------------------------------------- ObHTableLSGetCtx ----------------------------------------
 */
 struct ObHTableLSGetCtx
 {
  ObHTableLSGetCtx(table::ObTableQuery &query, ObString &arg_table_name, table::ObTableApiCredential &credential)
      : query_(query),
        arg_table_name_(arg_table_name),
        outer_credential_(credential)
  {}
  ~ObHTableLSGetCtx() {}

  common::ObArenaAllocator *allocator_;
  ObTableQueryAsyncCtx *query_ctx_;
  table::ObTableQuery &query_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  ObString &arg_table_name_;
  bool get_is_tablegroup_;
  table::ObTableTransParam *trans_param_;
  uint64_t table_id_;
  ObTabletID tablet_id_;
  table::ObTableApiCredential &outer_credential_;
  table::ObTableQueryIterableResult *iterable_result_;
 };

/**
 * ---------------------------------------- ObTableQueryAsyncEntifyDestroyGuard ----------------------------------------
 */
class ObTableQueryAsyncEntifyDestroyGuard
  {
  public:
    ObTableQueryAsyncEntifyDestroyGuard(lib::MemoryContext &entity) : ref_(entity) {}
    ~ObTableQueryAsyncEntifyDestroyGuard()
    {
      if (OB_NOT_NULL(ref_)) {
        DESTROY_CONTEXT(ref_);
        ref_ = NULL;
      }
    }
  private:
    lib::MemoryContext &ref_;
  };

/**
 * ---------------------------------------- ObTableQueryAsyncSession ----------------------------------------
 */
class ObTableQueryAsyncSession final
{
  friend class ObTableQueryASyncMgr;

public:
  explicit ObTableQueryAsyncSession()
    : in_use_(true),
      timeout_ts_(10000000),
      iterator_mementity_(nullptr),
      iterator_mementity_destroy_guard_(iterator_mementity_),
      allocator_("TableAQSessAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      tenant_id_(MTL_ID()),
      query_(),
      select_columns_(),
      result_iterator_(nullptr),
      query_ctx_(allocator_),
      lease_timeout_period_(60 * 1000 * 1000),
      trans_desc_(nullptr),
      row_count_(0),
      req_timeinfo_(nullptr)
  {}
  ~ObTableQueryAsyncSession() {
    if (OB_NOT_NULL(query_ctx_.sess_guard_)) {
      query_ctx_.sess_guard_->~ObTableApiSessGuard();
      query_ctx_.sess_guard_ = nullptr;
    }
    if (OB_NOT_NULL(req_timeinfo_)) {
      // before update_end_time:
      //  start time > end time, end time == 0, reentrant cnt == 1
      // after update_end_time:
      //  start time < end time, end time != 0, reentrant cnt == 0
      req_timeinfo_->update_end_time();
      req_timeinfo_->~ObReqTimeInfo();
      req_timeinfo_ = nullptr;
    }
  }

  void set_result_iterator(table::ObTableQueryResultIterator* iter);
  table::ObTableQueryResultIterator *& get_result_iter() { return result_iterator_; };
  void set_in_use(bool in_use) {in_use_ = in_use;}
  bool is_in_use() {return in_use_;}
  int init();

  void set_timout_ts(uint64_t timeout_ts) { timeout_ts_ = timeout_ts; }
  void set_timout_ts();
  void set_session_type(table::ObTableEntityType session_type) { session_type_ = session_type; }
  table::ObTableEntityType get_session_type() const { return session_type_; }
  table::ObTableQueryResultIterator *get_result_iterator() { return result_iterator_; }
  lib::MemoryContext &get_memory_ctx() { return iterator_mementity_; }
  ObArenaAllocator *get_allocator() {return &allocator_;}
  common::ObObjectID get_tenant_id() { return tenant_id_; }
  uint64_t get_lease_timeout_period() const { return lease_timeout_period_; }
  table::ObTableQuery &get_query() { return query_; }
  common::ObIArray<common::ObString> &get_select_columns() { return select_columns_; }
  int deep_copy_select_columns(const common::ObIArray<common::ObString> &query_cols_names_,
                               const common::ObIArray<common::ObString> &tb_ctx_cols_names_);
  ObTableQueryAsyncCtx &get_query_ctx() { return query_ctx_; }
public:
  sql::TransState* get_trans_state() {return &trans_state_;}
  transaction::ObTxDesc* get_trans_desc() {return trans_desc_;}
  int64_t &get_row_count() { return row_count_; }
  void set_trans_desc(transaction::ObTxDesc *trans_desc) { trans_desc_ = trans_desc; }
  void set_req_start_time(int64_t start_time) { start_time_ = start_time; }
  void update_req_timeinfo_start_time() const { req_timeinfo_->start_time_ = start_time_; req_timeinfo_->reentrant_cnt_++; }
  int alloc_req_timeinfo();
private:
  bool in_use_;
  uint64_t timeout_ts_;
  lib::MemoryContext iterator_mementity_;
  ObTableQueryAsyncEntifyDestroyGuard iterator_mementity_destroy_guard_;
  ObArenaAllocator allocator_;
  table::ObTableEntityType session_type_;
  common::ObObjectID tenant_id_;
  table::ObTableQuery query_; // deep copy from arg_.query_
  ObSEArray<ObString, 16> select_columns_; // deep copy from tb_ctx or query, which includes all the actual col names the user acquired
  table::ObTableQueryResultIterator *result_iterator_;
  ObTableQueryAsyncCtx query_ctx_;

private:
  // txn control
  sql::TransState trans_state_;
  uint64_t lease_timeout_period_;
  transaction::ObTxDesc *trans_desc_;
  int64_t row_count_;

  // start time for req_timeinfo_
  int64_t start_time_;
  // req time info
  ObReqTimeInfo *req_timeinfo_;
};

/**
 * ----------------------------------- ObTableQueryASyncMgr -------------------------------------
 */
class ObTableQueryASyncMgr : public common::ObTimerTask
{
  friend class ObTableQueryAsyncP;

public:
  using ObQueryHashMap =
    common::hash::ObHashMap<uint64_t, ObTableQueryAsyncSession *, common::hash::SpinReadWriteDefendMode>;
  using QuerySessionPair = common::hash::HashMapPair<uint64_t, ObTableQueryAsyncSession*>;
  ~ObTableQueryASyncMgr() {}
  ObTableQueryASyncMgr();
  struct ObGetAllSessionIdOp {
    explicit ObGetAllSessionIdOp(common::ObIArray<uint64_t>& session_id_array) : session_id_array_(session_id_array)
    {}
    int operator()(QuerySessionPair& entry);
    common::ObIArray<uint64_t>& session_id_array_;
  };

public:
  int get_query_session(uint64_t sessid, ObTableQueryAsyncSession *&query_sess_ctx);
  int set_query_session(uint64_t sessid, ObTableQueryAsyncSession *query_sess_ctx);
  void clean_timeout_query_session();
  void destroy_all_query_session();
  void free_query_session(ObTableQueryAsyncSession *query_session);
  virtual void runTimerTask() override;
  static int mtl_init(ObTableQueryASyncMgr *&query_async_mgr);
  int start();
  void stop();
  void wait();
  void destroy();

public:
  ObQueryHashMap *get_query_session_map();
  ObTableQueryAsyncSession *alloc_query_session();
  int generate_query_sessid(uint64_t &sess_id);
  lib::ObMutex& get_locker(uint64_t sessid) { return locker_arr_[sessid % DEFAULT_LOCK_ARR_SIZE];}

private:
  int init();
  int rollback_trans(ObTableQueryAsyncSession &query_session);
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryASyncMgr);

private:
  static const uint64_t INVALID_SESSION_ID = 0;
  static const uint64_t DEFAULT_LOCK_ARR_SIZE = 2000;
  static const uint64_t QUERY_SESSION_MAX_SIZE = 1000;
  static const uint64_t MIN_QUERY_SESSION_CLEAN_DELAY = 1 * 1000 * 1000;  // 1s


private:
  common::ObFIFOAllocator allocator_;
  int64_t session_id_;
  ObQueryHashMap query_session_map_;
  lib::ObMutex locker_arr_[DEFAULT_LOCK_ARR_SIZE];
  common::ObTimer timer_;
  bool is_inited_;
};

/**
 * -------------------------------------- ObTableQueryAsyncP ----------------------------------------
*/
class ObTableQueryAsyncP :
  public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE_QUERY_ASYNC> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE_QUERY_ASYNC>>
    ParentType;
  using ResultMergeIterator = table::ObMergeTableQueryResultIterator<common::ObNewRow, ObTableMergeFilterCompare>;
public:
  explicit ObTableQueryAsyncP(const ObGlobalContext &gctx);
  virtual ~ObTableQueryAsyncP() {}
  virtual int deserialize() override;
  virtual int before_process() override;
public:
  static int init_query_async_ctx(ObIAllocator *allocator,
                                  table::ObTableQuery &arg_query,
                                  schema::ObSchemaGetterGuard& schema_guard,
                                  const ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> &table_schemas,
                                  uint64_t arg_tenant_id,
                                  uint64_t arg_table_id,
                                  const ObTabletID &arg_tablet_id,
                                  ObTableQueryAsyncCtx *query_ctx);
  static int generate_table_query_info(ObIAllocator *allocator,
                                        schema::ObSchemaGetterGuard &schema_guard,
                                        const schema::ObSimpleTableSchemaV2* table_schema,
                                        uint64_t arg_tenant_id,
                                        uint64_t arg_table_id,
                                        const ObTabletID &arg_tablet_id,
                                        table::ObTableQuery &arg_query,
                                        ObTableQueryAsyncCtx *query_ctx,
                                        ObTableSingleQueryInfo *&info);
  static int init_tb_ctx(ObIAllocator* allocator,
                        ObTableConsistencyLevel level,
                        const table::ObTableEntityType &entity_type,
                        schema::ObSchemaGetterGuard& schema_guard,
                        table::ObTableApiCredential &credential,
                        bool tablegroup_req,
                        ObTableQueryAsyncCtx &query_ctx,
                        ObTableSingleQueryInfo& info,
                        const int64_t &timeout_ts,
                        table::ObTableCtx &ctx);
  static int get_table_result_iterator(ObIAllocator *allocator,
                                    ObTableQueryAsyncCtx &query_ctx,
                                    const ObString &arg_table_name,
                                    table::ObTableEntityType entity_type,
                                    table::ObTableQuery &query,
                                    transaction::ObTxDesc *txn_desc,
                                    transaction::ObTxReadSnapshot &tx_snapshot,
                                    table::ObTableQueryResult &result,
                                    table::ObTableQueryResultIterator*& result_iterator);
  template <typename ResultType>
  static int get_inner_htable_result_iterator(ObIAllocator *allocator,
                                        table::ObTableEntityType entity_type,
                                        const ObString& arg_table_name,
                                        ObTableQueryAsyncCtx &query_ctx,
                                        table::ObTableQuery &query,
                                        transaction::ObTxDesc *txn_desc,
                                        transaction::ObTxReadSnapshot &tx_snapshot,
                                        ResultType &out_result,
                                        common::ObIArray<table::ObTableQueryResultIterator*>& iterators_array);
  template <typename ResultType>
  static int generate_merge_result_iterator(ObIAllocator *allocator,
                                            table::ObTableQuery &query,
                                            common::ObIArray<table::ObTableQueryResultIterator *> &iterators_array,
                                            ResultType &result,
                                            table::ObTableQueryResultIterator*& result_iterator /* merge result iterator */);
  template <typename ResultType>
  static int get_htable_result_iterator(ObIAllocator *allocator,
                          ObTableQueryAsyncCtx &query_ctx,
                          const ObString &arg_table_name,
                          table::ObTableEntityType entity_type,
                          table::ObTableQuery &query,
                          transaction::ObTxDesc *txn_desc,
                          transaction::ObTxReadSnapshot &tx_snapshot,
                          ResultType &result,
                          table::ObTableQueryResultIterator*& result_iterator);
protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  virtual uint64_t get_request_checksum() override;
  int64_t get_trans_timeout_ts();
  // int init_query_ctx(ObIAllocator* allocator, const ObString &arg_table_name, bool is_tablegroup_req, table::ObTableQuery &arg_query, uint64_t table_id, ObTabletID tablet_id, ObTableQueryAsyncCtx *query_ctx);
  virtual table::ObTableEntityType get_entity_type() override { return arg_.entity_type_; }
  virtual bool is_kv_processor() override { return true; }

private:
  int process_query_start();
  int process_query_next();
  int process_query_end();
  int destory_query_session(bool need_rollback_trans);
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncP);

private:
  int get_session_id(uint64_t &real_sessid, uint64_t arg_sessid);
  int get_query_session(uint64_t sessid, ObTableQueryAsyncSession *&query_session);
  int query_scan_with_init(ObIAllocator* allocator, ObTableQueryAsyncCtx &query_ctx, table::ObTableQuery &origin_query);
  int query_scan_without_init(table::ObTableCtx &tb_ctx);

private:
  int check_query_type();
  int execute_query();
  int init_read_trans(const table::ObTableConsistencyLevel consistency_level,
                      const share::ObLSID &ls_id,
                      int64_t timeout_ts,
                      bool need_global_snapshot,
                      sql::TransState *trans_state);

  static int process_columns(const ObIArray<ObString>& columns,
                      ObArray<std::pair<ObString, bool>>& familys,
                      ObArray<ObString>& real_columns);

  static int update_table_info_columns(ObTableSingleQueryInfo* table_info,
                            const ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                            const ObArray<ObString>& real_columns,
                            const std::pair<ObString, bool>& family_addfamily_flag);

  static int check_family_existence_with_base_name(const ObString& table_name,
                                                    const ObString& base_tablegroup_name,
                                                    table::ObTableEntityType entity_type,
                                                    const ObArray<std::pair<ObString, bool>>& family_clear_flags,
                                                    std::pair<ObString, bool> &flag, bool &exist);

  static int process_table_info(ObTableSingleQueryInfo* table_info,
                      const ObArray<std::pair<ObString, bool>>& family_clear_flags,
                      const ObArray<ObString>& real_columns,
                      const std::pair<ObString, bool>& family_with_flag);

private:
  int64_t result_row_count_;
  uint64_t query_session_id_;
  ObArenaAllocator allocator_;
  ObTableQueryAsyncSession *query_session_;
  int64_t timeout_ts_;
  bool is_full_table_scan_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_QUERY_ASYNC_PROCESSOR_H */
