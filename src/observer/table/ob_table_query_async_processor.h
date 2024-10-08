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
#include "ob_table_scan_executor.h"
#include "ob_table_query_common.h"
#include "ob_table_merge_filter.h"

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
    executor_(nullptr),
    spec_(nullptr),
    expr_frame_info_(allocator),
    result_(),
    query_() {}

  ~ObTableSingleQueryInfo() {
    row_iter_.close();
    if (OB_NOT_NULL(spec_)) {
      spec_->destroy_executor(executor_);
      spec_->~ObTableApiSpec();
    }
  }

  int64_t to_string(char *buf, const int64_t len) const {
    return OB_SUCCESS;
  }

  OB_INLINE int iter_open() {
    return row_iter_.open(executor_);
  }
  table::ObTableCtx tb_ctx_;
  table::ObTableApiScanRowIterator row_iter_;
  table::ObTableApiScanExecutor *executor_;
  table::ObTableApiSpec *spec_;
  ObExprFrameInfo expr_frame_info_;
  table::ObTableQueryIterableResult result_;
  table::ObTableQuery query_;
};

class ObTableHbaseRowKeyDefaultCompare
{
public:
  ObTableHbaseRowKeyDefaultCompare(): result_code_(common::OB_SUCCESS) {}
  int compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret);
  bool operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs);
  OB_INLINE int get_error_code() const { return result_code_; }
  int result_code_;
};

/**
 * ---------------------------------------- ObTableQueryAsyncCtx ----------------------------------------
 */
struct ObTableQueryAsyncCtx
{
  explicit ObTableQueryAsyncCtx(common::ObIAllocator &allocator)
      : table_id_(OB_INVALID_ID),
        part_idx_(OB_INVALID_ID),
        subpart_idx_(OB_INVALID_ID),
        schema_version_(OB_INVALID_SCHEMA_VERSION),
        sess_guard_(),
        tb_ctx_(allocator),
        expr_frame_info_(allocator),
        spec_(nullptr),
        executor_(nullptr)
  {}
  virtual ~ObTableQueryAsyncCtx()
  {
    row_iter_.close();
    for (int i = 0; i < multi_cf_infos_.count(); i++) {
      if (OB_NOT_NULL(multi_cf_infos_.at(i))) {
        multi_cf_infos_.at(i)->~ObTableSingleQueryInfo();
      }
    }
    if (OB_NOT_NULL(spec_) && OB_NOT_NULL(executor_)) {
      spec_->destroy_executor(executor_);
    }
  }

  uint64_t table_id_;

  // Computes the real tablet ID on the server side during multi-table queries
  int64_t part_idx_;
  int64_t subpart_idx_;
  int64_t schema_version_;
  table::ObTableApiSessGuard sess_guard_;
  table::ObTableCtx tb_ctx_;
  ObExprFrameInfo expr_frame_info_;
  table::ObTableApiSpec *spec_;
  table::ObTableApiScanExecutor *executor_;
  table::ObTableApiScanRowIterator row_iter_;
  common::ObArray<ObTableSingleQueryInfo*> multi_cf_infos_;
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
      lease_timeout_period_(60 * 1000 * 1000)
  {}
  ~ObTableQueryAsyncSession() {}

  void set_result_iterator(table::ObTableQueryResultIterator* iter);
  table::ObTableQueryResultIterator *get_result_iter() { return result_iterator_; };
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
  void set_trans_desc(transaction::ObTxDesc *trans_desc) { trans_desc_ = trans_desc; }
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
  using ResultMergeIterator = table::ObMergeTableQueryResultIterator<common::ObNewRow, ObTableHbaseRowKeyDefaultCompare>;
public:
  explicit ObTableQueryAsyncP(const ObGlobalContext &gctx);
  virtual ~ObTableQueryAsyncP() {}
  virtual int deserialize() override;
  virtual int before_process() override;
protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  virtual uint64_t get_request_checksum() override;
  int init_query_ctx(const ObString &arg_table_name);
  int64_t get_trans_timeout_ts();
  int init_multi_cf_query_ctx(const ObString &arg_tablegroup_name);
  virtual table::ObTableEntityType get_entity_type() override { return arg_.entity_type_; }
  virtual bool is_kv_processor() override { return true; }
private:
  int process_query_start();
  int process_multi_cf_query_start();
  int process_query_next();
  int process_multi_cf_query_next();
  int process_query_end();
  int destory_query_session(bool need_rollback_trans);
  int init_schema_cache_guard();
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncP);

private:
  int get_session_id(uint64_t &real_sessid, uint64_t arg_sessid);
  int get_query_session(uint64_t sessid, ObTableQueryAsyncSession *&query_session);
  int query_scan_with_init();

  int query_scan_multi_cf_with_init();
  int query_scan_without_init(table::ObTableCtx &tb_ctx);
  int query_scan_multi_cf_without_init();

private:
  int check_query_type();
  int init_tb_ctx(table::ObTableCtx &ctx);

  int init_tb_ctx(table::ObTableCtx &ctx, ObTableSingleQueryInfo& info);
  int init_tb_ctx(table::ObTableCtx &ctx, uint64_t table_id);

  int create_result_iterator(ObTableQueryAsyncSession* query_session, table::ObTableQueryResultIterator*& result_iter, uint64_t table_id);

  int generate_multi_result_iterator(ObArray<table::ObTableQueryResultIterator*>& array);

  int generate_merge_result_iterator(const ObArray<table::ObTableQueryResultIterator*>& array);

  int execute_multi_cf_query();
  int execute_query();
  int start_trans(bool is_readonly,
                  const table::ObTableConsistencyLevel consistency_level,
                  const share::ObLSID &ls_id,
                  int64_t timeout_ts,
                  bool need_global_snapshot,
                  sql::TransState *trans_state);

  int process_columns(const ObIArray<ObString>& columns,
                      ObArray<std::pair<ObString, bool>>& familys,
                      ObArray<ObString>& real_columns);

  int update_table_info_columns(ObTableSingleQueryInfo* table_info, const ObArray<ObString>& real_columns);

  bool found_family(const ObString& table_name, const ObArray<std::pair<ObString, bool>>& family_clear_flags, std::pair<ObString, bool>& flag);

  int process_table_info(ObTableSingleQueryInfo* table_info,
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
