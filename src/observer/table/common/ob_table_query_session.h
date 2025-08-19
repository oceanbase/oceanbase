/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef _OB_TABLE_QUERY_SESSION_H
#define _OB_TABLE_QUERY_SESSION_H
#include "observer/table/async_query/ob_hbase_async_query_iter.h"
#include "observer/table/common/ob_table_common_struct.h"
#include "observer/table/ob_table_batch_common.h"

namespace oceanbase
{
namespace table
{

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

class ObITableQueryAsyncSession
{
public:
  ObITableQueryAsyncSession(int64_t sess_id)
    : allocator_("TableAQSessAlc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      in_use_(true),
      iterator_mementity_(nullptr),
      iterator_mementity_destroy_guard_(iterator_mementity_),
      start_time_(0),
      req_timeinfo_(nullptr),
      timeout_ts_(10000000),
      sess_id_(sess_id)
  {}
  virtual ~ObITableQueryAsyncSession()
  {
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
public:
  virtual transaction::ObTxDesc *get_trans_desc() = 0;
  virtual sql::TransState* get_trans_state() = 0;
  virtual void set_timeout_ts() = 0;
public:
  virtual int init();
  bool is_in_use() { return in_use_; }
  void set_in_use(bool in_use) { in_use_ = in_use; }
  table::ObTableEntityType get_session_type() const { return session_type_; }
  virtual uint64_t get_timeout_ts() { return timeout_ts_; }
  lib::MemoryContext &get_memory_ctx() { return iterator_mementity_; }
  void set_req_start_time(int64_t start_time) { start_time_ = start_time; }
  void update_req_timeinfo_start_time() const { req_timeinfo_->start_time_ = start_time_; req_timeinfo_->reentrant_cnt_++; }
  int alloc_req_timeinfo();
  void set_session_type(table::ObTableEntityType session_type) { session_type_ = session_type; }
  static int get_session_id(uint64_t &real_sessid, uint64_t arg_sessid);
  int64_t get_session_id() { return sess_id_; }
protected:
  ObArenaAllocator allocator_;
  bool in_use_;
  lib::MemoryContext iterator_mementity_;
  ObTableQueryAsyncEntifyDestroyGuard iterator_mementity_destroy_guard_;
  // start time for req_timeinfo_
  int64_t start_time_;
  // req time info
  observer::ObReqTimeInfo *req_timeinfo_;
  table::ObTableEntityType session_type_;
  uint64_t timeout_ts_;
  uint64_t sess_id_;
};

class ObTableNewQueryAsyncSession : public ObITableQueryAsyncSession
{
public:
  ObTableNewQueryAsyncSession(int64_t sess_id)
    : ObITableQueryAsyncSession(sess_id),
      query_iter_(nullptr),
      table_exec_ctx_(nullptr)
  {}
  virtual ~ObTableNewQueryAsyncSession()
  {
    if (OB_NOT_NULL(query_iter_)) {
      query_iter_->~ObIAsyncQueryIter();
    }

    if (OB_NOT_NULL(table_exec_ctx_)) {
      table_exec_ctx_->~ObTableExecCtx();
    }
  }
public:
  virtual transaction::ObTxDesc *get_trans_desc() override { return async_query_ctx_.trans_param_.trans_desc_; }
  virtual sql::TransState* get_trans_state() override { return &async_query_ctx_.trans_param_.trans_state_; }
  virtual void set_timeout_ts() override;
  virtual uint64_t get_lease_timeout_period() const;
public:
  int get_or_create_query_iter(table::ObTableEntityType entity_type,
                               table::ObIAsyncQueryIter *&query_iter);
  int init_query_ctx(const uint64_t table_id,
                     const ObTabletID tablet_id,
                     const ObString &table_name,
                     const bool is_tablegroup_req,
                     table::ObTableApiCredential &credential);
  int get_or_create_exec_ctx(table::ObTableExecCtx *&async_exec_ctx);
public:
  struct ObTableNewQueryAsyncCtx
  {
    ObTableNewQueryAsyncCtx()
      : table_id_(common::OB_INVALID_ID),
        tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
        table_schema_(nullptr)
    {}
    ObTableTransParam &get_trans_param() { return trans_param_; }
    ObTableApiCredential credential_;
    ObTableApiSessGuard sess_guard_;
    schema::ObSchemaGetterGuard schema_guard_;
    ObKvSchemaCacheGuard schema_cache_guard_;
    ObTableTransParam trans_param_;
    uint64_t table_id_;
    common::ObTabletID tablet_id_;
    const share::schema::ObTableSchema *table_schema_;
  };
private:
  table::ObIAsyncQueryIter *query_iter_;
  ObTableNewQueryAsyncCtx async_query_ctx_;
  table::ObTableExecCtx *table_exec_ctx_;
};

struct ObTableInfoBase
{
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


struct ObTableSingleQueryInfo : public ObTableInfoBase
{
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
    tb_ctx_.set_sess_guard(nullptr);
  }

  int64_t to_string(char *buf, const int64_t len) const {
    return OB_SUCCESS;
  }
  // session guard 来自 ObTableQueryAsyncCtx
  table::ObTableCtx tb_ctx_;
  table::ObTableApiScanRowIterator row_iter_;
  table::ObTableQueryIterableResult result_;
  table::ObTableQuery query_;
  table::ObTableApiCacheGuard table_cache_guard_;
};

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

class ObTableQueryAsyncSession : public ObITableQueryAsyncSession
{
public:
  explicit ObTableQueryAsyncSession(int64_t sess_id)
    : ObITableQueryAsyncSession(sess_id),
      tenant_id_(MTL_ID()),
      query_(),
      select_columns_(),
      result_iterator_(nullptr),
      query_ctx_(allocator_),
      lease_timeout_period_(60 * 1000 * 1000),
      trans_desc_(nullptr),
      row_count_(0)
  {}
  virtual ~ObTableQueryAsyncSession()
  {
    if (OB_NOT_NULL(query_ctx_.sess_guard_)) {
      query_ctx_.sess_guard_->~ObTableApiSessGuard();
      query_ctx_.sess_guard_ = nullptr;
      // multi_cf_infos_ 中tb_ctx的sess_guard就来自于query_ctx_
      query_ctx_.multi_cf_infos_[0]->tb_ctx_.set_sess_guard(nullptr);
    }
    if (OB_NOT_NULL(result_iterator_)) {
      result_iterator_->~ObTableQueryResultIterator();
      result_iterator_ = nullptr;
    }
  }

  void set_result_iterator(table::ObTableQueryResultIterator* iter);
  void set_hbase_result_iterator(table::ObHbaseQueryResultIterator* iter);
  table::ObTableQueryResultIterator *& get_result_iter() { return result_iterator_; };
  int init();

  virtual void set_timeout_ts() override;
  table::ObTableQueryResultIterator *get_result_iterator() { return result_iterator_; }
  table::ObHbaseQueryResultIterator *get_hbase_result_iterator() { return hbase_result_iterator_; }
  ObArenaAllocator *get_allocator() {return &allocator_;}
  common::ObObjectID get_tenant_id() { return tenant_id_; }
  virtual uint64_t get_lease_timeout_period() const { return lease_timeout_period_; }
  table::ObTableQuery &get_query() { return query_; }
  common::ObIArray<common::ObString> &get_select_columns() { return select_columns_; }
  int deep_copy_select_columns(const common::ObIArray<common::ObString> &query_cols_names_,
                               const common::ObIArray<common::ObString> &tb_ctx_cols_names_);
  ObTableQueryAsyncCtx &get_query_ctx() { return query_ctx_; }
public:
  virtual sql::TransState* get_trans_state() override {return &trans_state_;}
  virtual transaction::ObTxDesc* get_trans_desc() override {return trans_desc_;}
  int64_t &get_row_count() { return row_count_; }
  void set_trans_desc(transaction::ObTxDesc *trans_desc) { trans_desc_ = trans_desc; }
private:
  bool in_use_;
  common::ObObjectID tenant_id_;
  table::ObTableQuery query_; // deep copy from arg_.query_
  ObSEArray<ObString, 16> select_columns_; // deep copy from tb_ctx or query, which includes all the actual col names the user acquired
  table::ObTableQueryResultIterator *result_iterator_;
  table::ObHbaseQueryResultIterator *hbase_result_iterator_;
  ObTableQueryAsyncCtx query_ctx_;

private:
  // txn control
  sql::TransState trans_state_;
  uint64_t lease_timeout_period_;
  transaction::ObTxDesc *trans_desc_;
  int64_t row_count_;
};



} // end of namespace table
} // end of namespace oceanbase

#endif
