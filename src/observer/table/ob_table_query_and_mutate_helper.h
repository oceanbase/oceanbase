/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_TABLE_QUERY_AND_MUTATE_HELPER_H
#define _OB_TABLE_QUERY_AND_MUTATE_HELPER_H
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "ob_table_context.h"
#include "ob_table_scan_executor.h"
#include "ob_table_update_executor.h"
#include "ob_table_insert_executor.h"
#include "ob_table_cache.h"
#include "ob_table_op_wrapper.h"
#include "ob_table_query_common.h"
#include "ob_table_audit.h"

namespace oceanbase
{
namespace observer
{
struct ObTableQMParam
{
  explicit ObTableQMParam(const table::ObITableQueryAndMutate &query_and_mutate)
      : query_and_mutate_(query_and_mutate),
        schema_guard_(nullptr),
        simple_table_schema_(nullptr),
        schema_cache_guard_(nullptr),
        sess_guard_(nullptr),
        trans_desc_(nullptr),
        tx_snapshot_(nullptr),
        query_and_mutate_result_(nullptr),
        single_op_result_(nullptr)
  {}

  TO_STRING_KV(K_(table_id),
               K_(tablet_id),
               K_(credential),
               K_(entity_type),
               KP_(schema_guard),
               KP_(simple_table_schema),
               KP_(schema_cache_guard),
               KP_(sess_guard),
               KP_(trans_desc),
               KP_(tx_snapshot),
               KP_(query_and_mutate_result),
               KP_(single_op_result));

  const table::ObITableQueryAndMutate &query_and_mutate_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  const share::schema::ObSimpleTableSchemaV2 *simple_table_schema_;
  table::ObKvSchemaCacheGuard *schema_cache_guard_;
  table::ObTableApiSessGuard *sess_guard_;
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  int64_t timeout_ts_;
  table::ObTableApiCredential credential_;
  table::ObTableEntityType entity_type_;
  transaction::ObTxDesc *trans_desc_;
  const transaction::ObTxReadSnapshot *tx_snapshot_;
  table::ObTableQueryAndMutateResult *query_and_mutate_result_;
  table::ObTableSingleOpResult *single_op_result_;
};

class QueryAndMutateHelper
{
public:
  explicit QueryAndMutateHelper(common::ObIAllocator &allocator,
                                ObTableQMParam &qm_param,
                                table::ObTableAuditCtx &audit_ctx)
    : allocator_(allocator),
      query_and_mutate_(qm_param.query_and_mutate_),
      tb_ctx_(allocator_),
      audit_ctx_(audit_ctx)
    {
      schema_guard_ = qm_param.schema_guard_;
      simple_table_schema_ = qm_param.simple_table_schema_;
      schema_cache_guard_ = qm_param.schema_cache_guard_;
      sess_guard_ = qm_param.sess_guard_;
      table_id_ = qm_param.table_id_;
      tablet_id_ = qm_param.tablet_id_;
      timeout_ts_ = qm_param.timeout_ts_;
      credential_ = qm_param.credential_;
      entity_type_ = qm_param.entity_type_;
      trans_desc_ = qm_param.trans_desc_;
      tx_snapshot_ = qm_param.tx_snapshot_;
      is_hkv_ = entity_type_ == table::ObTableEntityType::ET_HKV;
      query_and_mutate_result_ = qm_param.query_and_mutate_result_;
      single_op_result_ = qm_param.single_op_result_;
    }
  virtual ~QueryAndMutateHelper() {}
public:
  int execute_query_and_mutate();

private:
  typedef std::pair<common::ObString, int32_t> ColumnIdx;
  class ColumnIdxComparator;
  int sort_qualifier(common::ObIArray<ColumnIdx> &columns,
                     const table::ObTableBatchOperation &increment);
  int init_scan_tb_ctx(table::ObTableApiCacheGuard &cache_guard);
  int init_tb_ctx(table::ObTableCtx &ctx,
                  table::ObTableOperationType::Type op_type,
                  const table::ObITableEntity &entity);
  int generate_new_value(const table::ObITableEntity *old_entity,
                         const table::ObITableEntity &src_entity,
                         bool is_increment,
                         table::ObTableEntity &new_entity);
  int add_to_results(const table::ObTableEntity &new_entity);
  int execute_htable_delete(const table::ObTableOperation &table_operation, table::ObTableCtx *&tb_ctx);
  int execute_htable_put(const table::ObTableOperation &table_operation, table::ObTableCtx *&tb_ctx);
  int execute_htable_inc_or_append(table::ObTableQueryResult *result);
  int execute_htable_insert(const table::ObITableEntity &new_entity);
  int execute_htable_mutation(table::ObTableQueryResultIterator *result_iterator);
  int execute_table_mutation(table::ObTableQueryResultIterator *result_iterator);
  int execute_one_mutation(common::ObIAllocator &allocator,
                           table::ObTableQueryResult &one_result,
                           const common::ObIArray<common::ObString> &names,
                           int64_t &affected_rows,
                           bool &end_in_advance);
  int get_rowkey_column_names(common::ObIArray<common::ObString> &names);
  // check whether checkAndMutate's expected value is null or not
  int check_expected_value_is_null(table::ObTableQueryResultIterator *result_iter, bool &is_null);
  // check value whether the value of first row of htable query result is null(empty string) or not
  int check_result_value_is_null(table::ObTableQueryResult *query_result, bool &is_null_value);

  int check_and_execute(table::ObTableQueryResultIterator *result_iterator);
private:
  template<int TYPE>
  int process_dml_op(const table::ObITableEntity &new_entity,
                     int64_t &affected_rows)
  {
    int ret = OB_SUCCESS;
    const table::ObTableBatchOperation &mutations = query_and_mutate_.get_mutations();
    const table::ObTableOperation &mutation = mutations.at(0);
    OB_TABLE_START_AUDIT(credential_,
                         *sess_guard_,
                         simple_table_schema_->get_table_name_str(),
                         &audit_ctx_, mutation);

    SMART_VAR(table::ObTableCtx, tb_ctx, allocator_) {
      table::ObTableApiSpec *spec = nullptr;
      table::ObTableApiExecutor *executor = nullptr;
      table::ObTableOperationResult op_result;
      if (OB_FAIL(init_tb_ctx(tb_ctx,
                              mutation.type(),
                              new_entity))) {
        SERVER_LOG(WARN, "fail to init table ctx", K(ret));
      } else if (OB_FAIL(tb_ctx.init_trans(trans_desc_, *tx_snapshot_))) {
        SERVER_LOG(WARN, "fail to init trans", K(ret), K(tb_ctx));
      } else if (OB_FAIL(table::ObTableOpWrapper::process_op<TYPE>(tb_ctx, op_result))) {
        SERVER_LOG(WARN, "fail to process insert op", K(ret));
      } else {
        affected_rows = op_result.get_affected_rows();
      }
    }

    OB_TABLE_END_AUDIT(ret_code, ret,
                       snapshot, get_tx_snapshot(),
                       stmt_type, table::ObTableAuditUtils::get_stmt_type(mutation.type()));
    return ret;
  }

  int process_incr_or_append_op(const table::ObITableEntity &new_entity,
                                int64_t &affected_rows)
  {
    int ret = OB_SUCCESS;
    const table::ObTableBatchOperation &mutations = query_and_mutate_.get_mutations();
    const table::ObTableOperation &mutation = mutations.at(0);
    OB_TABLE_START_AUDIT(credential_,
                         *sess_guard_,
                         simple_table_schema_->get_table_name_str(),
                         &audit_ctx_, mutation);

    SMART_VAR(table::ObTableCtx, tb_ctx, allocator_) {
      table::ObTableApiSpec *spec = nullptr;
      table::ObTableApiExecutor *executor = nullptr;
      table::ObTableOperationResult op_result;
      if (OB_FAIL(init_tb_ctx(tb_ctx,
                              mutation.type(),
                              new_entity))) {
        SERVER_LOG(WARN, "fail to init table ctx", K(ret));
      } else if (OB_FAIL(tb_ctx.init_trans(trans_desc_, *tx_snapshot_))) {
        SERVER_LOG(WARN, "fail to init trans", K(ret), K(tb_ctx));
      } else if (OB_FAIL(table::ObTableOpWrapper::process_incr_or_append_op(tb_ctx, op_result))) {
        SERVER_LOG(WARN, "fail to process insert op", K(ret));
      } else {
        affected_rows = op_result.get_affected_rows();
      }
    }

    OB_TABLE_END_AUDIT(ret_code, ret,
                       snapshot, get_tx_snapshot(),
                       stmt_type, table::ObTableAuditUtils::get_stmt_type(mutation.type()));
    return ret;
  }

  int process_insert(const table::ObITableEntity &new_entity,
                     int64_t &affected_rows)
  {
    int ret = OB_SUCCESS;
    if (!tb_ctx_.is_ttl_table()) {
      ret = process_dml_op<table::TABLE_API_EXEC_INSERT>(new_entity, affected_rows);
    } else {
      ret = process_dml_op<table::TABLE_API_EXEC_TTL>(new_entity, affected_rows);
    }
    return ret;
  }
  int process_insert_up(const table::ObITableEntity &new_entity,
                        int64_t &affected_rows)
  {
    int ret = OB_SUCCESS;
    if (!tb_ctx_.is_ttl_table()) {
      ret = process_dml_op<table::TABLE_API_EXEC_INSERT_UP>(new_entity, affected_rows);
    } else {
      ret = process_dml_op<table::TABLE_API_EXEC_TTL>(new_entity, affected_rows);
    }
    return ret;
  }

  void set_result_affected_rows(const int64_t affected_rows)
  {
    if (OB_NOT_NULL(query_and_mutate_result_)) {
      query_and_mutate_result_->affected_rows_ = affected_rows;
    } else if (OB_NOT_NULL(single_op_result_)) {
      single_op_result_->set_affected_rows(affected_rows);
    }
  }

private:
  OB_INLINE transaction::ObTxDesc* get_trans_desc() const { return trans_desc_; }
  OB_INLINE const transaction::ObTxReadSnapshot &get_tx_snapshot() const { return *tx_snapshot_; }
  int64_t get_timeout_ts() const { return timeout_ts_; }
private:
  common::ObIAllocator &allocator_; // processor's allocator
  table::ObTableEntityFactory<table::ObTableSingleOpEntity> default_entity_factory_;
  const table::ObITableQueryAndMutate &query_and_mutate_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  const share::schema::ObSimpleTableSchemaV2 *simple_table_schema_;
  table::ObKvSchemaCacheGuard *schema_cache_guard_;
  table::ObTableApiSessGuard *sess_guard_;
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  int64_t timeout_ts_;
  table::ObTableApiCredential credential_;
  table::ObTableCtx tb_ctx_;
  table::ObTableQueryResult one_result_;
  table::ObTableEntityType entity_type_;
  transaction::ObTxDesc *trans_desc_;
  const transaction::ObTxReadSnapshot *tx_snapshot_;
  bool is_hkv_;
  table::ObTableQueryAndMutateResult *query_and_mutate_result_;
  table::ObTableSingleOpResult *single_op_result_;
  table::ObTableAuditCtx &audit_ctx_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_QUERY_AND_MUTATE_HELPER_H */
