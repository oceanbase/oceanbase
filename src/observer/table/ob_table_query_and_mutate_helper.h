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


namespace oceanbase
{
namespace observer
{

class QueryAndMutateHelper
{
public:
  explicit QueryAndMutateHelper(common::ObIAllocator &allocator,
                                const table::ObITableQueryAndMutate &query_and_mutate,
                                const uint64_t table_id, const ObTabletID &tablet_id,
                                int64_t timeout_ts, table::ObTableApiCredential &credential,
                                const table::ObTableEntityType &entity_type, transaction::ObTxDesc *trans_desc,
                                const transaction::ObTxReadSnapshot &tx_snapshot, table::ObTableQueryAndMutateResult &result)
    : allocator_(allocator), query_and_mutate_(query_and_mutate),
      table_id_(table_id), tablet_id_(tablet_id), timeout_ts_(timeout_ts),
      credential_(credential), tb_ctx_(allocator_), entity_type_(entity_type), trans_desc_(trans_desc),
      tx_snapshot_(&tx_snapshot), is_hkv_(entity_type == table::ObTableEntityType::ET_HKV),
      query_and_mutate_result_(&result), single_op_result_(NULL)
    {
    }
  explicit QueryAndMutateHelper(common::ObIAllocator &allocator,
                                const table::ObITableQueryAndMutate &query_and_mutate,
                                const uint64_t table_id, const ObTabletID &tablet_id,
                                int64_t timeout_ts, table::ObTableApiCredential &credential,
                                const table::ObTableEntityType &entity_type, transaction::ObTxDesc *trans_desc,
                                const transaction::ObTxReadSnapshot &tx_snapshot, table::ObTableSingleOpResult &result)
    : allocator_(allocator), query_and_mutate_(query_and_mutate), table_id_(table_id), tablet_id_(tablet_id),
      timeout_ts_(timeout_ts), credential_(credential), tb_ctx_(allocator_),
      entity_type_(entity_type), trans_desc_(trans_desc), tx_snapshot_(&tx_snapshot),
      is_hkv_(entity_type == table::ObTableEntityType::ET_HKV),
      query_and_mutate_result_(NULL), single_op_result_(&result)
    {
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
  int refresh_query_range(const ObObj &new_q_obj);
  int generate_new_value(const ObNewRow *old_row,
                         const table::ObITableEntity &src_entity,
                         bool is_increment,
                         table::ObTableEntity &new_entity);
  int add_to_results(const ObObj &rk,
                     const ObObj &cq,
                     const ObObj &ts,
                     const ObObj &value);
  int get_old_row(table::ObTableApiSpec &scan_spec, ObNewRow *&row);
  int execute_htable_delete();
  int execute_htable_put();
  int execute_htable_increment(table::ObTableApiSpec &scan_spec);
  int execute_htable_insert(const table::ObITableEntity &new_entity);
  int execute_htable_put(const table::ObITableEntity &new_entity);
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
      // set InsertOrUpdate since we only support InsUp in single_op now
      single_op_result_->set_type(table::ObTableOperationType::INSERT_OR_UPDATE);
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
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  int64_t timeout_ts_;
  table::ObTableApiCredential &credential_;
  table::ObTableCtx tb_ctx_;
  table::ObTableQueryResult one_result_;
  table::ObTableEntityType entity_type_;
  transaction::ObTxDesc *trans_desc_;
  const transaction::ObTxReadSnapshot *tx_snapshot_;
  bool is_hkv_;
  table::ObTableQueryAndMutateResult *query_and_mutate_result_;
  table::ObTableSingleOpResult *single_op_result_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_QUERY_AND_MUTATE_HELPER_H */
