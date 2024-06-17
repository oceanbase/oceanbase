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

#ifndef _OB_TABLE_QUERY_AND_MUTATE_PROCESSOR_H
#define _OB_TABLE_QUERY_AND_MUTATE_PROCESSOR_H 1
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
class ObTableQueryAndMutateP: public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_QUERY_AND_MUTATE> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_QUERY_AND_MUTATE> > ParentType;
public:
  explicit ObTableQueryAndMutateP(const ObGlobalContext &gctx);
  virtual ~ObTableQueryAndMutateP() {}

  virtual int deserialize() override;
protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  virtual table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) override;
  virtual void audit_on_finish() override;
  virtual uint64_t get_request_checksum() override;
  virtual bool is_kv_processor() override { return true; }

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
  int execute_htable_mutation(table::ObTableQueryResultIterator *result_iterator,
                              int64_t &affected_rows);
  int execute_table_mutation(table::ObTableQueryResultIterator *result_iterator,
                             int64_t &affected_rows);
  int execute_one_mutation(table::ObTableQueryResult &one_result,
                           const common::ObIArray<common::ObString> &names,
                           int64_t &affected_rows);
  int get_rowkey_column_names(common::ObIArray<common::ObString> &names);
  // check whether checkAndMutate's expected value is null or not
  static int check_expected_value_is_null(table::ObTableQueryResultIterator *result_iter, bool &is_null);
  // check value whether the value of first row of htable query result is null(empty string) or not
  static int check_result_value_is_null(table::ObTableQueryResult *query_result, bool &is_null_value);
private:
  template<int TYPE>
  int process_dml_op(const table::ObITableEntity &new_entity, int64_t &affected_rows)
  {
    int ret = OB_SUCCESS;
    table::ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
    const table::ObTableOperation &mutation = mutations.at(0);

    SMART_VAR(table::ObTableCtx, tb_ctx, allocator_) {
      table::ObTableApiSpec *spec = nullptr;
      table::ObTableApiExecutor *executor = nullptr;
      table::ObTableOperationResult op_result;
      if (OB_FAIL(init_tb_ctx(tb_ctx,
                              mutation.type(),
                              new_entity))) {
        SERVER_LOG(WARN, "fail to init table ctx", K(ret));
      } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
        SERVER_LOG(WARN, "fail to init trans", K(ret), K(tb_ctx));
      } else if (OB_FAIL(table::ObTableOpWrapper::process_op<TYPE>(tb_ctx, op_result))) {
        SERVER_LOG(WARN, "fail to process insert op", K(ret));
      } else {
        affected_rows = op_result.get_affected_rows();
      }
    }

    return ret;
  }
  int process_insert(const table::ObITableEntity &new_entity, int64_t &affected_rows)
  {
    int ret = OB_SUCCESS;
    if (!tb_ctx_.is_ttl_table()) {
      ret = process_dml_op<table::TABLE_API_EXEC_INSERT>(new_entity, affected_rows);
    } else {
      ret = process_dml_op<table::TABLE_API_EXEC_TTL>(new_entity, affected_rows);
    }
    return ret;
  }
  int process_insert_up(const table::ObITableEntity &new_entity, int64_t &affected_rows)
  {
    int ret = OB_SUCCESS;
    if (!tb_ctx_.is_ttl_table()) {
      ret = process_dml_op<table::TABLE_API_EXEC_INSERT_UP>(new_entity, affected_rows);
    } else {
      ret = process_dml_op<table::TABLE_API_EXEC_TTL>(new_entity, affected_rows);
    }
    return ret;
  }
private:
  common::ObArenaAllocator allocator_;
  table::ObTableCtx tb_ctx_;
  table::ObTableEntityFactory<table::ObTableEntity> default_entity_factory_;

  table::ObTableQueryResult one_result_;
  bool end_in_advance_; // 提前终止标志
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAndMutateP);
};
} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_QUERY_AND_MUTATE_PROCESSOR_H */
