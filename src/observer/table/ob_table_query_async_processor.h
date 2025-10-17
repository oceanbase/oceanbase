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
#include "ob_table_old_merge_filter.h"
#include "ob_table_batch_service.h"
#include "observer/table/common/ob_hbase_common_struct.h"
#include "observer/table/common/ob_table_query_session.h"

namespace oceanbase
{

namespace storage
{
class ObPartitionService;
}
namespace observer
{

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
  table::ObTableQueryAsyncCtx *query_ctx_;
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
 * -------------------------------------- ObTableQueryAsyncP ----------------------------------------
*/
class ObTableQueryAsyncP :
  public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE_QUERY_ASYNC> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE_QUERY_ASYNC>>
    ParentType;
  using ResultMergeIterator = table::ObOldMergeTableQueryResultIterator<common::ObNewRow, table::ObTableMergeFilterCompare>;
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
                                  table::ObTableQueryAsyncCtx *query_ctx);
  static int generate_table_query_info(ObIAllocator *allocator,
                                        schema::ObSchemaGetterGuard &schema_guard,
                                        const schema::ObSimpleTableSchemaV2* table_schema,
                                        uint64_t arg_tenant_id,
                                        uint64_t arg_table_id,
                                        const ObTabletID &arg_tablet_id,
                                        table::ObTableQuery &arg_query,
                                        table::ObTableQueryAsyncCtx *query_ctx,
                                        table::ObTableSingleQueryInfo *&info);
  static int init_tb_ctx(ObIAllocator* allocator,
                        ObTableConsistencyLevel level,
                        const table::ObTableEntityType &entity_type,
                        schema::ObSchemaGetterGuard& schema_guard,
                        table::ObTableApiCredential &credential,
                        bool tablegroup_req,
                        table::ObTableQueryAsyncCtx &query_ctx,
                        table::ObTableSingleQueryInfo& info,
                        const int64_t &timeout_ts,
                        table::ObTableCtx &ctx);
  static int get_table_result_iterator(ObIAllocator *allocator,
                                    table::ObTableQueryAsyncCtx &query_ctx,
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
                                        table::ObTableQueryAsyncCtx &query_ctx,
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
                          table::ObTableQueryAsyncCtx &query_ctx,
                          const ObString &arg_table_name,
                          table::ObTableEntityType entity_type,
                          table::ObTableQuery &query,
                          transaction::ObTxDesc *txn_desc,
                          transaction::ObTxReadSnapshot &tx_snapshot,
                          ResultType &result,
                          table::ObTableQueryResultIterator*& result_iterator);
protected:
  int new_try_process();
  int old_try_process();
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  virtual uint64_t get_request_checksum() override;
  int64_t get_trans_timeout_ts();
  // int init_query_ctx(ObIAllocator* allocator, const ObString &arg_table_name, bool is_tablegroup_req, table::ObTableQuery &arg_query, uint64_t table_id, ObTabletID tablet_id, table::ObTableQueryAsyncCtx *query_ctx);
  virtual table::ObTableEntityType get_entity_type() override { return arg_.entity_type_; }
  virtual bool is_kv_processor() override { return true; }

private:
  int process_query_start();
  int process_query_next();
  int process_query_end();
  int destory_query_session(bool need_rollback_trans);
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncP);

private:
  int get_old_session(uint64_t sessid, table::ObTableQueryAsyncSession *&query_session);
  int query_scan_with_init(ObIAllocator* allocator, table::ObTableQueryAsyncCtx &query_ctx, table::ObTableQuery &origin_query);
  int query_scan_without_init(table::ObTableCtx &tb_ctx);
  int modify_ret_for_session_not_exist(const table::ObQueryOperationType &query_type);

private:
  int execute_query();
  int init_read_trans(const table::ObTableConsistencyLevel consistency_level,
                      const share::ObLSID &ls_id,
                      int64_t timeout_ts,
                      bool need_global_snapshot,
                      sql::TransState *trans_state);

  int init_read_trans(const ObTableConsistencyLevel consistency_level,
                      const uint64_t table_id,
                      const common::ObIArray<common::ObTabletID> &tablet_ids,
                      int64_t timeout_ts,
                      table::ObTableTransParam &trans_param);

  static int process_columns(const ObIArray<ObString>& columns,
                             ObArray<std::pair<ObString, bool>>& familys,
                             ObArray<ObString>& real_columns);

  static int update_table_info_columns(table::ObTableSingleQueryInfo* table_info,
                            const ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                            const ObArray<ObString>& real_columns,
                            const std::pair<ObString, bool>& family_addfamily_flag);

  static int process_table_info(table::ObTableSingleQueryInfo* table_info,
                      const ObArray<std::pair<ObString, bool>>& family_clear_flags,
                      const ObArray<ObString>& real_columns,
                      const std::pair<ObString, bool>& family_with_flag);
  virtual bool is_new_try_process() override;
private:
  int64_t result_row_count_;
  uint64_t query_session_id_;
  table::ObTableQueryAsyncSession *query_session_;
  int64_t timeout_ts_;
  bool is_full_table_scan_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_QUERY_ASYNC_PROCESSOR_H */
