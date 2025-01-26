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

#ifndef _OB_TABLE_LS_EXECUTE_PROCESSOR_H
#define _OB_TABLE_LS_EXECUTE_PROCESSOR_H
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "ob_table_context.h"
#include "ob_table_batch_service.h"
#include "ob_table_query_async_processor.h"
#include "group/ob_table_group_service.h"


namespace oceanbase
{
namespace observer
{


struct ObTableHbaseMutationInfo : public ObTableInfoBase
{
  ObTableHbaseMutationInfo() {}
  int init(const schema::ObSimpleTableSchemaV2 *simple_table_schema, share::schema::ObSchemaGetterGuard &schema_guard);
};


/// @see RPC_S(PR5 ls_op_execute, obrpc::OB_TABLE_API_LS_EXECUTE, (table::ObTableLSOpRequest), table::ObTableLSOpResult);
class ObTableLSExecuteP: public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LS_EXECUTE> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LS_EXECUTE> > ParentType;
  class LSExecuteIter;
  class HTableLSExecuteIter;

public:
  explicit ObTableLSExecuteP(const ObGlobalContext &gctx);
  virtual ~ObTableLSExecuteP();
  virtual int deserialize() override;

protected:
  virtual int check_arg() override;

  virtual int try_process() override;
  virtual void reset_ctx() override;
  virtual uint64_t get_request_checksum();
  virtual int before_process();
  virtual table::ObTableEntityType get_entity_type() override { return arg_.entity_type_; }
  virtual bool is_kv_processor() override { return true; }

private:
  int get_ls_id(ObLSID &ls_id, const share::schema::ObSimpleTableSchemaV2 *simple_table_schema);
  int check_arg_for_query_and_mutate(const table::ObTableSingleOp &single_op);
  int execute_tablet_op(const table::ObTableTabletOp &tablet_op,
                        uint64_t table_id,
                        table::ObKvSchemaCacheGuard *schema_cache_guard,
                        const ObSimpleTableSchemaV2 *simple_table_schema,
                        table::ObTableTabletOpResult &tablet_result);
  int execute_tablet_query_and_mutate(const uint64_t table_id,
                                      const table::ObTableTabletOp &tablet_op,
                                      table::ObTableTabletOpResult&tablet_result);
  int execute_single_query_and_mutate(const uint64_t table_id,
                                      const common::ObTabletID tablet_id,
                                      const table::ObTableSingleOp &single_op,
                                      table::ObTableSingleOpResult &result);
  // batch service
  int execute_tablet_batch_ops(const table::ObTableTabletOp &tablet_op,
                              uint64_t table_id,
                              table::ObKvSchemaCacheGuard *schema_cache_guard,
                              const ObSimpleTableSchemaV2 *simple_table_schema,
                              table::ObTableTabletOpResult&tablet_result);

  int try_process_tablegroup(table::ObTableLSOpResult &ls_result);

  int init_batch_ctx(const table::ObTableTabletOp &tablet_op,
                     ObIArray<table::ObTableOperation> &table_operations,
                     uint64_t table_id,
                     table::ObKvSchemaCacheGuard *shcema_cache_guard,
                     const ObSimpleTableSchemaV2 *table_schema,
                     table::ObTableTabletOpResult &tablet_result,
                     table::ObTableBatchCtx &batch_ctx);
  int init_tb_ctx(const table::ObTableTabletOp &tablet_op,
                  const table::ObTableOperation &table_operation,
                  table::ObKvSchemaCacheGuard *shcema_cache_guard,
                  const ObSimpleTableSchemaV2 *table_schema,
                  table::ObTableCtx &tb_ctx);

  int add_dict_and_bm_to_result_entity(const table::ObTableTabletOp &tablet_op,
                                       table::ObTableTabletOpResult &tablet_result);
  int execute_table_api_ls_op(table::ObTableLSOpResult &ls_result);

  int execute_htable_api_ls_op(table::ObTableLSOpResult &ls_result);
  // void record_aft_rtn_rows(const table::ObTableLSOpResult &ls_result);

  int find_htable_get_info(ObString &arg_table_name,
                           uint64_t &real_table_id,
                           ObTabletID &real_tablet_id,
                           bool &get_is_tablegroup,
                           ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> &table_schemas,
                           HTableLSExecuteIter &htable_iter,
                           const table::ObTableSingleOp &single_op);

  int aggregate_htable_get_result(common::ObIAllocator &allocator,
                                  table::ObTableQueryResultIterator *result_iterator,
                                  table::ObTableOperationResult &single_op_result);
  int64_t get_trans_timeout_ts();
  int init_query_info_tb_ctx(bool get_is_tablegroup, ObTableQueryAsyncCtx *query_ctx);
  int execute_htable_get(ObTableQueryAsyncCtx *query_ctx,
                         HTableLSExecuteIter &htable_ls_iter,
                         table::ObTableOperationResult &single_op_result);
  int init_query_ctx(bool get_is_tablegroup,
                     table::ObTableSingleOp &single_op,
                     ObSEArray<const ObSimpleTableSchemaV2 *, 8> &table_schemas,
                     uint64_t real_table_id,
                     ObTabletID &real_tablet_id,
                     ObTableQueryAsyncCtx *query_ctx,
                     ObIAllocator &allocator);
  int aggregate_single_op_result(ObTableLSExecuteP::HTableLSExecuteIter &htable_ls_iter,
                                 table::ObTableTabletOp &tablet_ops,
                                 table::ObTableTabletOpResult &tmp_tablet_result,
                                 table::ObTableTabletOpResult &tablet_result);
  int execute_htable_tablet_ops(table::ObTableTabletOp &tablet_ops,
                                ObTableLSExecuteP::HTableLSExecuteIter &htable_ls_iter,
                                table::ObTableTabletOpResult &tmp_tablet_result,
                                table::ObTableTabletOpResult &tablet_result);
  int create_cb_result(table::ObTableLSOpResult *&cb_result);
  int process_group_commit();
  int init_group_ctx(table::ObTableGroupCtx &ctx, share::ObLSID ls_id);

private:
  class LSExecuteIter {

  public:
    LSExecuteIter(ObTableLSExecuteP &outer_exectute_process);
    virtual ~LSExecuteIter();

    virtual int init();
    virtual void reuse() {};
    virtual int get_next_ctx(ObIArray<table::ObTableOperation> &table_operations,
                             table::ObTableTabletOpResult &tablet_result,
                             table::ObTableQueryBatchCtx *&ctx) = 0;
    virtual int set_tablet_ops(table::ObTableTabletOp &tablet_ops)
    {
      tablet_ops_ = &tablet_ops;
      return OB_SUCCESS;
    }

    virtual table::ObTableTabletOp *get_tablet_ops() { return tablet_ops_; }

    common::ObIAllocator &get_allocator() { return allocator_; }

  protected:
    int init_batch_ctx(uint64_t table_id,
                      table::ObTableSingleOp &single_op,
                      table::ObKvSchemaCacheGuard *shcema_cache_guard,
                      const ObSimpleTableSchemaV2 *simple_table_schema,
                      table::ObTableBatchCtx &batch_ctx);

    int init_tb_ctx(table::ObTableSingleOp &single_op,
                    table::ObKvSchemaCacheGuard *shcema_cache_guard,
                    const ObSimpleTableSchemaV2 *table_schema,
                    table::ObTableCtx &tb_ctx);

  protected:
    common::ObArenaAllocator allocator_;
    ObTableLSExecuteP &outer_exectute_process_;
    table::ObTableTabletOp *tablet_ops_;
    common::ObTabletID tablet_id_;
    ObArray<std::pair<std::pair<table::ObTableOperationType::Type, uint64_t>, table::ObTableBatchCtx *>> batch_ctxs_;
    ObTableQueryAsyncCtx *query_ctx_;
    uint64_t ops_timestamp_;

    DISALLOW_COPY_AND_ASSIGN(LSExecuteIter);
  };

  class ObTableLSExecuteIter : public LSExecuteIter
  {
  public:
    ObTableLSExecuteIter(ObTableLSExecuteP &outer_exectute_process);
    virtual ~ObTableLSExecuteIter() {};

    virtual int get_next_ctx(ObIArray<table::ObTableOperation> &table_operations,
                             table::ObTableTabletOpResult &tablet_result,
                             table::ObTableQueryBatchCtx *&ctx) override;

    DISALLOW_COPY_AND_ASSIGN(ObTableLSExecuteIter);
  };

  class HTableLSExecuteIter : public LSExecuteIter
  {
  public:
    explicit HTableLSExecuteIter(ObTableLSExecuteP &outer_exectute_process);
    virtual ~HTableLSExecuteIter();
    int init();
    int set_tablet_ops(table::ObTableTabletOp &tablet_ops) override;
    void reuse() override;
    table::ObTableTabletOp &get_same_ctx_ops() { return same_ctx_ops_; }

    virtual int get_next_ctx(ObIArray<table::ObTableOperation> &table_operations,
                             table::ObTableTabletOpResult &tablet_result,
                             table::ObTableQueryBatchCtx *&ctx) override;
    int find_real_tablet_id(uint64_t arg_table_id, uint64_t real_table_id, ObTabletID &real_tablet_id);

  private:
    int find_real_table_id(const ObString &family_name, uint64_t &real_table_id);

    int convert_batch_ctx(ObIArray<table::ObTableOperation> &table_operations,
                          table::ObTableTabletOpResult &tablet_result,
                          table::ObTableBatchCtx &ctx);
    int construct_delete_family_op(const table::ObTableSingleOp &single_op,
                                  const ObTableHbaseMutationInfo &mutation_info);
    int get_family_from_op(table::ObTableSingleOp &curr_single_op,
                          ObString &family);

    int modify_htable_quailfier_and_timestamp(const table::ObTableSingleOp &curr_single_op,
                                              table::ObTableOperationType::Type type,
                                              int64_t now_ms);
    int modify_htable_timestamp(const table::ObTableSingleOp &curr_single_op,
                                int64_t now_ms);

    int init_mutation_info(ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> &table_schemas);
    int init_tablegroup_batch_ctx(ObIArray<table::ObTableOperation> &table_operations,
                                  table::ObTableTabletOpResult &tablet_result,
                                  table::ObTableSingleOp &curr_single_op,
                                  table::ObTableBatchCtx *&batch_ctx);
    int init_table_batch_ctx(ObIArray<table::ObTableOperation> &table_operations,
                             table::ObTableTabletOpResult &tablet_result,
                             table::ObTableSingleOp &curr_single_op,
                             table::ObTableBatchCtx *&batch_ctx);
  public:
    ObArray<int64_t> origin_delete_pos_;
    ObArray<ObTableHbaseMutationInfo *> hbase_infos_;
    ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> table_schemas_;
  private:
    table::ObTableTabletOp same_ctx_ops_;
    uint64_t curr_op_index_;
    ObString table_group_name_;
    DISALLOW_COPY_AND_ASSIGN(HTableLSExecuteIter);
  };

private:
  common::ObArenaAllocator allocator_;
  table::ObTableEntityFactory<table::ObTableSingleOpEntity> *default_entity_factory_;
  table::ObTableLSExecuteCreateCbFunctor cb_functor_;
  table::ObTableLSExecuteEndTransCb *cb_;
  bool is_group_commit_;
};

}  // end namespace observer
}  // end namespace oceanbase

#endif /* _OB_TABLE_BATCH_EXECUTE_PROCESSOR_H */
