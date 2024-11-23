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


namespace oceanbase
{
namespace observer
{


struct ObTableHbaseMutationInfo : public ObTableInfoBase
{
  ObTableHbaseMutationInfo(common::ObIAllocator &allocator)
      : ctx_(allocator)
  {}

  int init(const schema::ObSimpleTableSchemaV2 *simple_table_schema, share::schema::ObSchemaGetterGuard &schema_guard);
  TO_STRING_KV(K(ctx_));

private:
  table::ObTableCtx ctx_;
};


/// @see RPC_S(PR5 ls_op_execute, obrpc::OB_TABLE_API_LS_EXECUTE, (table::ObTableLSOpRequest), table::ObTableLSOpResult);
class ObTableLSExecuteP: public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LS_EXECUTE> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LS_EXECUTE> > ParentType;
  class LSExecuteIter;

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
  int execute_ls_op(table::ObTableLSOpResult &ls_result);

  int execute_ls_op_tablegroup(table::ObTableLSOpResult &ls_result);
  // void record_aft_rtn_rows(const table::ObTableLSOpResult &ls_result);

private:
  class LSExecuteIter {

  public:
    LSExecuteIter(ObTableLSExecuteP &outer_exectute_process);
    virtual ~LSExecuteIter();

    virtual int init();
    virtual void reset() {};
    virtual int get_next_batch_ctx(table::ObTableTabletOpResult &tablet_result,
                                   table::ObTableBatchCtx *&batch_ctx) = 0;
    virtual int set_tablet_ops(table::ObTableTabletOp &tablet_ops)
    {
      tablet_ops_(tablet_ops);
      return OB_SUCCESS;
    }

  protected:
    int init_batch_ctx(uint64_t table_id,
                      table::ObTableSingleOp &single_op,
                      table::ObKvSchemaCacheGuard *shcema_cache_guard,
                      const ObSimpleTableSchemaV2 *simple_table_schema,
                      table::ObTableTabletOpResult &tablet_result,
                      table::ObTableBatchCtx &batch_ctx);

    int init_tb_ctx(table::ObTableSingleOp &single_op,
                    table::ObKvSchemaCacheGuard *shcema_cache_guard,
                    const ObSimpleTableSchemaV2 *table_schema,
                    table::ObTableCtx &tb_ctx);

  protected:
    ObTableLSExecuteP &outer_exectute_process_;
    table::ObTableTabletOp tablet_ops_;
    common::ObTabletID tablet_id_;
    common::ObArenaAllocator allocator_;
    ObArray<std::pair<std::pair<table::ObTableOperationType::Type, uint64_t>, table::ObTableBatchCtx *>> batch_ctxs_;
    uint64_t ops_timestamp_;

    DISALLOW_COPY_AND_ASSIGN(LSExecuteIter);
  };

  class ObTableLSExecuteIter : public LSExecuteIter {
  public:
    ObTableLSExecuteIter(ObTableLSExecuteP &outer_exectute_process);
    virtual ~ObTableLSExecuteIter() {};

    int get_next_batch_ctx(table::ObTableTabletOpResult &tablet_result,
                           table::ObTableBatchCtx *&batch_ctx) override;

    DISALLOW_COPY_AND_ASSIGN(ObTableLSExecuteIter);
  };

  class HTableLSExecuteIter : public LSExecuteIter {
  public:
    explicit HTableLSExecuteIter(ObTableLSExecuteP &outer_exectute_process);
    virtual ~HTableLSExecuteIter() {};
    int init();
    int set_tablet_ops(table::ObTableTabletOp &tablet_ops) override;
    void reset() override;
    table::ObTableTabletOp &get_same_ctx_ops() { return same_ctx_ops_; }

    int get_next_batch_ctx(table::ObTableTabletOpResult &tablet_result,
                           table::ObTableBatchCtx *&batch_ctx) override;

  private:
    int find_real_table_id(const ObString &family_name, uint64_t &real_table_id);
    int find_real_tablet_id(uint64_t arg_table_id, uint64_t real_table_id, ObTabletID &real_tablet_id);
    int convert_batch_ctx(table::ObTableBatchCtx &batch_ctx);
    int init_multi_schema_info(const ObString &arg_tablegroup_name);
    int construct_delete_family_op(const table::ObTableSingleOp &single_op,
                                  const ObTableHbaseMutationInfo &mutation_info);
    int get_family_from_op(table::ObTableSingleOp &curr_single_op,
                          ObString &family);

    int modify_htable_quailfier_and_timestamp(const table::ObTableSingleOp &curr_single_op,
                                              table::ObTableOperationType::Type type,
                                              int64_t now_ms);

  private:
    table::ObTableTabletOp same_ctx_ops_;
    uint64_t curr_op_index_;
    ObString table_group_name_;
    ObArray<ObTableHbaseMutationInfo *> hbase_infos_;
    ObArray<table::ObTableOperation> table_operations_;
    DISALLOW_COPY_AND_ASSIGN(HTableLSExecuteIter);
  };

private:
  common::ObArenaAllocator allocator_;
  table::ObTableEntityFactory<table::ObTableSingleOpEntity> *default_entity_factory_;
  table::ObTableLSExecuteCreateCbFunctor cb_functor_;
  table::ObTableLSExecuteEndTransCb *cb_;
};

}  // end namespace observer
}  // end namespace oceanbase

#endif /* _OB_TABLE_BATCH_EXECUTE_PROCESSOR_H */
