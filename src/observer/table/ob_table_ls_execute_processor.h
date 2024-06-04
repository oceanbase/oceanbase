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
/// @see RPC_S(PR5 ls_op_execute, obrpc::OB_TABLE_API_LS_EXECUTE, (table::ObTableLSOpRequest), table::ObTableLSOpResult);
class ObTableLSExecuteP: public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LS_EXECUTE> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_LS_EXECUTE> > ParentType;
public:
  explicit ObTableLSExecuteP(const ObGlobalContext &gctx);
  virtual ~ObTableLSExecuteP() = default;
  virtual int deserialize() override;

protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  virtual uint64_t get_request_checksum();
  virtual int before_process();

private:
  int get_ls_id(ObLSID &ls_id);
  int check_arg_for_query_and_mutate(const table::ObTableSingleOp &single_op);
  int execute_tablet_op(const table::ObTableTabletOp &tablet_op, table::ObTableTabletOpResult&tablet_result);
  int execute_tablet_query_and_mutate(const table::ObTableTabletOp &tablet_op, table::ObTableTabletOpResult&tablet_result);
  int execute_single_query_and_mutate(const uint64_t table_id,
                                      const common::ObTabletID tablet_id,
                                      const table::ObTableSingleOp &single_op,
                                      table::ObTableSingleOpResult &result);
  // batch service
  int execute_tablet_batch_ops(const table::ObTableTabletOp &tablet_op, table::ObTableTabletOpResult&tablet_result);
  int init_batch_ctx(table::ObTableBatchCtx &batch_ctx,
                     const table::ObTableTabletOp &tablet_op,
                     ObIArray<table::ObTableOperation> &table_operations,
                     table::ObTableTabletOpResult &tablet_result);
  int init_tb_ctx(table::ObTableCtx &tb_ctx,
                  const table::ObTableTabletOp &tablet_op,
                  const table::ObTableOperation &table_operation);
  int add_dict_and_bm_to_result_entity(const table::ObTableTabletOp &tablet_op,
                                       table::ObTableTabletOpResult &tablet_result);
  int add_tablet_result(const table::ObTableTabletOpResult &tablet_result);
private:
  common::ObArenaAllocator allocator_;
  table::ObTableEntityFactory<table::ObTableSingleOpEntity> default_entity_factory_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_BATCH_EXECUTE_PROCESSOR_H */
