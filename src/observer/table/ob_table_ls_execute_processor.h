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
  table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) override;
  virtual void audit_on_finish() override;
  virtual uint64_t get_request_checksum();
  int check_has_global_index(bool &exist_global_index);

private:
  int get_ls_id(ObLSID &ls_id);
  int execute_tablet_op(const table::ObTableTabletOp &tablet_op, table::ObTableTabletOpResult&tablet_result);
  int execute_single_op(const uint64_t table_id,
                        const common::ObTabletID tablet_id,
                        const table::ObTableSingleOp &single_op,
                        table::ObTableSingleOpResult &result);
private:
  common::ObArenaAllocator allocator_;
  table::ObTableEntityFactory<table::ObTableSingleOpEntity> default_entity_factory_;
  table::ObTableCtx tb_ctx_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_BATCH_EXECUTE_PROCESSOR_H */
