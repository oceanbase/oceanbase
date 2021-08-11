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

#ifndef _OB_TABLE_EXECUTE_PROCESSOR_H
#define _OB_TABLE_EXECUTE_PROCESSOR_H 1
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "ob_table_service.h"

namespace oceanbase
{
namespace observer
{
/// @see RPC_S(PR5 execute, obrpc::OB_TABLE_API_EXECUTE, (table::ObTableOperationRequest), table::ObTableOperationResult);
class ObTableApiExecuteP: public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE> > ParentType;
public:
  explicit ObTableApiExecuteP(const ObGlobalContext &gctx);
  virtual ~ObTableApiExecuteP() = default;

  virtual int deserialize() override;
  virtual int process() override;
  virtual int response(const int retcode) override;
protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) override;
  virtual void audit_on_finish() override;
  virtual uint64_t get_request_checksum() override;

private:
  int check_arg2() const;
  int revert_get_ctx();
  int get_partition_id(uint64_t table_id, const ObRowkey &rowkey, uint64_t &partition_id);
  int process_get();
  int process_insert();
  int process_del();
  int process_update();
  int process_insert_or_update();
  int process_replace();
  int process_increment();
private:
  table::ObTableEntity request_entity_;
  table::ObTableEntity result_entity_;
  common::ObArenaAllocator allocator_;
  table::ObTableEntityFactory<table::ObTableEntity> default_entity_factory_;
  // the life of scan_ctx_ should be longer than process()
  ObTableServiceGetCtx get_ctx_;
  bool need_rollback_trans_;
  int64_t query_timeout_ts_;
};


} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_EXECUTE_PROCESSOR_H */
