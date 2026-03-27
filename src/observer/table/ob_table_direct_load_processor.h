/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "observer/table_load/client/ob_table_direct_load_exec_context.h"
#include "observer/table/ob_table_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{

/// @see RPC_S(PR5 direct_load, obrpc::OB_TABLE_API_DIRECT_LOAD, (table::ObTableDirectLoadRequest), table::ObTableDirectLoadResult);
class ObTableDirectLoadP : public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_DIRECT_LOAD>>
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_DIRECT_LOAD>> ParentType;
public:
  explicit ObTableDirectLoadP(const ObGlobalContext &gctx);
  virtual ~ObTableDirectLoadP() = default;
protected:
  int check_arg() override;
  int try_process() override;
  table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) override;
  uint64_t get_request_checksum() override;
  virtual table::ObTableEntityType get_entity_type() override { return table::ObTableEntityType::ET_DYNAMIC; }
  virtual bool is_kv_processor() override { return false; }
private:
  ObTableDirectLoadExecContext exec_ctx_;
};

} // namespace observer
} // namespace oceanbase
