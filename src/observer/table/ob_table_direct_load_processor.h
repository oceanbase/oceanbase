/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
  virtual bool is_kv_processor() override { return false; }
private:
  common::ObArenaAllocator allocator_;
  ObTableDirectLoadExecContext exec_ctx_;
};

} // namespace observer
} // namespace oceanbase
