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

#include "lib/allocator/page_arena.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{

/// RPC_S(PR5 direct_load_control, OB_DIRECT_LOAD_CONTROL, (observer::ObDirectLoadControlRequest), observer::ObDirectLoadControlResult);
class ObDirectLoadControlP : public obrpc::ObRpcProcessor<obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_DIRECT_LOAD_CONTROL>>
{
public:
  ObDirectLoadControlP(const ObGlobalContext &gctx) : gctx_(gctx), allocator_("TLD_RpcP")
  {
    allocator_.set_tenant_id(MTL_ID());
  }
protected:
  int process();
private:
  const ObGlobalContext &gctx_ __maybe_unused;
  ObArenaAllocator allocator_;
};

} // namespace observer
} // namespace oceanbase
