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

#ifndef OCEANBASE_STORAGE_NET_THROT_RPC_PROXY_H_
#define OCEANBASE_STORAGE_NET_THROT_RPC_PROXY_H_

#include "observer/net/ob_shared_storage_net_throt_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_define.h"
#include "share/rpc/ob_async_rpc_proxy.h"

namespace oceanbase
{

namespace obrpc
{
RPC_F(OB_SHARED_STORAGE_NET_THROT_PREDICT, obrpc::ObSSNTEndpointArg, obrpc::ObSharedDeviceResourceArray,
    ObSharedStorageNetThrotPredictProxy);
RPC_F(OB_SHARED_STORAGE_NET_THROT_SET, obrpc::ObSharedDeviceResourceArray, obrpc::ObSSNTSetRes,
    ObSharedStorageNetThrotSetProxy);
class ObSSNTRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObSSNTRpcProxy);
  RPC_S(PR5 shared_storage_net_throt_register, OB_SHARED_STORAGE_NET_THROT_REGISTER, (obrpc::ObSSNTEndpointArg));
};
}  // namespace obrpc
}  // namespace oceanbase
#endif /* OCEANBASE_STORAGE_NET_THROT_RPC_PROXY_H_ */