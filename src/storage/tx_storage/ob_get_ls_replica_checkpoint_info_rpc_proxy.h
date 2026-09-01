/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_TX_STORAGE_OB_GET_LS_REPLICA_CHECKPOINT_INFO_RPC_PROXY_H_
#define OCEANBASE_STORAGE_TX_STORAGE_OB_GET_LS_REPLICA_CHECKPOINT_INFO_RPC_PROXY_H_

#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "storage/tx_storage/ob_get_ls_replica_checkpoint_info.h"

namespace oceanbase
{
namespace obrpc
{

class ObGetLSReplicaCheckpointInfoRpcProxy : public ObRpcProxy
{
public:
  DEFINE_TO(ObGetLSReplicaCheckpointInfoRpcProxy);

  RPC_S(PR1 get_ls_replica_checkpoint_info,
      OB_GET_LS_REPLICA_CHECKPOINT_INFO,
      (ObGetLSReplicaCheckpointInfoArg),
      ObGetLSReplicaCheckpointInfoRes);
};

class ObRpcGetLSReplicaCheckpointInfoP : public ObRpcProcessor<
    ObGetLSReplicaCheckpointInfoRpcProxy::ObRpc<OB_GET_LS_REPLICA_CHECKPOINT_INFO> >
{
public:
  ObRpcGetLSReplicaCheckpointInfoP() {}
  virtual ~ObRpcGetLSReplicaCheckpointInfoP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcGetLSReplicaCheckpointInfoP);
};

} // namespace obrpc
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TX_STORAGE_OB_GET_LS_REPLICA_CHECKPOINT_INFO_RPC_PROXY_H_
