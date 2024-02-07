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

#ifndef OCEANBASE_STORAGE_STORAGE_ASYNC_RPC_H_
#define OCEANBASE_STORAGE_STORAGE_ASYNC_RPC_H_

#include "share/ob_rpc_struct.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/rpc/ob_async_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "rpc/obrpc/ob_rpc_proxy.h"

namespace oceanbase
{
namespace storage
{

#define RPC_HA(code, arg, result, name) \
  typedef obrpc::ObAsyncRpcProxy<code, arg, result, \
    int (obrpc::ObStorageRpcProxy::*)(const arg &, obrpc::ObStorageRpcProxy::AsyncCB<code> *, const obrpc::ObRpcOpts &), obrpc::ObStorageRpcProxy> name

RPC_HA(obrpc::OB_HA_CHECK_TRANSFER_TABLET_BACKFILL, obrpc::ObCheckTransferTabletBackfillArg, obrpc::ObCheckTransferTabletBackfillRes, ObCheckTransferTabletBackfillProxy);
RPC_HA(obrpc::OB_HA_CHANGE_MEMBER_SERVICE, obrpc::ObStorageChangeMemberArg, obrpc::ObStorageChangeMemberRes, ObHAChangeMemberProxy);
RPC_HA(obrpc::OB_CHECK_START_TRANSFER_TABLETS, obrpc::ObTransferTabletInfoArg, obrpc::ObStorageRpcProxy::ObRpc<obrpc::OB_CHECK_START_TRANSFER_TABLETS>::Response, ObCheckStartTransferTabletsProxy);
RPC_HA(obrpc::OB_HA_FETCH_LS_REPLAY_SCN, obrpc::ObFetchLSReplayScnArg, obrpc::ObFetchLSReplayScnRes, ObFetchLSReplayScnProxy);

}//end namespace storage
}//end namespace oceanbase

#endif //OCEANBASE_STORAGE_STORAGE_ASYNC_RPC_H_
