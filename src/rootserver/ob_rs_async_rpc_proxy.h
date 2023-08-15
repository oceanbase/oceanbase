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

#ifndef OCEANBASE_ROOTSERVER_OB_RS_ASYNC_RPC_PROXY_H_
#define OCEANBASE_ROOTSERVER_OB_RS_ASYNC_RPC_PROXY_H_

#include "share/ob_rpc_struct.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/rpc/ob_async_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_heartbeat_struct.h"

namespace oceanbase
{
namespace rootserver
{

RPC_F(obrpc::OB_MINOR_FREEZE, obrpc::ObMinorFreezeArg,
      obrpc::Int64, ObMinorFreezeProxy);
RPC_F(obrpc::OB_GET_WRS_INFO, obrpc::ObGetWRSArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_GET_WRS_INFO>::Response, ObGetWRSProxy);
RPC_F(obrpc::OB_CHECK_SCHEMA_VERSION_ELAPSED, obrpc::ObCheckSchemaVersionElapsedArg,
    obrpc::ObCheckSchemaVersionElapsedResult, ObCheckSchemaVersionElapsedProxy);
RPC_F(obrpc::OB_DDL_BUILD_SINGLE_REPLICA_REQUEST, obrpc::ObDDLBuildSingleReplicaRequestArg,
    obrpc::ObDDLBuildSingleReplicaRequestResult, ObDDLBuildSingleReplicaRequestProxy);
RPC_F(obrpc::OB_CHECK_MODIFY_TIME_ELAPSED, obrpc::ObCheckModifyTimeElapsedArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_CHECK_MODIFY_TIME_ELAPSED>::Response, ObCheckCtxCreateTimestampElapsedProxy);
RPC_F(obrpc::OB_PARTITION_STOP_WRITE, obrpc::Int64,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_PARTITION_STOP_WRITE>::Response, ObRpcStopWriteProxy);
RPC_F(obrpc::OB_PARTITION_CHECK_LOG, obrpc::Int64,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_PARTITION_CHECK_LOG>::Response, ObRpcCheckLogProxy);
RPC_F(obrpc::OB_NOTIFY_TENANT_SERVER_UNIT_RESOURCE, obrpc::TenantServerUnitConfig,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_NOTIFY_TENANT_SERVER_UNIT_RESOURCE>::Response, ObNotifyTenantServerResourceProxy);
RPC_F(obrpc::OB_CHECK_FROZEN_SCN, obrpc::ObCheckFrozenScnArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_CHECK_FROZEN_SCN>::Response, ObCheckFrozenScnProxy);
RPC_F(obrpc::OB_GET_MIN_SSTABLE_SCHEMA_VERSION, obrpc::ObGetMinSSTableSchemaVersionArg,
    obrpc::ObGetMinSSTableSchemaVersionRes, ObGetMinSSTableSchemaVersionProxy);
RPC_F(obrpc::OB_DETECT_MASTER_RS_LS, obrpc::ObDetectMasterRsArg,
      obrpc::ObDetectMasterRsLSResult, ObDetectMasterRsLSProxy);
RPC_F(obrpc::OB_GET_ROOT_SERVER_ROLE, obrpc::ObDetectMasterRsArg,
      obrpc::ObGetRootserverRoleResult, ObGetRootserverRoleProxy);
RPC_F(obrpc::OB_CREATE_LS, obrpc::ObCreateLSArg, obrpc::ObCreateLSResult, ObLSCreatorProxy);
RPC_F(obrpc::OB_CREATE_TABLET, obrpc::ObBatchCreateTabletArg, obrpc::ObCreateTabletBatchRes, ObTabletCreatorProxy);
RPC_F(obrpc::OB_SET_MEMBER_LIST, obrpc::ObSetMemberListArgV2, obrpc::ObSetMemberListResult, ObSetMemberListProxy);
RPC_F(obrpc::OB_BATCH_BROADCAST_SCHEMA, obrpc::ObBatchBroadcastSchemaArg, obrpc::ObBatchBroadcastSchemaResult, ObBatchBroadcastSchemaProxy);
RPC_F(obrpc::OB_DROP_TABLET, obrpc::ObBatchRemoveTabletArg, obrpc::ObRemoveTabletRes, ObTabletDropProxy);
RPC_F(obrpc::OB_SWITCH_SCHEMA, obrpc::ObSwitchSchemaArg, obrpc::ObSwitchSchemaResult, ObSwitchSchemaProxy);
RPC_F(obrpc::OB_GET_LS_ACCESS_MODE, obrpc::ObGetLSAccessModeInfoArg, obrpc::ObLSAccessModeInfo, ObGetLSAccessModeProxy);
RPC_F(obrpc::OB_CHANGE_LS_ACCESS_MODE, obrpc::ObLSAccessModeInfo, obrpc::ObChangeLSAccessModeRes, ObChangeLSAccessModeProxy);
RPC_F(obrpc::OB_BATCH_GET_TABLET_AUTOINC_SEQ, obrpc::ObBatchGetTabletAutoincSeqArg,
    obrpc::ObBatchGetTabletAutoincSeqRes, ObBatchGetTabletAutoincSeqProxy);
RPC_F(obrpc::OB_BATCH_SET_TABLET_AUTOINC_SEQ, obrpc::ObBatchSetTabletAutoincSeqArg,
    obrpc::ObBatchSetTabletAutoincSeqRes, ObBatchSetTabletAutoincSeqProxy);
RPC_F(obrpc::OB_GET_LS_SYNC_SCN, obrpc::ObGetLSSyncScnArg, obrpc::ObGetLSSyncScnRes, ObGetLSSyncScnProxy);
RPC_F(obrpc::OB_INIT_TENANT_CONFIG, obrpc::ObInitTenantConfigArg,
    obrpc::ObInitTenantConfigRes, ObInitTenantConfigProxy);
RPC_F(obrpc::OB_GET_LEADER_LOCATIONS, obrpc::ObGetLeaderLocationsArg,
      obrpc::ObGetLeaderLocationsResult, ObGetLeaderLocationsProxy);
RPC_F(obrpc::OB_DDL_CHECK_TABLET_MERGE_STATUS, obrpc::ObDDLCheckTabletMergeStatusArg,
    obrpc::ObDDLCheckTabletMergeStatusResult, ObCheckTabletMergeStatusProxy);
RPC_F(obrpc::OB_REFRESH_TENANT_INFO, obrpc::ObRefreshTenantInfoArg, obrpc::ObRefreshTenantInfoRes, ObRefreshTenantInfoProxy);
RPC_F(obrpc::OB_GET_LS_REPLAYED_SCN, obrpc::ObGetLSReplayedScnArg,
      obrpc::ObGetLSReplayedScnRes, ObGetLSReplayedScnProxy);
RPC_F(obrpc::OB_SEND_HEARTBEAT, share::ObHBRequest, share::ObHBResponse, ObSendHeartbeatProxy);
RPC_F(obrpc::OB_GET_SERVER_RESOURCE_INFO, obrpc::ObGetServerResourceInfoArg, obrpc::ObGetServerResourceInfoResult, ObGetServerResourceInfoProxy);
RPC_F(obrpc::OB_NOTIFY_SWITCH_LEADER, obrpc::ObNotifySwitchLeaderArg,
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_NOTIFY_SWITCH_LEADER>::Response, ObNotifySwitchLeaderProxy);
RPC_F(obrpc::OB_UPDATE_TENANT_INFO_CACHE, obrpc::ObUpdateTenantInfoCacheArg, obrpc::ObUpdateTenantInfoCacheRes, ObUpdateTenantInfoCacheProxy);
RPC_F(obrpc::OB_BROADCAST_CONSENSUS_VERSION, obrpc::ObBroadcastConsensusVersionArg, obrpc::ObBroadcastConsensusVersionRes, ObBroadcstConsensusVersionProxy);
#ifdef OB_BUILD_TDE_SECURITY
RPC_F(obrpc::OB_RESTORE_KEY, obrpc::ObRestoreKeyArg, obrpc::ObRestoreKeyResult, ObRestoreKeyProxy);
RPC_F(obrpc::OB_SET_ROOT_KEY, obrpc::ObRootKeyArg, obrpc::ObRootKeyResult, ObSetRootKeyProxy);
#endif
}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_RS_ASYNC_RPC_PROXY_H_
