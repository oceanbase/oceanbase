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

#ifndef OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_RPC_PROXY_H_
#define OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_RPC_PROXY_H_

#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "share/rpc/ob_async_rpc_proxy.h"

namespace oceanbase
{

namespace share
{
class ObWrCreateSnapshotArg;
class ObWrPurgeSnapshotArg;
class ObWrUserSubmitSnapArg;
class ObWrUserSubmitSnapResp;
class ObWrUserModifySettingsArg;
}  // namespace share

namespace obrpc
{
class ObWrRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObWrRpcProxy);
  virtual ~ObWrRpcProxy() {}
  RPC_AP(@PR5 wr_async_take_snapshot, obrpc::OB_WR_ASYNC_SNAPSHOT_TASK,
      (share::ObWrCreateSnapshotArg));
  RPC_AP(@PR5 wr_async_purge_snapshot, obrpc::OB_WR_ASYNC_PURGE_SNAPSHOT_TASK,
      (share::ObWrPurgeSnapshotArg));
  RPC_S(@PR5 wr_sync_user_submit_snapshot_task, obrpc::OB_WR_SYNC_USER_SUBMIT_SNAPSHOT_TASK,
      (share::ObWrUserSubmitSnapArg), share::ObWrUserSubmitSnapResp);
  RPC_S(@PR5 wr_sync_user_modify_settings_task, obrpc::OB_WR_SYNC_USER_MODIFY_SETTINGS_TASK,
      (share::ObWrUserModifySettingsArg));
};

}  // namespace obrpc
}  // namespace oceanbase
#endif  // OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_RPC_PROXY_H_
