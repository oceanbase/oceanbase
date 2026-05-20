/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TABLET_OB_DROP_GTT_V2_SESSION_TABLET_RPC_H
#define OCEANBASE_STORAGE_TABLET_OB_DROP_GTT_V2_SESSION_TABLET_RPC_H

#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/tablet/ob_drop_gtt_v2_session_tablet_arg.h"
#include "observer/ob_server_struct.h"
#include "storage/tablet/ob_session_tablet_info_map.h"

namespace oceanbase
{
namespace storage
{

class ObRpcDropGTTV2SessionTabletP : public obrpc::ObRpcProcessor<
    obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_DROP_GTT_V2_SESSION_TABLET>>
{
public:
  explicit ObRpcDropGTTV2SessionTabletP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObRpcDropGTTV2SessionTabletP() = default;
protected:
  int process() override;
public:
  static int handle_in_tenant(const share::ObDropGTTV2SessionTabletArg &arg,
                              share::ObDropGTTV2SessionTabletRes &result);
private:
  static int do_delete_as_creator(const share::ObDropGTTV2SessionTabletArg &arg,
                                  common::ObIArray<ObSessionTabletInfo> &creator_tablet_infos);
  const observer::ObGlobalContext &gctx_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TABLET_OB_DROP_GTT_V2_SESSION_TABLET_RPC_H
