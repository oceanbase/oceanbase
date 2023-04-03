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

#ifndef OCEANBASE_SHARE_OB_RL_RPC_H
#define OCEANBASE_SHARE_OB_RL_RPC_H

#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/utility.h"
#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/frame/ob_net_easy.h"
#include "ob_rl_struct.h"

namespace oceanbase {
namespace share {

using namespace oceanbase::obrpc;

class ObRatelimitMgr;

class ObRlGetRegionBWRequest
{
  OB_UNIS_VERSION(1);

public:
  ObRlGetRegionBWRequest() {}
  ~ObRlGetRegionBWRequest() {}
  // TO_STRING_KV(K_(addr));

public:
  common::ObAddr addr_;
};

class ObRlGetRegionBWResponse
{
  OB_UNIS_VERSION(1);

public:
  ObRlGetRegionBWResponse() {}
  ~ObRlGetRegionBWResponse() {}
  // TO_STRING_KV(K_(addr));

public:
  common::ObAddr addr_;
  ObRegionBwSEArray ob_region_bw_list_;
};

class ObRatelimitProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObRatelimitProxy);

  RPC_AP(PR1 get_region_bw, OB_GET_REGION_BW, (ObRlGetRegionBWRequest), ObRlGetRegionBWResponse);
};

class ObRLGetRegionBWCallback : public ObRatelimitProxy::AsyncCB<obrpc::OB_GET_REGION_BW>
{
public:
  ObRLGetRegionBWCallback() {is_inited_ = false; rl_mgr_ = NULL;}
  ~ObRLGetRegionBWCallback() {}

  int init(ObRatelimitMgr *rl_mgr, common::ObAddr &self_addr);
  void destroy();
  void set_args(const Request &arg);
  void set_dst_addr(common::ObAddr &dst_addr);
  void set_dst_server_idx(int dst_server_idx);
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const override;
  virtual void on_invalid() override;
  virtual int on_error(int easy_err) override;
  virtual int process() override;

private:
  bool is_inited_;
  int dst_server_idx_;
  common::ObAddr dst_server_addr_;
  common::ObAddr self_addr_;
  ObRatelimitMgr *rl_mgr_;
};

class ObRLGetRegionBWP : public ObRpcProcessor<ObRatelimitProxy::ObRpc<OB_GET_REGION_BW>>
{
public:
  explicit ObRLGetRegionBWP(const observer::ObGlobalContext &global_ctx);
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObRLGetRegionBWP);

private:
  const observer::ObGlobalContext *global_ctx_;
  rpc::frame::ObNetEasy *net_;
  ObRatelimitMgr *rl_mgr_;
};

class ObRatelimitRPC
{
  enum {
    RL_RPC_TIMEOUT = 100 * 1000, // 100ms
  };
public:
  ObRatelimitRPC();
  int get_server_bw(common::ObAddr &ob_addr, int idx);
  int init(common::ObAddr& self_addr,
           oceanbase::rpc::frame::ObNetEasy *net,
           observer::ObGlobalContext *gctx,
           rpc::frame::ObReqTransport *transport,
           ObRatelimitMgr *rl_mgr);
  void destroy();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t dst_cluster_id_;
  common::ObAddr self_addr_;
  const observer::ObGlobalContext *gctx_;
  oceanbase::rpc::frame::ObNetEasy *net_;
  ObRatelimitMgr *rl_mgr_;
  ObRatelimitProxy        rl_rpc_proxy_;
  ObRLGetRegionBWCallback rl_rpc_cb_;
};

} // namesapce share
} // namespace oceanbase

#endif