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

#define private public
#include "logservice/arbserver/ob_arb_srv_network_frame.h"
#include "logservice/arbserver/ob_arb_server_timer.h"
#undef private
#include "ob_simple_arb_server.h"

namespace oceanbase
{
namespace palflite
{
int PalfEnvLiteMgr::get_palf_env_lite(const palflite::PalfEnvKey &key,
                                      PalfEnvLite *&palf_env_lite)
{
  // NB:保证arb server的node id为1002
  palf_env_lite = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(palf_env_lite_map_.get(key, palf_env_lite))) {
    CLOG_LOG(WARN, "get from map failed", KR(ret), KPC(this), K(key));
  } else if (true == palf_env_lite->has_set_deleted()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
  }
  if (OB_FAIL(ret) && NULL != palf_env_lite) {
    palf_env_lite_map_.revert(palf_env_lite);
    palf_env_lite = NULL;
  }
  return ret;
}
}

namespace unittest
{

ObSimpleArbServer::ObSimpleArbServer()
  : palf_env_mgr_(),
    srv_network_frame_(),
    timer_(),
    allocator_(1),
    is_inited_(false)
{}

ObSimpleArbServer::~ObSimpleArbServer()
{
  if (NULL != tenant_base_) {
    destroy();
    ob_delete(tenant_base_);
    tenant_base_ = NULL;
  }
  CLOG_LOG_RET(WARN, OB_SUCCESS, "reset tenant_base_");
}

int ObSimpleArbServer::simple_init(const std::string &cluster_name,
                                   const common::ObAddr &addr,
                                   const int64_t node_id,
                                   bool is_bootstrap)
{
  const std::string logserver_dir = cluster_name + "/port_" + std::to_string(addr.get_port());

  ObBaseLogWriterCfg cfg;
  std::string clog_dir = logserver_dir + "/clog";
  rpc::frame::ObNetOptions opts;
  arbserver::ArbNetOptions arb_opts;
  opts.rpc_io_cnt_ = 10;                     // rpc io thread count
  opts.high_prio_rpc_io_cnt_ = 0;            // high priority io thread count
  opts.mysql_io_cnt_ = 0;                    // mysql io thread count
  opts.batch_rpc_io_cnt_ = 0;                // batch rpc io thread count
  opts.tcp_user_timeout_ = 10 * 1000 * 1000; // 10s
  arb_opts.opts_ = opts;
  arb_opts.rpc_port_ = static_cast<uint32_t>(addr.get_port());
  arb_opts.self_ = addr;
  arb_opts.negotiation_enable_ = 0;
  arb_opts.mittest_ = true;
  ObMemAttr ele_attr(1, ObNewModIds::OB_ELECTION);
  int ret = OB_SUCCESS;
  tenant_base_ = OB_NEW(ObTenantBase, "TestBase", node_id);
  auto malloc = ObMallocAllocator::get_instance();
  if (NULL == malloc->get_tenant_ctx_allocator(node_id, 0)) {
    malloc->create_and_add_tenant_allocator(node_id);
  }
  tenant_base_->init();
  ObTenantEnv::set_tenant(tenant_base_);
  std::vector<std::string> dirs{logserver_dir, clog_dir};

  for (auto &dir : dirs) {
    if (-1 == mkdir(dir.c_str(), 0777)) {
      if (errno == EEXIST) {
        ret = OB_SUCCESS;
        SERVER_LOG(INFO, "for restart");
      } else {
        SERVER_LOG(WARN, "mkdir failed", K(errno));
        ret = OB_IO_ERROR;
        break;
      }
    }
  }
  cfg.group_commit_max_item_cnt_ = 1;
  cfg.group_commit_min_item_cnt_ = 1;
  if (is_bootstrap && OB_FAIL(OB_LOGGER.init(cfg, true))) {
    SERVER_LOG(ERROR, "OB_LOGGER init failed");
  }
  if (OB_FAIL(ret)) {
  } else if (is_bootstrap && OB_FAIL(blacklist_.init(addr))) {
    CLOG_LOG(WARN, "blacklist_ init failed", K(ret), K(opts));
  } else if (is_bootstrap && OB_FAIL(srv_network_frame_.init(arb_opts, &palf_env_mgr_))) {
    CLOG_LOG(WARN, "ObArbSrvNetWorkFrame init failed", K(ret), K(opts));
  } else if (OB_FAIL(palf_env_mgr_.init(logserver_dir.c_str(), addr,
          srv_network_frame_.get_req_transport()))) {
    CLOG_LOG(WARN, "PalfEnvLiteMgr init failed", K(ret), K(addr), K(clog_dir.c_str()));
  } else if (OB_FAIL(mock_election_map_.init(ele_attr))) {
    SERVER_LOG(ERROR, "mock_election_map_ init fail", K(ret));
  } else if (OB_FAIL(timer_.init(lib::TGDefIDs::ArbServerTimer, &palf_env_mgr_))) {
    CLOG_LOG(WARN, "timer init failed", K(ret), K(addr), K(clog_dir.c_str()));
  } else {
    filter_ = [this, &ret](const ObAddr &src) -> bool {
      if (blacklist_.need_filter_packet_by_blacklist(src)) {
        SERVER_LOG(WARN, "need_filter_packet_by_blacklist", K(src));
        return true;
      } else if (blacklist_.need_drop_by_loss_config(src)) {
        SERVER_LOG(WARN, "need_drop_by_loss_config", K(src));
        return true;
      }
      return false;
    };
    srv_network_frame_.normal_rpc_qhandler_.filter_ = &filter_;
    base_dir_ = clog_dir;
    self_ = addr;
    node_id_ = node_id;
    timer_.timer_interval_ = 1000*1000;
    is_inited_ = true;
    CLOG_LOG(INFO, "simple_arb_server init success", K(node_id), KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObSimpleArbServer::destroy()
{
  is_inited_ = false;
  srv_network_frame_.destroy();
  palf_env_mgr_.destroy();
  timer_.destroy();
}


int ObSimpleArbServer::simple_start(const bool is_bootstrat)
{
  int ret = OB_SUCCESS;
  palflite::PalfEnvKey key(cluster_id_, OB_SERVER_TENANT_ID);
  arbserver::GCMsgEpoch epoch = arbserver::GCMsgEpoch(1, 1);
  if (OB_FAIL(srv_network_frame_.start())) {
    CLOG_LOG(WARN, "start ObArbSrvNetWorkFrame failed", K(ret));
  } else if (OB_FAIL(palf_env_mgr_.add_cluster(self_, cluster_id_, "arbserver_test", epoch))) {
  } else if (OB_FAIL(palf_env_mgr_.create_palf_env_lite(key))) {
    CLOG_LOG(WARN, "PalfEnvLiteMgr create_palf_env_lite failed", K(ret));
  } else if (OB_FAIL(timer_.start())) {
    CLOG_LOG(WARN, "timer start failed", K(ret));
  } else {
    CLOG_LOG(INFO, "start ObSimpleArbServer success", KPC(this));
  }
  return ret;
}


int ObSimpleArbServer::simple_close(const bool is_shutdown)
{
  CLOG_LOG_RET(WARN, OB_SUCCESS, "arb simple_close");
  srv_network_frame_.destroy();
  palf_env_mgr_.destroy();
  timer_.destroy();
  return OB_SUCCESS;
}

int ObSimpleArbServer::simple_restart(const std::string &cluster_name,
                                      const int64_t node_idx)
{
  int ret = OB_SUCCESS;
  srv_network_frame_.deliver_.stop();
  palf_env_mgr_.destroy();
  timer_.destroy();
  if (OB_FAIL(simple_init(cluster_name, self_, node_idx, false))) {
    CLOG_LOG(WARN, "simple_init failed", K(ret));
  } else if (OB_FAIL(srv_network_frame_.deliver_.start(srv_network_frame_.normal_rpc_qhandler_,
      srv_network_frame_.server_rpc_qhandler_))) {
    CLOG_LOG(WARN, "start ObArbSrvNetWorkFrame failed", K(ret));
  } else if (OB_FAIL(timer_.start())) {
    CLOG_LOG(WARN, "timer start failed", K(ret));
  }
  return ret;
}

int ObSimpleArbServer::create_palf_env_lite(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  palflite::PalfEnvKey key(cluster_id_, tenant_id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSimpleArbServer not inited", K(ret), K(key));
  } else if (OB_FAIL(palf_env_mgr_.create_palf_env_lite(key))) {
    CLOG_LOG(WARN, "create_palf_env_lite failed", K(ret), K(key));
  } else {
  }
  return ret;
}

int ObSimpleArbServer::remove_palf_env_lite(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  palflite::PalfEnvKey key(cluster_id_, tenant_id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSimpleArbServer not inited", K(ret), K(tenant_id));
  } else if (OB_FAIL(palf_env_mgr_.remove_palf_env_lite(key))) {
    CLOG_LOG(WARN, "remove_palf_env_lite failed", K(ret), K(key));
  } else {
  }
  return ret;
}

int ObSimpleArbServer::get_palf_env_lite(const int64_t tenant_id, PalfEnvLiteGuard &guard)
{
  int ret = OB_SUCCESS;
  palflite::PalfEnvLite *palf_env_impl = NULL;
  palflite::PalfEnvKey key(cluster_id_, tenant_id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSimpleArbServer not inited", K(ret), K(tenant_id));
  } else if (OB_FAIL(palf_env_mgr_.get_palf_env_lite(key, palf_env_impl))) {
    CLOG_LOG(WARN, "get_palf_env_lite failed", K(ret), K(tenant_id));
  } else {
    guard.palf_env_mgr_ = &palf_env_mgr_;
    guard.palf_env_lite_ = palf_env_impl;
  }
  return ret;
}
}
}
