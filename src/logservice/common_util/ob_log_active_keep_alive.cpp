/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX CLOG

#ifdef OB_BUILD_ARBITRATION
#include "logservice/arbserver/palf_env_lite.h"
#include "logservice/arbserver/palf_env_lite_mgr.h"
#endif // OB_BUILD_ARBITRATION
#include "logservice/common_util/ob_log_active_keep_alive.h"
#include "logservice/ob_log_service.h"
#include "logservice/palf/palf_env.h"
#include "observer/ob_server.h"
#include "rpc/obrpc/ob_net_keepalive.h"

namespace oceanbase
{
namespace logservice
{

using namespace oceanbase::common;

ObLogActiveKeepAlive::ObLogActiveKeepAlive()
  : is_inited_(false),
    self_addr_(),
    addr_set_()
{
}

int ObLogActiveKeepAlive::init(const common::ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogActiveKeepAlive init twice", KR(ret), K(self_addr));
  } else if (!self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(self_addr));
  } else if (OB_FAIL(addr_set_.create(512, "Arb84WmV", "Arb5vULE2B"))) {
    CLOG_LOG(WARN, "create addr_set_ failed", KR(ret));
  } else {
    self_addr_ = self_addr;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogActiveKeepAlive init success", K(self_addr));
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    addr_set_.destroy();
    CLOG_LOG(WARN, "ObLogActiveKeepAlive init failed", KR(ret), K(self_addr));
  }
  return ret;
}

bool ObLogActiveKeepAlive::is_inited() const
{
  return is_inited_;
}

void ObLogActiveKeepAlive::destroy()
{
  if (is_inited_) {
    CLOG_LOG(INFO, "ObLogActiveKeepAlive destroy", KPC(this));
    is_inited_ = false;
    self_addr_.reset();
    addr_set_.destroy();
  }
}

void ObLogActiveKeepAlive::do_work()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogActiveKeepAlive is not initialized", KR(ret), KPC(this));
  } else {
    if (OB_FAIL(refresh_addr_set_())) {
      CLOG_LOG(WARN, "refresh_addr_set_ failed", KR(ret), KPC(this));
    } else {
      probe_once_();
    }
  }
}

int ObLogActiveKeepAlive::refresh_addr_set_()
{
  int ret = OB_SUCCESS;
  const bool in_arb_mode = observer::ObServer::get_instance().is_arbitration_mode();
  // old snapshot may include removed replicas, clear to avoid stale warnings
  addr_set_.clear();
  if (in_arb_mode) {
    ret = refresh_addr_set_in_arb_mode_();
  } else {
    ret = refresh_addr_set_in_normal_observer_();
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
ObLogActiveKeepAlive::ArbEnvForEacher::ArbEnvForEacher(ObLogActiveKeepAlive *active_keep_alive)
  : active_keep_alive_(active_keep_alive) {}

ObLogActiveKeepAlive::ArbEnvForEacher::~ArbEnvForEacher()
{
  active_keep_alive_ = nullptr;
}

bool ObLogActiveKeepAlive::ArbEnvForEacher::operator()(
  const palflite::PalfEnvKey &key, palflite::PalfEnvLite *env)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(env)) {
    tmp_ret = OB_INVALID_ARGUMENT;
    CLOG_LOG_RET(WARN, tmp_ret, "invalid argument", KR(tmp_ret), K(key), KP(env));
  } else if (false == is_valid_cluster_id(key.cluster_id_)) {
    tmp_ret = OB_INVALID_ARGUMENT;
    CLOG_LOG_RET(WARN, tmp_ret, "invalid cluster_id", KR(tmp_ret), K(key));
  } else {
    const int64_t cluster_id = key.cluster_id_;
    const int64_t tenant_id = key.tenant_id_;
    common::ObFunction<int (palf::IPalfHandleImpl *)>
      for_each_handle = CollectAddrFunctor(active_keep_alive_, cluster_id, tenant_id);
    if (OB_TMP_FAIL(env->for_each(for_each_handle))) {
      // do not stop global scan
      CLOG_LOG_RET(WARN, tmp_ret, "PalfEnvLite for_each failed", KR(tmp_ret), K(key), KP(env));
    }
  }
  return true; // continue
}
#endif // OB_BUILD_ARBITRATION

int ObLogActiveKeepAlive::refresh_addr_set_in_arb_mode_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ARBITRATION
  ArbEnvForEacher for_each_env = ArbEnvForEacher(this);
  palflite::PalfEnvLiteMgr &palf_env_lite_mgr = palflite::PalfEnvLiteMgr::get_instance();
  if (OB_FAIL(palf_env_lite_mgr.for_each(for_each_env))) {
    CLOG_LOG(WARN, "PalfEnvLiteMgr for_each failed", KR(ret), K(palf_env_lite_mgr));
  }
#endif // OB_BUILD_ARBITRATION
  return ret;
}

int ObLogActiveKeepAlive::refresh_addr_set_in_normal_observer_()
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;
  const int64_t cluster_id = GCONF.cluster_id.get_value();
  if (false == is_valid_cluster_id(cluster_id)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid cluster_id", KR(ret), K(cluster_id));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "omt is not ready, skip refreshing probe targets", KR(ret));
  } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
    CLOG_LOG(WARN, "get_mtl_tenant_ids failed", KR(ret));
  } else {
    // IMPORTANT: never break tenant_ids.count() due to one tenant failure.
    for (int64_t i = 0; i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int tmp_ret = OB_SUCCESS;
      share::ObTenantSwitchGuard guard = share::_make_tenant_switch_guard();
      logservice::ObLogService *log_service = nullptr;
      ipalf::IPalfEnv *palf_env = nullptr;
      palf::IPalfEnvImpl *palf_env_impl = nullptr;
      common::ObFunction<int (palf::IPalfHandleImpl *)> for_each_handle = CollectAddrFunctor(this, cluster_id, tenant_id);
      if (OB_TMP_FAIL(guard.switch_to(tenant_id))) {
        CLOG_LOG_RET(WARN, tmp_ret, "switch_to tenant failed", KR(tmp_ret), K(tenant_id));
      } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService *))
                  || OB_ISNULL(palf_env = log_service->get_palf_env())
                  || OB_ISNULL(palf_env_impl = static_cast<palf::PalfEnv *>(palf_env)->get_palf_env_impl())) {
        tmp_ret = OB_INVALID_ARGUMENT;
        CLOG_LOG_RET(WARN, tmp_ret, "get palf_env_impl failed",
            KR(tmp_ret), K(tenant_id), KP(log_service), KP(palf_env), KP(palf_env_impl));
      } else if (OB_TMP_FAIL(palf_env_impl->for_each(for_each_handle))) {
        CLOG_LOG_RET(WARN, tmp_ret, "PalfEnv for_each failed", KR(tmp_ret), K(cluster_id), K(tenant_id), KP(palf_env_impl));
      }
    }
  }
  return ret;
}

ObLogActiveKeepAlive::CollectAddrFunctor::CollectAddrFunctor(
  ObLogActiveKeepAlive *active_keep_alive, const int64_t cluster_id, const int64_t tenant_id_)
  : active_keep_alive_(active_keep_alive),
    cluster_id_(cluster_id),
    tenant_id_(tenant_id_) {}

ObLogActiveKeepAlive::CollectAddrFunctor::~CollectAddrFunctor()
{
 active_keep_alive_ = nullptr;
}

int ObLogActiveKeepAlive::CollectAddrFunctor::operator()(palf::IPalfHandleImpl *handle)
{
 int ret = OB_SUCCESS;
 if (OB_ISNULL(handle)) {
   ret = OB_INVALID_ARGUMENT;
   CLOG_LOG(WARN, "invalid argument", KR(ret), K(cluster_id_), K(tenant_id_), KP(handle));
 } else {
   // IMPORTANT: never break env->for_each due to one LS failure.
   int tmp_ret = OB_SUCCESS;
   if (OB_TMP_FAIL(active_keep_alive_->collect_addr_from_handle_(cluster_id_, handle))) {
     CLOG_LOG_RET(WARN, tmp_ret, "collect_addr_from_handle failed", KR(tmp_ret), K(cluster_id_), K(tenant_id_), KP(handle));
   }
 }
 return ret;
}

int ObLogActiveKeepAlive::collect_addr_from_handle_(const int64_t cluster_id, palf::IPalfHandleImpl *handle)
{
  int ret = OB_SUCCESS;
  int64_t palf_id = OB_INVALID_ID;
  common::ObMemberList member_list;
  common::ObMember arb_member;
  int64_t paxos_replica_num = 0;
  if (OB_ISNULL(handle)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(cluster_id), KP(handle));
  } else if (OB_FAIL(handle->get_palf_id(palf_id))) {
    CLOG_LOG(WARN, "get_palf_id failed", KR(ret), K(cluster_id), K(palf_id), KP(handle));
  } else if (OB_FAIL(handle->get_paxos_member_list(member_list, paxos_replica_num, false /*filter_logonly_member*/))) {
    CLOG_LOG(WARN, "get_paxos_member_list failed", KR(ret), K(cluster_id), K(palf_id));
  } else if (0 >= paxos_replica_num || 0 >= member_list.get_member_number()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid paxos_replica_num or member_list", KR(ret), K(palf_id), K(paxos_replica_num), K(member_list.get_member_number()));
#ifdef OB_BUILD_ARBITRATION
  } else if (OB_FAIL(handle->get_arbitration_member(arb_member))) {
    CLOG_LOG(WARN, "get_arbitration_member failed", KR(ret), K(cluster_id), K(palf_id), KP(handle));
  } else if (arb_member.is_valid() && OB_FAIL(member_list.add_member(arb_member)) && OB_ENTRY_EXIST != ret) {
    CLOG_LOG(WARN, "add_member failed", KR(ret), K(cluster_id), K(palf_id), K(arb_member), K(member_list));
#endif
  } else {
    const int64_t cnt = member_list.get_member_number();
    const common::ObAddr &self_addr = self_addr_;
    for (int64_t i = 0; i < cnt && OB_SUCC(ret); ++i) {
      common::ObMember member;
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(member_list.get_member_by_index(i, member))) {
        CLOG_LOG(WARN, "get_member_by_index failed", KR(ret), K(cluster_id), K(palf_id), K(member_list), K(i), K(cnt));
      } else if (self_addr == member.get_server()) { // skip self
      } else if (OB_TMP_FAIL(addr_set_.set_refactored(member.get_server(), 1, 0 /*flag: no overwrite*/)) && OB_HASH_EXIST != tmp_ret) {
        ret = tmp_ret;
        CLOG_LOG(WARN, "insert peer addr set failed", KR(ret), K(member), K(palf_id), K(member_list), K(i), K(cnt));
      }
    }
  }
  return ret;
}

void ObLogActiveKeepAlive::probe_once_()
{
  int ret = OB_SUCCESS;
  int64_t failed_cnt = 0;
  int64_t blacklisted_cnt = 0;
  int64_t eagain_cnt = 0;
  int64_t success_cnt = 0;
  CLOG_LOG(TRACE, "probe_once_", KPC(this));
  for (AddrSet::const_iterator iter = addr_set_.begin(); OB_SUCC(ret) && iter != addr_set_.end(); ++iter) {
    const common::ObAddr &addr = iter->first;
    bool in_blacklist = false;
    int tmp_ret = OB_SUCCESS;
    tmp_ret = obrpc::ObNetKeepAlive::get_instance().probe_connectivity(addr, in_blacklist);
    if (OB_SUCCESS != tmp_ret && OB_EAGAIN != tmp_ret) {
      failed_cnt++;
      CLOG_LOG_RET(ERROR, tmp_ret, "active keepalive probe_connectivity failed",
               KR(tmp_ret),
               "dst", addr,
               K(in_blacklist));
    } else if (OB_EAGAIN == tmp_ret) {
      eagain_cnt++;
      CLOG_LOG_RET(WARN, tmp_ret, "active keepalive probe_connectivity eagain",
               KR(tmp_ret),
               "dst", addr,
               K(in_blacklist));
    } else if (in_blacklist) {
      blacklisted_cnt++;
      tmp_ret = OB_CONNECT_ERROR;
      CLOG_LOG_RET(ERROR, tmp_ret, "active keepalive probe_connectivity blacklisted for new connections",
               KR(tmp_ret),
               "dst", addr,
               K(in_blacklist));
    } else {
      success_cnt++;
      CLOG_LOG_RET(TRACE, tmp_ret, "active keepalive probe_connectivity success",
               KR(tmp_ret),
               "dst", addr,
               K(in_blacklist));
    }
  }
  CLOG_LOG(INFO, "active keepalive probe_once_ finish",
           KR(ret),
           K(failed_cnt),
           K(blacklisted_cnt),
           K(eagain_cnt),
           K(success_cnt),
           KPC(this));
}

} // namespace logservice
} // namespace oceanbase
