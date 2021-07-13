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

#include <time.h>
#include "ob_ha_gts.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "rpc/frame/ob_req_transport.h"
#include "share/ob_srv_rpc_proxy.h"
#include "ob_ha_gts_mgr.h"

namespace oceanbase {
using namespace common;
namespace gts {
class ObGtsPingCB : public obrpc::ObSrvRpcProxy::AsyncCB<obrpc::OB_HA_GTS_PING_REQUEST> {
public:
  ObGtsPingCB() : gts_mgr_(NULL)
  {}
  virtual ~ObGtsPingCB()
  {}

public:
  int init(ObHaGtsManager* gts_mgr);
  void on_timeout() final;
  int process();
  virtual void set_args(const obrpc::ObHaGtsPingRequest& arg)
  {
    UNUSED(arg);
  }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB* clone(const rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    ObGtsPingCB* newcb = nullptr;
    if (buf != nullptr) {
      newcb = new (buf) ObGtsPingCB();
      newcb->gts_mgr_ = gts_mgr_;
    }
    return newcb;
  }

private:
  ObHaGtsManager* gts_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGtsPingCB);
};

int ObGtsPingCB::init(ObHaGtsManager* gts_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gts_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret));
  } else {
    gts_mgr_ = gts_mgr;
  }
  return ret;
}

void ObGtsPingCB::on_timeout()
{
  STORAGE_LOG(TRACE, "ObGtsPingCB on_timeout");
}

int ObGtsPingCB::process()
{
  int ret = OB_SUCCESS;
  obrpc::ObRpcResultCode& rcode = obrpc::ObSrvRpcProxy::AsyncCB<obrpc::OB_HA_GTS_PING_REQUEST>::rcode_;
  obrpc::ObHaGtsPingResponse& response = obrpc::ObSrvRpcProxy::AsyncCB<obrpc::OB_HA_GTS_PING_REQUEST>::result_;
  if (OB_SUCCESS == rcode.rcode_) {
    if (OB_FAIL(gts_mgr_->handle_ping_response(response))) {
      STORAGE_LOG(WARN, "handle_ping_response failed", K(ret), K(response));
    }
  } else {
    ret = rcode.rcode_;
  }
  return ret;
}

ObHaGts::ObHaGts()
{
  reset();
}

ObHaGts::~ObHaGts()
{
  destroy();
}

int ObHaGts::init(const uint64_t gts_id, const int64_t epoch_id, const common::ObMemberList& member_list,
    const int64_t min_start_timestamp, obrpc::ObSrvRpcProxy* rpc_proxy, ObHaGtsManager* gts_mgr,
    const common::ObAddr& self_addr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "ObHaGts is inited twice", K(ret));
    // High available GTS support up to three replicas
  } else if (!is_valid_gts_id(gts_id) || epoch_id <= 0 || member_list.get_member_number() > 3 ||
             member_list.get_member_number() <= 0 || min_start_timestamp <= 0 || NULL == gts_mgr || NULL == rpc_proxy ||
             !self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR,
        "invalid arguments",
        K(ret),
        K(gts_id),
        K(epoch_id),
        K(member_list),
        K(min_start_timestamp),
        KP(rpc_proxy),
        K(self_addr));
  } else if (OB_FAIL(member_list_.deep_copy(member_list))) {
    STORAGE_LOG(ERROR, "member_list_ deep_copy failed", K(ret));
  } else if (OB_FAIL(req_map_.init(ObModIds::OB_HA_GTS_REQ_MAP))) {
    STORAGE_LOG(ERROR, "req_map_ init failed", K(ret));
  } else if (OB_FAIL(heartbeat_map_.init(ObModIds::OB_HA_GTS_HEARTBEAT_MAP))) {
    STORAGE_LOG(ERROR, "heartbeat_map_ init failed", K(ret));
  } else {
    is_inited_ = true;
    created_ts_ = ObTimeUtility::current_time();
    gts_id_ = gts_id;
    local_ts_ = std::max(min_start_timestamp, ObTimeUtility::current_time());
    epoch_id_ = epoch_id;
    next_req_id_ = 0;
    rpc_proxy_ = rpc_proxy;
    gts_mgr_ = gts_mgr;
    self_addr_ = self_addr;
    is_serving_ = member_list_.contains(self_addr_);
    is_single_mode_ = is_serving_ && (member_list_.get_member_number() == 1);
    if (is_serving_) {
      if (OB_SUCCESS != (tmp_ret = member_list_.remove_server(self_addr_))) {
        STORAGE_LOG(ERROR, "member_list_ remove_server failed", K(ret));
      }
    }
  }

  if (!is_inited_) {
    destroy();
  }
  STORAGE_LOG(INFO, "ObHaGts is inited", K(ret));
  return ret;
}

bool ObHaGts::is_inited() const
{
  return is_inited_;
}

void ObHaGts::reset()
{
  is_inited_ = false;
  created_ts_ = OB_INVALID_TIMESTAMP;
  gts_id_ = OB_INVALID_ID;
  local_ts_ = OB_INVALID_TIMESTAMP;
  epoch_id_ = OB_INVALID_TIMESTAMP;
  member_list_.reset();
  next_req_id_ = OB_INVALID_ID;
  req_map_.reset();
  heartbeat_map_.reset();
  last_change_member_ts_ = OB_INVALID_TIMESTAMP;
  rpc_proxy_ = NULL;
  gts_mgr_ = NULL;
  self_addr_.reset();
  is_serving_ = false;
  is_single_mode_ = false;
}

void ObHaGts::destroy()
{
  WLockGuard guard(lock_);
  is_inited_ = false;
  gts_id_ = OB_INVALID_ID;
  local_ts_ = OB_INVALID_TIMESTAMP;
  epoch_id_ = OB_INVALID_TIMESTAMP;
  member_list_.reset();
  next_req_id_ = OB_INVALID_ID;
  req_map_.destroy();
  heartbeat_map_.destroy();
  last_change_member_ts_ = OB_INVALID_TIMESTAMP;
  rpc_proxy_ = NULL;
  gts_mgr_ = NULL;
  self_addr_.reset();
  is_serving_ = false;
  is_single_mode_ = false;
  STORAGE_LOG(INFO, "ObHaGts destroy");
  return;
}

int ObHaGts::handle_ping_request(const obrpc::ObHaGtsPingRequest& request, obrpc::ObHaGtsPingResponse& response)
{
  EVENT_INC(HA_GTS_HANDLE_PING_REQUEST_COUNT);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(request));
  } else if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      STORAGE_LOG(WARN, "handle_ping_request try_lock failed", K(ret));
    }
    // Lock successfully
  } else {
    if (!is_serving_) {
    } else if (request.get_epoch_id() != epoch_id_) {
    } else {
      const int64_t response_ts = max(ATOMIC_LOAD(&local_ts_) + 1, request.get_request_ts());
      inc_update(&local_ts_, response_ts);

      response.set(gts_id_, request.get_req_id(), request.get_epoch_id(), response_ts);
    }
    lock_.rdunlock();
  }
  return ret;
}

int ObHaGts::handle_ping_response(const obrpc::ObHaGtsPingResponse& response)
{
  EVENT_INC(HA_GTS_HANDLE_PING_RESPONSE_COUNT);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else if (!response.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(response));
  } else if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      STORAGE_LOG(WARN, "handle_ping_response try_lock failed", K(ret));
    }
  } else {
    if (!is_serving_) {
    } else if (response.get_epoch_id() != epoch_id_) {
    } else {
      const int64_t response_ts = response.get_response_ts();
      inc_update(&local_ts_, response_ts);
      const ObGtsReqID req_id(response.get_req_id());
      EraseIfFunctor functor;
      if (OB_FAIL(req_map_.erase_if(req_id, functor)) && OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "req_map_ erase_if failed", K(ret), K(response));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        const ObGtsReq& req = functor.get_req();
        obrpc::ObHaGtsGetResponse gts_resp;
        gts_resp.set(gts_id_, req.get_tenant_id(), req.get_srr(), response_ts);
        if (OB_SUCCESS != (tmp_ret = send_gts_response_(req.get_client_addr(), gts_resp))) {
          STORAGE_LOG(WARN, "send_gts_response_ failed", K(ret), K(gts_resp));
        }
      }
    }
    lock_.rdunlock();
  }
  return ret;
}

int ObHaGts::handle_get_request(const obrpc::ObHaGtsGetRequest& request)
{
  EVENT_INC(HA_GTS_HANDLE_GET_REQUEST_COUNT);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(request));
  } else if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      STORAGE_LOG(WARN, "handle_get_request try_lock failed", K(ret));
    }
  } else if (OB_LIKELY(!is_single_mode_)) {
    const uint64_t req_id = ATOMIC_FAA(&next_req_id_, 1);
    ObGtsReqID req_id_wrapper(req_id);
    ObGtsReq req(request.get_self_addr(), request.get_tenant_id(), request.get_srr());
    if (!is_serving_) {
    } else if (OB_FAIL(req_map_.insert(req_id_wrapper, req))) {
      STORAGE_LOG(WARN, "req_map_ insert failed", K(ret), K(req_id));
    } else {
      // local_ts may be updated by the RPC thread, therefore need to
      // use atomic operations
      // there is no need to increase local_ts, can read the value of this replica
      // by pushing local_ts up in PingResponse
      const int64_t request_ts = max(ATOMIC_LOAD(&local_ts_) + 1, ObTimeUtility::current_time());
      obrpc::ObHaGtsPingRequest request;
      request.set(gts_id_, req_id, epoch_id_, request_ts);

      for (int64_t i = 0; i < member_list_.get_member_number(); i++) {
        common::ObAddr server;
        if (OB_SUCCESS != (tmp_ret = member_list_.get_server_by_index(i, server))) {
          STORAGE_LOG(ERROR, "member_list_ get_server_by_index failed", K(ret));
        } else if (OB_SUCCESS != (tmp_ret = ping_request_(server, request))) {
          STORAGE_LOG(WARN, "ping_request_ failed", K(tmp_ret), K(server), K(request));
        } else {
          // do nothing
        }
      }
    }
    lock_.rdunlock();
    // Single replica mode, can directly respond to the clients
  } else {
    if (!is_serving_) {
    } else {
      const int64_t response_ts = max(ATOMIC_LOAD(&local_ts_) + 1, ObTimeUtility::current_time());
      inc_update(&local_ts_, response_ts);
      obrpc::ObHaGtsGetResponse gts_resp;
      gts_resp.set(gts_id_, request.get_tenant_id(), request.get_srr(), response_ts);
      if (OB_SUCCESS != (tmp_ret = send_gts_response_(request.get_self_addr(), gts_resp))) {
        STORAGE_LOG(WARN, "send_gts_response_ failed", K(ret), K(gts_resp));
      }
    }
    lock_.rdunlock();
  }
  return ret;
}

int ObHaGts::handle_heartbeat(const obrpc::ObHaGtsHeartbeat& heartbeat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else if (!heartbeat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(heartbeat));
  } else {
    const ObAddr& addr = heartbeat.get_addr();
    HeartbeatFunctor functor;
    if (OB_FAIL(heartbeat_map_.operate(addr, functor)) && OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "heartbeat_map_ operate failed", K(ret), K(gts_id_), K(addr));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(heartbeat_map_.insert(addr, ObTimeUtility::current_time()))) {
        STORAGE_LOG(WARN, "heartbeat_map_ insert failed", K(ret), K(gts_id_), K(addr));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObHaGts::send_heartbeat()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      STORAGE_LOG(WARN, "send_heartbeat try_lock failed", K(ret));
    }
  } else {
    if (!is_serving_) {
    } else {
      obrpc::ObHaGtsHeartbeat heartbeat;
      heartbeat.set(gts_id_, self_addr_);

      for (int64_t i = 0; i < member_list_.get_member_number(); i++) {
        common::ObAddr server;
        if (OB_SUCCESS != (tmp_ret = member_list_.get_server_by_index(i, server))) {
          STORAGE_LOG(ERROR, "member_list_ get_server_by_index failed", K(ret));
        } else if (OB_SUCCESS != (tmp_ret = send_heartbeat_(server, heartbeat))) {
          STORAGE_LOG(WARN, "send_heartbeat_ failed", K(tmp_ret), K(server), K(heartbeat));
        } else {
          // do nothing
        }
      }
    }
    lock_.rdunlock();
  }
  return ret;
}

int ObHaGts::check_member_status(bool& need_change_member, bool& miss_replica, common::ObAddr& offline_replica,
    int64_t& epoch_id, common::ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  need_change_member = false;
  miss_replica = false;
  offline_replica.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      STORAGE_LOG(WARN, "check_member_status try_lock failed", K(ret));
    }
  } else {
    if (ObTimeUtility::current_time() - last_change_member_ts_ >= AUTO_CHANGE_MEMBER_INTERVAL &&
        ObTimeUtility::current_time() - created_ts_ >= INTERVAL_WITH_CREATED_TS && is_serving_) {
      if (member_list_.get_member_number() == 1) {
        need_change_member = true;
        miss_replica = true;
      } else {
        CheckHeartbeatFunctor functor(member_list_);
        if (OB_FAIL(heartbeat_map_.remove_if(functor))) {
          STORAGE_LOG(WARN, "heartbeat_map_ remove_if failed", K(ret));
        } else if (functor.get_offline_replica().is_valid()) {
          need_change_member = true;
          offline_replica = functor.get_offline_replica();
        } else if (member_list_.get_member_number() != functor.get_heartbeat_member_list().get_member_number()) {
          // Deal with the secne where replica has never sent a heartbeat
          need_change_member = true;
          if (OB_FAIL(get_miss_replica_(member_list_, functor.get_heartbeat_member_list(), offline_replica))) {
            STORAGE_LOG(ERROR, "get_missing_replica_ failed", K(ret));
          }
        }
      }
    }
    if (need_change_member) {
      epoch_id = epoch_id_;
      member_list = member_list_;
      last_change_member_ts_ = ObTimeUtility::current_time();
      if (OB_FAIL(member_list.add_server(self_addr_))) {
        STORAGE_LOG(WARN, "member_list add_server failed", K(ret), K(member_list), K(self_addr_));
      }
    }
    lock_.rdunlock();
  }
  return ret;
}

int ObHaGts::try_update_meta(const int64_t epoch_id, const common::ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else if (epoch_id <= 0 || member_list.get_member_number() > 3 || member_list.get_member_number() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(gts_id_), K(epoch_id), K(member_list));
  } else if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "try_update_meta try_lock failed", K(ret));
  } else {
    if (epoch_id <= epoch_id_) {
      lock_.rdunlock();
    } else {
      lock_.rdunlock();
      do {
        WLockGuard guard(lock_);
        epoch_id_ = epoch_id;
        member_list_ = member_list;
        is_serving_ = member_list_.contains(self_addr_);
        is_single_mode_ = is_serving_ && (member_list_.get_member_number() == 1);
        if (is_serving_) {
          if (OB_SUCCESS != (tmp_ret = member_list_.remove_server(self_addr_))) {
            STORAGE_LOG(ERROR, "member_list_ remove_server failed", K(ret));
          }
        }
      } while (0);
      STORAGE_LOG(INFO,
          "udpate meta",
          K(ret),
          K(gts_id_),
          K(self_addr_),
          K(epoch_id_),
          K(member_list_),
          K(is_serving_),
          K(is_single_mode_));
    }
  }
  return ret;
}

int ObHaGts::get_meta(int64_t& epoch_id, common::ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "get_meta try_lock failed", K(ret));
  } else {
    if (!is_serving_) {
      ret = OB_GTS_IS_NOT_SERVING;
      STORAGE_LOG(WARN, "gts is not serving", K(ret));
    } else {
      epoch_id = epoch_id_;
      member_list = member_list_;
      if (OB_FAIL(member_list.add_server(self_addr_))) {
        STORAGE_LOG(WARN, "member_list add_server failed", K(ret));
      }
    }
    lock_.rdunlock();
  }
  return ret;
}

int ObHaGts::remove_stale_req()
{
  int ret = OB_SUCCESS;
  RemoveIfFunctor functor(transaction::MonotonicTs::current_time());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else if (OB_FAIL(req_map_.remove_if(functor))) {
    STORAGE_LOG(ERROR, "req_map_ remove_if failed", K(ret));
  }
  return ret;
}

int ObHaGts::get_local_ts(int64_t& local_ts) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else {
    local_ts = ATOMIC_LOAD(&local_ts_);
  }
  return ret;
}

int ObHaGts::inc_update_local_ts(const int64_t local_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObHaGts is not inited", K(ret));
  } else {
    inc_update(&local_ts_, local_ts);
  }
  return ret;
}

int ObHaGts::ping_request_(const common::ObAddr& server, const obrpc::ObHaGtsPingRequest& request)
{
  int ret = OB_SUCCESS;
  ObGtsPingCB cb;
  if (OB_FAIL(cb.init(gts_mgr_))) {
    STORAGE_LOG(WARN, "ObGtsPingCB init failed", K(ret));
  } else if (OB_FAIL(rpc_proxy_->to(server)
                         .by(OB_SERVER_TENANT_ID)
                         .timeout(HA_GTS_RPC_TIMEOUT)
                         .ha_gts_ping_request(request, &cb))) {
    STORAGE_LOG(WARN, "ha_gts_ping_request failed", K(ret));
  }
  STORAGE_LOG(TRACE, "ping_request_", K(server), K(request));
  return ret;
}

int ObHaGts::send_gts_response_(const common::ObAddr& server, const obrpc::ObHaGtsGetResponse& response)
{
  EVENT_INC(HA_GTS_SEND_GET_RESPONSE_COUNT);
  STORAGE_LOG(TRACE, "send_gts_response_", K(server), K(response));
  return rpc_proxy_->to(server).by(OB_SERVER_TENANT_ID).timeout(HA_GTS_RPC_TIMEOUT).ha_gts_get_response(response, NULL);
}

int ObHaGts::send_heartbeat_(const common::ObAddr& server, const obrpc::ObHaGtsHeartbeat& heartbeat)
{
  STORAGE_LOG(TRACE, "send_heartbeat_", K(server), K(heartbeat));
  return rpc_proxy_->to(server).by(OB_SERVER_TENANT_ID).timeout(HA_GTS_RPC_TIMEOUT).ha_gts_heartbeat(heartbeat, NULL);
}

int ObHaGts::get_miss_replica_(const common::ObMemberList& member_list1, const common::ObMemberList& member_list2,
    common::ObAddr& miss_replica) const
{
  int ret = OB_SUCCESS;
  miss_replica.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < member_list1.get_member_number(); i++) {
    common::ObAddr server;
    if (OB_FAIL(member_list1.get_server_by_index(i, server))) {
      STORAGE_LOG(ERROR, "get_server_by_index failed", K(ret));
    } else if (!member_list2.contains(server)) {
      miss_replica = server;
      break;
    }
  }
  if (!miss_replica.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int64_t ObHaGtsFactory::alloc_cnt_ = 0;
int64_t ObHaGtsFactory::free_cnt_ = 0;
}  // namespace gts
}  // namespace oceanbase
