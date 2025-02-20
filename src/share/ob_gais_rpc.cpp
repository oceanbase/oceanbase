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

#define USING_LOG_PREFIX SHARE

#include "ob_gais_rpc.h"
#include "share/ob_global_autoinc_service.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::observer;
using namespace oceanbase::share;
using namespace oceanbase::transaction;

namespace obrpc
{

#define GAIS_WITH_RPC_SERVICE                                        \
  int ret = OB_SUCCESS;                                              \
  ObGlobalAutoIncService *gais = nullptr;                            \
  const uint64_t tenant_id = arg_.autoinc_key_.tenant_id_;           \
  if (tenant_id != MTL_ID()) {                                       \
    ret = OB_ERR_UNEXPECTED;                                         \
    LOG_ERROR("tenant is not match", K(ret), K(tenant_id));          \
  } else if (OB_ISNULL(gais = MTL(ObGlobalAutoIncService *))) {      \
    ret = OB_ERR_UNEXPECTED;                                         \
    LOG_WARN("global autoinc service is null", K(ret));              \
  } else                                                             \

OB_SERIALIZE_MEMBER(ObGAISNextValRpcResult, start_inclusive_, end_inclusive_, sync_value_);

OB_SERIALIZE_MEMBER(ObGAISCurrValRpcResult, sequence_value_, sync_value_);

OB_DEF_SERIALIZE(ObGAISNextSequenceValRpcResult)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(nextval_);
  return ret;
}

OB_DEF_DESERIALIZE(ObGAISNextSequenceValRpcResult)
{
  int ret = OB_SUCCESS;
  share::ObSequenceValue nextval;
  OB_UNIS_DECODE(nextval);
  // deep copy is needed to ensure that the memory of nextval_ will not be reclaimed
  if (OB_SUCC(ret) && OB_FAIL(nextval_.assign(nextval))) {
    LOG_WARN("fail to assign nextval", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObGAISNextSequenceValRpcResult)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(nextval_);
  return len;
}

int ObGAISNextValRpcResult::init(const uint64_t start_inclusive, const uint64_t end_inclusive,
                                 const uint64_t sync_value)
{
  int ret = OB_SUCCESS;
  if (start_inclusive <= 0 || end_inclusive <= 0 || start_inclusive > end_inclusive ||
      sync_value_ > end_inclusive) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_inclusive), K(end_inclusive), K(sync_value));
  } else {
    start_inclusive_ = start_inclusive;
    end_inclusive_ = end_inclusive;
    sync_value_ = sync_value;
  }
  return ret;
}

int ObGAISCurrValRpcResult::init(const uint64_t sequence_value, const uint64_t sync_value)
{
  int ret = OB_SUCCESS;
  if (sequence_value < sync_value) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sequence_value), K(sync_value));
  } else {
    sequence_value_ = sequence_value;
    sync_value_ = sync_value;
  }
  return ret;
}

int ObGAISNextAutoIncP::process()
{
  GAIS_WITH_RPC_SERVICE {
    if (OB_FAIL(gais->handle_next_autoinc_request(arg_, result_))) {
      LOG_WARN("handle next autoinc request failed", K(ret));
    }
  }
  return ret;
}

int ObGAISCurrAutoIncP::process()
{
  GAIS_WITH_RPC_SERVICE {
    if (OB_FAIL(gais->handle_curr_autoinc_request(arg_, result_))) {
      LOG_WARN("handle curr autoinc request failed", K(ret));
    }
  }
  return ret;
}

int ObGAISPushAutoIncP::process()
{
  GAIS_WITH_RPC_SERVICE {
    if (OB_FAIL(gais->handle_push_autoinc_request(arg_, result_))) {
      LOG_WARN("handle push autoinc request failed", K(ret));
    }
  }
  return ret;
}

int ObGAISClearAutoIncCacheP::process()
{
  GAIS_WITH_RPC_SERVICE {
    if (OB_FAIL(gais->handle_clear_autoinc_cache_request(arg_))) {
      LOG_WARN("handle clear autoinc cache request failed", K(ret));
    }
  }
  return ret;
}

int ObGAISBroadcastAutoIncCacheP::process()
{
  int ret = OB_SUCCESS;
  ObGlobalAutoIncService *gais = nullptr;
  const uint64_t tenant_id = arg_.tenant_id_;
  if (OB_ISNULL(gais = MTL(ObGlobalAutoIncService *))) {
    // ignore if tenant service not in this server
  } else if (OB_FAIL(gais->receive_global_autoinc_cache(arg_))) {
    LOG_WARN("handle clear autoinc cache request failed", K(ret));
  }
  return ret;
}

int ObGAISNextSequenceP::process()
{
  int ret = OB_SUCCESS;
  ObGlobalAutoIncService *gais = nullptr;
  const uint64_t tenant_id = arg_.schema_.get_tenant_id();
  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant is not match", K(ret), K(tenant_id));
  } else if (OB_ISNULL(gais = MTL(ObGlobalAutoIncService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global sequence service is null", K(ret));
  } else {
    if (OB_FAIL(gais->handle_next_sequence_request(arg_, result_))) {
      LOG_WARN("handle next sequence request failed", K(ret));
    }
  }
  return ret;
}

int ObGAISNextSequenceValRpcResult::init(const share::ObSequenceValue nextval)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(nextval_.assign(nextval))) {
    LOG_WARN("fail to assign nextval", K(ret));
  }
  return ret;
}
} // obrpc

namespace share
{
int ObGAISRequestRpc::init(ObGAISRpcProxy *rpc_proxy, const ObAddr &self)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("gais request rpc inited twice", KR(ret));
  } else if (OB_ISNULL(rpc_proxy) || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(rpc_proxy), K(self));
  } else {
    rpc_proxy_ = rpc_proxy;
    self_ = self;
    is_inited_ = true;
    LOG_INFO("gais request rpc inited success", KP(this), K(self));
  }
  return ret;
}

void ObGAISRequestRpc::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    is_inited_ = false;
    rpc_proxy_ = NULL;
    self_.reset();
    LOG_INFO("gais request rpc destroy");
  }
}

int ObGAISRequestRpc::next_autoinc_val(const ObAddr &server,
                                       const ObGAISNextAutoIncValReq &msg,
                                       ObGAISNextValRpcResult &rpc_result)
{
  int ret = OB_SUCCESS;
  const uint64_t timeout = THIS_WORKER.get_timeout_remain();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("gais request rpc not inited", KR(ret));
  } else if (!server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(msg));
  } else if (server == self_) {
    // Use local calls instead of rpc
    ObGlobalAutoIncService *gais = nullptr;
    const uint64_t tenant_id = msg.autoinc_key_.tenant_id_;
    MTL_SWITCH(tenant_id) {
      if (OB_ISNULL(gais = MTL(ObGlobalAutoIncService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("global autoinc service is null", K(ret));
      } else if (OB_FAIL(gais->handle_next_autoinc_request(msg, rpc_result))) {
        LOG_WARN("post local gais require autoinc request failed", KR(ret), K(server), K(msg));
      } else if (!rpc_result.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("post local gais require autoinc and gais_rpc_result is invalid", KR(ret), K(server),
                  K(msg), K(rpc_result));
      } else {
        LOG_TRACE("post local require autoinc request success", K(msg), K(rpc_result));
      }
    }
  } else if (OB_FAIL(rpc_proxy_->to(server).by(msg.autoinc_key_.tenant_id_).timeout(timeout).next_autoinc_val(msg, rpc_result))) {
    LOG_WARN("post require autoinc request failed", KR(ret), K(server), K(msg));
  } else if (!rpc_result.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("post remote gais require autoinc request and gais_rpc_result is invalid",
              KR(ret), K(server), K(msg), K(rpc_result));
  } else {
    LOG_TRACE("post remote require autoinc request success", K(server), K(msg), K(rpc_result));
  }
  return ret;
}

int ObGAISRequestRpc::curr_autoinc_val(const ObAddr &server,
                                       const ObGAISAutoIncKeyArg &msg,
                                       ObGAISCurrValRpcResult &rpc_result)
{
  int ret = OB_SUCCESS;
  const uint64_t timeout = THIS_WORKER.get_timeout_remain();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("gais request rpc not inited", KR(ret));
  } else if (!server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(msg));
  } else if (server == self_) {
    // Use local calls instead of rpc
    ObGlobalAutoIncService *gais = nullptr;
    const uint64_t tenant_id = msg.autoinc_key_.tenant_id_;
    MTL_SWITCH(tenant_id) {
      if (OB_ISNULL(gais = MTL(ObGlobalAutoIncService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("global autoinc service is null", K(ret));
      } else if (OB_FAIL(gais->handle_curr_autoinc_request(msg, rpc_result))) {
        LOG_WARN("post local gais get autoinc request failed", KR(ret), K(server), K(msg));
      } else {
        LOG_TRACE("post local get autoinc request success", K(msg), K(rpc_result));
      }
    }
  } else if (OB_FAIL(rpc_proxy_->to(server).by(msg.autoinc_key_.tenant_id_).timeout(timeout).curr_autoinc_val(msg, rpc_result))) {
    LOG_WARN("post gais request failed", KR(ret), K(server), K(msg));
  } else {
    LOG_TRACE("post get autoinc request success", K(server), K(msg), K(rpc_result));
  }
  return ret;
}

int ObGAISRequestRpc::push_autoinc_val(const ObAddr &server,
                                       const ObGAISPushAutoIncValReq &msg,
                                       uint64_t &sync_value)
{
  int ret = OB_SUCCESS;
  const uint64_t timeout = THIS_WORKER.get_timeout_remain();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("gais request rpc not inited", KR(ret));
  } else if (!server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(msg));
  } else if (server == self_) {
    // Use local calls instead of rpc
    ObGlobalAutoIncService *gais = nullptr;
    const uint64_t tenant_id = msg.autoinc_key_.tenant_id_;
    MTL_SWITCH(tenant_id) {
      if (OB_ISNULL(gais = MTL(ObGlobalAutoIncService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("global autoinc service is null", K(ret));
      } else if (OB_FAIL(gais->handle_push_autoinc_request(msg, sync_value))) {
        LOG_WARN("post local gais push global request failed", KR(ret), K(server), K(msg));
      } else {
        LOG_TRACE("post local gais push global request request success", K(msg), K(sync_value));
      }
    }
  } else if (OB_FAIL(rpc_proxy_->to(server).by(msg.autoinc_key_.tenant_id_).timeout(timeout).push_autoinc_val(msg, sync_value))) {
    LOG_WARN("post remote push global request failed", KR(ret), K(server), K(msg));
  } else {
    LOG_TRACE("post remote push global request success", K(server), K(msg), K(sync_value));
  }
  return ret;
}

int ObGAISRequestRpc::clear_autoinc_cache(const ObAddr &server, const ObGAISAutoIncKeyArg &msg)
{
  int ret = OB_SUCCESS;
  const uint64_t timeout = THIS_WORKER.get_timeout_remain();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("gais request rpc not inited", KR(ret));
  } else if (!server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(msg));
  } else if (server == self_) {
    // Use local calls instead of rpc
    ObGlobalAutoIncService *gais = nullptr;
    const uint64_t tenant_id = msg.autoinc_key_.tenant_id_;
    MTL_SWITCH(tenant_id) {
      if (OB_ISNULL(gais = MTL(ObGlobalAutoIncService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("global autoinc service is null", K(ret));
      } else if (OB_FAIL(gais->handle_clear_autoinc_cache_request(msg))) {
        LOG_WARN("post local gais clear autoinc cache failed", KR(ret), K(server), K(msg));
      } else {
        LOG_TRACE("clear autoinc cache success", K(server), K(msg));
      }
    }
  } else if (OB_FAIL(rpc_proxy_->to(server).by(msg.autoinc_key_.tenant_id_).timeout(timeout).clear_autoinc_cache(msg))) {
    LOG_WARN("post gais request failed", KR(ret), K(server), K(msg));
  } else {
    LOG_TRACE("clear autoinc cache success", K(server), K(msg));
  }
  return ret;
}

int ObGAISRequestRpc::broadcast_global_autoinc_cache(const ObGAISBroadcastAutoIncCacheReq &msg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("gais request rpc not inited", KR(ret));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(msg));
  } else {
    ObZone null_zone;
    ObSEArray<ObAddr, 8> server_list;
    if (OB_FAIL(SVR_TRACER.get_alive_servers(null_zone, server_list))) {
      LOG_WARN("fail to get alive server", K(ret));
    } else {
      const uint64_t tenant_id = msg.tenant_id_;
      const static int64_t BROADCAST_OP_TIMEOUT = 1000 * 1000; // 1s
      for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); i++) {
        ObAddr &dest = server_list.at(i);
        if (dest == self_) {
          // ignore broadcast to self
        } else if (OB_FAIL(rpc_proxy_->to(dest).by(tenant_id).timeout(BROADCAST_OP_TIMEOUT)
                                               .broadcast_autoinc_cache(msg, NULL))) {
          LOG_WARN("fail to broadcast autoinc cache to server", K(ret), K(msg), K(dest));
          ret = OB_SUCCESS;
        } else {
          LOG_DEBUG("broadcast autoinc cache success", K(dest), K(msg));
        }
      }
    }
  }
  return ret;
}

int ObGAISRequestRpc::next_sequence_val(const common::ObAddr &server,
                       const ObGAISNextSequenceValReq &msg,
                       ObGAISNextSequenceValRpcResult &rpc_result)
{
  int ret = OB_SUCCESS;
  const uint64_t timeout = THIS_WORKER.get_timeout_remain();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("gais request rpc not inited", KR(ret));
  } else if (!server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(msg));
  } else if (server == self_) {
    // Use local calls instead of rpc
    ObGlobalAutoIncService *gais = nullptr;
    const uint64_t tenant_id = msg.schema_.get_tenant_id();
    MTL_SWITCH(tenant_id) {
      if (OB_ISNULL(gais = MTL(ObGlobalAutoIncService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("global autoinc service is null", K(ret));
      } else if (OB_FAIL(gais->handle_next_sequence_request(msg, rpc_result))) {
        LOG_WARN("post local gais require autoinc request failed", KR(ret), K(server), K(msg));
      } else {
        LOG_TRACE("post local require autoinc request success", K(msg), K(rpc_result));
      }
    }
  } else if (OB_UNLIKELY(OB_ISNULL(rpc_proxy_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy is null", K(ret));
  }else if (OB_FAIL(rpc_proxy_->to(server).by(msg.schema_.get_tenant_id()).timeout(timeout).next_sequence_val(msg, rpc_result))) {
    LOG_WARN("post require autoinc request failed", KR(ret), K(server), K(msg));
  } else {
    LOG_TRACE("post remote require autoinc request success", K(server), K(msg), K(rpc_result));
  }
  return ret;
}

} // share
} // oceanbase
