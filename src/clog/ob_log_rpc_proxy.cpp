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

#include "lib/compress/ob_compressor_pool.h"
#include "share/config/ob_server_config.h"
#include "share/ob_cluster_version.h"
#include "rpc/frame/ob_req_transport.h"  // ObReqTransport
#include "observer/ob_server.h"
#include "ob_i_log_engine.h"
#include "ob_log_rpc_proxy.h"
#include "ob_log_req.h"

namespace oceanbase {
using namespace common;
using rpc::frame::ObReqTransport;
using rpc::frame::SPAlloc;
namespace obrpc {
DEFINE_SERIALIZE(ObLogRpcProxy::Buffer)
{
  int ret = OK_;
  LST_DO_CODE(OB_UNIS_ENCODE, pcode_, len_);
  if (OB_FAIL(ret)) {
  } else if (pos + len_ > buf_len) {
    ret = OB_SERIALIZE_ERROR;
  } else {
    MEMCPY(buf + pos, data_, len_);
    pos += len_;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObLogRpcProxy::Buffer)
{
  int ret = OK_;
  LST_DO_CODE(OB_UNIS_DECODE, pcode_, len_);
  if (OB_FAIL(ret)) {
  } else if (len_ <= 0 || pos + len_ <= 0 || pos + len_ > data_len) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    data_ = buf + pos;
    pos += len_;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLogRpcProxy::Buffer)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, pcode_, len_);
  len += len_;
  return len;
}

class CallBack : public ObLogRpcProxy::AsyncCB<OB_CLOG> {
public:
  CallBack()
  {}
  virtual ~CallBack()
  {}
  int decode(void* pkt)
  {
    int ret = OB_SUCCESS;
    UNUSED(pkt);
    return ret;
  }
  int process()
  {
    int ret = OB_SUCCESS;
    ObRpcResultCode& rcode = ObLogRpcProxy::AsyncCB<OB_CLOG>::rcode_;
    const common::ObAddr& dst = ObLogRpcProxy::AsyncCB<OB_CLOG>::dst_;
    if (common::OB_SUCCESS != rcode.rcode_) {
      if (REACH_TIME_INTERVAL(LOG_INTERVAL)) {
        CLOG_LOG(WARN, "clog rpc error", K(rcode), K(dst));
        ret = rcode.rcode_;
      }
    }

    return ret;
  }
  void set_args(const ObLogRpcProxy::Buffer& arg)
  {
    UNUSED(arg);
  }
  rpc::frame::ObReqTransport::AsyncCB* clone(const SPAlloc& alloc) const
  {
    rpc::frame::ObReqTransport::AsyncCB* ret_cb = NULL;
    void* buf = alloc(sizeof(*this));
    if (OB_ISNULL(buf)) {
      ret_cb = NULL;
    } else {
      CallBack* newcb = new (buf) CallBack();
      ret_cb = newcb;
    }
    return ret_cb;
  }
  void on_timeout()
  {
    const common::ObAddr& dst = ObLogRpcProxy::AsyncCB<OB_CLOG>::dst_;
    if (REACH_TIME_INTERVAL(LOG_INTERVAL)) {
      CLOG_LOG(WARN, "clog rpc timeout", K(dst));
    }
  }

private:
  static const int64_t LOG_INTERVAL = 1 * 1000 * 1000;  // 1S
};

int ObLogRpcProxy::post(const common::ObAddr& server, int pcode, const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  static CallBack cb;
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  ret = this->to(server)
            .timeout(clog::CLOG_RPC_TIMEOUT)
            .trace_time(true)
            .max_process_handler_time(MAX_PROCESS_HANDLER_TIME)
            .by(tenant_id)
            .log_rpc(Buffer(pcode, data, len), &cb);
  return ret;
}

bool ObLogRpcProxy::cluster_version_before_1472_() const
{
  return GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1472;
}
};  // namespace obrpc
};  // end namespace oceanbase
