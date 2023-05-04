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

#include "ob_blacklist_proxy.h"

namespace oceanbase
{
using namespace common;
using rpc::frame::ObReqTransport;
using rpc::frame::SPAlloc;

namespace obrpc
{
OB_SERIALIZE_MEMBER(ObBlacklistReq, sender_, send_timestamp_);
OB_SERIALIZE_MEMBER(ObBlacklistResp, sender_, req_send_timestamp_, req_recv_timestamp_, server_start_time_);

void ObBlacklistReq::reset()
{
  sender_.reset();
  send_timestamp_ = OB_INVALID_TIMESTAMP;
}

void ObBlacklistResp::reset()
{
  sender_.reset();
  req_send_timestamp_ = OB_INVALID_TIMESTAMP;
  req_recv_timestamp_ = OB_INVALID_TIMESTAMP;
  server_start_time_ = 0;
}

class BlacklistReqCallBack : public ObBlacklistRpcProxy::AsyncCB<OB_SERVER_BLACKLIST_REQ>
{
public:
  BlacklistReqCallBack() {}
  virtual ~BlacklistReqCallBack() {}
  void set_args(const typename ObBlacklistRpcProxy::ObRpc<OB_SERVER_BLACKLIST_REQ>::Request &args)
  { UNUSED(args); }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(const oceanbase::rpc::frame::SPAlloc &alloc) const
  {
    BlacklistReqCallBack *newcb = NULL;
    void *buf = alloc(sizeof(*this));
    if (NULL != buf) {
      newcb = new(buf) BlacklistReqCallBack();
    }
    return newcb;
  }
public:
  int process()
  {
    return common::OB_SUCCESS;
  }
  void on_timeout()
  {}
private:
  DISALLOW_COPY_AND_ASSIGN(BlacklistReqCallBack);
};

class BlacklistRespCallBack : public ObBlacklistRpcProxy::AsyncCB<OB_SERVER_BLACKLIST_RESP>
{
public:
  BlacklistRespCallBack() {}
  virtual ~BlacklistRespCallBack() {}
  void set_args(const typename ObBlacklistRpcProxy::ObRpc<OB_SERVER_BLACKLIST_RESP>::Request &args)
  { UNUSED(args); }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(const oceanbase::rpc::frame::SPAlloc &alloc) const
  {
    BlacklistRespCallBack *newcb = NULL;
    void *buf = alloc(sizeof(*this));
    if (NULL != buf) {
      newcb = new(buf) BlacklistRespCallBack();
    }
    return newcb;
  }
public:
  int process()
  {
    return common::OB_SUCCESS;
  }
  void on_timeout()
  {}
private:
  DISALLOW_COPY_AND_ASSIGN(BlacklistRespCallBack);
};

int ObBlacklistRpcProxy::send_req(const common::ObAddr &dst, const int64_t dst_cluster_id, const ObBlacklistReq &req)
{
  int ret = OB_SUCCESS;
  if (!dst.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    static BlacklistReqCallBack cb;
    ret = this->to(dst).dst_cluster_id(dst_cluster_id).by(OB_SERVER_TENANT_ID).timeout(BLACK_LIST_MSG_TIMEOUT).post_request(req, &cb);
  }
  return ret;
}

int ObBlacklistRpcProxy::send_resp(const common::ObAddr &dst, const int64_t dst_cluster_id, const ObBlacklistResp &resp)
{
  int ret = OB_SUCCESS;
  if (!dst.is_valid() || !resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    static BlacklistRespCallBack cb;
    ret = this->to(dst).dst_cluster_id(dst_cluster_id).by(OB_SERVER_TENANT_ID).timeout(BLACK_LIST_MSG_TIMEOUT).post_response(resp, &cb);
  }
  return ret;
}

};
};
