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

#include "ob_batch_proxy.h"
#include "share/config/ob_server_config.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace common;
namespace obrpc
{

DEFINE_SERIALIZE(ObBatchPacket)
{
  int ret = OK_;
  LST_DO_CODE(OB_UNIS_ENCODE, size_, id_, src_);
  if (OB_FAIL(ret)) {
  } else if (pos + size_ > buf_len) {
    ret = OB_SERIALIZE_ERROR;
  } else {
    MEMCPY(buf + pos, buf_, size_);
    pos += size_;
    if (OB_FAIL(src_addr_.serialize(buf, buf_len, pos))) {
      CLOG_LOG(WARN, "failed to serialize addr", K_(src_addr), K(ret));
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObBatchPacket)
{
  int ret = OK_;
  LST_DO_CODE(OB_UNIS_DECODE, size_, id_, src_);
  if (OB_FAIL(ret)) {
  } else if (pos + size_ > data_len || size_ < 0) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    buf_ = (char*)(buf + pos);
    pos += size_;
    // In order to support IPv6, used ObAddr for version 2.2 and abover
    if (pos < data_len) {
      if (OB_FAIL(src_addr_.deserialize(buf, data_len, pos))) {
        CLOG_LOG(WARN, "failed to deserialize addr", K_(src_addr), K(ret));
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObBatchPacket)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, size_, id_, src_);
  len += size_;
  len += src_addr_.get_serialize_size();
  return len;
}

class BatchCallBack : public ObBatchRpcProxy::AsyncCB<OB_BATCH>
{
public:
  BatchCallBack() {}
  virtual ~BatchCallBack() {}
  void set_args(const typename ObBatchRpcProxy::ObRpc<OB_BATCH>::Request &args) { UNUSED(args); }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const
  {
    BatchCallBack *newcb = NULL;
    void *buf = alloc(sizeof(*this));
    if (NULL != buf) {
      newcb = new(buf) BatchCallBack();
    }
    return newcb;
  }

public:
  int process() { return common::OB_SUCCESS; }
  void on_timeout()
  {
    const ObAddr &dst = ObBatchRpcProxy::AsyncCB<OB_BATCH>::dst_;
    const int error = this->get_error();
    RPC_LOG_RET(WARN, OB_TIMEOUT, "batch rpc timeout", K(dst), K(error));
  }
private:
  DISALLOW_COPY_AND_ASSIGN(BatchCallBack);
};

int ObBatchRpcProxy::post_batch(uint64_t tenant_id, const common::ObAddr &addr, const int64_t dst_cluster_id, int batch_type, ObBatchPacket& pkt)
{
  int ret = OB_SUCCESS;
  static BatchCallBack s_cb;
  BatchCallBack *cb = &s_cb;

  ret = this->to(addr).dst_cluster_id(dst_cluster_id).by(tenant_id).as(OB_SERVER_TENANT_ID).post_packet(pkt, cb);
  return ret;
}

}; // end namespace rpc
}; // end namespace oceanbase
