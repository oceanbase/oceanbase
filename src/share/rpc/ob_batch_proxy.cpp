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
#include "lib/utility/ob_tracepoint.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/utility/utility.h"
#include "observer/omt/ob_tenant.h"
#include "sql/das/ob_das_rpc_processor.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_task.h"

namespace oceanbase
{
using namespace common;
namespace obrpc
{

ERRSIM_POINT_DEF(ERRSIM_LOOKUP_BATCH_RPC_TIMEOUT);

int64_t g_batch_rpc_delay_cached[BATCH_REQ_TYPE_COUNT] =
    {2000, 2000, 500, 0, 0, 0, 0, 0, 2000, 100, 100};

void refresh_batch_rpc_delay_us_cache()
{
  const int64_t v = GCONF.das_batch_rpc_delay_us;
  ATOMIC_STORE(&g_batch_rpc_delay_cached[LOOKUP_DAS_BATCH_REQ1], v);
  ATOMIC_STORE(&g_batch_rpc_delay_cached[LOOKUP_DAS_BATCH_REQ2], v);
}

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

// Dedicated callback for LOOKUP_DAS_BATCH_REQ1/2. Parses the aggregated batch packet
// in set_args() to collect request_ids of TASK_TYPE sub-requests into ids_, so that
// on transport failure (on_timeout/on_invalid) we can fetch each waiter's
// ObRpcDasAsyncAccessCallBack via DasBatchReqMap and force it to complete, instead of
// silently relying on the SQL-level timeout.
class LookupBatchCallBack : public ObBatchRpcProxy::AsyncCB<OB_BATCH>
{
public:
  static const int64_t MAX_IDS = 256;

  LookupBatchCallBack() : tenant_id_(OB_INVALID_TENANT_ID), id_cnt_(0) {}
  virtual ~LookupBatchCallBack() {}

  void set_args(const typename ObBatchRpcProxy::ObRpc<OB_BATCH>::Request &pkt) override
  {
    int ret = OB_SUCCESS;
    id_cnt_ = 0;
    tenant_id_ = OB_INVALID_TENANT_ID;
    if (OB_ISNULL(pkt.buf_) || pkt.size_ <= 0) {
      // empty packet, nothing to collect
    } else {
      int64_t pos = 0;
      const int64_t total = pkt.size_;
      while (pos + static_cast<int64_t>(sizeof(ObBatchPacket::Req)) < total
             && id_cnt_ < MAX_IDS) {
        const ObBatchPacket::Req *req =
            reinterpret_cast<const ObBatchPacket::Req*>(pkt.buf_ + pos);
        const int64_t req_body_end = pos + static_cast<int64_t>(sizeof(*req)) + req->size_;
        if (req_body_end > total || req->size_ < 0) {
          break;
        }
        const uint32_t flag = (req->type_ >> 24) & 0xff;
        const uint32_t req_batch_type = (req->type_ >> 16) & 0xff;
        const uint32_t sub_type = req->type_ & 0xffff;
        pos = req_body_end;
        if ((INVALID_BATCH_REQ == req_batch_type) || (OB_SQL_LOOKUP_DAS_TASK_TYPE != sub_type)) {
          continue;
        }
        const char *body = reinterpret_cast<const char*>(req + 1);
        const int64_t body_size = req->size_;
        int64_t body_pos = 0;
        if (1 == flag) {
          ObCurTraceId::TraceId tmp;
          if (OB_FAIL(tmp.deserialize(body, body_size, body_pos))) {
            ret = OB_SUCCESS; // skip this entry, continue
            continue;
          }
        }
        uint64_t this_tenant = OB_INVALID_TENANT_ID;
        int64_t request_id = OB_INVALID_ID;
        if (OB_FAIL(sql::ObDASLookupBatchTask::peek_request_id(body + body_pos,
                                                               body_size - body_pos,
                                                               this_tenant, request_id))) {
          ret = OB_SUCCESS;
          continue;
        }
        if (OB_INVALID_ID == request_id) {
          continue;
        }
        if (OB_INVALID_TENANT_ID == tenant_id_) {
          tenant_id_ = this_tenant;
        }
        ids_[id_cnt_++] = request_id;
      }
      if (id_cnt_ >= MAX_IDS) {
        RPC_LOG_RET(WARN, common::OB_BUF_NOT_ENOUGH,
            "lookup batch callback ids_ full, excess requests fall back to "
            "check_lookup_batch_task_timeout", K_(tenant_id), K_(id_cnt));
      }
    }
  }

  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const override
  {
    LookupBatchCallBack *newcb = NULL;
    void *buf = alloc(sizeof(*this));
    if (NULL != buf) {
      newcb = new(buf) LookupBatchCallBack();
    }
    return newcb;
  }

  int process() override { return common::OB_SUCCESS; }

  void on_timeout() override
  {
    const ObAddr &dst = ObBatchRpcProxy::AsyncCB<OB_BATCH>::dst_;
    const int error = this->get_error();
    RPC_LOG_RET(WARN, OB_TIMEOUT, "lookup batch rpc timeout", K(dst), K(error),
                K_(tenant_id), K_(id_cnt));
    wake_waiters_(true /* is_timeout */);
  }

  void on_invalid() override
  {
    const ObAddr &dst = ObBatchRpcProxy::AsyncCB<OB_BATCH>::dst_;
    const int error = this->get_error();
    RPC_LOG_RET(WARN, OB_INVALID_ERROR, "lookup batch rpc invalid", K(dst), K(error),
                K_(tenant_id), K_(id_cnt));
    wake_waiters_(false /* is_invalid */);
  }

private:
  void wake_waiters_(bool is_timeout)
  {
    if (OB_INVALID_TENANT_ID == tenant_id_ || id_cnt_ <= 0) {
      return;
    }
    int ret = OB_SUCCESS;
    MTL_SWITCH(tenant_id_) {
      sql::ObDataAccessService *das = MTL(sql::ObDataAccessService *);
      if (OB_ISNULL(das)) {
        // tenant dropped, fall back to check_lookup_batch_task_timeout
      } else {
        const int64_t current_ts = ObTimeUtility::current_time();
        for (int64_t i = 0; i < id_cnt_; ++i) {
          sql::ObRpcDasAsyncAccessCallBack *cb = NULL;
          if (is_timeout) {
            // RPC-layer timeout may fire much earlier than the DAS deadline in ob_batch;
            // only erase + fail when the cb itself has actually reached its deadline,
            // otherwise leave it for ObDASRef::check_lookup_batch_task_timeout.
            sql::ObRpcDasAsyncAccessCallBack *peek_cb = NULL;
            if (OB_SUCC(das->peek_das_batch_request(ids_[i], peek_cb))
                && OB_NOT_NULL(peek_cb)
                && peek_cb->get_async_cb_context()->get_timeout_ts() <= current_ts
                && OB_SUCC(das->fetch_and_erase_das_batch_request(ids_[i], cb))
                && OB_NOT_NULL(cb)) {
              cb->fail_lookup_batch_result(OB_TIMEOUT);
            }
          } else if (OB_SUCC(das->fetch_and_erase_das_batch_request(ids_[i], cb))
                     && OB_NOT_NULL(cb)) {
            const int err = (this->get_error() == OB_ALLOCATE_MEMORY_FAILED)
                                ? OB_ALLOCATE_MEMORY_FAILED
                                : OB_INVALID_ERROR;
            cb->fail_lookup_batch_result(err);
          }
        }
      }
    }
    UNUSED(ret);
  }

  uint64_t tenant_id_;
  int64_t id_cnt_;
  int64_t ids_[MAX_IDS];
  DISALLOW_COPY_AND_ASSIGN(LookupBatchCallBack);
};

int ObBatchRpcProxy::post_batch(uint64_t tenant_id, const common::ObAddr &addr, const int64_t dst_cluster_id, int batch_type, ObBatchPacket& pkt)
{
  int ret = OB_SUCCESS;
  static BatchCallBack s_cb;
  static LookupBatchCallBack s_lookup_cb;
  if (CLOG_BATCH_REQ == batch_type) {
    ret = this->to(addr).dst_cluster_id(dst_cluster_id).by(tenant_id).group_id(share::OBCG_CLOG).post_packet(pkt, &s_cb);
  } else if (LOOKUP_DAS_BATCH_REQ1 == batch_type || LOOKUP_DAS_BATCH_REQ2 == batch_type) {
    int64_t rpc_timeout = ObRpcProxy::MAX_RPC_TIMEOUT;
    const int64_t simulate_timeout = -EVENT_CALL(ERRSIM_LOOKUP_BATCH_RPC_TIMEOUT);
    if (simulate_timeout > 0) {
      rpc_timeout = simulate_timeout;
      RPC_LOG(INFO, "simulate lookup batch rpc timeout", K(rpc_timeout), K(batch_type), K(tenant_id), K(addr));
    }
    ret = this->to(addr)
              .dst_cluster_id(dst_cluster_id)
              .by(tenant_id)
              .as(tenant_id)
              .timeout(rpc_timeout)
              .post_packet(pkt, &s_lookup_cb);
  } else {
    ret = this->to(addr).dst_cluster_id(dst_cluster_id).by(tenant_id).as(OB_SERVER_TENANT_ID).post_packet(pkt, &s_cb);
  }
  return ret;
}

}; // end namespace rpc
}; // end namespace oceanbase
