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

#include "ob_batch_rpc.h"
#include "lib/thread_local/thread_buffer.h"
#include "share/ob_cluster_version.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace obrpc
{
static char* get_rpc_buffer(int64_t& size)
{
  return get_tc_buffer(size);
}

int build_batch_packet(const ObAddr &sender, const uint32_t batch_type, const uint32_t sub_type,
    const ObIFill& req, ObBatchPacket *&pkt,
    bool &is_dynamic_alloc)
{
  int ret = OB_SUCCESS;
  bool is_retry = false;
  ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
  uint32_t flag = 0;
  // with trace id
  flag = 1;
  // 优先从线程局部分配内存, 如果分配的内存不够大, 则动态分配内存
  while (true) {
    bool need_retry = false;
    // rewrite ret
    ret = OB_SUCCESS;
    int64_t limit = 0;
    ObSimpleReqHeader *req_header = NULL;
    const int64_t header_end_pos = sizeof(*pkt) + sizeof(*req_header);
    if (OB_ISNULL(pkt = (ObBatchPacket *)get_rpc_buffer(limit))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      is_dynamic_alloc = false;
      if (req.get_estimate_size() > (limit * 4 / 5) || is_retry) {
        // 多分配1024字节用于填充其他字段
        limit = req.get_req_size() + header_end_pos + 1024;
        if (OB_ISNULL(pkt = (ObBatchPacket *)ob_malloc(limit, SET_USE_500("RPC_BATCH_BUF")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          is_dynamic_alloc = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      req_header = (ObSimpleReqHeader*)(pkt + 1);
      char *buf = (char*)pkt + header_end_pos;
      int64_t pos = 0;
      int64_t filled_size = 0;
      int64_t total_req_size = 0;
      if ((1 == flag) && OB_FAIL(trace_id->serialize(buf, limit - header_end_pos, pos))) {
        RPC_LOG(WARN, "serialize traceid failed", K(ret), K(sender), K(batch_type), K(sub_type));
      } else if (OB_FAIL(req.fill_buffer(buf + pos, limit - header_end_pos - pos, filled_size))) {
        if (OB_SIZE_OVERFLOW != ret) {
          RPC_LOG(WARN, "serialize request failed", K(ret), K(sender), K(batch_type), K(sub_type));
        }
      } else {
        total_req_size = pos + filled_size;
        pkt->set((int32_t)(sizeof(*req_header) + total_req_size), sender, (char*)(pkt + 1));
        req_header->set(flag, batch_type, sub_type, (int32_t)total_req_size);
      }
      if (OB_FAIL(ret) && !is_retry) {
        need_retry = true;
        is_retry = true;
      }
    }
    if (!need_retry) {
      break;
    }
  }
  if (OB_FAIL(ret) && is_dynamic_alloc) {
    ob_free(pkt);
    pkt = NULL;
    is_dynamic_alloc = false;
  }
  return ret;
}

int64_t ObRpcBuffer::send(Rpc& rpc, uint64_t tenant_id, const ObAddr &sender, bool send_current)
{
  int64_t seq = -1;
  int64_t req_cnt = 0;
  char* buf = NULL;
  int64_t size = buffer_.read(seq, buf, req_cnt, send_current);
  if (size > 0) {
    Packet* pkt = (Packet*)buf;
    pkt->set((int32_t)(size - sizeof(*pkt)), sender, (char*)(pkt+1));
    common::ObAddr dest = server_;
    if (CLOG_BATCH_REQ_NODELAY == batch_type_) {
      uint32_t new_port = dest.get_port() + BATCH_RPC_PORT_DELTA;
      dest.set_port(new_port);
    }
    rpc.post_batch(tenant_id, dest, dst_cluster_id_, batch_type_, *pkt);
    buffer_.reuse(seq);
  }
  return size;
}


int ObBatchRpcBase::post(const uint64_t tenant_id, const ObAddr &dest, const int64_t dst_cluster_id,
    const uint32_t batch_type, const uint32_t sub_type,
    const Req &req)
{
  int ret = OB_SUCCESS;

  bool is_dynamic_alloc = false;
  CriticalGuard(get_qs());
  RpcBuffer* buffer = fetch(tenant_id, dest, dst_cluster_id);

  ObAddr new_dest = dest;
  if (CLOG_BATCH_REQ_NODELAY == batch_type) {
    new_dest.set_port(dest.get_port() + BATCH_RPC_PORT_DELTA);
  }
  if (NULL == buffer || !buffer->fill(sub_type, req)) {
    Packet* pkt = NULL;
    if (OB_FAIL(build_batch_packet(self_, batch_type, sub_type, req, pkt, is_dynamic_alloc))) {
      RPC_LOG(WARN, "build_batch_packet fail", K(ret));
    } else if (OB_ISNULL(pkt)) {
      ret = OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "pkt is NULL", K(ret));
    } else {
      if (OB_FAIL(rpc_->post_batch(tenant_id, new_dest, dst_cluster_id, batch_type, *pkt))) {
        RPC_LOG(WARN, "do_post fail", K(ret));
      }
      if (OB_UNLIKELY(is_dynamic_alloc)) {
        ob_free(pkt);
        pkt = NULL;
      }
    }
  } else {
    if (delay_us_ <= 0) {
      cond_.signal();
    }
  }
  return ret;
}

int build_batch_packet(const ObAddr &sender, const uint32_t batch_type, const int16_t sub_type,
    const ObLSID& ls, const ObIFill& req, ObBatchPacket *&pkt,
    bool &is_dynamic_alloc)
{
  int ret = OB_SUCCESS;
  bool is_retry = false;
  ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
  uint32_t flag = 0;
  // with trace id
  flag = 1;
  // 优先从线程局部分配内存, 如果分配的内存不够大, 则动态分配内存
  while (true) {
    bool need_retry = false;
    // rewrite ret
    ret = OB_SUCCESS;
    int64_t limit = 0;
    ObSimpleReqHeader *req_header = NULL;
    const int64_t header_end_pos = sizeof(*pkt) + sizeof(*req_header);
    if (OB_ISNULL(pkt = (ObBatchPacket *)get_rpc_buffer(limit))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      is_dynamic_alloc = false;
      if (req.get_estimate_size() > (limit * 4 / 5) || is_retry) {
        // 多分配1024字节用于填充其他字段
        limit = req.get_req_size() + header_end_pos + 1024;
        if (OB_ISNULL(pkt = (ObBatchPacket *)ob_malloc(limit, SET_USE_500("RPC_BATCH_BUF")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          is_dynamic_alloc = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      req_header = (ObSimpleReqHeader*)(pkt + 1);
      char *buf = (char*)pkt + header_end_pos;
      int64_t pos = 0;
      int64_t filled_size = 0;
      int64_t total_req_size = 0;
      if ((1 == flag) && OB_FAIL(trace_id->serialize(buf, limit - header_end_pos, pos))) {
        RPC_LOG(WARN, "serialize traceid failed", K(ret), K(sender), K(batch_type), K(sub_type), K(ls));
      } else if (OB_FAIL(ls.serialize(buf, limit - header_end_pos, pos))) {
        RPC_LOG(WARN, "serialize ls failed", K(ret), K(sender), K(batch_type), K(sub_type), K(ls));
      } else if (OB_FAIL(req.fill_buffer(buf + pos, limit - header_end_pos - pos, filled_size))) {
        if (OB_SIZE_OVERFLOW != ret) {
          RPC_LOG(WARN, "serialize request failed", K(ret), K(sender), K(batch_type), K(sub_type), K(ls));
        }
      } else {
        total_req_size = pos + filled_size;
        pkt->set((int32_t)(sizeof(*req_header) + total_req_size), sender, (char*)(pkt + 1));
        req_header->set(flag, batch_type, sub_type, (int32_t)total_req_size);
      }
      if (OB_FAIL(ret) && !is_retry) {
        need_retry = true;
        is_retry = true;
      }
    }
    if (!need_retry) {
      break;
    }
  }
  if (OB_FAIL(ret) && is_dynamic_alloc) {
    ob_free(pkt);
    pkt = NULL;
    is_dynamic_alloc = false;
  }
  return ret;
}

int ObBatchRpcBase::post(const uint64_t tenant_id, const ObAddr &dest, const int64_t dst_cluster_id,
    const uint32_t batch_type, const int16_t sub_type,
    const ObLSID& ls, const Req &req)
{
  int ret = OB_SUCCESS;

  bool is_dynamic_alloc = false;
  CriticalGuard(get_qs());
  RpcBuffer* buffer = fetch(tenant_id, dest, dst_cluster_id);

  ObAddr new_dest = dest;
  if (CLOG_BATCH_REQ_NODELAY == batch_type) {
    new_dest.set_port(dest.get_port() + BATCH_RPC_PORT_DELTA);
  }
  if (NULL == buffer || !buffer->fill(sub_type, ls, req)) {
    Packet* pkt = NULL;
    if (OB_FAIL(build_batch_packet(self_, batch_type, sub_type, ls, req, pkt, is_dynamic_alloc))) {
      RPC_LOG(WARN, "build_batch_packet fail", K(ret));
    } else if (OB_ISNULL(pkt)) {
      ret = OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "pkt is NULL", K(ret));
    } else {
      if (OB_FAIL(rpc_->post_batch(tenant_id, new_dest, dst_cluster_id, batch_type, *pkt))) {
        RPC_LOG(WARN, "do_post fail", K(ret));
      }
      if (OB_UNLIKELY(is_dynamic_alloc)) {
        ob_free(pkt);
        pkt = NULL;
      }
    }
  } else {
    if (delay_us_ <= 0) {
      cond_.signal();
    }
  }
  return ret;
}

ObRpcBuffer* ObBatchRpcBase::fetch(const uint64_t tenant_id, const ObAddr &server, const int64_t dst_cluster_id)
{
  RpcBuffer* buffer = NULL;
  if (!is_inited_) {
  } else {
    RpcBuffer key(tenant_id, server, dst_cluster_id, 0, 0, 0);
    while (NULL == buffer) {
      if (0 == buffer_map_->get(&key, buffer)) {
        // do nothing
      } else if (NULL == (buffer = create_buffer(tenant_id, server, dst_cluster_id))) {
        break;
      } else if (0 != buffer_map_->insert(buffer)) {
        destroy_buffer(buffer);
        buffer = NULL;
      } else {
        RPC_LOG(INFO, "RpcBuffer.create", K(server), K(dst_cluster_id), K(tenant_id), "hash", buffer->hash(), K_(batch_type));
      }
    }
  }
  return buffer;
}

void ObBatchRpcBase::do_work()
{
  if (is_inited_) {
    const int64_t start_ts = common::ObTimeUtility::current_time();
    static const int64_t CLEAN_SVR_INTERVAL = 3600 * 1000 * 1000l;  // clean server when idle time reachs 1h
    bool need_gc = false;
    RpcBuffer* iter = NULL;
    while(NULL != (iter = buffer_map_->quick_next(iter))) {
      iter->freeze();
    }
    while(NULL != (iter = buffer_map_->quick_next(iter))) {
      int cnt = 0;
      while (iter->send(*rpc_, iter->get_tenant_id(), self_, 0 == cnt) > 0) {
        cnt++;
      }
      if (start_ts - iter->get_last_use_ts() > CLEAN_SVR_INTERVAL
          && iter->is_empty()) {
        need_gc = true;
      }
    }
    if (OB_UNLIKELY(need_gc)) {
      ObSEArray<common::ObAddr, BUCKET_NUM> del_list;
      ObSEArray<uint64_t, BUCKET_NUM> tenant_list;
      ObSEArray<int64_t, BUCKET_NUM> del_cluster_id_list;
      while(NULL != (iter = buffer_map_->quick_next(iter))) {
        if (start_ts - iter->get_last_use_ts() > CLEAN_SVR_INTERVAL && iter->is_empty()) {
          (void) del_list.push_back(iter->get_server());
          (void) tenant_list.push_back(iter->get_tenant_id());
          (void) del_cluster_id_list.push_back(iter->get_dst_cluster_id());
        }
      }
      // 执行GC
      for (int64_t i = 0; i < del_list.count(); ++i) {
        ObAddr cur_server = del_list.at(i);
        const uint64_t tenant_id = tenant_list.at(i);
        const int64_t cluster_id = del_cluster_id_list.at(i);
        RpcBuffer cur_key(tenant_id, cur_server, cluster_id, 0, 0, 0);
        const uint64_t hash_val = cur_key.hash();
        RpcBuffer* cur_buf = NULL;
        if (0 == buffer_map_->del(&cur_key, cur_buf)) {
          // del succ, then wait
          WaitQuiescent(get_qs());
          // send all msg
          while (cur_buf->send(*rpc_, cur_buf->get_tenant_id(), self_, true) > 0) {
          }
          destroy_buffer(cur_buf);
          RPC_LOG(INFO, "batch_rpc delete server success", K(cur_server), K(cluster_id), K(tenant_id), K(hash_val), K_(batch_type));
        }
      }
    }
    const int64_t cost_time = common::ObTimeUtility::current_time() - start_ts;
    int64_t sleep_ts = delay_us_ > 0 ? delay_us_ : (10 * 1000);
    sleep_ts -= cost_time;
    if (sleep_ts < 0) {
      sleep_ts = 0;
    }
    if (delay_us_ > 0) {
      ob_usleep((int32_t)sleep_ts);
    } else {
      cond_.wait(sleep_ts);
    }
  }
}

ObRpcBuffer* ObBatchRpcBase::create_buffer(const uint64_t tenant_id, const ObAddr& addr, const int64_t dst_cluster_id)
{
  const int64_t alloc_size = get_batch_buffer_size(batch_type_) * BATCH_BUFFER_COUNT;
  RpcBuffer* p = (RpcBuffer*)ob_malloc(alloc_size, SET_USE_500(ObModIds::OB_RPC_BUFFER));
  const int64_t buf_size = (alloc_size - 1024) / BATCH_BUFFER_COUNT;
  return (NULL == p)? NULL: new(p)RpcBuffer(tenant_id, addr, dst_cluster_id, batch_type_, buf_size, BATCH_BUFFER_COUNT);
}

void ObBatchRpcBase::destroy_buffer(RpcBuffer* p)
{
  ob_free(p);
}

int ObBatchRpcBase::get_dst_svr_list(common::ObIArray<share::ObCascadMember> &dst_list)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    RpcBuffer* iter = NULL;
    CriticalGuard(get_qs());
    while(OB_SUCC(ret) && NULL != (iter = buffer_map_->quick_next(iter))) {
      ObAddr addr = iter->get_server();
      const int64_t dst_cluster_id = iter->get_dst_cluster_id();
      share::ObCascadMember member(addr, dst_cluster_id);
      bool is_need_add = true;
      if (self_ == addr) {
        is_need_add = false;
      } else {
        const int64_t cnt = dst_list.count();
        for (int64_t i = 0; i < cnt; ++i) {
          if (addr == dst_list.at(i).get_server()) {
            if (dst_cluster_id == dst_list.at(i).get_cluster_id()
                || common::OB_INVALID_CLUSTER_ID == dst_cluster_id) {
              // 过滤cluster_id相同或者无效的场景
              is_need_add = false;
              break;
            }
          }
        }
      }
      if (is_need_add && OB_FAIL(dst_list.push_back(member))) {
        RPC_LOG(WARN, "dst_list.push_back failed", K(member));
      } else if (is_need_add) {
        RPC_LOG(INFO, "dst_list.push_back finished", K(ret), K(is_need_add), K(member), K(batch_type_), K(dst_list));
      }
    }
  }
  return ret;
}

int ObBatchRpc::get_dst_svr_list(common::ObIArray<share::ObCascadMember> &dst_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_THREAD_COUNT; ++i) {
    if (OB_FAIL(base_[i].get_dst_svr_list(dst_list))) {
      RPC_LOG(WARN, "get_dst_svr_list failed", K(ret));
    }
  }
  return ret;
}


}; // end namespace clog
}; // end namespace oceanbase
