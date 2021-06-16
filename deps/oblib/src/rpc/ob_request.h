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

#ifndef _OCEABASE_RPC_OB_REQUEST_H_
#define _OCEABASE_RPC_OB_REQUEST_H_

#include <arpa/inet.h>
#include "io/easy_io.h"
#include "lib/oblog/ob_log.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/queue/ob_link.h"
#include "lib/hash/ob_fixed_hash2.h"
#include "rpc/ob_packet.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/ob_lock_wait_node.h"
#include "rpc/ob_reusable_mem.h"

namespace oceanbase {
namespace rpc {

using common::ObAddr;
typedef common::ObCurTraceId::TraceId TraceId;

class ObRequest : public common::ObLink {
public:
  enum Type { OB_RPC, OB_MYSQL, OB_TASK, OB_GTS_TASK, OB_SQL_TASK };

public:
  explicit ObRequest(Type type)
      : ez_req_(NULL),
        type_(type),
        group_id_(0),
        pkt_(NULL),
        connection_phase_(ConnectionPhaseEnum::CPE_CONNECTED),
        recv_timestamp_(0),
        enqueue_timestamp_(0),
        request_arrival_time_(0),
        arrival_push_diff_(0),
        push_pop_diff_(0),
        pop_process_start_diff_(0),
        process_start_end_diff_(0),
        process_end_response_diff_(0),
        trace_id_(),
        discard_flag_(false),
        large_retry_flag_(false),
        retry_times_(0)
  {}
  virtual ~ObRequest()
  {}

  Type get_type() const
  {
    return type_;
  }
  void set_type(const Type& type)
  {
    type_ = type;
  }

  int32_t get_group_id() const
  {
    return group_id_;
  }
  void set_group_id(const int32_t& group_id)
  {
    group_id_ = group_id;
  }
  bool large_retry_flag() const
  {
    return large_retry_flag_;
  }
  void set_large_retry_flag(bool large_retry_flag)
  {
    large_retry_flag_ = large_retry_flag;
  }

  void set_packet(const ObPacket* pkt);
  const ObPacket& get_packet() const;

  easy_request_t* get_request() const;
  void set_request(easy_request_t* r);
  void set_request_rtcode(int8_t rt) const;

  int64_t get_send_timestamp() const;
  int64_t get_receive_timestamp() const;
  void set_receive_timestamp(const int64_t recv_timestamp);
  void set_enqueue_timestamp(const int64_t enqueue_timestamp);
  void set_request_arrival_time(const int64_t now);
  void set_arrival_push_diff(const int64_t now);
  void set_push_pop_diff(const int64_t now);
  void set_pop_process_start_diff(const int64_t now);
  void set_process_start_end_diff(const int64_t now);
  void set_process_end_response_diff(const int64_t now);
  void set_discard_flag(const bool discard_flag);
  void set_retry_times(const int32_t retry_times);
  int64_t get_enqueue_timestamp() const;
  int64_t get_request_arrival_time() const;
  int32_t get_arrival_push_diff() const;
  int32_t get_push_pop_diff() const;
  int32_t get_pop_process_start_diff() const;
  int32_t get_process_start_end_diff() const;
  int32_t get_process_end_response_diff() const;
  bool get_discard_flag() const;
  int32_t get_retry_times() const;

  ObAddr get_peer() const;

  void set_connection_phase(ConnectionPhaseEnum connection_phase)
  {
    connection_phase_ = connection_phase;
  }
  ConnectionPhaseEnum get_connection_phase() const
  {
    return connection_phase_;
  }
  bool is_in_connected_phase() const
  {
    return ConnectionPhaseEnum::CPE_CONNECTED == connection_phase_;
  }
  bool is_in_ssl_connect_phase() const
  {
    return ConnectionPhaseEnum::CPE_SSL_CONNECT == connection_phase_;
  }
  bool is_in_authed_phase() const
  {
    return ConnectionPhaseEnum::CPE_AUTHED == connection_phase_;
  }

  void set_session(void* session);
  void* get_session() const;
  SSL* get_ssl_st() const;

  void disconnect() const;
  easy_request_t* get_ez_req() const;
  void on_process_begin();
  char* easy_reusable_alloc(const int64_t size) const;
  char* easy_alloc(const int64_t size) const;

  TraceId generate_trace_id(const ObAddr& addr);
  const TraceId& get_trace_id() const
  {
    return trace_id_;
  }

  ObLockWaitNode& get_lock_wait_node()
  {
    return lock_wait_node_;
  }
  bool is_retry_on_lock() const
  {
    return lock_wait_node_.try_lock_times_ > 0;
  }
  VIRTUAL_TO_STRING_KV("packet", pkt_, "type", type_, "group", group_id_, "connection_phase", connection_phase_);

  ObLockWaitNode lock_wait_node_;

public:
  easy_request_t* ez_req_;  // set in ObRequest new
protected:
  Type type_;
  int32_t group_id_;
  const ObPacket* pkt_;  // set in rpc handler
  ConnectionPhaseEnum connection_phase_;
  int64_t recv_timestamp_;
  int64_t enqueue_timestamp_;
  mutable ObReusableMem reusable_mem_;
  int64_t request_arrival_time_;
  int32_t arrival_push_diff_;
  int32_t push_pop_diff_;
  int32_t pop_process_start_diff_;
  int32_t process_start_end_diff_;
  int32_t process_end_response_diff_;

  mutable TraceId trace_id_;
  bool discard_flag_;
  bool large_retry_flag_;
  int32_t retry_times_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRequest);
};  // end of class ObRequest

inline void ObRequest::set_packet(const ObPacket* pkt)
{
  pkt_ = pkt;
}

inline const ObPacket& ObRequest::get_packet() const
{
  return *pkt_;
}

inline easy_request_t* ObRequest::get_request() const
{
  return ez_req_;
}

inline void ObRequest::set_request(easy_request_t* r)
{
  ez_req_ = r;
}

inline void ObRequest::set_request_rtcode(int8_t rt) const
{
  if (OB_ISNULL(ez_req_)) {
    RPC_LOG(ERROR, "invalid argument", K(ez_req_));
  } else {
    ez_req_->retcode = rt;
  }
}

inline int64_t ObRequest::get_receive_timestamp() const
{
  return recv_timestamp_;
}

inline void ObRequest::set_receive_timestamp(const int64_t recv_timestamp)
{
  recv_timestamp_ = recv_timestamp;
}

inline int64_t ObRequest::get_enqueue_timestamp() const
{
  return enqueue_timestamp_;
}

inline void ObRequest::set_enqueue_timestamp(const int64_t enqueue_timestamp)
{
  enqueue_timestamp_ = enqueue_timestamp;
}

inline int64_t ObRequest::get_request_arrival_time() const
{
  return request_arrival_time_;
}

inline void ObRequest::set_request_arrival_time(const int64_t request_arrival_time)
{
  request_arrival_time_ = request_arrival_time;
}

inline int32_t ObRequest::get_arrival_push_diff() const
{
  return arrival_push_diff_;
}

inline void ObRequest::set_arrival_push_diff(const int64_t now)
{
  arrival_push_diff_ = (int32_t)(now - request_arrival_time_);
}

inline int32_t ObRequest::get_push_pop_diff() const
{
  return push_pop_diff_;
}

inline void ObRequest::set_push_pop_diff(const int64_t now)
{
  push_pop_diff_ = (int32_t)(now - request_arrival_time_ - arrival_push_diff_);
}

inline int32_t ObRequest::get_pop_process_start_diff() const
{
  return pop_process_start_diff_;
}

inline void ObRequest::set_pop_process_start_diff(const int64_t now)
{
  pop_process_start_diff_ = (int32_t)(now - request_arrival_time_ - arrival_push_diff_ - push_pop_diff_);
}

inline int32_t ObRequest::get_process_start_end_diff() const
{
  return process_start_end_diff_;
}

inline void ObRequest::set_process_start_end_diff(const int64_t now)
{
  process_start_end_diff_ =
      (int32_t)(now - request_arrival_time_ - arrival_push_diff_ - push_pop_diff_ - pop_process_start_diff_);
}

inline int32_t ObRequest::get_process_end_response_diff() const
{
  return process_end_response_diff_;
}

inline void ObRequest::set_process_end_response_diff(const int64_t now)
{
  process_end_response_diff_ = (int32_t)(now - request_arrival_time_ - arrival_push_diff_ - push_pop_diff_ -
                                         pop_process_start_diff_ - process_start_end_diff_);
}

inline bool ObRequest::get_discard_flag() const
{
  return discard_flag_;
}

inline void ObRequest::set_discard_flag(const bool discard_flag)
{
  discard_flag_ = discard_flag;
}

inline ObAddr ObRequest::get_peer() const
{
  ObAddr addr;
  if (OB_ISNULL(ez_req_) || OB_ISNULL(ez_req_->ms) || OB_ISNULL(ez_req_->ms->c)) {
    RPC_LOG(ERROR, "invalid argument", K(ez_req_));
  } else {
    easy_addr_t& ez = ez_req_->ms->c->addr;
    if (AF_INET == ez.family && !addr.set_ipv4_addr(ntohl(ez.u.addr), ntohs(ez.port))) {
      RPC_LOG(ERROR, "fail to set ipv4 addr", K(ez_req_));
    } else if (AF_INET6 == ez.family && !addr.set_ipv6_addr(ez.u.addr6, ntohs(ez.port))) {
      RPC_LOG(ERROR, "fail to set ipv6 addr", K(ez_req_));
    }  // otherwise leave addr be zeros
  }
  return addr;
}

inline void ObRequest::set_session(void* session)
{
  if (OB_ISNULL(session) || OB_ISNULL(ez_req_) || OB_ISNULL(ez_req_->ms) || OB_ISNULL(ez_req_->ms->c)) {
    RPC_LOG(ERROR, "invalid argument", K(ez_req_));
  } else {
    ez_req_->ms->c->user_data = session;
  }
}

inline void* ObRequest::get_session() const
{
  void* session = NULL;
  if (OB_ISNULL(ez_req_) || OB_ISNULL(ez_req_->ms) || OB_ISNULL(ez_req_->ms->c)) {
    RPC_LOG(ERROR, "invalid argument", K(ez_req_));
  } else {
    session = ez_req_->ms->c->user_data;
  }
  return session;
}

inline SSL* ObRequest::get_ssl_st() const
{
  SSL* ssl_st = NULL;
  if (OB_ISNULL(ez_req_) || OB_ISNULL(ez_req_->ms) || OB_ISNULL(ez_req_->ms->c)) {
    RPC_LOG(ERROR, "invalid argument", K(ez_req_));
  } else if (NULL != ez_req_->ms->c->sc) {
    ssl_st = ez_req_->ms->c->sc->connection;
  }
  return ssl_st;
}

inline int32_t ObRequest::get_retry_times() const
{
  return retry_times_;
}

inline void ObRequest::set_retry_times(const int32_t retry_times)
{
  retry_times_ = retry_times;
}
inline int64_t ObRequest::get_send_timestamp() const
{
  int64_t ts = 0;
  if (type_ == OB_RPC && NULL != pkt_) {
    ts = reinterpret_cast<const obrpc::ObRpcPacket*>(pkt_)->get_timestamp();
  }
  return ts;
}

inline void ObRequest::disconnect() const
{
  if (OB_ISNULL(ez_req_) || OB_ISNULL(ez_req_->ms)) {
    RPC_LOG(ERROR, "invalid argument", K(ez_req_));
  } else {
    easy_connection_destroy_dispatch(ez_req_->ms->c);
  }
}

inline easy_request_t* ObRequest::get_ez_req() const
{
  return ez_req_;
}

inline TraceId ObRequest::generate_trace_id(const ObAddr& addr)
{
  if (trace_id_.is_invalid()) {
    if (OB_RPC == type_ && !OB_ISNULL(pkt_)) {
      const obrpc::ObRpcPacket* packet = static_cast<const obrpc::ObRpcPacket*>(pkt_);
      const uint64_t* trace_id = packet->get_trace_id();
      trace_id_.set(trace_id);
    }
    if (trace_id_.is_invalid()) {
      trace_id_.init(addr);
    }
  }
  return trace_id_;
}

}  // namespace rpc
}  // end of namespace oceanbase

#endif /* _OCEABASE_RPC_OB_REQUEST_H_ */
