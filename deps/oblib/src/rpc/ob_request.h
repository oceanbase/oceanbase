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
#include "lib/time/ob_time_utility.h"
#include "lib/queue/ob_link.h"
#include "lib/hash/ob_fixed_hash2.h"
#include "rpc/ob_packet.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/ob_lock_wait_node.h"
#include "rpc/ob_reusable_mem.h"

namespace oceanbase
{
namespace obmysql
{
  int get_fd_from_sess(void *sess);
}

namespace rpc
{

using common::ObAddr;
typedef common::ObCurTraceId::TraceId TraceId;
class ObSqlRequestOperator;
class ObRequest: public common::ObLink
{
public:
  friend class ObSqlRequestOperator;
  enum Type { OB_RPC, OB_MYSQL, OB_TASK, OB_TS_TASK, OB_SQL_TASK, OB_SQL_SOCK_TASK };
  enum TransportProto { TRANSPORT_PROTO_EASY = 0, TRANSPORT_PROTO_POC = 1 };
  enum Stat {
      OB_EASY_REQUEST_EZ_RECV                 = 0,
      OB_EASY_REQUEST_RPC_DELIVER             = 1,
      OB_EASY_REQUEST_MYSQL_DELIVER           = 2,
      OB_EASY_REQUEST_TENANT_RECEIVED         = 3,
      OB_EASY_REQUEST_TENANT_DISPATCHED       = 4,
      OB_EASY_REQUEST_WORKER_PROCESSOR_RUN    = 5,
      OB_EASY_REQUEST_QHANDLER_PROCESSOR_RUN  = 6,
      OB_EASY_REQUEST_RPC_PROCESSOR_RUN       = 7,
      OB_EASY_REQUEST_SQL_PROCESSOR_HANDLE    = 8,
      OB_EASY_REQUEST_SQL_SOCK_PROCESSOR_RUN  = 9,
      OB_EASY_REQUEST_SQL_PROCESSOR_RUN       = 10,
      OB_EASY_REQUEST_MPQUERY_PROCESS         = 11,
      OB_EASY_REQUEST_MPQUERY_PROCESS_DONE    = 12,
      OB_EASY_REQUEST_RPC_PROCESSOR_RUN_DONE  = 13,
      OB_EASY_REQUEST_RPC_ASYNC_RSP           = 14,
      OB_EASY_REQUEST_TABLE_API_END_TRANS     = 15,
      OB_EASY_REQUEST_TABLE_API_ACOM_TRANS    = 16,
      OB_EASY_REQUEST_WAKEUP                  = 255,
      OB_FINISH_SQL_REQUEST                   = 256,
  };
public:
  explicit ObRequest(Type type, int nio_protocol=0)
      : ez_req_(NULL), handling_state_(-1), nio_protocol_(nio_protocol), type_(type), handle_ctx_(NULL), group_id_(0), sql_req_level_(0), pkt_(NULL),
        connection_phase_(ConnectionPhaseEnum::CPE_CONNECTED),
        recv_timestamp_(0), enqueue_timestamp_(0),
        request_arrival_time_(0), recv_mts_(), arrival_push_diff_(0),
        push_pop_diff_(0), pop_process_start_diff_(0),
        process_start_end_diff_(0), process_end_response_diff_(0),
        trace_id_(), discard_flag_(false), large_retry_flag_(false), retry_times_(0)
  {
  }
  virtual ~ObRequest() {}

  int get_nio_protocol() const { return nio_protocol_; }
  void set_server_handle_context(void* ctx) { handle_ctx_ = ctx; }
  void* get_server_handle_context() const { return handle_ctx_; }
  Type get_type() const { return type_; }
  void set_type(const Type &type) { type_ = type; }

  int32_t get_group_id() const { return group_id_; }
  void set_group_id(const int32_t &group_id) { group_id_ = group_id; }
  int64_t get_sql_request_level() const { return sql_req_level_; }
  void set_sql_request_level(const int64_t &sql_req_level) { sql_req_level_ = sql_req_level; }
  bool large_retry_flag() const { return large_retry_flag_; }
  void set_large_retry_flag(bool large_retry_flag) { large_retry_flag_ = large_retry_flag; }

  void set_packet(const ObPacket *pkt);
  const ObPacket &get_packet() const;
  void set_ez_req(easy_request_t *r);
  void enable_request_ratelimit();
  void set_request_background_flow();
  void set_request_opacket_size(int64_t size);
  int64_t get_send_timestamp() const;
  int64_t get_receive_timestamp() const;
  common::ObMonotonicTs get_receive_mts() const;
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
  int get_connfd();

  void set_connection_phase(ConnectionPhaseEnum connection_phase) { connection_phase_ = connection_phase; }
  bool is_in_connected_phase() const { return ConnectionPhaseEnum:: CPE_CONNECTED == connection_phase_; }
  bool is_from_unix_domain() const;

  easy_request_t *get_ez_req() const;
  void on_process_begin() { reusable_mem_.reuse(); }

  TraceId generate_trace_id(const ObAddr &addr);
  const TraceId &get_trace_id() const { return trace_id_; }
  void reset_trace_id() { trace_id_.reset(); }
  int set_trace_point(int trace_point = 0);

  ObLockWaitNode &get_lock_wait_node() { return lock_wait_node_; }
  bool is_retry_on_lock() const { return lock_wait_node_.try_lock_times_ > 0;}
  VIRTUAL_TO_STRING_KV("packet", pkt_, "type", type_, "group", group_id_, "sql_req_level", sql_req_level_, "connection_phase", connection_phase_, K(recv_timestamp_), K(enqueue_timestamp_), K(request_arrival_time_), K(trace_id_));

  ObLockWaitNode lock_wait_node_;
  mutable ObReusableMem reusable_mem_;
public:
  easy_request_t *ez_req_; // set in ObRequest new
  int32_t handling_state_; //for sql nio or other frame work
protected:
  int nio_protocol_;
  Type type_;
  void* handle_ctx_;
  int32_t group_id_;
  int64_t sql_req_level_;
  const ObPacket *pkt_; // set in rpc handler
  ConnectionPhaseEnum connection_phase_;
  int64_t recv_timestamp_;
  int64_t enqueue_timestamp_;
  int64_t request_arrival_time_;
  // only used by transaction
  common::ObMonotonicTs recv_mts_;
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
}; // end of class ObRequest

inline void ObRequest::set_packet(const ObPacket *pkt)
{
  pkt_ = pkt;
}

inline const ObPacket &ObRequest::get_packet() const
{
  return *pkt_;
}

inline void ObRequest::set_ez_req(easy_request_t *r)
{
  ez_req_ = r;
}

inline void ObRequest::enable_request_ratelimit()
{
  if (OB_ISNULL(ez_req_)) {
    RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid argument", K(ez_req_));
  } else {
    ez_req_->ratelimit_enabled = 1;
    ez_req_->redispatched = 0;
  }
}

inline void ObRequest::set_request_background_flow()
{
  if (OB_ISNULL(ez_req_)) {
    RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid argument", K(ez_req_));
  } else {
    ez_req_->is_bg_flow = 1;
  }
}

inline void ObRequest::set_request_opacket_size(int64_t size)
{
  if (OB_ISNULL(ez_req_)) {
    RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid argument", K(ez_req_));
  } else {
    ez_req_->opacket_size = size;
  }
}

inline int64_t ObRequest::get_receive_timestamp() const
{
  return recv_timestamp_;
}

inline common::ObMonotonicTs ObRequest::get_receive_mts() const
{
  return recv_mts_;
}

inline void ObRequest::set_receive_timestamp(const int64_t recv_timestamp)
{
  recv_timestamp_ = recv_timestamp;
  // used by transaction
  recv_mts_ = ObMonotonicTs::current_time();
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
  process_start_end_diff_ = (int32_t)(now - request_arrival_time_ - arrival_push_diff_
    - push_pop_diff_ - pop_process_start_diff_);
}

inline int32_t ObRequest::get_process_end_response_diff() const
{
  return process_end_response_diff_;
}

inline void ObRequest::set_process_end_response_diff(const int64_t now)
{
  process_end_response_diff_ = (int32_t)(now - request_arrival_time_ - arrival_push_diff_
    - push_pop_diff_ - pop_process_start_diff_ - process_start_end_diff_);
}

inline bool ObRequest::get_discard_flag() const
{
  return discard_flag_;
}

inline void ObRequest::set_discard_flag(const bool discard_flag)
{
  discard_flag_ = discard_flag;
}

inline int32_t ObRequest::get_retry_times() const
{
  return retry_times_;
}

inline int ObRequest::get_connfd()
{
  int connfd = -1;
  if (TRANSPORT_PROTO_EASY == nio_protocol_) {
    if (OB_ISNULL(ez_req_)) {
      RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid argument", K(ez_req_));
    } else {
      connfd = ez_req_->ms->c->fd;
    }
  } else if (TRANSPORT_PROTO_POC == nio_protocol_) {
    connfd = obmysql::get_fd_from_sess(handle_ctx_);
  }
  return connfd;
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

inline easy_request_t *ObRequest::get_ez_req() const
{
  return ez_req_;
}

inline TraceId ObRequest::generate_trace_id(const ObAddr &addr)
{
  if (trace_id_.is_invalid()) {
    if (OB_RPC == type_ && !OB_ISNULL(pkt_)) {
      const obrpc::ObRpcPacket *packet = static_cast<const obrpc::ObRpcPacket*>(pkt_);
      const uint64_t *trace_id = packet->get_trace_id();
      trace_id_.set(trace_id);
    }
    if (trace_id_.is_invalid()) {
      trace_id_.init(addr);
    }
  }
  return trace_id_;
}

inline  bool ObRequest::is_from_unix_domain() const
{
    bool ret = false;
    if (ez_req_ != NULL && ez_req_->ms != NULL) {
      easy_connection_t *c = ez_req_->ms->c;
      if (c != NULL) {
        ret = (c->addr.family == AF_UNIX);
      }
    }
    return ret;
}

void on_translate_fail(ObRequest* req, int ret);
} // end of namespace rp
} // end of namespace oceanbase
#include "ob_sql_request_operator.h"
#include "ob_rpc_request_operator.h"

#endif /* _OCEABASE_RPC_OB_REQUEST_H_ */
