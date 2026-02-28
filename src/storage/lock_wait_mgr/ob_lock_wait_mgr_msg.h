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

#ifndef OCEANBASE_LOCKWAITMGR_OB_LOCK_WAIT_MGR_MSG_
#define OCEANBASE_LOCKWAITMGR_OB_LOCK_WAIT_MGR_MSG_

#include "share/ob_define.h"
#include "share/rpc/ob_batch_proxy.h"
namespace oceanbase {

namespace lockwaitmgr {
enum LOCK_WAIT_MGR_MSG_TYPE
{
  LWM_UNKNOWN = 0,
  LWM_DST_ENQUEUE = 1, // inform destination to enqueue
  LWM_DST_ENQUEUE_RESP = 2,
  LWM_CHECK_NODE_STATE = 3,
  LWM_CHECK_NODE_STATE_RESP = 4,
  LWM_LOCK_RELEASE = 5, // inform remote scheduler lock release
  LWM_WAKE_UP  = 6, // wake up remote wait queue
  LWM_MSG_MAX // max msg type
};

class ObLWMMsgTypeChecker
{
  public:
    static bool is_valid_msg_type(const int16_t msg_type)
    {
      return LOCK_WAIT_MGR_MSG_TYPE::LWM_UNKNOWN < msg_type && msg_type < LOCK_WAIT_MGR_MSG_TYPE::LWM_MSG_MAX;
    }
};

struct ObLockWaitMgrMsg : public obrpc::ObIFill
{
public:
  explicit ObLockWaitMgrMsg(const int type = LWM_UNKNOWN) :
    type_(type),
    tenant_id_(OB_INVALID_TENANT_ID),
    hash_(0),
    node_id_(0),
    sender_addr_() {}
  ~ObLockWaitMgrMsg() {}
  virtual bool is_valid() const {
    return LWM_UNKNOWN != type_
        && is_valid_tenant_id(tenant_id_)
        && hash_ > 0
        && node_id_ > 0
        && sender_addr_.is_valid();
  }
  void init(uint64_t tenant_id,
            uint64_t hash,
            rpc::NodeID node_id,
            const common::ObAddr &sender_addr) {
    tenant_id_ = tenant_id;
    hash_ = hash;
    node_id_ = node_id;
    sender_addr_ = sender_addr;
  }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  int16_t get_msg_type() const { return type_; }
  uint64_t get_hash() const { return hash_; }
  void set_hash(uint64_t hash) { hash_ = hash; }
  rpc::NodeID get_node_id() const { return node_id_; }
  void set_node_id( rpc::NodeID node_id) { node_id_ = node_id; }
  common::ObAddr get_sender_addr() const { return sender_addr_; }
  void set_sender_addr(const common::ObAddr &addr) { sender_addr_ = addr; }
  virtual int fill_buffer(char* buf, int64_t size, int64_t &filled_size) const override {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const override { return get_serialize_size(); }
  VIRTUAL_TO_STRING_KV(K_(type),
                       K_(tenant_id),
                       K_(hash),
                       K_(node_id),
                       K_(sender_addr));
  OB_UNIS_VERSION_V(1);
private:
  int16_t type_;
  uint64_t tenant_id_;
  uint64_t hash_;
  rpc::NodeID node_id_;
  common::ObAddr sender_addr_;
};


struct ObLockWaitMgrDstEnqueueMsg : public ObLockWaitMgrMsg
{
  static const int QUERY_SQL_BUFFER_SIZE = 128;
public:
  ObLockWaitMgrDstEnqueueMsg() :
    ObLockWaitMgrMsg(LWM_DST_ENQUEUE), lock_seq_(0) {}
  int64_t get_lock_seq() const { return lock_seq_; }
  int64_t get_lock_ts() const { return lock_ts_; }
  uint32_t get_sess_id() const { return sess_id_; }
  int64_t get_tx_id() const { return tx_id_; }
  int64_t get_holder_tx_id() const { return holder_tx_id_; }
  int64_t get_query_timeout() const { return query_timeout_us_; }
  int64_t get_recv_ts() const { return recv_ts_; }
  int64_t get_ls_id() const { return ls_id_; }
  uint64_t get_tablet_id() const { return tablet_id_; }
  int64_t get_holder_tx_hold_seq() const { return holder_tx_hold_seq_value_; }
  int64_t get_tx_active_ts() const { return tx_active_ts_; }
  int64_t get_abs_timeout_ts() const { return abs_timeout_ts_; }
  const ObString& get_query_sql() const { return query_sql_; }
  void init(uint64_t tenant_id, const common::ObAddr &sender_addr, uint64_t hash,
            int64_t lock_seq, int64_t lock_ts, rpc::NodeID node_id, int64_t tx_id,
            int64_t holder_tx_id, uint32_t sess_id, int64_t query_timeout_us,
            int64_t recv_ts, int64_t ls_id, uint64_t tablet_id, int64_t holder_tx_hold_seq_value,
            int64_t tx_active_ts, int64_t abs_timeout_ts, ObString &query_sql) {
    ObLockWaitMgrMsg::init(tenant_id, hash, node_id, sender_addr);
    lock_seq_ = lock_seq;
    lock_ts_ = lock_ts;
    tx_id_ = tx_id;
    holder_tx_id_ = holder_tx_id;
    sess_id_ = sess_id;
    query_timeout_us_ = query_timeout_us;
    recv_ts_ = recv_ts;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    holder_tx_hold_seq_value_ = holder_tx_hold_seq_value;
    tx_active_ts_ = tx_active_ts;
    abs_timeout_ts_ = abs_timeout_ts;
    query_sql_.assign(query_sql.ptr(), query_sql.length());
  }
  char buffer_for_serialization_[QUERY_SQL_BUFFER_SIZE];
  bool is_valid() const {
    return ObLockWaitMgrMsg::is_valid()
        && LWM_DST_ENQUEUE == get_msg_type()
        && lock_seq_ >= 0
        && lock_ts_ > 0
        && sess_id_ > 0
        && tx_id_ > 0
        && query_timeout_us_ > 0
        && recv_ts_ > 0
        && ls_id_ > 0
        && tablet_id_ > 0
        && holder_tx_hold_seq_value_ > 0
        && abs_timeout_ts_ > 0
        && !query_sql_.empty();
  }
  INHERIT_TO_STRING_KV("LockWaitMgrMsg", ObLockWaitMgrMsg, K_(lock_seq),
    K_(lock_ts), K_(tx_id), K_(holder_tx_id), K_(sess_id), K_(query_timeout_us), K_(recv_ts),
    K_(tablet_id), K_(ls_id), K_(holder_tx_hold_seq_value), K_(tx_active_ts), K_(abs_timeout_ts),
    K_(query_sql));
  OB_UNIS_VERSION_V(1);
private:
  int64_t lock_seq_;
  int64_t lock_ts_;
  int64_t tx_id_;
  int64_t holder_tx_id_;
  uint32_t sess_id_;
  int64_t query_timeout_us_;
  int64_t recv_ts_;
  int64_t ls_id_;
  uint64_t tablet_id_;
  int64_t holder_tx_hold_seq_value_;
  int64_t tx_active_ts_;
  int64_t abs_timeout_ts_;
  ObString query_sql_;
};

struct ObLockWaitMgrDstEnqueueRespMsg : public ObLockWaitMgrMsg
{
public:
  ObLockWaitMgrDstEnqueueRespMsg() :
    ObLockWaitMgrMsg(LWM_DST_ENQUEUE_RESP), enqueue_succ_(false) {}
  bool get_enenqueue_succ() const { return enqueue_succ_; }
  bool is_valid() const {
    return ObLockWaitMgrMsg::is_valid()
        &&  LWM_DST_ENQUEUE_RESP == get_msg_type();
  }
  bool &get_enqueue_succ() { return enqueue_succ_; }
  INHERIT_TO_STRING_KV("LockWaitMgrMsg", ObLockWaitMgrMsg, K_(enqueue_succ));
  OB_UNIS_VERSION_V(1);
private:
  bool enqueue_succ_;
};

struct ObLockWaitMgrCheckNodeStateMsg : public ObLockWaitMgrMsg
{
  ObLockWaitMgrCheckNodeStateMsg () :
    ObLockWaitMgrMsg(LWM_CHECK_NODE_STATE) {}
  bool is_valid() const {
    return ObLockWaitMgrMsg::is_valid()
        && LWM_CHECK_NODE_STATE == get_msg_type();
  }
  OB_UNIS_VERSION_V(1);
};

struct ObLockWaitMgrCheckNodeStateRespMsg : public ObLockWaitMgrMsg
{
public:
  ObLockWaitMgrCheckNodeStateRespMsg () :
    ObLockWaitMgrMsg(LWM_CHECK_NODE_STATE_RESP), is_exsit_(false) {}
  bool get_is_exsit() const { return is_exsit_; }
  void set_is_exsit(bool is_exsit) { is_exsit_ = is_exsit; }
  bool is_valid() const {
    return ObLockWaitMgrMsg::is_valid()
        && LWM_CHECK_NODE_STATE_RESP == get_msg_type();
  }
  INHERIT_TO_STRING_KV("LockWaitMgrMsg", ObLockWaitMgrMsg, K_(is_exsit));
  OB_UNIS_VERSION_V(1);
private:
  bool is_exsit_;
};

struct ObLockWaitMgrLockReleaseMsg : public ObLockWaitMgrMsg
{
  ObLockWaitMgrLockReleaseMsg () :
    ObLockWaitMgrMsg(LWM_LOCK_RELEASE) {}
  bool is_valid() const {
    return ObLockWaitMgrMsg::is_valid()
        && LWM_LOCK_RELEASE == get_msg_type();
  }
  OB_UNIS_VERSION_V(1);
};

struct ObLockWaitMgrWakeUpRemoteMsg : public ObLockWaitMgrMsg
{
  ObLockWaitMgrWakeUpRemoteMsg() :
    ObLockWaitMgrMsg(LWM_WAKE_UP) {}
  bool is_valid() const {
    return is_valid_tenant_id(get_tenant_id())
        && get_hash() > 0
        && get_sender_addr().is_valid()
        && LWM_WAKE_UP == get_msg_type();
  }
  OB_UNIS_VERSION_V(1);
};

} // namespace lockwaitmgr
} // namespace oceanbase

#endif // OCEANBASE_MEMTABLE_OB_LOCK_WAIT_MGR_MSG_