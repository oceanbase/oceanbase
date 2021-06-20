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

#ifndef OCEANBASE_GTS_OB_HA_GTS_H_
#define OCEANBASE_GTS_OB_HA_GTS_H_

#include "common/ob_member_list.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "share/ob_rpc_struct.h"
#include "ob_ha_gts_define.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}
namespace gts {
class ObHaGtsManager;
class ObGtsReq {
public:
  ObGtsReq(const common::ObAddr& client_addr, const uint64_t tenant_id, const transaction::MonotonicTs srr)
      : client_addr_(client_addr), tenant_id_(tenant_id), srr_(srr)
  {}
  ObGtsReq()
  {
    reset();
  }
  ~ObGtsReq()
  {}

public:
  void reset()
  {
    client_addr_.reset();
    tenant_id_ = OB_INVALID_TENANT_ID;
    srr_.reset();
  }
  const common::ObAddr& get_client_addr() const
  {
    return client_addr_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  transaction::MonotonicTs get_srr() const
  {
    return srr_;
  }

private:
  common::ObAddr client_addr_;
  uint64_t tenant_id_;
  transaction::MonotonicTs srr_;
};

typedef common::LinkHashNode<ObGtsID> HaGtsHashNode;
typedef common::LinkHashValue<ObGtsID> HaGtsHashValue;
class ObHaGts : public HaGtsHashValue {
public:
  ObHaGts();
  ~ObHaGts();

public:
  int init(const uint64_t gts_id, const int64_t epoch_id, const common::ObMemberList& member_list,
      const int64_t min_start_timestamp, obrpc::ObSrvRpcProxy* rpc_proxy, ObHaGtsManager* gts_mgr,
      const common::ObAddr& self_addr);
  bool is_inited() const;
  void reset();
  void destroy();

public:
  int handle_ping_request(const obrpc::ObHaGtsPingRequest& request, obrpc::ObHaGtsPingResponse& response);
  int handle_ping_response(const obrpc::ObHaGtsPingResponse& response);
  int handle_get_request(const obrpc::ObHaGtsGetRequest& request);
  int handle_heartbeat(const obrpc::ObHaGtsHeartbeat& heartbeat);
  int send_heartbeat();
  // Check the status of member and provide a basis for
  // membership change
  // Membership change trigger conditions:
  // 1) The time from the last change is more than 1s(to avoid frequent changes);
  // 2) The time from the create timestamp of gts replica is more than 30s
  //    (to avoid deletion during cluster startup);
  // 3) The number of member which is in memberlist is 2;
  // 4) The replica doesn't receive heartbeat.
  int check_member_status(bool& need_change_member, bool& miss_replica, common::ObAddr& offline_replica,
      int64_t& epoch_id, common::ObMemberList& member_list);
  int try_update_meta(const int64_t epoch_id, const common::ObMemberList& member_list);
  int get_meta(int64_t& epoch_id, common::ObMemberList& member_list) const;
  int remove_stale_req();
  int get_local_ts(int64_t& local_ts) const;
  int inc_update_local_ts(const int64_t local_ts);
  const static int64_t HA_GTS_RPC_TIMEOUT = 1 * 1000 * 1000;  // 1s
  const static int64_t STALE_REQ_INTERVAL = 1 * 1000 * 1000;  // 1s
private:
  int ping_request_(const common::ObAddr& server, const obrpc::ObHaGtsPingRequest& request);
  int send_gts_response_(const common::ObAddr& server, const obrpc::ObHaGtsGetResponse& response);
  int send_heartbeat_(const common::ObAddr& server, const obrpc::ObHaGtsHeartbeat& heartbeat);
  int get_miss_replica_(const common::ObMemberList& member_list1, const common::ObMemberList& member_list2,
      common::ObAddr& miss_replica) const;

private:
  typedef common::RWLock RWLock;
  typedef RWLock::WLockGuard WLockGuard;

  class EraseIfFunctor {
  public:
    EraseIfFunctor() : req_()
    {}
    ~EraseIfFunctor()
    {}

  public:
    bool operator()(const ObGtsReqID& req_id, ObGtsReq& req)
    {
      UNUSED(req_id);
      req_ = req;
      return true;
    }
    const ObGtsReq& get_req() const
    {
      return req_;
    }

  private:
    ObGtsReq req_;
  };

  class RemoveIfFunctor {
  public:
    RemoveIfFunctor(const transaction::MonotonicTs cur_ts) : cur_ts_(cur_ts)
    {}
    ~RemoveIfFunctor()
    {}

  public:
    bool operator()(const ObGtsReqID& req_id, ObGtsReq& req)
    {
      UNUSED(req_id);
      return cur_ts_ - req.get_srr() >= transaction::MonotonicTs(STALE_REQ_INTERVAL);
    }

  private:
    transaction::MonotonicTs cur_ts_;
  };

  class HeartbeatFunctor {
  public:
    HeartbeatFunctor()
    {}
    ~HeartbeatFunctor()
    {}

  public:
    bool operator()(const ObAddr& addr, int64_t& heartbeat_ts)
    {
      UNUSED(addr);
      heartbeat_ts = common::ObTimeUtility::current_time();
      return true;
    }
  };

  class CheckHeartbeatFunctor {
  public:
    CheckHeartbeatFunctor(const common::ObMemberList& member_list)
        : member_list_(member_list), heartbeat_member_list_(), offline_replica_()
    {}
    ~CheckHeartbeatFunctor()
    {}

  public:
    bool operator()(const ObAddr& addr, int64_t& heartbeat_ts)
    {
      int ret = OB_SUCCESS;
      bool bool_ret = false;
      if (member_list_.contains(addr)) {
        if (ObTimeUtility::current_time() - heartbeat_ts > GTS_OFFLINE_THRESHOLD) {
          offline_replica_ = addr;
        }
        if (OB_FAIL(heartbeat_member_list_.add_server(addr))) {
          STORAGE_LOG(WARN, "heartbeat_member_list_ add_server failed", K(ret));
        }
      } else {
        bool_ret = true;
      }
      return bool_ret;
    }
    const common::ObAddr& get_offline_replica() const
    {
      return offline_replica_;
    }
    const common::ObMemberList& get_heartbeat_member_list() const
    {
      return heartbeat_member_list_;
    }

  private:
    const common::ObMemberList& member_list_;
    common::ObMemberList heartbeat_member_list_;
    common::ObAddr offline_replica_;
  };

private:
  bool is_inited_;
  int64_t created_ts_;
  mutable RWLock lock_;
  uint64_t gts_id_;
  // Store the max response timestamp
  int64_t local_ts_;
  // The memberlist and its' version for replica
  int64_t epoch_id_;
  common::ObMemberList member_list_;
  uint64_t next_req_id_;
  common::ObLinearHashMap<ObGtsReqID, ObGtsReq> req_map_;
  common::ObLinearHashMap<ObAddr, int64_t> heartbeat_map_;
  int64_t last_change_member_ts_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObHaGtsManager* gts_mgr_;
  common::ObAddr self_addr_;
  bool is_serving_;
  // Single replica mode, need to be modified under lock protection
  bool is_single_mode_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHaGts);
};

class ObHaGtsFactory {
public:
  static ObHaGts* alloc()
  {
    ATOMIC_INC(&alloc_cnt_);
    return op_reclaim_alloc(ObHaGts);
  }
  static void free(ObHaGts* gts)
  {
    ATOMIC_INC(&free_cnt_);
    op_reclaim_free(gts);
  }
  static void statistics()
  {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      CLOG_LOG(INFO, "ObHaGtsFactory statistics", K(alloc_cnt_), K(free_cnt_), "using_cnt", alloc_cnt_ - free_cnt_);
    }
  }

private:
  static int64_t alloc_cnt_;
  static int64_t free_cnt_;
};

class ObHaGtsAlloc {
public:
  ObHaGts* alloc_value()
  {
    return NULL;
  }
  void free_value(ObHaGts* gts)
  {
    if (NULL != gts) {
      gts->destroy();
      ObHaGtsFactory::free(gts);
      gts = NULL;
    }
  }
  HaGtsHashNode* alloc_node(ObHaGts* gts)
  {
    UNUSED(gts);
    return op_reclaim_alloc(HaGtsHashNode);
  }
  void free_node(HaGtsHashNode* node)
  {
    if (NULL != node) {
      op_reclaim_free(node);
      node = NULL;
    }
  }
};
}  // namespace gts
}  // namespace oceanbase

#endif  // OCEANBASE_GTS_OB_HA_GTS_H_
