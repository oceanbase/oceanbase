// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_LOGSERVICE_OB_ARBITRATION_SERVICE_H_
#define OCEANBASE_LOGSERVICE_OB_ARBITRATION_SERVICE_H_

#include "share/ob_thread_pool.h"
#include "share/ob_occam_timer.h"
#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "lib/lock/ob_tc_rwlock.h"              // RWLock
#include "common/ob_member_list.h"              // common::ObMemberList
#include "logrpc/ob_log_rpc_req.h"              // ProbeMsg

namespace oceanbase
{
namespace palf
{
class PalfEnv;
class LogMemberAckInfo;
}

namespace obrpc
{
class ObLogServiceRpcProxy;
}

namespace logservice
{
class IObNetKeepAliveAdapter;
enum ServerAliveStatus
{
  UNKNOWN = 0,
  ALIVE,
  DEAD,
};

inline const char *server_status_to_string(ServerAliveStatus status)
{
  #define SERVER_ALIVE_STATUS_TO_STR(x) case(ServerAliveStatus::x): return #x
  switch(status)
  {
    SERVER_ALIVE_STATUS_TO_STR(ALIVE);
    SERVER_ALIVE_STATUS_TO_STR(DEAD);
    default:
      return "UNKNOWN";
  }
  #undef SERVER_ALIVE_STATUS_TO_STR
}

struct ServerProbeCtx
{
  ServerProbeCtx()
      : last_req_ts_(OB_INVALID_TIMESTAMP),
        last_resp_ts_(OB_INVALID_TIMESTAMP),
        alive_status_(UNKNOWN)
  { }

  void reset_probe_ts()
  {
    last_req_ts_ = OB_INVALID_TIMESTAMP;
    last_resp_ts_ = OB_INVALID_TIMESTAMP;
  }

  // ts are valid only in current status
  int64_t last_req_ts_;
  int64_t last_resp_ts_;
  ServerAliveStatus alive_status_;
  TO_STRING_KV(K_(last_req_ts), K_(last_resp_ts), "alive_status", server_status_to_string(alive_status_));
};

class ServerProbeService
{
public:
  ServerProbeService()
      : self_(),
        probing_servers_(),
        rpc_proxy_(NULL),
        timer_(),
        is_running_(false),
        is_inited_(false)
  { }
  ~ServerProbeService() { destroy(); }
  int init(const common::ObAddr &self, obrpc::ObLogServiceRpcProxy *rpc_proxy);
  void destroy();
  int start_probe_server(const common::ObAddr &server);
  int stop_probe_server(const common::ObAddr &server);
  int get_server_alive_status(const common::ObAddr &server, ServerAliveStatus &status) const;
  void run_probe_once();
  int handle_server_probe_msg(const common::ObAddr &sender, const LogServerProbeMsg &req);
private:
  void run_probe_once_();
private:
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  static constexpr int64_t DEAD_SERVER_TIMEOUT = 2 * 1000 * 1000;  // 2s
  typedef common::ObLinearHashMap<common::ObAddr, ServerProbeCtx> ServerProbeMap;
private:
  mutable RWLock lock_;
  common::ObAddr self_;
  ServerProbeMap probing_servers_;
  obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  ObOccamTimer timer_;
  bool is_running_;
  bool is_inited_;
};

class ObArbitrationService : public share::ObThreadPool
{
public:
  ObArbitrationService();
  virtual ~ObArbitrationService();
  int init(const common::ObAddr &self,
           palf::PalfEnv *palf_env,
           obrpc::ObLogServiceRpcProxy *rpc_proxy,
           IObNetKeepAliveAdapter *net_keepalive);
  int start();
  void destroy();
  void run1();
  int handle_server_probe_msg(const common::ObAddr &sender, const LogServerProbeMsg &req);
private:
  static constexpr int64_t MIN_LOOP_INTERVAL_US = 10 * 1000;                        // 10ms
  static constexpr int64_t DEGRADE_ACTION_TIMEOUT_US = 10 * 1000 * 1000L;           // 10s
  static constexpr int64_t UPGRADE_ACTION_TIMEOUT_US = 10 * 1000 * 1000L;           // 10s
  static constexpr int64_t MAX_PALF_COUNT = 200;
  typedef common::ObLinearHashMap<palf::LSKey, palf::LogMemberAckInfoList> PalfAckInfoMap;
private:
  class DoDegradeFunctor
  {
  public:
    explicit DoDegradeFunctor(const common::ObAddr &addr,
                              palf::PalfEnv *palf_env,
                              ServerProbeService *probe_srv,
                              IObNetKeepAliveAdapter *net_keepalive)
        : self_(addr),
          palf_env_(palf_env),
          probe_srv_(probe_srv),
          net_keepalive_(net_keepalive)
    { }
    bool operator()(const palf::LSKey &ls_key, const palf::LogMemberAckInfoList &degrade_servers);

  public:
    common::ObSEArray<int64_t, MAX_PALF_COUNT> need_removed_palfs_;
    bool is_valid()
    {
      return self_.is_valid() && OB_NOT_NULL(palf_env_) &&
          OB_NOT_NULL(probe_srv_) && OB_NOT_NULL(net_keepalive_);
    }
  private:
    bool is_all_other_servers_alive_(
        const common::ObMemberList &all_servers,
        const common::GlobalLearnerList &degraded_list,
        const palf::LogMemberAckInfoList &excepted_servers);
    bool is_allow_degrade_(
        const common::ObMemberList &expected_member_list,
        const int64_t replica_num,
        const common::GlobalLearnerList &degraded_list,
        const palf::LogMemberAckInfoList &may_be_degraded_servers);
    common::ObAddr self_;
    palf::PalfEnv *palf_env_;
    ServerProbeService *probe_srv_;
    IObNetKeepAliveAdapter *net_keepalive_;
  };

  class DoUpgradeFunctor
  {
  public:
    explicit DoUpgradeFunctor(palf::PalfEnv *palf_env, ServerProbeService *probe_srv, IObNetKeepAliveAdapter *net_keepalive)
        : palf_env_(palf_env),
          probe_srv_(probe_srv),
          net_keepalive_(net_keepalive)
    { }
    bool operator()(const palf::LSKey &ls_key, const palf::LogMemberAckInfoList &upgrade_servers);

  public:
    common::ObSEArray<int64_t, MAX_PALF_COUNT> need_removed_palfs_;
  private:
    palf::PalfEnv *palf_env_;
    ServerProbeService *probe_srv_;
    IObNetKeepAliveAdapter *net_keepalive_;
  };

private:
  int start_probe_servers_(const palf::LogMemberAckInfoList &servers);
  int start_probe_servers_(const common::ObMemberList &servers);
  void start_probe_server_(const common::ObAddr &server) const;
  int follower_probe_others_(const int64_t palf_id, const common::ObMemberList &paxos_member_list);
  int get_server_sync_info_();
  void run_loop_();
  void update_arb_timeout_();
  int update_server_map_(PalfAckInfoMap &palf_map,
                         const palf::LSKey &key,
                         const palf::LogMemberAckInfoList &val,
                         bool &is_updated)
  {
    int ret = OB_SUCCESS;
    palf::LogMemberAckInfoList existed_val;
    is_updated = false;
    if (OB_FAIL(palf_map.get(key, existed_val))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(palf_map.insert(key, val))) {
          CLOG_LOG(WARN, "palf_map insert failed", K(key), K(val));
        } else {
          is_updated = true;
        }
      }
    } else if (palf::ack_info_list_addr_equal(existed_val, val)) {
      // pass, do not insert
    } else if (OB_FAIL(palf_map.insert_or_update(key, val))) {
      CLOG_LOG(WARN, "palf_map insert_or_update failed", K(key), K(val));
    } else {
      is_updated = true;
      CLOG_LOG(TRACE, "update_server_map_ success", KR(ret), K(key), K(val), K(is_updated));
    }
    CLOG_LOG(TRACE, "update_server_map_ finish", KR(ret), K(key), K(val), K(is_updated));
    return ret;
  }
private:
  common::ObAddr self_;
  ServerProbeService probe_srv_;
  PalfAckInfoMap may_be_degraded_palfs_;
  PalfAckInfoMap may_be_upgraded_palfs_;
  int64_t arb_timeout_us_;
  int64_t follower_last_probe_time_us_;
  palf::PalfEnv *palf_env_;
  obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  IObNetKeepAliveAdapter *net_keepalive_;
  bool is_inited_;
};

} // logservice
} // oceanbase

#endif
