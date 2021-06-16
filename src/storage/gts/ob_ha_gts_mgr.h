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

#ifndef OCEANBASE_GTS_OB_HA_GTS_MGR_H_
#define OCEANBASE_GTS_OB_HA_GTS_MGR_H_

#include "common/ob_member_list.h"
#include "lib/task/ob_timer.h"
#include "share/ob_gts_info.h"
#include "share/ob_gts_table_operator.h"
#include "ob_ha_gts.h"
#include "ob_ha_gts_define.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}
namespace common {
class ObMySQLProxy;
}
namespace gts {
class ObHaGtsManager {
public:
  ObHaGtsManager();
  ~ObHaGtsManager();

public:
  int init(obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* sql_proxy, const common::ObAddr& self_addr);
  void reset();
  void destroy();
  int start();
  void stop();
  void wait();

public:
  int handle_ping_request(const obrpc::ObHaGtsPingRequest& request, obrpc::ObHaGtsPingResponse& response);
  int handle_ping_response(const obrpc::ObHaGtsPingResponse& response);
  int handle_get_request(const obrpc::ObHaGtsGetRequest& request);
  int handle_heartbeat(const obrpc::ObHaGtsHeartbeat& heartbeat);
  int handle_update_meta(const obrpc::ObHaGtsUpdateMetaRequest& request, obrpc::ObHaGtsUpdateMetaResponse& response);
  int handle_change_member(
      const obrpc::ObHaGtsChangeMemberRequest& request, obrpc::ObHaGtsChangeMemberResponse& response);
  void remove_stale_req();
  void load_all_gts();
  void execute_heartbeat();
  void check_member_status();
  int execute_auto_change_member(const uint64_t gts_id, const bool miss_replica, const common::ObAddr& offline_replica,
      const int64_t epoch_id, const common::ObMemberList& member_list);

private:
  class RemoveStaleReqTask : public common::ObTimerTask {
  public:
    RemoveStaleReqTask() : ha_gts_mgr_(NULL)
    {}
    ~RemoveStaleReqTask()
    {}

  public:
    int init(ObHaGtsManager* ha_gts_mgr);
    virtual void runTimerTask();

  private:
    ObHaGtsManager* ha_gts_mgr_;
  };

  class LoadAllGtsTask : public common::ObTimerTask {
  public:
    LoadAllGtsTask() : ha_gts_mgr_(NULL)
    {}
    ~LoadAllGtsTask()
    {}

  public:
    int init(ObHaGtsManager* ha_gts_mgr);
    virtual void runTimerTask();

  private:
    ObHaGtsManager* ha_gts_mgr_;
  };

  class HeartbeatTask : public common::ObTimerTask {
  public:
    HeartbeatTask() : ha_gts_mgr_(NULL)
    {}
    ~HeartbeatTask()
    {}

  public:
    int init(ObHaGtsManager* ha_gts_mgr);
    virtual void runTimerTask();

  private:
    ObHaGtsManager* ha_gts_mgr_;
  };

  class CheckMemberStatusTask : public common::ObTimerTask {
  public:
    CheckMemberStatusTask() : ha_gts_mgr_(NULL)
    {}
    ~CheckMemberStatusTask()
    {}

  public:
    int init(ObHaGtsManager* ha_gts_mgr);
    virtual void runTimerTask();

  private:
    ObHaGtsManager* ha_gts_mgr_;
  };

  class RemoveStaleReqFunctor {
  public:
    RemoveStaleReqFunctor()
    {}
    ~RemoveStaleReqFunctor()
    {}

  public:
    bool operator()(const ObGtsID& gts_id, ObHaGts*& ha_gts)
    {
      UNUSED(gts_id);
      ha_gts->remove_stale_req();
      return true;
    }
  };
  class GCFunctor;
  class HeartbeatFunctor;
  class CheckMemberStatusFunctor;
  typedef common::ObLinkHashMap<ObGtsID, ObHaGts, ObHaGtsAlloc> GtsMap;
  typedef common::ObSEArray<common::ObGtsInfo, 4> ObGtsInfoArray;
  const static int64_t REMOVE_STALE_REQ_TASK_INTERVAL = 5 * 1000 * 1000;  // 5s
  const static int64_t LOAD_ALL_GTS_TASK_INTERVAL = 1 * 1000 * 1000;      // 1s
  const static int64_t HEARTBEAT_INTERVAL = 10 * 1000;                    // 10ms
  const static int64_t CHECK_MEMBER_STATUS_INTERVAL = 100 * 1000;         // 100ms
private:
  int create_gts_(const uint64_t gts_id, const int64_t epoch_id, const common::ObMemberList& member_list,
      const int64_t min_start_timestamp);
  int remove_gts_(const uint64_t gts_id);
  int get_gts_(const uint64_t gts_id, ObHaGts*& gts);
  int revert_gts_(ObHaGts* gts);
  int get_gts_info_array_(ObGtsInfoArray& gts_info_array);
  int handle_gts_info_array_(const ObGtsInfoArray& gts_info_array);
  // replace offline_replica to standby
  int execute_change_member_(const uint64_t gts_id, const common::ObAddr& offline_replica);
  int execute_auto_remove_member_(const uint64_t gts_id, const common::ObAddr& offline_replica, int64_t& epoch_id,
      common::ObMemberList& member_list, common::ObAddr& standby);
  int execute_auto_add_member_(const uint64_t gts_id, const int64_t epoch_id, const common::ObMemberList& member_list,
      const common::ObAddr& standby);
  int notify_gts_replica_meta_(const uint64_t gts_id, const int64_t epoch_id, const common::ObMemberList& member_list,
      const common::ObAddr& server);
  int get_gts_replica_(const common::ObMemberList& member_list, common::ObAddr& server) const;
  int send_update_meta_msg_(const uint64_t gts_id, const int64_t epoch_id, const common::ObMemberList& member_list,
      const int64_t local_ts, const common::ObAddr& server, int64_t& replica_local_ts);

private:
  bool is_inited_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  share::ObGtsTableOperator gts_table_operator_;
  common::ObAddr self_addr_;
  GtsMap gts_map_;
  RemoveStaleReqTask remove_stale_req_task_;
  LoadAllGtsTask load_all_gts_task_;
  CheckMemberStatusTask check_member_status_task_;
  HeartbeatTask heartbeat_task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHaGtsManager);
};
}  // namespace gts
}  // namespace oceanbase

#endif  // OCEANBASE_GTS_OB_HA_GTS_MGR_H_
