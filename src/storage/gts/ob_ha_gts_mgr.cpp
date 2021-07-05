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

#include "ob_ha_gts_mgr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/net/ob_addr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace gts {
int ObHaGtsManager::RemoveStaleReqTask::init(ObHaGtsManager* ha_gts_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ha_gts_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret));
  } else {
    ha_gts_mgr_ = ha_gts_mgr;
  }
  return ret;
}

void ObHaGtsManager::RemoveStaleReqTask::runTimerTask()
{
  if (OB_ISNULL(ha_gts_mgr_)) {
    STORAGE_LOG(ERROR, "RemoveStaleReqTask is not inited");
  } else {
    ha_gts_mgr_->remove_stale_req();
  }
}

int ObHaGtsManager::LoadAllGtsTask::init(ObHaGtsManager* ha_gts_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ha_gts_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret));
  } else {
    ha_gts_mgr_ = ha_gts_mgr;
  }
  return ret;
}

void ObHaGtsManager::LoadAllGtsTask::runTimerTask()
{
  if (OB_ISNULL(ha_gts_mgr_)) {
    STORAGE_LOG(ERROR, "LoadAllGtsTask is not inited");
  } else {
    ha_gts_mgr_->load_all_gts();
  }
}

int ObHaGtsManager::HeartbeatTask::init(ObHaGtsManager* ha_gts_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ha_gts_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret));
  } else {
    ha_gts_mgr_ = ha_gts_mgr;
  }
  return ret;
}

void ObHaGtsManager::HeartbeatTask::runTimerTask()
{
  if (OB_ISNULL(ha_gts_mgr_)) {
    STORAGE_LOG(ERROR, "HeartbeatTask is not inited");
  } else {
    ha_gts_mgr_->execute_heartbeat();
  }
}

int ObHaGtsManager::CheckMemberStatusTask::init(ObHaGtsManager* ha_gts_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ha_gts_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret));
  } else {
    ha_gts_mgr_ = ha_gts_mgr;
  }
  return ret;
}

void ObHaGtsManager::CheckMemberStatusTask::runTimerTask()
{
  if (OB_ISNULL(ha_gts_mgr_)) {
    STORAGE_LOG(ERROR, "CheckMemberStatusTask is not inited");
  } else {
    ha_gts_mgr_->check_member_status();
  }
}

class ObHaGtsManager::GCFunctor {
public:
  GCFunctor(const ObGtsInfoArray& gts_info_array) : gts_info_array_(gts_info_array)
  {}
  ~GCFunctor()
  {}

public:
  bool operator()(const ObGtsID& gts_id, ObHaGts*& ha_gts)
  {
    UNUSED(ha_gts);
    const uint64_t gts = gts_id.get_value();
    return !is_gts_exist_(gts);
  }

private:
  bool is_gts_exist_(const uint64_t gts_id) const
  {
    bool bool_ret = false;
    for (int64_t i = 0; !bool_ret && i < gts_info_array_.count(); i++) {
      bool_ret = (gts_info_array_[i].gts_id_ == gts_id);
    }
    return bool_ret;
  }
  const ObGtsInfoArray& gts_info_array_;
};

class ObHaGtsManager::HeartbeatFunctor {
public:
  HeartbeatFunctor()
  {}
  ~HeartbeatFunctor()
  {}

public:
  bool operator()(const ObGtsID& gts_id, ObHaGts*& ha_gts)
  {
    UNUSED(gts_id);
    ha_gts->send_heartbeat();
    return true;
  }
};

class ObHaGtsManager::CheckMemberStatusFunctor {
public:
  CheckMemberStatusFunctor(ObHaGtsManager* ha_gts_mgr) : host_(ha_gts_mgr)
  {}
  ~CheckMemberStatusFunctor()
  {}

public:
  bool operator()(const ObGtsID& gts_id, ObHaGts*& ha_gts)
  {
    int ret = OB_SUCCESS;
    bool need_change_member = false;
    bool miss_replica = false;
    common::ObAddr offline_replica;
    int64_t epoch_id = OB_INVALID_TIMESTAMP;
    common::ObMemberList member_list;
    if (OB_FAIL(
            ha_gts->check_member_status(need_change_member, miss_replica, offline_replica, epoch_id, member_list))) {
      STORAGE_LOG(WARN, "check_member_status failed", K(ret), K(gts_id));
    } else if (need_change_member) {
      if (miss_replica == false && !offline_replica.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "CheckMemberStatusFunctor unexpected return value", K(miss_replica), K(offline_replica));
      } else if (OB_FAIL(host_->execute_auto_change_member(
                     gts_id.get_value(), miss_replica, offline_replica, epoch_id, member_list))) {
        STORAGE_LOG(WARN, "execute_auto_change_member failed", K(ret));
      }
    } else {
      // do nothing
    }
    return true;
  }

private:
  ObHaGtsManager* host_;
};

ObHaGtsManager::ObHaGtsManager()
{
  reset();
}

ObHaGtsManager::~ObHaGtsManager()
{
  destroy();
}

int ObHaGtsManager::init(
    obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* sql_proxy, const common::ObAddr& self_addr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "ObHaGtsManager is inited", K(ret));
  } else if (OB_ISNULL(rpc_proxy) || OB_ISNULL(sql_proxy) || !self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), KP(rpc_proxy), KP(sql_proxy), K(self_addr));
  } else if (OB_FAIL(gts_table_operator_.init(sql_proxy))) {
    STORAGE_LOG(ERROR, "gts_table_operator_ init failed", K(ret));
  } else if (OB_FAIL(gts_map_.init(ObModIds::OB_HASH_BUCKET_HA_GTS))) {
    STORAGE_LOG(WARN, "gts_map_ init failed", K(ret));
  } else if (OB_FAIL(remove_stale_req_task_.init(this))) {
    STORAGE_LOG(ERROR, "remove_stale_req_task_ init failed", K(ret));
  } else if (OB_FAIL(load_all_gts_task_.init(this))) {
    STORAGE_LOG(ERROR, "load_all_gts_task init failed", K(ret));
  } else if (OB_FAIL(check_member_status_task_.init(this))) {
    STORAGE_LOG(ERROR, "check_member_status_task_ init failed", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::HAGtsMgr))) {
    STORAGE_LOG(ERROR, "timer_ init failed", K(ret));
  } else if (OB_FAIL(heartbeat_task_.init(this))) {
    STORAGE_LOG(ERROR, "heartbeat_task_ init failed", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::HAGtsHB))) {
    STORAGE_LOG(ERROR, "heartbeat_timer_ init failed", K(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    self_addr_ = self_addr;
    is_inited_ = true;
  }

  if (!is_inited_) {
    destroy();
  }
  STORAGE_LOG(INFO, "ObHaGtsManager is inited", K(ret));
  return ret;
}

void ObHaGtsManager::reset()
{
  is_inited_ = false;
  rpc_proxy_ = NULL;
  self_addr_.reset();
  gts_map_.reset();
  STORAGE_LOG(INFO, "ObHaGtsManager reset");
}

void ObHaGtsManager::destroy()
{
  is_inited_ = false;
  rpc_proxy_ = NULL;
  self_addr_.reset();
  gts_map_.destroy();
  // remove_stale_req_task_ no need call destroy()
  // load_all_gts_task_ no need call destroy()
  // check_member_status_task_ no need call destroy()
  TG_DESTROY(lib::TGDefIDs::HAGtsMgr);
  TG_DESTROY(lib::TGDefIDs::HAGtsHB);
  // heartbeat_task_ no need call destroy()
  STORAGE_LOG(INFO, "ObHaGtsManager destroy");
}

int ObHaGtsManager::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::HAGtsMgr, remove_stale_req_task_, REMOVE_STALE_REQ_TASK_INTERVAL, repeat))) {
    STORAGE_LOG(WARN, "fail to schedule remove_stale_req_task_", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::HAGtsMgr, load_all_gts_task_, LOAD_ALL_GTS_TASK_INTERVAL, repeat))) {
    STORAGE_LOG(WARN, "fail to schedule load_all_gts_task_", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(
                 lib::TGDefIDs::HAGtsMgr, check_member_status_task_, CHECK_MEMBER_STATUS_INTERVAL, repeat))) {
    STORAGE_LOG(WARN, "fail to schedule check_member_status_task_", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::HAGtsHB, heartbeat_task_, HEARTBEAT_INTERVAL, repeat))) {
    STORAGE_LOG(WARN, "fail to schedule heartbeat_task_", K(ret));
  }
  STORAGE_LOG(INFO, "ObHaGtsManager start finished", K(ret));
  return ret;
}

void ObHaGtsManager::stop()
{
  TG_STOP(lib::TGDefIDs::HAGtsMgr);
  TG_STOP(lib::TGDefIDs::HAGtsHB);
  STORAGE_LOG(INFO, "ObHaGtsManager stop finished");
}

void ObHaGtsManager::wait()
{
  TG_WAIT(lib::TGDefIDs::HAGtsMgr);
  TG_WAIT(lib::TGDefIDs::HAGtsHB);
  STORAGE_LOG(INFO, "ObHaGtsManager wait finished");
}

int ObHaGtsManager::create_gts_(const uint64_t gts_id, const int64_t epoch_id, const common::ObMemberList& member_list,
    const int64_t min_start_timestamp)
{
  int ret = OB_SUCCESS;
  ObHaGts* tmp_gts = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret), K(gts_id));
    // HA GTS support 3 replicas at most
  } else if (!is_valid_gts_id(gts_id) || epoch_id <= 0 || member_list.get_member_number() > 3 ||
             member_list.get_member_number() <= 0 || min_start_timestamp <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(gts_id), K(epoch_id), K(member_list), K(min_start_timestamp));
  } else if (OB_SUCCESS == (ret = get_gts_(gts_id, tmp_gts))) {
    revert_gts_(tmp_gts);
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "when create_gts, gts has already existed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    revert_gts_(tmp_gts);
    STORAGE_LOG(ERROR, "get gts failed", K(ret), K(gts_id));
  } else {
    ret = OB_SUCCESS;
    ObGtsID gts_id_wrapper(gts_id);
    if (NULL == (tmp_gts = ObHaGtsFactory::alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc gts failed", K(ret), K(gts_id));
    } else if (OB_FAIL(
                   tmp_gts->init(gts_id, epoch_id, member_list, min_start_timestamp, rpc_proxy_, this, self_addr_))) {
      STORAGE_LOG(WARN, "gts init failed", K(ret), K(gts_id));
    } else if (OB_FAIL(gts_map_.insert_and_get(gts_id_wrapper, tmp_gts))) {
      STORAGE_LOG(WARN, "insert_and_get gts failed", K(ret), K(gts_id));
    } else {
      // do nothing
    }

    (void)revert_gts_(tmp_gts);
    if (OB_SUCCESS != ret && NULL != tmp_gts) {
      tmp_gts->destroy();
      ObHaGtsFactory::free(tmp_gts);
      tmp_gts = NULL;
    }
  }
  return ret;
}

int ObHaGtsManager::remove_gts_(const uint64_t gts_id)
{
  int ret = OB_SUCCESS;
  ObGtsID gts_id_wrapper(gts_id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else if (!is_valid_gts_id(gts_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(gts_id));
  } else if (OB_FAIL(gts_map_.del(gts_id_wrapper))) {
    STORAGE_LOG(ERROR, "del gts failed", K(ret), K(gts_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObHaGtsManager::handle_ping_request(const obrpc::ObHaGtsPingRequest& request, obrpc::ObHaGtsPingResponse& response)
{
  int ret = OB_SUCCESS;
  const uint64_t gts_id = request.get_gts_id();
  ObHaGts* gts = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(request));
  } else if (OB_FAIL(get_gts_(gts_id, gts))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id), K(request));
      }
    } else {
      STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id), K(request));
    }
  } else if (OB_FAIL(gts->handle_ping_request(request, response))) {
    STORAGE_LOG(WARN, "handle_ping_request failed", K(ret), K(request));
  } else {
    // do nothing
  }

  (void)revert_gts_(gts);
  return ret;
}

int ObHaGtsManager::handle_ping_response(const obrpc::ObHaGtsPingResponse& response)
{
  int ret = OB_SUCCESS;
  const uint64_t gts_id = response.get_gts_id();
  ObHaGts* gts = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else if (!response.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(response));
  } else if (OB_FAIL(get_gts_(gts_id, gts))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id), K(response));
      }
    } else {
      STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id), K(response));
    }
  } else if (OB_FAIL(gts->handle_ping_response(response))) {
    STORAGE_LOG(WARN, "handle_ping_response failed", K(ret), K(response));
  } else {
    // do nothing
  }

  (void)revert_gts_(gts);
  return ret;
}

int ObHaGtsManager::handle_get_request(const obrpc::ObHaGtsGetRequest& request)
{
  int ret = OB_SUCCESS;
  const uint64_t gts_id = request.get_gts_id();
  ObHaGts* gts = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(request));
  } else if (OB_FAIL(get_gts_(gts_id, gts))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id), K(request));
      }
    } else {
      STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id), K(request));
    }
  } else if (OB_FAIL(gts->handle_get_request(request))) {
    STORAGE_LOG(WARN, "handle_get_request failed", K(ret), K(request));
  } else {
    // do nothing
  }
  (void)revert_gts_(gts);
  STORAGE_LOG(TRACE, "handle_get_request", K(ret), K(request));
  return ret;
}

int ObHaGtsManager::handle_heartbeat(const obrpc::ObHaGtsHeartbeat& heartbeat)
{
  int ret = OB_SUCCESS;
  const uint64_t gts_id = heartbeat.get_gts_id();
  ObHaGts* gts = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else if (!heartbeat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(heartbeat));
  } else if (OB_FAIL(get_gts_(gts_id, gts))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id), K(heartbeat));
      }
    } else {
      STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id), K(heartbeat));
    }
  } else if (OB_FAIL(gts->handle_heartbeat(heartbeat))) {
    STORAGE_LOG(WARN, "handle_heartbeat failed", K(ret), K(gts_id), K(heartbeat));
  } else {
    // do nothing
  }
  (void)revert_gts_(gts);
  return ret;
}

int ObHaGtsManager::handle_update_meta(
    const obrpc::ObHaGtsUpdateMetaRequest& request, obrpc::ObHaGtsUpdateMetaResponse& response)
{
  int ret = OB_SUCCESS;
  const uint64_t gts_id = request.get_gts_id();
  const int64_t epoch_id = request.get_epoch_id();
  const common::ObMemberList& member_list = request.get_member_list();
  const int64_t local_ts = request.get_local_ts();
  int64_t replica_local_ts = OB_INVALID_TIMESTAMP;
  ObHaGts* gts = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(request));
  } else if (OB_FAIL(get_gts_(gts_id, gts))) {
    STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id), K(request));
  } else if (OB_FAIL(gts->try_update_meta(epoch_id, member_list))) {
    STORAGE_LOG(WARN, "try_update_meta failed", K(ret), K(gts_id), K(request));
  } else if (OB_FAIL(gts->inc_update_local_ts(local_ts))) {
    STORAGE_LOG(WARN, "inc_update_local_ts failed", K(ret), K(gts_id), K(request));
  } else if (OB_FAIL(gts->get_local_ts(replica_local_ts))) {
    STORAGE_LOG(WARN, "get_local_ts failed", K(ret), K(gts_id), K(request));
  } else {
    response.set(replica_local_ts);
  }

  return ret;
}

int ObHaGtsManager::handle_change_member(
    const obrpc::ObHaGtsChangeMemberRequest& request, obrpc::ObHaGtsChangeMemberResponse& response)
{
  UNUSED(response);
  int ret = OB_SUCCESS;
  const uint64_t gts_id = request.get_gts_id();
  const common::ObAddr& offline_replica = request.get_offline_replica();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else if (!request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(request));
  } else if (OB_FAIL(execute_change_member_(gts_id, offline_replica))) {
    STORAGE_LOG(WARN, "execute_change_member_ failed", K(ret), K(gts_id), K(offline_replica));
  }
  return ret;
}

void ObHaGtsManager::remove_stale_req()
{
  int ret = OB_SUCCESS;
  RemoveStaleReqFunctor functor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else if (OB_FAIL(gts_map_.for_each(functor))) {
    STORAGE_LOG(WARN, "gts_map_ for_each failed", K(ret));
  }
}

void ObHaGtsManager::load_all_gts()
{
  int ret = OB_SUCCESS;
  ObGtsInfoArray gts_info_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else if (OB_FAIL(get_gts_info_array_(gts_info_array))) {
    STORAGE_LOG(WARN, "get_gts_info_array_ failed", K(ret));
  } else if (OB_FAIL(handle_gts_info_array_(gts_info_array))) {
    STORAGE_LOG(WARN, "handle_gts_info_array_ failed", K(ret));
  }
  return;
}

void ObHaGtsManager::execute_heartbeat()
{
  int ret = OB_SUCCESS;
  HeartbeatFunctor functor;
  if (OB_FAIL(gts_map_.for_each(functor))) {
    STORAGE_LOG(WARN, "gts_map_ failed", K(ret));
  }
}

void ObHaGtsManager::check_member_status()
{
  int ret = OB_SUCCESS;
  CheckMemberStatusFunctor functor(this);
  if (OB_FAIL(gts_map_.for_each(functor))) {
    STORAGE_LOG(WARN, "gts_map_ failed", K(ret));
  }
}

int ObHaGtsManager::execute_auto_change_member(const uint64_t gts_id, const bool miss_replica,
    const common::ObAddr& offline_replica, const int64_t epoch_id, const common::ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  ObAddr standby;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret), K(gts_id));
  } else if (!is_valid_gts_id(gts_id) || (miss_replica == false && !offline_replica.is_valid()) || epoch_id <= 0 ||
             member_list.get_member_number() > 3 || member_list.get_member_number() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR,
        "invalid arguments",
        K(ret),
        K(gts_id),
        K(miss_replica),
        K(offline_replica),
        K(epoch_id),
        K(member_list));
  } else if (offline_replica.is_valid()) {
    int64_t curr_epoch_id = epoch_id;
    common::ObMemberList curr_member_list = member_list;
    if (OB_FAIL(execute_auto_remove_member_(gts_id, offline_replica, curr_epoch_id, curr_member_list, standby))) {
      STORAGE_LOG(WARN,
          "execute_auto_remove_member_ failed",
          K(ret),
          K(gts_id),
          K(offline_replica),
          K(curr_epoch_id),
          K(curr_member_list));
    } else if (OB_FAIL(execute_auto_add_member_(gts_id, curr_epoch_id, curr_member_list, standby))) {
      STORAGE_LOG(WARN,
          "execute_auto_add_member_ failed",
          K(ret),
          K(gts_id),
          K(curr_epoch_id),
          K(curr_member_list),
          K(standby));
    }
  } else if (miss_replica) {
    if (OB_FAIL(execute_auto_add_member_(gts_id, epoch_id, member_list, standby))) {
      STORAGE_LOG(WARN, "execute_auto_add_member_ failed", K(ret), K(gts_id), K(epoch_id), K(member_list));
    }
  }
  STORAGE_LOG(INFO,
      "execute_auto_change_member finished",
      K(ret),
      K(gts_id),
      K(miss_replica),
      K(offline_replica),
      K(epoch_id),
      K(member_list));
  return ret;
}

int ObHaGtsManager::get_gts_(const uint64_t gts_id, ObHaGts*& gts)
{
  int ret = OB_SUCCESS;

  ObHaGts* tmp_gts = NULL;
  ObGtsID gts_id_wrapper(gts_id);
  if (OB_FAIL(gts_map_.get(gts_id_wrapper, tmp_gts))) {
    STORAGE_LOG(TRACE, "gts_map_ get failed", K(ret), K(gts_id));
  } else if (OB_ISNULL(tmp_gts)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tmp_gts is NULL", K(ret));
  } else {
    gts = tmp_gts;
  }

  return ret;
}

int ObHaGtsManager::revert_gts_(ObHaGts* gts)
{
  int ret = OB_SUCCESS;
  if (NULL != gts) {
    gts_map_.revert(gts);
  }
  return ret;
}

int ObHaGtsManager::get_gts_info_array_(ObGtsInfoArray& gts_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gts_table_operator_.get_gts_infos(gts_info_array))) {
    STORAGE_LOG(WARN, "get_gts_infos failed", K(ret));
  }
  STORAGE_LOG(INFO, "get_gts_infos", K(ret), K(gts_info_array));
  return ret;
}

int ObHaGtsManager::handle_gts_info_array_(const ObGtsInfoArray& gts_info_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < gts_info_array.count(); i++) {
      const ObGtsInfo& gts_info = gts_info_array[i];
      const uint64_t gts_id = gts_info.gts_id_;
      const uint64_t epoch_id = gts_info.epoch_id_;
      const common::ObMemberList& member_list = gts_info.member_list_;
      const int64_t min_start_timestamp = ObClockGenerator::getCurrentTime();
      ObHaGts* gts = NULL;

      if (!gts_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "gts_info is invalid", K(ret), K(gts_info));
      } else if (member_list.contains(self_addr_)) {
        if (OB_SUCCESS != (tmp_ret = get_gts_(gts_id, gts))) {
          if (OB_ENTRY_NOT_EXIST == tmp_ret) {
            tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = create_gts_(gts_id, epoch_id, member_list, min_start_timestamp))) {
              // create gts failed, wait next timer task to retry
              STORAGE_LOG(WARN,
                  "create_gts_ failed",
                  K(tmp_ret),
                  K(gts_id),
                  K(epoch_id),
                  K(member_list),
                  K(min_start_timestamp));
            }
          } else {
            STORAGE_LOG(WARN, "get_gts_ failed", K(tmp_ret), K(gts_id));
          }
        } else {
          // gts already exists
          if (OB_SUCCESS != (tmp_ret = gts->try_update_meta(epoch_id, member_list))) {
            STORAGE_LOG(WARN, "try_update_meta failed", K(tmp_ret), K(gts_id), K(epoch_id), K(member_list));
          }
        }
        (void)revert_gts_(gts);
      } else {
        // do nothing
      }
    }

    // gts GC
    if (OB_SUCC(ret)) {
      GCFunctor functor(gts_info_array);
      if (OB_FAIL(gts_map_.remove_if(functor))) {
        STORAGE_LOG(WARN, "gts_map_ remove_if failed", K(ret));
      }
    }
  }
  return ret;
}

int ObHaGtsManager::execute_change_member_(const uint64_t gts_id, const common::ObAddr& offline_replica)
{
  int ret = OB_SUCCESS;
  ObHaGts* gts = NULL;
  int64_t epoch_id = OB_INVALID_TIMESTAMP;
  common::ObMemberList member_list;
  const bool miss_replica = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObHaGtsManager is not inited", K(ret), K(gts_id));
  } else if (!is_valid_gts_id(gts_id) || !offline_replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(gts_id), K(offline_replica));
  } else if (OB_FAIL(get_gts_(gts_id, gts))) {
    STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id));
  } else if (OB_FAIL(gts->get_meta(epoch_id, member_list))) {
    STORAGE_LOG(WARN, "gts get_meta failed", K(ret), K(gts_id));
  } else if (OB_FAIL(execute_auto_change_member(gts_id, miss_replica, offline_replica, epoch_id, member_list))) {
    STORAGE_LOG(WARN,
        "execute_auto_change_member failed",
        K(ret),
        K(gts_id),
        K(miss_replica),
        K(offline_replica),
        K(epoch_id),
        K(member_list));
  }

  (void)revert_gts_(gts);
  STORAGE_LOG(
      INFO, "execute_change_member finished", K(ret), K(gts_id), K(offline_replica), K(epoch_id), K(member_list));
  return ret;
}

int ObHaGtsManager::execute_auto_remove_member_(const uint64_t gts_id, const common::ObAddr& offline_replica,
    int64_t& epoch_id, common::ObMemberList& member_list, common::ObAddr& standby)
{
  int ret = OB_SUCCESS;
  if (member_list.get_member_number() != 3) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "member_number is not 3", K(ret), K(gts_id), K(member_list));
  } else if (OB_FAIL(member_list.remove_server(offline_replica))) {
    STORAGE_LOG(WARN, "member_list remove_server failed", K(ret), K(gts_id), K(member_list), K(offline_replica));
  } else if (OB_FAIL(gts_table_operator_.get_gts_standby(gts_id, standby))) {
    STORAGE_LOG(WARN, "get_gts_standby failed", K(ret), K(gts_id));
  } else if (!standby.is_valid()) {
    ret = OB_GTS_STANDBY_IS_INVALID;
    STORAGE_LOG(WARN, "standby is invalid", K(ret), K(gts_id));
  } else {
    const int64_t curr_epoch_id = epoch_id;
    const int64_t new_epoch_id = std::max(curr_epoch_id + 1, ObTimeUtility::current_time());
    if (OB_FAIL(
            gts_table_operator_.update_gts_member_list(gts_id, curr_epoch_id, new_epoch_id, member_list, standby))) {
      STORAGE_LOG(WARN,
          "update_gts_member_list failed",
          K(ret),
          K(gts_id),
          K(curr_epoch_id),
          K(new_epoch_id),
          K(member_list),
          K(standby));
    } else {
      epoch_id = new_epoch_id;
    }
  }
  STORAGE_LOG(INFO,
      "execute_auto_remove_member_ finished",
      K(ret),
      K(gts_id),
      K(offline_replica),
      K(epoch_id),
      K(member_list),
      K(standby));
  return ret;
}

int ObHaGtsManager::execute_auto_add_member_(const uint64_t gts_id, const int64_t epoch_id,
    const common::ObMemberList& member_list, const common::ObAddr& standby)
{
  int ret = OB_SUCCESS;
  common::ObMemberList new_member_list = member_list;
  common::ObAddr curr_standby = standby;

  if (member_list.get_member_number() != 2) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "member_number is not 2", K(ret), K(gts_id), K(member_list));
  } else if (!curr_standby.is_valid()) {
    if (OB_FAIL(gts_table_operator_.get_gts_standby(gts_id, curr_standby))) {
      STORAGE_LOG(WARN, "get_gts_standby failed", K(ret), K(gts_id));
    } else if (!curr_standby.is_valid()) {
      ret = OB_GTS_STANDBY_IS_INVALID;
      STORAGE_LOG(WARN, "curr_standby is invalid", K(ret), K(gts_id));
    }
  }

  if (OB_SUCC(ret)) {
    ObAddr server;
    if (OB_FAIL(get_gts_replica_(member_list, server))) {
      STORAGE_LOG(WARN, "get_gts_replica_ failed", K(ret), K(member_list));
    } else if (OB_FAIL(notify_gts_replica_meta_(gts_id, epoch_id, member_list, server))) {
      STORAGE_LOG(WARN, "notify_gts_replica_meta_ failed", K(ret), K(gts_id), K(epoch_id), K(member_list));
    } else if (OB_FAIL(new_member_list.add_server(curr_standby))) {
      STORAGE_LOG(WARN, "new_member_list add_server failed", K(ret), K(gts_id), K(new_member_list), K(curr_standby));
    } else {
      const int64_t curr_epoch_id = epoch_id;
      const int64_t new_epoch_id = std::max(curr_epoch_id + 1, ObTimeUtility::current_time());
      if (OB_FAIL(gts_table_operator_.update_gts_member_list_and_standby(
              gts_id, curr_epoch_id, new_epoch_id, new_member_list, curr_standby))) {
        STORAGE_LOG(WARN,
            "update_gts_member_list_and_standby failed",
            K(ret),
            K(gts_id),
            K(curr_epoch_id),
            K(new_epoch_id),
            K(new_member_list),
            K(curr_standby));
      } else if (OB_FAIL(notify_gts_replica_meta_(gts_id, new_epoch_id, new_member_list, server))) {
        STORAGE_LOG(WARN, "notify_gts_replica_meta_ failed", K(ret), K(gts_id), K(new_epoch_id), K(new_member_list));
      } else {
        // do nothing
      }
    }
  }
  STORAGE_LOG(INFO, "execute_auto_add_member_ finished", K(ret), K(gts_id), K(epoch_id), K(member_list), K(standby));

  return ret;
}

int ObHaGtsManager::notify_gts_replica_meta_(const uint64_t gts_id, const int64_t epoch_id,
    const common::ObMemberList& member_list, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObHaGts* gts = NULL;
  int64_t local_ts = OB_INVALID_TIMESTAMP;
  int64_t replica_local_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(get_gts_(gts_id, gts))) {
    STORAGE_LOG(WARN, "get_gts_ failed", K(ret), K(gts_id));
  } else if (OB_FAIL(gts->try_update_meta(epoch_id, member_list))) {
    STORAGE_LOG(WARN, "try_update_meta failed", K(ret), K(gts_id), K(epoch_id), K(member_list));
  } else if (OB_FAIL(gts->get_local_ts(local_ts))) {
    STORAGE_LOG(WARN, "get_local_ts failed", K(ret), K(gts_id));
  } else if (OB_FAIL(send_update_meta_msg_(gts_id, epoch_id, member_list, local_ts, server, replica_local_ts))) {
    STORAGE_LOG(WARN, "send_update_meta_msg_ failed", K(ret), K(gts_id));
  } else if (OB_FAIL(gts->inc_update_local_ts(replica_local_ts))) {
    STORAGE_LOG(WARN, "inc_update_local_ts failed", K(ret), K(gts_id));
  }

  (void)revert_gts_(gts);
  STORAGE_LOG(INFO, "notify_gts_replica_meta_ finished", K(ret), K(gts_id), K(epoch_id), K(member_list), K(server));
  return ret;
}

int ObHaGtsManager::get_gts_replica_(const common::ObMemberList& member_list, common::ObAddr& server) const
{
  int ret = OB_SUCCESS;
  common::ObMemberList tmp_member_list = member_list;
  if (tmp_member_list.get_member_number() != 2) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "member_list get_member_number is not 2", K(ret), K(member_list));
  } else if (OB_FAIL(tmp_member_list.remove_server(self_addr_))) {
    STORAGE_LOG(WARN, "member_list remove_server failed", K(ret), K(member_list));
  } else if (OB_FAIL(tmp_member_list.get_server_by_index(0, server))) {
    STORAGE_LOG(WARN, "get_server_by_index failed", K(ret));
  } else {
    // do nothing;
  }
  return ret;
}

int ObHaGtsManager::send_update_meta_msg_(const uint64_t gts_id, const int64_t epoch_id,
    const common::ObMemberList& member_list, const int64_t local_ts, const common::ObAddr& server,
    int64_t& replica_local_ts)
{
  int ret = OB_SUCCESS;
  obrpc::ObHaGtsUpdateMetaRequest request;
  obrpc::ObHaGtsUpdateMetaResponse response;
  request.set(gts_id, epoch_id, member_list, local_ts);
  const int64_t TIMEOUT = 1 * 1000 * 1000;
  if (OB_FAIL(rpc_proxy_->to(server).by(OB_SERVER_TENANT_ID).timeout(TIMEOUT).ha_gts_update_meta(request, response))) {
    STORAGE_LOG(WARN, "ha_gts_update_meta failed", K(ret));
  } else {
    replica_local_ts = response.get_local_ts();
  }
  return ret;
}
}  // namespace gts
}  // namespace oceanbase
