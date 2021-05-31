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

#include "ob_external_leader_heartbeat_handler.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_group.h"
#include "ob_log_define.h"
#include "ob_partition_log_service.h"
#include "ob_log_reader_interface.h"
#include "ob_i_log_engine.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace clog;
using namespace storage;
namespace logservice {

int ObExtLeaderHeartbeatHandler::init(ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == partition_service)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_service_ = partition_service;
  }
  return ret;
}

int ObExtLeaderHeartbeatHandler::get_leader_info(
    const ObPartitionKey& pkey, ObIPartitionLogService* pls, ObRole& role, int64_t& leader_epoch)
{
  UNUSED(pkey);
  int ret = OB_SUCCESS;
  // ObPartitionLogService::get_leader_info also check election
  ret = pls->get_role_and_leader_epoch(role, leader_epoch);
  // in follower, leader_epoch can be invalid
  if (OB_UNLIKELY(INVALID_ROLE == role) || OB_UNLIKELY(LEADER == role && OB_INVALID_TIMESTAMP == leader_epoch)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "get invalid role info", K(ret), K(pkey), K(role), K(leader_epoch));
  }
  return ret;
}

int ObExtLeaderHeartbeatHandler::get_last_slide(
    const ObPartitionKey& pkey, ObIPartitionLogService* pls, uint64_t& last_slide_log_id, int64_t& last_slide_ts)
{
  UNUSED(pkey);
  int ret = OB_SUCCESS;
  uint64_t start_log_id_dummy = OB_INVALID_ID;
  int64_t start_ts_dummy = OB_INVALID_TIMESTAMP;
  uint64_t max_log_id = OB_INVALID_ID;
  int64_t max_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(pls->get_log_id_range(start_log_id_dummy, start_ts_dummy, max_log_id, max_ts))) {
    CLOG_LOG(WARN,
        "get max log id error",
        K(ret),
        K(pkey),
        K(start_log_id_dummy),
        K(start_ts_dummy),
        K(max_log_id),
        K(max_ts));
  } else {
    last_slide_log_id = max_log_id;
    last_slide_ts = max_ts;
  }
  return ret;
}

int ObExtLeaderHeartbeatHandler::get_heartbeat_on_leader(const ObPartitionKey& pkey, ObIPartitionLogService* pls,
    const int64_t leader_epoch, uint64_t& next_served_log_id, int64_t& next_served_ts)
{
  UNUSED(pkey);
  int ret = OB_SUCCESS;
  ObRole role2 = INVALID_ROLE;
  int64_t leader_epoch2 = OB_INVALID_TIMESTAMP;
  if (OB_ISNULL(pls)) {
    EXTLOG_LOG(WARN, "invalid argument", K(pls));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(pls->get_next_served_log_info_for_leader(next_served_log_id, next_served_ts))) {
    EXTLOG_LOG(WARN, "get_next_served_log_info_for_leader from partition log service fail", K(ret), K(pkey));
  } else if (OB_FAIL(get_leader_info(pkey, pls, role2, leader_epoch2))) {
    EXTLOG_LOG(WARN, "get leader info error", K(ret), K(role2), K(leader_epoch2));
  } else if (LEADER == role2 && leader_epoch2 == leader_epoch) {
    // leader double check success
  } else {
    // double check fail
    ret = OB_NOT_MASTER;
  }

  EXTLOG_LOG(TRACE,
      "get heartbeat on leader",
      K(ret),
      K(pkey),
      K(next_served_log_id),
      K(next_served_ts),
      K(role2),
      K(leader_epoch),
      K(leader_epoch2));
  return ret;
}

// expect return OB_NOT_MASTER
int ObExtLeaderHeartbeatHandler::get_heartbeat_on_follower(
    const ObPartitionKey& pkey, ObIPartitionLogService* pls, uint64_t& next_served_log_id, int64_t& next_served_ts)
{
  int ret = OB_SUCCESS;
  uint64_t last_slide_log_id = OB_INVALID_ID;
  int64_t last_slide_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(get_last_slide(pkey, pls, last_slide_log_id, last_slide_ts))) {
    EXTLOG_LOG(WARN, "get last slide info error", K(ret), K(pkey));
  } else {
    // on Follower, return max_slide_log_id and max_slide_ts
    next_served_log_id = last_slide_log_id + 1;
    next_served_ts = last_slide_ts + 1;
    ret = OB_NOT_MASTER;
  }

  EXTLOG_LOG(TRACE,
      "get heartbeat on follower",
      K(ret),
      K(pkey),
      K(next_served_log_id),
      K(next_served_ts),
      K(last_slide_log_id));
  return ret;
}

int ObExtLeaderHeartbeatHandler::get_heartbeat_on_partition(
    const ObPartitionKey& pkey, uint64_t& next_served_log_id, int64_t& next_served_ts)
{
  int ret = OB_SUCCESS;
  ObRole role = INVALID_ROLE;
  int64_t leader_epoch = OB_INVALID_TIMESTAMP;
  ObIPartitionLogService* pls = NULL;
  ObIPartitionGroupGuard guard;
  if (OB_SUCCESS != partition_service_->get_partition(pkey, guard) ||
      OB_UNLIKELY(NULL == guard.get_partition_group()) || OB_UNLIKELY(!(guard.get_partition_group()->is_valid())) ||
      OB_UNLIKELY(NULL == (pls = guard.get_partition_group()->get_log_service()))) {
    // this partition has been removed before
    ret = OB_PARTITION_NOT_EXIST;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "get null pls when get_heartbeat_on_partition", K(ret), K(pkey));
    }
  } else if (OB_FAIL(get_leader_info(pkey, pls, role, leader_epoch))) {
    EXTLOG_LOG(WARN, "get_leader_info error", K(ret), K(pkey), K(role), K(leader_epoch));
  } else if (LEADER == role) {
    if (OB_FAIL(get_heartbeat_on_leader(pkey, pls, leader_epoch, next_served_log_id, next_served_ts))) {
      if (OB_NOT_MASTER == ret) {
        // No leader in the process of requesting the leader, try requesting follower process
        ret = OB_SUCCESS;
        if (OB_FAIL(get_heartbeat_on_follower(pkey, pls, next_served_log_id, next_served_ts))) {
          if (OB_NOT_MASTER == ret) {
            // expected
          } else {
            EXTLOG_LOG(
                WARN, "get heartbeat on follower error", K(ret), K(pkey), K(next_served_log_id), K(next_served_ts));
          }
        }
      } else {
        EXTLOG_LOG(WARN, "get heartbeat on leader error", K(ret), K(pkey), K(next_served_log_id), K(next_served_ts));
      }
    }
  } else if (FOLLOWER == role) {
    if (OB_FAIL(get_heartbeat_on_follower(pkey, pls, next_served_log_id, next_served_ts))) {
      if (OB_NOT_MASTER == ret) {
        // expected
      } else {
        EXTLOG_LOG(WARN, "get heartbeat on follower error", K(ret), K(pkey), K(next_served_log_id), K(next_served_ts));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "unknown role", K(ret), K(pkey), K(role), K(leader_epoch));
  }
  return ret;
}

int ObExtLeaderHeartbeatHandler::do_req_heartbeat_info(
    const ObLogLeaderHeartbeatReq& req_msg, ObLogLeaderHeartbeatResp& resp_msg)
{
  int ret = OB_SUCCESS;
  int pkey_ret = OB_SUCCESS;
  const ObLogLeaderHeartbeatReq::ParamArray& params = req_msg.get_params();
  const int64_t partition_count = params.count();
  ObLogLeaderHeartbeatResp::Result result_item;
  EVENT_ADD(CLOG_EXTLOG_HEARTBEAT_PARTITION_COUNT, partition_count);

  for (int64_t i = 0; OB_SUCC(ret) && i < partition_count; i++) {
    uint64_t next_served_log_id = OB_INVALID_ID;
    int64_t next_served_ts = OB_INVALID_TIMESTAMP;
    const ObPartitionKey& pkey = params[i].pkey_;

    pkey_ret = get_heartbeat_on_partition(pkey, next_served_log_id, next_served_ts);

    result_item.reset();
    if (OB_SUCCESS == pkey_ret || OB_NOT_MASTER == pkey_ret || OB_PARTITION_NOT_EXIST == pkey_ret) {
      result_item.err_ = pkey_ret;
      result_item.next_served_log_id_ = next_served_log_id;
      result_item.next_served_ts_ = next_served_ts;
      if (OB_FAIL(resp_msg.append_result(result_item))) {
        EXTLOG_LOG(WARN, "resp_msg append result item error", K(ret), K(i), "param", params[i], K(result_item));
      }
    } else {
      ret = pkey_ret;
      EXTLOG_LOG(WARN, "get heartbeat on partition error", K(ret), K(i), "param", params[i]);
    }
  }  // end for
  EXTLOG_LOG(TRACE, "do req heartbeat info", K(ret), K(req_msg), K(resp_msg));
  return ret;
}

int ObExtLeaderHeartbeatHandler::leader_heartbeat(
    const ObLogLeaderHeartbeatReq& req_msg, ObLogLeaderHeartbeatResp& resp_msg)
{
  int ret = OB_SUCCESS;
  if (!req_msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "ObLogLeaderHeartbeatReq is invalid", K(ret), K(req_msg));
  } else if (OB_FAIL(do_req_heartbeat_info(req_msg, resp_msg))) {
    EXTLOG_LOG(WARN, "do_req_heartbeat_info error", K(req_msg), K(resp_msg));
  }
  resp_msg.set_debug_err(ret);
  if (OB_FAIL(ret)) {  // rewrite ret
    ret = OB_ERR_SYS;
  }
  resp_msg.set_err(ret);
  return ret;
}

}  // namespace logservice
}  // namespace oceanbase
