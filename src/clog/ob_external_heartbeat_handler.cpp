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

#include "ob_external_heartbeat_handler.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/ob_partition_service.h"
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

int ObExtHeartbeatHandler::init(ObPartitionService* partition_service, ObILogEngine* log_engine)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == partition_service) || OB_UNLIKELY(NULL == log_engine)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_service_ = partition_service;
    log_engine_ = log_engine;
  }
  return ret;
}

void ObExtHeartbeatHandler::destroy()
{
  partition_service_ = NULL;
  log_engine_ = NULL;
}

uint64_t ObExtHeartbeatHandler::get_last_slide_log_id(const ObPartitionKey& pkey)
{
  uint64_t last_slide_log_id = OB_INVALID_ID;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionLogService* pls = NULL;
  if (OB_ISNULL(partition_service_)) {
    EXTLOG_LOG(ERROR, "null partition_service_", K(pkey));
  } else if (OB_SUCCESS != partition_service_->get_partition(pkey, guard) || NULL == guard.get_partition_group() ||
             NULL == (pls = guard.get_partition_group()->get_log_service())) {
    // partition not exist in partition_service_
  } else if (!(guard.get_partition_group()->is_valid())) {
    // partition is invalid
  } else {
    uint64_t start_log_id_dummy = OB_INVALID_ID;
    int64_t start_ts_dummy = OB_INVALID_TIMESTAMP;
    uint64_t max_log_id = OB_INVALID_ID;
    int64_t max_ts_dummy = OB_INVALID_TIMESTAMP;
    if (OB_SUCCESS != (tmp_ret = pls->get_log_id_range(start_log_id_dummy, start_ts_dummy, max_log_id, max_ts_dummy))) {
      CLOG_LOG(WARN,
          "get max log id error",
          K(tmp_ret),
          K(start_log_id_dummy),
          K(start_ts_dummy),
          K(max_log_id),
          K(max_ts_dummy));
    } else {
      last_slide_log_id = max_log_id;
    }
  }
  return last_slide_log_id;
}

int ObExtHeartbeatHandler::get_predict_timestamp(
    const ObPartitionKey& pkey, const uint64_t last_log_id, int64_t& predict_ts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "get leader info error, invalid pkey", K(ret), K(pkey));
  } else {
    ObIPartitionGroupGuard guard;
    ObIPartitionLogService* pls = NULL;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard)) || NULL == guard.get_partition_group() ||
        NULL == (pls = guard.get_partition_group()->get_log_service())) {
      // partition not exist in partition_service_
      ret = OB_PARTITION_NOT_EXIST;
    } else if (!(guard.get_partition_group()->is_valid())) {
      ret = OB_INVALID_PARTITION;
    } else if (OB_FAIL(pls->get_next_timestamp(last_log_id, predict_ts))) {
      EXTLOG_LOG(WARN, "pls get next timestamp error", K(ret), K(pkey), K(last_log_id));
    } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == predict_ts)) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "invalid predict timestamp", K(ret), K(pkey), K(last_log_id), K(predict_ts));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObExtHeartbeatHandler::get_leader_info(const ObPartitionKey& pkey, ObRole& role, int64_t& leader_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "get leader info error, invalid pkey", K(ret), K(pkey));
  } else {
    ObIPartitionGroupGuard guard;
    ObIPartitionLogService* pls = NULL;
    if (OB_FAIL(partition_service_->get_partition(pkey, guard)) || NULL == guard.get_partition_group() ||
        NULL == (pls = guard.get_partition_group()->get_log_service())) {
      // partition not exist in partition_service_
      ret = OB_PARTITION_NOT_EXIST;
    } else if (!(guard.get_partition_group()->is_valid())) {
      ret = OB_INVALID_PARTITION;
    } else {
      // ObPartitionLogService::get_leader_info also check election
      pls->get_role_and_leader_epoch(role, leader_epoch);
      // in follower, leader_epoch can be invalid
      if (OB_UNLIKELY(INVALID_ROLE == role) || OB_UNLIKELY(LEADER == role && OB_INVALID_TIMESTAMP == leader_epoch)) {
        ret = OB_ERR_UNEXPECTED;
        EXTLOG_LOG(ERROR, "get invalid role info", K(ret), K(pkey), K(role), K(leader_epoch));
      }
    }
  }
  return ret;
}

int ObExtHeartbeatHandler::get_predict_heartbeat(
    const ObPartitionKey& pkey, const uint64_t last_log_id, int64_t& res_ts)
{
  int ret = OB_SUCCESS;
  int ts_ret = OB_SUCCESS;
  ObRole role = INVALID_ROLE;
  int64_t leader_epoch = OB_INVALID_TIMESTAMP;
  int64_t predict_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(get_leader_info(pkey, role, leader_epoch))) {
    EXTLOG_LOG(WARN, "get leader info error", K(ret), K(pkey), K(last_log_id));
    ret = OB_NEED_RETRY;
  } else if (LEADER == role) {
    ts_ret = get_predict_timestamp(pkey, last_log_id, predict_ts);
    ObRole role_again = INVALID_ROLE;
    int64_t leader_epoch_again = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(get_leader_info(pkey, role_again, leader_epoch_again))) {
      // partition not exist
      EXTLOG_LOG(WARN, "get leader info again error", K(ret), K(pkey), K(last_log_id));
      ret = OB_NEED_RETRY;
    } else if (role_again == role && leader_epoch_again == leader_epoch) {
      // double check success
      if (OB_SUCCESS == ts_ret) {
        res_ts = 0 - predict_ts;  // predict res_ts
        EXTLOG_LOG(TRACE, "double check success", K(ret), K(pkey), K(last_log_id));
      } else {
        // double check success, but get predict timestamp error
        ret = ts_ret;
        EXTLOG_LOG(WARN, "sw get_predict_timestamp error", K(ret), K(pkey));
      }
    } else {
      // double check fail, retry, ignore ts_ret
      ret = OB_NEED_RETRY;  // OB_NEED_RETRY and OB_EAGAIN are both OK
      EXTLOG_LOG(WARN, "double check fail", K(ret), K(pkey), K(last_log_id));
    }
  } else if (FOLLOWER == role) {
    // follower do not know whether new log written in leader
    // liboblog need ask leader
    ret = OB_NEED_RETRY;
    EXTLOG_LOG(TRACE, "liboblog is asking a follower", K(ret), K(pkey), K(last_log_id));
  } else {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "unknown role", K(ret), K(role));
  }
  return ret;
}

int ObExtHeartbeatHandler::get_heartbeat_on_partition(
    const ObPartitionKey& pkey, const uint64_t last_log_id, int64_t& res_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == last_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "get heartbeat_on_partition error", K(ret), K(pkey), K(last_log_id));
  } else {
    uint64_t last_slide_log_id = get_last_slide_log_id(pkey);
    if (OB_INVALID_ID == last_slide_log_id) {
      // partition not exist in partition_service_
      ret = OB_NEED_RETRY;
      EXTLOG_LOG(WARN, "partition not exist when get heartbeat", K(ret), K(pkey), K(last_log_id));
    } else if (last_slide_log_id == last_log_id) {
      ret = get_predict_heartbeat(pkey, last_log_id, res_ts);
      if (OB_SUCCESS != ret && OB_NEED_RETRY != ret) {
        EXTLOG_LOG(WARN, "get predict heartbeat error", K(ret), K(pkey), K(last_log_id));
      }
    } else {
      ret = OB_NEED_RETRY;  // do not ask me
      EXTLOG_LOG(
          TRACE, "liboblog ask a wrong replica for heartbeat", K(ret), K(pkey), K(last_log_id), K(last_slide_log_id));
    }
  }
  return ret;
}

int ObExtHeartbeatHandler::do_req_heartbeat_info(
    const ObLogReqHeartbeatInfoRequest& req_msg, ObLogReqHeartbeatInfoResponse& response)
{
  int ret = OB_SUCCESS;
  int pkey_ret = OB_SUCCESS;
  const ObLogReqHeartbeatInfoRequest::ParamArray& params = req_msg.get_params();
  const int64_t partition_count = params.count();
  ObLogReqHeartbeatInfoResponse::Result result_item;
  EVENT_ADD(CLOG_EXTLOG_HEARTBEAT_PARTITION_COUNT, partition_count);

  for (int64_t i = 0; OB_SUCC(ret) && i < partition_count; i++) {
    int64_t res_ts = OB_INVALID_TIMESTAMP;
    int64_t eagain_count = 0;
    const int64_t eagain_limit = 10;
    const ObPartitionKey& pkey = params[i].pkey_;
    const uint64_t last_log_id = params[i].log_id_;

    do {
      pkey_ret = get_heartbeat_on_partition(pkey, last_log_id, res_ts);
    } while (OB_EAGAIN == pkey_ret && eagain_limit > eagain_count++);
    EXTLOG_LOG(TRACE, "get heartbeat on partition finish", K(pkey_ret), K(pkey), K(last_log_id));

    if (OB_SUCCESS == pkey_ret || OB_NEED_RETRY == pkey_ret || OB_NOT_SUPPORTED == pkey_ret || OB_EAGAIN == pkey_ret) {
      result_item.reset();
      if (OB_NOT_SUPPORTED == pkey_ret) {
        result_item.err_ = OB_NOT_SUPPORTED;
        result_item.tstamp_ = OB_INVALID_TIMESTAMP;
      } else if (OB_SUCCESS == pkey_ret) {
        result_item.err_ = OB_SUCCESS;
        result_item.tstamp_ = res_ts;
      } else if (OB_NEED_RETRY == pkey_ret || OB_EAGAIN == pkey_ret) {
        result_item.err_ = OB_NEED_RETRY;
        result_item.tstamp_ = OB_INVALID_TIMESTAMP;
      }
      if (OB_FAIL(response.append_result(result_item))) {
        EXTLOG_LOG(WARN, "response append result item error", K(ret), K(i), "param", params[i], K(result_item));
      }
    } else {
      ret = pkey_ret;
      EXTLOG_LOG(WARN, "get heartbeat on partition error", K(ret), K(i), "param", params[i]);
    }
  }  // end for
  EXTLOG_LOG(TRACE, "do req heartbeat info", K(ret), K(req_msg), K(response));
  return ret;
}

int ObExtHeartbeatHandler::req_heartbeat_info(
    const ObLogReqHeartbeatInfoRequest& req_msg, ObLogReqHeartbeatInfoResponse& response)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == partition_service_) || OB_UNLIKELY(NULL == log_engine_)) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "log external executor not inited", K(ret), KP(log_engine_), KP(partition_service_));
  } else if (!req_msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "ObLogReqHeartbeatInfoRequest is invalid", K(ret), K(req_msg));
  } else if (OB_FAIL(do_req_heartbeat_info(req_msg, response))) {
    EXTLOG_LOG(WARN, "do_req_heartbeat_info error", K(req_msg), K(response));
  }
  if (OB_FAIL(ret)) {
    ret = OB_ERR_SYS;
  }
  response.set_err(ret);
  return ret;
}

}  // namespace logservice
}  // namespace oceanbase
