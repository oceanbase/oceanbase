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

#include "ob_clog_sync_rpc.h"
#include "ob_clog_mgr.h"
#include "storage/ob_partition_service.h"
namespace oceanbase {
using namespace common;
using namespace clog;

namespace obrpc {
// ObLogGetMCTsProcessor
int ObLogGetMCTsProcessor::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObLogGetMCTsRequest& request_msg = arg_;
    ObLogGetMCTsResponse& result = result_;
    ret = partition_service_->get_clog_mgr()->handle_get_mc_ts_request(request_msg, result);
  }
  return ret;
}

int ObLogGetMcCtxArrayProcessor::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObLogGetMcCtxArrayRequest& request_msg = arg_;
    ObLogGetMcCtxArrayResponse& result = result_;
    ret = partition_service_->get_clog_mgr()->handle_get_mc_ctx_array_request(request_msg, result);
  }
  return ret;
}

int ObLogGetPriorityArrayProcessor::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObLogGetPriorityArrayRequest& request_msg = arg_;
    ObLogGetPriorityArrayResponse& result = result_;
    ret = partition_service_->get_clog_mgr()->handle_get_priority_array_request(request_msg, result);
  }
  return ret;
}

int ObLogGetRemoteLogProcessor::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KP(partition_service_));
  } else {
    const ObLogGetRemoteLogRequest& request_msg = arg_;
    ObLogGetRemoteLogResponse& result = result_;
    ret = partition_service_->get_clog_mgr()->handle_get_remote_log_request(request_msg, result);
  }
  return ret;
}
}  // namespace obrpc
}  // namespace oceanbase
