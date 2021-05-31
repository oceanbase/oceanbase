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

#include "ob_log_callback_task.h"

namespace oceanbase {
using namespace common;
namespace clog {
int ObLogCallbackTask::init(const CallbackTaskType& task_type, const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  task_type_ = task_type;
  partition_key_ = partition_key;
  return ret;
}

void ObLogCallbackTask::reset()
{
  task_type_ = CLOG_UNKNOWN_CB;
  partition_key_.reset();
}

int ObGeneralCallbackTask::init(common::ObICallback* callback)
{
  int ret = OB_SUCCESS;
  ObPartitionKey pkey;

  if (OB_ISNULL(callback)) {
    CLOG_LOG(WARN, "invalid argument", KP(callback));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObLogCallbackTask::init(CLOG_BATCH_CB, pkey))) {
    CLOG_LOG(WARN, "ObLogCallbackTask init error", K(ret), K(pkey));
  } else {
    callback_ = callback;
  }

  return ret;
}

void ObGeneralCallbackTask::reset()
{
  ObLogCallbackTask::reset();
  callback_ = NULL;
}

int ObGeneralCallbackTask::handle_callback()
{
  return NULL != callback_ ? callback_->callback() : OB_NOT_INIT;
}

int ObMemberChangeCallbackTask::init(const CallbackTaskType& task_type, const common::ObPartitionKey& partition_key,
    const clog::ObLogType log_type, const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
    const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list,
    const common::ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;

  if (task_type < CLOG_UNKNOWN_CB || task_type > CLOG_MEMBER_CHANGE_SUCCESS_CB || !partition_key.is_valid() ||
      mc_timestamp <= 0 || replica_num <= 0 || OB_INVALID_ID == ms_log_id || prev_member_list.get_member_number() < 0 ||
      curr_member_list.get_member_number() < 0) {
    CLOG_LOG(WARN,
        "invalid argument",
        K(task_type),
        K(partition_key),
        K(mc_timestamp),
        K(ms_log_id),
        K(replica_num),
        K(prev_member_list),
        K(curr_member_list));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObLogCallbackTask::init(task_type, partition_key))) {
    CLOG_LOG(WARN, "ObLogCallbackTask init error", K(ret), K(task_type), K(partition_key));
  } else {
    log_type_ = log_type;
    ms_log_id_ = ms_log_id;
    mc_timestamp_ = mc_timestamp;
    replica_num_ = replica_num;
    ms_proposal_id_ = ms_proposal_id;
    if (OB_FAIL(prev_member_list_.deep_copy(prev_member_list))) {
      CLOG_LOG(WARN, "prev member list deep copy error", K(ret));
    } else if (OB_FAIL(curr_member_list_.deep_copy(curr_member_list))) {
      CLOG_LOG(WARN, "curr member list deep copy error", K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

void ObMemberChangeCallbackTask::reset()
{
  ObLogCallbackTask::reset();
  log_type_ = OB_LOG_UNKNOWN;
  ms_log_id_ = common::OB_INVALID_ID;
  mc_timestamp_ = common::OB_INVALID_TIMESTAMP;
  replica_num_ = 0;
  prev_member_list_.reset();
  curr_member_list_.reset();
  ms_proposal_id_.reset();
}
}  // namespace clog
}  // namespace oceanbase
