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

#include "ob_log_broadcast_info_mgr.h"
#include <algorithm>
#include "ob_log_membership_mgr_V2.h"
#include "lib/container/ob_se_array_iterator.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObLogBroadcastInfoMgr::ObLogBroadcastInfoMgr() : is_inited_(false), membership_mgr_(NULL)
{}

ObLogBroadcastInfoMgr::~ObLogBroadcastInfoMgr()
{
  destroy();
}

int ObLogBroadcastInfoMgr::init(const common::ObPartitionKey& partition_key, const ObILogMembershipMgr* membership_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "init twice", K(ret), K(partition_key));
  } else if (!partition_key.is_valid() || OB_ISNULL(membership_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key), KP(membership_mgr));
  } else {
    partition_key_ = partition_key;
    membership_mgr_ = membership_mgr;

    is_inited_ = true;
  }

  if (!is_inited_ && OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}

void ObLogBroadcastInfoMgr::destroy()
{
  membership_mgr_ = NULL;
  partition_key_.reset();
  broadcast_info_.destroy();
  is_inited_ = false;
}

int ObLogBroadcastInfoMgr::update_broadcast_info(
    const common::ObAddr& server, const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "is not inited", K(ret), K(partition_key_));
  } else if (!server.is_valid() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type) ||
             OB_INVALID_ID == max_confirmed_log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(server), K(replica_type), K(max_confirmed_log_id));
  } else {
    BroadcastInfo info;
    info.server_ = server;
    info.replica_type_ = replica_type;
    info.max_confirmed_log_id_ = max_confirmed_log_id;
    info.update_ts_ = ObTimeUtility::current_time();
    const ObMemberList member_list = membership_mgr_->get_curr_member_list();
    if (OB_FAIL(clear_expired_broadcast_info(member_list))) {
      CLOG_LOG(WARN, "failed to clear_expired_broadcast_info", K(ret));
    } else if (!(member_list.contains(server))) {
      ret = OB_ENTRY_NOT_EXIST;
      CLOG_LOG(WARN, "server is not in member_list, no need to update", K(server), K(member_list), K(ret));
    } else {
      // update or push back the info
      bool found = false;
      ARRAY_FOREACH_X(broadcast_info_, idx, cnt, OB_SUCC(ret) && !found)
      {
        BroadcastInfo& local_info = broadcast_info_.at(idx);
        if (server == local_info.server_) {
          local_info = info;
          found = true;
        }
      }

      if (OB_SUCC(ret) && !found) {
        if (OB_FAIL(broadcast_info_.push_back(info))) {
          CLOG_LOG(WARN, "failed to push back broadcast_info", K(info), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogBroadcastInfoMgr::get_recyclable_log_id(uint64_t& log_id) const
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "is not inited", K(ret), K(partition_key_));
  } else {
    const ObMemberList member_list = membership_mgr_->get_curr_member_list();
    const int64_t replica_num = membership_mgr_->get_replica_num();
    // If the broadcast_info information of all members has been received,
    // log_id can be obtained based on the information of all F replicas (
    // If the number of F replicas exceeds the majority of member_list, then judge the majority).
    // Otherwise, log_id must be obtained based on a majority F replicas.
    if (is_member_completed_(member_list)) {
      ret = check_majority_or_all_(member_list, replica_num, log_id);
    } else {
      ret = check_majority_(member_list, replica_num, log_id);
    }
  }

  return ret;
}

bool ObLogBroadcastInfoMgr::is_member_completed_(const common::ObMemberList& member_list) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; (OB_SUCCESS == ret) && idx < member_list.get_member_number(); idx++) {
    ObAddr server;
    BroadcastInfo info;
    if (OB_FAIL(member_list.get_server_by_index(idx, server))) {
      CLOG_LOG(ERROR, "get_server_by_index failed", K(ret));
    } else if (OB_FAIL(get_broadcast_info_by_server(server, info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        break;
      } else {
        CLOG_LOG(ERROR, "broadcast_info_ get failed", K(ret));
      }
    } else if (ObTimeUtility::current_time() - info.update_ts_ > CLOG_BROADCAST_MAX_INTERVAL) {
      ret = OB_ENTRY_NOT_EXIST;
      break;
    }
  }
  if (OB_SUCCESS == ret) {
    bool_ret = true;
  }
  return bool_ret;
}

int ObLogBroadcastInfoMgr::check_majority_or_all_(
    const common::ObMemberList& member_list, const int64_t replica_num, uint64_t& log_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_majority_(member_list, replica_num, log_id))) {
    CLOG_LOG(WARN, "check_majority_ failed", K(ret), K(partition_key_));
  } else if (0 == log_id && OB_FAIL(check_all_(member_list, log_id))) {
    CLOG_LOG(WARN, "check_all_ failed", K(ret), K(partition_key_));
  }
  return ret;
}

int ObLogBroadcastInfoMgr::check_all_(const common::ObMemberList& member_list, uint64_t& log_id) const
{
  int ret = OB_SUCCESS;
  log_id = 0;
  uint64_t saved_log_id = OB_INVALID_ID;
  for (int64_t idx = 0; (OB_SUCCESS == ret) && idx < member_list.get_member_number(); idx++) {
    ObAddr server;
    BroadcastInfo info;
    if (OB_FAIL(member_list.get_server_by_index(idx, server))) {
      CLOG_LOG(ERROR, "get_server_by_index failed", K(ret));
    } else if (OB_FAIL(get_broadcast_info_by_server(server, info))) {
      CLOG_LOG(ERROR, "broadcast_info_ get failed", K(ret));
    } else if (info.replica_type_ != REPLICA_TYPE_LOGONLY) {
      if (info.max_confirmed_log_id_ < saved_log_id) {
        saved_log_id = info.max_confirmed_log_id_;
      }
    }
  }
  if (OB_SUCCESS == ret) {
    log_id = saved_log_id;
  }
  return ret;
}

static inline int my_greater(int64_t a, int64_t b)
{
  return a > b;
}

int ObLogBroadcastInfoMgr::check_majority_(
    const common::ObMemberList& member_list, const int64_t replica_num, uint64_t& log_id) const
{
  int ret = OB_SUCCESS;
  log_id = 0;
  int64_t check_num = 0;
  int64_t majority = (replica_num / 2) + 1;
  ObSEArray<int64_t, 5> log_id_array;
  for (int64_t idx = 0; (OB_SUCCESS == ret) && idx < member_list.get_member_number(); idx++) {
    ObAddr server;
    BroadcastInfo info;
    if (OB_FAIL(member_list.get_server_by_index(idx, server))) {
      CLOG_LOG(ERROR, "get_server_by_index failed", K(ret));
    } else if (OB_FAIL(get_broadcast_info_by_server(server, info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        CLOG_LOG(ERROR, "broadcast_info_ get failed", K(ret));
      }
    } else if (info.replica_type_ != REPLICA_TYPE_LOGONLY &&
               ObTimeUtility::current_time() - info.update_ts_ <= CLOG_BROADCAST_MAX_INTERVAL) {
      if (OB_FAIL(log_id_array.push_back(info.max_confirmed_log_id_))) {
        CLOG_LOG(WARN, "log_id_array push_back failed", K(ret), K(partition_key_));
      } else {
        check_num++;
      }
    }
  }
  if (OB_SUCCESS == ret && check_num >= majority) {
    std::sort(log_id_array.begin(), log_id_array.end(), my_greater);
    log_id = log_id_array[majority - 1];
  }
  return ret;
}

int ObLogBroadcastInfoMgr::get_broadcast_info_by_server(const ObAddr& server, BroadcastInfo& out_info) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  ARRAY_FOREACH_X(broadcast_info_, idx, cnt, OB_SUCC(ret) && !found)
  {
    const BroadcastInfo& info = broadcast_info_.at(idx);
    if (server == info.server_) {
      out_info = info;
      found = true;
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObLogBroadcastInfoMgr::clear_expired_broadcast_info(const ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < broadcast_info_.count();) {
    BroadcastInfo& info = broadcast_info_.at(idx);
    if (!(member_list.contains(info.server_))) {
      if (OB_FAIL(broadcast_info_.remove(idx))) {
        CLOG_LOG(WARN, "failed to remove expired broadcast info", K(idx), K(info), K(ret));
      }
    } else {
      ++idx;  // check next element
    }
  }
  return ret;
}

}  // namespace clog
}  // namespace oceanbase
