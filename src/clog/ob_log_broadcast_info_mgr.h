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

#ifndef OCEANBASE_CLOG_OB_LOG_BROADCAST_INFO_MGR_H_
#define OCEANBASE_CLOG_OB_LOG_BROADCAST_INFO_MGR_H_

#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/net/ob_addr.h"

namespace oceanbase {
namespace clog {
class ObILogMembershipMgr;
class ObLogBroadcastInfoMgr {
public:
  ObLogBroadcastInfoMgr();
  ~ObLogBroadcastInfoMgr();

public:
  int init(const common::ObPartitionKey& partition_key, const ObILogMembershipMgr* membership_mgr);
  void destroy();
  int update_broadcast_info(
      const common::ObAddr& server, const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id);
  int get_recyclable_log_id(uint64_t& log_id) const;

private:
  struct BroadcastInfo {
  public:
    BroadcastInfo()
        : server_(),
          replica_type_(common::REPLICA_TYPE_MAX),
          max_confirmed_log_id_(common::OB_INVALID_ID),
          update_ts_(common::OB_INVALID_TIMESTAMP)
    {}
    ~BroadcastInfo()
    {}
    TO_STRING_KV(K_(server), K_(replica_type), K_(max_confirmed_log_id), K_(update_ts));

  public:
    common::ObAddr server_;
    common::ObReplicaType replica_type_;
    uint64_t max_confirmed_log_id_;
    int64_t update_ts_;
  };

private:
  bool is_member_completed_(const common::ObMemberList& member_list) const;
  int check_majority_or_all_(
      const common::ObMemberList& member_list, const int64_t replica_num, uint64_t& log_id) const;
  int check_all_(const common::ObMemberList& member_list, uint64_t& log_id) const;
  int check_majority_(const common::ObMemberList& member_list, const int64_t replica_num, uint64_t& log_id) const;
  int get_broadcast_info_by_server(const common::ObAddr& observer, BroadcastInfo& out_info) const;
  int clear_expired_broadcast_info(const common::ObMemberList& member_list);

private:
  bool is_inited_;
  mutable common::ObSpinLock lock_;
  common::ObPartitionKey partition_key_;
  const ObILogMembershipMgr* membership_mgr_;
  common::ObSEArray<BroadcastInfo, common::OB_MAX_MEMBER_NUMBER> broadcast_info_;
  // common::ObLinearHashMap<common::ObAddr, BroadcastInfo> broadcast_info_;

  DISALLOW_COPY_AND_ASSIGN(ObLogBroadcastInfoMgr);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_STORAGE_INFO_MGR_H_
