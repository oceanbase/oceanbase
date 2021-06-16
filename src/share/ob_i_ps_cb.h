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

#ifndef OCEANBASE_SHARE_OB_I_PS_CB_H_
#define OCEANBASE_SHARE_OB_I_PS_CB_H_

#include "common/ob_partition_key.h"
#include "common/ob_role.h"
#include "common/ob_member_list.h"
#include "common/ob_idc.h"
#include "clog/ob_log_define.h"

namespace oceanbase {
namespace common {
class ObAddr;
class ObProposalID;
}  // namespace common
namespace share {
class ObIPSCb {
public:
  virtual int on_leader_revoke(const common::ObPartitionKey& pkey, const common::ObRole& role) = 0;
  virtual int on_leader_takeover(
      const common::ObPartitionKey& pkey, const common::ObRole& role, const bool is_elected_by_changing_leader) = 0;
  virtual int on_leader_active(
      const common::ObPartitionKey& pkey, const common::ObRole& role, const bool is_elected_by_changing_leader) = 0;
  virtual int on_member_change_success(const common::ObPartitionKey& pkey, const clog::ObLogType log_type,
      const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
      const common::ObMemberList& prev_member_list, const common::ObMemberList& curr_member_list,
      const common::ObProposalID& ms_proposal_id) = 0;
  virtual bool is_take_over_done(const common::ObPartitionKey& pkey) const = 0;
  virtual bool is_revoke_done(const common::ObPartitionKey& pkey) const = 0;
  virtual bool is_tenant_active(const common::ObPartitionKey& pkey) const = 0;
  virtual bool is_tenant_active(const uint64_t tenant_id) const = 0;
  virtual int nonblock_get_strong_leader_from_loc_cache(const common::ObPartitionKey& pkey, common::ObAddr& leader,
      int64_t& cluster_id, const bool is_need_renew = false) = 0;
  virtual int nonblock_get_leader_by_election_from_loc_cache(const common::ObPartitionKey& pkey, int64_t cluster_id,
      common::ObAddr& leader, const bool is_need_renew = false) = 0;
  virtual int nonblock_get_leader_by_election_from_loc_cache(
      const common::ObPartitionKey& pkey, common::ObAddr& leader, const bool is_need_renew = false) = 0;
  virtual int nonblock_get_strong_leader_from_loc_cache(
      const common::ObPartitionKey& pkey, common::ObAddr& leader, const bool is_need_renew = false) = 0;
  virtual int handle_log_missing(const common::ObPartitionKey& pkey, const common::ObAddr& server) = 0;
  virtual int get_server_region(const common::ObAddr& server, common::ObRegion& region) const = 0;
  virtual int get_server_idc(const common::ObAddr& server, common::ObIDC& idc) const = 0;
  virtual int get_server_cluster_id(const common::ObAddr& server, int64_t& cluster_id) const = 0;
  virtual int record_server_region(const common::ObAddr& server, const common::ObRegion& region) = 0;
  virtual int record_server_idc(const common::ObAddr& server, const common::ObIDC& idc) = 0;
  virtual int record_server_cluster_id(const common::ObAddr& server, const int64_t cluster_id) = 0;

  virtual int check_partition_exist(const common::ObPartitionKey& pkey, bool& exist) = 0;
  virtual bool is_service_started() const
  {
    return true;
  }
  virtual int get_global_max_decided_trans_version(int64_t& max_decided_trans_version) const
  {
    UNUSED(max_decided_trans_version);
    int ret = common::OB_NOT_SUPPORTED;
    STORAGE_LOG(ERROR, "get_global_max_decided_trans_version not implemented");
    return ret;
  }
  virtual int submit_pt_update_task(const common::ObPartitionKey& pkey) = 0;
  virtual int submit_pt_update_role_task(const common::ObPartitionKey& pkey) = 0;
};
}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_I_PS_CB_H_
