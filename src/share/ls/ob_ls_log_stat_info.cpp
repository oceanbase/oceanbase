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

#define USING_LOG_PREFIX SHARE

#include "share/ls/ob_ls_log_stat_info.h"
#include "rootserver/ob_root_utils.h"   // majority
#include "logservice/palf/log_define.h" // INVALID_PROPOSAL_ID

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace share
{

ObLSLogStatReplica::ObLSLogStatReplica()
{
  reset();
}

int ObLSLogStatReplica::init(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const common::ObAddr &server,
    const ObRole &role,
    const int64_t proposal_id,
    const ObLSReplica::MemberList &member_list,
    const int64_t paxos_replica_num,
    const int64_t end_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id)
      || !server.is_valid()
      || INVALID_ROLE == role)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(ls_id), K(server),
        K(role), K(proposal_id), K(member_list), K(paxos_replica_num), K(end_scn));
  } else if (LEADER == role
      && (INVALID_PROPOSAL_ID == proposal_id
      || paxos_replica_num <= 0
      || member_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid leader replica", KR(ret), K(tenant_id), K(ls_id), K(server),
        K(role), K(proposal_id), K(member_list), K(paxos_replica_num), K(end_scn));
  } else if (OB_FAIL(member_list_.assign(member_list))) {
    LOG_WARN("fail to assign", KR(ret), K(member_list), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    server_ = server;
    role_ = role;
    proposal_id_ = proposal_id;
    paxos_replica_num_ = paxos_replica_num;
    end_scn_ = end_scn;
  }
  return ret;
}

bool ObLSLogStatReplica::is_valid() const
{
  return ls_id_.is_valid_with_tenant(tenant_id_)
      && server_.is_valid()
      && INVALID_ROLE != role_;
}

void ObLSLogStatReplica::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  server_.reset();
  role_ = INVALID_ROLE;
  proposal_id_ = INVALID_PROPOSAL_ID;
  member_list_.reset();
  paxos_replica_num_ = OB_INVALID_COUNT;
  end_scn_ = OB_INVALID_SCN_VAL;
}

int ObLSLogStatReplica::assign(const ObLSLogStatReplica &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(member_list_.assign(other.member_list_))) {
    LOG_WARN("fail to assign", KR(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    server_ = other.server_;
    role_ = other.role_;
    proposal_id_ = other.proposal_id_;
    paxos_replica_num_ = other.paxos_replica_num_;
    end_scn_ = other.end_scn_;
  }
  return ret;
}

bool ObLSLogStatReplica::is_in_member_list(
    const ObLSReplica::MemberList &member_list) const
{
  bool bret = false;
  ARRAY_FOREACH_NORET(member_list, idx) {
    if (member_list.at(idx).get_server() == server_) {
      bret = true;
      break;
    }
  }
  return bret;
}

ObLSLogStatInfo::ObLSLogStatInfo()
    : tenant_id_(OB_INVALID_TENANT_ID),
      ls_id_(),
      replicas_()
{
}

void ObLSLogStatInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  replicas_.reset();
}

int ObLSLogStatInfo::init(
    const uint64_t tenant_id,
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(ls_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    replicas_.reset();
  }
  return ret;
}

bool ObLSLogStatInfo::is_valid() const
{
  return ls_id_.is_valid_with_tenant(tenant_id_);
}

int ObLSLogStatInfo::assign(const ObLSLogStatInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    if (OB_FAIL(replicas_.assign(other.replicas_))) {
      LOG_WARN("assign failed", KR(ret), K_(replicas), K(other));
    }
  }
  return ret;
}

bool ObLSLogStatInfo::is_self_replica(const ObLSLogStatReplica &replica) const
{
  bool bret = false;
  if (OB_LIKELY(replica.is_valid())) {
    bret = (replica.get_tenant_id() == tenant_id_) && (replica.get_ls_id() == ls_id_);
  }
  return bret;
}

bool ObLSLogStatInfo::has_replica(const ObLSLogStatReplica &replica) const
{
  bool bret = false;
  ARRAY_FOREACH_NORET(replicas_, idx) {
    const ObLSLogStatReplica &self_replica = replicas_.at(idx);
    if (self_replica.get_tenant_id() == replica.get_tenant_id()
        && self_replica.get_ls_id() == replica.get_ls_id()
        && self_replica.get_server() == replica.get_server()) {
      bret = true;
      break;
    }
  }
  return bret;
}

int ObLSLogStatInfo::add_replica(const ObLSLogStatReplica &replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !replica.is_valid() || !is_self_replica(replica))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ObLSLogStatReplica", KR(ret), K(replica), KPC(this));
  } else if (has_replica(replica)) {
    // skip
  } else if (OB_FAIL(replicas_.push_back(replica))) {
    LOG_WARN("fail to push back", KR(ret), K(replica), K_(replicas));
  }
  return ret;
}

int ObLSLogStatInfo::check_has_majority(
    const common::ObIArray<ObAddr> &valid_servers, 
    const int64_t arb_replica_num,
    bool &has) const
{
  int ret = OB_SUCCESS;
  has = false;
  if (OB_UNLIKELY(!is_valid()
                  || (0 != arb_replica_num && 1 != arb_replica_num))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arb_replica_num), KPC(this));
  } else {
    int64_t paxos_count = 0;
    int64_t leader_idx = get_leader_index_();
    if (OB_INVALID_INDEX == leader_idx) {
      ret = OB_LEADER_NOT_EXIST;
      has = false;
      LOG_WARN("ls has no leader", KR(ret), KPC(this));
    } else if (OB_UNLIKELY(leader_idx < 0 || leader_idx >= replicas_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid leader index", KR(ret), K(leader_idx), KPC(this));
    } else {
      int64_t paxos_replica_num = replicas_.at(leader_idx).get_paxos_replica_num();
      if (OB_UNLIKELY(paxos_replica_num <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("paxos_replica_num can't be smaller than or equal to 0",
            KR(ret), K(paxos_replica_num), KPC(this));
      } else {
        ARRAY_FOREACH_N(replicas_, idx, cnt) {
          const ObAddr &server = replicas_.at(idx).get_server();
          if (common::has_exist_in_array(valid_servers, server)) {
            ++paxos_count;
          }
        }
        has = paxos_count + arb_replica_num >= rootserver::majority(paxos_replica_num);
        LOG_INFO("check has majority finished", KR(ret), K_(tenant_id), K_(ls_id),
            "has_majority", has, "leader_replica", replicas_.at(leader_idx),
            "paxos_count_in_valid_server", paxos_count, K(arb_replica_num), K(valid_servers), KPC(this));
      }
    }
  }
  return ret;
}

int ObLSLogStatInfo::check_log_sync(
    const common::ObIArray<ObAddr> &valid_servers, 
    const int64_t arb_replica_num,
    bool &is_log_sync) const
{
  int ret = OB_SUCCESS;
  is_log_sync = false;
  const int64_t leader_idx = get_leader_index_();
  if (OB_UNLIKELY(!is_valid()
                  || (0 != arb_replica_num && 1 != arb_replica_num))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arb_replica_num), KPC(this));
  } else if (OB_INVALID_INDEX == leader_idx) {
    ret = OB_LEADER_NOT_EXIST;
    is_log_sync = false;
    LOG_WARN("ls has no leader", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(leader_idx < 0 || leader_idx >= replicas_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid leader index", KR(ret), K(leader_idx), KPC(this));
  } else {
    const int64_t leader_end_scn_ts = replicas_.at(leader_idx).get_end_scn();
    int64_t in_sync_count = 0;
    ARRAY_FOREACH_N(replicas_, idx, cnt) {
      const ObAddr &server = replicas_.at(idx).get_server();
      const int64_t replica_end_scn_ts = replicas_.at(idx).get_end_scn();
      if (common::has_exist_in_array(valid_servers, server)) {
        const int64_t diff = leader_end_scn_ts - replica_end_scn_ts;
        //the diff may less than 0, follower may replay fast then leadr
        if (diff <= LOG_IN_SYNC_INTERVAL_NS) {
          ++in_sync_count;
        } else {
          LOG_INFO("replica is not in sync", K(leader_end_scn_ts), K(replica_end_scn_ts), K(diff),
              "leader_replica", replicas_.at(leader_idx), "current_replica", replicas_.at(idx));
        }
      }
    } // end foreach replicas_
    int64_t paxos_replica_num = replicas_.at(leader_idx).get_paxos_replica_num();
    is_log_sync = (in_sync_count + arb_replica_num >= rootserver::majority(paxos_replica_num));
    LOG_INFO("check log sync finished", KR(ret), K(is_log_sync), K(in_sync_count), K(arb_replica_num),
        K(paxos_replica_num), K(valid_servers), "leader_replica", replicas_.at(leader_idx), KPC(this));
  }
  return ret;
}

int ObLSLogStatInfo::get_leader_replica(ObLSLogStatReplica &leader) const
{
  int ret = OB_SUCCESS;
  leader.reset();
  int64_t leader_idx = get_leader_index_();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls log stat info", KR(ret), KPC(this));
  } else if (OB_INVALID_INDEX == leader_idx) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("ls has no leader", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(leader_idx < 0 || leader_idx >= replicas_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid leader index", KR(ret), K(leader_idx), KPC(this));
  } else if (OB_FAIL(leader.assign(replicas_.at(leader_idx)))) {
    LOG_WARN("fail to assign", KR(ret), KPC(this));
  }
  return ret;
}

int64_t ObLSLogStatInfo::get_leader_index_() const
{
  int64_t index = OB_INVALID_INDEX;
  int64_t max_proposal_id = INVALID_PROPOSAL_ID;
  ARRAY_FOREACH_NORET(replicas_, idx) {
    const ObLSLogStatReplica &replica = replicas_.at(idx);
    if (LEADER == replica.get_role()) {
      if (INVALID_PROPOSAL_ID == max_proposal_id
          || replica.get_proposal_id() > max_proposal_id) {
        max_proposal_id = replica.get_proposal_id();
        index = idx;
      }
    }
  }
  return index;
}

bool ObLSLogStatInfo::has_leader() const
{
  bool bret = false;
  int64_t leader_idx = get_leader_index_();
  if (OB_INVALID_INDEX != leader_idx) {
    bret = true;
  }
  return bret;
}

} // end namespace share
} // end namespace oceanbase
