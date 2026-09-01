/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ob_ls_replica_scn_snapshot.h"

#include <functional>

#include "lib/utility/utility.h"
#include "lib/utility/ob_sort.h"
#include "observer/ob_server_struct.h"  // GCTX
#include "logservice/palf/palf_handle_impl.h"

namespace oceanbase
{
namespace storage
{

int construct_new_member_list_for_majority_scn_collect(
    const palf::PalfStat &palf_stat,
    common::ObIArray<common::ObAddr> &active_f_member_list,
    int64_t &paxos_replica_number_new)
{
  int ret = OB_SUCCESS;
  const common::ObAddr &self_addr = GCTX.self_addr();
  bool found_self = false;
  const common::ObMemberList &member_list = palf_stat.paxos_member_list_;
  const common::GlobalLearnerList &degraded_list = palf_stat.degraded_list_;
  active_f_member_list.reset();
  paxos_replica_number_new = palf_stat.paxos_replica_num_;

  if (OB_UNLIKELY(!self_addr.is_valid()
      || !palf_stat.is_valid()
      || !palf_stat.config_version_.is_valid()
      || 0 >= member_list.get_member_number()
      || 0 > degraded_list.get_member_number()
      || 0 >= palf_stat.paxos_replica_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(palf_stat));
  } else {
    common::ObMember member;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      member.reset();
      if (OB_FAIL(member_list.get_member_by_index(i, member))) {
        LOG_WARN("failed to get member by index", KR(ret), K(i), K(palf_stat));
      } else if (OB_UNLIKELY(!member.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("member is invalid", KR(ret), K(member), K(palf_stat));
      } else if (degraded_list.contains(member.get_server())) {
        --paxos_replica_number_new;
      } else if (member.is_logonly()) {
      } else if (FALSE_IT(found_self = (found_self || (self_addr == member.get_server())))) {
      } else if (OB_FAIL(active_f_member_list.push_back(member.get_server()))) {
        LOG_WARN("failed to add member", KR(ret), K(member), K(palf_stat));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(0 >= paxos_replica_number_new)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid paxos replica number after filtering degraded members",
          KR(ret), K(palf_stat), K(paxos_replica_number_new));
    } else if (!found_self) {
      ret = OB_EAGAIN;
      LOG_WARN("current leader is not a non-degraded full member, try again", KR(ret), K(palf_stat),
          K(active_f_member_list), K(paxos_replica_number_new));
    }
  }
  return ret;
}

int64_t calc_majority_scn_quorum_count(
    const int64_t full_replica_count,
    const int64_t paxos_replica_number)
{
  return common::min(full_replica_count, paxos_replica_number / 2 + 1);
}

int calc_majority_min_scn(
    const int64_t quorum_count,
    common::ObIArray<share::SCN> &scn_list,
    share::SCN &majority_min_scn)
{
  int ret = OB_SUCCESS;
  majority_min_scn.set_max();
  if (OB_UNLIKELY(0 >= quorum_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(quorum_count));
  } else if (scn_list.count() < quorum_count) {
    ret = OB_EAGAIN;
    LOG_WARN("scn count does not reach quorum", KR(ret), K(quorum_count), K(scn_list));
  } else {
    (void)lib::ob_sort(&scn_list.at(0), &scn_list.at(0) + scn_list.count(),
        std::greater<share::SCN>());
    for (int64_t i = 0; i < quorum_count; ++i) {
      majority_min_scn = share::SCN::min(majority_min_scn, scn_list.at(i));
    }
  }
  return ret;
}

int ObLSReplicaSCN::init(
    const common::ObAddr &server,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(scn));
  } else {
    server_ = server;
    scn_ = scn;
  }
  return ret;
}

bool ObLSReplicaSCN::is_valid() const
{
  return server_.is_valid() && scn_.is_valid();
}

ObLSReplicaSCNSnapshot::ObLSReplicaSCNSnapshot()
    : config_version_(),
      quorum_count_(0),
      replica_scns_()
{}

int ObLSReplicaSCNSnapshot::init(const palf::PalfStat &palf_stat)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObAddr, common::OB_MAX_MEMBER_NUMBER> active_f_member_list;
  int64_t paxos_replica_number = 0;
  int64_t quorum_count = 0;

  // Record quorum in each snapshot so cache admission can check whether a new
  // collection has enough samples before replacing the cached snapshot.
  if (OB_FAIL(construct_new_member_list_for_majority_scn_collect(
      palf_stat, active_f_member_list, paxos_replica_number))) {
    LOG_WARN("failed to construct member list", KR(ret), K(palf_stat));
  } else if (FALSE_IT(quorum_count = calc_majority_scn_quorum_count(
      active_f_member_list.count(), paxos_replica_number))) {
  } else if (OB_UNLIKELY(0 >= quorum_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid quorum count", KR(ret), K(palf_stat),
        K(paxos_replica_number), K(active_f_member_list), K(quorum_count));
  } else {
    reset();
    config_version_ = palf_stat.config_version_;
    quorum_count_ = quorum_count;
  }
  return ret;
}

int ObLSReplicaSCNSnapshot::filter_from(
    const palf::PalfStat &palf_stat,
    const ObLSReplicaSCNSnapshot &raw_snapshot)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObAddr, common::OB_MAX_MEMBER_NUMBER> active_f_member_list;
  int64_t unused_paxos_replica_number = 0;

  if (OB_UNLIKELY(this == &raw_snapshot
      || !raw_snapshot.is_valid()
      || palf_stat.config_version_ != raw_snapshot.get_config_version())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(palf_stat), K(raw_snapshot));
  } else if (OB_FAIL(construct_new_member_list_for_majority_scn_collect(
      palf_stat, active_f_member_list, unused_paxos_replica_number))) {
    LOG_WARN("failed to construct member list", KR(ret), K(palf_stat));
  } else {
    reset();
    config_version_ = palf_stat.config_version_;
    quorum_count_ = raw_snapshot.get_quorum_count();
    const common::ObIArray<ObLSReplicaSCN> &raw_replica_scns =
        raw_snapshot.get_replica_scns();
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_replica_scns.count(); ++i) {
      const ObLSReplicaSCN &replica_scn = raw_replica_scns.at(i);
      if (common::has_exist_in_array(active_f_member_list, replica_scn.get_server())
          && OB_FAIL(replica_scns_.push_back(replica_scn))) {
        LOG_WARN("failed to add filtered replica scn", KR(ret), K(replica_scn),
            K(active_f_member_list), K(palf_stat));
      }
    }
  }
  return ret;
}

int ObLSReplicaSCNSnapshot::assign(const ObLSReplicaSCNSnapshot &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_UNLIKELY(!other.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid replica scn snapshot", KR(ret), K(other));
    } else if (OB_FAIL(replica_scns_.assign(other.replica_scns_))) {
      LOG_WARN("failed to assign replica scns", KR(ret), K(other));
    } else {
      config_version_ = other.config_version_;
      quorum_count_ = other.quorum_count_;
    }
  }
  return ret;
}

int ObLSReplicaSCNSnapshot::add_replica_scn(const ObLSReplicaSCN &replica_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !replica_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_scn), KPC(this));
  } else if (OB_FAIL(replica_scns_.push_back(replica_scn))) {
    LOG_WARN("failed to add replica scn", KR(ret), K(replica_scn), KPC(this));
  }
  return ret;
}

void ObLSReplicaSCNSnapshot::reset()
{
  config_version_.reset();
  quorum_count_ = 0;
  replica_scns_.reset();
}

bool ObLSReplicaSCNSnapshot::is_valid() const
{
  bool is_valid = config_version_.is_valid()
      && 0 < quorum_count_;
  for (int64_t i = 0; is_valid && i < replica_scns_.count(); ++i) {
    is_valid = replica_scns_.at(i).is_valid();
  }
  return is_valid;
}

bool ObLSReplicaSCNSnapshot::has_quorum() const
{
  return is_valid() && replica_scns_.count() >= quorum_count_;
}

} // namespace storage
} // namespace oceanbase
