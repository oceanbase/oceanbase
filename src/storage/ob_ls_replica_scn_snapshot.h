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

#ifndef OCEANBASE_STORAGE_OB_LS_REPLICA_SCN_SNAPSHOT_
#define OCEANBASE_STORAGE_OB_LS_REPLICA_SCN_SNAPSHOT_

#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "logservice/palf/log_meta_info.h"
#include "share/scn.h"

namespace oceanbase
{
namespace palf
{
struct PalfStat;
}
namespace storage
{

// Build the non-degraded FULL member list for majority SCN collection.
// Degraded members are excluded from both the list and Paxos replica number.
// LOGONLY members remain Paxos replica number but are excluded from the list
// because they have no readable SCN.
// Return OB_EAGAIN if self is not in the resulting list.
int construct_new_member_list_for_majority_scn_collect(
    const palf::PalfStat &palf_stat,
    common::ObIArray<common::ObAddr> &active_f_member_list,
    int64_t &paxos_replica_number_new);

// The SCN quorum is capped by the number of FULL replicas that can provide
// SCN samples. The Paxos replica number includes LOGONLY replicas, and both
// counts exclude degraded replicas.
int64_t calc_majority_scn_quorum_count(
    const int64_t full_replica_count,
    const int64_t paxos_replica_number);

int calc_majority_min_scn(
    const int64_t quorum_count,
    common::ObIArray<share::SCN> &scn_list,
    share::SCN &majority_min_scn);

struct ObLSReplicaSCN
{
public:
  ObLSReplicaSCN() : server_(), scn_() {}
  ~ObLSReplicaSCN() {}
  int init(const common::ObAddr &server, const share::SCN &scn);
  bool is_valid() const;
  const share::SCN &get_scn() const { return scn_; }
  const common::ObAddr &get_server() const { return server_; }
  TO_STRING_KV(K_(server), K_(scn));
private:
  common::ObAddr server_;
  share::SCN scn_;
};

struct ObLSReplicaSCNSnapshot
{
public:
  ObLSReplicaSCNSnapshot();
  ~ObLSReplicaSCNSnapshot() { reset(); }
  int init(const palf::PalfStat &palf_stat);
  int filter_from(
      const palf::PalfStat &palf_stat,
      const ObLSReplicaSCNSnapshot &raw_snapshot);
  int assign(const ObLSReplicaSCNSnapshot &other);
  int add_replica_scn(const ObLSReplicaSCN &replica_scn);
  void reset();
  bool is_valid() const;
  bool has_quorum() const;
  const palf::LogConfigVersion &get_config_version() const { return config_version_; }
  int64_t get_quorum_count() const { return quorum_count_; }
  const common::ObIArray<ObLSReplicaSCN> &get_replica_scns() const
  {
    return replica_scns_;
  }
  TO_STRING_KV(K_(config_version), K_(quorum_count), K_(replica_scns));

private:
  palf::LogConfigVersion config_version_;
  int64_t quorum_count_;
  common::ObSEArray<ObLSReplicaSCN, common::OB_MAX_MEMBER_NUMBER + 1> replica_scns_;
};

} // namespace storage
} // namespace oceanbase

#endif
