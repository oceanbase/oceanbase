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

#ifndef OCEANBASE_STORAGE_LS_OB_LS_REPLICA_CHECKPOINT_COLLECTOR_
#define OCEANBASE_STORAGE_LS_OB_LS_REPLICA_CHECKPOINT_COLLECTOR_

#include "common/ob_member_list.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "share/ob_ls_id.h"
#include "storage/ob_ls_replica_scn_snapshot.h"

namespace oceanbase
{
namespace palf
{
struct PalfStat;
}
namespace storage
{

class ObLSReplicaCheckpointCollector
{
public:
  ObLSReplicaCheckpointCollector();
  ~ObLSReplicaCheckpointCollector() { reset(); }
  int init(const share::ObLSID &ls_id);
  void reset();
  int update_replica_checkpoint_info(
      const palf::PalfStat &palf_stat,
      const ObLSReplicaSCNSnapshot &scn_snapshot);
  int get_majority_min_replica_checkpoint_scn(
      share::SCN &checkpoint_scn) const;
private:
  int get_latest_palf_stat_(palf::PalfStat &palf_stat) const;
  int cache_checkpoint_snapshot_(
      const ObLSReplicaSCNSnapshot &checkpoint_snapshot);
  int get_checkpoint_snapshot_(
      const palf::LogConfigVersion &config_version,
      ObLSReplicaSCNSnapshot &checkpoint_snapshot) const;
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  mutable common::SpinRWLock lock_;
  ObLSReplicaSCNSnapshot checkpoint_snapshot_;
};

}
}

#endif
