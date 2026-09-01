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

#include "storage/ls/ob_ls_replica_checkpoint_collector.h"

#include "logservice/ob_log_service.h"
#include "logservice/palf/palf_handle_impl.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace storage
{
ObLSReplicaCheckpointCollector::ObLSReplicaCheckpointCollector()
    : is_inited_(false),
      ls_id_(),
      lock_(),
      checkpoint_snapshot_()
{}

int ObLSReplicaCheckpointCollector::init(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("failed to lock replica checkpoint collector", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("replica checkpoint collector has been initialized", KR(ret),
        K(ls_id), K_(ls_id));
  } else {
    checkpoint_snapshot_.reset();
    ls_id_ = ls_id;
    is_inited_ = true;
  }
  return ret;
}

void ObLSReplicaCheckpointCollector::reset()
{
  SpinWLockGuard guard(lock_);
  is_inited_ = false;
  ls_id_.reset();
  checkpoint_snapshot_.reset();
}

int ObLSReplicaCheckpointCollector::update_replica_checkpoint_info(
    const palf::PalfStat &palf_stat,
    const ObLSReplicaSCNSnapshot &scn_snapshot)
{
  int ret = OB_SUCCESS;
  ObLSReplicaSCNSnapshot filtered_scn_snapshot;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica checkpoint collector is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!palf_stat.is_valid()
      || palf_stat.palf_id_ != ls_id_.id()
      || !palf_stat.paxos_member_list_.is_valid()
      || !scn_snapshot.is_valid()
      || palf_stat.config_version_ != scn_snapshot.get_config_version())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(ls_id), K(palf_stat), K(scn_snapshot));
  } else if (!is_strong_leader(palf_stat.role_)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not leader", KR(ret), K_(ls_id), K(palf_stat));
  } else if (OB_FAIL(filtered_scn_snapshot.filter_from(palf_stat, scn_snapshot))) {
    LOG_WARN("failed to init filtered replica checkpoint snapshot", KR(ret),
        K(palf_stat), K(scn_snapshot));
  }
  if (OB_FAIL(ret)) { // do nothing
  } else if (OB_FAIL(cache_checkpoint_snapshot_(filtered_scn_snapshot))) {
    LOG_WARN("failed to cache replica checkpoint snapshot", KR(ret),
        K(palf_stat), K(scn_snapshot), K(filtered_scn_snapshot));
  }
  return ret;
}

int ObLSReplicaCheckpointCollector::get_latest_palf_stat_(
    palf::PalfStat &palf_stat) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = nullptr;
  palf::PalfHandleGuard palf_handle_guard;
  palf_stat.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica checkpoint collector is not initialized", KR(ret));
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get MTL log service", KR(ret), K_(ls_id));
  } else if (OB_FAIL(log_service->open_palf(ls_id_, palf_handle_guard))) {
    LOG_WARN("failed to open palf", KR(ret), K_(ls_id));
  } else if (OB_FAIL(palf_handle_guard.stat(palf_stat))) {
    LOG_WARN("failed to get palf stat", KR(ret), K_(ls_id));
  }
  return ret;
}

int ObLSReplicaCheckpointCollector::cache_checkpoint_snapshot_(
    const ObLSReplicaSCNSnapshot &checkpoint_snapshot)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("failed to lock replica checkpoint collector", KR(ret),
        K(checkpoint_snapshot));
  } else if (!checkpoint_snapshot.has_quorum()
      && checkpoint_snapshot_.get_config_version()
          == checkpoint_snapshot.get_config_version()
      && checkpoint_snapshot_.has_quorum()) {
    // Same-config temporary sample loss must not erase a previously proven result.
    LOG_WARN("replica checkpoint snapshot does not reach quorum, keep old result",
        K(checkpoint_snapshot), K_(checkpoint_snapshot));
  } else if (OB_FAIL(checkpoint_snapshot_.assign(checkpoint_snapshot))) {
    LOG_WARN("failed to assign replica checkpoint snapshot", KR(ret),
        K(checkpoint_snapshot));
  } else {
    LOG_INFO("update replica checkpoint info", K_(ls_id), K(checkpoint_snapshot));
  }
  return ret;
}

int ObLSReplicaCheckpointCollector::get_checkpoint_snapshot_(
    const palf::LogConfigVersion &config_version,
    ObLSReplicaSCNSnapshot &checkpoint_snapshot) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("failed to lock replica checkpoint collector", KR(ret),
        K(config_version));
  } else if (checkpoint_snapshot_.get_config_version() != config_version) {
    ret = OB_NEED_RETRY;
    LOG_WARN("replica checkpoint collect result is not ready for current config",
        KR(ret), K(config_version), K_(checkpoint_snapshot));
  } else if (OB_UNLIKELY(!checkpoint_snapshot_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached replica checkpoint snapshot is invalid", KR(ret),
        K(config_version), K_(checkpoint_snapshot));
  } else if (OB_FAIL(checkpoint_snapshot.assign(checkpoint_snapshot_))) {
    LOG_WARN("failed to copy replica checkpoint snapshot", KR(ret),
        K(config_version), K_(checkpoint_snapshot));
  }
  return ret;
}

int ObLSReplicaCheckpointCollector::get_majority_min_replica_checkpoint_scn(
    SCN &checkpoint_scn) const
{
  int ret = OB_SUCCESS;
  palf::PalfStat palf_stat;
  ObLSReplicaSCNSnapshot checkpoint_snapshot;
  ObSEArray<SCN, common::OB_MAX_MEMBER_NUMBER> checkpoint_scns;
  checkpoint_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica checkpoint collector is not initialized", KR(ret));
  } else if (OB_FAIL(get_latest_palf_stat_(palf_stat))) {
    LOG_WARN("failed to get latest palf stat", KR(ret), K_(ls_id));
  } else if (OB_UNLIKELY(!palf_stat.is_valid()
      || palf_stat.palf_id_ != ls_id_.id()
      || !palf_stat.config_version_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid palf stat", KR(ret), K_(ls_id), K(palf_stat));
  } else if (!is_strong_leader(palf_stat.role_)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not master, need retry", KR(ret), K_(ls_id), K(palf_stat));
  } else if (OB_FAIL(get_checkpoint_snapshot_(
      palf_stat.config_version_, checkpoint_snapshot))) {
    LOG_WARN("failed to get replica checkpoint snapshot", KR(ret),
        K_(ls_id), "config_version", palf_stat.config_version_);
  } else {
    const ObIArray<ObLSReplicaSCN> &replica_scns = checkpoint_snapshot.get_replica_scns();
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_scns.count(); ++i) {
      if (OB_FAIL(checkpoint_scns.push_back(replica_scns.at(i).get_scn()))) {
        LOG_WARN("failed to copy replica checkpoint scn", KR(ret), K(i), K(checkpoint_snapshot));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc_majority_min_scn(
        checkpoint_snapshot.get_quorum_count(), checkpoint_scns, checkpoint_scn))) {
      LOG_WARN("failed to calc majority min checkpoint scn", KR(ret),
          K_(ls_id), "config_version", palf_stat.config_version_,
          K(checkpoint_snapshot), K(checkpoint_scns));
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
