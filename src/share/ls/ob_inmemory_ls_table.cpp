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

#define USING_LOG_PREFIX SHARE_PT

#include "ob_inmemory_ls_table.h"    // for ObInMemoryLSTable's functions
#include "lib/container/ob_se_array.h"       // for array-related functions: at(), count()

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share;

ObInMemoryLSTable::ObInMemoryLSTable()
  : ObLSTable(),
    inited_(false),
    ls_info_(OB_SYS_TENANT_ID, SYS_LS),
    lock_(),
    rs_list_change_cb_(NULL)
{
}

ObInMemoryLSTable::~ObInMemoryLSTable()
{
}

int ObInMemoryLSTable::init(ObIRsListChangeCb &rs_list_change_cb)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    rs_list_change_cb_ = &rs_list_change_cb;
    inited_ = true;
  }
  return ret;
}

void ObInMemoryLSTable::reuse()
{
  WLockGuard lock_guard(lock_);
  ls_info_.get_replicas().reuse();
}

// get ls_info from memory directly
int ObInMemoryLSTable::get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObLSTable::Mode mode,
    ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  int64_t local_cluster_id = GCONF.cluster_id;
  RLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_CLUSTER_ID == cluster_id
             || local_cluster_id != cluster_id
             || ObLSTable::INNER_TABLE_ONLY_MODE == mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get through inmemory with invalid cluster_id or not local cluster_id or invalid mode",
             KR(ret), K(cluster_id), K(local_cluster_id), K(mode));
  } else if (!is_sys_tenant(tenant_id) || !ls_id.is_sys_ls()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for sys tenant's sys ls", KR(ret), KT(tenant_id), K(ls_id));
  } else {
    const bool filter_flag_replica = false;
    if (OB_FAIL(inner_get_(tenant_id, ls_id, filter_flag_replica, ls_info))) {
      LOG_WARN("inner_get failed", KR(ret), KT(tenant_id), K(ls_id), K(filter_flag_replica));
    }
  }
  return ret;
}

// get each replica from ls_info_ and deal with flag replica
int ObInMemoryLSTable::inner_get_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const bool filter_flag_replica,
    ObLSInfo &ls_info)
{
  UNUSED(filter_flag_replica);

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_sys_tenant(tenant_id) || !ls_id.is_sys_ls()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for sys tenant's sys ls", KR(ret), KT(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_info.init(tenant_id, ls_id, ls_info_.get_replicas()))) {
    LOG_WARN("fail to init ls_info", KR(ret), K(tenant_id), K(ls_info_));
  }
  return ret;
}

int ObInMemoryLSTable::update(
    const ObLSReplica &replica,
    const bool inner_table_only)
{
  int ret = OB_SUCCESS;
  WLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!replica.is_valid()
      || !is_sys_tenant(replica.get_tenant_id())
      || !replica.get_ls_id().is_sys_ls()
      || inner_table_only) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica), "tenant_id", replica.get_tenant_id(),
             "ls_id", replica.get_ls_id().id(), K(inner_table_only));
  } else if (replica.is_strong_leader()) {
    // TODO: try to short the logic of update_leader/follower_replica
    if (OB_FAIL(update_leader_replica_(replica))) {
      LOG_WARN("update leader replica failed", KR(ret), K(replica));
    }
  } else {
    if (OB_FAIL(update_follower_replica_(replica))) {
      LOG_WARN("update follower replica failed", KR(ret), K(replica));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_info_.update_replica_status())) {// TODO: make sure the actions of update_replica_status()
      LOG_WARN("update replica type failed", KR(ret), K_(ls_info));
    } else if (OB_FAIL(rs_list_change_cb_->submit_update_rslist_task())) {
      LOG_WARN("submit_update_rslist_task failed", KR(ret));
    }
  }
  LOG_INFO("update ls replica", KR(ret), K(replica), K_(ls_info));

  return ret;
}

int ObInMemoryLSTable::update_follower_replica_(const ObLSReplica &replica)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!replica.is_valid()
      || !is_sys_tenant(replica.get_tenant_id())
      || !replica.get_ls_id().is_sys_ls()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica));
  } else {
    ObLSReplica new_replica;
    if (OB_FAIL(new_replica.assign(replica))) {
      LOG_WARN("failed to assign new_replica", KR(ret), K(replica));
    } else if (FALSE_IT(new_replica.update_to_follower_role())) {
      // shall never be here
    } else if (OB_FAIL(ls_info_.add_replica(new_replica))) {
      LOG_WARN("add replica failed", KR(ret), K(replica));
    } else {
      LOG_INFO("add follower replica for sys ls", K(new_replica));
    }
  }
  LOG_INFO("inmemory ls table add replica", KR(ret), K(replica));
  return ret;
}

int ObInMemoryLSTable::update_leader_replica_(const ObLSReplica &replica)
{
  int ret = OB_SUCCESS;
  common::ObRole role = FOLLOWER;
  int64_t proposal_id = 0;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!replica.is_valid()
      || !replica.is_strong_leader()
      || !is_sys_tenant(replica.get_tenant_id())
      || !replica.get_ls_id().is_sys_ls()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica));
  } else if (GCONF.self_addr_ != replica.get_server()) {
    ret = OB_NOT_MASTER;
    LOG_WARN("get invalid replica info, leader should be self", "self", GCONF.self_addr_, K(replica));
  } else if (OB_FAIL(get_role(OB_SYS_TENANT_ID, SYS_LS, role))) {
    LOG_WARN("get role from ObLS failed", KR(ret));
  } else if (!is_strong_leader(role)) {
    if (OB_FAIL(update_follower_replica_(replica))) {
      LOG_WARN("update follower replica failed", KR(ret), K(replica));
    }
  } else {
    int64_t max_proposal_id = 0;
    ObLSInfo::ReplicaArray &all_replicas = ls_info_.get_replicas();
    for (int64_t i = 0; OB_SUCC(ret) && i < all_replicas.count(); ++i) {
      if ((all_replicas.at(i).is_strong_leader())
          && all_replicas.at(i).get_server() != replica.get_server()) {
        LOG_INFO("update replica role to FOLLOWER", "replica", all_replicas.at(i));
        all_replicas.at(i).set_role(FOLLOWER);
      }
      if (max_proposal_id < all_replicas.at(i).get_proposal_id()) {
        max_proposal_id = all_replicas.at(i).get_proposal_id();
      }
    } //end for
    if (OB_SUCC(ret)) {
      if (max_proposal_id > replica.get_proposal_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid replica info. leader should the max proposal_id", KR(ret), K(replica), K(ls_info_));
      }
    }

    if (OB_SUCC(ret)) {
      ObLSReplica new_replica;
      if (OB_FAIL(new_replica.assign(replica))) {
        LOG_WARN("failed to assign new_replica", KR(ret), K(replica));
      } else {
        new_replica.set_modify_time_us(ObTimeUtility::current_time());
        if (OB_FAIL(ls_info_.add_replica(new_replica))) {
          LOG_WARN("add replica failed", KR(ret), K(replica));
        } else {
          LOG_INFO("add leader replica for sys ls",
                   K(new_replica), K(ls_info_));
        }
      }
    }
  }
  return ret;
}

int ObInMemoryLSTable::remove(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObAddr &server,
    const bool inner_table_only)
{
  int ret = OB_SUCCESS;
  WLockGuard lock_guard(lock_);
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(rs_list_change_cb_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(tenant_id)
      || !ls_id.is_sys_ls()
      || !server.is_valid())
      || inner_table_only) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove with invalid argument", KR(ret),
             K(tenant_id), K(ls_id), K(server), K(inner_table_only));
  } else if (OB_FAIL(ls_info_.remove(server))) {
    LOG_WARN("remove server replica failed", KR(ret), K(server), K_(ls_info));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_info_.update_replica_status())) {
      LOG_WARN("update replica type failed", KR(ret), K_(ls_info));
    } else if (OB_FAIL(rs_list_change_cb_->submit_update_rslist_task())) {
      LOG_WARN("submit_update_rslist_task failed", KR(ret));
    }
  }
  // TODO:check_leader() if necessary
  return ret;
}

} // end namespace share
} // end namespace oceanbase
