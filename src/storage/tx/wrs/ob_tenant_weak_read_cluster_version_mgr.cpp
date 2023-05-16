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

#define USING_LOG_PREFIX  TRANS

#include "share/ob_errno.h"
#include "ob_tenant_weak_read_cluster_version_mgr.h"
#include "lib/stat/ob_latch_define.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace transaction
{

ObTenantWeakReadClusterVersionMgr::ObTenantWeakReadClusterVersionMgr() :
    tenant_id_(OB_INVALID_ID),
    svr_array_(),
    rwlock_(common::ObLatchIds::WRS_CLUSTER_VERSION_MGR_LOCK)
{}

ObTenantWeakReadClusterVersionMgr::~ObTenantWeakReadClusterVersionMgr()
{
}

void ObTenantWeakReadClusterVersionMgr::reset(const uint64_t tenant_id)
{
  WLockGuard guard(rwlock_);
  tenant_id_ = tenant_id;
  svr_array_.reset();
}

int64_t ObTenantWeakReadClusterVersionMgr::get_server_count() const
{
  RLockGuard guard(rwlock_);
  return svr_array_.count();
}

bool ObTenantWeakReadClusterVersionMgr::find_match_server(int64_t &pre_count,
     const common::ObAddr &addr,
     ServerInfo *&psi)
{
  int exist = false;
  int64_t i = 0;
  for (i = pre_count; i < svr_array_.count(); i++) {
    if (svr_array_.at(i).match(addr)) {
      psi = &(svr_array_.at(i));
      exist = true;
      break;
    }
  }
  pre_count = i;

  return exist;
}

int ObTenantWeakReadClusterVersionMgr::update_server_version(const common::ObAddr &addr,
    const SCN version,
    const int64_t valid_part_count,
    const int64_t total_part_count,
    const int64_t generate_tstamp,
    bool &is_new_server)
{
  int ret = OB_SUCCESS;
  bool server_exist = false;
  int64_t pre_count = 0;
  ServerInfo *psi = NULL;

  is_new_server = false;

  // read lock
  RLockGuard guard(rwlock_);

  // try to find the server info first
  server_exist = find_match_server(pre_count, addr, psi);

  // insert new if not exist
  if (!server_exist) {
    ServerInfo si(addr, version, valid_part_count, total_part_count);

    // release read lock, do write lock
    if (OB_FAIL(rwlock_.unlock())) {
      LOG_WARN("unlock fail", KR(ret));
    } else if (OB_FAIL(rwlock_.wrlock())) {
      LOG_WARN("write lock fail", KR(ret));
      // not get server on read lock, while other thread push this server
      // double check with write lock
    } else if (!find_match_server(pre_count, addr, psi)) {
      if (OB_FAIL(svr_array_.push_back(si))) {
        LOG_WARN("add new server info entry fail", KR(ret), K(si), K(svr_array_), K(addr), K(version),
            K(valid_part_count), K(total_part_count));
      } else {
        is_new_server = true;
      }
    } else {
      server_exist = true;
    }
  }

  if (OB_SUCC(ret) && server_exist) {
    if (OB_ISNULL(psi)) {
      // should not exist
      LOG_WARN("update server version, NULL pointer", KR(ret), K(addr));
      ret = OB_ERROR;
    } else {
      psi->update(version, valid_part_count, total_part_count, generate_tstamp);
    }
  }

  return ret;
}

SCN ObTenantWeakReadClusterVersionMgr::get_cluster_version(const SCN base_version,
    int64_t &skip_server_count,
    const bool force_print) const
{
  SCN min_version;
  ObSEArray<common::ObAddr, 16> skip_servers;
  bool need_print = force_print;

  RLockGuard guard(rwlock_);

  // scan all server version, get the min version not smaller than base_version
  for (int64_t i = 0; i < svr_array_.count(); i++) {
    bool need_skip = false;
    bool is_first_skipped = false;
    SCN version = svr_array_.at(i).get_version(need_skip, is_first_skipped);

    if (need_skip) {
      skip_servers.push_back(svr_array_.at(i).addr_);
      // if the server is hte first time to be skipped, print INFO
      if (is_first_skipped) {
        need_print = true;
      }
    }
    // if server version >= base_version and is valid
    if (! need_skip && version >= base_version) {
      if (!min_version.is_valid() || min_version > version) {
        min_version = version;
      }
    }
    LOG_DEBUG("[WEAK_READ_SERVER_VERSION_MGR] get cluster version from server", K_(tenant_id), K(i),
        K(need_skip), K(base_version), K(min_version), "server_verion", svr_array_.at(i));
  }

  skip_server_count = skip_servers.count();

  // if no server version in valid, use base_version
  if (!min_version.is_valid()) {
    min_version = base_version;
  }
  if (need_print) {
    LOG_INFO("[WEAK_READ_SERVER_VERSION_MGR] compute version", K_(tenant_id), K(min_version),
	    K(base_version), "server_count", svr_array_.count(), K_(svr_array),
      "skip_server_count", skip_servers.count(), K(skip_servers));
  } else {
    LOG_DEBUG("[WEAK_READ_SERVER_VERSION_MGR] compute version", K_(tenant_id), K(min_version),
	    K(base_version), "server_count", svr_array_.count(), K_(svr_array),
      "skip_server_count", skip_servers.count(), K(skip_servers));
  }

  return min_version;
}

//////////////////////////// ObTenantWeakReadClusterVersionMgr::ServerInfo ///////////////////////////////////

ObTenantWeakReadClusterVersionMgr::ServerInfo::ServerInfo() :
    addr_(),
    version_(),
    valid_part_count_(0),
    total_part_count_(0),
    generate_tstamp_(0),
    is_skipped_(false),
    lock_()
{
}

ObTenantWeakReadClusterVersionMgr::ServerInfo::ServerInfo(const ObAddr &addr,
    const SCN version,
    const int64_t valid_part_count,
    const int64_t total_part_count)
{
  addr_ = addr;
  version_ = version;
  valid_part_count_ = valid_part_count;
  total_part_count_ = total_part_count;
  generate_tstamp_ = ObTimeUtility::current_time();
  is_skipped_ = false;
}

bool ObTenantWeakReadClusterVersionMgr::ServerInfo::match(const common::ObAddr &addr) const
{
  SpinLockGuard guard(lock_);
  return addr_ == addr;
}

void ObTenantWeakReadClusterVersionMgr::ServerInfo::update(const SCN version,
    const int64_t valid_part_count,
    const int64_t total_part_count,
    const int64_t generate_tstamp)
{
  SpinLockGuard guard(lock_);

  // update heartbeat generation timestamp no matter server version changes or not
  if (version >= version_) {
    version_ = version;
    valid_part_count_ = valid_part_count;
    total_part_count_ = total_part_count;
    generate_tstamp_ = std::max(generate_tstamp, generate_tstamp_);
  }
}

SCN ObTenantWeakReadClusterVersionMgr::ServerInfo::get_version(bool &need_skip,
    bool &is_first_skipped) const
{
  int64_t cur_tstamp = ObTimeUtility::current_time();
  SCN ret_version;

  SpinLockGuard guard(lock_);

  is_first_skipped = false;
  need_skip = false;
  ret_version = version_;

  // skip server if it has no valid partition or no heartbeat received from it for a long time
  if (0 == valid_part_count_
      || (cur_tstamp - generate_tstamp_) > MAX_SERVER_ALIVE_HEARTBEAT_INTERVAL) {
    need_skip = true;
    // 判断是否是第一次need_skip
    if (! is_skipped_) {
      is_first_skipped = true;
    }
    is_skipped_ = true;
  } else {
    need_skip = false;
    is_skipped_ = false;
  }

  return ret_version;
}

}
}
