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

#define USING_LOG_PREFIX TRANS

#include "share/ob_errno.h"
#include "share/ob_define.h"      // is_valid_read_snapshot_version
#include "ob_tenant_weak_read_server_version_mgr.h"
#include "lib/stat/ob_latch_define.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace transaction
{

ObTenantWeakReadServerVersionMgr::ObTenantWeakReadServerVersionMgr() :
    server_version_(),
    rwlock_(common::ObLatchIds::WRS_SERVER_VERSION_LOCK),
    server_version_for_stat_()
{}

ObTenantWeakReadServerVersionMgr::~ObTenantWeakReadServerVersionMgr()
{}

SCN ObTenantWeakReadServerVersionMgr::get_version() const
{
  SCN ret_version;
  ServerVersion sv;

  get_version(sv);

  ret_version = sv.version_;
  return ret_version;
}

SCN ObTenantWeakReadServerVersionMgr::get_version(int64_t &total_part_count,
	int64_t &valid_part_count) const
{
  ServerVersion sv;

  get_version(sv);

  total_part_count = sv.total_part_count_;
  valid_part_count = sv.valid_inner_part_count_  + sv.valid_user_part_count_;

  return sv.version_;
}

void ObTenantWeakReadServerVersionMgr::get_version(ServerVersion &sv) const
{
  SpinRLockGuard guard(rwlock_);
  sv.reset(server_version_.version_,
      server_version_.total_part_count_,
      server_version_.valid_inner_part_count_,
      server_version_.valid_user_part_count_);
}

int ObTenantWeakReadServerVersionMgr::update_with_part_info(const uint64_t tenant_id,
    const int64_t epoch_tstamp,
    const bool need_skip,
    const bool is_user_part,
    const SCN version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(epoch_tstamp <= 0)
      || OB_UNLIKELY(! need_skip && !version.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, epoch or version", K(ret), K(tenant_id),
        K(epoch_tstamp), K(need_skip), K(is_user_part), K(version));
  } else {
    // NOTE: single thread update, no need lock
    server_version_for_stat_.update_with_part_info(epoch_tstamp, need_skip, is_user_part, version);
  }
  return ret;
}

int ObTenantWeakReadServerVersionMgr::generate_new_version(const uint64_t tenant_id,
    const int64_t epoch_tstamp,
    const SCN base_version_when_no_valid_partition,
    const bool need_print_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(epoch_tstamp <= 0)
      || OB_UNLIKELY(OB_INVALID_ID == tenant_id)
      || OB_UNLIKELY(!base_version_when_no_valid_partition.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(epoch_tstamp), K(tenant_id),
        K(base_version_when_no_valid_partition));
  }
  // amend server version stat without lock
  else if (OB_FAIL(server_version_for_stat_.amend(tenant_id, epoch_tstamp,
      base_version_when_no_valid_partition))) {
    LOG_WARN("amend server version fail", KR(ret), K(tenant_id), K(epoch_tstamp),
        K(base_version_when_no_valid_partition), K(server_version_for_stat_));
  } else {
    // write lock
    SpinWLockGuard guard(rwlock_);

    LOG_DEBUG("[WRS] update tenant weak read server version", K(tenant_id), "old_version", server_version_,
        "new_version", server_version_for_stat_);

    // update SERVER version with write lock
    if (OB_FAIL(server_version_.update(server_version_for_stat_))) {
      LOG_WARN("update server version fail", KR(ret), K(server_version_for_stat_),
          K(server_version_), K(tenant_id), K(epoch_tstamp), K(base_version_when_no_valid_partition));
    } else {
      if (need_print_status) {
        LOG_INFO("[WRS] update tenant weak read server version", K(tenant_id), K_(server_version),
            "version_delta", ObTimeUtility::current_time() - server_version_.version_.convert_to_ts());
      }
    }
  }
  return ret;
}

/////////////////// ObTenantWeakReadServerVersionMgr::ServerVersionInner /////////////////////////


void ObTenantWeakReadServerVersionMgr::ServerVersionInner::update_with_part_info(const int64_t epoch_tstamp,
    const bool need_skip,
    const bool is_user_part,
    const SCN version)
{
  if (epoch_tstamp != epoch_tstamp_) {
    // new epoch statistic start
    reset();
    epoch_tstamp_ = epoch_tstamp;
  }

  if (! need_skip) {
    SCN &target_version = version_;
    int64_t &target_valid_count = is_user_part ? valid_user_part_count_ : valid_inner_part_count_;
    // update target version
    if (!target_version.is_valid()) {
      target_version = version;
    } else {
      if (target_version > version) {
        target_version = version;
      }
    }
    // target valid count + 1
    target_valid_count++;
  }

  // update total partition count
  total_part_count_++;
}

// scan all partitions of the tenant finished, amend tenant server level weak read version
// generate weak read version for tenant without partitions in local server
int ObTenantWeakReadServerVersionMgr::ServerVersionInner::amend(const uint64_t tenant_id,
    const int64_t new_epoch_tstamp,
    const SCN base_version_when_no_valid_partition)
{
  int ret = OB_SUCCESS;
  static const int64_t PRINT_INTERVAL = 10 * 1000 * 1000;

  // tenant had no partition
  if (0 == epoch_tstamp_ || epoch_tstamp_ < new_epoch_tstamp) {
    // reset statistic result
    reset();
    epoch_tstamp_ = new_epoch_tstamp;
  }
  // server epoch timestamp should not bigger than new epoch timestamp
  else if (OB_UNLIKELY(epoch_tstamp_ > new_epoch_tstamp)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local epoch timestamp is greater than new epoch timestmap", K(tenant_id),
        K(epoch_tstamp_), K(new_epoch_tstamp));
  }

  if (OB_SUCCESS == ret) {
    // no valid partition
    if (0 == (valid_inner_part_count_ + valid_user_part_count_)) {
      // generate weak read version with base version
      version_ = base_version_when_no_valid_partition;

      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        LOG_INFO("[WRS] tenant has no valid partition on current server, "
            "generate weak read snapshot version based on current timestamp",
            K(tenant_id), K(epoch_tstamp_), K(total_part_count_), K(valid_inner_part_count_),
            K(valid_user_part_count_), K(version_), K(base_version_when_no_valid_partition));
      }
    } else if (!version_.is_valid()) {
      LOG_WARN("version is invalid while valid partition count is not zero, unexpected",
          K(valid_inner_part_count_), K(valid_user_part_count_), K(version_), K(epoch_tstamp_), K(total_part_count_));
      ret = OB_ERR_UNEXPECTED;
    }

  }
  return ret;
}

int ObTenantWeakReadServerVersionMgr::ServerVersionInner::update(const ServerVersionInner &new_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == new_version.epoch_tstamp_)
      || OB_UNLIKELY(epoch_tstamp_ >= new_version.epoch_tstamp_)
      || OB_UNLIKELY(!new_version.version_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid new server version", KR(ret), K(new_version), KPC(this));
  } else {
    epoch_tstamp_ = new_version.epoch_tstamp_;
    total_part_count_ = new_version.total_part_count_;
    valid_inner_part_count_ = new_version.valid_inner_part_count_;
    valid_user_part_count_ = new_version.valid_user_part_count_;

    // version should incease monotonically
    version_ = SCN::max(new_version.version_, version_);
  }
  return ret;
}
}
}
