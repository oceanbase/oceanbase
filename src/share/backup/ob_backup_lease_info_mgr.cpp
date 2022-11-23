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

#include "lib/ob_define.h"
#define USING_LOG_PREFIX RS
#include "ob_backup_lease_info_mgr.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "rootserver/ob_root_service.h"
#include "observer/ob_server_struct.h"
#include "share/ob_server_status.h"
#include "share/backup/ob_backup_manager.h"
#include "share/ob_cluster_version.h"
#include "logservice/ob_log_service.h"
#include "share/backup/ob_backup_operator.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace storage;

namespace share
{
ObBackupLeaseInfo::ObBackupLeaseInfo()
  : is_leader_(false),
    lease_start_ts_(0),
    leader_epoch_(0),
    leader_takeover_ts_(0),
    round_(0),
    lease_epoch_(0)
{
}

void ObBackupLeaseInfo::reset()
{
  is_leader_ = false;
  lease_start_ts_ = 0;
  leader_epoch_ = 0;
  leader_takeover_ts_ = 0 ;
  round_ = 0;
}

bool ObBackupLeaseInfo::is_valid() const
{
  bool valid = true;

  if (round_ < 0 || lease_epoch_ < 0) {
    valid = false;
  } else if (is_leader_) {
    if (lease_start_ts_ < 0 || leader_epoch_ < 0 || leader_takeover_ts_ < 0) {
      valid = false;
    }
  } else {
    if (lease_start_ts_ != 0 || leader_epoch_ != 0 || leader_takeover_ts_ != 0) {
      valid = false;
    }
  }
  return valid;
}

int ObBackupLeaseInfo::set_new_lease(
    const int64_t start_ts, const int64_t epoch,
    const int64_t takeover_ts, const int64_t round,
    const int64_t lease_epoch)
{
  int ret = OB_SUCCESS;

  if (round < round_ || start_ts < lease_start_ts_
      || epoch < leader_epoch_ || takeover_ts < leader_takeover_ts_
      || round <= 0 || lease_epoch_ < 0) {
     ret = OB_INVALID_ARGUMENT;
     LOG_WARN("invalid args", K(ret), K(start_ts), K(epoch), K(takeover_ts), K(round), K(*this));
  } else {
    reset();
    is_leader_ = true;
    lease_start_ts_ = start_ts;
    leader_epoch_ = epoch;
    leader_takeover_ts_ = takeover_ts;
    round_ = round;
    lease_epoch_ = lease_epoch;

    if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lease info", K(ret), K(*this));
    }
  }
  return ret;
}

int ObBackupLeaseInfo::release_lease(const int64_t round)
{
  int ret = OB_SUCCESS;

  if (round < round_ || round <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid round", K(ret), K(round));
  } else {
    reset();
    round_ = round;
  }
  return ret;
}

int ObBackupLeaseInfo::update_lease_start_ts(const int64_t now_ts)
{
  int ret = OB_SUCCESS;

  if (now_ts <= lease_start_ts_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(now_ts), K(*this));
  } else {
    lease_start_ts_ = now_ts;
    if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lease info", K(ret), K(*this));
    }
  }
  return ret;
}

ObBackupLeaseInfoMgr::ObBackupLeaseInfoMgr()
  : is_inited_(false),
    local_addr_(),
    sql_proxy_(NULL)
{
}

ObBackupLeaseInfoMgr::~ObBackupLeaseInfoMgr()
{
}

int ObBackupLeaseInfoMgr::init(
    const ObAddr &addr,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup lease info mgr init twice", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup lease info mgr get invalid argument", K(ret), K(addr));
  } else {
    local_addr_ = addr;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLeaseInfoMgr::clean_backup_lease_info(
    const int64_t next_round, const ObBackupLeaseInfo &old_lease_info,
    ObBackupLeaseInfo &new_lease_info)
{
  int ret = OB_SUCCESS;
  new_lease_info = old_lease_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (next_round < 0 || !old_lease_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(next_round), K(old_lease_info));
  } else if (OB_FAIL(new_lease_info.release_lease(next_round))) {
    LOG_WARN("failed to release lease", K(ret), K(next_round));
  } else if (OB_FAIL(clean_backup_scheduler_leadear_())) {
    LOG_WARN("failed to clean backup scheduler leader", K(ret));
  }

  return ret;
}

int ObBackupLeaseInfoMgr::renew_lease(const int64_t can_be_leader_ts,
    const int64_t next_round,
    const ObBackupLeaseInfo &old_lease_info,
    ObBackupLeaseInfo &new_lease_info,
    const char *&msg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup lease info mgr do not init", K(ret));
  } else if (can_be_leader_ts < 0 || next_round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(can_be_leader_ts), K(next_round));
  } else {
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      ObRole role = FOLLOWER;
      int64_t proposal_id = OB_INVALID_TIMESTAMP;
      int64_t takeover_time = OB_INVALID_TIMESTAMP;
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(ObLSID(SYS_LS), role, proposal_id))) {
        LOG_WARN("get palf role failed", KR(ret));
      } else if (FALSE_IT(takeover_time = proposal_id)) {
        // TODO: fix me later
      } else if (!is_strong_leader(role)) {
        ret = OB_LEADER_NOT_EXIST;
        LOG_WARN("not stronge leader, cannot renew backup lease", K(ret), K(role),
            K(proposal_id), K(takeover_time));
      } else if (OB_FAIL(do_renew_lase_(proposal_id, takeover_time,
          can_be_leader_ts, next_round, old_lease_info, new_lease_info, msg))) {
          LOG_WARN("failed to check can backup", K(ret), K(role));
      }
    }
  }
  return ret;
}

int ObBackupLeaseInfoMgr::get_backup_scheduler_leader_(
    common::ObISQLClient &sql_client,
    common::ObAddr &backup_leader_addr,
    bool &has_leader,
    int64_t &lease_epoch)
{
  int ret = OB_SUCCESS;
  bool is_compat = false;
  if (OB_FAIL(get_backup_scheduler_leader_v2_(sql_client, backup_leader_addr, has_leader, lease_epoch))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get_backup_scheduler_leader_v2", K(ret));
    } else if (OB_FAIL(get_backup_scheduler_leader_v1_(sql_client, backup_leader_addr, has_leader))) {
      LOG_WARN("failed to get_backup_scheduler_leader_v1", K(ret));
    } else {
      lease_epoch = 0;
      is_compat = true;
    }
  }
  LOG_INFO("succeed to get backup scheduler leader", K(ret), K(backup_leader_addr), K(is_compat), K(has_leader), K(lease_epoch));
  return ret;
}

int ObBackupLeaseInfoMgr::get_backup_scheduler_leader_v1_(
    common::ObISQLClient &sql_client,
    common::ObAddr &scheduler_leader_addr,
    bool &has_leader)
{
  int ret = OB_SUCCESS;
  scheduler_leader_addr.reset();
  ObBackupInfoManager info_manager;
  ObArray<uint64_t> tenant_ids;

  has_leader = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup lease info mgr do not init", K(ret));
  } else if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to push tenant id into array", K(ret));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(info_manager.get_backup_scheduler_leader(OB_SYS_TENANT_ID, sql_client,
      scheduler_leader_addr, has_leader))) {
    LOG_WARN("failed to get backup scheduler leader", K(ret));
  }
  return ret;
}

int ObBackupLeaseInfoMgr::get_backup_scheduler_leader_v2_(
    common::ObISQLClient &sql_client,
    common::ObAddr &lease_leader,
    bool &has_leader,
    int64_t &lease_epoch)
{
  int ret = OB_SUCCESS;
  const bool for_update = false;
  lease_leader.reset();
  has_leader = true;
  lease_epoch = 0;
  if (OB_FAIL(ObBackupInfoOperator::get_backup_leader(sql_client, for_update, lease_leader, has_leader))) {
    LOG_WARN("Failed to get backup leader", K(ret));
  } else if (OB_FAIL(ObBackupInfoOperator::get_backup_leader_epoch(sql_client, for_update, lease_epoch))) {
    LOG_WARN("failed to get backup leader epoch", K(ret));
  }
  return ret;
}

int ObBackupLeaseInfoMgr::update_backup_scheduler_leader_(
    const common::ObAddr &lease_leader,
    int64_t &lease_epoch,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const uint64_t cluster_observer_version = GET_MIN_CLUSTER_VERSION();
  common::ObClusterVersion version;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!lease_leader.is_valid() || lease_epoch < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(lease_leader), K(lease_epoch));
  } else if (OB_FAIL(version.init(cluster_observer_version))) {
    LOG_WARN("failed to init version", K(ret), K(cluster_observer_version));
  } else if (cluster_observer_version < CLUSTER_VERSION_2277
      || (cluster_observer_version >= CLUSTER_VERSION_3000 && cluster_observer_version <= CLUSTER_VERSION_3100)) {
    if (lease_epoch > 0) {
      ret = OB_ERR_SYS;
      LOG_ERROR("for old version, lease epoch must be 0", K(ret), K(lease_leader), K(lease_epoch), K(version));
    } else if (OB_FAIL(update_backup_scheduler_leader_v1_(lease_leader, sql_client))) {
      LOG_WARN("failed to update_backup_scheduler_leader_v1_", K(ret), K(lease_leader));
    }
  } else {
    int64_t new_lease_epoch = lease_epoch + 1;
    if (OB_FAIL(update_backup_scheduler_leader_v2_(lease_leader, new_lease_epoch, sql_client))) {
      LOG_WARN("failed to update_backup_scheduler_leader_v2_", K(ret), K(lease_leader), K(new_lease_epoch));
    } else {
      lease_epoch = new_lease_epoch;
    }
  }
  FLOG_INFO("update_backup_scheduler_leader_", K(ret), K(cluster_observer_version), K(version), K(lease_leader), K(lease_epoch));
  return ret;
}

int ObBackupLeaseInfoMgr::update_backup_scheduler_leader_v1_(
    const ObAddr &scheduler_leader_addr,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObBackupInfoManager info_manager;
  ObArray<uint64_t> tenant_ids;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup lease info mgr do not init", K(ret));
  } else if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to push tenant id into array", K(ret));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(info_manager.update_backup_scheduler_leader(OB_SYS_TENANT_ID, scheduler_leader_addr, sql_client))) {
    LOG_WARN("failed to get backup scheduler leader", K(ret), K(scheduler_leader_addr));
  } else {
    FLOG_INFO("succeed to update backup scheduler leader", K(scheduler_leader_addr));
  }
  return ret;
}

int ObBackupLeaseInfoMgr::update_backup_scheduler_leader_v2_(
    const common::ObAddr &lease_leader,
    const int64_t lease_epoch,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  int64_t got_lease_epoch = 0;
  common::ObAddr got_leader;
  bool has_leader = true;
  const bool for_update = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!lease_leader.is_valid() || lease_epoch < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(lease_leader), K(lease_epoch));
  } else if (OB_FAIL(ObBackupInfoOperator::get_backup_leader(sql_client, for_update, got_leader, has_leader))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get backup leader", K(ret), K(got_leader), K(has_leader));
    } else {
      ret = OB_SUCCESS;
      FLOG_INFO("backup leader not exist, just update it", K(ret), K(lease_leader), K(lease_epoch));
    }
  } else if (OB_FAIL(ObBackupInfoOperator::get_backup_leader_epoch(sql_client, for_update, got_lease_epoch))) {
    LOG_WARN("failed to get backup leader epoch", K(ret));
  } else if (0 == got_lease_epoch && 0 == lease_epoch) {
    // compat mode
  } else if (got_lease_epoch + 1 != lease_epoch) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot set backup leader with invalid lease epoch", K(ret), K(has_leader), K(lease_epoch), K(got_lease_epoch));
  }
  if (FAILEDx(ObBackupInfoOperator::set_backup_leader_epoch(sql_client, lease_epoch))) {
    LOG_WARN("failed to set backup leader epoch", K(ret), K(lease_leader), K(lease_epoch));
  } else if (OB_FAIL(ObBackupInfoOperator::set_backup_leader(sql_client, lease_leader))) {
    LOG_WARN("Failed set backup leader", K(ret), K(lease_leader));
  }

  return ret;
}


int ObBackupLeaseInfoMgr::clean_backup_scheduler_leadear_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(clean_backup_scheduler_leadear_v1_())) {
   LOG_WARN("Failed to clean_backup_scheduler_leadear_v1", K(ret));
  } else if (OB_FAIL(clean_backup_scheduler_leadear_v2_())) {
    LOG_WARN("Failed to clean_backup_scheduler_leadear_v2", K(ret));
  }
  FLOG_INFO("clean_backup_scheduler_leadear_", K(ret), K(local_addr_));
  return ret;
}

int ObBackupLeaseInfoMgr::clean_backup_scheduler_leadear_v1_()
{
  int ret = OB_SUCCESS;
  ObBackupInfoManager info_manager;
  ObArray<uint64_t> tenant_ids;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup lease info mgr do not init", K(ret));
  } else if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to push tenant id into array", K(ret));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(info_manager.clean_backup_scheduler_leader(OB_SYS_TENANT_ID, local_addr_))) {
    LOG_WARN("failed to clean backup scheduler leader", K(ret), K(local_addr_));
  } else {
    FLOG_INFO("succeed to clean backup scheduler leader", K(local_addr_));
  }
  return ret;
}

int ObBackupLeaseInfoMgr::clean_backup_scheduler_leadear_v2_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupInfoOperator::clean_backup_leader(*sql_proxy_, local_addr_))) {
    LOG_WARN("Failed to clean backup leader", K(ret));
  }
  return ret;
}

int ObBackupLeaseInfoMgr::do_renew_lase_(
    const int64_t leader_epoch,
    const int64_t takeover_time,
    const int64_t can_be_leader_ts,
    const int64_t next_round,
    const ObBackupLeaseInfo &old_lease_info,
    ObBackupLeaseInfo &new_lease_info,
    const char *&msg)
{
  int ret = OB_SUCCESS;
  int64_t now_ts = ObTimeUtil::current_time();
  new_lease_info.reset();
  msg = "";
  int64_t max_lease_takeover_time = ObBackupLeaseInfo::MAX_LEASE_TAKEOVER_TIME;

#ifdef ERRSIM
      max_lease_takeover_time = ObServerConfig::get_instance().backup_lease_takeover_time;
#endif
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup lease info mgr do not init", K(ret));
  } else if (leader_epoch < 0 || now_ts < 0 || takeover_time < 0
      || can_be_leader_ts < 0 || next_round < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(leader_epoch), K(now_ts), K(takeover_time));
  } else {
    if (old_lease_info.is_leader_
        && old_lease_info.leader_epoch_ == leader_epoch
        && old_lease_info.leader_takeover_ts_ == takeover_time) {
      new_lease_info = old_lease_info;
      if (OB_FAIL(new_lease_info.update_lease_start_ts(now_ts))) {
        LOG_WARN("failed to update lease start ts", K(ret));
      }
    } else {
      ObMySQLTransaction trans;
      ObAddr backup_scheduler_leader;
      bool has_leader = true;
      int64_t lease_epoch = 0;

      if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        if (OB_FAIL(get_backup_scheduler_leader_(trans, backup_scheduler_leader, has_leader, lease_epoch))) {
          LOG_WARN("failed to get backup scheduler leader", K(ret));
        } else if (!has_leader) {
          // first time as leader, if the lease epoch is 0, then we donot use multi-version small files.
          // Otherwise, lease epoch is bigger than 0.
          int64_t new_lease_epoch = lease_epoch;
          // update_backup_scheduler_leader_ will increase the lease epoch if needed.
          if (OB_FAIL(update_backup_scheduler_leader_(local_addr_, new_lease_epoch, trans))) {
            LOG_WARN("failed to update backup scheduler leader", K(ret), K(local_addr_), K(new_lease_epoch));
          } else {
            backup_scheduler_leader = local_addr_;
            lease_epoch = new_lease_epoch;
          }
        }

        if (OB_FAIL(ret)) {
        } else {
          if (local_addr_ == backup_scheduler_leader) {
            if (OB_FAIL(new_lease_info.set_new_lease(now_ts, leader_epoch,
                takeover_time, next_round, lease_epoch))) {
              LOG_WARN("failed to set new lease info", K(ret));
            } else {
              msg = "got backup lease fast";
              FLOG_INFO(msg, K(now_ts), K(new_lease_info));
            }
          } else if (takeover_time + max_lease_takeover_time < now_ts
              && can_be_leader_ts + max_lease_takeover_time < now_ts) {
            // Leader switch, I am the leader.
            int64_t new_lease_epoch = lease_epoch;
            // update_backup_scheduler_leader_ will increase the lease epoch if needed.
            if (OB_FAIL(update_backup_scheduler_leader_(local_addr_, new_lease_epoch, trans))) {
              LOG_WARN("failed to update backup scheduler leader", K(ret), K(local_addr_));
            } else if (OB_FAIL(new_lease_info.set_new_lease(now_ts, leader_epoch,
                takeover_time, next_round, new_lease_epoch))) {
              LOG_WARN("failed to set new lease info", K(ret));
            } else {
              msg = "got backup lease after old lease expired";
              FLOG_INFO(msg, K(now_ts), K(new_lease_info));
            }
          } else { // not own lease
            if (OB_FAIL(new_lease_info.release_lease(next_round))) {
              LOG_WARN("failed to release lease", K(ret), K(next_round));
            } else {
              const int64_t need_wait_ts = std::max(
                  max_lease_takeover_time + takeover_time - now_ts,
                  max_lease_takeover_time + can_be_leader_ts - now_ts);
              LOG_INFO("not own lease now, need wait", K(takeover_time), K(can_be_leader_ts),
                  K(now_ts), K(need_wait_ts), K(backup_scheduler_leader));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            new_lease_info.reset();
            LOG_WARN("failed to commit backup scheduler leader", K(ret), K(new_lease_info));
          } else if (new_lease_info.is_leader_) {
            FLOG_INFO("succeed to update backup scheduler leader", K(ret), K_(local_addr),
                K(new_lease_info), K(msg));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
            OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObFakeBackupLeaseService::check_lease()
{
  return OB_SUCCESS;
}

int ObFakeBackupLeaseService::get_lease_status(bool &is_lease_valid)
{
  is_lease_valid = true;
  return OB_SUCCESS;
}

int64_t ObFakeBackupLeaseService::get_lease_version() const
{
  return INT64_MAX;
}
} //share
} //oceanbase
