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

#define USING_LOG_PREFIX RS

#include "ob_update_rs_list_task.h"

#include "lib/profile/ob_trace_id.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_root_addr_agent.h"
#include "share/ob_debug_sync.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_root_service.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace rootserver {
using namespace common;
using namespace share;

ObUpdateRsListTask::ObUpdateRsListTask()
    : inited_(false),
      pt_operator_(NULL),
      root_addr_agent_(NULL),
      server_mgr_(NULL),
      zone_mgr_(NULL),
      lock_(NULL),
      force_update_(false),
      self_addr_()
{}

ObUpdateRsListTask::~ObUpdateRsListTask()
{}

volatile int64_t ObUpdateRsListTask::g_wait_cnt_ = 0;

bool ObUpdateRsListTask::try_lock()
{
  int64_t cnt = ATOMIC_AAF(&g_wait_cnt_, 1);
  if (1 != cnt) {
    if (cnt < 0) {
      LOG_ERROR("unexpected waiting task cnt", K(cnt));
    } else {
      LOG_INFO("update rslist task exist, do not submit again", K(cnt));
    }
    ATOMIC_DEC(&g_wait_cnt_);
  }
  return cnt == 1;
}

void ObUpdateRsListTask::unlock()
{
  ATOMIC_DEC(&g_wait_cnt_);
}

void ObUpdateRsListTask::clear_lock()
{
  ATOMIC_SET(&g_wait_cnt_, 0);
}

int ObUpdateRsListTask::init(ObPartitionTableOperator& pt_operator, ObRootAddrAgent* agent, ObServerManager& server_mgr,
    ObZoneManager& zone_mgr, SpinRWLock& lock, const bool force_update, const common::ObAddr& self_addr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == agent) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("agent is null", KP(agent), K(ret));
  } else if (!agent->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("agent is invalid", "agent valid", agent->is_valid(), K(ret));
  } else {
    pt_operator_ = &pt_operator;
    root_addr_agent_ = agent;
    server_mgr_ = &server_mgr;
    zone_mgr_ = &zone_mgr;
    lock_ = &lock;
    force_update_ = force_update;
    self_addr_ = self_addr;
    inited_ = true;
  }
  return ret;
}

int ObUpdateRsListTask::process_without_lock()
{
  int ret = OB_SUCCESS;
  bool rs_list_diff_member_list = false;
  bool inner_need_update = false;
  LOG_INFO("start to process rs list update task");

  DEBUG_SYNC(HANG_UPDATE_RS_LIST);
  ObSEArray<ObRootAddr, ObPartitionReplica::DEFAULT_REPLICA_COUNT> new_rs_list;
  ObSEArray<ObRootAddr, ObPartitionReplica::DEFAULT_REPLICA_COUNT> new_readonly_rs_list;
  const common::ObClusterType cluster_type = common::PRIMARY_CLUSTER;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(pt_operator_) || OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is null", KP(pt_operator_), KP(server_mgr_));
  } else if (OB_FAIL(get_rs_list(*pt_operator_,
                 *server_mgr_,
                 self_addr_,
                 new_rs_list,
                 new_readonly_rs_list,
                 rs_list_diff_member_list))) {
    LOG_WARN("get_rs_list failed", K(ret));
  } else if (common::INVALID_CLUSTER_TYPE == cluster_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster type is invalid", K(ret), K(cluster_type));
  } else {
    int64_t timestamp = ObTimeUtility::current_time();
    if (force_update_) {
      inner_need_update = true;
      if (OB_FAIL(
              root_addr_agent_->store(new_rs_list, new_readonly_rs_list, inner_need_update, cluster_type, timestamp))) {
        LOG_WARN("store rs_list failed",
            KR(ret),
            K(new_rs_list),
            K(new_readonly_rs_list),
            K(inner_need_update),
            K(cluster_type),
            K(timestamp));
      } else {
        LOG_INFO("store rs list succeed", K(new_rs_list), K(new_readonly_rs_list), K(cluster_type));
      }
    } else {
      // not force_update, check need update first
      bool need_update = false;
      bool inner_need_update = false;
      if (OB_FAIL(check_need_update(new_rs_list,
              new_readonly_rs_list,
              cluster_type,
              rs_list_diff_member_list,
              need_update,
              inner_need_update))) {
        LOG_WARN("failed to check need to update rs list", K(ret), K(new_rs_list), K(cluster_type));
      } else if (need_update) {
        if (OB_FAIL(root_addr_agent_->store(
                new_rs_list, new_readonly_rs_list, inner_need_update, cluster_type, timestamp))) {
          LOG_WARN("store rs_list failed", K(new_rs_list), K(new_readonly_rs_list), K(inner_need_update), K(ret));
        } else {
          LOG_INFO("store rs list succeed",
              K(ret),
              K(new_rs_list),
              K(new_readonly_rs_list),
              K(inner_need_update),
              K(cluster_type));
        }
      }
    }
  }
  return ret;
}

int ObUpdateRsListTask::process()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(*lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (!ObRootServiceRoleChecker::is_rootserver(reinterpret_cast<ObIPartPropertyGetter*>(GCTX.ob_service_))) {
      ret = OB_NOT_MASTER;
      LOG_WARN("not master", K(ret));
    } else if (OB_FAIL(process_without_lock())) {
      LOG_WARN("task process failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObUpdateRsListTask::unlock();
    } else {
      // ObUpdateRsListTask will be retried indefinitely until success.
      // do worry about unlocking while not success
    }
  }
  return ret;
}

int64_t ObUpdateRsListTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask* ObUpdateRsListTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), K(ret));
  } else {
    task = new (buf) ObUpdateRsListTask();
    if (OB_FAIL(static_cast<ObUpdateRsListTask*>(task)->init(
            *pt_operator_, root_addr_agent_, *server_mgr_, *zone_mgr_, *lock_, force_update_, self_addr_))) {
      LOG_WARN("init task failed",
          KP(pt_operator_),
          KP(root_addr_agent_),
          KP(server_mgr_),
          KP(zone_mgr_),
          KP(lock_),
          K(ret));
    }

    if (OB_FAIL(ret)) {
      static_cast<ObUpdateRsListTask*>(task)->~ObUpdateRsListTask();
      task = NULL;
    }
  }
  return task;
}

int ObUpdateRsListTask::get_rs_list(ObPartitionTableOperator& pt, ObServerManager& server_mgr, const ObAddr& self_addr,
    share::ObIAddrList& rs_list, share::ObIAddrList& readonly_rs_list, bool& rs_list_diff_member_list)
{
  int ret = OB_SUCCESS;
  ObPartitionInfo partition_info;
  const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  const int64_t partition_id = ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID;
  if (OB_FAIL(pt.get(table_id, partition_id, partition_info))) {
    LOG_WARN("pt_operator get failed", KT(table_id), K(partition_id), K(ret));
  } else {
    const ObPartitionReplica* leader_replica = NULL;
    ObRootAddr rs;
    FOREACH_CNT_X(replica, partition_info.get_replicas_v2(), OB_SUCCESS == ret)
    {
      bool is_server_alive = false;
      if (server_mgr.has_build()) {
        if (OB_FAIL(server_mgr.check_server_alive(replica->server_, is_server_alive))) {
          LOG_WARN("check_server_alive failed", "server", replica->server_, K(ret));
        }
      } else {
        is_server_alive = true;
      }
      if (OB_SUCCESS == ret &&
          (replica->server_ == self_addr || (replica->is_in_service() && is_server_alive &&
                                                ObReplicaTypeCheck::is_paxos_replica_V2(replica->replica_type_)))) {
        rs.reset();
        rs.server_ = replica->server_;
        rs.role_ = replica->role_;
        rs.sql_port_ = replica->sql_port_;
        if (OB_FAIL(rs_list.push_back(rs))) {
          LOG_WARN("add rs failed", K(ret), K(rs));
        }
      } else if (OB_SUCCESS == ret && is_server_alive && share::REPLICA_STATUS_NORMAL == replica->replica_status_ &&
                 common::REPLICA_TYPE_READONLY == replica->replica_type_) {
        rs.reset();
        rs.server_ = replica->server_;
        rs.role_ = replica->role_;
        rs.sql_port_ = replica->sql_port_;
        if (OB_FAIL(readonly_rs_list.push_back(rs))) {
          LOG_WARN("add readonly rs failed", K(ret), K(rs));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_info.find_leader_v2(leader_replica))) {
        LOG_WARN("fail to get leader", K(ret), K(partition_info));
      } else if (NULL == leader_replica) {
        ret = OB_LEADER_NOT_EXIST;
        LOG_WARN("leader not exist", K(partition_info), K(ret));
      } else {
        // check rs list diff member list
        rs_list_diff_member_list = false;
        if (rs_list.count() != leader_replica->member_list_.count()) {
          rs_list_diff_member_list = true;
        } else {
          for (int64_t i = 0; i < rs_list.count(); ++i) {
            ObAddr& rs_list_server = rs_list.at(i).server_;
            bool found = false;
            for (int64_t j = 0; j < leader_replica->member_list_.count(); ++j) {
              const ObAddr& member_list_server = leader_replica->member_list_.at(j).server_;
              if (rs_list_server == member_list_server) {
                found = true;
                break;
              }
            }
            if (!found) {
              rs_list_diff_member_list = true;
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}
// need_update: need to update rs_list
// inner_need_update: need to update parameter
int ObUpdateRsListTask::check_need_update(const share::ObIAddrList& rs_list, const share::ObIAddrList& readonly_rs_list,
    const common::ObClusterType cluster_type, const bool rs_list_diff_member_list, bool& need_update,
    bool& inner_need_update)
{
  int ret = OB_SUCCESS;
  if (0 >= rs_list.count() || common::INVALID_CLUSTER_TYPE == cluster_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs list or cluster type is invalid", K(ret), K(rs_list), K(cluster_type));
  } else {
    ObSEArray<ObRootAddr, ObPartitionReplica::DEFAULT_REPLICA_COUNT> cur_rs_list;
    ObSEArray<ObRootAddr, ObPartitionReplica::DEFAULT_REPLICA_COUNT> cur_readonly_rs_list;
    ObClusterType old_cluster_type;
    if (OB_FAIL(root_addr_agent_->fetch(cur_rs_list, cur_readonly_rs_list, old_cluster_type))) {
      LOG_WARN("fetch current rs_list failed", K(ret));
    } else {
      if (rs_list_diff_member_list) {
        bool is_subset = false;
        if (OB_FAIL(check_rs_list_subset(rs_list, cur_rs_list, is_subset))) {
          LOG_WARN("check_rs_list_subset failed", K(rs_list), K(cur_rs_list), K(ret));
        } else if (is_subset) {
          // if new rs_list is subset of old rs_list, not force to update parameter
          // only update web config and cluster_manager
          inner_need_update = false;
        } else {
          inner_need_update = true;
        }
      } else {
        inner_need_update = true;
      }

      if (OB_SUCC(ret)) {
        bool rs_list_diff = false;
        bool readonly_rs_list_diff = false;
        bool cluster_type_diff = old_cluster_type != cluster_type;
        if (OB_FAIL(check_rs_list_diff(rs_list, cur_rs_list, rs_list_diff))) {
          LOG_WARN("check_rs_list_diff failed", K(rs_list), K(cur_rs_list), K(ret));
        } else if (OB_FAIL(check_rs_list_diff(readonly_rs_list, cur_readonly_rs_list, readonly_rs_list_diff))) {
          LOG_WARN("check_readonly_rs_list_diff failed", K(readonly_rs_list), K(cur_readonly_rs_list), K(ret));
        } else if (rs_list_diff) {
          // need update all agents while rs_list undergoes change
          need_update = true;
        } else if (readonly_rs_list_diff || cluster_type_diff) {
          // no need to update parameter while only cluster type and readonly rs_list undergo change
          need_update = true;
          inner_need_update = false;
        }
      }
    }
  }

  return ret;
}

int ObUpdateRsListTask::check_rs_list_diff(
    const share::ObIAddrList& left, const share::ObIAddrList& right, bool& different)
{
  int ret = OB_SUCCESS;
  different = false;
  if (left.count() != right.count()) {
    different = true;
  } else {
    for (int64_t i = 0; i < left.count(); ++i) {
      bool found = false;
      for (int64_t j = 0; j < right.count(); ++j) {
        if (left.at(i) == right.at(j)) {
          found = true;
          break;
        }
      }
      if (!found) {
        different = true;
        break;
      }
    }
  }
  return ret;
}

int ObUpdateRsListTask::check_rs_list_subset(
    const share::ObIAddrList& left, const share::ObIAddrList& right, bool& is_subset)
{
  int ret = OB_SUCCESS;
  is_subset = true;
  if (left.count() >= right.count()) {
    is_subset = false;
  } else {
    for (int64_t i = 0; i < left.count(); ++i) {
      bool found = false;
      for (int64_t j = 0; j < right.count(); ++j) {
        if (left.at(i) == right.at(j)) {
          found = true;
          break;
        }
      }
      if (!found) {
        is_subset = false;
        break;
      }
    }
  }
  return ret;
}

ObUpdateRsListTimerTask::ObUpdateRsListTimerTask(ObRootService& rs) : ObAsyncTimerTask(rs.get_task_queue()), rs_(rs)
{}

int ObUpdateRsListTimerTask::process()
{
  int ret = OB_SUCCESS;
  if (!rs_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool force_update = false;
    if (OB_FAIL(rs_.submit_update_rslist_task(force_update))) {
      LOG_WARN("failed to submit update rs list", K(ret));
    }
  }
  return ret;
}

ObAsyncTask* ObUpdateRsListTimerTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObUpdateRsListTimerTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObUpdateRsListTimerTask(rs_);
  }
  return task;
}
}  // end namespace rootserver
}  // end namespace oceanbase
