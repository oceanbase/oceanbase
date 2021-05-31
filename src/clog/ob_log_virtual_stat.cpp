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

#include "ob_log_virtual_stat.h"
#include "ob_log_task.h"
#include "ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObClogVirtualStat::ObClogVirtualStat()
    : is_inited_(false), state_mgr_(NULL), sw_(NULL), mm_(NULL), cm_(NULL), pls_(NULL)
{}

ObClogVirtualStat::~ObClogVirtualStat()
{}

int ObClogVirtualStat::init(common::ObAddr self, common::ObPartitionKey& partition_key, ObLogStateMgr* state_mgr,
    ObLogSlidingWindow* sw, ObLogMembershipMgr* mm, ObLogCascadingMgr* cm, ObIPartitionLogService* pls)
{
  int ret = OB_SUCCESS;
  if (!self.is_valid() || !partition_key.is_valid() || NULL == state_mgr || NULL == sw || NULL == mm || NULL == cm ||
      NULL == pls) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(self), K(partition_key));
  } else {
    self_ = self;
    partition_key_ = partition_key;
    state_mgr_ = state_mgr;
    sw_ = sw;
    mm_ = mm;
    cm_ = cm;
    pls_ = pls;
    is_inited_ = true;
  }
  return ret;
}

int ObClogVirtualStat::get_server_ip(char* buffer, uint32_t size)
{
  int ret = OB_SUCCESS;
  if (NULL == buffer || 0 >= size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!self_.ip_to_string(buffer, size)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "get ip fail", K(ret), KP(buffer), K(size), K_(self));
  } else {
    // do nothing;
  }
  return ret;
}

int32_t ObClogVirtualStat::get_port() const
{
  return self_.get_port();
}

uint64_t ObClogVirtualStat::get_table_id() const
{
  return partition_key_.get_table_id();
}

int64_t ObClogVirtualStat::get_partition_idx() const
{
  return partition_key_.get_partition_id();
}

int32_t ObClogVirtualStat::get_partition_cnt() const
{
  return partition_key_.get_partition_cnt();
}

const char* ObClogVirtualStat::get_replicate_role() const
{
  int ret = OB_SUCCESS;
  const char* role_str = NULL;
  common::ObRole role = INVALID_ROLE;
  if (NULL == state_mgr_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), K_(is_inited), KP_(state_mgr));
  } else if (OB_FAIL(pls_->get_role(role))) {
    SERVER_LOG(WARN, "get_role failed", K(ret), K_(partition_key));
  } else {
    if (LEADER == role) {
      role_str = "LEADER";
    } else if (RESTORE_LEADER == role) {
      role_str = "RESTORE_LEADER";
    } else if (STANDBY_LEADER == role) {
      role_str = "STANDBY_LEADER";
    } else {
      role_str = "FOLLOWER";
    }
  }
  return role_str;
}

const char* ObClogVirtualStat::get_replicate_state() const
{
  int ret = OB_SUCCESS;
  const char* state_str = NULL;
  if (NULL == state_mgr_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), K_(is_inited), KP_(state_mgr));
  } else {
    int16_t state = state_mgr_->get_state();
    switch (state) {
      case INIT:
        state_str = "INIT";
        break;
      case REPLAY:
        state_str = "REPLAY";
        break;
      case RECONFIRM:
        state_str = "RECONFIRM";
        break;
      case ACTIVE:
        state_str = "ACTIVE";
        break;
      case TAKING_OVER:
        state_str = "TAKING_OVER";
        break;
      case REVOKING:
        state_str = "REVOKING";
        break;
      default:
        state_str = "OB_STATE_UNKNOWN";
    }
  }
  return state_str;
}

common::ObAddr ObClogVirtualStat::get_leader() const
{
  int ret = OB_SUCCESS;
  common::ObAddr addr;
  if (NULL == state_mgr_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key));
  } else if (pls_->is_archive_restoring()) {
    addr = pls_->get_restore_leader();
  } else {
    addr = state_mgr_->get_leader();
  }
  return addr;
}

uint64_t ObClogVirtualStat::get_last_index_log_id() const
{
  int ret = OB_SUCCESS;
  uint64_t log_id = OB_INVALID_ID;
  if (NULL == sw_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(sw), K_(is_inited));
  } else {
    // TODO
  }
  return log_id;
}

int ObClogVirtualStat::get_last_index_log_timestamp(int64_t& timestamp) const
{
  int ret = OB_SUCCESS;
  UNUSED(timestamp);
  if (NULL == sw_ || !is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    // TODO
  }
  return ret;
}

uint64_t ObClogVirtualStat::get_max_log_id() const
{
  int ret = OB_SUCCESS;
  uint64_t log_id = OB_INVALID_ID;
  if (NULL == sw_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key));
  } else {
    log_id = sw_->get_max_log_id();
  }
  return log_id;
}

uint64_t ObClogVirtualStat::get_start_log_id() const
{
  int ret = OB_SUCCESS;
  uint64_t log_id = OB_INVALID_ID;
  if (NULL == sw_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(sw), K_(is_inited));
  } else {
    log_id = sw_->get_start_id();
  }
  return log_id;
}

common::ObAddr ObClogVirtualStat::get_parent() const
{
  int ret = OB_SUCCESS;
  common::ObAddr addr;
  if (NULL == cm_ || NULL == pls_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key));
  } else if (pls_->is_archive_restoring()) {
    common::ObAddr restore_leader;
    restore_leader = pls_->get_restore_leader();
    if (self_ != restore_leader) {
      // follower's parent is restore_leader
      addr = restore_leader;
    }
  } else {
    addr = cm_->get_parent_addr();
  }
  return addr;
}

int32_t ObClogVirtualStat::get_replica_type() const
{
  int ret = OB_SUCCESS;
  int32_t replica_type = REPLICA_TYPE_MAX;
  if (NULL == mm_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(mm), K_(is_inited));
  } else {
    replica_type = mm_->get_replica_type();
  }
  return replica_type;
}

common::ObVersion ObClogVirtualStat::get_freeze_version() const
{
  int ret = OB_SUCCESS;
  common::ObVersion freeze_version;
  if (NULL == state_mgr_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(state_mgr), K_(is_inited));
  } else {
    freeze_version = state_mgr_->get_freeze_version();
  }
  return freeze_version;
}

common::ObMemberList ObClogVirtualStat::get_curr_member_list() const
{
  int ret = OB_SUCCESS;
  common::ObMemberList mem_list;
  if (NULL == mm_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(mm), K_(is_inited));
  } else {
    mem_list = mm_->get_curr_member_list();
  }
  return mem_list;
}

share::ObCascadMemberList ObClogVirtualStat::get_children_list() const
{
  int ret = OB_SUCCESS;
  share::ObCascadMemberList children_list;
  if (NULL == cm_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(mm), K_(is_inited));
  } else if (OB_FAIL(cm_->get_children_list(children_list))) {
    SERVER_LOG(WARN, "get_children_list failed", K(ret), K_(partition_key));
  } else {
    // do nothing
  }
  return children_list;
}

uint64_t ObClogVirtualStat::get_member_ship_log_id() const
{
  int ret = OB_SUCCESS;
  uint64_t log_id = OB_INVALID_ID;
  if (NULL == mm_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(mm), K_(is_inited));
  } else {
    log_id = mm_->get_log_id();
  }
  return log_id;
}

bool ObClogVirtualStat::is_offline() const
{
  int ret = OB_SUCCESS;
  bool is_offline = false;
  if (NULL == state_mgr_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(state_mgr), K_(is_inited));
  } else {
    is_offline = state_mgr_->is_offline();
  }
  return is_offline;
}

bool ObClogVirtualStat::is_in_sync() const
{
  int ret = OB_SUCCESS;
  bool is_in_sync = false;
  if (NULL == pls_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(pls), K_(is_inited));
  } else if (OB_FAIL(pls_->is_in_sync(is_in_sync))) {
    is_in_sync = false;
    SERVER_LOG(WARN, "ObClogVirtualStat is_in_sync failed", K(ret), K_(partition_key));
  } else {
    // do nothing
  }
  return is_in_sync;
}

bool ObClogVirtualStat::allow_gc() const
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;

  if (NULL == pls_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(pls), K_(is_inited));
  } else if (OB_FAIL(pls_->allow_gc(bool_ret))) {
    SERVER_LOG(WARN, "get clog allow_gc fail", K(ret), K_(partition_key));
  }

  return bool_ret;
}

bool ObClogVirtualStat::is_need_rebuild() const
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  if (NULL == pls_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(pls), K_(is_inited));
  } else if (OB_FAIL(pls_->is_need_rebuild(bool_ret))) {
    SERVER_LOG(WARN, "get clog is_need_rebuild fail", K(ret), K_(partition_key));
  }

  return bool_ret;
}

int64_t ObClogVirtualStat::get_quorum() const
{
  int ret = OB_SUCCESS;
  int64_t ret_value = -1;

  if (NULL == pls_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat is not inited", K(ret), K(partition_key_));
  } else if (OB_FAIL(pls_->get_replica_num(ret_value))) {
    SERVER_LOG(WARN, "get replica num failed", K(ret), K(partition_key_));
  }

  return ret_value;
}

uint64_t ObClogVirtualStat::get_next_replay_ts_delta() const
{
  int ret = OB_SUCCESS;
  uint64_t unused_log_id = OB_INVALID_ID;
  int64_t next_replay_ts = 0;
  uint64_t next_replay_ts_delta = UINT64_MAX;  // default is UINT64_MAX
  if (NULL == pls_ || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObClogVirtualStat not init", K(ret), K_(partition_key), KP_(pls), K_(is_inited));
  } else if (OB_FAIL(pls_->get_next_replay_log_info(unused_log_id, next_replay_ts))) {
    SERVER_LOG(WARN, "get_next_replay_log_info failed", K(ret), K_(partition_key));
  } else {
    next_replay_ts_delta = ObTimeUtility::current_time() - next_replay_ts;
  }
  return next_replay_ts_delta;
}

}  // namespace clog
}  // namespace oceanbase
