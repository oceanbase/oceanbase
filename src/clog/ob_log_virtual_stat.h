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

#ifndef OCEANBASE_CLOG_OB_CLOG_VIRTUAL_STAT_
#define OCEANBASE_CLOG_OB_CLOG_VIRTUAL_STAT_

#include "ob_log_membership_mgr_V2.h"
#include "ob_log_sliding_window.h"
#include "ob_log_state_mgr.h"
#include "ob_log_cascading_mgr.h"

namespace oceanbase {
namespace clog {
class ObIPartitionLogService;
class ObClogVirtualStat {
public:
  ObClogVirtualStat();
  virtual ~ObClogVirtualStat();

public:
  int init(common::ObAddr self, common::ObPartitionKey& partition_key, ObLogStateMgr* state_mgr, ObLogSlidingWindow* sw,
      ObLogMembershipMgr* mm, ObLogCascadingMgr* cm, ObIPartitionLogService* pls);
  int get_server_ip(char* buffer, uint32_t size);
  int32_t get_port() const;
  uint64_t get_table_id() const;
  int64_t get_partition_idx() const;
  int32_t get_partition_cnt() const;
  const char* get_replicate_role() const;
  const char* get_replicate_state() const;
  common::ObAddr get_leader() const;
  uint64_t get_last_index_log_id() const;
  int get_last_index_log_timestamp(int64_t& timestamp) const;
  uint64_t get_start_log_id() const;
  common::ObAddr get_parent() const;
  int32_t get_replica_type() const;
  uint64_t get_max_log_id() const;
  common::ObVersion get_freeze_version() const;
  common::ObMemberList get_curr_member_list() const;
  share::ObCascadMemberList get_children_list() const;
  uint64_t get_member_ship_log_id() const;
  bool is_offline() const;
  bool is_in_sync() const;
  bool allow_gc() const;
  int64_t get_quorum() const;
  bool is_need_rebuild() const;
  uint64_t get_next_replay_ts_delta() const;

private:
  bool is_inited_;
  common::ObAddr self_;
  common::ObPartitionKey partition_key_;
  ObLogStateMgr* state_mgr_;
  ObLogSlidingWindow* sw_;
  ObLogMembershipMgr* mm_;
  ObLogCascadingMgr* cm_;
  ObIPartitionLogService* pls_;
};  // class ObClogVirtualStat
}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_CLOG_VIRTUAL_STAT_
