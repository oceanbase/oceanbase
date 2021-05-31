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

#include "ob_replica_filter.h"
#include "ob_partition_info.h"
#include "ob_iserver_trace.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_utils.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/utility/utility.h"
#include "common/ob_region.h"
#include "common/ob_zone.h"

namespace oceanbase {
namespace share {
using namespace schema;
using namespace common;

int ObVersionReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    pass = (data_version_ == replica.data_version_);
  }
  return ret;
}

int ObStatusReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid() || replica_status_ < 0 || replica_status_ >= REPLICA_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica), K_(replica_status));
  } else {
    pass = (replica_status_ == replica.replica_status_);
  }
  return ret;
}

int ObPersistentStatusReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid() || status_ < 0 || status_ >= REPLICA_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica), K_(status));
  } else {
    pass = (status_ != replica.status_);
  }
  return ret;
}

int ObZoneReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid() || zone_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica), K_(zone));
  } else {
    pass = (zone_ == replica.zone_);
  }
  return ret;
}

int ObFilterLogReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    pass = (REPLICA_TYPE_LOGONLY != replica.replica_type_);
  }
  return ret;
}

int ObOnlyFullReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    pass = (REPLICA_TYPE_FULL == replica.replica_type_);
  }
  return ret;
}

int ObOnlyPaxosReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    pass = ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_);
  }
  return ret;
}

int ObOnlyUserTableFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    int64_t pure_id = extract_pure_id(replica.table_id_);
    pass = (pure_id >= OB_MIN_USER_TABLE_ID);
  }
  return ret;
}

int ObOnlyInnerTableFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    int64_t pure_id = extract_pure_id(replica.table_id_);
    pass = (pure_id < OB_MIN_USER_TABLE_ID);
  }
  return ret;
}

int ObOnlyUserTenantFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    int64_t tenant_id = extract_tenant_id(replica.table_id_);
    pass = (tenant_id >= OB_USER_TENANT_ID);
  }
  return ret;
}

int ObServerReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid() || !server_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica), K_(server));
  } else {
    pass = (server_ == replica.server_);
  }
  return ret;
}

int ObValidVersionReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    pass = (replica.data_version_ >= 0);
  }
  return ret;
}

int ObPermanentOfflineFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  bool is_offline = false;
  if (!replica.is_valid() || NULL == tracker_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica), KP_(tracker));
  } else {
    if (OB_FAIL(tracker_->check_server_permanent_offline(replica.server_, is_offline))) {
      LOG_WARN("check_server_alive failed", "server", replica.server_, K(ret));
    } else {
      pass = !is_offline;
    }
  }
  return ret;
}

int ObServerTraceFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid() || NULL == tracker_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica), KP_(tracker));
  } else {
    bool exist = false;
    if (OB_FAIL(tracker_->is_server_exist(replica.server_, exist))) {
      LOG_WARN("check_server_alive failed", "server", replica.server_, K(ret));
    } else {
      pass = exist;
    }
  }
  return ret;
}

int ObStoppedServerFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid() || NULL == tracker_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica), KP_(tracker));
  } else {
    bool is_stopped = false;
    if (OB_FAIL(tracker_->is_server_stopped(replica.server_, is_stopped))) {
      LOG_WARN("check_server_stopped failed", "server", replica.server_, K(ret));
    } else {
      pass = !is_stopped;
    }
  }
  return ret;
}

int ObAliveReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  bool is_alive = false;
  if (!replica.is_valid() || NULL == tracker_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica), KP_(tracker));
  } else {
    if (OB_FAIL(tracker_->check_server_alive(replica.server_, is_alive))) {
      LOG_WARN("check_server_alive failed", "server", replica.server_, K(ret));
    } else {
      pass = is_alive;
    }
  }
  return ret;
}

int ObBlockServerReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  bool is_blocked = true;
  if (!replica.is_valid() || NULL == tracker_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica), KP_(tracker));
  } else {
    if (OB_FAIL(tracker_->check_migrate_in_blocked(replica.server_, is_blocked))) {
      LOG_WARN("check_in_block failed", "server", replica.server_, K(ret));
    } else {
      pass = !is_blocked;
    }
  }
  return ret;
}

int ObInServiceReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  bool is_in_service = false;
  if (!replica.is_valid() || NULL == tracker_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica), KP_(tracker));
  } else {
    if (OB_FAIL(tracker_->check_in_service(replica.server_, is_in_service))) {
      LOG_WARN("check_in_service failed", "server", replica.server_, K(ret));
    } else {
      pass = is_in_service;
    }
  }
  return ret;
}

int ObInvalidServerFilter::set_server_list(const ObIArray<ObAddr>& server_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(invalid_server_list_.assign(server_list))) {
    LOG_WARN("fail to assign unit", K(ret), K(server_list));
  }
  return ret;
}

int ObInvalidServerFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica));
  } else if (has_exist_in_array(invalid_server_list_, replica.server_)) {
    pass = false;
  } else {
    pass = true;
  }
  return ret;
}

int ObUnitFilter::set_unit_ids(const ObIArray<uint64_t>& unit_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(unit_ids_.assign(unit_ids))) {
    LOG_WARN("fail to assign unit", K(ret), K(unit_ids));
  }
  return ret;
}

int ObUnitFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica), K(unit_ids_));
  } else if (has_exist_in_array(unit_ids_, replica.unit_id_)) {
    pass = false;
  } else {
    pass = true;
  }
  return ret;
}

int ObOnlyLeaderByElectionFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica));
  } else if (replica.is_leader_by_election()) {
    pass = true;
  } else {
    pass = false;
  }
  return ret;
}

int ObOnlyClusterPrivateTable::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica));
  } else if (!schema::ObSysTableChecker::is_cluster_private_tenant_table(replica.table_id_)) {
    pass = false;
  } else {
    pass = true;
  }
  return ret;
}

int ObFilterSpecialTable::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica));
  } else if (schema::ObSysTableChecker::is_cluster_private_tenant_table(replica.table_id_)) {
    pass = false;
  } else {
    pass = true;
  }
  return ret;
}

int ObValidReplicaTypeFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    pass = ObReplicaTypeCheck::is_replica_type_valid(replica.replica_type_);
  }
  return ret;
}

int ObNotLogOnlyReplicaTypeFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica));
  } else {
    pass = !ObReplicaTypeCheck::is_log_replica(replica.replica_type_);
  }
  return ret;
}

int ObInvalidPaxosReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  pass = true;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else if (replica.is_paxos_candidate() && !replica.in_member_list_) {
    pass = false;
  }
  return ret;
}

int ObMemberListReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    pass = replica.in_member_list_;
  }
  return ret;
}

ObReplicaFilterHolder::ObReplicaFilterHolder()
    : skip_empty_partition_(false),
      version_filter_(0),
      status_filter_(REPLICA_STATUS_NORMAL),
      persistent_status_filter_(REPLICA_STATUS_NORMAL),
      zone_filter_(0),
      server_filter_(ObAddr()),
      alive_filter_(NULL),
      in_service_filter_(NULL),
      invalid_server_filter_(),
      server_trace_filter_(NULL),
      only_user_table_filter_(),
      only_full_replica_filter_(),
      member_list_filter_(),
      permanent_offline_filter_(NULL),
      stopped_server_filter_(NULL),
      only_user_tenant_filter_(),
      rebuild_replica_filter_(),
      restore_replica_filter_(),
      filter_log_replica_filter_(),
      unit_filter_(),
      block_server_filter_(NULL),
      filter_special_table_(),
      only_cluster_private_table_(),
      only_paxos_replica_filter_(),
      cutdata_status_filter_()
{}

ObReplicaFilterHolder::~ObReplicaFilterHolder()
{}

void ObReplicaFilterHolder::reuse()
{
  skip_empty_partition_ = false;
  while (!filter_list_.is_empty()) {
    filter_list_.remove_first();
  }
}

int ObReplicaFilterHolder::build_skip_flag()
{
  int ret = OB_SUCCESS;
  skip_empty_partition_ = false;
  if (filter_list_.is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty filter", K(ret));
  } else {
    DLIST_FOREACH(it, filter_list_)
    {
      if (it->skip_empty_partition()) {
        skip_empty_partition_ = true;
        break;
      }
    }
  }
  return ret;
}

int ObReplicaFilterHolder::add(ObReplicaFilter& filter)
{
  int ret = OB_SUCCESS;
  if (!filter_list_.add_last(&filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add filter to filter list failed", K(ret));
  } else if (OB_FAIL(build_skip_flag())) {
    LOG_WARN("build skip flag failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_valid_version()
{
  int ret = OB_SUCCESS;
  if (NULL != valid_version_filter_.get_prev()) {
    valid_version_filter_.unlink();
  }
  if (OB_FAIL(add(valid_version_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_in_member_list()
{
  int ret = OB_SUCCESS;
  if (NULL != member_list_filter_.get_prev()) {
    member_list_filter_.unlink();
  }
  if (OB_FAIL(add(member_list_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_version(const int64_t version)
{
  int ret = OB_SUCCESS;
  if (NULL != version_filter_.get_prev()) {
    version_filter_.unlink();
  }
  version_filter_ = ObVersionReplicaFilter(version);
  if (OB_FAIL(add(version_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_filter_invalid_paxos_replica()
{
  int ret = OB_SUCCESS;
  if (NULL != invalid_paxos_replica_filter_.get_prev()) {
    invalid_paxos_replica_filter_.unlink();
  }
  invalid_paxos_replica_filter_ = ObInvalidPaxosReplicaFilter();
  if (OB_FAIL(add(invalid_paxos_replica_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_replica_status(const ObReplicaStatus status)
{
  int ret = OB_SUCCESS;
  if (NULL != status_filter_.get_prev()) {
    status_filter_.unlink();
  }
  if (status < 0 || status >= REPLICA_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else {
    status_filter_ = ObStatusReplicaFilter(status);
    if (OB_FAIL(add(status_filter_))) {
      LOG_WARN("add filter failed", K(ret));
    }
  }
  return ret;
}

int ObReplicaFilterHolder::set_persistent_replica_status_not_equal(const ObReplicaStatus status)
{
  int ret = OB_SUCCESS;
  if (NULL != persistent_status_filter_.get_prev()) {
    persistent_status_filter_.unlink();
  }
  if (status < 0 || status >= REPLICA_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else {
    persistent_status_filter_.set_replica_status(status);
    if (OB_FAIL(add(persistent_status_filter_))) {
      LOG_WARN("add filter failed", K(ret));
    }
  }
  return ret;
}

int ObReplicaFilterHolder::set_zone(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  if (NULL != zone_filter_.get_prev()) {
    zone_filter_.unlink();
  }
  if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    zone_filter_ = ObZoneReplicaFilter(zone);
    if (OB_FAIL(add(zone_filter_))) {
      LOG_WARN("add filter failed", K(ret));
    }
  }
  return ret;
}

int ObReplicaFilterHolder::set_server(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (NULL != server_filter_.get_prev()) {
    server_filter_.unlink();
  }
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    server_filter_ = ObServerReplicaFilter(server);
    if (OB_FAIL(add(server_filter_))) {
      LOG_WARN("add filter failed", K(ret));
    }
  }
  return ret;
}

int ObReplicaFilterHolder::set_filter_stopped_server(const ObIServerTrace& tracker)
{
  int ret = OB_SUCCESS;
  if (NULL != stopped_server_filter_.get_prev()) {
    stopped_server_filter_.unlink();
  }
  stopped_server_filter_ = ObStoppedServerFilter(&tracker);
  if (OB_FAIL(add(stopped_server_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_only_full_replica()
{
  int ret = OB_SUCCESS;
  if (NULL != only_full_replica_filter_.get_prev()) {
    only_full_replica_filter_.unlink();
  }
  only_full_replica_filter_ = ObOnlyFullReplicaFilter();
  if (OB_FAIL(add(only_full_replica_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_only_paxos_replica()
{
  int ret = OB_SUCCESS;
  if (NULL != only_paxos_replica_filter_.get_prev()) {
    only_paxos_replica_filter_.unlink();
  }
  only_paxos_replica_filter_ = ObOnlyPaxosReplicaFilter();
  if (OB_FAIL(add(only_paxos_replica_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_filter_log_replica()
{
  int ret = OB_SUCCESS;
  if (NULL != filter_log_replica_filter_.get_prev()) {
    filter_log_replica_filter_.unlink();
  }
  filter_log_replica_filter_ = ObFilterLogReplicaFilter();
  if (OB_FAIL(add(filter_log_replica_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_only_user_table()
{
  int ret = OB_SUCCESS;
  if (NULL != only_user_table_filter_.get_prev()) {
    only_user_table_filter_.unlink();
  }
  only_user_table_filter_ = ObOnlyUserTableFilter();
  if (OB_FAIL(add(only_user_table_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_only_inner_table()
{
  int ret = OB_SUCCESS;
  if (NULL != only_inner_table_filter_.get_prev()) {
    only_inner_table_filter_.unlink();
  }
  only_inner_table_filter_ = ObOnlyInnerTableFilter();
  if (OB_FAIL(add(only_inner_table_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_only_user_tenant()
{
  int ret = OB_SUCCESS;
  if (NULL != only_user_tenant_filter_.get_prev()) {
    only_user_tenant_filter_.unlink();
  }
  only_user_tenant_filter_ = ObOnlyUserTenantFilter();
  if (OB_FAIL(add(only_user_tenant_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_filter_permanent_offline(const ObIServerTrace& tracker)
{
  int ret = OB_SUCCESS;
  if (NULL != permanent_offline_filter_.get_prev()) {
    permanent_offline_filter_.unlink();
  }
  permanent_offline_filter_ = ObPermanentOfflineFilter(&tracker);
  if (OB_FAIL(add(permanent_offline_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_only_alive_server(const ObIServerTrace& tracker)
{
  int ret = OB_SUCCESS;
  if (NULL != alive_filter_.get_prev()) {
    alive_filter_.unlink();
  }
  alive_filter_ = ObAliveReplicaFilter(&tracker);
  if (OB_FAIL(add(alive_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}
int ObReplicaFilterHolder::filter_rebuild_replica()
{
  int ret = OB_SUCCESS;
  if (NULL != rebuild_replica_filter_.get_prev()) {
    rebuild_replica_filter_.unlink();
  }
  rebuild_replica_filter_ = ObRebuildReplicaFilter();
  if (OB_FAIL(add(rebuild_replica_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}
int ObReplicaFilterHolder::filter_restore_replica()
{
  int ret = OB_SUCCESS;
  if (NULL != restore_replica_filter_.get_prev()) {
    restore_replica_filter_.unlink();
  }
  restore_replica_filter_ = ObRestoreReplicaFilter();
  if (OB_FAIL(add(restore_replica_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_only_cutdata_status()
{
  int ret = OB_SUCCESS;
  if (NULL != cutdata_status_filter_.get_prev()) {
    cutdata_status_filter_.unlink();
  }
  cutdata_status_filter_ = ObCutdataStatusFilter();
  if (OB_FAIL(add(cutdata_status_filter_))) {
    LOG_WARN("add filter failed", KR(ret), K(cutdata_status_filter_));
  }
  return ret;
}

int ObReplicaFilterHolder::filter_block_server(const ObIServerTrace& tracker)
{
  int ret = OB_SUCCESS;
  if (NULL != block_server_filter_.get_prev()) {
    block_server_filter_.unlink();
  }
  block_server_filter_ = ObBlockServerReplicaFilter(&tracker);
  if (OB_FAIL(add(block_server_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}
int ObReplicaFilterHolder::set_only_in_service_server(const ObIServerTrace& tracker)
{
  int ret = OB_SUCCESS;
  if (NULL != in_service_filter_.get_prev()) {
    in_service_filter_.unlink();
  }
  in_service_filter_ = ObInServiceReplicaFilter(&tracker);
  if (OB_FAIL(add(in_service_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::filter_invalid_server(const common::ObIArray<ObAddr>& invalid_server)
{
  int ret = OB_SUCCESS;
  if (NULL != invalid_server_filter_.get_prev()) {
    invalid_server_filter_.unlink();
  }
  invalid_server_filter_ = ObInvalidServerFilter();
  if (OB_FAIL(invalid_server_filter_.set_server_list(invalid_server))) {
    LOG_WARN("fail to set server list", KR(ret), K(invalid_server));
  } else if (OB_FAIL(add(invalid_server_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::filter_delete_server(const ObIServerTrace& tracker)
{
  int ret = OB_SUCCESS;
  if (NULL != server_trace_filter_.get_prev()) {
    server_trace_filter_.unlink();
  }
  server_trace_filter_ = ObServerTraceFilter(&tracker);
  if (OB_FAIL(add(server_trace_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_filter_unit(const common::ObIArray<uint64_t>& unit_ids)
{
  int ret = OB_SUCCESS;
  if (NULL != unit_filter_.get_prev()) {
    unit_filter_.unlink();
  }
  unit_filter_ = ObUnitFilter();
  if (OB_FAIL(unit_filter_.set_unit_ids(unit_ids))) {
    LOG_WARN("fail to set unit ids", K(ret), K(unit_ids));
  } else if (OB_FAIL(add(unit_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::only_leader_by_election()
{
  int ret = OB_SUCCESS;
  if (NULL != only_leader_by_election_filter_.get_prev()) {
    only_leader_by_election_filter_.unlink();
  }
  only_leader_by_election_filter_ = ObOnlyLeaderByElectionFilter();
  if (OB_FAIL(add(only_leader_by_election_filter_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::only_cluster_private_table()
{
  int ret = OB_SUCCESS;
  if (NULL != only_cluster_private_table_.get_prev()) {
    only_cluster_private_table_.unlink();
  }
  only_cluster_private_table_ = ObOnlyClusterPrivateTable();
  if (OB_FAIL(add(only_cluster_private_table_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::set_only_not_logonly_replica()
{
  int ret = OB_SUCCESS;
  if (NULL != not_logonly_replica_type_filter_.get_prev()) {
    not_logonly_replica_type_filter_.unlink();
  }
  not_logonly_replica_type_filter_ = ObNotLogOnlyReplicaTypeFilter();
  if (OB_FAIL(add(not_logonly_replica_type_filter_))) {
    LOG_WARN("add filter failed", KR(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::filter_special_table()
{
  int ret = OB_SUCCESS;
  if (NULL != filter_special_table_.get_prev()) {
    filter_special_table_.unlink();
  }
  filter_special_table_ = ObFilterSpecialTable();
  if (OB_FAIL(add(filter_special_table_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}

int ObReplicaFilterHolder::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    pass = true;
    DLIST_FOREACH(it, filter_list_)
    {
      if (OB_FAIL(it->check(replica, pass))) {
        LOG_WARN("check replica failed", K(ret));
      } else {
        if (!pass) {
          break;
        }
      }
    }
  }
  return ret;
}
int ObRebuildReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  pass = true;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else if (replica.rebuild_) {
    pass = false;
  }
  return ret;
}
int ObRestoreReplicaFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  pass = true;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else if (replica.is_restore_) {
    pass = false;
  }
  return ret;
}

int ObCutdataStatusFilter::check(const ObPartitionReplica& replica, bool& pass) const
{
  int ret = OB_SUCCESS;
  pass = true;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica));
  } else {
    pass = (REPLICA_RESTORE_CUT_DATA == replica.is_restore_);
  }
  return ret;
}

/*****************************************************************
 *    Filter for tenant partition iterator
 *****************************************************************
 */
void ObPartitionEntityFilterHolder::reuse()
{
  while (!filter_list_.is_empty()) {
    filter_list_.remove_first();
  }
}

int ObPartitionEntityFilterHolder::add(ObPartitionEntityFilter& filter)
{
  int ret = OB_SUCCESS;
  if (!filter_list_.add_last(&filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add filter to filter list failed", K(ret));
  }
  return ret;
}

int ObPartitionEntityFilterHolder::filter_single_primary_zone(const ObIZoneTrace& zone_trace)
{
  int ret = OB_SUCCESS;
  if (NULL != filter_single_primary_zone_.get_prev()) {
    filter_single_primary_zone_.unlink();
  }
  filter_single_primary_zone_ = ObFilterSinglePrimaryZone(&zone_trace);
  if (OB_FAIL(add(filter_single_primary_zone_))) {
    LOG_WARN("add filter failed", K(ret));
  }
  return ret;
}
int ObPartitionEntityFilterHolder::check(const ObPrimaryZone& primary_zone_info,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, bool& pass) const
{
  int ret = OB_SUCCESS;
  pass = true;
  DLIST_FOREACH(it, filter_list_)
  {
    if (OB_FAIL(it->check(primary_zone_info, zone_locality, pass))) {
      LOG_WARN("check table failed", K(ret));
    } else if (!pass) {
      break;
    } else {
    }  // go on check
  }
  return ret;
}
/****************************************************
 * this is used to filter single primary zone
 ****************************************************
 */
int ObFilterSinglePrimaryZone::check(const ObPrimaryZone& primary_zone_info,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, bool& pass) const
{
  int ret = OB_SUCCESS;
  pass = true;
  int64_t full_replica_num = 0;
  if (OB_ISNULL(zone_trace_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("get invalid schema_guard", K(ret), K(zone_trace_));
  } else if (OB_FAIL(get_full_replica_num(zone_locality, full_replica_num))) {
    LOG_WARN("fail to get full replica num", K(ret));
  } else if (full_replica_num <= 1) {
    pass = false;
  } else {
    ObSEArray<ObZone, OB_DEFAULT_SE_ARRAY_COUNT> primary_zone;
    ObRegion region;
    int64_t first_score = 0;
    int64_t primary_zone_num = 0;
    const ObIArray<ObZoneScore>& primary_zone_array = primary_zone_info.primary_zone_array_;
    for (int64_t i = 0; i < primary_zone_array.count() && OB_SUCC(ret); i++) {
      if (0 == first_score) {
        first_score = primary_zone_array.at(i).score_;
      }
      if (primary_zone_array.at(i).score_ == first_score) {
        if (OB_FAIL(primary_zone.push_back(primary_zone_array.at(i).zone_))) {
          LOG_WARN("fail to push back", K(ret), "zone", primary_zone_array.at(i).zone_);
        }
      }  // end if (primary_zone_array.at(i).score_ == first_score)
    }    // end for
    if (OB_SUCC(ret)) {
      if (primary_zone.count() > 1) {
        pass = true;
      } else if (primary_zone.count() == 0) {
        // no primary zone, but it have more than one full replica
        pass = true;
      } else if (OB_FAIL(zone_trace_->get_region(primary_zone.at(0), region))) {
        LOG_WARN("fail to get region", K(ret), "zone", primary_zone.at(0));
      } else if (OB_FAIL(zone_trace_->get_zone_count(region, primary_zone_num))) {
        LOG_WARN("fail to get zone count", K(ret), K(region));
      } else if (primary_zone_num <= 1) {
        pass = false;
      } else {
        pass = true;
      }
    }
  }
  return ret;
}

int ObFilterSinglePrimaryZone::get_full_replica_num(
    const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, int64_t& full_replica_num) const
{
  int ret = OB_SUCCESS;
  full_replica_num = 0;
  for (int64_t i = 0; i < zone_locality.count(); ++i) {
    full_replica_num += zone_locality.at(i).get_full_replica_num();
  }
  return ret;
}
}  // end namespace share
}  // end namespace oceanbase
