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

#ifndef OCEANBASE_PARTITION_TABLE_OB_REPLICA_FILTER_H_
#define OCEANBASE_PARTITION_TABLE_OB_REPLICA_FILTER_H_

#include "lib/net/ob_addr.h"
#include "lib/list/ob_dlist.h"
#include "common/ob_zone.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/ob_primary_zone_util.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObSimpleTableSchemaV2;
class ObSchemaGetterGuard;
class ObPrimaryZone;
}  // namespace schema
class ObIServerTrace;
class ObIZoneTrace;

class ObIReplicaFilter {
public:
  ObIReplicaFilter()
  {}
  virtual ~ObIReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const = 0;
  virtual bool skip_empty_partition() const
  {
    return false;
  }
};

class ObReplicaFilter : public ObIReplicaFilter, public common::ObDLinkBase<ObReplicaFilter> {
public:
  ObReplicaFilter()
  {}
  virtual ~ObReplicaFilter()
  {}
};

class ObVersionReplicaFilter : public ObReplicaFilter {
public:
  explicit ObVersionReplicaFilter(int64_t data_version) : data_version_(data_version)
  {}
  virtual ~ObVersionReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  int64_t data_version_;
};

class ObStatusReplicaFilter : public ObReplicaFilter {
public:
  explicit ObStatusReplicaFilter(const ObReplicaStatus status) : replica_status_(status)
  {}
  virtual ~ObStatusReplicaFilter()
  {}

  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  ObReplicaStatus replica_status_;
};

class ObPersistentStatusReplicaFilter : public ObReplicaFilter {
public:
  explicit ObPersistentStatusReplicaFilter(const ObReplicaStatus status) : status_(status)
  {}
  virtual ~ObPersistentStatusReplicaFilter() = default;

  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  void set_replica_status(const ObReplicaStatus status)
  {
    status_ = status;
  }

private:
  ObReplicaStatus status_;
};

class ObZoneReplicaFilter : public ObReplicaFilter {
public:
  explicit ObZoneReplicaFilter(const common::ObZone& zone) : zone_(zone)
  {}
  virtual ~ObZoneReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  common::ObZone zone_;
};

class ObFilterLogReplicaFilter : public ObReplicaFilter {
public:
  ObFilterLogReplicaFilter()
  {}
  virtual ~ObFilterLogReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return true;
  }
};

class ObOnlyFullReplicaFilter : public ObReplicaFilter {
public:
  ObOnlyFullReplicaFilter()
  {}
  virtual ~ObOnlyFullReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return true;
  }
};

class ObOnlyPaxosReplicaFilter : public ObReplicaFilter {
public:
  ObOnlyPaxosReplicaFilter()
  {}
  virtual ~ObOnlyPaxosReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return true;
  }
};

class ObOnlyUserTableFilter : public ObReplicaFilter {
public:
  ObOnlyUserTableFilter()
  {}
  virtual ~ObOnlyUserTableFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return true;
  }
};

class ObOnlyInnerTableFilter : public ObReplicaFilter {
public:
  ObOnlyInnerTableFilter()
  {}
  virtual ~ObOnlyInnerTableFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return true;
  }
};

class ObOnlyUserTenantFilter : public ObReplicaFilter {
public:
  ObOnlyUserTenantFilter()
  {}
  virtual ~ObOnlyUserTenantFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return true;
  }
};

class ObServerReplicaFilter : public ObReplicaFilter {
public:
  explicit ObServerReplicaFilter(const common::ObAddr& server) : server_(server)
  {}
  virtual ~ObServerReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return true;
  }

private:
  common::ObAddr server_;
};

class ObValidVersionReplicaFilter : public ObReplicaFilter {
public:
  ObValidVersionReplicaFilter()
  {}
  virtual ~ObValidVersionReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
};

class ObPermanentOfflineFilter : public ObReplicaFilter {
public:
  explicit ObPermanentOfflineFilter(const ObIServerTrace* tracker) : tracker_(tracker)
  {}
  virtual ~ObPermanentOfflineFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  const ObIServerTrace* tracker_;
};

class ObOnlyLeaderByElectionFilter : public ObReplicaFilter {
public:
  explicit ObOnlyLeaderByElectionFilter()
  {}
  virtual ~ObOnlyLeaderByElectionFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return false;
  }
};

class ObServerTraceFilter : public ObReplicaFilter {
public:
  explicit ObServerTraceFilter(const ObIServerTrace* tracker) : tracker_(tracker)
  {}
  virtual ~ObServerTraceFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  const ObIServerTrace* tracker_;
};

class ObStoppedServerFilter : public ObReplicaFilter {
public:
  explicit ObStoppedServerFilter(const ObIServerTrace* tracker) : tracker_(tracker)
  {}
  virtual ~ObStoppedServerFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  const ObIServerTrace* tracker_;
};

class ObAliveReplicaFilter : public ObReplicaFilter {
public:
  explicit ObAliveReplicaFilter(const ObIServerTrace* tracker) : tracker_(tracker)
  {}
  virtual ~ObAliveReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  const ObIServerTrace* tracker_;
};

class ObInServiceReplicaFilter : public ObReplicaFilter {
public:
  explicit ObInServiceReplicaFilter(const ObIServerTrace* tracker) : tracker_(tracker)
  {}
  virtual ~ObInServiceReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  const ObIServerTrace* tracker_;
};

class ObBlockServerReplicaFilter : public ObReplicaFilter {
public:
  explicit ObBlockServerReplicaFilter(const ObIServerTrace* tracker) : tracker_(tracker)
  {}
  virtual ~ObBlockServerReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  const ObIServerTrace* tracker_;
};

class ObInvalidServerFilter : public ObReplicaFilter {
public:
  ObInvalidServerFilter() : invalid_server_list_()
  {}
  virtual ~ObInvalidServerFilter()
  {}
  int set_server_list(const common::ObIArray<common::ObAddr>& server_list);
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  common::ObArray<common::ObAddr> invalid_server_list_;
};

class ObUnitFilter : public ObReplicaFilter {
public:
  ObUnitFilter() : unit_ids_()
  {}
  virtual ~ObUnitFilter()
  {}
  int set_unit_ids(const common::ObIArray<uint64_t>& unit_ids);
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;

private:
  common::ObArray<uint64_t> unit_ids_;
};

class ObOnlyClusterPrivateTable : public ObReplicaFilter {
public:
  explicit ObOnlyClusterPrivateTable()
  {}
  virtual ~ObOnlyClusterPrivateTable()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return true;
  }
};

class ObFilterSpecialTable : public ObReplicaFilter {
public:
  explicit ObFilterSpecialTable()
  {}
  virtual ~ObFilterSpecialTable()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return true;
  }
};

class ObMemberListReplicaFilter : public ObReplicaFilter {
public:
  ObMemberListReplicaFilter()
  {}
  virtual ~ObMemberListReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
};

class ObInvalidPaxosReplicaFilter : public ObReplicaFilter {
public:
  ObInvalidPaxosReplicaFilter()
  {}
  virtual ~ObInvalidPaxosReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
};

class ObRebuildReplicaFilter : public ObReplicaFilter {
public:
  ObRebuildReplicaFilter()
  {}
  virtual ~ObRebuildReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
};

class ObRestoreReplicaFilter : public ObReplicaFilter {
public:
  ObRestoreReplicaFilter()
  {}
  virtual ~ObRestoreReplicaFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
};

class ObCutdataStatusFilter : public ObReplicaFilter {
public:
  ObCutdataStatusFilter()
  {}
  virtual ~ObCutdataStatusFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
};

class ObValidReplicaTypeFilter : public ObReplicaFilter {
public:
  ObValidReplicaTypeFilter()
  {}
  virtual ~ObValidReplicaTypeFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
};

class ObNotLogOnlyReplicaTypeFilter : public ObReplicaFilter {
public:
  explicit ObNotLogOnlyReplicaTypeFilter()
  {}
  virtual ~ObNotLogOnlyReplicaTypeFilter()
  {}
  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
};

class ObReplicaFilterHolder : public ObIReplicaFilter {
public:
  ObReplicaFilterHolder();
  virtual ~ObReplicaFilterHolder();

  virtual int check(const ObPartitionReplica& replica, bool& pass) const;
  virtual bool skip_empty_partition() const
  {
    return skip_empty_partition_;
  }

  int add(ObReplicaFilter& filter);

  void reuse();

  int set_valid_version();
  int set_in_member_list();
  int set_version(const int64_t version);

  int set_zone(const common::ObZone& zone);
  int set_server(const common::ObAddr& server);
  // filter the server not in ObIServerTracer
  int filter_delete_server(const ObIServerTrace& tracker);
  // filter the specific server_list
  int filter_invalid_server(const common::ObIArray<common::ObAddr>& invalid_server);
  // filter the server not alive;
  int set_only_alive_server(const ObIServerTrace& tracker);
  int filter_rebuild_replica();
  int filter_restore_replica();
  int filter_block_server(const ObIServerTrace& tracker);
  int set_only_in_service_server(const ObIServerTrace& tracker);
  int set_filter_unit(const common::ObIArray<uint64_t>& unit_ids);
  int filter_special_table();
  int only_cluster_private_table();
  int only_leader_by_election();
  int set_filter_permanent_offline(const ObIServerTrace& tracker);
  int set_filter_stopped_server(const ObIServerTrace& tracker);
  int set_only_user_table();
  int set_only_user_tenant();
  int set_only_inner_table();
  int set_only_full_replica();
  int set_only_paxos_replica();
  int set_replica_status(const ObReplicaStatus status);
  int set_persistent_replica_status_not_equal(const ObReplicaStatus status);
  int set_filter_log_replica();
  int set_filter_invalid_paxos_replica();
  int set_only_cutdata_status();
  int set_only_not_logonly_replica();

private:
  int build_skip_flag();

private:
  bool skip_empty_partition_;
  common::ObDList<ObReplicaFilter> filter_list_;

  ObValidVersionReplicaFilter valid_version_filter_;
  ObVersionReplicaFilter version_filter_;
  ObStatusReplicaFilter status_filter_;
  ObPersistentStatusReplicaFilter persistent_status_filter_;
  ObZoneReplicaFilter zone_filter_;
  ObServerReplicaFilter server_filter_;
  ObAliveReplicaFilter alive_filter_;
  ObInServiceReplicaFilter in_service_filter_;
  ObInvalidServerFilter invalid_server_filter_;
  ObServerTraceFilter server_trace_filter_;
  ObOnlyUserTableFilter only_user_table_filter_;
  ObOnlyInnerTableFilter only_inner_table_filter_;
  ObOnlyFullReplicaFilter only_full_replica_filter_;
  ObMemberListReplicaFilter member_list_filter_;
  ObPermanentOfflineFilter permanent_offline_filter_;
  // filter the paxos replicas not in the member list
  ObInvalidPaxosReplicaFilter invalid_paxos_replica_filter_;
  // filter the replicas with an invalid replica type
  ObValidReplicaTypeFilter valid_replica_type_filter_;
  // filter the replicas which are in the stopped servers
  ObStoppedServerFilter stopped_server_filter_;
  ObOnlyUserTenantFilter only_user_tenant_filter_;
  ObRebuildReplicaFilter rebuild_replica_filter_;
  ObRestoreReplicaFilter restore_replica_filter_;
  ObFilterLogReplicaFilter filter_log_replica_filter_;
  ObUnitFilter unit_filter_;
  ObBlockServerReplicaFilter block_server_filter_;
  ObFilterSpecialTable filter_special_table_;
  ObOnlyLeaderByElectionFilter only_leader_by_election_filter_;
  ObOnlyClusterPrivateTable only_cluster_private_table_;
  ObOnlyPaxosReplicaFilter only_paxos_replica_filter_;
  ObCutdataStatusFilter cutdata_status_filter_;
  ObNotLogOnlyReplicaTypeFilter not_logonly_replica_type_filter_;  // do not include logonly_replica
  DISALLOW_COPY_AND_ASSIGN(ObReplicaFilterHolder);
};

class ObIPartitionEntityFilter {
public:
  ObIPartitionEntityFilter()
  {}
  virtual ~ObIPartitionEntityFilter()
  {}
  virtual int check(const share::schema::ObPrimaryZone& primary_zone_info,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, bool& pass) const = 0;
};

class ObPartitionEntityFilter : public ObIPartitionEntityFilter, public common::ObDLinkBase<ObPartitionEntityFilter> {
public:
  ObPartitionEntityFilter()
  {}
  virtual ~ObPartitionEntityFilter()
  {}
};

class ObFilterSinglePrimaryZone : public ObPartitionEntityFilter {
public:
  ObFilterSinglePrimaryZone(const ObIZoneTrace* zone_trace) : zone_trace_(zone_trace)
  {}
  virtual ~ObFilterSinglePrimaryZone()
  {}
  virtual int check(const share::schema::ObPrimaryZone& primary_zone_info,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, bool& pass) const override;

private:
  int get_full_replica_num(
      const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, int64_t& full_replica_num) const;

private:
  const ObIZoneTrace* zone_trace_;
};

class ObPartitionEntityFilterHolder : public ObIPartitionEntityFilter {
public:
  ObPartitionEntityFilterHolder() : ObIPartitionEntityFilter(), filter_list_(), filter_single_primary_zone_(nullptr)
  {}
  virtual ~ObPartitionEntityFilterHolder()
  {}

public:
  virtual int check(const share::schema::ObPrimaryZone& primary_zone_info,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, bool& pass) const override;
  int add(ObPartitionEntityFilter& filter);
  void reuse();
  int filter_single_primary_zone(const ObIZoneTrace& zone_trace);

private:
  common::ObDList<ObPartitionEntityFilter> filter_list_;
  ObFilterSinglePrimaryZone filter_single_primary_zone_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionEntityFilterHolder);
};
}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_PARTITION_TABLE_OB_REPLICA_FILTER_H_
