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

#include "ob_alter_locality_checker.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_replica_info.h"
#include "share/ob_multi_cluster_util.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_replica_filter.h"
#include "observer/ob_server_struct.h"
#include "ob_leader_coordinator.h"
#include "ob_server_manager.h"
#include "ob_zone_manager.h"
#include "ob_unit_manager.h"
#include "ob_alloc_replica_strategy.h"
#include "share/ob_debug_sync.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver {

OB_SERIALIZE_MEMBER((ObCommitAlterTenantLocalityArg, ObDDLArg), tenant_id_);
OB_SERIALIZE_MEMBER((ObCommitAlterTablegroupLocalityArg, ObDDLArg), tablegroup_id_);
OB_SERIALIZE_MEMBER((ObCommitAlterTableLocalityArg, ObDDLArg), table_id_);

int ObAlterLocalityChecker::init(share::schema::ObMultiVersionSchemaService* schema_service,
    rootserver::ObLeaderCoordinator* leader_coordinator, obrpc::ObCommonRpcProxy* common_rpc_proxy,
    share::ObPartitionTableOperator* pt_operator, rootserver::ObServerManager* server_mgr, common::ObAddr& addr,
    rootserver::ObZoneManager* zone_mgr, rootserver::ObUnitManager* unit_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAlterLocalityChecker init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(NULL == schema_service) || OB_UNLIKELY(NULL == leader_coordinator) ||
             OB_UNLIKELY(NULL == common_rpc_proxy) || OB_UNLIKELY(NULL == pt_operator) ||
             OB_UNLIKELY(NULL == server_mgr) || OB_UNLIKELY(!addr.is_valid()) || OB_UNLIKELY(NULL == zone_mgr) ||
             OB_UNLIKELY(NULL == unit_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        KP(schema_service),
        KP(leader_coordinator),
        KP(common_rpc_proxy),
        KP(pt_operator),
        KP(server_mgr),
        K(addr),
        KP(zone_mgr),
        KP(unit_mgr));
  } else {
    schema_service_ = schema_service;
    leader_coordinator_ = leader_coordinator;
    common_rpc_proxy_ = common_rpc_proxy;
    pt_operator_ = pt_operator;
    server_mgr_ = server_mgr;
    self_ = addr;
    zone_mgr_ = zone_mgr;
    unit_mgr_ = unit_mgr;
    is_inited_ = true;
  }
  return ret;
}

/* 1 setting has_locality_modificatioin_ to true is used in the following two situations
 *   a when alter tenant/table locality, it is set to true;
 *   b set to true when new rs takes over
 *   when has_locality_modification_ is false, no alter locality, no need to check.
 * 2 check_alter_locality_finished() also invoked in get_schema_version_in_inner_table()
 *   when the schema version is the newest, get_schema_guard() is not invoked directly:
 *   a schema flushing may be delayed, get_schema_guard() cannot ensure local schema is the newest.
 *     check_alter_locality_finished() may not recognize an ddl operation to alter locality.
 *   b for primary cluster, schema should be checked tenant by tenant.
 *     for standby cluster, only schema of sys tenant need to be checked.
 */
int ObAlterLocalityChecker::check_alter_locality_finished(const uint64_t tenant_id,
    const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat)
{
  DEBUG_SYNC(BEFORE_CHECK_LOCALITY);
  ATOMIC_STORE(&has_locality_modification_, true);
  int ret = OB_SUCCESS;
  ObArray<uint64_t> all_tenant;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAlterLocalityChecker not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(NULL == GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", K(ret), KP(GCTX.sql_proxy_));
  } else if (!ATOMIC_LOAD(&has_locality_modification_)) {
    // no locality modification occured, ignore this time
  } else {
    // set has_locality_modification_ to false to check if this is a locality modification
    // during the execution of this check_alter_locality_finished routine
    // any locality modification is setting has_locality_modification_ to true
    ATOMIC_STORE(&has_locality_modification_, false);
    bool is_finished = true;
    bool is_standby_cluster = GCTX.is_standby_cluster();
    int bak_ret = OB_SUCCESS;
    bool this_tenant_finished = true;
    // in oceanbase primary cluster, schema_guard should be fetched every time this is executed
    // in oceanbase standby cluster, only fetch the sys tenant schema guard
    if (OB_SYS_TENANT_ID == tenant_id || !is_standby_cluster) {
      ObRefreshSchemaStatus schema_status;
      schema_status.tenant_id_ = tenant_id;
      int64_t version_in_inner_table = OB_INVALID_VERSION;
      int64_t local_schema_version = OB_INVALID_VERSION;
      int64_t get_schema_retry_cnt = 0;
      bool is_restore = false;
      while (OB_SUCC(ret) && get_schema_retry_cnt < GET_SCHEMA_RETRY_LIMIT) {
        if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
          LOG_WARN("fail to get schema guard", K(ret));
        } else if (OB_FAIL(schema_guard.check_tenant_is_restore(tenant_id, is_restore))) {
          LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
        } else if (is_restore) {
          // ignore when this tenant is in physical restore
          break;
        } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
                       *GCTX.sql_proxy_, schema_status, version_in_inner_table))) {
          LOG_WARN("fail to get version in inner table", K(ret));
        } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
          LOG_WARN("fail to get schema version from guard", K(ret), K(tenant_id));
        } else if (version_in_inner_table != local_schema_version) {
          ++get_schema_retry_cnt;
          usleep(GET_SCHEMA_INTERVAL);
        } else {
          break;
        }
      }
      if (OB_SUCC(ret) && get_schema_retry_cnt >= GET_SCHEMA_RETRY_LIMIT) {
        ret = OB_NEED_RETRY;
        LOG_WARN("reach retry limit, check tenant locality next round",
            K(ret),
            K(tenant_id),
            K(local_schema_version),
            K(version_in_inner_table));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_alter_locality_finished_by_tenant(
              tenant_id, hash_index_collection, tenant_stat, this_tenant_finished))) {
        LOG_WARN("fail to check alter locality finished by tenant", K(ret), K(tenant_id));
      } else {
        is_finished = (is_finished && this_tenant_finished);
      }
    }
    bak_ret = OB_SUCC(ret) ? bak_ret : ret;
    ret = OB_SUCCESS;  // overwrite ret
    ret = bak_ret;
    if (OB_FAIL(ret) || !is_finished) {
      ATOMIC_BCAS(&has_locality_modification_, false, true);
    }
  }
  return ret;
}

void ObAlterLocalityChecker::notify_locality_modification()
{
  ATOMIC_STORE(&has_locality_modification_, true);
}

bool ObAlterLocalityChecker::has_locality_modification() const
{
  return ATOMIC_LOAD(&has_locality_modification_);
}

int ObAlterLocalityChecker::check_alter_locality_finished_by_tenant(const uint64_t tenant_id,
    const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
    bool& is_finished)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema* tenant_schema = NULL;
  common::ObArray<uint64_t> table_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAlterLocalityChecker not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema)) || NULL == tenant_schema) {
    ret = OB_SUCCESS;  // rewrite to success, since tenant may be deleted
  } else {
    is_finished = true;
    // check table in new tablegroup
    bool null_locality_tablegroup_match = true;
    common::ObArray<const share::schema::ObTablegroupSchema*> tablegroup_schemas;
    if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_schema->get_tenant_id(), tablegroup_schemas))) {
      LOG_WARN("fail to get tablegroup schemas in tenant", K(ret), "tenant_id", tenant_schema->get_tenant_id());
    } else {
      for (int64_t i = 0; i < tablegroup_schemas.count() && OB_SUCC(ret); ++i) {
        bool tablegroup_locality_finished = true;
        const ObTablegroupSchema*& tablegroup_schema = tablegroup_schemas.at(i);
        if (OB_UNLIKELY(NULL == tablegroup_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, tablegroup schema null", K(ret), KP(tablegroup_schema));
        } else if (!is_new_tablegroup_id(tablegroup_schema->get_tablegroup_id())) {
          // ignore table not in new tablegroup
        } else if (OB_FAIL(process_single_tablegroup(schema_guard,
                       *tablegroup_schema,
                       hash_index_collection,
                       tenant_stat,
                       tablegroup_locality_finished))) {
          LOG_WARN(
              "fail to process single tablegroup", K(ret), "tablegroup_id", tablegroup_schema->get_tablegroup_id());
        } else {
          is_finished = is_finished && tablegroup_locality_finished;
          if (tablegroup_schema->get_locality_str().empty()) {
            null_locality_tablegroup_match = null_locality_tablegroup_match && tablegroup_locality_finished;
          }
          LOG_DEBUG("check alter locality finished by tablegroup",
              K(ret),
              "tablegroup_id",
              tablegroup_schema->get_tablegroup_id(),
              "tablegrouup_finished",
              static_cast<int32_t>(tablegroup_locality_finished),
              "tenant_finished",
              static_cast<int32_t>(is_finished));
        }
      }
    }
    // check table not in new tablegroup
    bool null_locality_table_match = true;
    if (OB_FAIL(ret)) {
      // skip
    } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id, table_ids))) {
      LOG_WARN("fail to get table ids in tenant", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
        bool table_locality_finished = true;
        const ObTableSchema* table_schema = NULL;
        if (OB_FAIL(check_stop())) {
          LOG_WARN("root balancer stop", K(ret));
        } else if (OB_FAIL(schema_guard.get_table_schema(table_ids.at(i), table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), "table_id", table_ids.at(i));
        } else if (OB_UNLIKELY(NULL == table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table with table_id do not exist", K(ret), "table_id", table_ids.at(i));
        } else if (!table_schema->has_self_partition()) {
          // ignore the table who doesn't have partitions
        } else if (is_new_tablegroup_id(table_schema->get_tablegroup_id())) {
          // new tablegroup locality modification has been processed before, no need to check any more
        } else if (OB_FAIL(process_single_table(schema_guard,
                       *table_schema,
                       *tenant_schema,
                       hash_index_collection,
                       tenant_stat,
                       null_locality_table_match,
                       table_locality_finished))) {
          LOG_WARN("fail to process single table", K(ret));
        } else {
          is_finished = is_finished && table_locality_finished;
          LOG_DEBUG("check alter locality finished by table",
              K(ret),
              "table_id",
              table_schema->get_table_id(),
              "table_finished",
              static_cast<int32_t>(table_locality_finished),
              "tenant_finished",
              static_cast<int32_t>(is_finished));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!tenant_schema->get_previous_locality_str().empty() && null_locality_table_match &&
          null_locality_tablegroup_match) {
        rootserver::ObCommitAlterTenantLocalityArg arg;
        arg.tenant_id_ = tenant_schema->get_tenant_id();
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        if (OB_FAIL(check_stop())) {
          LOG_WARN("balancer stop", K(ret));
        } else if (OB_FAIL(common_rpc_proxy_->to(self_).commit_alter_tenant_locality(arg))) {
          LOG_WARN("fail to commit alter tennat locality", K(ret));
        } else {
        }  // no more to do
      }
    }
  }
  return ret;
}

int ObAlterLocalityChecker::try_compensate_zone_locality(
    const bool compensate_readonly_all_server, common::ObIArray<share::ObZoneReplicaNumSet>& zone_locality)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && compensate_readonly_all_server) {
    common::ObArray<share::ReplicaAttr> readonly_set;
    if (OB_FAIL(readonly_set.push_back(ReplicaAttr(ObLocalityDistribution::ALL_SERVER_CNT, 100 /*percent*/)))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
        share::ObZoneReplicaAttrSet& locality_set = zone_locality.at(i);
        if (OB_FAIL(locality_set.replica_attr_set_.set_readonly_replica_attr_array(readonly_set))) {
          LOG_WARN("fail to set readonly replica attr array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAlterLocalityChecker::get_readonly_all_server_compensation_mode(
    share::schema::ObSchemaGetterGuard& guard, const uint64_t schema_id, bool& compensate_readonly_all_server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == schema_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_id));
  } else if (is_new_tablegroup_id(schema_id)) {
    compensate_readonly_all_server = false;
  } else {
    const share::schema::ObSimpleTableSchemaV2* table_schema = nullptr;
    if (OB_FAIL(guard.get_table_schema(schema_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), "table_id", schema_id);
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema ptr is null", K(ret), "table_id", schema_id);
    } else if (OB_FAIL(table_schema->check_is_duplicated(guard, compensate_readonly_all_server))) {
      LOG_WARN("fail to check duplicate scope cluter", K(ret), K(schema_id));
    }
  }
  return ret;
}

int ObAlterLocalityChecker::check_partition_entity_locality_distribution(
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObPartitionSchema& partition_schema,
    const ZoneLocalityIArray& zone_locality, const common::ObIArray<common::ObZone>& zone_list,
    const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
    bool& locality_match)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTablePartitionIterator iter;
  const bool need_fetch_faillist = true;
  iter.set_need_fetch_faillist(need_fetch_faillist);
  ObReplicaFilterHolder partition_filter;
  if (OB_FAIL(partition_filter.set_replica_status(REPLICA_STATUS_NORMAL))) {
    LOG_WARN("fail to set in member list filter", K(ret));
  } else if (OB_FAIL(iter.init(partition_schema.get_table_id(), schema_guard, *pt_operator_))) {
    LOG_WARN("fail to init table partition iterator", K(ret));
  } else {
    int64_t paxos_num = 0;
    FOREACH_CNT_X(locality, zone_locality, OB_SUCCESS == ret)
    {
      if (OB_ISNULL(locality)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get valid locality set", K(ret), KP(locality));
      } else {
        paxos_num += locality->get_paxos_replica_num();
      }
    }
    common::ObSEArray<share::ObZoneReplicaAttrSet, 7> dest_zone_locality;
    int64_t partition_info_cnt = 0;
    ObPartitionInfo info;
    info.set_allocator(&allocator);
    bool compensate_readonly_all_server =
        (ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER == partition_schema.get_duplicate_scope() &&
            partition_schema.get_locality_str().empty());
    while (OB_SUCC(ret) && OB_SUCC(iter.next(info)) && locality_match) {
      const int64_t partition_cnt = 0;
      ObPartitionKey part_key(info.get_table_id(), info.get_partition_id(), partition_cnt);
      const int64_t* index = tenant_stat.partition_map_.get(part_key);
      if (OB_ISNULL(index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid index", K(ret), KP(index));
      } else if ((*index) >= tenant_stat.all_partition_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid (*index)", K(ret), K(*index));
      } else {
        const Partition& partition = tenant_stat.all_partition_.at(*index);
        ++partition_info_cnt;
        if (OB_FAIL(info.filter(partition_filter))) {
          LOG_WARN("fail to filter partition", K(ret));
        } else if (OB_FAIL(ObLocalityUtil::generate_designated_zone_locality(compensate_readonly_all_server,
                       partition.tablegroup_id_,
                       partition.all_pg_idx_,
                       hash_index_collection,
                       tenant_stat,
                       zone_locality,
                       dest_zone_locality))) {
          LOG_WARN("fail to get partition locality", K(ret));
        } else if (OB_FAIL(check_locality_match_replica_distribution(
                       dest_zone_locality, zone_list, info, locality_match))) {
          LOG_WARN("fail to check locality match", K(ret));
        } else if (info.in_physical_restore()) {
        } else if (OB_FAIL(check_partition_quorum_match(info, paxos_num, locality_match))) {
          LOG_WARN("fail to check partition quorum", K(ret), K(info), K(paxos_num));
        } else {
        }  // no more to do
        info.reuse();
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      int64_t part_num = 0;
      if (OB_FAIL(partition_schema.get_all_partition_num(true, part_num))) {
        LOG_WARN("fail to get partition num", K(ret), K(part_num));
      } else if (part_num != partition_info_cnt && locality_match) {
        locality_match = false;
        LOG_WARN("partition info cnt do not match part num in table schema",
            K(partition_info_cnt),
            "part cnt in schema",
            part_num,
            "schema_id",
            partition_schema.get_table_id());
      } else {
      }  // no more to do
    }
  }
  return ret;
}

// a quorum check is needed for locality modification, this func only check the quorum,
// the method to get a valid quorum value is based on the following rules:
// 1 this partition has a leader, use the quorum of the leader replica
// 2 this partition has no leader, first check if the quorum values of
//   all the paxos members are the same, then use this quorum
int ObAlterLocalityChecker::check_partition_quorum_match(
    const ObPartitionInfo& partition, const int64_t paxos_num, bool& locality_match)
{
  int ret = OB_SUCCESS;
  int64_t replica_count = partition.replica_count();
  if (replica_count <= 0 || paxos_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition or paxos_num", K(ret), K(replica_count), K(paxos_num));
  } else {
    bool quorum_match = true;
    const ObIArray<ObPartitionReplica>& replicas = partition.get_replicas_v2();
    for (int64_t i = 0; i < replica_count; i++) {
      const ObPartitionReplica& replica = replicas.at(i);
      if (replica.is_leader_by_election()) {
        quorum_match = (paxos_num == replica.get_quorum());
        break;
      } else if (quorum_match && replica.is_paxos_candidate()) {
        quorum_match = (paxos_num == replica.get_quorum());
      }
    }
    if (!quorum_match) {
      locality_match = false;
      LOG_WARN("quorum not match", K(partition), K(paxos_num));
    }
  }
  return ret;
}

int ObAlterLocalityChecker::check_locality_match_replica_distribution(
    const share::schema::ZoneLocalityIArray& zone_locality, const common::ObIArray<common::ObZone>& zone_list,
    const share::ObPartitionInfo& info, bool& table_locality_match)
{
  int ret = OB_SUCCESS;
  ObFilterLocalityUtility filter_locality_utility(
      *zone_mgr_, zone_locality, *unit_mgr_, info.get_tenant_id(), zone_list);
  if (OB_FAIL(filter_locality_utility.init())) {
    LOG_WARN("fail to init filter locality utility", K(ret));
  } else if (OB_FAIL(filter_locality_utility.filter_locality(info))) {
    LOG_WARN("fail to filter locality", K(ret), K(info));
  } else {
    table_locality_match = filter_locality_utility.get_filter_result().count() <= 0;
    LOG_INFO("check locality match replica distribution",
        "tenant_id",
        info.get_tenant_id(),
        "table_id",
        info.get_table_id(),
        "partition_id",
        info.get_partition_id(),
        "filter result",
        filter_locality_utility.get_filter_result());
  }
  return ret;
}

/* 1 for the table with a non-empty locality, this func compares the table locality and
 *   its partition distributions to check if the locality modification has been finished,
 *   if so, a rpc will be invoked to set the previous_locality column to null to finish
 *   the locality modification of this table.
 * 2 for the table with an empty locality, this func compares the tenant locality and the
 *   partition distribution of this table to check if the locality modification has been finished,
 *   any onging locality modification sets the null_locality_table_match to false to mark the
 *   tenant locality modification has not been finished.
 *   the argument null_locality_table_match is an input/output argument
 */
int ObAlterLocalityChecker::process_single_table(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTableSchema& table_schema, const share::schema::ObTenantSchema& tenant_schema,
    const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
    bool& null_locality_table_match, bool& table_locality_finished)
{
  int ret = OB_SUCCESS;
  if (table_schema.get_locality_str().empty()) {
    // use tenant locality if this table locality is empty
    if (tenant_schema.get_previous_locality_str().empty()) {
      // no locality modification on this tenant
      table_locality_finished = true;
    } else if (!null_locality_table_match) {
      // locality modification of empty tables not finished
      table_locality_finished = false;
    } else {
      bool compensate_readonly_all_server = false;
      common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
      common::ObArray<common::ObZone> zone_list;
      if (OB_FAIL(tenant_schema.get_zone_list(zone_list))) {
        LOG_WARN("fail to get zone list", K(ret));
      } else if (OB_FAIL(tenant_schema.get_zone_replica_attr_array(zone_locality))) {
        LOG_WARN("fail to get zone replica attr array", K(ret));
      } else if (OB_FAIL(get_readonly_all_server_compensation_mode(
                     schema_guard, table_schema.get_table_id(), compensate_readonly_all_server))) {
        LOG_WARN("fail to get readonly all server compensation mode", K(ret));
      } else if (OB_FAIL(try_compensate_zone_locality(compensate_readonly_all_server, zone_locality))) {
        LOG_WARN("fail to try compensate zone locality", K(ret));
      } else if (OB_FAIL(check_partition_entity_locality_distribution(schema_guard,
                     table_schema,
                     zone_locality,
                     zone_list,
                     hash_index_collection,
                     tenant_stat,
                     null_locality_table_match))) {
        LOG_WARN("fail to check table locality distribution", K(ret));
      } else if (null_locality_table_match) {
        table_locality_finished = true;
      } else {
        table_locality_finished = false;
      }
    }
  } else {
    bool table_locality_match = true;
    common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
    common::ObArray<common::ObZone> zone_list;
    if (table_schema.get_previous_locality_str().empty()) {
      // no locality modification on this table, skip
      table_locality_finished = true;
    } else if (OB_FAIL(table_schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret));
    } else if (OB_FAIL(table_schema.get_zone_list(schema_guard, zone_list))) {
      LOG_WARN("fail to get zone list", K(ret));
    } else if (OB_FAIL(check_partition_entity_locality_distribution(schema_guard,
                   table_schema,
                   zone_locality,
                   zone_list,
                   hash_index_collection,
                   tenant_stat,
                   table_locality_match))) {
      LOG_WARN("fail to check table locality distribution", K(ret));
    } else if (table_locality_match) {
      table_locality_finished = true;
      ObCommitAlterTableLocalityArg arg;
      arg.table_id_ = table_schema.get_table_id();
      arg.exec_tenant_id_ = extract_tenant_id(table_schema.get_table_id());
      if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else if (OB_FAIL(common_rpc_proxy_->to(self_).commit_alter_table_locality(arg))) {
        LOG_WARN("fail to do commit alter table locality", K(ret));
      } else {
      }  // no more to do
    } else {
      table_locality_finished = false;
    }
  }
  return ret;
}

int ObAlterLocalityChecker::check_stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAlterLocalityChecker not inited", K(ret), K(is_inited_));
  } else if (is_stop_) {
    ret = OB_CANCELED;
  } else {
  }  // do nothing
  return ret;
}

int ObAlterLocalityChecker::process_single_binding_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTablegroupSchema& tablegroup_schema,
    const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
    bool& tablegroup_locality_finished)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAlterLocalityChecker not init", K(ret), K(is_inited_));
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), K(tablegroup_id));
  } else if (!tablegroup_schema.has_self_partition()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not a binding tablegroup", K(ret), K(tablegroup_id));
  } else {
    common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
    common::ObArray<common::ObZone> zone_list;
    if (OB_FAIL(tablegroup_schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret));
    } else if (OB_FAIL(tablegroup_schema.get_zone_list(schema_guard, zone_list))) {
      LOG_WARN("fail to get zone list", K(ret));
    } else if (OB_FAIL(check_partition_entity_locality_distribution(schema_guard,
                   tablegroup_schema,
                   zone_locality,
                   zone_list,
                   hash_index_collection,
                   tenant_stat,
                   tablegroup_locality_finished))) {
      LOG_WARN("fail to check table locality distribution", K(ret));
    }
  }
  return ret;
}

int ObAlterLocalityChecker::process_single_non_binding_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTablegroupSchema& tablegroup_schema,
    const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
    bool& tablegroup_locality_finished)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAlterLocalityChecker not init", K(ret), K(is_inited_));
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), K(tablegroup_id));
  } else if (tablegroup_schema.has_self_partition()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not a  non binding tablegroup", K(ret), K(tablegroup_id));
  } else {
    common::ObArray<uint64_t> table_ids;
    const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
    if (OB_FAIL(schema_guard.get_table_ids_in_tablegroup(tenant_id, tablegroup_id, table_ids))) {
      LOG_WARN("fail to get table ids in tablegroup", K(ret), K(tablegroup_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count() && tablegroup_locality_finished; ++i) {
        bool table_locality_finished = true;
        const ObTableSchema* table_schema = NULL;
        if (OB_FAIL(check_stop())) {
          LOG_WARN("root balancer stop", K(ret));
        } else if (OB_FAIL(schema_guard.get_table_schema(table_ids.at(i), table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), "table_id", table_ids.at(i));
        } else if (OB_UNLIKELY(NULL == table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table with table_id do not exist", K(ret), "table_id", table_ids.at(i));
        } else if (!table_schema->has_self_partition()) {
          // ignore the table who doesn't have partitions
        } else if (OB_FAIL(process_single_table_under_new_tablegroup(schema_guard,
                       *table_schema,
                       tablegroup_schema,
                       hash_index_collection,
                       tenant_stat,
                       table_locality_finished))) {
          LOG_WARN("fail to process single table", K(ret));
        } else {
          tablegroup_locality_finished = tablegroup_locality_finished && table_locality_finished;
          LOG_DEBUG("check alter locality finished by table",
              K(ret),
              "table_id",
              table_schema->get_table_id(),
              "tablegroup_id",
              table_schema->get_tablegroup_id(),
              "table_finished",
              static_cast<int32_t>(table_locality_finished),
              "tablegroup_finished",
              static_cast<int32_t>(tablegroup_locality_finished));
        }
      }
    }
  }
  return ret;
}

int ObAlterLocalityChecker::process_single_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTablegroupSchema& tablegroup_schema,
    const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
    bool& tablegroup_locality_finished)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAlterLocalityChecker not init", K(ret), K(is_inited_));
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), K(tablegroup_id));
  } else if (!tablegroup_schema.has_self_partition()) {
    if (OB_FAIL(process_single_non_binding_tablegroup(
            schema_guard, tablegroup_schema, hash_index_collection, tenant_stat, tablegroup_locality_finished))) {
      LOG_WARN("fail to process non binding single tablegroup", K(ret), K(tablegroup_id));
    }
  } else {
    if (OB_FAIL(process_single_binding_tablegroup(
            schema_guard, tablegroup_schema, hash_index_collection, tenant_stat, tablegroup_locality_finished))) {
      LOG_WARN("fail to process binding single tablegroup", K(ret), K(tablegroup_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (!tablegroup_schema.get_previous_locality_str().empty() && tablegroup_locality_finished) {
      rootserver::ObCommitAlterTablegroupLocalityArg arg;
      arg.tablegroup_id_ = tablegroup_schema.get_tablegroup_id();
      arg.exec_tenant_id_ = extract_tenant_id(tablegroup_id);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else if (OB_FAIL(common_rpc_proxy_->to(self_).commit_alter_tablegroup_locality(arg))) {
        LOG_WARN("fail to commit alter tablegroup locality", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObAlterLocalityChecker::process_single_table_under_new_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTableSchema& table_schema, const share::schema::ObTablegroupSchema& tablegroup_schema,
    const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
    bool& table_locality_finished)
{
  int ret = OB_SUCCESS;
  if (!is_new_tablegroup_id(table_schema.get_tablegroup_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), "tablegroup_id", table_schema.get_tablegroup_id());
  } else if (!table_schema.get_locality_str().empty()) {
    // table in new tablegroup, locality should be empty
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("locality should be empty", K(ret));
  } else {
    // when the table locality is empty, we use its tablegroup locality instead
    ObArray<share::ObZoneReplicaNumSet> zone_locality;
    common::ObArray<common::ObZone> zone_list;
    if (OB_FAIL(tablegroup_schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
      LOG_WARN("fail to get zone replica num array", K(ret));
    } else if (OB_FAIL(tablegroup_schema.get_zone_list(schema_guard, zone_list))) {
      LOG_WARN("fail to get zone list", K(ret));
    } else if (OB_FAIL(check_partition_entity_locality_distribution(schema_guard,
                   table_schema,
                   zone_locality,
                   zone_list,
                   hash_index_collection,
                   tenant_stat,
                   table_locality_finished))) {
      LOG_WARN("fail to check table locality distribution", K(ret));
    }
  }
  return ret;
}
}  // end namespace rootserver
}  // namespace oceanbase
