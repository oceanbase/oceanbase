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

#include "ob_root_utils.h"
#include "ob_balance_info.h"
#include "ob_unit_manager.h"
#include "lib/json/ob_json.h"
#include "lib/string/ob_sql_string.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"  // ObMultiVersionSchemaService
#include "share/schema/ob_schema_getter_guard.h"           // ObSchemaGetterGuard
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_alloc_replica_strategy.h"
#include "rootserver/ob_ddl_service.h"
#include "observer/ob_server_struct.h"
#include "share/ob_multi_cluster_util.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::obrpc;

int ObTenantUtils::get_tenant_ids(ObMultiVersionSchemaService* schema_service, ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service not init", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  }
  return ret;
}

int ObTenantUtils::get_tenant_ids(
    ObMultiVersionSchemaService* schema_service, int64_t& sys_schema_version, ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service not init", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, sys_schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret));
  }
  return ret;
}
/*
 * Define of small_tenant:
 * Small_tenant will facilitate the operations: leader_coordinate, balance, etc. small_tenant will be special handled by
 * RS. All partitions in small tenant will be gathered together as in the same partition group while doing leader
 * corrdinate and balance. Exclude L units, in primary cluster:
 *  ## normal_tenant
 *     ### Tenants of single unit or multi unit are considered as normal tenant while primary zone is setted (including
 * 'RANDOM' value) on database, tablegroup or table.
 *  ## small_tenant
 *     ### Small_tenant's every resource_pool has one and only one unit, if any resource_pool's unit num is larger than
 * 1, it can not be small_tenant.
 *     ### The primary_zone of any of database,tablegroup,table under the tenant must be empty.
 *
 * In standby cluster: all tenants are normal tenant.
 *
 * To avoid balance schema error, tenant during physical recovery are normal tenant,
 */
int ObTenantUtils::check_small_tenant(const uint64_t tenant_id, bool& small_tenant)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema* tenant_schema = NULL;
  share::schema::ObMultiVersionSchemaService* schema_service = NULL;
  rootserver::ObRootService* root_service = NULL;
  common::ObSEArray<share::ObZoneReplicaAttrSet, 7> zone_locality;
  const ObSysVariableSchema* sys_variable = nullptr;
  const ObSysVarSchema* pz_count_schema = nullptr;
  const common::ObString var_name(share::OB_SV__PRIMARY_ZONE_ENTITY_COUNT);
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(tenant_id), K(ret));
    // prevent batch migration failure and failure of maintenance.
    small_tenant = false;
  } else if (nullptr == (schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", K(ret));
  } else if (nullptr == (root_service = GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service ptr is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant schema null", K(ret), K(tenant_id), KP(tenant_schema));
  } else if (tenant_schema->is_restore()) {
    small_tenant = false;
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else if (tenant_schema->get_primary_zone().empty() ||
             tenant_schema->get_primary_zone() == ObString(OB_RANDOM_PRIMARY_ZONE)) {
    // it is not small_tenant while tenant's locality is empty or RANDOM
    small_tenant = false;
  } else {
    small_tenant = true;
    // primary zone condition
    const ObIArray<ObZoneScore>& primary_zone_array = tenant_schema->get_primary_zone_array();
    if (primary_zone_array.count() <= 0) {
      small_tenant = false;  // empty or random
    } else {
      int64_t high_primary_zone_counter = 0;
      const ObZoneScore& sample_zone = primary_zone_array.at(0);
      for (int64_t i = 0; i < primary_zone_array.count(); ++i) {
        if (sample_zone.score_ == primary_zone_array.at(i).score_) {
          high_primary_zone_counter++;
        } else {
          break;
        }
      }
      if (high_primary_zone_counter > 1) {
        small_tenant = false;
      }
    }
    // resource pool condition
    rootserver::ObUnitManager& unit_mgr = root_service->get_unit_mgr();
    ObArray<share::ObResourcePool> pools;
    if (!small_tenant) {
      // not a small tenant, no need to check any more
    } else if (OB_FAIL(unit_mgr.get_pools_of_tenant(tenant_id, pools))) {
      LOG_WARN("fail to get pools of tenant", K(ret), K(tenant_id));
    } else {
      const int64_t small_tenant_unit_limit = 1;
      FOREACH_CNT_X(pool, pools, OB_SUCCESS == ret && small_tenant)
      {
        // after 14x. tenant has F and L resource_pool, only take into account F pool.
        if (REPLICA_TYPE_LOGONLY != pool->replica_type_ && pool->unit_count_ > small_tenant_unit_limit) {
          small_tenant = false;
        }
      }
    }
    // tenant locality condition
    if (OB_FAIL(ret)) {
    } else if (!small_tenant) {
      // not a small, no need to check any more
    } else {
      for (int64_t i = 0; small_tenant && i < zone_locality.count(); ++i) {
        const share::ObZoneReplicaAttrSet& this_locality = zone_locality.at(i);
        if (this_locality.zone_set_.count() > 1) {
          small_tenant = false;
        }
      }
    }
    // check primary zone and locality condition
    if (OB_FAIL(ret)) {
    } else if (!small_tenant) {
      // not a small, no need to check any more
    } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable))) {
      LOG_WARN("fail to get sys variable schema", K(ret));
    } else if (OB_UNLIKELY(nullptr == sys_variable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys variable ptr is null", K(ret));
    } else {
      bool direct_check = false;
      common::ObObj pz_obj;
      int64_t pz_value = -2;
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      ret = sys_variable->get_sysvar_schema(var_name, pz_count_schema);
      if (OB_ERR_SYS_VARIABLE_UNKNOWN == ret) {
        direct_check = true;
      } else if (OB_SUCCESS == ret) {
        if (nullptr == pz_count_schema) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("primary zone count schema ptr is null", K(ret));
        } else if (OB_FAIL(pz_count_schema->get_value(&allocator, nullptr /*time zone info*/, pz_obj))) {
          LOG_WARN("fail to get value", K(ret));
        } else if (OB_FAIL(pz_obj.get_int(pz_value))) {
          LOG_WARN("fail to get int from pz obj", K(ret));
        } else if (pz_value < -1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pz value unexpected", K(ret));
        } else if (-1 == pz_value) {
          direct_check = true;
        } else if (0 == pz_value) {
          small_tenant = true;
        } else {
          small_tenant = false;
        }
      } else {
        LOG_WARN("fail to get sysvar schema", K(ret));
      }
      if (OB_FAIL(ret)) {
        // already failed
      } else if (direct_check) {
        if (OB_FAIL(check_small_tenant_primary_zone_and_locality_condition(schema_guard, tenant_id, small_tenant))) {
          LOG_WARN("fail to check", K(ret), K(tenant_id));
        }
      } else {
        // already check by primary zone count
      }
    }
  }
  return ret;
}

int ObTenantUtils::check_small_tenant_primary_zone_and_locality_condition(
    share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id, bool& small_tenant)
{
  int ret = OB_SUCCESS;
  common::ObArray<const ObDatabaseSchema*> database_schemas;
  common::ObArray<const ObSimpleTableSchemaV2*> table_schemas;
  common::ObArray<const ObTablegroupSchema*> tablegroup_schemas;
  small_tenant = true;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
    LOG_WARN("get table schemas in tenant failed", K(ret));
  } else {
    common::ObSEArray<share::ObZoneReplicaAttrSet, 7> zone_locality;
    // table schemas
    for (int64_t i = 0; small_tenant && OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
      zone_locality.reset();
      if (NULL == table_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret));
      } else if (table_schema->get_primary_zone().empty()) {
        // go on next
      } else {
        small_tenant = false;
      }
      if (OB_FAIL(ret)) {
      } else if (!small_tenant) {
      } else if (OB_FAIL(table_schema->get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
        LOG_WARN("fail to get zone locality", K(ret));
      } else {
        for (int64_t i = 0; small_tenant && i < zone_locality.count(); ++i) {
          const share::ObZoneReplicaAttrSet& this_locality = zone_locality.at(i);
          if (this_locality.zone_set_.count() > 1) {
            small_tenant = false;
          }
        }
      }
    }
    // databases
    if (OB_FAIL(ret)) {
    } else if (!small_tenant) {
    } else if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id, database_schemas))) {
      if (OB_TENANT_NOT_EXIST == ret && OB_SYS_TENANT_ID != tenant_id) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get databases", K(ret), K(tenant_id));
      }
    } else {
      for (int64_t i = 0; small_tenant && OB_SUCC(ret) && i < database_schemas.count(); ++i) {
        const ObDatabaseSchema* database_schema = database_schemas.at(i);
        if (NULL == database_schema) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database schema is null", K(ret));
        } else if (database_schema->get_primary_zone().empty()) {
          // go on next
        } else {
          small_tenant = false;
        }
      }
    }
    // tablegroups
    if (OB_FAIL(ret)) {
    } else if (!small_tenant) {
    } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroup_schemas))) {
      if (OB_TENANT_NOT_EXIST == ret && OB_SYS_TENANT_ID != tenant_id) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get tablegroup", K(ret), K(tenant_id));
      }
    } else {
      for (int64_t i = 0; small_tenant && OB_SUCC(ret) && i < tablegroup_schemas.count(); ++i) {
        const ObTablegroupSchema* tablegroup_schema = tablegroup_schemas.at(i);
        zone_locality.reset();
        if (NULL == tablegroup_schema) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database schema is null", K(ret));
        } else if (tablegroup_schema->get_primary_zone().empty()) {
          // go on next
        } else {
          small_tenant = false;
        }
        if (OB_FAIL(ret)) {
        } else if (!small_tenant) {
        } else if (OB_FAIL(tablegroup_schema->get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
          LOG_WARN("fail to get zone locality", K(ret));
        } else {
          for (int64_t i = 0; small_tenant && i < zone_locality.count(); ++i) {
            const share::ObZoneReplicaAttrSet& this_locality = zone_locality.at(i);
            if (this_locality.zone_set_.count() > 1) {
              small_tenant = false;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantUtils::remove_ineffective_task(ObMySQLTransaction& trans, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE TENANT_ID = %lu", OB_ALL_DDL_HELPER_TNAME, tenant_id))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write", KR(ret));
  }
  ROOTSERVICE_EVENT_ADD("standby", "remove_ddl_operation", K(ret), K(sql), K(tenant_id), K(affected_rows));
  return ret;
}

bool ObTenantUtils::is_balance_target_schema(const share::schema::ObTableSchema& table_schema)
{
  return USER_TABLE == table_schema.get_table_type() || TMP_TABLE == table_schema.get_table_type() ||
         MATERIALIZED_VIEW == table_schema.get_table_type() || TMP_TABLE_ORA_SESS == table_schema.get_table_type() ||
         TMP_TABLE_ORA_TRX == table_schema.get_table_type() || TMP_TABLE_ALL == table_schema.get_table_type() ||
         AUX_VERTIAL_PARTITION_TABLE == table_schema.get_table_type() || table_schema.is_global_index_table();
}

// Ensure:type transform will not affect cluster availability.
// Check method: F or L paxos replicas are enough to tolerate single server disaster.
bool ObBalanceTaskBuilder::can_do_type_transform(TenantBalanceStat& ts, const Partition& partition,
    const Replica& replica, ObReplicaType dest_type, int64_t dest_memstore_percent)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (!ObReplicaTypeCheck::is_replica_type_valid(dest_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_type));
  } else if (replica.replica_type_ == dest_type && replica.memstore_percent_ == dest_memstore_percent) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid argument, no need to do type transform", K(ret), K(replica), K(dest_type), K(dest_memstore_percent));
  } else if (REPLICA_TYPE_FULL == dest_type && replica.replica_type_ == dest_type &&
             replica.memstore_percent_ != dest_memstore_percent) {
    // replica's type transform F to D.
    int64_t full_replica_num = 0;
    int64_t full_replica_memstore_percent_0_num = 0;
    FOR_BEGIN_END_E(pr, partition, ts.all_replica_, OB_SUCC(ret))
    {
      if (pr->replica_type_ == REPLICA_TYPE_FULL) {
        full_replica_num++;
      }
      if (pr->replica_type_ == REPLICA_TYPE_FULL && 0 == pr->memstore_percent_) {
        full_replica_memstore_percent_0_num++;
      }
    }
    bret = true;
    if (1 >= full_replica_num - full_replica_memstore_percent_0_num && 0 == dest_memstore_percent) {
      bret = false;
      LOG_WARN("can not to do type transform. full replica at least one",
          K(partition),
          K(full_replica_num),
          K(full_replica_memstore_percent_0_num),
          K(replica),
          K(dest_type),
          K(dest_memstore_percent));
    }
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_)) {
    // non-paxos replica transform
    bret = true;
  } else {
    int64_t full_replica_num = 0;
    int64_t paxos_replica_num = 0;
    FOR_BEGIN_END_E(pr, partition, ts.all_replica_, OB_SUCC(ret))
    {
      if (pr->is_valid_paxos_member()) {
        paxos_replica_num++;
        if (pr->replica_type_ == REPLICA_TYPE_FULL) {
          full_replica_num++;
        }
      }
    }
    bool full_replica_enough = true;
    if (OB_SUCC(ret) && REPLICA_TYPE_FULL == replica.replica_type_) {
      // ensure the num of F replica is at least 2 after transform F to other
      int64_t schema_full_replica_count = 0;
      if (OB_FAIL(ts.get_full_replica_num(partition, schema_full_replica_count))) {
        LOG_WARN("fail to get full replica count", K(ret), K(partition));
      } else if (schema_full_replica_count < OB_MIN_SAFE_COPY_COUNT) {
        // while schema_full_replica_count is less than 3:
        // if schema_full_replica_count = 1, full_replica_num must larger than 1;
        // if schema_full_replica_count = 2, full_replica_num must larger than 2;
        if (full_replica_num <= schema_full_replica_count) {
          full_replica_enough = false;
          LOG_WARN("can not to do type transform. full replica is not enough",
              K(partition),
              K(schema_full_replica_count),
              K(full_replica_num),
              K(replica),
              K(dest_type));
        }
      } else {
        if (full_replica_num >= OB_MIN_SAFE_COPY_COUNT) {
          // after transform, full_replica_num must be at least 2 more.
          // nothing todo
        } else {
          full_replica_enough = false;
          LOG_WARN("can not to do type transform. full replica is not enough",
              K(partition),
              K(schema_full_replica_count),
              K(full_replica_num),
              K(replica),
              K(dest_type));
        }
      }
    }
    bool paxos_replica_enough = true;
    if (OB_SUCC(ret) && ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_) &&
        !ObReplicaTypeCheck::is_paxos_replica_V2(dest_type)) {
      // if replica_type transform from paxos to non-paxos, the paxos replica num must larger than majority.
      int64_t quorum = 0;
      if (OB_FAIL(ts.get_transform_quorum_size(partition, replica.zone_, replica.replica_type_, dest_type, quorum))) {
        LOG_WARN("fail to get quorum", K(ret), K(partition));
      } else if (quorum < OB_MIN_SAFE_COPY_COUNT) {
        if (paxos_replica_num <= 1) {
          // paxos replica num is zero after type transform
          paxos_replica_enough = false;
          LOG_WARN("can not to do type transform. paxos replica is not enough",
              K(partition),
              K(quorum),
              K(paxos_replica_num),
              K(replica),
              K(dest_type));
        }
      } else {
        if (paxos_replica_num < majority(quorum) + 2) {
          // can not do disaster recovery after transform
          paxos_replica_enough = false;
          LOG_WARN("can not to do type transform. paxos replica is not enough",
              K(partition),
              K(quorum),
              K(paxos_replica_num),
              K(replica),
              K(dest_type));
        }
      }
    }
    if (OB_SUCC(ret)) {
      bret = full_replica_enough && paxos_replica_enough;
    }
  }
  return bret;
}

bool ObRootServiceRoleChecker::is_rootserver(share::ObIPartPropertyGetter* prop_getter)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  ObRole role;
  const ObPartitionKey part_key(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM);

  if (OB_ISNULL(prop_getter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(prop_getter));
  } else if (OB_FAIL(prop_getter->get_role(part_key, role))) {
    LOG_WARN("get __all_core_table role failed", K(part_key), K(ret));
  } else {
    bret = (is_strong_leader(role));
    LOG_DEBUG("get __all_core_table role", K(part_key), K(role), K(bret));
  }
  return bret;
}

const char* ObRootBalanceHelp::BalanceItem[] = {"ENABLE_REBUILD",
    "ENABLE_EMERGENCY_REPLICATE",
    "ENABLE_TYPE_TRANSFORM",
    "ENABLE_DELETE_REDUNDANT",
    "ENABLE_REPLICATE_TO_UNIT",
    "ENABLE_SHRINK",
    "ENABLE_REPLICATE",
    "ENABLE_COORDINATE_PG",
    "ENABLE_MIGRATE_TO_UNIT",
    "ENABLE_PARTITION_BALANCE",
    "ENABLE_UNIT_BALANCE",
    "ENABLE_SERVER_BALANCE",
    "ENABLE_CANCEL_UNIT_MIGRATION",
    "ENABLE_MODIFY_QUORUM",
    "ENABLE_STOP_SERVER",
    ""};

int ObRootBalanceHelp::parse_balance_info(const ObString& json_str, ObRootBalanceHelp::BalanceController& switch_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  json::Parser parser;
  json::Value* data = NULL;
  switch_info.reset();
  if (json_str.empty()) {
    switch_info.init();
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(json_str.ptr(), json_str.length(), data))) {
    LOG_WARN("parse json failed", K(ret), K(json_str));
  } else if (NULL == data) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else if (json::JT_OBJECT != data->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json format", K(ret), K(json_str));
  } else {
    DLIST_FOREACH_X(it, data->get_object(), OB_SUCC(ret))
    {
      bool find = false;
      for (int64_t i = 0; i < ARRAYSIZEOF(BalanceItem) - 1 && !find && OB_SUCC(ret); i++) {
        if (it->name_.case_compare(BalanceItem[i]) == 0) {
          find = true;
          if (json::JT_STRING != it->value_->get_type()) {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("invalid config", K(ret), K(json_str));
          } else if (it->value_->get_string().case_compare("true") == 0) {
            switch_info.set(i, true);
          } else if (it->value_->get_string().case_compare("false") == 0) {
            switch_info.set(i, false);
          } else {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("invalid config", K(ret), K(json_str));
          }
        }  // if (it->name_.case_compare
      }    // for (int64_t i = 0;
      if (!find) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("get invalid token", K(ret), K(*it));
      }
    }  // DLIST_FOREACH_X(it
  }
  return ret;
}

int ObLocalityUtil::parse_zone_list_from_locality_str(ObString& locality_str, ObIArray<ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  if (locality_str.empty()) {
    // nothing todo
  } else {
    ObArray<ObString> sub_locality;
    ObString trimed_string = locality_str.trim();
    if (OB_FAIL(split_on(trimed_string, ',', sub_locality))) {
      LOG_WARN("fail to split string", K(ret), "locality", trimed_string);
    } else {
      ObArray<ObString> zone_infos;
      ObZone tmp_zone;
      for (int64_t i = 0; i < sub_locality.count() && OB_SUCC(ret); i++) {
        zone_infos.reset();
        tmp_zone.reset();
        ObString sub_trimed_string = sub_locality.at(i).trim();
        if (OB_FAIL(split_on(sub_trimed_string, '@', zone_infos))) {
          LOG_WARN("fail to split on string", K(ret), "string", sub_trimed_string);
        } else if (zone_infos.count() != 2) {
          // FULL{1},READONLY{1}@z3;
          // nothing todo
        } else if (OB_FAIL(tmp_zone.assign(zone_infos.at(1)))) {
          LOG_WARN("fail to assign zone", K(ret), K(zone_infos));
        } else if (has_exist_in_array(zone_list, tmp_zone)) {
          // nothint todo
        } else if (OB_FAIL(zone_list.push_back(tmp_zone))) {
          LOG_WARN("fail to push back", K(ret));
        }
        LOG_DEBUG(
            "split sub locality", K(ret), K(sub_trimed_string), K(tmp_zone), K(zone_infos), K(tmp_zone), K(zone_list));
      }
    }
  }
  return ret;
}

int ObLocalityUtil::generate_designated_zone_locality(const bool duplicate_table_compensate_readonly_all_server,
    const uint64_t tablegroup_id,  // tablegroup id must be invalid
    const common::ObPartitionKey& pkey, const balancer::HashIndexCollection& hash_index_collection,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& actual_zone_locality,
    common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality)
{
  int ret = OB_SUCCESS;
  designated_zone_locality.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < actual_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet& locality_set = actual_zone_locality.at(i);
    if (locality_set.zone_set_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone set count unexpected", K(ret), K(actual_zone_locality));
    } else if (1 == locality_set.zone_set_.count()) {
      if (OB_FAIL(designated_zone_locality.push_back(locality_set))) {
        LOG_WARN("fail to push back");
      }
    } else {  // mix_locality
      if (OB_FAIL(do_generate_designated_zone_locality(
              tablegroup_id, pkey, hash_index_collection, locality_set, designated_zone_locality))) {
        LOG_WARN("fail to do generate designated zone locality",
            KR(ret),
            K(tablegroup_id),
            K(pkey),
            K(locality_set),
            K(designated_zone_locality));
      }
    }
  }
  if (OB_SUCC(ret) && duplicate_table_compensate_readonly_all_server) {
    common::ObArray<share::ReplicaAttr> readonly_set;
    if (OB_FAIL(readonly_set.push_back(ReplicaAttr(ObLocalityDistribution::ALL_SERVER_CNT, 100 /*percent*/)))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < designated_zone_locality.count(); ++i) {
        share::ObZoneReplicaAttrSet& locality_set = designated_zone_locality.at(i);
        if (OB_FAIL(locality_set.replica_attr_set_.set_readonly_replica_attr_array(readonly_set))) {
          LOG_WARN("fail to set readonly replica attr array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityUtil::generate_designated_zone_locality(const bool duplicate_table_compensate_readonly_all_server,
    const uint64_t tablegroup_id,  // tablegroup id must be invalid
    const int64_t all_pg_idx, const balancer::HashIndexCollection& hash_index_collection,
    balancer::ITenantStatFinder& tenant_stat, const common::ObIArray<share::ObZoneReplicaAttrSet>& actual_zone_locality,
    common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality)
{
  int ret = OB_SUCCESS;
  designated_zone_locality.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < actual_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet& locality_set = actual_zone_locality.at(i);
    if (locality_set.zone_set_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone set count unexpected", K(ret), K(actual_zone_locality));
    } else if (1 == locality_set.zone_set_.count()) {
      if (OB_FAIL(designated_zone_locality.push_back(locality_set))) {
        LOG_WARN("fail to push back locality set", KR(ret), K(locality_set));
      }
    } else {  // mix locality
      if (OB_FAIL(do_generate_designated_zone_locality(
              tablegroup_id, all_pg_idx, hash_index_collection, tenant_stat, locality_set, designated_zone_locality))) {
        LOG_WARN("fail to do generate designated zone locality",
            KR(ret),
            K(tablegroup_id),
            K(all_pg_idx),
            K(actual_zone_locality),
            K(locality_set));
      }
    }
  }
  if (OB_SUCC(ret) && duplicate_table_compensate_readonly_all_server) {
    common::ObArray<share::ReplicaAttr> readonly_set;
    if (OB_FAIL(readonly_set.push_back(ReplicaAttr(ObLocalityDistribution::ALL_SERVER_CNT, 100 /*percent*/)))) {
      LOG_WARN("fail to push back redonly set", KR(ret), "count", readonly_set.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < designated_zone_locality.count(); ++i) {
        share::ObZoneReplicaAttrSet& locality_set = designated_zone_locality.at(i);
        if (OB_FAIL(locality_set.replica_attr_set_.set_readonly_replica_attr_array(readonly_set))) {
          LOG_WARN("fail to set readonly replica attr array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityUtil::do_generate_pure_designated_zone_locality(const int64_t x_axis,
    const share::ObZoneReplicaAttrSet& this_mix_locality,
    common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObZone, 7> zone_array;
  common::ObSEArray<SingleReplica, 7> task_array;
  // 1. generate base data
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < this_mix_locality.zone_set_.count(); ++i) {
      if (OB_FAIL(zone_array.push_back(this_mix_locality.zone_set_.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    std::sort(zone_array.begin(), zone_array.end());
    const ObReplicaAttrSet& replica_attr_set = this_mix_locality.replica_attr_set_;
    const ObIArray<ReplicaAttr>& full_set = replica_attr_set.get_full_replica_attr_array();
    const ObIArray<ReplicaAttr>& logonly_set = replica_attr_set.get_logonly_replica_attr_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < full_set.count(); ++i) {
      const share::ReplicaAttr& replica = full_set.at(i);
      const int64_t replica_num = replica.num_;
      for (int64_t j = 0; OB_SUCC(ret) && j < replica_num; ++j) {
        if (OB_FAIL(task_array.push_back(SingleReplica(REPLICA_TYPE_FULL, replica.memstore_percent_)))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < logonly_set.count(); ++i) {
      const share::ReplicaAttr& replica = logonly_set.at(i);
      const int64_t replica_num = replica.num_;
      for (int64_t j = 0; OB_SUCC(ret) && j < replica_num; ++j) {
        if (OB_FAIL(task_array.push_back(SingleReplica(REPLICA_TYPE_LOGONLY, replica.memstore_percent_)))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      SingleReplicaSort op;
      std::sort(task_array.begin(), task_array.end(), op);
      if (OB_FAIL(op.get_ret())) {
        LOG_WARN("fail to sort task array");
      } else if (task_array.count() != zone_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array count not match", K(ret), K(zone_array), K(task_array));
      }
    }
  }
  // 2. gen final result
  if (OB_SUCC(ret)) {
    const int64_t array_cnt = zone_array.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_array.count(); ++i) {
      share::ObZoneReplicaAttrSet locality_set;
      const common::ObZone& this_zone = zone_array.at((i + x_axis) % array_cnt);
      const SingleReplica& this_replica = task_array.at(i);
      if (FALSE_IT(locality_set.zone_ = this_zone)) {
        // will never by here
      } else if (OB_FAIL(locality_set.zone_set_.push_back(this_zone))) {
        LOG_WARN("fail to push back", K(ret), K(this_zone));
      } else if (REPLICA_TYPE_FULL == this_replica.replica_type_) {
        if (OB_FAIL(locality_set.replica_attr_set_.add_full_replica_num(
                share::ReplicaAttr(1 /*num*/, this_replica.memstore_percent_)))) {
          LOG_WARN("fail to add full replica num", K(ret));
        }
      } else if (REPLICA_TYPE_LOGONLY == this_replica.replica_type_) {
        if (OB_FAIL(locality_set.replica_attr_set_.add_logonly_replica_num(
                share::ReplicaAttr(1 /*num*/, this_replica.memstore_percent_)))) {
          LOG_WARN("fail to add logonly replica num", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica type unexpected", K(ret), K(this_replica));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(designated_zone_locality.push_back(locality_set))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityUtil::do_generate_designated_zone_locality(const uint64_t tablegroup_id,
    const common::ObPartitionKey& pkey, const balancer::HashIndexCollection& hash_index_collection,
    const share::ObZoneReplicaAttrSet& this_mix_locality,
    common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality)
{
  int ret = OB_SUCCESS;
  int64_t x_axis = -1;
  // 0. calc x_axis
  if (this_mix_locality.zone_set_.count() <= 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone set unexpected", K(ret), K(this_mix_locality));
  } else if (OB_SYS_TABLEGROUP_ID == extract_pure_id(tablegroup_id) || OB_SYS_TENANT_ID == pkey.get_tenant_id()) {
    x_axis = gen_locality_seed(pkey.get_tenant_id());
  } else {
    balancer::HashIndexMapItem hash_map_item;
    int tmp_ret = hash_index_collection.get_partition_index(pkey, hash_map_item);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      x_axis = gen_locality_seed(pkey.get_table_id());
    } else if (OB_SUCCESS != tmp_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get from hash collection", K(ret));
    } else {
      if (OB_FAIL(hash_map_item.get_balance_group_zone_x_axis(this_mix_locality.zone_set_.count(), x_axis))) {
        LOG_WARN("fail to get balance group zone axis", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_generate_pure_designated_zone_locality(x_axis, this_mix_locality, designated_zone_locality))) {
      LOG_WARN("fail to do generate pure designated zone locality",
          KR(ret),
          K(x_axis),
          K(this_mix_locality),
          K(designated_zone_locality));
    }
  }
  return ret;
}

int ObLocalityUtil::do_generate_designated_zone_locality(const uint64_t tablegroup_id, const int64_t all_pg_idx,
    const balancer::HashIndexCollection& hash_index_collection, balancer::ITenantStatFinder& tenant_stat,
    const share::ObZoneReplicaAttrSet& this_mix_locality,
    common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality)
{
  int ret = OB_SUCCESS;
  common::ObPartitionKey primary_pkey;
  int64_t x_axis = -1;
  // 0. calc x_axis
  if (this_mix_locality.zone_set_.count() <= 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone set unexpected", K(ret), K(this_mix_locality));
  } else if (OB_SYS_TABLEGROUP_ID == extract_pure_id(tablegroup_id) ||
             OB_SYS_TENANT_ID == tenant_stat.get_tenant_id()) {
    x_axis = gen_locality_seed(tenant_stat.get_tenant_id());
  } else if (OB_FAIL(tenant_stat.get_primary_partition_key(all_pg_idx, primary_pkey))) {
    LOG_WARN("fail to get primary partition pkey", K(ret), K(tablegroup_id), K(all_pg_idx));
  } else {
    balancer::HashIndexMapItem hash_map_item;
    int tmp_ret = hash_index_collection.get_partition_index(primary_pkey, hash_map_item);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      x_axis = gen_locality_seed(primary_pkey.get_table_id());
    } else if (OB_SUCCESS != tmp_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get from hash collection", K(ret));
    } else {
      if (OB_FAIL(hash_map_item.get_balance_group_zone_x_axis(this_mix_locality.zone_set_.count(), x_axis))) {
        LOG_WARN("fail to get balance group zone axis", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_generate_pure_designated_zone_locality(x_axis, this_mix_locality, designated_zone_locality))) {
      LOG_WARN("fail to do generate pure designated zone locality",
          KR(ret),
          K(x_axis),
          K(this_mix_locality),
          K(designated_zone_locality));
    }
  }
  return ret;
}

int ObTenantGroupParser::parse_tenant_groups(
    const common::ObString& ttg_str, common::ObIArray<TenantNameGroup>& tenant_groups)
{
  int ret = OB_SUCCESS;
  int64_t end = ttg_str.length();
  int64_t pos = 0;
  while (OB_SUCC(ret) && pos < end) {
    if (OB_FAIL(get_next_tenant_group(pos, end, ttg_str, tenant_groups))) {
      LOG_WARN("fail to parse single tenant group", K(ret), K(ttg_str));
    } else if (OB_FAIL(jump_to_next_ttg(pos, end, ttg_str))) {
      LOG_WARN("fail to jump to next", K(ret));
    }
  }
  return ret;
}

int ObTenantGroupParser::get_next_tenant_group(
    int64_t& pos, const int64_t end, const common::ObString& ttg_str, common::ObIArray<TenantNameGroup>& tenant_groups)
{
  int ret = OB_SUCCESS;
  jump_over_space(pos, end, ttg_str);
  if (pos >= end) {
    // reach to the end
  } else if ('(' != ttg_str[pos]) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else {
    // begin with a left brace
    ++pos;
    jump_over_space(pos, end, ttg_str);
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('"' == ttg_str[pos]) {
      ret = parse_vector_tenant_group(pos, end, ttg_str, tenant_groups);
    } else if ('(' == ttg_str[pos]) {
      ret = parse_matrix_tenant_group(pos, end, ttg_str, tenant_groups);
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
    }
    // end with a right brace
    if (OB_SUCC(ret)) {
      jump_over_space(pos, end, ttg_str);
      if (pos >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else if (')' != ttg_str[pos]) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else {
        ++pos;
      }
    }
  }
  return ret;
}

int ObTenantGroupParser::get_next_tenant_name(
    int64_t& pos, const int64_t end, const common::ObString& ttg_str, common::ObString& tenant_name)
{
  int ret = OB_SUCCESS;
  jump_over_space(pos, end, ttg_str);
  if (pos >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else if ('"' != ttg_str[pos]) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else {
    ++pos;  // jump over the " symbol
    jump_over_space(pos, end, ttg_str);
    int64_t start = pos;
    while (pos < end && !isspace(ttg_str[pos]) && '"' != ttg_str[pos]) {
      ++pos;
    }
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else {
      tenant_name.assign_ptr(ttg_str.ptr() + start, static_cast<int32_t>(pos - start));
    }
    // end with a " symbol
    if (OB_SUCC(ret)) {
      jump_over_space(pos, end, ttg_str);
      if (pos >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else if ('"' != ttg_str[pos]) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else {
        ++pos;
      }
    }
  }
  return ret;
}

int ObTenantGroupParser::jump_to_next_tenant_name(int64_t& pos, const int64_t end, const common::ObString& ttg_str)
{
  int ret = OB_SUCCESS;
  jump_over_space(pos, end, ttg_str);
  if (pos >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
  } else if (',' == ttg_str[pos]) {
    ++pos;
    jump_over_space(pos, end, ttg_str);
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
    } else if ('"' != ttg_str[pos]) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
    } else {
    }  // good, come across a " symbol
  } else if (')' == ttg_str[pos]) {
    // good, reach the end of this vector
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
  }
  return ret;
}

int ObTenantGroupParser::parse_tenant_vector(
    int64_t& pos, const int64_t end, const common::ObString& ttg_str, common::ObIArray<common::ObString>& tenant_names)
{
  int ret = OB_SUCCESS;
  tenant_names.reset();
  while (OB_SUCC(ret) && pos < end && ')' != ttg_str[pos]) {
    ObString tenant_name;
    if (OB_FAIL(get_next_tenant_name(pos, end, ttg_str, tenant_name))) {
      LOG_WARN("fail to get next tenant name", K(ret));
    } else if (has_exist_in_array(all_tenant_names_, tenant_name)) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if (OB_FAIL(tenant_names.push_back(tenant_name))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(all_tenant_names_.push_back(tenant_name))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(jump_to_next_tenant_name(pos, end, ttg_str))) {
      LOG_WARN("fail to jump to next tenant name", K(ret));
    } else {
    }  // next loop round
  }
  return ret;
}

int ObTenantGroupParser::parse_vector_tenant_group(
    int64_t& pos, const int64_t end, const common::ObString& ttg_str, common::ObIArray<TenantNameGroup>& tenant_groups)
{
  int ret = OB_SUCCESS;
  TenantNameGroup tenant_group;
  common::ObArray<common::ObString> tenant_names;
  if (OB_FAIL(parse_tenant_vector(pos, end, ttg_str, tenant_names))) {
    LOG_WARN("fail to parse tenant vector", K(ret));
  } else if (OB_FAIL(append(tenant_group.tenants_, tenant_names))) {
    LOG_WARN("fail to append", K(ret));
  } else {
    tenant_group.row_ = 1;
    tenant_group.column_ = tenant_group.tenants_.count();
    if (OB_FAIL(tenant_groups.push_back(tenant_group))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObTenantGroupParser::parse_matrix_tenant_group(
    int64_t& pos, const int64_t end, const common::ObString& ttg_str, common::ObIArray<TenantNameGroup>& tenant_groups)
{
  int ret = OB_SUCCESS;
  TenantNameGroup tenant_group;
  common::ObArray<common::ObString> tenant_names;
  bool first = true;
  // parse the second layer of two layer bracket structure
  while (OB_SUCC(ret) && pos < end && ')' != ttg_str[pos]) {
    tenant_names.reset();
    jump_over_space(pos, end, ttg_str);
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('(' != ttg_str[pos]) {  // start with left brace
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else {
      ++pos;  // jump over left brace
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(parse_tenant_vector(pos, end, ttg_str, tenant_names))) {
        LOG_WARN("fail to parse tenant vector", K(ret));
      } else if (first) {
        tenant_group.column_ = tenant_names.count();
        tenant_group.row_ = 0;
        first = false;
      } else if (tenant_group.column_ != tenant_names.count()) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append(tenant_group.tenants_, tenant_names))) {
        LOG_WARN("fail to append", K(ret));
      } else {
        ++tenant_group.row_;
      }
    }
    if (OB_SUCC(ret)) {
      jump_over_space(pos, end, ttg_str);
      if (pos >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
      } else if (')' != ttg_str[pos]) {  // end up with right brace for a row tenant
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
      } else {
        ++pos;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(jump_to_next_tenant_vector(pos, end, ttg_str))) {
        LOG_WARN("fail to jump to next tenant vector", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (tenant_group.row_ <= 0 || tenant_group.column_ <= 0) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if (OB_FAIL(tenant_groups.push_back(tenant_group))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObTenantGroupParser::jump_to_next_tenant_vector(int64_t& pos, const int64_t end, const common::ObString& ttg_str)
{
  int ret = OB_SUCCESS;
  while (pos < end && isspace(ttg_str[pos])) {
    ++pos;
  }
  if (pos >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else if (')' == ttg_str[pos]) {
    // reach to the end of this tenant group
  } else if (',' == ttg_str[pos]) {
    ++pos;  // good, and jump over the ',' symbol
    while (pos < end && isspace(ttg_str[pos])) {
      ++pos;
    }
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('(' == ttg_str[pos]) {
      // the next tenant vector left brace
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    }
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  }
  return ret;
}

void ObTenantGroupParser::jump_over_space(int64_t& pos, const int64_t end, const common::ObString& ttg_str)
{
  while (pos < end && isspace(ttg_str[pos])) {
    ++pos;
  }
  return;
}

int ObTenantGroupParser::jump_to_next_ttg(int64_t& pos, const int64_t end, const common::ObString& ttg_str)
{
  int ret = OB_SUCCESS;
  while (pos < end && isspace(ttg_str[pos])) {
    ++pos;
  }
  if (pos >= end) {
    // good, reach end
  } else if (',' == ttg_str[pos]) {
    ++pos;  // good, and jump over the ',' symbol
    while (pos < end && isspace(ttg_str[pos])) {
      ++pos;
    }
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('(' == ttg_str[pos]) {
      // good, the next ttg left brace symbol
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    }
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  }
  return ret;
}

int ObLocalityTaskHelp::filter_logonly_task(const common::ObIArray<share::ObResourcePoolName>& pools,
    ObUnitManager& unit_mgr, ObIArray<share::ObZoneReplicaNumSet>& zone_locality)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> logonly_unit_infos;
  ObArray<ObUnitInfo> unit_infos;
  if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pools));
  } else if (OB_FAIL(unit_mgr.get_unit_infos(pools, unit_infos))) {
    LOG_WARN("fail to get unit infos", K(ret), K(pools));
  } else {
    for (int64_t i = 0; i < unit_infos.count() && OB_SUCC(ret); ++i) {
      if (REPLICA_TYPE_LOGONLY != unit_infos.at(i).unit_.replica_type_) {
        // only L unit is counted
      } else if (OB_FAIL(logonly_unit_infos.push_back(unit_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret), K(i), K(unit_infos));
      }
    }
    for (int64_t i = 0; i < zone_locality.count() && OB_SUCC(ret); ++i) {
      share::ObZoneReplicaAttrSet& zone_replica_attr_set = zone_locality.at(i);
      if (zone_replica_attr_set.get_logonly_replica_num() <= 0) {
        // no L replica : nothing todo
      } else if (zone_replica_attr_set.zone_set_.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone set unexpected", K(ret), K(zone_replica_attr_set));
      } else {
        for (int64_t j = 0; j < logonly_unit_infos.count(); j++) {
          const ObUnitInfo& unit_info = logonly_unit_infos.at(j);
          if (!has_exist_in_array(zone_replica_attr_set.zone_set_, unit_info.unit_.zone_)) {
            // bypass
          } else if (zone_replica_attr_set.get_logonly_replica_num() <= 0) {
            // bypass
          } else {
            ret = zone_replica_attr_set.sub_logonly_replica_num(ReplicaAttr(1, 100));
          }
        }
      }
    }
  }
  return ret;
}

int ObLocalityTaskHelp::get_logonly_task_with_logonly_unit(const uint64_t tenant_id, ObUnitManager& unit_mgr,
    share::schema::ObSchemaGetterGuard& schema_guard, ObIArray<share::ObZoneReplicaNumSet>& zone_locality)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> logonly_unit_infos;
  const ObTenantSchema* tenant_schema = NULL;
  zone_locality.reset();
  common::ObArray<share::ObZoneReplicaAttrSet> tenant_zone_locality;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("get invalid tenant schema", K(ret), K(tenant_schema));
  } else if (OB_FAIL(unit_mgr.get_logonly_unit_by_tenant(tenant_id, logonly_unit_infos))) {
    LOG_WARN("fail to get logonly unit infos", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_schema->get_zone_replica_attr_array(tenant_zone_locality))) {
    LOG_WARN("fail to get zone replica attr array", K(ret));
  } else {
    share::ObZoneReplicaNumSet logonly_set;
    for (int64_t i = 0; i < logonly_unit_infos.count() && OB_SUCC(ret); i++) {
      const ObUnitInfo& unit = logonly_unit_infos.at(i);
      for (int64_t j = 0; j < tenant_zone_locality.count(); j++) {
        logonly_set.reset();
        const ObZoneReplicaNumSet& zone_set = tenant_zone_locality.at(j);
        if (zone_set.zone_ == unit.unit_.zone_ && zone_set.get_logonly_replica_num() == 1) {
          logonly_set.zone_ = zone_set.zone_;
          if (OB_FAIL(logonly_set.replica_attr_set_.add_logonly_replica_num(ReplicaAttr(1, 100)))) {
            LOG_WARN("fail to add logonly replica num", K(ret));
          } else if (OB_FAIL(zone_locality.push_back(logonly_set))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLocalityTaskHelp::filter_logonly_task(const uint64_t tenant_id, ObUnitManager& unit_mgr,
    share::schema::ObSchemaGetterGuard& schema_guard, ObIArray<share::ObZoneReplicaAttrSet>& zone_locality)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> logonly_unit_infos;
  if (OB_FAIL(unit_mgr.get_logonly_unit_by_tenant(schema_guard, tenant_id, logonly_unit_infos))) {
    LOG_WARN("fail to get loggonly unit by tenant", K(ret), K(tenant_id));
  } else {
    LOG_DEBUG("get all logonly unit", K(tenant_id), K(logonly_unit_infos), K(zone_locality));
    for (int64_t i = 0; i < zone_locality.count() && OB_SUCC(ret); ++i) {
      share::ObZoneReplicaAttrSet& zone_replica_attr_set = zone_locality.at(i);
      if (zone_replica_attr_set.get_logonly_replica_num() <= 0) {
        // no L replica : nothing todo
      } else if (zone_replica_attr_set.zone_set_.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone set unexpected", K(ret), K(zone_replica_attr_set));
      } else {
        for (int64_t j = 0; j < logonly_unit_infos.count(); j++) {
          const ObUnitInfo& unit_info = logonly_unit_infos.at(j);
          if (!has_exist_in_array(zone_replica_attr_set.zone_set_, unit_info.unit_.zone_)) {
            // bypass
          } else if (zone_replica_attr_set.get_logonly_replica_num() <= 0) {
            // bypass
          } else {
            ret = zone_replica_attr_set.sub_logonly_replica_num(ReplicaAttr(1, 100));
          }
        }
      }
    }
  }
  return ret;
}

int ObLocalityTaskHelp::alloc_logonly_replica(ObUnitManager& unit_mgr, const ObIArray<share::ObResourcePoolName>& pools,
    const common::ObIArray<ObZoneReplicaNumSet>& zone_locality, ObPartitionAddr& partition_addr)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitInfo> logonly_units;
  ObArray<ObUnitInfo> unit_infos;
  if (OB_FAIL(unit_mgr.get_unit_infos(pools, unit_infos))) {
    LOG_WARN("fail to get unit infos", K(ret), K(pools));
  } else {
    for (int64_t i = 0; i < unit_infos.count() && OB_SUCC(ret); i++) {
      if (REPLICA_TYPE_LOGONLY != unit_infos.at(i).unit_.replica_type_) {
        // nothing todo
      } else if (OB_FAIL(logonly_units.push_back(unit_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret), K(i), K(unit_infos));
      }
    }
  }
  ObReplicaAddr raddr;
  for (int64_t i = 0; i < logonly_units.count() && OB_SUCC(ret); i++) {
    for (int64_t j = 0; j < zone_locality.count() && OB_SUCC(ret); j++) {
      if (zone_locality.at(j).zone_ == logonly_units.at(i).unit_.zone_ &&
          zone_locality.at(j).get_logonly_replica_num() == 1) {
        raddr.reset();
        raddr.unit_id_ = logonly_units.at(i).unit_.unit_id_;
        raddr.addr_ = logonly_units.at(i).unit_.server_;
        raddr.zone_ = logonly_units.at(i).unit_.zone_;
        raddr.replica_type_ = REPLICA_TYPE_LOGONLY;
        if (OB_FAIL(partition_addr.push_back(raddr))) {
          LOG_WARN("fail to push back", K(ret), K(raddr));
        } else {
          LOG_INFO("alloc partition for logonly replica", K(raddr));
        }
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::calc_paxos_replica_num(
    const common::ObIArray<share::ObZoneReplicaNumSet>& zone_locality, int64_t& paxos_num)
{
  int ret = OB_SUCCESS;
  paxos_num = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); i++) {
    if (0 > zone_locality.at(i).get_paxos_replica_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid paxos replica num", K(ret), "zone_locality_set", zone_locality.at(i));
    } else {
      paxos_num += zone_locality.at(i).get_paxos_replica_num();
    }
  }
  return ret;
}

/*
 * the laws of alter locality:
 * 1. each locality altering only allows to execute one operation: add paoxs, reduce paxos and paxos type
 * transform(paxos->paxos, F->L). paxos->non_paxos is reduce paxos, non_paxos->paxos is add paxos.
 * 2. in a locality altering:
 *    2.1 if add paxos, need the orig_locality's paxos num >= majority(new_locality's paxos num);
 *    2.2 if reduce paxos, need new_locality's paxos num >= majority(orig_locality's paxos num);
 *    2.3 if paxos type transform, only one type transform is allowed for a locality altering.
 * 3. the laws of type transform:
 *   3.1 for L-replica, it is not allowed to transform replica_type other than F->L,
 *       and it is not allowed to transform L to other type.
 * 4. enable paxos num 1->2. must not paxos num 2->1
 * 5. there is no restriction on non-paoxs type transform
 */
int ObLocalityCheckHelp::check_alter_locality(const ObIArray<share::ObZoneReplicaNumSet>& pre_zone_locality,
    const ObIArray<share::ObZoneReplicaNumSet>& cur_zone_locality, ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks,
    bool& non_paxos_locality_modified, int64_t& pre_paxos_num, int64_t& cur_paxos_num)
{
  int ret = OB_SUCCESS;
  pre_paxos_num = 0;
  cur_paxos_num = 0;
  alter_paxos_tasks.reset();
  if (OB_UNLIKELY(pre_zone_locality.count() <= 0 || cur_zone_locality.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "pre_cnt", pre_zone_locality.count(), "cur_cnt", cur_zone_locality.count());
  } else if (OB_FAIL(get_alter_quorum_replica_task(
                 pre_zone_locality, cur_zone_locality, alter_paxos_tasks, non_paxos_locality_modified))) {
    LOG_WARN("fail to get alter paxos replica task", K(ret));
  } else if (OB_FAIL(calc_paxos_replica_num(pre_zone_locality, pre_paxos_num))) {
    LOG_WARN("fail to calc paxos replica num", K(ret));
  } else if (OB_FAIL(calc_paxos_replica_num(cur_zone_locality, cur_paxos_num))) {
    LOG_WARN("fail to calc paxos replica num", K(ret));
  } else if (OB_FAIL(check_alter_locality_valid(alter_paxos_tasks, pre_paxos_num, cur_paxos_num))) {
    LOG_WARN("check alter locality valid failed",
        K(ret),
        K(alter_paxos_tasks),
        K(pre_paxos_num),
        K(cur_paxos_num),
        K(non_paxos_locality_modified));
  }
  return ret;
}

/*
 * Two zone set either are perfect matched or have intersection
 */
int ObLocalityCheckHelp::check_multi_zone_locality_intersect(
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_cur_zone_locality, bool& intersect)
{
  int ret = OB_SUCCESS;
  intersect = false;
  for (int64_t i = 0; !intersect && OB_SUCC(ret) && i < multi_pre_zone_locality.count(); ++i) {
    for (int64_t j = 0; !intersect && OB_SUCC(ret) && j < multi_cur_zone_locality.count(); ++j) {
      const share::ObZoneReplicaAttrSet& pre_locality = multi_pre_zone_locality.at(i);
      const share::ObZoneReplicaAttrSet& cur_locality = multi_cur_zone_locality.at(j);
      const common::ObIArray<common::ObZone>& pre_zone_set = pre_locality.zone_set_;
      const common::ObIArray<common::ObZone>& cur_zone_set = cur_locality.zone_set_;
      if (pre_zone_set.count() <= 1 || cur_zone_set.count() <= 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone set count unexpected", K(ret));
      } else {
        intersect = check_zone_set_intersect(pre_zone_set, cur_zone_set);
      }
    }
  }
  return ret;
}

bool ObLocalityCheckHelp::check_zone_set_intersect(
    const common::ObIArray<common::ObZone>& pre_zone_set, const common::ObIArray<common::ObZone>& cur_zone_set)
{
  bool intersect = false;
  if (pre_zone_set.count() == cur_zone_set.count()) {
    // either there is no crossover or there is a perfect match
    int64_t counter = 0;
    for (int64_t i = 0; i < pre_zone_set.count(); ++i) {
      const common::ObZone& this_pre_zone = pre_zone_set.at(i);
      if (has_exist_in_array(cur_zone_set, this_pre_zone)) {
        counter++;
      }
    }
    if (0 == counter || pre_zone_set.count() == counter) {
      intersect = false;
    } else {
      intersect = true;
    }
  } else {
    // must no cross
    intersect = false;
    for (int64_t i = 0; !intersect && i < pre_zone_set.count(); ++i) {
      const common::ObZone& this_pre_zone = pre_zone_set.at(i);
      if (has_exist_in_array(cur_zone_set, this_pre_zone)) {
        intersect = true;
      }
    }
  }
  return intersect;
}

int ObLocalityCheckHelp::split_single_and_multi_zone_locality(
    const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
    common::ObIArray<share::ObZoneReplicaAttrSet>& single_zone_locality,
    common::ObIArray<share::ObZoneReplicaAttrSet>& multi_zone_locality)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone_locality.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "zone_locality_cnt", zone_locality.count());
  } else {
    single_zone_locality.reset();
    multi_zone_locality.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet& this_set = zone_locality.at(i);
      if (this_set.zone_set_.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone set count unexpected", K(ret), "zone_set_cnt", this_set.zone_set_.count());
      } else if (this_set.zone_set_.count() == 1) {
        if (OB_FAIL(single_zone_locality.push_back(this_set))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        if (OB_FAIL(multi_zone_locality.push_back(this_set))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::single_zone_locality_search(const share::ObZoneReplicaAttrSet& this_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& single_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_zone_locality, SingleZoneLocalitySearch& search_flag,
    int64_t& search_index, const share::ObZoneReplicaAttrSet*& search_zone_locality)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(this_locality.zone_set_.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    bool found = false;
    const common::ObZone& compare_zone = this_locality.zone_set_.at(0);
    for (int64_t i = 0; !found && OB_SUCC(ret) && i < single_zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet& this_zone_locality = single_zone_locality.at(i);
      if (this_zone_locality.zone_set_.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this zone set unexpected", K(ret), "zone_set_count", this_zone_locality.zone_set_.count());
      } else if (compare_zone != this_zone_locality.zone_set_.at(0)) {
        // go on check next
      } else {
        found = true;
        search_flag = SingleZoneLocalitySearch::SZLS_IN_ASSOC_SINGLE;
        search_index = i;
        search_zone_locality = &this_zone_locality;
      }
    }
    for (int64_t i = 0; !found && OB_SUCC(ret) && i < multi_zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet& this_zone_locality = multi_zone_locality.at(i);
      if (this_zone_locality.zone_set_.count() <= 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this zone set unexpected", K(ret), "zone_set_count", this_zone_locality.zone_set_.count());
      } else if (!has_exist_in_array(this_zone_locality.zone_set_, compare_zone)) {
        // go on check next
      } else {
        found = true;
        search_flag = SingleZoneLocalitySearch::SZLS_IN_ASSOC_MULTI;
        search_index = i;
        search_zone_locality = &this_zone_locality;
      }
    }
    if (OB_SUCC(ret)) {
      if (!found) {
        search_flag = SingleZoneLocalitySearch::SZLS_NOT_FOUND;
        search_index = -1;
        search_zone_locality = nullptr;
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::try_add_single_zone_alter_paxos_task(const share::ObZoneReplicaAttrSet& pre_locality,
    const share::ObZoneReplicaAttrSet& cur_locality, common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks,
    bool& non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pre_locality.zone_set_.count() != 1) || OB_UNLIKELY(cur_locality.zone_set_.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pre_locality), K(cur_locality));
  } else if (OB_FAIL(check_alter_single_zone_locality_valid(cur_locality, pre_locality, non_paxos_locality_modified))) {
    LOG_WARN("alter single zone locality is invalid",
        K(ret),
        K(cur_locality),
        K(pre_locality),
        K(non_paxos_locality_modified));
  } else {
    AlterPaxosLocalityTask alter_paxos_task;
    if (pre_locality.get_paxos_replica_num() == cur_locality.get_paxos_replica_num()) {
      if (!pre_locality.is_paxos_locality_match(cur_locality)) {
        alter_paxos_task.reset();
        alter_paxos_task.task_type_ = NOP_QUORUM;
        // onlu four situations:"F->L", "L->F", "F{m1}->F{m2}", "F{m1},L->F{m2},L"
        if (cur_locality.get_full_replica_num() > pre_locality.get_full_replica_num()) {
          if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_FULL))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_SUCC(ret) && cur_locality.get_logonly_replica_num() > pre_locality.get_logonly_replica_num()) {
          if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_LOGONLY))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_SUCC(ret) && cur_locality.get_logonly_replica_num() == pre_locality.get_logonly_replica_num() &&
            cur_locality.get_full_replica_num() == pre_locality.get_full_replica_num()) {
          // memstore_percent changed
          if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_FULL))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(alter_paxos_task.zone_set_.assign(pre_locality.zone_set_))) {
          LOG_WARN("fail to assign array", K(ret));
        } else if (OB_FAIL(alter_paxos_tasks.push_back(alter_paxos_task))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
      }  // paxos locality match
    } else {
      alter_paxos_task.reset();
      const int64_t delta = cur_locality.get_paxos_replica_num() - pre_locality.get_paxos_replica_num();
      alter_paxos_task.task_type_ = delta > 0 ? ADD_QUORUM : SUB_QUORUM;
      if (pre_locality.get_full_replica_num() != cur_locality.get_full_replica_num()) {
        if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_FULL))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
      if (OB_SUCC(ret) && pre_locality.get_logonly_replica_num() != cur_locality.get_logonly_replica_num()) {
        if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_LOGONLY))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(alter_paxos_task.zone_set_.assign(pre_locality.zone_set_))) {
        LOG_WARN("fail to assign array", K(ret));
      } else if (OB_FAIL(alter_paxos_tasks.push_back(alter_paxos_task))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::process_pre_single_zone_locality(
    const common::ObIArray<share::ObZoneReplicaAttrSet>& single_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& single_cur_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_cur_zone_locality,
    common::ObArray<XyIndex>& pre_in_cur_multi_indexes, common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks,
    bool& non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  pre_in_cur_multi_indexes.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < single_pre_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet& this_locality = single_pre_zone_locality.at(i);
    SingleZoneLocalitySearch search_flag = SingleZoneLocalitySearch::SZLS_INVALID;
    int64_t search_index = -1;
    const share::ObZoneReplicaAttrSet* search_zone_locality = nullptr;
    if (OB_FAIL(single_zone_locality_search(this_locality,
            single_cur_zone_locality,
            multi_cur_zone_locality,
            search_flag,
            search_index,
            search_zone_locality))) {
      LOG_WARN("fail to search single zone locality", K(ret));
    } else if (SingleZoneLocalitySearch::SZLS_IN_ASSOC_SINGLE == search_flag) {
      if (nullptr == search_zone_locality) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("search zone locality ptr is null", K(ret));
      } else if (OB_FAIL(try_add_single_zone_alter_paxos_task(
                     this_locality, *search_zone_locality, alter_paxos_tasks, non_paxos_locality_modified))) {
        LOG_WARN("fail to try add single zone alter paxos task", K(ret));
      }
    } else if (SingleZoneLocalitySearch::SZLS_IN_ASSOC_MULTI == search_flag) {
      if (search_index >= multi_cur_zone_locality.count() || search_index < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("search index unexpected",
            K(ret),
            K(search_index),
            "multi_cur_zone_locality_cnt",
            multi_cur_zone_locality.count());
      } else if (OB_FAIL(pre_in_cur_multi_indexes.push_back(XyIndex(i, search_index)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else if (SingleZoneLocalitySearch::SZLS_NOT_FOUND == search_flag) {
      // dealing with locality remove zone.
      ObZoneReplicaAttrSet cur_set;
      if (OB_FAIL(cur_set.zone_set_.assign(this_locality.zone_set_))) {
        LOG_WARN("fail to assign zone set", K(ret));
      } else if (OB_FAIL(try_add_single_zone_alter_paxos_task(
                     this_locality, cur_set, alter_paxos_tasks, non_paxos_locality_modified))) {
        LOG_WARN("fail to try add single zone alter paxos task", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search flag unexpected", K(ret), K(search_flag));
    }
  }
  if (OB_SUCC(ret)) {
    YIndexCmp cmp_operator;
    std::sort(pre_in_cur_multi_indexes.begin(), pre_in_cur_multi_indexes.end(), cmp_operator);
  }
  return ret;
}

int ObLocalityCheckHelp::process_cur_single_zone_locality(
    const common::ObIArray<share::ObZoneReplicaAttrSet>& single_cur_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& single_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_pre_zone_locality,
    common::ObArray<XyIndex>& cur_in_pre_multi_indexes, common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks,
    bool& non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  cur_in_pre_multi_indexes.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < single_cur_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet& this_locality = single_cur_zone_locality.at(i);
    SingleZoneLocalitySearch search_flag = SingleZoneLocalitySearch::SZLS_INVALID;
    int64_t search_index = -1;
    const share::ObZoneReplicaAttrSet* search_zone_locality = nullptr;
    if (OB_FAIL(single_zone_locality_search(this_locality,
            single_pre_zone_locality,
            multi_pre_zone_locality,
            search_flag,
            search_index,
            search_zone_locality))) {
      LOG_WARN("fail to search single zone locality", K(ret));
    } else if (SingleZoneLocalitySearch::SZLS_IN_ASSOC_SINGLE == search_flag) {
      // already process in process_pre_single_zone_locality
    } else if (SingleZoneLocalitySearch::SZLS_IN_ASSOC_MULTI == search_flag) {
      if (search_index >= multi_pre_zone_locality.count() || search_index < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("search index unexpected",
            K(ret),
            K(search_index),
            "multi_pre_zone_locality_cnt",
            multi_pre_zone_locality.count());
      } else if (OB_FAIL(cur_in_pre_multi_indexes.push_back(XyIndex(i, search_index)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else if (SingleZoneLocalitySearch::SZLS_NOT_FOUND == search_flag) {
      // dealing with locality add zone
      ObZoneReplicaAttrSet pre_set;
      if (OB_FAIL(pre_set.zone_set_.assign(this_locality.zone_set_))) {
        LOG_WARN("fail to assign zone set", K(ret));
      } else if (OB_FAIL(try_add_single_zone_alter_paxos_task(
                     pre_set, this_locality, alter_paxos_tasks, non_paxos_locality_modified))) {
        LOG_WARN("fail to try add single zone alter paxos task", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search flag unexpected", K(ret), K(search_flag));
    }
  }
  if (OB_SUCC(ret)) {
    YIndexCmp cmp_operator;
    std::sort(cur_in_pre_multi_indexes.begin(), cur_in_pre_multi_indexes.end(), cmp_operator);
  }
  return ret;
}

int ObLocalityCheckHelp::add_multi_zone_locality_task(common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks,
    const share::ObZoneReplicaAttrSet& multi_zone_locality, const QuorumTaskType quorum_task_type)
{
  int ret = OB_SUCCESS;
  AlterPaxosLocalityTask alter_paxos_task;
  alter_paxos_task.reset();
  alter_paxos_task.task_type_ = quorum_task_type;
  if (OB_FAIL(alter_paxos_task.zone_set_.assign(multi_zone_locality.zone_set_))) {
    LOG_WARN("fail to assign zone set", K(ret));
  } else {
    if (multi_zone_locality.get_full_replica_num() > 0) {
      if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_FULL))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret) && multi_zone_locality.get_logonly_replica_num() > 0) {
      if (OB_FAIL(alter_paxos_task.associated_replica_type_set_.push_back(REPLICA_TYPE_LOGONLY))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(alter_paxos_tasks.push_back(alter_paxos_task))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}
int ObLocalityCheckHelp::check_alter_locality_match(
    const share::ObZoneReplicaAttrSet& in_locality, const share::ObZoneReplicaAttrSet& out_locality)
{
  int ret = OB_SUCCESS;
  bool match = false;
  match = in_locality.is_alter_locality_match(out_locality);
  if (!match) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("too many locality changes are illegal", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "too many locality changes");
  }
  return ret;
}

int ObLocalityCheckHelp::check_single_and_multi_zone_locality_match(const int64_t start, const int64_t end,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& single_zone_locality,
    const common::ObIArray<XyIndex>& in_multi_indexes, const share::ObZoneReplicaAttrSet& multi_zone_locality)
{
  int ret = OB_SUCCESS;
  if (start >= end || start >= in_multi_indexes.count() || end > in_multi_indexes.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start), K(end), "in_multi_indexes_cnt", in_multi_indexes.count());
  } else {
    share::ObZoneReplicaAttrSet new_multi_zone_locality;
    for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
      const int64_t x = in_multi_indexes.at(i).x_;
      if (x < 0 || x >= single_zone_locality.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("x unexpected", K(ret), K(x), "single_zone_locality_cnt", single_zone_locality.count());
      } else if (OB_FAIL(new_multi_zone_locality.append(single_zone_locality.at(x)))) {
        LOG_WARN("fail to append", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_alter_locality_match(new_multi_zone_locality, multi_zone_locality))) {
        LOG_WARN("fail to check alter locality match", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::process_single_in_multi(
    const common::ObIArray<share::ObZoneReplicaAttrSet>& single_left_zone_locality,
    const common::ObIArray<XyIndex>& left_in_multi_indexes,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_right_zone_locality,
    common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks)
{
  int ret = OB_SUCCESS;
  int64_t start = 0;
  int64_t end = left_in_multi_indexes.count();
  while (OB_SUCC(ret) && start < end) {
    int64_t cursor = start;
    int64_t y_index = left_in_multi_indexes.at(start).y_;
    for (/*nop*/; cursor < end; ++cursor) {
      if (left_in_multi_indexes.at(start).y_ == left_in_multi_indexes.at(cursor).y_) {
        // go on
      } else {
        break;
      }
    }
    if (y_index < 0 || y_index >= multi_right_zone_locality.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("y_index unexpected", K(ret), K(y_index), "multi_zone_locality_cnt", multi_right_zone_locality.count());
    } else if (OB_FAIL(check_single_and_multi_zone_locality_match(start,
                   cursor,
                   single_left_zone_locality,
                   left_in_multi_indexes,
                   multi_right_zone_locality.at(y_index)))) {
    } else if (OB_FAIL(add_multi_zone_locality_task(
                   alter_paxos_tasks, multi_right_zone_locality.at(y_index), QuorumTaskType::NOP_QUORUM))) {
      LOG_WARN("fail to add multi zone locality task", K(ret));
    } else {
      start = cursor;
    }
  }
  return ret;
}

bool ObLocalityCheckHelp::has_exist_in_yindex_array(const common::ObIArray<XyIndex>& index_array, const int64_t y)
{
  bool found = false;
  for (int64_t i = 0; !found && i < index_array.count(); ++i) {
    const XyIndex& this_index = index_array.at(i);
    if (y == this_index.y_) {
      found = true;
    }
  }
  return found;
}

int ObLocalityCheckHelp::process_pre_multi_locality(const common::ObIArray<XyIndex>& pre_in_cur_multi_indexes,
    const common::ObIArray<XyIndex>& cur_in_pre_multi_indexes,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_cur_zone_locality,
    common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < multi_pre_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet& this_pre_locality = multi_pre_zone_locality.at(i);
    if (has_exist_in_yindex_array(cur_in_pre_multi_indexes, i)) {
      // bypass, already processed
    } else {
      bool found = false;
      for (int64_t j = 0; !found && j < multi_cur_zone_locality.count(); ++j) {
        const share::ObZoneReplicaAttrSet& this_cur_locality = multi_cur_zone_locality.at(j);
        if (has_exist_in_yindex_array(pre_in_cur_multi_indexes, j)) {
          found = true;
          // bypass, already processed
        } else if (this_pre_locality == this_cur_locality) {
          found = true;
          // locality not modified
        } else if (this_cur_locality.is_zone_set_match(this_pre_locality)) {
          // is processing in process_cur_multi_locality
          found = true;
        } else { /* go on check next */
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_alter_locality_match(this_pre_locality, this_cur_locality))) {
            LOG_WARN("fail to check locality match", K(ret));
          }
        }
      }
      if (!found && OB_SUCC(ret)) {
        if (OB_FAIL(add_multi_zone_locality_task(alter_paxos_tasks, this_pre_locality, QuorumTaskType::SUB_QUORUM))) {
          LOG_WARN("fail to add multi zone locality task", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::process_cur_multi_locality(const common::ObIArray<XyIndex>& pre_in_cur_multi_indexes,
    const common::ObIArray<XyIndex>& cur_in_pre_multi_indexes,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_cur_zone_locality,
    common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < multi_cur_zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet& this_cur_locality = multi_cur_zone_locality.at(i);
    if (has_exist_in_yindex_array(pre_in_cur_multi_indexes, i)) {
      // bypass, already process
    } else {
      bool found = false;
      for (int64_t j = 0; !found && j < multi_pre_zone_locality.count(); ++j) {
        const share::ObZoneReplicaAttrSet& this_pre_locality = multi_pre_zone_locality.at(j);
        if (has_exist_in_yindex_array(cur_in_pre_multi_indexes, j)) {
          found = true;
          // bypass, already processed
        } else if (this_cur_locality == this_pre_locality) {
          found = true;
          // locality not modified
        } else if (this_cur_locality.is_zone_set_match(this_pre_locality)) {
          found = true;
          if (OB_FAIL(add_multi_zone_locality_task(alter_paxos_tasks, this_cur_locality, QuorumTaskType::NOP_QUORUM))) {
            LOG_WARN("fail to add multi zone locality task", K(ret));
          }
        } else { /* go on check next */
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_alter_locality_match(this_cur_locality, this_pre_locality))) {
            LOG_WARN("fail to check alter locality match", K(ret));
          }
        }
      }
      if (!found && OB_SUCC(ret)) {
        if (OB_FAIL(add_multi_zone_locality_task(alter_paxos_tasks, this_cur_locality, QuorumTaskType::ADD_QUORUM))) {
          LOG_WARN("fail to add multi zone locality task", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::get_alter_quorum_replica_task(
    const common::ObIArray<share::ObZoneReplicaAttrSet>& pre_zone_locality,
    const common::ObIArray<share::ObZoneReplicaAttrSet>& cur_zone_locality,
    ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, bool& non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneReplicaAttrSet> single_pre_zone_locality;
  common::ObArray<share::ObZoneReplicaAttrSet> multi_pre_zone_locality;
  common::ObArray<share::ObZoneReplicaAttrSet> single_cur_zone_locality;
  common::ObArray<share::ObZoneReplicaAttrSet> multi_cur_zone_locality;
  common::ObArray<XyIndex> pre_in_cur_multi_indexes;
  common::ObArray<XyIndex> cur_in_pre_multi_indexes;
  bool intersect = false;
  if (OB_UNLIKELY(pre_zone_locality.count() <= 0) || OB_UNLIKELY(cur_zone_locality.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "pre_cnt", pre_zone_locality.count(), "cur_cnt", cur_zone_locality.count());
  } else if (OB_FAIL(split_single_and_multi_zone_locality(
                 pre_zone_locality, single_pre_zone_locality, multi_pre_zone_locality))) {
    LOG_WARN("fail to split single and multi zone locality", K(ret));
  } else if (OB_FAIL(split_single_and_multi_zone_locality(
                 cur_zone_locality, single_cur_zone_locality, multi_cur_zone_locality))) {
    LOG_WARN("fail to split single and multi zone locality", K(ret));
  } else if (OB_FAIL(
                 check_multi_zone_locality_intersect(multi_pre_zone_locality, multi_cur_zone_locality, intersect))) {
    LOG_WARN("fail to check multi zone locality intersect", K(ret));
  } else if (intersect) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("multi zone locality intersect before and after is not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "multi zone locality intersect before and after");
  } else if (OB_FAIL(process_pre_single_zone_locality(single_pre_zone_locality,
                 single_cur_zone_locality,
                 multi_cur_zone_locality,
                 pre_in_cur_multi_indexes,
                 alter_paxos_tasks,
                 non_paxos_locality_modified))) {
    LOG_WARN("fail to process pre single zone locality", K(ret));
  } else if (OB_FAIL(process_cur_single_zone_locality(single_cur_zone_locality,
                 single_pre_zone_locality,
                 multi_pre_zone_locality,
                 cur_in_pre_multi_indexes,
                 alter_paxos_tasks,
                 non_paxos_locality_modified))) {
    LOG_WARN("fail to process cur single zone locality", K(ret));
  } else if (OB_FAIL(process_single_in_multi(
                 single_pre_zone_locality, pre_in_cur_multi_indexes, multi_cur_zone_locality, alter_paxos_tasks))) {
    LOG_WARN("fail to process pre single in cur multi", K(ret));
  } else if (OB_FAIL(process_single_in_multi(
                 single_cur_zone_locality, cur_in_pre_multi_indexes, multi_pre_zone_locality, alter_paxos_tasks))) {
    LOG_WARN("fail to process cur single in pre multi", K(ret));
  } else if (OB_FAIL(process_pre_multi_locality(pre_in_cur_multi_indexes,
                 cur_in_pre_multi_indexes,
                 multi_pre_zone_locality,
                 multi_cur_zone_locality,
                 alter_paxos_tasks))) {
    LOG_WARN("fail to process pre multi locality", K(ret));
  } else if (OB_FAIL(process_cur_multi_locality(pre_in_cur_multi_indexes,
                 cur_in_pre_multi_indexes,
                 multi_pre_zone_locality,
                 multi_cur_zone_locality,
                 alter_paxos_tasks))) {
    LOG_WARN("fail to process cur multi locality", K(ret));
  }
  return ret;
}

int ObLocalityCheckHelp::check_alter_locality_valid(
    ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, int64_t pre_paxos_num, int64_t cur_paxos_num)
{
  int ret = OB_SUCCESS;
  if (pre_paxos_num <= 0 || cur_paxos_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pre_paxos_num or cur_paxos_num is invalid", K(ret), K(pre_paxos_num), K(cur_paxos_num));
  } else {
    // ensure only has ADD_QUORUM task or only SUB_QUORUM task or only one NOP_QUORUM task.
    int64_t add_task_num = 0;
    int64_t remove_task_num = 0;
    int64_t nop_task_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < alter_paxos_tasks.count(); i++) {
      const AlterPaxosLocalityTask& this_alter_paxos_task = alter_paxos_tasks.at(i);
      if (OB_FAIL(ret)) {
      } else if (SUB_QUORUM == this_alter_paxos_task.task_type_) {
        if (nop_task_num != 0 || add_task_num != 0) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("should only has one nop_task", K(ret), K(alter_paxos_tasks));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter locality with multi task");
        } else {
          remove_task_num++;
        }
      } else if (ADD_QUORUM == alter_paxos_tasks.at(i).task_type_) {
        if (nop_task_num != 0 || remove_task_num != 0) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("should only has add_tasks", K(ret), K(alter_paxos_tasks));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter locality with multi task");
        } else {
          add_task_num++;
        }
      } else if (NOP_QUORUM == alter_paxos_tasks.at(i).task_type_) {
        if (nop_task_num != 0 || add_task_num != 0 || remove_task_num != 0) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("should only has one nop_task", K(ret), K(alter_paxos_tasks));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter locality with multi task");
        } else {
          nop_task_num++;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid task_type", K(ret), "alter_paxos_task", alter_paxos_tasks.at(i));
      }
    }
    // 2. Locality's paxos member change need to meet the following principles:
    //    1) only add paxos member and pre_paxos_num >= majority(cur_paxos_num)
    //    2) only remove paxos member and cur_paxos_num >= majority(pre_paxos_num)
    //    3) only has a paxos -> paxos's type transform task.
    //       non_paxos -> paxos's type transform is add paxos member.
    //       paxos -> non_paxos's type transform is remove paxos member
    if (OB_SUCC(ret)) {
      bool passed = true;
      if (0 != nop_task_num) {
        // only has paxos->paxos's type transform task, no quorum value change.
      } else if (0 != add_task_num) {
        if (1 == pre_paxos_num && 1 == add_task_num) {
          // special process: enable locality's paxos member 1 -> 2
        } else if (pre_paxos_num >= majority(cur_paxos_num)) {
          // passed
        } else {
          passed = false;
        }
      } else if (0 != remove_task_num) {
        if (cur_paxos_num >= majority(pre_paxos_num)) {
          // passed
        } else {
          passed = false;
        }
      }
      if (!passed) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("violate locality principal",
            K(ret),
            K(pre_paxos_num),
            K(cur_paxos_num),
            K(add_task_num),
            K(remove_task_num),
            K(nop_task_num));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "violate locality principal");
      }
    }
  }
  return ret;
}

int ObLocalityCheckHelp::check_alter_single_zone_locality_valid(const ObZoneReplicaAttrSet& new_locality,
    const ObZoneReplicaAttrSet& orig_locality, bool& non_paxos_locality_modified)
{
  int ret = OB_SUCCESS;
  bool is_legal = true;
  // 1. check whether non_paxos member change
  if (!non_paxos_locality_modified) {
    const ObIArray<ReplicaAttr>& pre_readonly_replica =
        orig_locality.replica_attr_set_.get_readonly_replica_attr_array();
    const ObIArray<ReplicaAttr>& cur_readonly_replica =
        new_locality.replica_attr_set_.get_readonly_replica_attr_array();
    if (pre_readonly_replica.count() != cur_readonly_replica.count()) {
      non_paxos_locality_modified = true;
    } else {
      for (int64_t i = 0; !non_paxos_locality_modified && i < pre_readonly_replica.count(); ++i) {
        const ReplicaAttr& pre_replica_attr = pre_readonly_replica.at(i);
        const ReplicaAttr& cur_replica_attr = cur_readonly_replica.at(i);
        if (pre_replica_attr != cur_replica_attr) {
          non_paxos_locality_modified = true;
        }
      }
    }
  }
  // 2. check whether alter locality is legal.
  if (new_locality.get_logonly_replica_num() < orig_locality.get_logonly_replica_num()) {
    // L-replica must not transfrom to other replica type.
    if (new_locality.get_full_replica_num() > orig_locality.get_full_replica_num()) {
      is_legal = false;  // maybe L->F
    } else if (ObLocalityDistribution::ALL_SERVER_CNT == new_locality.get_readonly_replica_num() ||
               ObLocalityDistribution::ALL_SERVER_CNT == orig_locality.get_readonly_replica_num() ||
               new_locality.get_readonly_replica_num() > orig_locality.get_readonly_replica_num()) {
      if (0 != new_locality.get_readonly_replica_num()) {
        is_legal = false;  // maybe L->R
      }
    } else {
    }  // good
  } else if (new_locality.get_logonly_replica_num() > orig_locality.get_logonly_replica_num()) {
    // only enable F transform to L
    if (new_locality.get_full_replica_num() < orig_locality.get_full_replica_num()) {
      if (ObLocalityDistribution::ALL_SERVER_CNT == new_locality.get_readonly_replica_num() &&
          ObLocalityDistribution::ALL_SERVER_CNT != orig_locality.get_readonly_replica_num()) {
        if (0 != orig_locality.get_readonly_replica_num()) {
          is_legal = false;  // maybe R->L
        }
      } else if (ObLocalityDistribution::ALL_SERVER_CNT != new_locality.get_readonly_replica_num() &&
                 ObLocalityDistribution::ALL_SERVER_CNT == orig_locality.get_readonly_replica_num()) {
        is_legal = false;  // maybe R->L
      } else if (new_locality.get_readonly_replica_num() < orig_locality.get_readonly_replica_num()) {
        is_legal = false;  // maybe R->L
      }
    } else {
      if (ObLocalityDistribution::ALL_SERVER_CNT == new_locality.get_readonly_replica_num() ||
          ObLocalityDistribution::ALL_SERVER_CNT == orig_locality.get_readonly_replica_num() ||
          new_locality.get_readonly_replica_num() < orig_locality.get_readonly_replica_num()) {
        if (0 != orig_locality.get_readonly_replica_num()) {
          is_legal = false;  // maybe R->L
        }
      } else {
      }  // good
    }
  }
  if (!is_legal) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("invalid replica_type transformation", K(ret), K(new_locality), K(orig_locality));
  }

  return ret;
}

int ObQuorumGetter::get_schema_paxos_replica_num(const uint64_t schema_id, int64_t& cur_paxos_count) const
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == schema_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(schema_id));
  } else {
    if (!is_tablegroup_id(schema_id)) {
      const share::schema::ObTableSchema* table_schema = nullptr;
      if (OB_FAIL(schema_guard_->get_table_schema(schema_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(schema_id));
      } else if (OB_UNLIKELY(NULL == table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), "table_id", schema_id);
      } else if (OB_FAIL(table_schema->get_paxos_replica_num(*schema_guard_, cur_paxos_count))) {
        LOG_WARN("fail to get paxos replica num", K(ret), K(schema_id));
      }
    } else {
      const share::schema::ObTablegroupSchema* tg_schema = nullptr;
      if (OB_FAIL(schema_guard_->get_tablegroup_schema(schema_id, tg_schema))) {
        LOG_WARN("fail to get tablegroup schema", K(schema_id));
      } else if (OB_UNLIKELY(nullptr == tg_schema)) {
        ret = OB_TABLEGROUP_NOT_EXIST;
        LOG_WARN("tablegroup not exist", K(ret), "tg_id", schema_id);
      } else if (OB_FAIL(tg_schema->get_paxos_replica_num(*schema_guard_, cur_paxos_count))) {
        LOG_WARN("fail to get paxos replica num", K(ret), K(schema_id));
      }
    }
  }
  return ret;
}

int ObQuorumGetter::get_migrate_replica_quorum_size(
    const uint64_t table_id, const int64_t orig_quorum, int64_t& quorum) const
{
  // migrate_replica will not change quorum value, get quorum value reported by observer directly.
  int ret = OB_SUCCESS;
  quorum = 0;
  UNUSED(table_id);
  if (orig_quorum <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid quorum", K(ret), K(table_id), K(orig_quorum));
  } else {
    quorum = orig_quorum;
  }
  return ret;
}

/*
 * Calculate the new quorum after add replica.
 *
 * Quorum modification must meet following conditions:
 *  1. First of all, the principle of alter locality need be satisfied.
 *  2. Add replica is paxos member.
 *  3. Add replica has relationship with alter locality.
 *  4. Current quorum satifies:orig locality's paxos num <= quorum <= new locality's paxos num
 *If above conditions are satified, the remove_member operation will increase the current quorum value by 1,
 * and pass the quorum values to obs and persist it until it equal to new locality's paxos num
 *
 * Here we rely on the following assumption:
 *   1. @region locality is not support any more.
 *   2. signal zone has at most one F and one L.
 *
 * Possible situations:
 *   1. In abnormal situation, the quorum value may be pushed too fast, but it will not exceed the pre and new
 *locality's paxos num;
 *   2. After finish alter locality, there may lead to the mismatch between quorum and locality while report quorum not
 *timely. Those situations can be handled by modify quorum.
 */
int ObQuorumGetter::get_add_replica_quorum_size(const uint64_t table_id, const int64_t orig_quorum, const ObZone& zone,
    const ObReplicaType type, int64_t& quorum) const
{
  int ret = OB_SUCCESS;
  int64_t cur_paxos_count = 0;
  ObArray<AlterPaxosLocalityTask> alter_paxos_tasks;
  int64_t add_paxos_num = 0;
  quorum = 0;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), K(table_id));
  } else if (OB_FAIL(get_schema_paxos_replica_num(table_id, cur_paxos_count))) {
    LOG_WARN("fail to get schema paxos num", K(ret), "schema_id", table_id);
  } else if (OB_UNLIKELY(cur_paxos_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("paxos count err", K(ret), K(cur_paxos_count));
  } else if (orig_quorum <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("quorum from meta_table is invalid", K(ret), K(table_id), K(orig_quorum), K(zone), K(type));
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(type)) {
    // add non_paxos member, quorum not change
    quorum = orig_quorum;
    LOG_INFO(
        "add non-paxos member, quorum not change", K(ret), K(type), K(cur_paxos_count), K(table_id), K(orig_quorum));
  } else if (orig_quorum == cur_paxos_count) {
    // quorum do not change while quorum value is equal to target locality's paxos_count,
    quorum = orig_quorum;
    LOG_INFO(
        "reach cur_paxos_count, quorum not change", K(ret), K(type), K(cur_paxos_count), K(table_id), K(orig_quorum));
  } else if (OB_FAIL(get_alter_quorum_replica_task(table_id, orig_quorum, alter_paxos_tasks, add_paxos_num))) {
    LOG_WARN("fail to get alter quorum replica task", K(ret));
  } else if (0 == alter_paxos_tasks.count()) {
    // quorum not change while non alter paxos task.
    quorum = orig_quorum;
    LOG_INFO("paxos members count not change, quorum not change",
        K(ret),
        K(type),
        K(cur_paxos_count),
        K(add_paxos_num),
        K(table_id),
        K(orig_quorum));
  } else if (add_paxos_num <= 0) {
    // quorum not change while alter locality is no need to add paxos member
    quorum = orig_quorum;
    LOG_INFO("inrelevant locality change, quorum not change",
        K(ret),
        K(type),
        K(cur_paxos_count),
        K(add_paxos_num),
        K(table_id),
        K(orig_quorum));
  } else {
    quorum = min(orig_quorum + 1, cur_paxos_count);
  }
  return ret;
}

int ObQuorumGetter::get_transform_quorum_size(const uint64_t table_id, const int64_t orig_quorum, const ObZone& zone,
    const ObReplicaType src_type, const ObReplicaType dst_type, int64_t& quorum) const
{
  int ret = OB_SUCCESS;
  quorum = 0;
  if (ObReplicaTypeCheck::is_paxos_replica_V2(src_type) && !ObReplicaTypeCheck::is_paxos_replica_V2(dst_type)) {
    // src is paxos, and dest is not paxos, it is equivalent to remove src paxos member
    if (OB_FAIL(get_remove_replica_quorum_size(table_id, orig_quorum, zone, src_type, quorum))) {
      LOG_WARN("fail to get remove replica quorum size", K(ret));
    }
  } else {
    // src not paxos, dest is paxos, it is equivalent to add dest paxos member.
    if (OB_FAIL(get_add_replica_quorum_size(table_id, orig_quorum, zone, dst_type, quorum))) {
      LOG_WARN("fail to get add replica quorum size", K(ret));
    }
  }
  return ret;
}

/*
 * Calculate the new quorum after remove replica.
 *
 * Quorum modification must meet following conditions:
 *   1. Fist of all, the principle of alter locality should be satisfied.
 *   2. Reduce member is paxos.
 *   3. Reduce member has relationship with alter locality.
 *   4. Current quorum satisfies: new locality's paxos num <= quorum <= orig locality's paxos num
 * If above conditions are satified, the remove_member operation will reduce the current quorum value by 1,
 * and pass the quorum values to obs and persist it until it equal to new locality's paxos num
 *
 * Here we rely on the following assumption:
 *   1. @region locality is not support any more.
 *   2. signal zone has at most one F and one L.
 *
 * Possible situations:
 *   1. In abnormal situation, the quorum value may be pushed too fast, but it will not exceed the pre and new
 * locality's paxos num;
 *   2. After finish alter locality, there may lead to the mismatch between quorum and locality while permanent offline
 * or report quorum not timely. those situations can be handled by modify quorum.
 */
int ObQuorumGetter::get_remove_replica_quorum_size(const uint64_t table_id, const int64_t orig_quorum,
    const ObZone& zone, const ObReplicaType type, int64_t& quorum) const
{
  int ret = OB_SUCCESS;
  int64_t cur_paxos_count = 0;
  ObArray<AlterPaxosLocalityTask> alter_paxos_tasks;
  int64_t add_paxos_num = 0;
  quorum = 0;
  if (!ObReplicaTypeCheck::is_paxos_replica_V2(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("non paxos replica type", K(ret));
  } else if (OB_FAIL(get_schema_paxos_replica_num(table_id, cur_paxos_count))) {
    LOG_WARN("fail to get schema paxos replica num", K(ret), "schema_id", table_id);
  } else if (OB_UNLIKELY(cur_paxos_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("paxos count error", K(ret), K(cur_paxos_count));
  } else if (orig_quorum <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("quorum from meta_table is invalid", K(ret), K(table_id), K(orig_quorum), K(zone), K(type));
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(type)) {
    // reduce non_paxos member, quorum not change
    quorum = orig_quorum;
    LOG_INFO(
        "remove non-paxos member, quorum not change", K(ret), K(type), K(cur_paxos_count), K(table_id), K(orig_quorum));
  } else if (orig_quorum == cur_paxos_count) {
    // quorum value is equal to paxos_count of target locality, no need change
    quorum = orig_quorum;
    LOG_INFO(
        "reach cur_paxos_count, quorum not change", K(ret), K(type), K(cur_paxos_count), K(table_id), K(orig_quorum));
  } else if (OB_FAIL(get_alter_quorum_replica_task(table_id, orig_quorum, alter_paxos_tasks, add_paxos_num))) {
    LOG_WARN("fail to get alter quorum replica task", K(ret));
  } else if (0 == alter_paxos_tasks.count()) {
    // non paxos members count change, no quorum change
    quorum = orig_quorum;
    LOG_INFO("paxos members count not change, quorum not change",
        K(ret),
        K(type),
        K(cur_paxos_count),
        K(add_paxos_num),
        K(table_id),
        K(orig_quorum));
  } else if (add_paxos_num >= 0) {
    // the trend of alter locality is not reduce quorum
    // reduce member does not change quorum
    quorum = orig_quorum;
    LOG_INFO("inrelevant locality change, quorum not change",
        K(ret),
        K(type),
        K(cur_paxos_count),
        K(add_paxos_num),
        K(table_id),
        K(orig_quorum));
  } else {
    // if the replica reduction task is related to alter locality, it is considered that
    // the task will reduce the quorum value.
    bool finded = false;
    for (int64_t i = 0; OB_SUCC(ret) && !finded && i < alter_paxos_tasks.count(); i++) {
      AlterPaxosLocalityTask& alter_paxos_task = alter_paxos_tasks.at(i);
      if (SUB_QUORUM == alter_paxos_task.task_type_ &&
          has_exist_in_array(alter_paxos_task.associated_replica_type_set_, type) &&
          has_exist_in_array(alter_paxos_task.zone_set_, zone)) {
        finded = true;
      }
    }
    if (finded) {
      quorum = max(orig_quorum - 1, cur_paxos_count);
      LOG_INFO("quorum change", K(ret), K(type), K(cur_paxos_count), K(add_paxos_num), K(table_id), K(orig_quorum));
    } else {
      quorum = orig_quorum;
      LOG_INFO("inrelevant locality change, quorum not change",
          K(ret),
          K(type),
          K(cur_paxos_count),
          K(add_paxos_num),
          K(table_id),
          K(orig_quorum),
          K(alter_paxos_tasks));
    }
  }
  return ret;
}

int ObQuorumGetter::get_table_alter_quorum_replica_task(const uint64_t table_id, const int64_t orig_quorum,
    common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, int64_t& add_paxos_num) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = nullptr;
  const ObTablegroupSchema* tablegroup_schema = nullptr;
  const ObTenantSchema* tenant_schema = nullptr;
  bool finded = false;

  if (OB_FAIL(schema_guard_->get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), "table_id", table_id);
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), "table_id", table_id);
  } else if (!table_schema->get_locality_str().empty()) {
    // has own locality, no need to get from inheritance relationship
    if (table_schema->get_previous_locality_str().empty()) {
      // non alter locality, no paxos change.
      // do not process quorum value and locality not match,
      // use other command to modify quorum
    } else {
      if (OB_FAIL(inner_get_alter_quorum_replica_task(
              table_id, *table_schema, alter_paxos_tasks, orig_quorum, add_paxos_num))) {
        LOG_WARN("fail to get alter paxos replica task", K(ret));
      }
    }
    finded = true;
  }

  if (OB_FAIL(ret)) {
  } else if (finded || !is_new_tablegroup_id(table_schema->get_tablegroup_id())) {
    // skip
  } else if (OB_FAIL(schema_guard_->get_tablegroup_schema(table_schema->get_tablegroup_id(), tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup info", K(ret), "tablegroup_id", table_schema->get_tablegroup_id());
  } else if (NULL == tablegroup_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tablegroup not exist", K(ret), "tablegroup_id", table_schema->get_tablegroup_id());
  } else if (!tablegroup_schema->get_locality_str().empty()) {
    if (tablegroup_schema->get_previous_locality_str().empty()) {
      // non alter locality, no paxos change.
    } else {
      if (OB_FAIL(inner_get_alter_quorum_replica_task(
              table_id, *tablegroup_schema, alter_paxos_tasks, orig_quorum, add_paxos_num))) {
        LOG_WARN("fail to get alter paxos replica task", K(ret));
      }
    }
    finded = true;
  }

  if (OB_FAIL(ret)) {
  } else if (finded) {
    // skip
  } else if (OB_FAIL(schema_guard_->get_tenant_info(table_schema->get_tenant_id(), tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), "tenant_id", table_schema->get_tenant_id());
  } else if (NULL == tenant_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", K(ret), "tenant_id", table_schema->get_tenant_id());
  } else if (tenant_schema->get_locality_str().empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant locality null", K(ret), K(*tenant_schema));
  } else {
    if (tenant_schema->get_previous_locality_str().empty()) {
      // non alter locality, no paxos change.
    } else {
      if (OB_FAIL(inner_get_alter_quorum_replica_task(
              table_id, *tenant_schema, alter_paxos_tasks, orig_quorum, add_paxos_num))) {
        LOG_WARN("fail to get alter paxos replica task", K(ret));
      }
    }
  }
  return ret;
}

int ObQuorumGetter::get_tablegroup_alter_quorum_replica_task(const uint64_t tablegroup_id, const int64_t orig_quorum,
    common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, int64_t& add_paxos_num) const
{
  int ret = OB_SUCCESS;
  const ObTablegroupSchema* tablegroup_schema = nullptr;
  const ObTenantSchema* tenant_schema = nullptr;
  bool finded = false;

  if (OB_FAIL(schema_guard_->get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup info", K(ret), K(tablegroup_id));
  } else if (NULL == tablegroup_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tablegroup not exist", K(ret), K(tablegroup_id));
  } else if (!tablegroup_schema->get_locality_str().empty()) {
    if (tablegroup_schema->get_previous_locality_str().empty()) {
      // non alter locality, no paxos change.
    } else {
      if (OB_FAIL(inner_get_alter_quorum_replica_task(
              tablegroup_id, *tablegroup_schema, alter_paxos_tasks, orig_quorum, add_paxos_num))) {
        LOG_WARN("fail to get alter paxos replica task", K(ret));
      }
    }
    finded = true;
  }

  if (OB_FAIL(ret)) {
  } else if (finded) {
    // skip
  } else if (OB_FAIL(schema_guard_->get_tenant_info(tablegroup_schema->get_tenant_id(), tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), "tenant_id", tablegroup_schema->get_tenant_id());
  } else if (NULL == tenant_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", K(ret), "tenant_id", tablegroup_schema->get_tenant_id());
  } else if (tenant_schema->get_locality_str().empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant locality null", K(ret), K(*tenant_schema));
  } else {
    if (tenant_schema->get_previous_locality_str().empty()) {
      // non alter locality, no paxos change.
    } else {
      if (OB_FAIL(inner_get_alter_quorum_replica_task(
              tablegroup_id, *tenant_schema, alter_paxos_tasks, orig_quorum, add_paxos_num))) {
        LOG_WARN("fail to get alter paxos replica task", K(ret));
      }
    }
  }
  return ret;
}

// get task type of alter locality:
// 1. alter_paxos_tasks: record add/reduce paxos task type.
//    in type_transform task, will generate add and reduce paxos task at the same time.
// 2. add_paxos_num: the difference of quorum value before and after this alter locality
int ObQuorumGetter::get_alter_quorum_replica_task(const uint64_t table_id, const int64_t orig_quorum,
    ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, int64_t& add_paxos_num) const
{
  int ret = OB_SUCCESS;
  alter_paxos_tasks.reset();
  add_paxos_num = 0;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is invalid", K(ret), K(table_id));
  } else if (orig_quorum <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("quorum is invalid", K(ret), K(table_id), K(orig_quorum));
  } else {
    if (!is_tablegroup_id(table_id)) {
      if (OB_FAIL(get_table_alter_quorum_replica_task(table_id, orig_quorum, alter_paxos_tasks, add_paxos_num))) {
        LOG_WARN("fail to get table alter quorum replica task", K(ret));
      }
    } else {
      if (OB_FAIL(get_tablegroup_alter_quorum_replica_task(table_id, orig_quorum, alter_paxos_tasks, add_paxos_num))) {
        LOG_WARN("fail to get tablegroup alter quorum replica task", K(ret));
      }
    }
  }
  return ret;
}

int ObQuorumGetter::construct_zone_region_list(common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
    const common::ObIArray<common::ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  zone_region_list.reset();
  if (OB_UNLIKELY(NULL == zone_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("zone mgr is null", K(ret));
  } else {
    common::ObArray<share::ObZoneInfo> zone_infos;
    if (OB_FAIL(zone_mgr_->get_zone(zone_infos))) {
      LOG_WARN("fail to get zone", K(ret));
    } else {
      ObZoneRegion zone_region;
      for (int64_t i = 0; i < zone_infos.count() && OB_SUCC(ret); ++i) {
        zone_region.reset();
        share::ObZoneInfo& zone_info = zone_infos.at(i);
        if (OB_FAIL(zone_region.zone_.assign(zone_info.zone_.ptr()))) {
          LOG_WARN("fail to assign zone", K(ret));
        } else if (OB_FAIL(zone_region.region_.assign(zone_info.region_.info_.ptr()))) {
          LOG_WARN("fail to assign region", K(ret));
        } else if (!has_exist_in_array(zone_list, zone_region.zone_)) {
          // this zone do not exist in my zone list, ignore it
        } else if (OB_FAIL(zone_region_list.push_back(zone_region))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more to do
      }
    }
  }
  return ret;
}

int ObQuorumGetter::get_tenant_pool_zone_list(
    const uint64_t tenant_id, common::ObIArray<common::ObZone>& pool_zone_list) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_tenant_pool_zone_list(tenant_id, pool_zone_list))) {
    LOG_WARN("fail to get tenant pool zone list", K(ret));
  } else {
  }  // no more to do
  return ret;
}

template <typename SCHEMA>
int ObQuorumGetter::inner_get_alter_quorum_replica_task(const int64_t table_id, const SCHEMA& schema,
    ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, const int64_t orig_quorum, int64_t& add_paxos_num) const
{
  int ret = OB_SUCCESS;
  alter_paxos_tasks.reset();
  UNUSED(table_id);
  ObArray<share::ObZoneReplicaAttrSet> pre_zone_locality;
  ObArray<share::ObZoneReplicaAttrSet> cur_zone_locality;
  ObArray<common::ObZone> pool_zone_list;
  ObArray<share::schema::ObZoneRegion> zone_region_list;
  const ObString& previous_locality = schema.get_previous_locality_str();
  add_paxos_num = 0;
  if (previous_locality.empty()) {
    // defensive check, previous_locality can not be empty.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("previous locality null", K(ret), K(schema));
  } else if (orig_quorum <= 0) {
    // obs will report quorum value owner by clog(only leader), there are two situations while quorum value is -1:
    // 1. quorum value has not be reportted to meta_table.
    // 2. the partition is leaderless
    // in both case, alter quorum is not allowed
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("quorum is invalid", K(ret), K(orig_quorum));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr is null", K(ret));
  } else if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(*schema_guard_, cur_zone_locality))) {
    LOG_WARN("fail to get zone replica num array", K(ret));
  } else if (OB_FAIL(get_tenant_pool_zone_list(schema.get_tenant_id(), pool_zone_list))) {
    LOG_WARN("fail to get tenant pool zone list", K(ret));
  } else if (OB_FAIL(construct_zone_region_list(zone_region_list, pool_zone_list))) {
    LOG_WARN("fail to get zone region list", K(ret));
  } else {
    ObLocalityDistribution locality_dist;
    if (OB_FAIL(locality_dist.init())) {
      LOG_WARN("fail to init locality dist", K(ret));
    } else if (OB_FAIL(locality_dist.parse_locality(previous_locality, pool_zone_list, &zone_region_list))) {
      LOG_WARN("fail to parse locality", K(ret));
    } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(pre_zone_locality))) {
      LOG_WARN("fail to get zone region replica num array", K(ret));
    }

    int64_t pre_paxos_num = 0;
    int64_t cur_paxos_num = 0;
    bool non_paxos_locality_modified = false;  // not used
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObLocalityCheckHelp::check_alter_locality(pre_zone_locality,
                   cur_zone_locality,
                   alter_paxos_tasks,
                   non_paxos_locality_modified,
                   pre_paxos_num,
                   cur_paxos_num))) {
      LOG_WARN("fail to check and get paxos replica task", K(ret), K(pre_zone_locality), K(cur_zone_locality));
    } else {
      // check current quorum value meets the upper and lower limit of locality and previous_locality
      int64_t max_quorum = max(pre_paxos_num, cur_paxos_num);
      int64_t min_quorum = min(pre_paxos_num, cur_paxos_num);
      if (orig_quorum < min_quorum || orig_quorum > max_quorum) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("quorum is invalid", K(ret), K(orig_quorum), K(cur_paxos_num), K(pre_paxos_num));
      } else {
        add_paxos_num = cur_paxos_num - pre_paxos_num;
      }
    }
  }
  return ret;
}

int ObRootUtils::get_rs_default_timeout_ctx(ObTimeoutCtx& ctx)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = 2 * 1000 * 1000;  // 2s
  int64_t abs_timeout_us = ctx.get_abs_timeout();
  int64_t worker_timeout_us = THIS_WORKER.get_timeout_ts();

  if (0 < abs_timeout_us) {
    // nothing
    // ctx was setted, no need to set again
  } else if (INT64_MAX == worker_timeout_us) {
    // is backgroup thread, set timeout is 2s.
    abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US;
  } else if (0 < worker_timeout_us) {
    // if work has timeouts, set timeout equal to work's
    abs_timeout_us = worker_timeout_us;
  } else {
    // if work has no timeout, it is not possible, but ignore error, set timeout to 2s
    abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US;
  }

  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
    LOG_WARN("set timeout failed", K(ret), K(abs_timeout_us));
  } else if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("is timeout",
        K(ret),
        "abs_timeout",
        ctx.get_abs_timeout(),
        "this worker timeout ts",
        THIS_WORKER.get_timeout_ts());
  }
  return ret;
}

// get all observer that is stopeed, start_service_time<=0 and lease expire
int ObRootUtils::get_invalid_server_list(
    ObZoneManager& zone_mgr, ObServerManager& server_mgr, ObIArray<ObAddr>& invalid_server_list)
{
  int ret = OB_SUCCESS;
  invalid_server_list.reset();
  ObArray<ObAddr> stopped_server_list;
  ObArray<ObZone> stopped_zone_list;
  ObArray<ObServerStatus> server_list;
  ObZone empty_zone;
  if (OB_FAIL(get_stopped_zone_list(zone_mgr, server_mgr, stopped_zone_list, stopped_server_list))) {
    LOG_WARN("fail to get stopped zone list", KR(ret));
  } else if (OB_FAIL(invalid_server_list.assign(stopped_server_list))) {
    LOG_WARN("fail to assign array", KR(ret), K(stopped_zone_list));
  } else if (OB_FAIL(server_mgr.get_server_statuses(empty_zone, server_list))) {
    LOG_WARN("fail to get servers of zone", KR(ret));
  } else {
    for (int64_t i = 0; i < server_list.count() && OB_SUCC(ret); i++) {
      const ObServerStatus& status = server_list.at(i);
      if ((!status.is_alive() || !status.in_service()) && !has_exist_in_array(invalid_server_list, status.server_)) {
        if (OB_FAIL(invalid_server_list.push_back(status.server_))) {
          LOG_WARN("fail to push back", KR(ret), K(status));
        }
      }
    }
  }
  return ret;
}

int ObRootUtils::get_stopped_zone_list(ObZoneManager& zone_mgr, ObServerManager& server_mgr,
    ObIArray<ObZone>& stopped_zone_list, ObIArray<ObAddr>& stopped_server_list)
{
  int ret = OB_SUCCESS;
  ObServerManager::ObServerStatusArray server_array;
  ObZone empty_zone;
  if (OB_FAIL(server_mgr.get_server_statuses(empty_zone, server_array))) {
    LOG_WARN("fail to get server status", KR(ret));
  } else {
    for (int64_t i = 0; i < server_array.count() && OB_SUCC(ret); i++) {
      if (!server_array.at(i).is_stopped()) {
        // nothing todo
      } else {
        if (has_exist_in_array(stopped_zone_list, server_array.at(i).zone_)) {
          // nothing todo
        } else if (OB_FAIL(stopped_zone_list.push_back(server_array.at(i).zone_))) {
          LOG_WARN("fail to push back", KR(ret), "zone", server_array.at(i).zone_);
        }
        if (OB_FAIL(ret)) {
        } else if (has_exist_in_array(stopped_server_list, server_array.at(i).server_)) {
          // nothing todo
        } else if (OB_FAIL(stopped_server_list.push_back(server_array.at(i).server_))) {
          LOG_WARN("fail to push back", KR(ret), "server", server_array.at(i).server_);
        }
      }
    }
  }
  LOG_INFO("get stop observer", KR(ret), K(stopped_zone_list), K(stopped_server_list));
  // get stopped zone;
  ObArray<ObZoneInfo> zone_infos;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(zone_mgr.get_zone(zone_infos))) {
    LOG_WARN("fail to get zone", K(ret));
  } else {
    for (int64_t i = 0; i < zone_infos.count() && OB_SUCC(ret); i++) {
      if (ObZoneStatus::ACTIVE == zone_infos.at(i).status_) {
        // nothing todo
      } else {
        if (has_exist_in_array(stopped_zone_list, zone_infos.at(i).zone_)) {
          // nothing todo
        } else if (OB_FAIL(stopped_zone_list.push_back(zone_infos.at(i).zone_))) {
          LOG_WARN("fail to push back", KR(ret));
        }
        ObArray<common::ObAddr> server_list;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(server_mgr.get_servers_of_zone(zone_infos.at(i).zone_, server_list))) {
          LOG_WARN("fail to get server of zone", KR(ret), K(i), "zone", zone_infos.at(i).zone_);
        } else {
          for (int64_t j = 0; j < server_list.count() && OB_SUCC(ret); j++) {
            if (has_exist_in_array(stopped_server_list, server_list.at(j))) {
              // nothing todo
            } else if (OB_FAIL(stopped_server_list.push_back(server_list.at(j)))) {
              LOG_WARN("fail to push back", KR(ret), K(j));
            }
          }
        }
      }  // end else ACTIVE
    }    // end for zone_infos
  }
  LOG_INFO("get stopped zone list", KR(ret), K(stopped_server_list), K(stopped_zone_list));
  return ret;
}

int ObRootUtils::get_tenant_intersection(ObUnitManager& unit_mgr, ObIArray<ObAddr>& this_server_list,
    ObIArray<ObAddr>& other_server_list, ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObHashSet<uint64_t> this_tenant_ids_set;
  ObHashSet<uint64_t> other_tenant_ids_set;
  const int64_t TENANT_BUCKET_NUM = 1000;
  if (OB_FAIL(this_tenant_ids_set.create(TENANT_BUCKET_NUM))) {
    LOG_WARN("fail to create hashset", KR(ret));
  } else if (OB_FAIL(other_tenant_ids_set.create(TENANT_BUCKET_NUM))) {
    LOG_WARN("fail to create hashset", KR(ret));
  }
  for (int64_t i = 0; i < this_server_list.count() && OB_SUCC(ret); i++) {
    const ObAddr& server = this_server_list.at(i);
    if (OB_FAIL(unit_mgr.get_tenants_of_server(server, this_tenant_ids_set))) {
      LOG_WARN("fail to get tenant unit", KR(ret), K(server));
    }
  }
  for (int64_t i = 0; i < other_server_list.count() && OB_SUCC(ret); i++) {
    const ObAddr& server = other_server_list.at(i);
    if (OB_FAIL(unit_mgr.get_tenants_of_server(server, other_tenant_ids_set))) {
      LOG_WARN("fail to get tenant unit", KR(ret), K(server));
    }
  }
  ObHashSet<uint64_t>::const_iterator iter;
  for (iter = this_tenant_ids_set.begin(); iter != this_tenant_ids_set.end() && OB_SUCC(ret); iter++) {
    if (OB_FAIL(other_tenant_ids_set.exist_refactored(iter->first))) {
      if (OB_HASH_EXIST == ret) {
        if (OB_FAIL(tenant_ids.push_back(iter->first))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to check exist", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", KR(ret));
    }
  }  // end for iter
  return ret;
}

template <class T>
bool ObRootUtils::has_intersection(const common::ObIArray<T>& this_array, const common::ObIArray<T>& other_array)
{
  bool bret = false;
  for (int64_t i = 0; i < this_array.count() && !bret; i++) {
    if (has_exist_in_array(other_array, this_array.at(i))) {
      bret = true;
    }
  }
  return bret;
}

template <class T>
bool ObRootUtils::is_subset(const common::ObIArray<T>& superset_array, const common::ObIArray<T>& array)
{
  bool bret = true;
  for (int64_t i = 0; i < array.count() && bret; i++) {
    if (has_exist_in_array(superset_array, array.at(i))) {
      // nothing todo
    } else {
      bret = false;
    }
  }
  return bret;
}

// iter the tenant, table's primary_zone is covered by zone_list
int ObRootUtils::check_primary_region_in_zonelist(ObMultiVersionSchemaService* schema_service,
    ObDDLService* ddl_service, ObUnitManager& unit_mgr, ObZoneManager& zone_mgr, const ObIArray<uint64_t>& tenant_ids,
    const ObIArray<ObZone>& zone_list, bool& is_in)
{
  int ret = OB_SUCCESS;
  is_in = false;
  if (0 >= zone_list.count() || OB_ISNULL(schema_service) || OB_ISNULL(ddl_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_list));
  } else if (0 == tenant_ids.count()) {
    is_in = false;
  } else {
    int64_t primary_zone_count = 0;
    ObSchemaGetterGuard tenant_schema_guard;
    ObArray<ObZone> tenant_zone_list;
    const ObTenantSchema* tenant_info = NULL;
    const ObSysVariableSchema* sys_variable = nullptr;
    const ObSysVarSchema* pz_count_schema = nullptr;
    const common::ObString var_name(share::OB_SV__PRIMARY_ZONE_ENTITY_COUNT);
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    common::ObObj pz_obj;
    for (int64_t i = 0; i < tenant_ids.count() && !is_in; i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      primary_zone_count = 0;
      sys_variable = nullptr;
      pz_count_schema = nullptr;
      tenant_zone_list.reset();
      int64_t cur_ts_type = 0;
      if (OB_FAIL(unit_mgr.get_tenant_pool_zone_list(tenant_id, tenant_zone_list))) {
        LOG_WARN("fail to get tenant pool zone list", KR(ret), K(tenant_id));
      } else if (is_subset(zone_list, tenant_zone_list)) {
        // all zones of tenant is in the zone_list
        is_in = true;
      } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, tenant_schema_guard))) {
        LOG_WARN("get_schema_guard failed", K(ret));
      } else if (OB_FAIL(tenant_schema_guard.get_tenant_info(tenant_id, tenant_info))) {
        LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_info)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("invalid tenant info", KR(ret), K(tenant_id));
      } else if (OB_FAIL(tenant_schema_guard.get_sys_variable_schema(tenant_id, sys_variable))) {
        LOG_WARN("fail to get sys variable schema", K(ret), KR(tenant_id));
      } else if (OB_UNLIKELY(nullptr == sys_variable)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("sys variable ptr is null", KR(ret));
      } else if (OB_FAIL(sys_variable->get_sysvar_schema(var_name, pz_count_schema))) {
        LOG_WARN("fail to get sysvar schema", KR(ret));
      } else if (nullptr == pz_count_schema) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("primary zone count schema ptr is null", K(ret));
      } else if (OB_FAIL(pz_count_schema->get_value(&allocator, nullptr /*time zone info*/, pz_obj))) {
        LOG_WARN("fail to get value", K(ret));
      } else if (OB_FAIL(pz_obj.get_int(primary_zone_count))) {
        LOG_WARN("fail to get int from pz obj", K(ret));
      } else if (0 == primary_zone_count) {
        // only tenant has primary_zone, check tenant is enough
        bool has = false;
        if (OB_FAIL(check_left_f_in_primary_zone(zone_mgr, tenant_schema_guard, *tenant_info, zone_list, has))) {
          LOG_WARN("fail to check tenant locality", KR(ret), K(tenant_id));
        } else if (!has) {
          is_in = true;
          LOG_INFO("tenant primary zone has no full replica exist", K(tenant_id), K(zone_list));
        }
      } else if (OB_FAIL(tenant_schema_guard.get_timestamp_service_type(tenant_id, cur_ts_type))) {
        LOG_WARN("fail to get cur ts type", K(ret));
      } else if (transaction::is_ts_type_external_consistent(cur_ts_type)) {
        // if gts is on, primary_zone of tenant and tables must be in the same region.
        // the paoxs replica's location of  tenant and tables must be the same.
        // based on the above considerations, only check there is F left in tenant's primary_zone.
        bool has = false;
        if (OB_FAIL(check_left_f_in_primary_zone(zone_mgr, tenant_schema_guard, *tenant_info, zone_list, has))) {
        } else if (!has) {
          is_in = true;
          LOG_INFO("tenant primary zone has no full replica exist", K(tenant_id), K(zone_list));
        }
      } else {
        // iter all tables's primary_zone under tenant
        bool tenant_is_in = false;
        if (OB_FAIL(check_tenant_schema_primary_region_in_zonelist(
                zone_mgr, *tenant_info, tenant_schema_guard, zone_list, primary_zone_count, tenant_is_in))) {
          LOG_WARN("fail to check tenant schema in zone list", KR(ret), K(tenant_info));
        } else if (tenant_is_in) {
          is_in = true;
          LOG_INFO("schema in tenant may have no more full replica exist", K(tenant_id), K(zone_list));
        }
      }
    }  // end for tennant_ids
  }    // end else
  LOG_INFO("check primary region in zonelist", KR(ret), K(tenant_ids), K(zone_list), K(is_in));
  return ret;
}

int ObRootUtils::get_primary_zone(
    ObZoneManager& zone_mgr, const ObIArray<ObZoneScore>& zone_score_array, ObIArray<ObZone>& primary_zone)
{
  int ret = OB_SUCCESS;
  int64_t first_score = -1;
  ObArray<ObZone> zone_list;
  if (OB_UNLIKELY(0 >= zone_score_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_score_array));
  } else {
    for (int64_t i = 0; i < zone_score_array.count() && OB_SUCC(ret); i++) {
      const ObZoneScore& score = zone_score_array.at(i);
      if (-1 == first_score) {
        first_score = score.score_;
        if (OB_FAIL(zone_list.push_back(score.zone_))) {
          LOG_WARN("fail to push back", KR(ret), K(score));
        }
      } else if (first_score == score.score_) {
        if (OB_FAIL(zone_list.push_back(score.zone_))) {
          LOG_WARN("fail to push back", KR(ret), "zone", zone_score_array.at(i).zone_);
        }
      }
    }
    // get all zones in one region
    ObArray<ObRegion> region_list;
    ObRegion region;
    for (int64_t i = 0; i < zone_list.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(zone_mgr.get_region(zone_list.at(i), region))) {
        LOG_WARN("fail to get region", KR(ret), KR(i), K(zone_list));
      } else if (has_exist_in_array(region_list, region)) {
        // nothing todo
      } else if (OB_FAIL(region_list.push_back(region))) {
        LOG_WARN("fail to push region", KR(ret), K(region));
      }
    }
    // get all read_write zone
    ObArray<ObZoneInfo> all_zones;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(zone_mgr.get_zone(all_zones))) {
      LOG_WARN("fail to get zone", KR(ret));
    } else {
      for (int64_t i = 0; i < all_zones.count() && OB_SUCC(ret); i++) {
        const ObZoneInfo& zone = all_zones.at(i);
        ObZoneType zone_type = static_cast<ObZoneType>(zone.zone_type_.value_);
        ObRegion region(zone.region_.info_.ptr());
        if (has_exist_in_array(region_list, region) && common::ZONE_TYPE_READWRITE == zone_type) {
          if (OB_FAIL(primary_zone.push_back(zone.zone_))) {
            LOG_WARN("fail to push back", KR(ret), K(zone));
          }
        }
      }
    }
  }
  return ret;
}

template <class T>
int ObRootUtils::check_left_f_in_primary_zone(ObZoneManager& zone_mgr, ObSchemaGetterGuard& schema_guard,
    const T& schema_info, const ObIArray<ObZone>& zone_list, bool& has)
{
  int ret = OB_SUCCESS;
  has = false;
  ObArray<ObZoneReplicaAttrSet> zone_locality_array;
  ObSchema tmp_schema;
  ObPrimaryZone primary_zone(&tmp_schema);
  ObArray<ObZone> primary_zone_array;
  if (OB_UNLIKELY(!schema_info.is_valid()) || OB_UNLIKELY(0 >= zone_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_info), K(zone_list));
  } else if (OB_FAIL(schema_info.get_primary_zone_inherit(schema_guard, primary_zone))) {
    LOG_WARN("fail to get primary zone", KR(ret));
  } else if (ObPrimaryZoneUtil::no_need_to_check_primary_zone(primary_zone.get_primary_zone())) {
    // if primary_zone is random, no need to check left f in primary zone
    has = true;
    LOG_INFO("primary zone is RANDOM or empty, no need to check", KR(ret), K(primary_zone));
  } else if (OB_FAIL(schema_info.get_zone_replica_attr_array_inherit(schema_guard, zone_locality_array))) {
    LOG_WARN("fail to get zone replica array", KR(ret));
  } else if (OB_FAIL(get_primary_zone(zone_mgr, primary_zone.get_primary_zone_array(), primary_zone_array))) {
    LOG_WARN("fail to get primary zone", KR(ret), K(primary_zone));
  } else {
    for (int64_t i = 0; i < primary_zone_array.count() && OB_SUCC(ret) && !has; i++) {
      const ObZone& zone = primary_zone_array.at(i);
      for (int64_t j = 0; j < zone_locality_array.count() && OB_SUCC(ret) && !has; j++) {
        const ObZoneReplicaAttrSet& set = zone_locality_array.at(j);
        int64_t full_replica_num = set.get_full_replica_num();
        if (0 < full_replica_num && (has_exist_in_array(set.zone_set_, zone) || zone == set.zone_) &&
            !has_intersection(set.zone_set_, zone_list)) {
          // there is F replica in the zone_set where the primary_zone is located,
          // and, no zone of the zone_set is in zone_list(stopped).
          has = true;
        }
      }  // end for zone_locality_array
    }    // end primary_zone_array
  }
  LOG_INFO("check left f in primary zone", KR(ret), K(zone_list), K(has));
  return ret;
}

// iter all tables under the tenant.
// check primary_zone of all tables is not stopped.
int ObRootUtils::check_tenant_schema_primary_region_in_zonelist(ObZoneManager& zone_mgr,
    const ObTenantSchema& tenant_info, ObSchemaGetterGuard& schema_guard, const ObIArray<ObZone>& zone_list,
    const int64_t primary_zone_count, bool& is_in)
{
  int ret = OB_SUCCESS;
  bool need_check_tenant_info = false;
  ObArray<const ObSimpleTableSchemaV2*> tables;
  ObArray<const ObTablegroupSchema*> tablegroups;
  is_in = false;
  uint64_t tenant_id = tenant_info.get_tenant_id();
  if (OB_UNLIKELY(!tenant_info.is_valid()) || OB_UNLIKELY(0 >= zone_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_info), K(zone_list));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("fail to get table in tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups))) {
    LOG_WARN("fail to get tablegroup in tenant", KR(ret), K(tenant_id));
  } else {
    // check all tables
    int64_t tmp_pz_count = 0;
    for (int64_t i = 0; i < tables.count() && OB_SUCC(ret) && !is_in && tmp_pz_count < primary_zone_count; i++) {
      bool has = false;
      if (OB_ISNULL(tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid table", KR(ret), K(i));
      } else if (!tables.at(i)->has_self_partition()) {
        // nothing todo
      } else if (tables.at(i)->get_primary_zone().empty()) {
        need_check_tenant_info = true;
      } else if (FALSE_IT(tmp_pz_count = tmp_pz_count + 1)) {
        // nothing todo
      } else if (OB_FAIL(check_left_f_in_primary_zone(zone_mgr, schema_guard, *tables.at(i), zone_list, has))) {
        LOG_WARN("fail to check has left full replica in primary zone", KR(ret), K(i), K(tenant_id));
      } else if (!has) {
        LOG_INFO("table primary zone has no full replica exist",
            K(tenant_id),
            "table_id",
            tables.at(i)->get_table_id(),
            K(zone_list));
        is_in = true;
      }
    }
    // check all tablegroups
    for (int64_t i = 0; i < tablegroups.count() && OB_SUCC(ret) && !is_in && tmp_pz_count < primary_zone_count; i++) {
      bool has = false;
      if (OB_ISNULL(tablegroups.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid table", KR(ret), K(i));
      } else if (!tablegroups.at(i)->has_self_partition()) {
        // nothing todo
      } else if (tablegroups.at(i)->get_primary_zone().empty()) {
        need_check_tenant_info = true;
      } else if (FALSE_IT(tmp_pz_count = tmp_pz_count + 1)) {
        // nothing todo
      } else if (OB_FAIL(check_left_f_in_primary_zone(zone_mgr, schema_guard, *tablegroups.at(i), zone_list, has))) {
        LOG_WARN("fail to check has left full replica in primary zone", KR(ret), K(i), K(tenant_id));
      } else if (!has) {
        LOG_INFO("tablegroup primary zone has no full replica exist",
            K(tenant_id),
            "tablegroup_id",
            tablegroups.at(i)->get_tablegroup_id(),
            K(zone_list));
        is_in = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_in) {
  } else if (need_check_tenant_info) {
    bool has = false;
    if (OB_FAIL(check_left_f_in_primary_zone(zone_mgr, schema_guard, tenant_info, zone_list, has))) {
      LOG_WARN("fail to check has left full replica in primary zone", KR(ret), K(tenant_id));
    } else if (!has) {
      LOG_INFO("tenant primary zone has no full replica exist", K(tenant_id), K(zone_list));
      is_in = true;
    }
  }
  return ret;
}
///////////////////////////////
ObClusterType ObClusterInfoGetter::get_cluster_type_v2()
{
  return PRIMARY_CLUSTER;
}

ObClusterType ObClusterInfoGetter::get_cluster_type()
{
  return PRIMARY_CLUSTER;
}
