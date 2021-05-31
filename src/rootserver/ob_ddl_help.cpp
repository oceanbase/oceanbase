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
#include "ob_ddl_help.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_zone_manager.h"
#include "share/ob_primary_zone_util.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_partition_modify.h"
#include "lib/string/ob_sql_string.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "rootserver/ob_partition_creator.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace schema;
using namespace obrpc;

namespace rootserver {
// Create a table and join the tablegroup created after 2.0,
// table's primary_zone&locality must be consistent with tablegroup's
int ObTableGroupHelp::process_tablegroup_option_for_create_table(
    ObSchemaGetterGuard& schema_guard, ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tablegroup_id = table_schema.get_tablegroup_id();
  const ObTablegroupSchema* tablegroup_schema = NULL;
  if (!is_new_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tablegroup schema", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_service is null", K(ret));
  } else if (OB_FAIL(ddl_service_->check_locality_and_primary_zone_complete_match(
                 schema_guard, table_schema, *tablegroup_schema))) {
    LOG_WARN("fail to check locality and primary_zone", K(table_schema), KPC(tablegroup_schema));
  } else {
    // To make sure primary_zone&locality of created table is consistent with tablegroup's,
    // reset primary_zone and locality. It means to inherit from tablegroup.
    table_schema.reset_primary_zone_options();
    table_schema.reset_locality_options();
  }
  return ret;
}

int ObTableGroupHelp::process_tablegroup_option_for_alter_table(
    ObSchemaGetterGuard& schema_guard, const ObTableSchema& orig_table_schema, ObTableSchema& new_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tablegroup_id = new_table_schema.get_tablegroup_id();
  uint64_t orig_tablegroup_id = orig_table_schema.get_tablegroup_id();
  new_table_schema.set_tablegroup_id(orig_tablegroup_id);
  const ObTablegroupSchema* tablegroup_schema = NULL;
  if (!is_new_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tablegroup schema", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_service is null", K(ret));
  } else if (OB_FAIL(ddl_service_->check_locality_and_primary_zone_complete_match(
                 schema_guard, new_table_schema, *tablegroup_schema))) {
    LOG_WARN("fail to check locality and primary_zone", K(orig_table_schema), KPC(tablegroup_schema));
  } else {
    // To make sure primary_zone&locality of created table is consistent with tablegroup's,
    // reset primary_zone and locality. It means to inherit from tablegroup.
    new_table_schema.reset_primary_zone_options();
    new_table_schema.reset_locality_options();
    if (OB_SUCC(ret)) {
      new_table_schema.set_tablegroup_id(tablegroup_id);
    }
  }
  return ret;
}

/**
 * alter tablegroup add table_list
 * It can be considered as a batch implementation of alter table set tablegroup.
 *
 * In order to gradually migrate the table to the tablegroup created after 2.0, this method does
 * not support adding table to the tablegroup created before 2.0.
 */
int ObTableGroupHelp::add_tables_to_tablegroup(ObMySQLTransaction& trans, ObSchemaGetterGuard& schema_guard,
    const ObTablegroupSchema& tablegroup_schema, const ObAlterTablegroupArg& arg)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableItem>& table_items = arg.table_items_;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (OB_UNLIKELY(!trans.is_started()) || OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (table_items.count() <= 0) {
    // nothing todo
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    // In order to gradually migrate the table to the tablegroup created after 2.0, this method does
    // not support adding table to the tablegroup created before 2.0.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl_service is null", K(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    const bool is_index = false;
    const uint64_t tenant_id = arg.tenant_id_;
    ObString tablegroup_name = tablegroup_schema.get_tablegroup_name();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      const ObTableItem& table_item = table_items.at(i);
      const ObTableSchema* table_schema = NULL;
      uint64_t database_id = common::OB_INVALID_ID;
      // check database exist
      if (OB_FAIL(schema_guard.get_database_id(tenant_id, table_item.database_name_, database_id))) {
        LOG_WARN("failed to get database schema!", K(database_id), K(tenant_id), K(table_item));
      } else if (OB_FAIL(schema_guard.get_table_schema(
                     tenant_id, database_id, table_item.table_name_, is_index, table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_id), K(table_item));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(table_item.database_name_), to_cstring(table_item.table_name_));
        LOG_WARN("table not exist!", K(tenant_id), K(database_id), K(table_item), K(ret));
      } else if (is_inner_table(table_schema->get_table_id())) {
        // the tablegroup of sys table must be oceanbase
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("sys table's tablegroup should be oceanbase", KR(ret), K(arg), KPC(table_schema));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set the tablegroup of system table besides oceanbase");
      } else {
        ObTableSchema new_table_schema;
        ObSqlString sql;
        if (OB_FAIL(new_table_schema.assign(*table_schema))) {
          LOG_WARN("fail to assign schema", K(ret));
        } else {
          new_table_schema.set_tablegroup_id(tablegroup_id);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(check_table_alter_tablegroup(schema_guard, *table_schema, new_table_schema))) {
          LOG_WARN("fail to check primary zone and locality", K(ret));
        } else if (OB_FAIL(sql.append_fmt("ALTER TABLEGROUP %.*s ADD TABLE %.*s.%.*s",
                       tablegroup_name.length(),
                       tablegroup_name.ptr(),
                       table_item.database_name_.length(),
                       table_item.database_name_.ptr(),
                       table_item.table_name_.length(),
                       table_item.table_name_.ptr()))) {
          LOG_WARN("failed to append sql", K(ret));
        } else if (OB_FAIL(ddl_service_->check_tablegroup_in_single_database(schema_guard, new_table_schema))) {
          LOG_WARN("fail to check tablegroup in single database", K(ret));
        } else {
          ObString sql_str = sql.string();
          if (OB_FAIL(ddl_operator.alter_tablegroup(schema_guard, new_table_schema, trans, &sql_str))) {
            LOG_WARN("ddl operator alter tablegroup failed", K(ret), K(tenant_id), K(tablegroup_id));
          }
        }
      }  // no more
    }    // end for get all tableschema
  }
  return ret;
}

/**
 * Move table between tablegroups, or moved out of and into tablegroup, check the following conditions:
 * 1. To move into tablegroup created after 2.0, table's primary_zone&locality are required to be
 *    completely consistent with tablegroup's. If they are consistent, reset primary_zone&locality
 *    to inherit semantics.
 * 2. Remove table from tablegroup, do not check primary_zone&locality;
 * 3. In order to gradually migrate the table to the tablegroup created after 2.0, this method
 *    does not support adding table to the tablegroup created before 2.0.
 * 4. In order to avoid changing locality when previous one is not finished. If the source
 *    tenant/tablegroup or the destination tenant/tablegroup is in the process of changing locality,
 *    it is not allowed to change the tablegroup of table.
 */
int ObTableGroupHelp::check_table_alter_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTableSchema& orig_table_schema, share::schema::ObTableSchema& new_table_schema,
    bool alter_primary_zone /*=false*/)
{
  int ret = OB_SUCCESS;
  bool can_alter_tablegroup = is_new_tablegroup_id(new_table_schema.get_tablegroup_id()) ||
                              OB_INVALID_ID == new_table_schema.get_tablegroup_id();
  ObString src_previous_locality_str;
  ObString dst_previous_locality_str;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
  if (!can_alter_tablegroup) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cann't alter table's tablegroup",
        K(ret),
        "src_tg_id",
        orig_table_schema.get_tablegroup_id(),
        "dst_tg_id",
        new_table_schema.get_tablegroup_id());
  } else if (OB_FAIL(check_locality_in_modification(
                 schema_guard, orig_table_schema, new_table_schema.get_tablegroup_id()))) {
    // Check the source and destination whether are changing locality
    LOG_WARN("fail to check if locality is changing", K(ret));
  } else if (OB_INVALID_ID == new_table_schema.get_tablegroup_id()) {
    /**
     * When remove table from tablegroup, the following conditions need to be guaranteed:
     * 1) zone configured in table's primary_zone exists
     * 2) The locality of table can be transformed by at most one step to ensure that the locality
     *    matches with the paxos members in tenant's locality (regardless of whether the table is
     *    in tablegroup or not)
     * Based on above conditions, Remove table from tablegroup, do not need to check primary_zone&locality
     */
    if (OB_FAIL(set_table_options_for_reset_tablegroup(
            schema_guard, orig_table_schema, new_table_schema, alter_primary_zone))) {
      LOG_WARN("fail to reset tablegroup", K(ret));
    }
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(new_table_schema.get_tenant_id(), compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else {
    share::CompatModeGuard g(compat_mode);
    // Handling the source is a tablegroup created after 2.0
    if (OB_FAIL(process_tablegroup_option_for_alter_table(schema_guard, orig_table_schema, new_table_schema))) {
      LOG_WARN("fail to check primary_zone and locality", K(ret));
    } else if (OB_FAIL(check_partition_option_for_create_table(schema_guard, new_table_schema))) {
      LOG_WARN("fail to check tablegroup partition", K(ret), K(new_table_schema));
    }
  }
  if (OB_SUCC(ret)) {
    bool is_orig_tg_binding = false;
    bool is_new_tg_binding = true;
    const share::schema::ObTablegroupSchema* orig_tg = nullptr;
    const share::schema::ObTablegroupSchema* new_tg = nullptr;
    const int64_t orig_tg_id = orig_table_schema.get_tablegroup_id();
    const int64_t new_tg_id = new_table_schema.get_tablegroup_id();
    if (OB_INVALID_ID == orig_tg_id) {  // Did not belong to any tablegroup
      is_orig_tg_binding = false;
    } else if (OB_FAIL(schema_guard.get_tablegroup_schema(orig_tg_id, orig_tg))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(orig_tg_id));
    } else if (OB_UNLIKELY(nullptr == orig_tg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig tg ptr is null", K(ret), K(orig_tg_id));
    } else if (orig_tg->get_binding()) {
      is_orig_tg_binding = true;
    } else {
      is_orig_tg_binding = false;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_INVALID_ID == new_tg_id) {  // Did not belong to any tablegroup
      is_new_tg_binding = false;
    } else if (OB_FAIL(schema_guard.get_tablegroup_schema(new_tg_id, new_tg))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(new_tg_id));
    } else if (OB_UNLIKELY(nullptr == new_tg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("orig tg ptr is null", K(ret), K(new_tg_id));
    } else if (new_tg->get_binding()) {
      is_new_tg_binding = true;
    } else {
      is_new_tg_binding = false;
    }
    if (OB_SUCC(ret)) {
      if (is_orig_tg_binding || is_new_tg_binding) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("can not move table into or out of a binding tablegroup", K(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "move table into or out of a binding tablegroup");
      }
    }
    if (OB_SUCC(ret)) {
      if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != new_table_schema.get_duplicate_scope() &&
          OB_INVALID_ID != new_table_schema.get_tablegroup_id()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("duplicate table in tablegroup is not supported",
            K(ret),
            "table_id",
            new_table_schema.get_table_id(),
            "tablegroup_id",
            new_table_schema.get_tablegroup_id());
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "duplicate table in tablegroup");
      }
    }
  }
  return ret;
}

// Before calling this function, ensure that there isn't changing locality in progress
int ObTableGroupHelp::set_table_options_for_reset_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTableSchema& orig_table_schema, share::schema::ObTableSchema& new_table_schema,
    bool alter_primary_zone)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = orig_table_schema.get_tablegroup_id();
  if (OB_INVALID_ID != new_table_schema.get_tablegroup_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret));
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    // skip
  } else {
    /**
     * Set table's locality according the following rules when move table out from the tablegroup
     * created after 2.0
     * 1) If there is table's locality, let it be
     * 2) If table's locality is inherited from tablegroup, and there is tablegroup's locality.
     *    Table's locality needs to be set to be the same as tablegroup's
     * 3) if table's locality is inherited from tenant's locality (or is inherited from tablegroup,
     *    and tablegroup is inherited from tenant's locality), no need to set table's locality
     */
    const ObTablegroupSchema* tablegroup_schema = NULL;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
    } else if (OB_ISNULL(tablegroup_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid tablegroup schema", K(ret), K(tablegroup_id));
    } else if (!tablegroup_schema->get_locality_str().empty() && new_table_schema.get_locality_str().empty()) {
      ObArray<ObZone> zone_list;
      common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
      if (OB_FAIL(tablegroup_schema->get_zone_list(schema_guard, zone_list))) {
        LOG_WARN("fail to get zone list", K(ret));
      } else if (OB_FAIL(tablegroup_schema->get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
        LOG_WARN("fail to get zone replica attr array", K(ret));
      } else if (OB_FAIL(new_table_schema.set_zone_list(zone_list))) {
        LOG_WARN("fail to set zone list", K(ret));
      } else if (OB_FAIL(new_table_schema.set_zone_replica_attr_array(zone_locality))) {
        LOG_WARN("fail to set zone replica num array", K(ret));
      } else if (OB_FAIL(new_table_schema.set_locality(tablegroup_schema->get_locality_str()))) {
        LOG_WARN("fail to set locality", K(ret));
      }
    }
    /**
     * Set table's primary_zone according the following rules
     * 1) If there is tenant's primary_zone, use this value
     * 2) If there is table's primary_zone, let it be
     * 3) If table's primary_zone is inherited from tablegroup, and there is tablegroup's
     *    primary_zone. Table's primary_zone needs to be set to be the same as tablegroup's
     * 4) if table's primary_zone is inherited from database, let it be
     *    (assume that tablegroup cannot migrate across database)
     */
    if (OB_FAIL(ret)) {
      // skip
    } else if (alter_primary_zone) {
      // skip
    } else if (new_table_schema.get_primary_zone().empty()) {
      if (is_new_tablegroup_id(tablegroup_id)) {
        if (!tablegroup_schema->get_primary_zone().empty()) {
          const common::ObIArray<ObZoneScore>& primary_zone_array = tablegroup_schema->get_primary_zone_array();
          if (OB_FAIL(new_table_schema.set_primary_zone(tablegroup_schema->get_primary_zone()))) {
            LOG_WARN("fail to set primary_zone", K(ret));
          } else if (OB_FAIL(new_table_schema.set_primary_zone_array(primary_zone_array))) {
            LOG_WARN("fail to set primary_zone_array", K(ret));
          }
        }
      }
    }
    // check and format primary_zone
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ddl_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl_service is null", K(ret));
      } else if (OB_FAIL(ddl_service_->check_create_table_replica_options(new_table_schema, schema_guard))) {
        LOG_WARN("fail to check create table replica options", K(ret));
      }
    }
  }
  return ret;
}

// Check whether table and tablegroup are in the process of changing locality according to the
// inheritance relationship
int ObTableGroupHelp::check_locality_in_modification(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTableSchema& orig_table_schema, const uint64_t tablegroup_id)
{
  int ret = OB_SUCCESS;
  bool in_locality_modification = true;
  if (OB_FAIL(orig_table_schema.check_in_locality_modification(schema_guard, in_locality_modification))) {
    LOG_WARN("fail to check in locality modification", K(ret));
  } else if (in_locality_modification) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("modify tablegroup when locality is changing is not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "modify tablegroup when locality is changing");
  } else if (is_new_tablegroup_id(tablegroup_id)) {
    const ObTablegroupSchema* tablegroup_schema = NULL;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
    } else if (OB_ISNULL(tablegroup_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid tablegroup schema", K(ret), K(tablegroup_id));
    } else if (OB_FAIL(tablegroup_schema->check_in_locality_modification(schema_guard, in_locality_modification))) {
      LOG_WARN("fail to get check in locality modification", K(ret));
    } else if (in_locality_modification) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("modify tablegroup when locality is changing is not allowed", K(ret));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "modify tablegroup when locality is changing");
    }
  } else {
    const ObTenantSchema* tenant_schema = NULL;
    const uint64_t tenant_id = orig_table_schema.get_tenant_id();
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid tenant schema", K(ret));
    } else if (!tenant_schema->get_previous_locality_str().empty()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("modify tablegroup when locality is changing is not allowed",
          K(ret),
          "previous_locality",
          tenant_schema->get_previous_locality_str());
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "modify tablegroup when locality is changing");
    }
  }
  return ret;
}

// When creating a table, need to check whether the partition type of tablegroup and table are consistent
int ObTableGroupHelp::check_partition_option_for_create_table(ObSchemaGetterGuard& schema_guard, ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = table.get_tablegroup_id();
  const ObTablegroupSchema* tablegroup = NULL;
  if (!is_new_tablegroup_id(tablegroup_id)) {
    // For tablegroups created before 2.0, do not check partition
    LOG_INFO("skip to check tablegroup partition", KPC(tablegroup), K(table));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), KT(tablegroup_id));
  } else if (OB_ISNULL(tablegroup)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablegroup schema is null", K(ret), KT(tablegroup_id));
  } else if (table.is_in_splitting() || tablegroup->is_in_splitting()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table or tablegroup is splitting", K(ret), K(table), K(tablegroup));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "add table to tablegroup while either object is splitting");
  } else if (OB_FAIL(check_partition_option(*tablegroup, table))) {
    LOG_WARN("fail to check partition option", K(ret), KPC(tablegroup), K(table));
  }
  return ret;
}

/**
 * If partition data distribution methods are same, in the following scenarios,
 * the partition types of table and tablegroup are considered to be the same:
 * 1.For hash partition, the number of partitions must be the same
 * 2.For key partition, the number of partitions must be the same and the column_list_num must match
 * 3.For range/range column or list/list column partition,
 *   the number of partition expressions partitions/partition split point are required to be same
 */
int ObTableGroupHelp::check_partition_option(const ObTablegroupSchema& tablegroup, ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  bool is_matched = false;
  const uint64_t tablegroup_id = tablegroup.get_tablegroup_id();
  const uint64_t table_id = table.get_table_id();
  if (!table.is_sub_part_template()) {
    is_matched = false;
    LOG_WARN("nontemplate table cannot be in tablegroup", K(tablegroup), K(table));
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    // For tablegroups created before 2.0, do not check partition
    is_matched = true;
    LOG_WARN("skip to check tablegroup partition", K(tablegroup), K(table));
  } else if (tablegroup.get_part_level() != table.get_part_level()) {
    LOG_WARN("part level not matched",
        K(ret),
        KT(tablegroup_id),
        KT(table_id),
        "tg_part_level",
        tablegroup.get_part_level(),
        "table_part_level",
        table.get_part_level());
  } else if (PARTITION_LEVEL_ZERO == tablegroup.get_part_level()) {
    // Non-partitioned table
    is_matched = true;
    LOG_INFO("tablegroup & table has no partitions, just pass", K(ret));
  } else {
    if (PARTITION_LEVEL_ONE == table.get_part_level() || PARTITION_LEVEL_TWO == table.get_part_level()) {
      const bool is_subpart = false;
      if (OB_FAIL(check_partition_option(tablegroup, table, is_subpart, is_matched))) {
        LOG_WARN("level one partition not matched", K(ret), KT(tablegroup_id), KT(table_id));
      } else if (!is_matched) {
        // bypass
      } else if (!tablegroup.get_binding()) {
        // bypass
      } else if (OB_FAIL(set_mapping_pg_part_id(tablegroup, table))) {
        LOG_WARN("fail to set mapping pg part id", K(ret), KT(tablegroup_id), KT(table_id));
      }
    }
    if (OB_SUCC(ret) && is_matched && PARTITION_LEVEL_TWO == table.get_part_level()) {
      const bool is_subpart = true;
      if (OB_FAIL(check_partition_option(tablegroup, table, is_subpart, is_matched))) {
        LOG_WARN("level two partition not matched", K(ret), KT(tablegroup_id), KT(table_id));
      } else if (!is_matched) {
        // bypass
      } else if (!tablegroup.get_binding()) {
        // bypass
      } else if (OB_FAIL(set_mapping_pg_sub_part_id(tablegroup, table))) {
        LOG_WARN("fail to set mapping pg subpart id", K(ret), KT(tablegroup_id), KT(table_id));
      }
    }
  }
  if (OB_SUCC(ret) && !is_matched) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("partition option not match", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "table and tablegroup use different partition options");
  }
  return ret;
}

// FIXME:Support non-template subpartition
int ObTableGroupHelp::check_partition_option(
    const ObTablegroupSchema& tablegroup, ObTableSchema& table, bool is_subpart, bool& is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = false;
  const uint64_t tablegroup_id = tablegroup.get_tablegroup_id();
  const uint64_t table_id = table.get_table_id();
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
  if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(table.get_tenant_id(), compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else {
    share::CompatModeGuard g(compat_mode);
    if (!is_new_tablegroup_id(tablegroup_id)) {
      // For tablegroups created before 2.0, do not check partition
      is_matched = true;
      LOG_WARN("skip to check tablegroup partition", K(tablegroup), K(table));
    } else {
      const ObPartitionOption& tablegroup_part =
          !is_subpart ? tablegroup.get_part_option() : tablegroup.get_sub_part_option();
      const ObPartitionOption& table_part = !is_subpart ? table.get_part_option() : table.get_sub_part_option();
      ObPartitionFuncType tg_part_func_type = tablegroup_part.get_part_func_type();
      ObPartitionFuncType table_part_func_type = table_part.get_part_func_type();
      if (tg_part_func_type != table_part_func_type &&
          (!is_key_part(tg_part_func_type) || !is_key_part(table_part_func_type))) {
        // skip
        LOG_WARN("partition func type not matched", KT(tablegroup_id), KT(table_id), K(tablegroup_part), K(table_part));
      } else if (PARTITION_FUNC_TYPE_MAX == tg_part_func_type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid part_func_type", K(ret), K(tg_part_func_type), K(table_part_func_type));
      } else if (is_hash_like_part(tg_part_func_type)) {
        is_matched = tablegroup_part.get_part_num() == table_part.get_part_num();
        if (!is_matched) {
          LOG_WARN("partition num not matched", KT(tablegroup_id), KT(table_id), K(tablegroup_part), K(table_part));
        } else if (is_key_part(tg_part_func_type)) {
          // For key partition, needs compared column_list_num
          is_matched = false;
          int64_t tablegroup_expr_num = OB_INVALID_INDEX;
          int64_t table_expr_num = OB_INVALID_INDEX;
          if (!is_subpart) {
            if (OB_FAIL(tablegroup.calc_part_func_expr_num(tablegroup_expr_num))) {
              LOG_WARN("fail to get part_func_expr_num", K(ret), K(tablegroup));
            } else if (OB_FAIL(table.calc_part_func_expr_num(table_expr_num))) {
              LOG_WARN("fail to get part_func_expr_num", K(ret), K(table));
            }
          } else {
            if (OB_FAIL(tablegroup.calc_subpart_func_expr_num(tablegroup_expr_num))) {
              LOG_WARN("fail to get subpart_func_expr_num", K(ret), K(tablegroup));
            } else if (OB_FAIL(table.calc_subpart_func_expr_num(table_expr_num))) {
              LOG_WARN("fail to get subpart_func_expr_num", K(ret), K(table));
            }
          }
          if (OB_FAIL(ret)) {
            // skip
          } else if (OB_INVALID_INDEX == tablegroup_expr_num || OB_INVALID_INDEX == table_expr_num) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr num is invalid", K(ret), K(tablegroup_expr_num), K(table_expr_num));
          } else {
            is_matched = (tablegroup_expr_num == table_expr_num);
          }
        }
      } else if (is_range_part(tg_part_func_type) || is_list_part(tg_part_func_type)) {
        const int64_t tg_part_num = tablegroup_part.get_part_num();
        const int64_t table_part_num = table_part.get_part_num();
        if (tg_part_num != table_part_num) {
          LOG_WARN("range partition not matched",
              KT(tablegroup_id),
              KT(table_id),
              K(tablegroup_part),
              K(table_part),
              K(tg_part_num),
              K(table_part_num));
        } else if (!is_subpart) {
          if (OB_ISNULL(tablegroup.get_part_array()) || OB_ISNULL(table.get_part_array())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition_array is null", K(ret), K(tablegroup), K(table));
          } else {
            is_matched = true;
            for (int i = 0; i < tg_part_num && is_matched && OB_SUCC(ret); i++) {
              is_matched = false;
              ObPartition* tg_part = tablegroup.get_part_array()[i];
              for (int j = 0; j < table_part_num && !is_matched && OB_SUCC(ret); j++) {
                ObPartition* table_part = table.get_part_array()[j];
                if (OB_ISNULL(tg_part) || OB_ISNULL(table_part)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("partition is null", K(ret), KPC(tg_part), KPC(table_part));
                } else if (OB_FAIL(ObPartitionUtils::check_partition_value(
                               *tg_part, *table_part, tg_part_func_type, is_matched))) {
                  LOG_WARN("fail to check partition value", KPC(tg_part), KPC(table_part), K(tg_part_func_type));
                }
              }
            }
          }
          if (!is_matched) {
            LOG_WARN("range partition not matched",
                K(ret),
                KT(tablegroup_id),
                KT(table_id),
                "tg_partition_array",
                ObArrayWrap<ObPartition*>(tablegroup.get_part_array(), tg_part_num),
                "table_part_array",
                ObArrayWrap<ObPartition*>(table.get_part_array(), table_part_num));
          }
        } else {
          if (OB_ISNULL(tablegroup.get_def_subpart_array()) || OB_ISNULL(table.get_def_subpart_array())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("def_sub_partition_array is null", K(ret), K(tablegroup), K(table));
          } else {
            is_matched = true;
            for (int i = 0; i < tg_part_num && is_matched && OB_SUCC(ret); i++) {
              is_matched = false;
              ObSubPartition* tg_part = tablegroup.get_def_subpart_array()[i];
              for (int j = 0; j < table_part_num && !is_matched && OB_SUCC(ret); j++) {
                ObSubPartition* table_part = table.get_def_subpart_array()[j];
                if (OB_ISNULL(tg_part) || OB_ISNULL(table_part)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("partition is null", K(ret), KPC(tg_part), KPC(table_part));
                } else if (OB_FAIL(ObPartitionUtils::check_partition_value(
                               *tg_part, *table_part, tg_part_func_type, is_matched))) {
                  LOG_WARN("fail to check partition value", KPC(tg_part), KPC(table_part), K(tg_part_func_type));
                }
              }
            }
          }
          if (!is_matched) {
            LOG_WARN("range partition not matched",
                K(ret),
                KT(tablegroup_id),
                KT(table_id),
                "tg_partition_array",
                ObArrayWrap<ObSubPartition*>(tablegroup.get_def_subpart_array(), tg_part_num),
                "table_part_array",
                ObArrayWrap<ObSubPartition*>(table.get_def_subpart_array(), table_part_num));
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid part func type", KT(tablegroup_id), KT(table_id), K(tablegroup_part), K(table_part));
      }
    }
  }
  return ret;
}

int ObTableGroupHelp::set_mapping_pg_part_id(
    const share::schema::ObTablegroupSchema& tablegroup, share::schema::ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  if (!tablegroup.get_binding()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not binding tablegroup", K(ret), "tg_id", tablegroup.get_binding());
  } else if (PARTITION_LEVEL_ONE == table.get_part_level() || PARTITION_LEVEL_TWO == table.get_part_level()) {
    if (tablegroup.get_partition_num() != table.get_partition_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part num not match",
          K(ret),
          "tg_part_num",
          tablegroup.get_partition_num(),
          "tg_id",
          tablegroup.get_tablegroup_id(),
          "table_part_num",
          table.get_partition_num(),
          "table_id",
          table.get_table_id());
    } else if (OB_UNLIKELY(nullptr == tablegroup.get_part_array()) || OB_UNLIKELY(nullptr == table.get_part_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_array ptr is null", "tg_id", tablegroup.get_tablegroup_id(), "table_id", table.get_table_id());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tablegroup.get_partition_num(); ++i) {
        ObPartition* table_partition = table.get_part_array()[i];
        const ObPartition* tg_partition = tablegroup.get_part_array()[i];
        if (OB_UNLIKELY(nullptr == table_partition || nullptr == tg_partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table partition ptr or tg partition is null", K(ret));
        } else {
          table_partition->set_mapping_pg_part_id(tg_partition->get_part_id());
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "table part level unexpected", K(ret), "table_id", table.get_table_id(), "part_level", table.get_part_level());
  }
  return ret;
}

// FIXME:Support non-template subpartition
int ObTableGroupHelp::set_mapping_pg_sub_part_id(
    const share::schema::ObTablegroupSchema& tablegroup, share::schema::ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  if (!tablegroup.get_binding()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not binding tablegroup", K(ret), "tg_id", tablegroup.get_binding());
  } else if (PARTITION_LEVEL_TWO == table.get_part_level()) {
    if (tablegroup.get_def_subpartition_num() != table.get_def_subpartition_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part num not match",
          K(ret),
          "tg_def_subpart_num",
          tablegroup.get_def_subpartition_num(),
          "tg_id",
          tablegroup.get_tablegroup_id(),
          "table_def_subpart_num",
          table.get_def_subpartition_num(),
          "table_id",
          table.get_table_id());
    } else if (OB_UNLIKELY(nullptr == tablegroup.get_def_subpart_array()) ||
               OB_UNLIKELY(nullptr == table.get_def_subpart_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_array ptr is null", "tg_id", tablegroup.get_tablegroup_id(), "table_id", table.get_table_id());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tablegroup.get_def_subpartition_num(); ++i) {
        ObSubPartition* table_partition = table.get_def_subpart_array()[i];
        const ObSubPartition* tg_partition = tablegroup.get_def_subpart_array()[i];
        if (OB_UNLIKELY(nullptr == table_partition || nullptr == tg_partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table partition ptr or tg partition is null", K(ret));
        } else {
          table_partition->set_mapping_pg_sub_part_id(tg_partition->get_sub_part_id());
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "table part level unexpected", K(ret), "table_id", table.get_table_id(), "part_level", table.get_part_level());
  }
  return ret;
}

/**
 * Check whether adding or dropping partition is legal, currently only adding or dropping the
 * first-level range/range columns partition is allowed
 * For drop partition:
 * 1.part_name need exist(When drop a partition which has same name partition,
 *                        it is intercepted by resolver)
 * For add partition:
 * 1.part_name needs to not exist
 * 2.Ensure that high_bound_val increases monotonically
 * For modify partition:
 * The partition expressions of all tables are required to exist
 */
int ObTableGroupHelp::check_alter_partition(const ObPartitionSchema*& orig_part_schema,
    ObPartitionSchema*& alter_part_schema, const ObAlterTablegroupArg::ModifiableOptions alter_part_type,
    int64_t expr_num, bool is_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_part_schema) || OB_ISNULL(alter_part_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("orig_part_schema or alter_part_schema is null", KP(orig_part_schema), KP(alter_part_schema), K(ret));
  } else if (ObAlterTablegroupArg::MAX_OPTION == alter_part_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid alter_part_type", K(ret), K(alter_part_type));
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("in upgrade, can not do partition maintenance", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "partition maintenance during upgrade");
  } else if (ObAlterTablegroupArg::DROP_PARTITION == alter_part_type) {
    if (OB_FAIL(check_drop_partition(orig_part_schema, alter_part_schema, is_tablegroup))) {
      LOG_WARN("failed to check drop partition", K(ret));
    }
  } else {
    // Except drop partition, may lack for the definition of partition name in Oracle mode
    ObTablegroupSchema* tablegroup_schema = NULL;
    AlterTableSchema* alter_table_schema = NULL;
    if (is_tablegroup) {
      tablegroup_schema = static_cast<ObTablegroupSchema*>(alter_part_schema);
      if (OB_ISNULL(tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed get tablegroup schema", K(ret), K(tablegroup_schema));
      } else if (tablegroup_schema->is_range_part() || tablegroup_schema->is_list_part()) {
        if (OB_FAIL(ddl_service_->complete_split_partition(
                *orig_part_schema, *tablegroup_schema, ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type))) {
          LOG_WARN("failed to complete partition", K(ret));
        }
      }
    } else {
      // For split partition, if the split partition name of the table is not filled,
      // then do not fill the last partition range, only fill the partition name
      alter_table_schema = static_cast<AlterTableSchema*>(alter_part_schema);
      bool is_split = false;
      if (OB_ISNULL(alter_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed get alter table schema", K(ret), KP(alter_part_schema));
      } else if (alter_table_schema->is_range_part() || alter_table_schema->is_list_part()) {
        if (OB_FAIL(ddl_service_->complete_split_partition(*orig_part_schema, *alter_table_schema, is_split))) {
          LOG_WARN("failed to complete partition", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type) {
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("converting non-partitioned table to partition table before version 2.1 not supported",
          K(ret),
          K(GET_MIN_CLUSTER_VERSION()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "converting non-partitioned table to partition table before version 2.1");
    } else if (OB_FAIL(ddl_service_->check_split_partition_can_execute())) {
      LOG_WARN("failed to check split partition can execute", K(ret));
    }
  } else if (!orig_part_schema->is_range_part() && !orig_part_schema->is_list_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can only be used on RANGE/RANGE COLUMNS, list/list columns partitions", K(ret));
    LOG_USER_ERROR(
        OB_NOT_SUPPORTED, "current partition maintenance operation for non {range/range, list/list} partitions");
  } else if (expr_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr num is invalid", K(ret), K(expr_num));
  } else if (ObAlterTablegroupArg::ADD_PARTITION == alter_part_type) {
    if (OB_FAIL(check_add_partition(orig_part_schema, alter_part_schema, expr_num, is_tablegroup))) {
      LOG_WARN("failed to check add partition", K(ret));
    }
  } else if (ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type ||
             ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type) {
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Split partition before version 2.1 not supportted", K(ret), K(GET_MIN_CLUSTER_VERSION()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "split partition before version 2.1");
    } else if (OB_FAIL(check_split_partition(
                   orig_part_schema, alter_part_schema, alter_part_type, expr_num, is_tablegroup))) {
      LOG_WARN("failed to check split partition", K(ret));
    }
  } else if (ObAlterTablegroupArg::DROP_PARTITION != alter_part_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected alter partition type", K(ret), K(alter_part_type));
  }
  return ret;
}

int ObTableGroupHelp::check_drop_partition(const share::schema::ObPartitionSchema*& orig_part_schema,
    const share::schema::ObPartitionSchema* alter_part_schema, bool is_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_part_schema) || OB_ISNULL(alter_part_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("orig_part_schema or alter_part_schema is null", K(ret), KP(orig_part_schema), KP(alter_part_schema));
  } else {
    if (alter_part_schema->get_part_option().get_part_num() >= orig_part_schema->get_part_option().get_part_num()) {
      ret = OB_ERR_DROP_LAST_PARTITION;
      LOG_WARN("cannot drop all partitions",
          K(ret),
          "partitions current",
          orig_part_schema->get_part_option().get_part_num(),
          "partitions to be dropped",
          alter_part_schema->get_part_option().get_part_num());
      LOG_USER_ERROR(OB_ERR_DROP_LAST_PARTITION);
    } else {
      const int64_t part_num = alter_part_schema->get_part_option().get_part_num();
      ObPartition** part_array = alter_part_schema->get_part_array();
      const int64_t orig_part_num = orig_part_schema->get_part_option().get_part_num();
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        ObPartition* alter_part = part_array[i];
        bool found = false;
        for (int64_t j = 0; j < orig_part_num && OB_SUCC(ret) && !found; j++) {
          const ObPartition* part = orig_part_schema->get_part_array()[j];
          if (OB_ISNULL(part) || OB_ISNULL(alter_part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret), KP(part), KP(alter_part));
          } else if (is_tablegroup) {
            if (ObCharset::case_insensitive_equal(part->get_part_name(), alter_part->get_part_name())) {
              if (OB_FAIL(alter_part->assign(*part))) {
                LOG_WARN("partition assign failed", K(ret), K(part), K(alter_part));
              }
              found = true;
            }
          } else if (OB_FAIL(ObDDLOperator::check_part_equal(
                         orig_part_schema->get_part_option().get_part_func_type(), part, alter_part, found))) {
            LOG_WARN("check_part_equal failed", K(ret));
          } else if (found) {
            if (OB_FAIL(alter_part->assign(*part))) {
              LOG_WARN("partition assign failed", K(ret), K(part), K(alter_part));
            }
          }
          // TODO: Not allowed to drop a object which is splitting
          if (OB_SUCC(ret) && found) {
            if (!part->allow_ddl_operator()) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("in spliting can not", K(ret), K(part));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "drop a partition while it is splitting");
            }
          }
        }  // end for
        if (OB_SUCC(ret)) {
          if (!found) {
            ret = OB_ERR_DROP_PARTITION_NON_EXISTENT;
            LOG_WARN("partition to be dropped not exist", K(ret), "partition name", part_array[i]->get_part_name());
            LOG_USER_ERROR(OB_ERR_DROP_PARTITION_NON_EXISTENT, "DROP");
          }
        }
      }
    }
  }
  return ret;
}

int ObTableGroupHelp::check_partarray_expr_name_valid(const share::schema::ObPartitionSchema*& orig_part_schema,
    const share::schema::ObPartitionSchema* alter_part_schema, int64_t expr_num, const ObString* split_part_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_part_schema) || OB_ISNULL(alter_part_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("orig_part_schema or alter_part_schema is null", K(ret), KP(orig_part_schema), KP(alter_part_schema));
  } else {
    const int64_t part_num = alter_part_schema->get_part_option().get_part_num();
    ObPartition** part_array = alter_part_schema->get_part_array();
    ObPartition** orig_part_array = orig_part_schema->get_part_array();
    const int64_t orig_part_num = orig_part_schema->get_part_option().get_part_num();
    const ObPartitionFuncType part_func_type = orig_part_schema->get_part_option().get_part_func_type();
    if (OB_ISNULL(part_array) || OB_ISNULL(orig_part_array) || orig_part_num < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), KP(part_array), KP(orig_part_array), K(orig_part_num));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
      ObPartition* alter_part = part_array[i];
      bool found = false;
      // Check whether the type and number of partition value meet the requirements
      const ObPartition* orig_part = orig_part_array[0];
      if (OB_ISNULL(orig_part) || OB_ISNULL(alter_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), KP(orig_part), KP(alter_part));
      } else if (OB_FAIL(check_part_expr_num_and_value_type(alter_part, orig_part, part_func_type, expr_num))) {
        LOG_WARN("fail to check part", K(ret), K(part_func_type), K(expr_num), K(alter_part), K(orig_part));
      }
      // Check partition name for naming conflict
      for (int64_t j = 0; j < orig_part_num && OB_SUCC(ret) && !found; j++) {
        const ObPartition* part = orig_part_array[j];
        if (OB_ISNULL(part) || OB_ISNULL(alter_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), KP(part), KP(alter_part));
        } else if (ObCharset::case_insensitive_equal(part->get_part_name(), alter_part->get_part_name())) {
          if (OB_NOT_NULL(split_part_name) &&
              ObCharset::case_insensitive_equal(part->get_part_name(), *split_part_name)) {
            found = false;
          } else {
            found = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (found) {
          ret = OB_ERR_SAME_NAME_PARTITION;
          LOG_WARN("duplicate partition name", K(part_array[i]->get_part_name()), K(ret));
          LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION,
              part_array[i]->get_part_name().length(),
              part_array[i]->get_part_name().ptr());
        }
      }
    }
  }
  return ret;
}

int ObTableGroupHelp::check_add_partition(const share::schema::ObPartitionSchema*& orig_part_schema,
    share::schema::ObPartitionSchema*& alter_part_schema, int64_t expr_num, bool is_tablegroup)
{
  int ret = OB_SUCCESS;
  UNUSED(is_tablegroup);
  if (OB_ISNULL(orig_part_schema) || OB_ISNULL(alter_part_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("orig_part_schema or alter_part_schema is null", K(ret), KP(orig_part_schema), KP(alter_part_schema));
  } else {
    bool is_oracle_mode = false;
    const int64_t part_num = alter_part_schema->get_part_option().get_part_num();
    if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
            orig_part_schema->get_tenant_id(), is_oracle_mode))) {
      LOG_WARN("fail to check is oracle mode", K(ret));
    } else if ((is_oracle_mode && orig_part_schema->get_all_part_num() + part_num > OB_MAX_PARTITION_NUM_ORACLE) ||
               (!is_oracle_mode && orig_part_schema->get_all_part_num() + part_num > OB_MAX_PARTITION_NUM_MYSQL)) {
      ret = OB_TOO_MANY_PARTITIONS_ERROR;
      LOG_WARN("too partitions",
          K(ret),
          "partition cnt current",
          orig_part_schema->get_all_part_num(),
          "partition cnt to be added",
          part_num);
    } else if (OB_FAIL(check_partarray_expr_name_valid(orig_part_schema, alter_part_schema, expr_num))) {
      LOG_WARN("failed to check new partition expr and name", K(ret));
    } else if (orig_part_schema->is_range_part()) {
      const ObRowkey* rowkey_last =
          &orig_part_schema->get_part_array()[orig_part_schema->get_part_option().get_part_num() - 1]
               ->get_high_bound_val();
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        const ObRowkey* rowkey_cur = &alter_part_schema->get_part_array()[i]->get_high_bound_val();
        if (*rowkey_cur <= *rowkey_last) {
          ret = OB_ERR_RANGE_NOT_INCREASING_ERROR;
          LOG_WARN("range values should increasing", K(ret), K(rowkey_cur), K(rowkey_last));
          const ObString& err_msg =
              orig_part_schema->get_part_array()[orig_part_schema->get_part_option().get_part_num() - 1]
                  ->get_part_name();
          LOG_USER_ERROR(
              OB_ERR_RANGE_NOT_INCREASING_ERROR, lib::is_oracle_mode() ? err_msg.length() : 0, err_msg.ptr());
        } else {
          rowkey_last = rowkey_cur;
        }
      }
    } else if (orig_part_schema->is_list_part()) {
      if (OB_FAIL(ddl_service_->check_add_list_partition(*orig_part_schema, *alter_part_schema))) {
        LOG_WARN("failed to check add list partition", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected alter partition type",
          K(ret),
          "part type",
          orig_part_schema->get_part_option().get_part_func_type());
    }
  }
  return ret;
}
/**
 * split partition check:
 * For table, need to first determine the split partition, use the split high value of the tablegroup,
 * and then fill part_name according the corresponding part_name
 * For split syntax, first complete the split information
 * Then check that partition exists, the range is the same, and the partition increase monotonicity
 */
int ObTableGroupHelp::check_split_partition(const share::schema::ObPartitionSchema*& orig_part_schema,
    share::schema::ObPartitionSchema*& alter_part_schema,
    const obrpc::ObAlterTablegroupArg::ModifiableOptions alter_part_type, int64_t expr_num, bool is_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_part_schema) || OB_ISNULL(alter_part_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("orig_part_schema or alter_part_schema is null", K(ret), KP(orig_part_schema), KP(alter_part_schema));
  } else if (OB_FAIL(ddl_service_->check_split_partition_can_execute())) {
    LOG_WARN("failed to check split partition can execute", K(ret));
  } else {
    const int64_t part_num = alter_part_schema->get_part_option().get_part_num();
    ObPartition** part_array = alter_part_schema->get_part_array();
    bool is_oracle_mode = false;
    if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
            orig_part_schema->get_tenant_id(), is_oracle_mode))) {
      LOG_WARN("fail to check is oracle mode", K(ret));
    } else if (OB_ISNULL(part_array)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part array is null", K(ret), KP(part_array));
    } else if ((is_oracle_mode && orig_part_schema->get_all_part_num() + part_num - 1 > OB_MAX_PARTITION_NUM_ORACLE) ||
               (!is_oracle_mode && orig_part_schema->get_all_part_num() + part_num - 1 > OB_MAX_PARTITION_NUM_MYSQL)) {
      ret = OB_TOO_MANY_PARTITIONS_ERROR;
      LOG_WARN("too partitions",
          K(ret),
          "partition cnt current",
          orig_part_schema->get_all_part_num(),
          "partition cnt to be added",
          part_num);
    }

    int64_t index = 0;  // Mark which partition is to be split
    const ObString* split_part_name = NULL;
    if (OB_SUCC(ret)) {
      int64_t org_part_num = orig_part_schema->get_partition_num();
      ObTablegroupSchema* tablegroup_schema = NULL;
      AlterTableSchema* alter_table_schema = NULL;
      if (is_tablegroup) {
        tablegroup_schema = static_cast<ObTablegroupSchema*>(alter_part_schema);
        if (OB_ISNULL(tablegroup_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed get tablegroup schema", K(ret), K(tablegroup_schema));
        }
      } else {
        alter_table_schema = static_cast<AlterTableSchema*>(alter_part_schema);
        if (OB_ISNULL(alter_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed get alter table schema", K(ret), KP(alter_part_schema));
        }
      }
      // Need to obtain the partition name of the source split partition, the partition name of the
      // new partitions can be the same as the partition name of the source split partition
      for (; OB_SUCC(ret) && index < org_part_num; ++index) {
        ObPartition* org_part = orig_part_schema->get_part_array()[index];
        if (OB_ISNULL(org_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", K(ret), K(org_part), K(index));
        } else if (is_tablegroup) {
          if (ObCharset::case_insensitive_equal(
                  tablegroup_schema->get_split_partition_name(), org_part->get_part_name())) {
            split_part_name = &(tablegroup_schema->get_split_partition_name());
            if (orig_part_schema->is_range_part()) {
              if (OB_FAIL(tablegroup_schema->set_split_rowkey(org_part->get_high_bound_val()))) {
                LOG_WARN("failed set split row key", K(ret), K(org_part));
              }
            } else if (orig_part_schema->is_list_part()) {
              // use the first rowkey of tablegroup partition list value for comparison
              if (0 == org_part->get_list_row_values().count()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("list row values id empty", K(ret), K(org_part));
              } else {
                common::ObRowkey rowkey;
                rowkey.assign(
                    org_part->get_list_row_values().at(0).cells_, org_part->get_list_row_values().at(0).count_);
                if (OB_FAIL(tablegroup_schema->set_split_list_value(rowkey))) {
                  LOG_WARN("failed to set split list row value", K(ret), K(org_part));
                }
              }
            }
            break;
          }
        } else if (alter_table_schema->is_range_part()) {
          // For the table, use high_bound_value to mark the split partition
          if (org_part->get_high_bound_val() == alter_table_schema->get_split_high_bound_value()) {
            alter_table_schema->set_split_partition_name(org_part->get_part_name());
            split_part_name = &(alter_table_schema->get_split_partition_name());
            break;
          }
        } else if (alter_table_schema->is_list_part()) {
          if (ObDDLOperator::is_list_values_equal(
                  alter_table_schema->get_split_list_row_values(), org_part->get_list_row_values())) {
            alter_table_schema->set_split_partition_name(org_part->get_part_name());
            split_part_name = &(alter_table_schema->get_split_partition_name());
            break;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (index >= org_part_num) {
        ret = OB_ERR_DROP_PARTITION_NON_EXISTENT;
        LOG_WARN("partition to be split not exist", K(ret), "alter_part_schema", *alter_part_schema);
        LOG_USER_ERROR(OB_ERR_DROP_PARTITION_NON_EXISTENT, "SPLIT");
      } else if (!is_tablegroup && ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type) {
        // After filling the split partition name for the table, need to fill the last value that
        // may not be filled
        bool is_split = true;
        if (OB_FAIL(ddl_service_->complete_split_partition(*orig_part_schema, *alter_table_schema, is_split))) {
          LOG_WARN("failed to complete partition", K(ret));
        }
      }
    }
    // Check whether the partition name conflicts, need to pass in the split partition name
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
                   check_partarray_expr_name_valid(orig_part_schema, alter_part_schema, expr_num, split_part_name))) {
      LOG_WARN("failed to check new partition expr and name", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (orig_part_schema->is_range_part()) {
      // Check the first partition created after partition split, the range needs to increase
      // in the same series with previous partition
      if (OB_SUCC(ret) && 0 != index) {
        const ObRowkey* rowkey_left = &orig_part_schema->get_part_array()[index - 1]->get_high_bound_val();
        const ObRowkey* rowkey_beg = &part_array[0]->get_high_bound_val();
        if (*rowkey_beg <= *rowkey_left) {
          ret = OB_ERR_RANGE_NOT_INCREASING_ERROR;
          LOG_WARN("split must is equal", K(ret), "origin rowkey", *rowkey_left, "new rowkey", *rowkey_beg);
          const ObString& err_msg = orig_part_schema->get_part_array()[index - 1]->get_part_name();
          LOG_USER_ERROR(
              OB_ERR_RANGE_NOT_INCREASING_ERROR, lib::is_oracle_mode() ? err_msg.length() : 0, err_msg.ptr());
        }
      }
      // Range values of added partition should increase Monotonicity
      const ObRowkey* rowkey_last = &part_array[part_num - 1]->get_high_bound_val();
      for (int64_t i = part_num - 2; OB_SUCC(ret) && i >= 0; --i) {
        const ObRowkey* rowkey_cur = &part_array[i]->get_high_bound_val();
        if (*rowkey_cur >= *rowkey_last) {
          ret = OB_ERR_RANGE_NOT_INCREASING_ERROR;
          LOG_WARN("range values should increasing", K(ret), "curr_rowkey", *rowkey_cur, "last_rowkey", *rowkey_last);
          const ObString& err_msg = part_array[i]->get_part_name();
          LOG_USER_ERROR(
              OB_ERR_RANGE_NOT_INCREASING_ERROR, lib::is_oracle_mode() ? err_msg.length() : 0, err_msg.ptr());
        } else {
          rowkey_last = rowkey_cur;
        }
      }
      // To ensure that there is no data loss after partition split, the high bound value of the
      // last partition split point should be same with spilt source partition
      if (OB_SUCC(ret)) {
        const ObRowkey* rowkey_last = &orig_part_schema->get_part_array()[index]->get_high_bound_val();
        const ObRowkey* rowkey_cur = &part_array[part_num - 1]->get_high_bound_val();
        if (*rowkey_cur != *rowkey_last) {
          ret = OB_ERR_REORGANIZE_OUTSIDE_RANGE;
          LOG_WARN("split must is equal", K(ret), "curr_rowkey", *rowkey_cur, "last_rowkey", *rowkey_last);
          LOG_USER_ERROR(OB_ERR_REORGANIZE_OUTSIDE_RANGE);
        }
      }
    } else if (orig_part_schema->is_list_part()) {
      if (OB_FAIL(ddl_service_->check_split_list_partition_match(*alter_part_schema, *orig_part_schema, index))) {
        LOG_WARN("failed to check add list partition", K(ret), K(index));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected alter partition type",
          K(ret),
          "part type",
          orig_part_schema->get_part_option().get_part_func_type());
    }
  }
  return ret;
}

// 1. check the validity of partition_option
// 2. create or drop tablegroup partition
// 3. create or drop table partition which is in one tablegroup
int ObTableGroupHelp::modify_partition_option(ObMySQLTransaction& trans, ObSchemaGetterGuard& schema_guard,
    const ObTablegroupSchema& tablegroup_schema, const ObAlterTablegroupArg& arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  int64_t expr_num = tablegroup_schema.get_part_func_expr_num();
  ObTablegroupSchema& alter_tablegroup_schema = const_cast<ObTablegroupSchema&>(arg.alter_tablegroup_schema_);
  alter_tablegroup_schema.set_tablegroup_id(tablegroup_id);
  alter_tablegroup_schema.set_tenant_id(tenant_id);
  alter_tablegroup_schema.set_binding(tablegroup_schema.get_binding());
  alter_tablegroup_schema.get_part_option().set_max_used_part_id(
      tablegroup_schema.get_part_option().get_max_used_part_id());
  alter_tablegroup_schema.get_part_option().set_partition_cnt_within_partition_table(
      tablegroup_schema.get_part_option().get_partition_cnt_within_partition_table());
  ObAlterTablegroupArg::ModifiableOptions alter_part_type = ObAlterTablegroupArg::MAX_OPTION;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
  if (!arg.is_alter_partitions()) {
    // skip
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("check tablegroup before ver 2.0 alter partition optition is not supported", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret), KP(schema_service_));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(arg.tenant_id_, compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret), K(arg.tenant_id_));
  } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    share::CompatModeGuard g(compat_mode);
    if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::ADD_PARTITION)) {
      alter_tablegroup_schema.get_part_option().set_part_num(alter_tablegroup_schema.get_partition_num());
      alter_part_type = ObAlterTablegroupArg::ADD_PARTITION;
      if (OB_FAIL(modify_add_partition(
              trans, schema_guard, tablegroup_schema, alter_tablegroup_schema, new_schema_version, expr_num, arg))) {
        LOG_WARN("failed to modify add partition", K(ret), K(compat_mode));
      }
    } else if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::DROP_PARTITION)) {
      alter_part_type = ObAlterTablegroupArg::DROP_PARTITION;
      alter_tablegroup_schema.get_part_option().set_part_num(alter_tablegroup_schema.get_partition_num());
      if (OB_FAIL(modify_drop_partition(
              trans, schema_guard, tablegroup_schema, alter_tablegroup_schema, new_schema_version, expr_num, arg))) {
        LOG_WARN("failed to modify drop partition", K(ret), K(compat_mode));
      }
    } else if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::PARTITIONED_TABLE)) {
      alter_part_type = ObAlterTablegroupArg::PARTITIONED_TABLE;
    } else if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::REORGANIZE_PARTITION)) {
      alter_part_type = ObAlterTablegroupArg::REORGANIZE_PARTITION;
    } else if (arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::SPLIT_PARTITION)) {
      alter_part_type = ObAlterTablegroupArg::SPLIT_PARTITION;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid alter_part_type", K(ret), K(alter_part_type));
    }
    if (OB_FAIL(ret)) {
    } else if (ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type ||
               ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type ||
               ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type) {
      // When splitting partition, cannot assign partition_num to part_num,
      // because partition_num is 0 when hash partition is splitting
      if (OB_FAIL(modify_split_partition(trans,
              schema_guard,
              tablegroup_schema,
              alter_tablegroup_schema,
              new_schema_version,
              expr_num,
              alter_part_type,
              arg))) {
        LOG_WARN("failed to split partition", K(ret), K(compat_mode));
      }
    }
  }
  return ret;
}
// drop partition
// check alter tabletablegroup partition, batch modify table schema, modify tablegroup schema
int ObTableGroupHelp::modify_drop_partition(common::ObMySQLTransaction& trans,
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
    share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version, const int64_t expr_num,
    const obrpc::ObAlterTablegroupArg& arg)
{
  int ret = OB_SUCCESS;
  const ObPartitionSchema* orig_schema = &orig_tablegroup_schema;
  ObPartitionSchema* alter_schema = &inc_tablegroup_schema;
  ObTablegroupSchema new_tablegroup_schema;
  bool is_tablegroup = true;
  if (PARTITION_LEVEL_ZERO == orig_schema->get_part_level()) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_WARN("unsupport management on non-partition table", K(ret));
  } else if (!orig_schema->is_list_part() && !orig_schema->is_range_part()) {
    ret = OB_ERR_ONLY_ON_RANGE_LIST_PARTITION;
    LOG_WARN("can only be used on RANGE/LIST partitions", K(ret));
  } else if (OB_FAIL(check_alter_partition(
                 orig_schema, alter_schema, ObAlterTablegroupArg::DROP_PARTITION, expr_num, is_tablegroup))) {
    LOG_WARN("failed to check alter partition", K(ret), K(orig_schema), K(alter_schema));
  } else if (OB_FAIL(batch_modify_table_partitions(trans,
                 schema_guard,
                 inc_tablegroup_schema,
                 orig_tablegroup_schema,
                 new_schema_version,
                 expr_num,
                 ObAlterTablegroupArg::DROP_PARTITION,
                 arg.create_mode_))) {
    LOG_WARN("failed to modify table partition", K(ret), K(orig_schema), K(alter_schema));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service or sql_proxy is null", K(ret), KP(schema_service_), KP(sql_proxy_));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    if (OB_FAIL(ddl_operator.drop_tablegroup_partitions(orig_tablegroup_schema,
            inc_tablegroup_schema,
            new_schema_version,
            new_tablegroup_schema,
            trans,
            &(arg.ddl_stmt_str_)))) {
      LOG_WARN("fail to drop tablegroup partition", K(ret), K(orig_schema), K(new_tablegroup_schema));
    }
  }
  return ret;
}
// Add partition
// Modify the table schema, fill table partition name, check table partition name conflicts,
// fill tablegroup partition name, check tablegroup partition, modify tablegroup schema
int ObTableGroupHelp::modify_add_partition(common::ObMySQLTransaction& trans,
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
    share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version, const int64_t expr_num,
    const obrpc::ObAlterTablegroupArg& arg)
{
  int ret = OB_SUCCESS;
  const ObPartitionSchema* orig_schema = &orig_tablegroup_schema;
  ObPartitionSchema* alter_schema = &inc_tablegroup_schema;
  ObTablegroupSchema new_tablegroup_schema;
  bool is_tablegroup = true;

  if (PARTITION_LEVEL_ZERO == orig_schema->get_part_level()) {
    ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
    LOG_WARN("unsupport management on non-partition table", K(ret));
  } else if (!orig_schema->is_list_part() && !orig_schema->is_range_part()) {
    ret = OB_ERR_ONLY_ON_RANGE_LIST_PARTITION;
    LOG_WARN("can only be used on RANGE/LIST partitions", K(ret));
  } else if (OB_FAIL(check_alter_partition(
                 orig_schema, alter_schema, ObAlterTablegroupArg::ADD_PARTITION, expr_num, is_tablegroup))) {
    LOG_WARN("failed to check tablegroup schema", K(ret), K(inc_tablegroup_schema));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service or sql_proxy is null", K(ret), KP(schema_service_), KP(sql_proxy_));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    if (OB_FAIL(ddl_operator.add_tablegroup_partitions(orig_tablegroup_schema,
            inc_tablegroup_schema,
            new_schema_version,
            new_tablegroup_schema,
            trans,
            &(arg.ddl_stmt_str_)))) {
      LOG_WARN("fail to add tablegroup partition", K(ret), K(orig_tablegroup_schema), K(inc_tablegroup_schema));
    } else if (!orig_tablegroup_schema.get_binding()) {
      // nothing
    } else {
      ObArray<int64_t> new_partition_ids;
      // Need set tablegroup_schema's part_id which get from inc_tablegroup_schema, and
      // fill corresponding pgkey into all_part table of the user table
      ObTablegroupSchema& tmp_tg_schema = inc_tablegroup_schema;
      tmp_tg_schema.set_schema_version(new_schema_version);
      ObPartIdsGeneratorForAdd<ObTablegroupSchema> gen(orig_tablegroup_schema, inc_tablegroup_schema);
      if (OB_FAIL(gen.gen(new_partition_ids))) {
        LOG_WARN("fail to gen new part ids", K(ret), K(new_partition_ids));
        // For binding table, should construct the mapping from pkey to pgkey, so the partition of
        // the tablegroup must contain part_id
        // Need to put the pkey to pgkey mapping in alter_table_schema, so add the sub-partition
      } else if (OB_FAIL(tmp_tg_schema.try_assign_def_subpart_array(orig_tablegroup_schema))) {
        LOG_WARN("failed to assign sub partition", K(ret), K(orig_tablegroup_schema));
      } else if (OB_FAIL(set_add_partition_part_id(orig_tablegroup_schema, tmp_tg_schema))) {
        LOG_WARN("failed to add partitions", K(ret), K(inc_tablegroup_schema));
        // Add tablegroup partition
      } else if (OB_FAIL(ddl_service_->add_tablegroup_partitions_for_add(
                     orig_tablegroup_schema, inc_tablegroup_schema, new_partition_ids, arg.create_mode_))) {
        LOG_WARN("failed to create tablegroup partition", K(ret), K(inc_tablegroup_schema));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(batch_modify_table_partitions(trans,
                 schema_guard,
                 inc_tablegroup_schema,
                 inc_tablegroup_schema,
                 new_schema_version,
                 expr_num,
                 ObAlterTablegroupArg::ADD_PARTITION,
                 arg.create_mode_))) {
    // add_partition does not construct new_tablegroup_schema, use inc instead
    LOG_WARN("failed to modify table schema", K(ret), K(inc_tablegroup_schema));
  }

  return ret;
}
// Partition split, table split
int ObTableGroupHelp::modify_split_partition(common::ObMySQLTransaction& trans,
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
    share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t new_schema_version, const int64_t expr_num,
    obrpc::ObAlterTablegroupArg::ModifiableOptions alter_part_type, const obrpc::ObAlterTablegroupArg& arg)
{
  int ret = OB_SUCCESS;
  ObTablegroupSchema new_tablegroup_schema;
  bool is_tablegroup = true;
  const ObPartitionSchema* orig_schema = &orig_tablegroup_schema;
  ObPartitionSchema* alter_schema = &inc_tablegroup_schema;
  // Partition split use OB_CREATE_TABLE_MODE_STRICT mode
  ObCreateTableMode create_mode = OB_CREATE_TABLE_MODE_STRICT;
  if (PARTITION_LEVEL_TWO == inc_tablegroup_schema.get_part_level()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not split table into two level", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "specified split parition operation");
  } else if (ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type ||
             ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type) {
    if (PARTITION_LEVEL_ONE != orig_tablegroup_schema.get_part_level()) {
      ret = OB_ERR_PARTITION_MGMT_ON_NONPARTITIONED;
      LOG_WARN("unsupport management on non-partition table", K(ret));
    } else if (!inc_tablegroup_schema.is_list_part() && !inc_tablegroup_schema.is_range_part()) {
      ret = OB_ERR_ONLY_ON_RANGE_LIST_PARTITION;
      LOG_WARN("can only be used on RANGE/LIST partitions", K(ret));
    }
  } else if (ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type) {
    if (PARTITION_LEVEL_ZERO != orig_tablegroup_schema.get_part_level()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("requested table is already partitioned", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition already partitioned table");
    } else if (OB_FAIL(ddl_service_->check_split_table_partition_valid(*alter_schema))) {
      // When the table is split to a partitioned table, need to ensure that the last partition
      // rowkey is default or maxvalue
      LOG_WARN("failed to check partition table vaild", K(ret));
    } else {
      // Check partition keys of all tables already exist
      // Check the partition types are same
      int64_t tablegroup_id = orig_tablegroup_schema.get_tablegroup_id();
      int64_t tenant_id = orig_tablegroup_schema.get_tenant_id();
      if (OB_FAIL(check_table_partition_key_exist(
              schema_guard, inc_tablegroup_schema.get_part_option().get_part_func_type(), tenant_id, tablegroup_id))) {
        LOG_WARN("fail to check table partition key", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_alter_partition(orig_schema, alter_schema, alter_part_type, expr_num, is_tablegroup))) {
    LOG_WARN("failed to check tablegroup schema", K(ret), K(orig_tablegroup_schema), K(alter_schema));
  } else if (OB_FAIL(new_tablegroup_schema.assign(orig_tablegroup_schema))) {
    LOG_WARN("failed to assign orig tablegroup schema", K(ret), K(orig_tablegroup_schema));
  } else {
    ObSplitInfo split_info;
    ObSchemaOperationType opt_type;
    ObAlterTableArg::AlterPartitionType type = ObAlterTableArg::NO_OPERATION;
    if (ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type) {
      type = ObAlterTableArg::PARTITIONED_TABLE;
      opt_type = OB_DDL_PARTITIONED_TABLEGROUP_TABLE;
    } else if (ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type) {
      type = ObAlterTableArg::REORGANIZE_PARTITION;
      opt_type = OB_DDL_SPLIT_TABLEGROUP_PARTITION;
    } else if (ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type) {
      type = ObAlterTableArg::SPLIT_PARTITION;
      opt_type = OB_DDL_SPLIT_TABLEGROUP_PARTITION;
    }
    new_tablegroup_schema.ObPartitionSchema::reset();

    if (OB_FAIL(ObPartitionSplitHelper::fill_split_info<ObTablegroupSchema>(orig_tablegroup_schema,
            inc_tablegroup_schema,
            new_tablegroup_schema,
            type,
            new_schema_version,
            split_info,
            false))) {
      LOG_WARN("fail to fill split info", K(ret), K(orig_tablegroup_schema), K(inc_tablegroup_schema));
    } else if (OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service or sql_proxy is null", K(ret), KP(schema_service_), KP(sql_proxy_));
    } else {
      ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
      ObArray<int64_t> new_partition_ids;
      ObPartIdsGeneratorForAdd<ObTablegroupSchema> gen(orig_tablegroup_schema, inc_tablegroup_schema);
      if (OB_FAIL(ddl_operator.split_tablegroup_partitions(
              orig_tablegroup_schema, new_tablegroup_schema, split_info, trans, &(arg.ddl_stmt_str_), opt_type))) {
        LOG_WARN("fail to split tablegroup partitions",
            K(ret),
            K(orig_tablegroup_schema),
            K(new_tablegroup_schema),
            K(split_info));
      } else if (!orig_tablegroup_schema.get_binding()) {
        // Nothing
      } else if (OB_FAIL(gen.gen(new_partition_ids))) {
        LOG_WARN("fail to gen new part ids", K(ret), K(new_partition_ids));
        // Add pg partition
      } else if (1 < new_partition_ids.count()) {
        new_tablegroup_schema.get_part_option().set_part_num(new_tablegroup_schema.get_partition_num());
        if (OB_FAIL(ddl_service_->add_tablegroup_partitions_for_split(
                orig_tablegroup_schema, new_tablegroup_schema, new_partition_ids, create_mode))) {
          LOG_WARN("failed to create tablegroup partition", K(ret), K(inc_tablegroup_schema));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        LOG_DEBUG("split tablegroup partitions success",
            K(ret),
            K(orig_tablegroup_schema),
            K(split_info),
            K(inc_tablegroup_schema));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(batch_modify_table_partitions(trans,
                 schema_guard,
                 inc_tablegroup_schema,
                 new_tablegroup_schema,
                 new_schema_version,
                 expr_num,
                 alter_part_type,
                 create_mode))) {
    LOG_WARN("failed to modify table schema", K(ret), K(inc_tablegroup_schema));
  }
  return ret;
}

int ObTableGroupHelp::batch_modify_table_partitions(ObMySQLTransaction& trans, ObSchemaGetterGuard& schema_guard,
    ObTablegroupSchema& inc_tablegroup_schema, const ObTablegroupSchema& new_tablegroup_schema,
    const int64_t new_schema_version, const int64_t expr_num, ObAlterTablegroupArg::ModifiableOptions alter_part_type,
    ObCreateTableMode create_mode)
{
  int ret = OB_SUCCESS;
  const int64_t tablegroup_id = new_tablegroup_schema.get_tablegroup_id();
  uint64_t tenant_id = extract_tenant_id(tablegroup_id);
  ObArray<const ObTableSchema*> table_schemas;
  if (!is_new_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablegroup_id", K(ret), K(tablegroup_id));
  } else if (new_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_version is invalid", K(ret), K(new_schema_version));
  } else if (ObAlterTablegroupArg::MAX_OPTION == alter_part_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(alter_part_type));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schemas in tablegroup", K(ret), K(tenant_id), K(tablegroup_id));
  } else {
    bool is_tablegroup = false;
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      const ObTableSchema& orig_table_schema = *(table_schemas.at(i));
      if (!orig_table_schema.has_partition()) {
        // No need to deal with tables without partition
        continue;
      }
      AlterTableSchema alter_table_schema;
      bool is_split = false;
      if (ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type ||
          ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type ||
          ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type) {
        is_split = true;
        // TODO: Need to support merge in the future
      }
      // When the index is not available, partition management is prohibited.
      // Does not need to check whether the partition key meets the requirements of the index key for tablegroup
      if (OB_FAIL(ddl_service_->check_index_valid_for_alter_partition(
              orig_table_schema, schema_guard, ObAlterTablegroupArg::DROP_PARTITION == alter_part_type, is_split))) {
        LOG_WARN("failed to check index valid",
            K(ret),
            "alter_part_type",
            alter_part_type,
            K(is_split),
            K(orig_table_schema));
      } else if (OB_FAIL(alter_table_schema.assign(orig_table_schema))) {
        LOG_WARN("failed to assign from origin table schema", K(ret), K(orig_table_schema));
      } else if (OB_FAIL(alter_table_schema.assign_tablegroup_partition(inc_tablegroup_schema))) {
        LOG_WARN("fail to assign from tablegroup_schema", K(ret), K(orig_table_schema), K(inc_tablegroup_schema));
      } else {
        ObPartitionSchema* inc_table = &alter_table_schema;
        const ObPartitionSchema* orig_table = &orig_table_schema;
        if (OB_FAIL(check_alter_partition(orig_table, inc_table, alter_part_type, expr_num, is_tablegroup))) {
          LOG_WARN("fail to check partition optition", K(ret));
        } else if (ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type &&
                   1 == alter_table_schema.get_part_option().get_part_num()) {
          // When partition num is 1, no real split is performed, and have already written the table
          // schema for all tables in the tablegroup, no further processing is required
        } else {
          ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
          if (ObAlterTablegroupArg::ADD_PARTITION == alter_part_type) {
            if (OB_FAIL(add_table_partition_in_tablegroup(orig_table_schema,
                    inc_tablegroup_schema,
                    new_schema_version,
                    alter_table_schema,
                    create_mode,
                    trans))) {
              LOG_WARN("fail to add table partition", K(ret));
            }
          } else if (ObAlterTablegroupArg::DROP_PARTITION == alter_part_type) {
            ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
            if (OB_FAIL(ddl_operator.drop_table_partitions(
                    orig_table_schema, alter_table_schema, new_schema_version, trans))) {
              LOG_WARN("fail to drop table partitions", K(ret));
            }
          } else if (ObAlterTablegroupArg::PARTITIONED_TABLE == alter_part_type) {
            ObAlterTableArg::AlterPartitionType type = ObAlterTableArg::PARTITIONED_TABLE;
            if (OB_FAIL(split_table_partition_in_tablegroup(orig_table_schema,
                    new_tablegroup_schema,
                    new_schema_version,
                    type,
                    alter_table_schema,
                    create_mode,
                    trans))) {
              LOG_WARN("fail to split table partition", K(ret));
            }
          } else if (ObAlterTablegroupArg::REORGANIZE_PARTITION == alter_part_type ||
                     ObAlterTablegroupArg::SPLIT_PARTITION == alter_part_type) {
            ObAlterTableArg::AlterPartitionType type = ObAlterTableArg::REORGANIZE_PARTITION;
            if (OB_FAIL(split_table_partition_in_tablegroup(orig_table_schema,
                    new_tablegroup_schema,
                    new_schema_version,
                    type,
                    alter_table_schema,
                    create_mode,
                    trans))) {
              LOG_WARN("fail to split table partition", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid alter_part_type", K(ret), K(alter_part_type));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableGroupHelp::check_part_expr_num_and_value_type(
    const ObPartition* alter_part, const ObPartition* orig_part, const ObPartitionFuncType part_type, int64_t expr_num)
{
  int ret = OB_SUCCESS;
  const bool is_check_value = true;
  bool is_oracle_mode = false;
  if (OB_ISNULL(alter_part) || OB_ISNULL(orig_part)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alter_part is null", K(ret));
  } else if (expr_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr num is invalid", K(ret), K(expr_num));
  } else if (OB_FAIL(
                 ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(orig_part->get_tenant_id(), is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", K(ret));
  } else if (is_range_part(part_type)) {
    const ObRowkey rowkey = alter_part->get_high_bound_val();
    const ObRowkey tg_rowkey = orig_part->get_high_bound_val();
    if (PARTITION_FUNC_TYPE_RANGE == part_type) {
      // For range partition, need to ensure that the number of expressions is 1,
      // and the value is an integer
      if (1 != rowkey.get_obj_cnt()) {
        ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
        LOG_WARN(
            "Inconsistency in usage of column lists for partitioning near", K(ret), "expr_num", rowkey.get_obj_cnt());
      } else if (OB_ISNULL(rowkey.get_obj_ptr()) || OB_ISNULL(tg_rowkey.get_obj_ptr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey obj ptr is null", K(ret), KPC(alter_part));
      } else {
        const ObObj& obj = rowkey.get_obj_ptr()[0];
        if (obj.is_max_value()) {
          // just pass
        } else if (!ob_is_integer_type(obj.get_type())) {
          ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
          LOG_WARN("obj type is invalid", K(ret), K(rowkey));
          LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
              alter_part->get_part_name().length(),
              alter_part->get_part_name().ptr());
        }
      }
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type) {
      // For range column partition, need to ensure that the number of partition expression is
      // consistent with that of the tablegroup
      if (expr_num != rowkey.get_obj_cnt()) {
        ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
        LOG_WARN("Inconsistency in usage of column lists for partitioning near",
            K(ret),
            K(expr_num),
            "obj_cnt",
            rowkey.get_obj_cnt());
      } else if (expr_num != tg_rowkey.get_obj_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("obj cnt not matched", K(ret), K(expr_num), "obj_cnt", tg_rowkey.get_obj_cnt());
      } else if (OB_ISNULL(rowkey.get_obj_ptr()) || OB_ISNULL(tg_rowkey.get_obj_ptr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey obj ptr is null", K(ret), KPC(alter_part), KPC(orig_part));
      } else {
        ObCastCtx cast_ctx(
            const_cast<ObPartition*>(alter_part)->get_allocator(), NULL, CM_NONE, ObCharset::get_system_collation());
        for (int64_t k = 0; k < rowkey.get_obj_cnt() && OB_SUCC(ret); k++) {
          const ObObj& obj = rowkey.get_obj_ptr()[k];
          ObObj& tg_obj = const_cast<ObObj&>(tg_rowkey.get_obj_ptr()[k]);
          if (obj.is_max_value() || tg_obj.is_max_value()) {
            // just pass
          } else if (!sql::ObResolverUtils::is_valid_partition_column_type(obj.get_type(), part_type, is_check_value)) {
            ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
            LOG_WARN("obj type is invalid", K(ret), K(obj));
            LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
                alter_part->get_part_name().length(),
                alter_part->get_part_name().ptr());
          } else if (obj.get_type() != tg_obj.get_type()) {
            if (!ObPartitionUtils::is_types_equal_for_partition_check(obj.get_type(), tg_obj.get_type())) {
              ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
              LOG_USER_ERROR(OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
              LOG_WARN("object type is invalid ", K(ret), KPC(alter_part), KPC(orig_part), K(is_oracle_mode));
            } else if (OB_FAIL(ObObjCaster::to_type(obj.get_type(), cast_ctx, tg_obj, tg_obj))) {
              LOG_WARN("failed to cast object", K(obj), K(ret), K(k));
            }
          }
        }
      }
    }
  } else if (is_list_part(part_type)) {
    const common::ObIArray<common::ObNewRow>* new_rowkey = &(alter_part->get_list_row_values());
    const common::ObIArray<common::ObNewRow>* orig_rowkey = &(orig_part->get_list_row_values());
    if (1 > new_rowkey->count() || 1 > orig_rowkey->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("list value is empty", K(ret), "orig num", orig_rowkey->count(), "new num", new_rowkey->count());
    } else if (PARTITION_FUNC_TYPE_LIST == part_type) {
      int64_t count = new_rowkey->count();
      for (int64_t index = 0; OB_SUCC(ret) && index < count; ++index) {
        if (1 != new_rowkey->at(index).get_count()) {
          ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
          LOG_WARN("Inconsistency in usage of column lists for partitioning near",
              K(ret),
              K(index),
              "obj count",
              new_rowkey->at(index).get_count());
        } else if (new_rowkey->at(index).get_cell(0).is_max_value()) {
          // No need to check default partition
        } else if (!ob_is_integer_type(new_rowkey->at(index).get_cell(0).get_type())) {
          // Currently only support int type for list partition
          ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
          LOG_WARN("obj type is invalid", K(ret), K(index), K(new_rowkey));
          LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
              alter_part->get_part_name().length(),
              alter_part->get_part_name().ptr());
        }
      }
    } else if (PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
      if ((1 == new_rowkey->count() && 1 == new_rowkey->at(0).get_count() &&
              new_rowkey->at(0).get_cell(0).is_max_value())) {
        // There will only be one default partition, and the semantics is different from
        // the maxvalue of the range partition
      } else {
        int64_t count = new_rowkey->count();
        for (int64_t index = 0; OB_SUCC(ret) && index < count; ++index) {
          int64_t obj_count = new_rowkey->at(index).get_count();
          if (obj_count != expr_num) {
            ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
            LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret), K(expr_num), K(obj_count));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < obj_count; ++i) {
            const ObObj& obj = new_rowkey->at(index).get_cell(i);
            if (!sql::ObResolverUtils::is_valid_partition_column_type(obj.get_type(), part_type, is_check_value)) {
              ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
              LOG_WARN("obj type is invalid", K(ret), K(obj));
              LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR,
                  alter_part->get_part_name().length(),
                  alter_part->get_part_name().ptr());
            }
          }
        }  // end for
        if (OB_FAIL(ret)) {
        } else if (1 == orig_rowkey->count() && 1 == orig_rowkey->at(0).get_count() &&
                   orig_rowkey->at(0).get_cell(0).is_max_value()) {
          // pass nothing to check
        } else {
          // Compare the number of column partition expressions is consistent
          int64_t count = new_rowkey->count();
          for (int64_t index = 0; OB_SUCC(ret) && index < count; ++index) {
            if (new_rowkey->at(index).get_count() != orig_rowkey->at(0).get_count()) {
              ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
              LOG_WARN("Inconsistency in usage of column lists for partitioning near",
                  K(ret),
                  K(index),
                  "obj count",
                  new_rowkey->at(index).get_count(),
                  "orig count",
                  orig_rowkey->at(0).get_count());
            } else {
              int64_t obj_count = new_rowkey->at(index).get_count();
              ObCastCtx cast_ctx(const_cast<ObPartition*>(alter_part)->get_allocator(),
                  NULL,
                  CM_NONE,
                  ObCharset::get_system_collation());
              for (int64_t i = 0; OB_SUCC(ret) && i < obj_count; ++i) {
                ObObj& obj = const_cast<ObObj&>(new_rowkey->at(index).get_cell(i));
                if (obj.get_type() != orig_rowkey->at(0).get_cell(i).get_type()) {
                  if (!ObPartitionUtils::is_types_equal_for_partition_check(
                          obj.get_type(), orig_rowkey->at(0).get_cell(i).get_type())) {
                    ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
                    LOG_USER_ERROR(OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
                    LOG_WARN("object type is invalid ", K(ret), KPC(alter_part), KPC(orig_part), K(is_oracle_mode));
                  } else if (OB_FAIL(
                                 ObObjCaster::to_type(orig_rowkey->at(0).get_cell(i).get_type(), cast_ctx, obj, obj))) {
                    LOG_WARN("failed to cast object", K(orig_rowkey->at(0).get_cell(i)), K(ret));
                  }
                }
              }  // end for check obj type
            }
          }  // end for check newrow
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only support range/range column, list/list columns partition", K(ret), K(part_type));
  }
  return ret;
}
//////////////////////////////
//////////////////////////////

int ObPartitionSplitHelper::build_split_info(
    const share::schema::ObPartitionSchema& table_schema, share::ObSplitPartition& split_info)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_in_splitting()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table schema", K(ret), K(table_schema));
  } else {
    ObSplitPartitionPair pair;
    if (OB_FAIL(build_split_pair(&table_schema, pair))) {
      LOG_WARN("failed to build split pair", K(ret), K(pair), K(table_schema));
    } else if (!pair.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pair is invalid", K(ret), K(pair), K(table_schema));
    } else if (OB_FAIL(split_info.get_spp_array().push_back(pair))) {
      LOG_WARN("fail to push back", K(ret), K(split_info), K(pair));
    } else {
      split_info.set_schema_version(table_schema.get_partition_schema_version());
    }
  }
  LOG_INFO("build split info success", K(ret), K(split_info));
  return ret;
}

int ObPartitionSplitHelper::build_split_pair(
    const share::schema::ObPartitionSchema* table_schema, share::ObSplitPartitionPair& pair)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema) || !table_schema->is_in_splitting()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is null or not in split", K(ret), K(table_schema));
  } else {
    int64_t source_partition_id = OB_INVALID_PARTITION_ID;
    ObPartition** part_array = table_schema->get_part_array();
    const int64_t part_num = table_schema->get_partition_num();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid table schema", K(ret), K(table_schema));
    }
    ObPartitionKey part_key;
    for (int64_t i = 0; i < part_num && OB_SUCC(ret); i++) {
      const ObPartition* partition = part_array[i];
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid partition", K(ret), K(partition));
      } else if (0 == partition->get_source_part_ids().count()) {
        continue;
      } else if (1 != partition->get_source_part_ids().count()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support, one partition can split only once in a round",
            K(ret),
            "source_part_ids",
            partition->get_source_part_ids());
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "split one partition multiple times");
      } else if (OB_INVALID_PARTITION_ID == source_partition_id) {
        if (OB_FAIL(pair.get_source_pkey().init(table_schema->get_table_id(),
                partition->get_source_part_ids().at(0),
                table_schema->get_partition_cnt()))) {
          LOG_WARN("failed to set source key", K(ret), K(partition));
        } else {
          source_partition_id = partition->get_source_part_ids().at(0);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (source_partition_id != partition->get_source_part_ids().at(0)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support split different partition",
            K(source_partition_id),
            "source_partition_ids",
            partition->get_source_part_ids());
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "split one partition multiple times");
      } else {
        part_key.reset();
        if (OB_FAIL(part_key.init(
                table_schema->get_table_id(), partition->get_part_id(), table_schema->get_partition_cnt()))) {
          LOG_WARN("failed to init partitionkey",
              K(ret),
              K(part_key),
              "partition",
              *partition,
              "table_schema",
              *table_schema);
        } else if (OB_FAIL(pair.get_dest_array().push_back(part_key))) {
          LOG_WARN("fail to push back", K(ret), K(part_key));
        }
      }
    }
  }
  return ret;
}

int ObPartitionSplitHelper::check_split_result(
    const ObSplitPartitionArg& arg, const ObSplitPartitionResult& result, ObSplitProgress& split_status)
{
  int ret = OB_SUCCESS;
  split_status = PHYSICAL_SPLIT_FINISH;
  if (arg.split_info_.get_spp_array().count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg), K(result));
  } else if (arg.split_info_.get_spp_array().count() != result.get_result().count()) {
    LOG_WARN("partition not match", K(arg), K(result));
    split_status = UNKNOWN_SPLIT_PROGRESS;
  } else {
    const common::ObIArray<ObPartitionSplitProgress>& progress_array = result.get_result();
    for (int64_t i = 0; i < progress_array.count(); i++) {
      const ObPartitionSplitProgress& progress = progress_array.at(i);
      if (split_status > progress.get_progress()) {
        split_status = static_cast<ObSplitProgress>(progress.get_progress());
      }
    }
  }
  return ret;
}

int ObTableGroupHelp::check_table_partition_key_exist(ObSchemaGetterGuard& schema_guard,
    const share::schema::ObPartitionFuncType part_func_type, const int64_t tenant_id, const int64_t tg_id)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema*> table_schemas;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == tg_id || !is_tablegroup_id(tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tg_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id, tg_id, table_schemas))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(tg_id));
  } else {
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      const ObTableSchema*& table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid table schema", K(ret), K(i));
      } else if (!table_schema->has_partition()) {
        // No partition information for index tables, etc.
      } else if (0 >= table_schema->get_partition_key_column_num()) {
        // Only consider first-level partition split
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("table in tablegroup has no partition_key, refuse to split by tablegroup",
            K(ret),
            K(tenant_id),
            K(tg_id),
            K(table_schema));
      } else if (part_func_type != table_schema->get_part_option().get_part_func_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("table partition func different tablegroup",
            K(ret),
            K(part_func_type),
            "table partition_func",
            table_schema->get_part_option().get_part_func_type());
      }
    }
  }
  return ret;
}
// new_tablegroup_schemacontains hash partition information, not in inc_tablegroup_schema
// Because need to specify the table to tablegroup mapping relationship,
// need to ensure that there is part_id information in tablegroup partition
int ObTableGroupHelp::split_table_partition_in_tablegroup(const ObTableSchema& orig_table_schema,
    const share::schema::ObTablegroupSchema& new_tablegroup_schema, const int64_t schema_version,
    const ObAlterTableArg::AlterPartitionType op_type, AlterTableSchema& alter_table_schema,
    ObCreateTableMode create_mode, ObMySQLTransaction& client)
{
  int ret = OB_SUCCESS;
  ObSplitInfo split_info;
  ObTableSchema new_table_schema;
  ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
  ObArray<int64_t> new_partition_ids;  // no need init
  bool is_alter_tablegroup = true;
  int64_t frozen_version = 0;
  int64_t frozen_timestamp = 0;

  ObPartIdsGeneratorForAdd<ObTableSchema> gen(orig_table_schema, alter_table_schema);
  if (OB_FAIL(gen.gen(new_partition_ids))) {
    LOG_WARN("fail to gen partition ids", K(ret));
  } else if (OB_FAIL(new_table_schema.assign(orig_table_schema))) {
    LOG_WARN("failed to assign table schema", K(ret), K(orig_table_schema));
  } else {
    new_table_schema.reset_partition_schema();  // Reset the partition schema before split
  }
  if (OB_FAIL(ret)) {
  } else if (orig_table_schema.is_no_pk_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table has no primary key, refuse to split", K(ret), "table_id", orig_table_schema.get_table_id());
  } else if (OB_FAIL(ObPartitionSplitHelper::fill_split_info(
                 orig_table_schema, alter_table_schema, new_table_schema, op_type, schema_version, split_info, true))) {
    LOG_WARN("fail to split info", K(ret), K(orig_table_schema), K(alter_table_schema));
  } else if (!new_tablegroup_schema.get_binding()) {
  } else if (OB_FAIL(set_mapping_pg_part_id(new_tablegroup_schema, new_table_schema))) {
    LOG_WARN("failed to set mapping pg part id", K(ret), K(new_tablegroup_schema), K(new_table_schema));
  } else if (PARTITION_LEVEL_TWO == new_tablegroup_schema.get_part_level()) {
    if (OB_FAIL(set_mapping_pg_sub_part_id(new_tablegroup_schema, new_table_schema))) {
      LOG_WARN("failed to set map pg sub part is", K(ret), K(new_tablegroup_schema), K(new_table_schema));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ddl_operator.split_table_partitions(
                 new_table_schema, orig_table_schema, alter_table_schema, split_info, is_alter_tablegroup, client))) {
    LOG_WARN(
        "fail to split table partitions", K(ret), K(new_table_schema), K(alter_table_schema), K(orig_table_schema));
  } else if (OB_FAIL(ddl_service_->get_zone_mgr().get_frozen_info(frozen_version, frozen_timestamp))) {
    LOG_WARN("fail to get frozen info", K(ret));
  } else if (new_tablegroup_schema.get_binding()) {
    /**
     * In fill_split_info, change the part_num of new_schema to the value after splitting,
     * but in fact there are only partitions newly added in part_array after splitting.
     * In the later code, it will use part_num in many iterators when adding partition,
     * so after modifying schema, change it back to prevent core dump.
     */
    new_table_schema.get_part_option().set_part_num(new_table_schema.get_partition_num());
    if (OB_FAIL(ddl_service_->binding_add_partitions_for_add(
            schema_version, new_table_schema, orig_table_schema, new_tablegroup_schema, create_mode))) {
      LOG_WARN("failed to create binding create partition",
          K(ret),
          K(schema_version),
          K(alter_table_schema),
          K(new_tablegroup_schema));
    }
  } else {
    if (OB_FAIL(ddl_service_->add_partitions_for_split(
            schema_version, orig_table_schema, alter_table_schema, new_table_schema, new_partition_ids))) {
      LOG_WARN(
          "fail to add partition for split", K(ret), K(orig_table_schema), K(alter_table_schema), K(new_table_schema));
    }
  }
  return ret;
}

int ObTableGroupHelp::add_table_partition_in_tablegroup(const share::schema::ObTableSchema& orig_table_schema,
    const share::schema::ObTablegroupSchema& inc_tablegroup_schema, const int64_t schema_version,
    share::schema::AlterTableSchema& alter_table_schema, obrpc::ObCreateTableMode create_mode,
    common::ObMySQLTransaction& client)
{
  int ret = OB_SUCCESS;
  ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
  alter_table_schema.get_part_option().set_partition_cnt_within_partition_table(
      orig_table_schema.get_part_option().get_partition_cnt_within_partition_table());
  alter_table_schema.set_binding(orig_table_schema.get_binding());
  if (!inc_tablegroup_schema.get_binding()) {
    // Need to put the pkey to pgkey mapping in alter_table_schema, so add the sub-partition
  } else if (OB_FAIL(alter_table_schema.try_assign_def_subpart_array(orig_table_schema))) {
    LOG_WARN("failed to assign sub partition", K(ret), K(orig_table_schema));
  } else if (OB_FAIL(set_add_partition_part_id(orig_table_schema, alter_table_schema))) {
    LOG_WARN("failed to set part id", K(ret), K(orig_table_schema), K(alter_table_schema));
  } else if (OB_FAIL(set_mapping_pg_part_id(inc_tablegroup_schema, alter_table_schema))) {
    LOG_WARN("failed to set map pg part id", K(ret), K(inc_tablegroup_schema), K(alter_table_schema));
  } else if (PARTITION_LEVEL_TWO == inc_tablegroup_schema.get_part_level()) {
    if (OB_FAIL(set_mapping_pg_sub_part_id(inc_tablegroup_schema, alter_table_schema))) {
      LOG_WARN("failed to set map pg sub part is", K(ret), K(inc_tablegroup_schema), K(alter_table_schema));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(
                 ddl_operator.add_table_partitions(orig_table_schema, alter_table_schema, schema_version, client))) {
    LOG_WARN("fail to add table partitions", K(ret));
  } else if (inc_tablegroup_schema.get_binding()) {
    // Bind Table's Partition to PG
    alter_table_schema.get_part_option().set_part_num(alter_table_schema.get_partition_num());
    if (OB_FAIL(ddl_service_->binding_add_partitions_for_add(
            schema_version, alter_table_schema, orig_table_schema, inc_tablegroup_schema, create_mode))) {
      LOG_WARN("failed to add binding partition",
          K(ret),
          K(schema_version),
          K(alter_table_schema),
          K(inc_tablegroup_schema));
    }
  } else {
    ObArray<int64_t> new_partition_ids;
    ObPartIdsGeneratorForAdd<ObPartitionSchema> gen(orig_table_schema, alter_table_schema);
    if (OB_FAIL(gen.gen(new_partition_ids))) {
      LOG_WARN("fail to gen new part ids", K(ret), K(new_partition_ids));
    } else if (OB_FAIL(ddl_service_->add_partitions_for_add(
                   schema_version, orig_table_schema, alter_table_schema, new_partition_ids, create_mode))) {
      LOG_WARN("failed to add  partition", K(ret), K(schema_version), K(alter_table_schema), K(inc_tablegroup_schema));
    }
  }
  return ret;
}

// FIXME:Support non-template subpartition
int ObTableGroupHelp::set_add_partition_part_id(
    const share::schema::ObPartitionSchema& orig_schema, share::schema::ObPartitionSchema& alter_schema)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> new_partition_ids;
  ObPartIdsGeneratorForAdd<ObPartitionSchema> gen(orig_schema, alter_schema);
  // alter_schema may not have sub-partition information
  const int64_t subpart_num = orig_schema.get_sub_part_option().get_part_num();
  const int64_t partition_num = alter_schema.get_partition_num() * subpart_num;
  if (OB_FAIL(gen.gen(new_partition_ids))) {
    LOG_WARN("fail to gen new part ids", K(ret), K(new_partition_ids));
  } else if (new_partition_ids.count() != partition_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition count not match", K(new_partition_ids), K(alter_schema));
  } else {
    ObPartition** part_array = alter_schema.get_part_array();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part array is null", K(ret), K(alter_schema));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_partition_ids.count(); i += subpart_num) {
      ObPartition* part = part_array[i / subpart_num];
      const int64_t partition_id = new_partition_ids.at(i);
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part is null", K(ret), K(i));
      } else if (is_twopart(partition_id)) {
        part->set_part_id(extract_part_idx(partition_id));
      } else {
        part->set_part_id(partition_id);
      }
    }
  }
  return ret;
}

int ObTableGroupHelp::update_max_part_id_if_needed(ObDDLService& ddl_service, ObSchemaGetterGuard& schema_guard,
    const ObTablegroupSchema& tablegroup_schema, const ObAlterTablegroupArg& arg)
{
  INIT_SUCC(ret);
  ObDDLSQLTransaction trans(schema_service_);
  const uint64_t tenant_id = tablegroup_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (!tablegroup_schema.is_valid() || !arg.is_valid() || OB_ISNULL(schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_service_));
  } else if (!arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::ADD_PARTITION) &&
             !arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::PARTITIONED_TABLE) &&
             !arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::REORGANIZE_PARTITION) &&
             !arg.alter_option_bitset_.has_member(ObAlterTablegroupArg::SPLIT_PARTITION)) {
    // nothing todo
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else if (OB_FAIL(ddl_operator.batch_update_max_used_part_id(
                   trans, schema_guard, new_schema_version, tablegroup_schema, arg))) {
      LOG_WARN("fail to batch update max used part id", K(ret));
    }
  }
  if (trans.is_started()) {
    bool is_commit = (OB_SUCCESS == ret);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(is_commit))) {
      LOG_WARN("fail to end trans", K(ret), K(tmp_ret), K(is_commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = ddl_service.publish_schema(tenant_id))) {
    LOG_WARN("fail to public schema", K(ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

// When force drop tablegroup partition, you need to force drop all corresponding table
// partition of the tablegroup in the same transaction.
int ObTableGroupHelp::batch_force_drop_table_parts(const uint64_t tenant_id,
    share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTablegroupSchema* orig_tablegroup_schema,
    const share::schema::ObTablegroupSchema& alter_tablegroup_schema, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema*> table_schemas;
  ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);

  if (OB_ISNULL(orig_tablegroup_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_tablegroup_schema is null", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(
                 tenant_id, orig_tablegroup_schema->get_tablegroup_id(), table_schemas))) {
    LOG_WARN("fail to get table schemas in tablegroup",
        K(ret),
        K(tenant_id),
        K(orig_tablegroup_schema->get_tablegroup_id()));
  } else {
    const int64_t part_num = alter_tablegroup_schema.get_part_option().get_part_num();
    ObPartition** part_array = alter_tablegroup_schema.get_part_array();
    const ObTableSchema* orig_table_schema = NULL;
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {  // outer for
      orig_table_schema = table_schemas.at(i);
      ObSArray<int64_t> part_ids_for_delay_delete_part;
      for (int64_t j = 0; OB_SUCC(ret) && j < part_num; ++j) {  // inner for
        ObPartition* alter_part = part_array[j];
        found = false;
        ObDroppedPartIterator iter(*orig_table_schema);
        const ObPartition* table_part = NULL;
        while (OB_SUCC(ret) && !found && OB_SUCC(iter.next(table_part))) {
          if (OB_FAIL(ObDDLOperator::check_part_equal(
                  orig_table_schema->get_part_option().get_part_func_type(), table_part, alter_part, found))) {
            LOG_WARN("check_part_equal failed", K(ret));
          } else if (found == true) {
            if (OB_FAIL(part_ids_for_delay_delete_part.push_back(table_part->get_part_id()))) {
              LOG_WARN("fail to push back to part_ids_for_delay_delete_part", K(ret));
            }
          }
        }                          // end while
        if (OB_ITER_END == ret) {  // !found
          ret = OB_ERR_DROP_PARTITION_NON_EXISTENT;
          LOG_WARN("partition to be dropped not exist", K(ret));
        }
      }  // end inner for
      if (OB_SUCC(ret)) {
        ObTableSchema alter_table_schema;
        if (OB_FAIL(ddl_operator.add_dropped_part_to_partition_schema(
                part_ids_for_delay_delete_part, *orig_table_schema, alter_table_schema))) {
          LOG_WARN("fail to add dropped part to partition schema", K(ret));
        } else if (OB_FAIL(ddl_operator.drop_table_partitions_for_inspection(
                       *orig_table_schema, alter_table_schema, trans))) {
          LOG_WARN("failed to drop table partitions for inspection", K(ret));
        }
      }
    }  // end outer for
  }

  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
