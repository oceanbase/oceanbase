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

#include "ob_root_inspection.h"

#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_zone_info.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/system_variable/ob_system_variable_init.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_root_service.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "share/schema/ob_table_iter.h"
#include "share/ob_primary_zone_util.h"
#include "share/ob_upgrade_utils.h"
#include "share/rc/ob_context.h"
#include "share/schema/ob_schema_mgr.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
using namespace sql;
namespace rootserver {
////////////////////////////////////////////////////////////////
int ObTenantChecker::inspect(bool& passed, const char*& warning_info)
{
  int ret = OB_SUCCESS;
  // always passed
  passed = true;
  UNUSED(warning_info);
  if (OB_FAIL(alter_tenant_primary_zone())) {
    LOG_WARN("fail to alter tenant primary_zone", K(ret));
  } else if (OB_FAIL(check_create_tenant_end())) {
    LOG_WARN("fail to check create tenant end", K(ret));
  }
  return ret;
}

// If the primary_zone of tenant is null, need to set to 'RANDOM'
int ObTenantChecker::alter_tenant_primary_zone()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else {
    const ObTenantSchema* tenant_schema = NULL;
    int64_t affected_rows = 0;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCCESS == ret)
    {
      if (OB_ISNULL(tenant_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_id is null", K(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_info(*tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", K(ret), K(*tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema is null", K(ret), K(*tenant_id));
      } else if (tenant_schema->get_primary_zone().empty()) {
        ObSqlString sql;
        if (OB_FAIL(sql.append_fmt("ALTER TENANT %s set primary_zone = RANDOM", tenant_schema->get_tenant_name()))) {
          LOG_WARN("fail to generate sql", K(ret), K(*tenant_id));
        } else if (OB_ISNULL(sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sql_proxy is null", K(ret));
        } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("inspector",
              "alter_tenant_primary_zone",
              "tenant_id",
              tenant_schema->get_tenant_id(),
              "tenant",
              tenant_schema->get_tenant_name());
        }
      }
    }
  }
  return ret;
}

int ObTenantChecker::check_create_tenant_end()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service not init", K(ret));
  } else if (!schema_service_->is_sys_full_schema()) {
    // skip
  } else if (GCTX.is_standby_cluster()) {
    // skip
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else {
    const ObSimpleTenantSchema* tenant_schema = NULL;
    int64_t schema_version = OB_INVALID_VERSION;
    FOREACH_CNT(tenant_id, tenant_ids)
    {
      // overwrite ret
      if (OB_FAIL(schema_guard.get_schema_version(*tenant_id, schema_version))) {
        LOG_WARN("fail to get tenant schema version", K(ret), K(*tenant_id));
      } else if (schema_version <= OB_CORE_SCHEMA_VERSION) {
        // create tenant failed or schema is old, try again
      } else if (OB_FAIL(schema_guard.get_tenant_info(*tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", K(ret), K(*tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant not exist", K(ret), K(*tenant_id));
      } else if (tenant_schema->is_creating()) {
        obrpc::ObCreateTenantEndArg arg;
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        arg.tenant_id_ = *tenant_id;
        if (OB_FAIL(rpc_proxy_.create_tenant_end(arg))) {
          LOG_WARN("fail to execute create tenant end", K(ret), K(*tenant_id));
        } else {
          LOG_INFO("execute create_tenant_end", K(ret), K(*tenant_id), K(schema_version));
          ROOTSERVICE_EVENT_ADD(
              "inspector", "tenant_checker", "info", "execute create_tenant_end", "tenant_id", *tenant_id);
        }
      }
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////
ObTableGroupChecker::ObTableGroupChecker(share::schema::ObMultiVersionSchemaService& schema_service)
    : schema_service_(schema_service),
      check_primary_zone_map_(),
      check_locality_map_(),
      check_part_option_map_(),
      primary_zone_not_match_set_(),
      locality_not_match_set_(),
      part_option_not_match_set_(),
      is_inited_(false)
{}

int ObTableGroupChecker::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(check_primary_zone_map_.create(TABLEGROUP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_TABLEGROUP_MAP))) {
    LOG_WARN("init check_primary_zone_map failed", K(ret));
  } else if (OB_FAIL(check_locality_map_.create(TABLEGROUP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_TABLEGROUP_MAP))) {
    LOG_WARN("init check_locality_map failed", K(ret));
  } else if (OB_FAIL(check_part_option_map_.create(TABLEGROUP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_TABLEGROUP_MAP))) {
    LOG_WARN("init check_part_option_map failed", K(ret));
  } else if (OB_FAIL(primary_zone_not_match_set_.create(TABLEGROUP_BUCKET_NUM))) {
    LOG_WARN("init primary_zone_not_match_set failed", K(ret));
  } else if (OB_FAIL(locality_not_match_set_.create(TABLEGROUP_BUCKET_NUM))) {
    LOG_WARN("init locality_not_match_set failed", K(ret));
  } else if (OB_FAIL(part_option_not_match_set_.create(TABLEGROUP_BUCKET_NUM))) {
    LOG_WARN("init part_option_not_match_set failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObTableGroupChecker::~ObTableGroupChecker()
{}

int ObTableGroupChecker::inspect(bool& passed, const char*& warning_info)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObTableIterator iter;
  passed = true;
  check_primary_zone_map_.reuse();
  check_part_option_map_.reuse();
  primary_zone_not_match_set_.reuse();
  part_option_not_match_set_.reuse();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablegroup checker is not init", K(ret));
  } else if (OB_FAIL(schema_service_.get_schema_guard(schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(iter.init(&schema_guard))) {
    LOG_WARN("init table iterator failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      uint64_t table_id = OB_INVALID_ID;
      if (OB_FAIL(iter.next(table_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter table failed", K(ret));
        }
      } else {
        const ObTableSchema* table = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(table_id, table))) {
          LOG_WARN("get table schema failed", K(ret), KT(table_id));
        } else if (NULL == table) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(table), K(table_id));
        } else {
          FETCH_ENTITY(TENANT_SPACE, table->get_tenant_id())
          {
            if (is_sys_table(table->get_table_id()) || !table->has_partition()) {
              // skip, check the partitioned user table
            } else if (OB_FAIL(check_primary_zone(*table, schema_guard))) {
              LOG_WARN("check table primary_zone fail", K(table));
            } else if (OB_FAIL(check_locality(*table, schema_guard))) {
              LOG_WARN("check table locality fail", K(table));
            } else if (OB_FAIL(check_part_option(*table, schema_guard))) {
              LOG_WARN("check part option fail", KPC(table));
            }
          }
          else
          {
            LOG_WARN("failed to switch tenant", K(ret), "schema", *table);
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (primary_zone_not_match_set_.size() > 0) {
      LOG_WARN("tables primary_zone in one tablegroup are not the same", K_(primary_zone_not_match_set));
      ROOTSERVICE_EVENT_ADD("inspector", "check_primary_zone", K_(primary_zone_not_match_set));
    }
    if (locality_not_match_set_.size() > 0) {
      LOG_WARN("tables locality in one tablegroup are not the same", K_(locality_not_match_set));
      ROOTSERVICE_EVENT_ADD("inspector", "check_locality", K_(locality_not_match_set));
    }
    if (part_option_not_match_set_.size() > 0) {
      LOG_WARN("tables part option in one tablegroup are not the same", K_(part_option_not_match_set));
      ROOTSERVICE_EVENT_ADD("inspector", "check_part_option", K_(part_option_not_match_set));
    }
    if (primary_zone_not_match_set_.size() > 0 || locality_not_match_set_.size() > 0 ||
        part_option_not_match_set_.size() > 0) {
      passed = false;
      warning_info = "tablegroup has tables that have different primary_zone/locality/part_option";
    }
  }
  return ret;
}

// Check the primary_zone of tables in the same tablegroup:
// 1. For tablegroups created before 2.0, the primary_zone tables in tablegroup should be same.
// 2. For tablegroups created after 2.0, the primary_zone tables in tablegroup should be empty, representing inheritance
// semantics
int ObTableGroupChecker::check_primary_zone(const ObTableSchema& table, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = table.get_tablegroup_id();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablegroup checker is not init", K(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    // skip check if tablegroup_id is default value
  } else {
    bool is_matched = true;
    if (is_new_tablegroup_id(tablegroup_id)) {
      is_matched = table.get_primary_zone().empty();
    } else {
      const ObTableSchema* table_in_map = NULL;
      int tmp_ret = OB_SUCCESS;
      if (OB_HASH_NOT_EXIST != (tmp_ret = primary_zone_not_match_set_.exist_refactored(tablegroup_id))) {
        // already in primary_zone_not_match_set_, skip check
        if (OB_HASH_EXIST != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to check if tablegroup_id exist", K(ret), K(tablegroup_id));
        }
      } else if (OB_FAIL(check_primary_zone_map_.get_refactored(tablegroup_id, table_in_map))) {
        // not in check_primary_zone_map_, set it in the map
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(check_primary_zone_map_.set_refactored(tablegroup_id, &table))) {
            LOG_WARN("set table_schema in hashmap fail", K(ret), K(tablegroup_id), K(table));
          }
        } else {
          LOG_WARN("check tablegroup_id in hashmap fail", K(ret), K(tablegroup_id));
        }
      } else if (OB_ISNULL(table_in_map)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_in_map ptr is null", K(ret));
      } else if (OB_FAIL(ObPrimaryZoneUtil::check_primary_zone_equal(schema_guard, *table_in_map, table, is_matched))) {
        LOG_WARN("check primary_zone_array failed", K(ret), KPC(table_in_map), K(table));
      }
    }
    if (OB_FAIL(ret) || is_matched) {
      // skip
    } else if (OB_FAIL(primary_zone_not_match_set_.set_refactored(tablegroup_id))) {
      // set to primary_zone_not_match_set_ while primary_zone not equal
      LOG_WARN("set tablegroup_id in hashset fail", K(ret));
    } else {
      LOG_INFO("tables in one tablegroup have different primary_zone",
          K(ret),
          K(tablegroup_id),
          "table_id",
          table.get_table_id());
    }
  }
  return ret;
}

// Check the locality of tables in the same tablegroup:
// 1. For tablegroups created before 2.0, the locality tables in tablegroup should be same.
// 2. For tablegroups created after 2.0, the locality tables in tablegroup should be empty, representing inheritance
// semantics
int ObTableGroupChecker::check_locality(const ObTableSchema& table, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = table.get_tablegroup_id();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablegroup checker is not init", K(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    // skip check if tablegroup_id is default value
  } else {
    bool is_matched = true;
    if (is_new_tablegroup_id(tablegroup_id)) {
      is_matched = table.get_locality_str().empty();
    } else {
      const ObTableSchema* table_in_map = NULL;
      int tmp_ret = OB_SUCCESS;
      if (OB_HASH_NOT_EXIST != (tmp_ret = locality_not_match_set_.exist_refactored(tablegroup_id))) {
        // skip check if already in locality_not_match_set_
        if (OB_HASH_EXIST != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to check if tablegroup_id exist", K(ret), K(tablegroup_id));
        }
      } else if (OB_FAIL(check_locality_map_.get_refactored(tablegroup_id, table_in_map))) {
        // set to the map while not in check_locality_map_
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(check_locality_map_.set_refactored(tablegroup_id, &table))) {
            LOG_WARN("set table_schema in hashmap fail", K(ret), K(tablegroup_id), K(table));
          }
        } else {
          LOG_WARN("check tablegroup_id in hashmap fail", K(ret), K(tablegroup_id));
        }
      } else if (OB_ISNULL(table_in_map)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_in_map ptr is null", K(ret));
      } else if (OB_FAIL(
                     ObLocalityUtil::check_locality_completed_match(schema_guard, *table_in_map, table, is_matched))) {
        LOG_WARN("check locality_array failed", K(ret), KPC(table_in_map), K(table));
      }
    }
    if (OB_FAIL(ret) || is_matched) {
      // skip
    } else if (OB_FAIL(locality_not_match_set_.set_refactored(tablegroup_id))) {
      // set to locality_not_match_set_ while locality not equal
      LOG_WARN("set tablegroup_id in hashset fail", K(ret));
    } else {
      LOG_INFO("tables in one tablegroup have different locality",
          K(ret),
          K(tablegroup_id),
          "table_id",
          table.get_table_id());
    }
  }
  return ret;
}

// Check the partition_option of tables in the same tablegroup:
// 1. For tablegroups created before 2.0, the part_type and part_num of tables in tablegroup should be same.
// 2. For tablegroups created after 2.0:
//    1) tablegroup is nonpartition, Allow "non partitioned table" or "partitioned table with 1 number of partitions" in
//    tablegroup.
//       in addition, the partition_num, partition_type, partition_value and number of expression vectors of tables must
//       be same.
//    2) tablegroup is partitioned, the partition_num, partition_type, partition_value and number of expression vectors
//       of tables in tablegroup should be same.
int ObTableGroupChecker::check_part_option(const ObTableSchema& table, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tablegroup_id = table.get_tablegroup_id();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablegroup checker is not init", K(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    // skip check while tablegroup_id is default value
  } else if (!table.is_user_table()) {
    // only check user table
  } else if (OB_HASH_NOT_EXIST != (tmp_ret = part_option_not_match_set_.exist_refactored(tablegroup_id))) {
    // skip check while already in part_option_not_match_set_
    if (OB_HASH_EXIST != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("fail to check if tablegroup_id exist", K(ret), K(tablegroup_id));
    }
  } else {
    const ObTableSchema* table_in_map;
    bool is_matched = true;
    if (!is_new_tablegroup_id(tablegroup_id)) {
      if (OB_FAIL(check_part_option_map_.get_refactored(tablegroup_id, table_in_map))) {
        // set into map while not in check_part_option_map_
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(check_part_option_map_.set_refactored(tablegroup_id, &table))) {
            LOG_WARN("set table_schema in hashmap fail", K(ret), K(tablegroup_id), K(table));
          }
        } else {
          LOG_WARN("check tablegroup_id in hashmap fail", K(ret), K(tablegroup_id));
        }
      } else if (OB_ISNULL(table_in_map)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_in_map is null ptr", K(ret));
      } else if (OB_FAIL(check_if_part_option_equal(*table_in_map, table, is_matched))) {
        LOG_WARN("check if part option equal fail", K(ret), KPC(table_in_map), K(table));
      }
    } else {
      const ObTablegroupSchema* tablegroup = NULL;
      if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), KT(tablegroup_id));
      } else if (OB_ISNULL(tablegroup)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup schema is null", K(ret), KT(tablegroup_id));
      } else if (PARTITION_LEVEL_ZERO == tablegroup->get_part_level()) {
        if (PARTITION_LEVEL_ZERO == table.get_part_level()) {
          // no need to check partition_option of nonpartitioned table
        } else if (1 != table.get_all_part_num()) {
          is_matched = false;
        } else if (OB_FAIL(check_part_option_map_.get_refactored(tablegroup_id, table_in_map))) {
          // set to the map while not in check_part_option_map_
          if (OB_HASH_NOT_EXIST == ret) {
            if (OB_FAIL(check_part_option_map_.set_refactored(tablegroup_id, &table))) {
              LOG_WARN("set table_schema in hashmap fail", K(ret), K(tablegroup_id), K(table));
            }
          } else {
            LOG_WARN("check tablegroup_id in hashmap fail", K(ret), K(tablegroup_id));
          }
        } else if (OB_FAIL(check_if_part_option_equal_v2(table, *table_in_map, is_matched))) {
          LOG_WARN("check if part option equal fail", K(ret), KPC(table_in_map), K(table));
        }
      } else {
        if (OB_FAIL(check_if_part_option_equal_v2(table, *tablegroup, is_matched))) {
          LOG_WARN("check if part option equal fail", K(ret), KPC(tablegroup), K(table));
        }
      }
    }
    if (OB_FAIL(ret) || is_matched) {
      // skip
    } else if (OB_FAIL(part_option_not_match_set_.set_refactored(tablegroup_id))) {
      LOG_WARN("set tablegroup_id in hashset fail", K(ret));
    } else {
      LOG_INFO("tables in one tablegroup have different part/subpart option",
          K(tablegroup_id),
          "table_id",
          table.get_table_id());
    }
  }
  return ret;
}

// For tablegroups created before 2.0, partition_type and partition_num of tables in tablegroup should be equal
int ObTableGroupChecker::check_if_part_option_equal(const ObTableSchema& t1, const ObTableSchema& t2, bool& is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablegroup checker is not init", K(ret));
  } else {
    is_matched = t1.get_part_level() == t2.get_part_level() &&
                 t1.get_part_option().get_part_func_type() == t2.get_part_option().get_part_func_type() &&
                 t1.get_part_option().get_part_num() == t2.get_part_option().get_part_num() &&
                 t1.get_sub_part_option().get_part_func_type() == t2.get_sub_part_option().get_part_func_type() &&
                 t1.get_sub_part_option().get_part_num() == t2.get_sub_part_option().get_part_num();
    if (!is_matched) {
      LOG_ERROR("tables in one tablegroup have different part/subpart option",
          "table1_id",
          t1.get_table_id(),
          "table1_part_level",
          t1.get_part_level(),
          "table1_database_id",
          t1.get_database_id(),
          "table1_tenant_id",
          t1.get_tenant_id(),
          "table1_part_option",
          t1.get_part_option(),
          "table1_subpart_option",
          t1.get_sub_part_option(),
          "table2_id",
          t2.get_table_id(),
          "table2_part_level",
          t2.get_part_level(),
          "table2_database_id",
          t2.get_database_id(),
          "table2_tenant_id",
          t2.get_tenant_id(),
          "table2_part_option",
          t2.get_part_option(),
          "table2_subpart_option",
          t2.get_sub_part_option());
    }
  }
  return ret;
}

// For tablegroup create after 2.0, the check of partition_option is more strict.
// You need to check the partition mode, the number of partitions, the number of expression vectors and the value of the
// partition point
// FIXME:Support non templated subpartition
template <typename SCHEMA>
int ObTableGroupChecker::check_if_part_option_equal_v2(
    const ObTableSchema& table, const SCHEMA& schema, bool& is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablegroup checker is not init", K(ret));
  } else {
    is_matched =
        table.get_part_level() == schema.get_part_level() &&
        (table.get_part_option().get_part_func_type() == schema.get_part_option().get_part_func_type() ||
            (is_key_part(table.get_part_option().get_part_func_type()) &&
                is_key_part(schema.get_part_option().get_part_func_type()))) &&
        table.get_part_option().get_part_num() == schema.get_part_option().get_part_num() &&
        (table.get_sub_part_option().get_part_func_type() == schema.get_sub_part_option().get_part_func_type() ||
            (is_key_part(table.get_sub_part_option().get_part_func_type()) &&
                is_key_part(schema.get_sub_part_option().get_part_func_type()))) &&
        table.get_sub_part_option().get_part_num() == schema.get_sub_part_option().get_part_num();
    // check expr value
    if (!is_matched) {
      // skip
    } else if (PARTITION_LEVEL_ONE <= table.get_part_level()) {
      if (table.is_key_part()) {
        int64_t table_expr_num = OB_INVALID_INDEX;
        int64_t schema_expr_num = OB_INVALID_INDEX;
        if (OB_FAIL(table.calc_part_func_expr_num(table_expr_num))) {
          LOG_WARN("fail to get table part_func_expr_num", K(ret));
        } else if (OB_FAIL(schema.calc_part_func_expr_num(schema_expr_num))) {
          LOG_WARN("fail to get schema part_func_expr_num", K(ret));
        }
        is_matched = (table_expr_num == schema_expr_num);
      } else if (table.is_range_part() || table.is_list_part()) {
        if (OB_ISNULL(table.get_part_array()) || OB_ISNULL(schema.get_part_array())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition_array is null", K(ret), K(schema), K(table));
        } else {
          is_matched = true;
          const int64_t table_part_num = table.get_part_option().get_part_num();
          const int64_t schema_part_num = schema.get_part_option().get_part_num();
          const ObPartitionFuncType part_func_type = table.get_part_option().get_part_func_type();
          for (int64_t i = 0; OB_SUCC(ret) && is_matched && i < table_part_num; i++) {
            is_matched = false;
            ObPartition* table_part = table.get_part_array()[i];
            for (int64_t j = 0; OB_SUCC(ret) && !is_matched && j < schema_part_num; j++) {
              ObPartition* schema_part = schema.get_part_array()[j];
              if (OB_ISNULL(schema_part) || OB_ISNULL(table_part)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("partition is null", K(ret), KPC(schema_part), KPC(table_part));
              } else if (OB_FAIL(ObPartitionUtils::check_partition_value(
                             *schema_part, *table_part, part_func_type, is_matched))) {
                LOG_WARN("fail to check partition value", KPC(schema_part), KPC(table_part), K(part_func_type));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) || !is_matched) {
      // skip
    } else if (PARTITION_LEVEL_TWO == table.get_part_level()) {
      if (table.is_key_subpart()) {
        int64_t table_expr_num = OB_INVALID_INDEX;
        int64_t schema_expr_num = OB_INVALID_INDEX;
        if (OB_FAIL(table.calc_subpart_func_expr_num(table_expr_num))) {
          LOG_WARN("fail to get table subpart_func_expr_num", K(ret));
        } else if (OB_FAIL(schema.calc_subpart_func_expr_num(schema_expr_num))) {
          LOG_WARN("fail to get schema subpart_func_expr_num", K(ret));
        }
        is_matched = (table_expr_num == schema_expr_num);
      } else if (table.is_range_subpart() || table.is_list_subpart()) {
        if (OB_ISNULL(table.get_def_subpart_array()) || OB_ISNULL(schema.get_def_subpart_array())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition_array is null", K(ret), K(schema), K(table));
        } else {
          is_matched = true;
          const int64_t table_sub_part_num = table.get_sub_part_option().get_part_num();
          const int64_t schema_sub_part_num = schema.get_sub_part_option().get_part_num();
          const ObPartitionFuncType part_func_type = table.get_sub_part_option().get_part_func_type();
          for (int64_t i = 0; OB_SUCC(ret) && is_matched && i < table_sub_part_num; i++) {
            is_matched = false;
            ObSubPartition* table_part = table.get_def_subpart_array()[i];
            for (int64_t j = 0; OB_SUCC(ret) && !is_matched && j < schema_sub_part_num; j++) {
              ObSubPartition* schema_part = schema.get_def_subpart_array()[j];
              if (OB_ISNULL(schema_part) || OB_ISNULL(table_part)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("partition is null", K(ret), KPC(schema_part), KPC(table_part));
              } else if (OB_FAIL(ObPartitionUtils::check_partition_value(
                             *schema_part, *table_part, part_func_type, is_matched))) {
                LOG_WARN("fail to check partition value", KPC(schema_part), KPC(table_part), K(part_func_type));
              }
            }
          }
        }
      }
    }
  }
  if (!is_matched) {
    LOG_ERROR("table & tablegroup partition option not match", K(ret), K(table), K(schema));
  }
  return ret;
}
////////////////////////////////////////////////////////////////
/*
 * There are two operations in this inspection:
 * 1. Check whether there is a tenant whose transaction 2 fails. It needs to give an alarm and delete the tenant
 * manually
 * 2. The real drop tenant operation needs to be triggered by RS because of delay of drop tenant.
 */
int ObDropTenantChecker::inspect(bool& passed, const char*& warning_info)
{
  int ret = OB_SUCCESS;
  passed = true;
  if (!schema_service_.is_sys_full_schema()) {
    // skip
  } else if (GCTX.is_standby_cluster()) {
    // 1. In standby cluster, As long as transaction one is successful, transaction two does not need to be executed,
    //    and there is no case that tenant creation fails and drop tenant is needed.
    // 2. Even though tenant is delay dropped, Standby synchronizes OB_DDL_DEL_TENANT through DDL,
    //    no need to trigger drop_tenant by inspection.
  } else {
    obrpc::ObGetSchemaArg arg;
    obrpc::ObTenantSchemaVersions result;
    ObSchemaGetterGuard schema_guard;
    arg.ignore_fail_ = true;
    arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
    if (OB_FAIL(rpc_proxy_.get_tenant_schema_versions(arg, result))) {
      LOG_WARN("fail to get tenant schema versions", K(ret));
    } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    }
    // 1. drop garbage tenant(failed to create tenant)
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < result.tenant_schema_versions_.count(); i++) {
        TenantIdAndSchemaVersion& tenant = result.tenant_schema_versions_.at(i);
        int tmp_ret = OB_SUCCESS;
        if (tenant.schema_version_ == OB_CORE_SCHEMA_VERSION) {
          const ObSimpleTenantSchema* tenant_schema = NULL;
          uint64_t tenant_id = tenant.tenant_id_;
          if (OB_SUCCESS != (tmp_ret = schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
            LOG_WARN("fail to get tenant info", K(tmp_ret), K(tenant_id));
          } else if (OB_ISNULL(tenant_schema)) {
            tmp_ret = OB_TENANT_NOT_EXIST;
          } else {
            LOG_ERROR("tenant maybe create failed", K(tenant_id));
            ROOTSERVICE_EVENT_ADD("inspector",
                "drop_tenant_checker",
                "info",
                "tenant maybe create failed",
                "tenant_id",
                tenant_id,
                "tenant_name",
                tenant_schema->get_tenant_name_str());
          }
          if (!passed && OB_SUCCESS != tmp_ret) {
            passed = false;
          }
        }
      }
    }
    // 2. drop tenant in delay dropped
    if (OB_SUCC(ret)) {
      ObArray<uint64_t> tenant_ids;
      if (OB_FAIL(schema_service_.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
        LOG_WARN("get_schema_guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
        LOG_WARN("get_tenant_ids failed", K(ret));
      } else {
        const ObSimpleTenantSchema* tenant_schema = NULL;
        FOREACH(tenant_id, tenant_ids)
        {
          int tmp_ret = OB_SUCCESS;
          if (OB_ISNULL(tenant_id)) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant is null", K(tmp_ret));
          } else if (OB_SUCCESS != (tmp_ret = schema_guard.get_tenant_info(*tenant_id, tenant_schema))) {
            LOG_WARN("fail to get tenant schema", K(tmp_ret), K(*tenant_id));
          } else if (OB_ISNULL(tenant_schema)) {
            tmp_ret = OB_TENANT_NOT_EXIST;
          } else if (tenant_schema->is_dropping()) {
            int64_t drop_tenant_time = tenant_schema->get_drop_tenant_time();
            int64_t drop_schema_version = tenant_schema->get_schema_version();
            int64_t current_time = ObTimeUtility::current_time();
            const ObString& tenant_name = tenant_schema->get_tenant_name_str();
            bool is_delay_delete = false;
            if (drop_tenant_time + GCONF.schema_history_expire_time > current_time) {
              // skip
            } else if (OB_SUCCESS != (tmp_ret = record_log_archive_history_(
                                          *tenant_id, drop_schema_version, drop_tenant_time, is_delay_delete))) {
              LOG_WARN("failed to record log archive history", K(tmp_ret), K(*tenant_id), K(tenant_name));
            } else if (is_delay_delete) {
              LOG_INFO("log archive not backup, try later", K(*tenant_id), K(tenant_name));
            } else if (OB_SUCCESS != (tmp_ret = drop_tenant_force(tenant_name))) {
              LOG_WARN("fail to drop tenant force", K(tmp_ret), K(tenant_name));
            } else {
              LOG_INFO("drop tenant which reach expire_time",
                  K(*tenant_id),
                  K(tenant_name),
                  K(drop_tenant_time),
                  K(drop_schema_version));
              ROOTSERVICE_EVENT_ADD("inspector",
                  "drop_tenant_checker",
                  "info",
                  "drop tenant which reach expire time",
                  "tenant_id",
                  *tenant_id,
                  "tenant_name",
                  tenant_name);
            }
            if (!passed && OB_SUCCESS != tmp_ret) {
              passed = false;
            }
          }
        }  // end for
      }
    }
    if (!passed) {
      warning_info = "check drop tenant failed";
    }
  }
  return ret;
}

int ObDropTenantChecker::record_log_archive_history_(
    const uint64_t tenant_id, const int64_t drop_schema_version, const int64_t drop_tenant_time, bool& is_delay_delete)
{
  int ret = OB_SUCCESS;
  // In theory, the deleted log does not need to be backed up.
  // Here, it is reserved for 60s
  int64_t safe_drop_time = drop_tenant_time + 60 * 1000 * 1000;
  int64_t checkpoint_ts = 0;
  int64_t reserved_schema_version = OB_INVALID_VERSION;
  is_delay_delete = true;

  if (OB_FAIL(ObBackupInfoMgr::get_instance().get_delay_delete_schema_version(
          OB_SYS_TENANT_ID, schema_service_, is_delay_delete, reserved_schema_version))) {
    LOG_WARN("fail to get delay delete schema_version", K(ret));
  } else if (is_delay_delete && reserved_schema_version < drop_schema_version && 0 != reserved_schema_version) {
    LOG_INFO("not backup yet, try later", K(ret), K(reserved_schema_version), K(drop_schema_version));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_checkpoint(checkpoint_ts))) {
    LOG_WARN("Failed to get log archive checkpoint", K(ret));
  } else if (0 == checkpoint_ts) {
    is_delay_delete = false;
    LOG_INFO("no archive log now", K(tenant_id), K(checkpoint_ts));  // delete log
  } else if (checkpoint_ts < safe_drop_time) {
    is_delay_delete = true;
    LOG_INFO("log archive checkpoint timestamp is not match safe drop time, need wait",
        K(tenant_id),
        K(drop_schema_version),
        K(drop_tenant_time),
        K(safe_drop_time),
        K(checkpoint_ts));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().record_drop_tenant_log_archive_history(tenant_id))) {
    LOG_WARN("failed to record log archive history", K(ret), K(tenant_id));
  } else {
    is_delay_delete = false;
    LOG_INFO("succeed to record_log_archive_history",
        K(tenant_id),
        K(drop_schema_version),
        K(drop_tenant_time),
        K(checkpoint_ts),
        K(is_delay_delete));
  }

  return ret;
}

int ObDropTenantChecker::drop_tenant_force(const ObString& tenant_name)
{
  int ret = OB_SUCCESS;
  obrpc::ObDropTenantArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.tenant_name_ = tenant_name;
  arg.if_exist_ = true;
  arg.delay_to_drop_ = false;
  ObSqlString sql;
  if (OB_FAIL(sql.append_fmt("DROP TENANT IF EXISTS %s FORCE", arg.tenant_name_.ptr()))) {
    LOG_WARN("fail to generate sql", K(ret), K(arg));
  } else if (FALSE_IT(arg.ddl_stmt_str_ = sql.string())) {
  } else if (OB_FAIL(rpc_proxy_.drop_tenant(arg))) {
    LOG_WARN("fail to drop tenant", K(ret), K(arg));
  }
  return ret;
}
////////////////////////////////////////////////////////////////
// if passed = false, Indicates that there may be legacy objects that need to be dropped by force
int ObForceDropSchemaChecker::inspect(bool& passed, const char*& warning_info)
{
  int ret = OB_SUCCESS;
  UNUSED(warning_info);
  passed = true;
  if (GCTX.is_standby_cluster()) {
    // skip
  } else {
    ObArray<uint64_t> tenant_ids;
    {
      ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(schema_service_.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
        LOG_WARN("fail to get tenant_ids", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t task_cnt = 0;
      bool stop = false;
      for (int64_t i = 0; !stop && i < tenant_ids.count(); i++) {  // ignore failure
        uint64_t tenant_id = tenant_ids.at(i);
        int64_t recycle_schema_version = OB_INVALID_VERSION;
        {
          int64_t local_schema_version = OB_INVALID_VERSION;
          const ObSimpleTenantSchema* tenant_schema = NULL;
          ObSchemaGetterGuard schema_guard;
          if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
            LOG_WARN("get schema guard failed", K(ret));
          } else if (OB_FAIL(get_recycle_schema_version(tenant_id, recycle_schema_version))) {
            LOG_WARN("fail to get recycle schema version", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, local_schema_version))) {
            LOG_WARN("fail to get schema version", K(ret), K(tenant_id));
          } else if (!ObSchemaService::is_formal_version(local_schema_version)) {
            ret = OB_EAGAIN;
            LOG_INFO("schema version is not formal, try later", K(ret), K(tenant_id), K(local_schema_version));
          } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
            LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
          } else if (OB_ISNULL(tenant_schema)) {
            ret = OB_TENANT_NOT_EXIST;
            LOG_WARN("tenant not exist", K(ret), K(tenant_id));
          } else if (tenant_schema->is_restore()) {
            // In the process of recovery,
            // the inspection should avoid dropping the schema which is delay dropped
            ret = OB_EAGAIN;
            LOG_INFO("tenant is restore, try later", K(ret), K(tenant_id), K(local_schema_version));
          }
        }
        if (FAILEDx(force_drop_schema(tenant_id, recycle_schema_version, task_cnt))) {
          LOG_WARN("fail to force drop schema", K(ret), K(tenant_id), K(recycle_schema_version), K(task_cnt));
        } else if (passed && task_cnt >= BATCH_DROP_SCHEMA_NUM) {
          // Passed is false, indicating that there are still legacy tasks
          passed = false;
        }
      }
      LOG_INFO("run force drop schema checker", K(ret), K(stop), K(task_cnt));
    }
  }
  return ret;
}

int ObForceDropSchemaChecker::check_dropped_schema_exist(const uint64_t tenant_id, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (OB_FAIL(check_dropped_database_exist(tenant_id, exist))) {
    LOG_WARN("fail to check dropped database exist", K(ret), K(tenant_id));
  } else if (!exist && OB_FAIL(check_dropped_tablegroup_exist(tenant_id, exist))) {
    LOG_WARN("fail to check dropped tablegroup exist", K(ret), K(tenant_id));
  } else if (!exist && OB_FAIL(check_dropped_table_exist(tenant_id, exist))) {
    LOG_WARN("fail to check dropped table exist", K(ret), K(tenant_id));
  }
  return ret;
}

int ObForceDropSchemaChecker::check_dropped_database_exist(const uint64_t tenant_id, bool& exist)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObDatabaseSchema*> schemas;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("fail to get database schemas", K(ret), K(tenant_id));
  } else {
    bool stop = false;
    for (int64_t j = 0; OB_SUCC(ret) && !stop && !exist && j < schemas.count(); j++) {
      const ObDatabaseSchema*& database = schemas.at(j);
      if (OB_ISNULL(database)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database is null", K(ret));
      } else if (database->is_dropped_schema()) {
        exist = true;
        LOG_INFO("dropped database exist", K(ret), "database_id", database->get_database_id());
      }
      stop = !root_service_.is_full_service();
    }
  }
  return ret;
}

int ObForceDropSchemaChecker::check_dropped_tablegroup_exist(const uint64_t tenant_id, bool& exist)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObTablegroupSchema*> schemas;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("fail to get tablegroup schemas", K(ret), K(tenant_id));
  } else {
    bool stop = false;
    for (int64_t j = 0; OB_SUCC(ret) && !stop && !exist && j < schemas.count(); j++) {
      const ObTablegroupSchema*& tablegroup = schemas.at(j);
      if (OB_ISNULL(tablegroup)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup is null", K(ret));
      } else if (!is_new_tablegroup_id(tablegroup->get_tablegroup_id())) {
        // skip
      } else if (tablegroup->is_dropped_schema() || tablegroup->get_dropped_partition_num() > 0) {
        exist = true;
        LOG_INFO("dropped tablegroup or dropped tablegroup partition exist",
            K(ret),
            "tablegroup_id",
            tablegroup->get_tablegroup_id(),
            "dropped_partition_cnt",
            tablegroup->get_dropped_partition_num());
      }
      stop = !root_service_.is_full_service();
    }
  }
  return ret;
}

int ObForceDropSchemaChecker::check_dropped_table_exist(const uint64_t tenant_id, bool& exist)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2*> schemas;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("fail to get table schemas", K(ret), K(tenant_id));
  } else {
    bool stop = false;
    for (int64_t j = 0; OB_SUCC(ret) && !stop && !exist && j < schemas.count(); j++) {
      const ObSimpleTableSchemaV2*& table = schemas.at(j);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null", K(ret));
      } else if (is_inner_table(table->get_table_id())) {
        // skip
      } else if (table->is_dropped_schema() || table->get_dropped_partition_num() > 0) {
        exist = true;
        LOG_INFO("dropped table or dropped table partition exist",
            K(ret),
            "table_id",
            table->get_table_id(),
            "dropped_partition_cnt",
            table->get_dropped_partition_num());
      }
      stop = !root_service_.is_full_service();
    }
  }
  return ret;
}

int ObForceDropSchemaChecker::force_drop_schema(
    const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  // drop schema according to the dependency of schema:
  // 1. data table schema is dropped defore index schema.
  // 2. table schema in tablegroup is dropped before tablegroup schema.
  // 3. table schema in database is dropped before database schema.
  // 4. When dropping a partition, ensure that all tables in tablegroup and the tablegroup partitions should be dropped
  // in a transaction.
  if (OB_FAIL(force_drop_index(tenant_id, recycle_schema_version, task_cnt))) {
    LOG_WARN("fail to drop index", K(ret), K(tenant_id), K(recycle_schema_version), K(task_cnt));
  } else if (OB_FAIL(force_drop_tablegroup_partitions(tenant_id, recycle_schema_version, task_cnt))) {
    LOG_WARN("fail to drop tablegroup partitions", K(ret), K(tenant_id), K(recycle_schema_version), K(task_cnt));
  } else if (OB_FAIL(force_drop_table_and_partitions(tenant_id, recycle_schema_version, task_cnt))) {
    LOG_WARN("fail to drop table and partitions", K(ret), K(tenant_id), K(recycle_schema_version), K(task_cnt));
  } else if (OB_FAIL(force_drop_tablegroup(tenant_id, recycle_schema_version, task_cnt))) {
    LOG_WARN("fail to drop tablegroup", K(ret), K(tenant_id), K(recycle_schema_version), K(task_cnt));
  } else if (OB_FAIL(force_drop_database(tenant_id, recycle_schema_version, task_cnt))) {
    LOG_WARN("fail to drop database", K(ret), K(tenant_id), K(recycle_schema_version), K(task_cnt));
  }
  return ret;
}

int ObForceDropSchemaChecker::force_drop_index(
    const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2*> schemas;
  DropSchemaMode mode = DROP_SCHEMA_ONLY;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("fail to get table schemas", K(ret), K(tenant_id));
  } else {
    bool stop = false;
    for (int64_t j = 0; !stop && j < schemas.count(); j++) {  // ignore failure
      const ObSimpleTableSchemaV2*& table = schemas.at(j);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null", K(ret));
      } else if (is_inner_table(table->get_table_id()) || !table->is_aux_table()) {
        // skip
      } else if (OB_FAIL(try_drop_schema(*table, recycle_schema_version, mode, task_cnt))) {
        LOG_WARN("try drop schema failed", K(ret), K(recycle_schema_version), K(mode), KPC(table));
      }
      stop = !root_service_.is_full_service() || task_cnt >= BATCH_DROP_SCHEMA_NUM;
    }
  }
  return ret;
}

int ObForceDropSchemaChecker::force_drop_tablegroup_partitions(
    const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObTablegroupSchema*> schemas;
  DropSchemaMode mode = DROP_PARTITION_ONLY;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("fail to get tablegroup schemas", K(ret), K(tenant_id));
  } else {
    bool stop = false;
    for (int64_t j = 0; !stop && j < schemas.count(); j++) {  // ignore failure
      const ObTablegroupSchema*& tablegroup = schemas.at(j);
      if (OB_ISNULL(tablegroup)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup is null", K(ret));
      } else if (!is_new_tablegroup_id(tablegroup->get_tablegroup_id())) {
        // skip
      } else if (OB_FAIL(try_drop_schema(*tablegroup, recycle_schema_version, mode, task_cnt))) {
        LOG_WARN("try drop schema failed", K(ret), K(recycle_schema_version), K(mode), KPC(tablegroup));
      }
      stop = !root_service_.is_full_service() || task_cnt >= BATCH_DROP_SCHEMA_NUM;
    }
  }
  return ret;
}

int ObForceDropSchemaChecker::force_drop_table_and_partitions(
    const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2*> schemas;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("fail to get table schemas", K(ret), K(tenant_id));
  } else {
    bool stop = false;
    for (int64_t j = 0; !stop && j < schemas.count(); j++) {  // ignore failure
      const ObSimpleTableSchemaV2*& table = schemas.at(j);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null", K(ret));
      } else if (is_inner_table(table->get_table_id())) {
        // skip
      } else {
        // table in pg should be dropped in force_drop_schema by pg
        DropSchemaMode mode = table->get_binding() ? DROP_SCHEMA_ONLY : DROP_SCHEMA_AND_PARTITION;
        if (OB_FAIL(try_drop_schema(*table, recycle_schema_version, mode, task_cnt))) {
          LOG_WARN("try drop schema failed", K(ret), K(recycle_schema_version), K(mode), KPC(table));
        }
      }
      stop = !root_service_.is_full_service() || task_cnt >= BATCH_DROP_SCHEMA_NUM;
    }
  }
  return ret;
}

int ObForceDropSchemaChecker::force_drop_tablegroup(
    const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObTablegroupSchema*> schemas;
  DropSchemaMode mode = DROP_SCHEMA_ONLY;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("fail to get tablegroup schemas", K(ret), K(tenant_id));
  } else {
    bool stop = false;
    for (int64_t j = 0; !stop && j < schemas.count(); j++) {  // ignore failure
      const ObTablegroupSchema*& tablegroup = schemas.at(j);
      if (OB_ISNULL(tablegroup)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup is null", K(ret));
      } else if (!is_new_tablegroup_id(tablegroup->get_tablegroup_id())) {
        // skip
      } else if (OB_FAIL(try_drop_schema(*tablegroup, recycle_schema_version, mode, task_cnt))) {
        LOG_WARN("try drop schema failed", K(ret), K(recycle_schema_version), K(mode), KPC(tablegroup));
      }
      stop = !root_service_.is_full_service() || task_cnt >= BATCH_DROP_SCHEMA_NUM;
    }
  }
  return ret;
}

int ObForceDropSchemaChecker::force_drop_database(
    const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObDatabaseSchema*> schemas;
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("fail to get database schemas", K(ret), K(tenant_id));
  } else {
    bool stop = false;
    for (int64_t j = 0; !stop && j < schemas.count(); j++) {  // ignore failure
      const ObDatabaseSchema*& database = schemas.at(j);
      if (OB_ISNULL(database)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database is null", K(ret));
      } else {
        obrpc::ObForceDropSchemaArg arg;
        arg.exec_tenant_id_ = database->get_tenant_id();
        arg.schema_id_ = database->get_database_id();
        arg.recycle_schema_version_ = recycle_schema_version;
        arg.type_ = share::schema::DATABASE_SCHEMA;
        bool need_drop = false;
        int64_t drop_schema_version = database->get_drop_schema_version();
        if (database->is_dropped_schema()) {
          need_drop = schema_need_drop(drop_schema_version, recycle_schema_version);
        }
        if (OB_SUCC(ret) && need_drop) {
          if (FAILOVER_MODE == task_mode_) {
            // Failover need clear delayed deleted schema objects. But can not clear by RPC as DDL thread has been
            // occupied.
            if (OB_FAIL(root_service_.force_drop_schema(arg))) {
              LOG_WARN("force_drop_schema failed", KR(ret), K(arg));
            }
          } else if (OB_FAIL(rpc_proxy_.force_drop_schema(arg))) {
            LOG_WARN("force_drop_schema failed", K(ret), K(arg));
          }
          if (OB_SUCC(ret)) {
            LOG_INFO("force drop schema", K(arg));
            task_cnt++;
            usleep(DROP_SCHEMA_IDLE_TIME);
          }
        }
      }
      stop = !root_service_.is_full_service() || task_cnt >= BATCH_DROP_SCHEMA_NUM;
    }
  }
  return ret;
}

bool ObForceDropSchemaChecker::schema_need_drop(const int64_t drop_schema_version, const int64_t recycle_schema_version)
{
  bool bret = false;
  if (drop_schema_version > 0 && (0 == recycle_schema_version /*close backup*/
                                     || drop_schema_version <= recycle_schema_version)) {
    bret = true;
  }
  return bret;
}

int ObForceDropSchemaChecker::get_dropped_partition_ids(const int64_t recycle_schema_version,
    const ObPartitionSchema& schema, ObArray<int64_t>& dropped_partition_ids,
    ObArray<int64_t>& dropped_subpartition_ids)
{
  int ret = OB_SUCCESS;
  uint64_t schema_id = schema.get_table_id();
  if (OB_INVALID_ID == schema_id || recycle_schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(schema_id), K(recycle_schema_version));
  } else {
    // iter dropped partition array
    ObDroppedPartIterator iter(schema);
    const ObPartition* part = NULL;
    while (OB_SUCC(ret) && OB_SUCC(iter.next(part))) {
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part is null", K(ret));
      } else {
        int64_t partition_id = part->get_part_id();
        int64_t drop_schema_version = part->get_drop_schema_version();
        if (schema_need_drop(drop_schema_version, recycle_schema_version)) {
          if (OB_FAIL(dropped_partition_ids.push_back(partition_id))) {
            LOG_WARN("fail to push back", K(ret), K(schema_id), K(partition_id));
          } else {
            LOG_INFO("partition need drop", K(ret), K(schema_id), K(partition_id));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
      LOG_WARN("fail to iter dropped partition_id", K(ret), K(recycle_schema_version), K(schema));
    }
    // iter dropped subpartition array
    if (OB_SUCC(ret) && PARTITION_LEVEL_TWO == schema.get_part_level() && !schema.is_sub_part_template()) {
      // not templated subpartition table may has subpartitions in deply dropped
      bool check_dropped_schema = false;
      ObPartIteratorV2 iter(schema, check_dropped_schema);
      const ObPartition* part = NULL;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(part))) {
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part is null", K(ret));
        } else if (part->get_dropped_subpartition_num() <= 0) {
          // skip
        } else if (OB_ISNULL(part->get_dropped_subpart_array())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dropped subpartition array is null", K(ret), KPC(part));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < part->get_dropped_subpartition_num(); i++) {
            const ObSubPartition* subpart = part->get_dropped_subpart_array()[i];
            if (OB_ISNULL(subpart)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("subpart is null", K(ret), KPC(part));
            } else {
              int64_t subpartition_id = generate_phy_part_id(part->get_part_id(), subpart->get_sub_part_id());
              int64_t drop_schema_version = subpart->get_drop_schema_version();
              if (schema_need_drop(drop_schema_version, recycle_schema_version)) {
                if (OB_FAIL(dropped_subpartition_ids.push_back(subpartition_id))) {
                  LOG_WARN("fail to push back", K(ret), K(schema_id), K(subpartition_id));
                } else {
                  LOG_INFO("subpartition need drop", K(ret), K(schema_id), K(subpartition_id));
                }
              }
            }
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
        LOG_WARN("fail to iter dropped subpartition_id", K(ret), K(recycle_schema_version), K(schema));
      }
    }
  }
  if (OB_SUCC(ret)) {  // default ASC
    std::sort(dropped_partition_ids.begin(), dropped_partition_ids.end());
    std::sort(dropped_subpartition_ids.begin(), dropped_subpartition_ids.end());
  }
  return ret;
}

int ObForceDropSchemaChecker::get_recycle_schema_version(const uint64_t tenant_id, int64_t& recycle_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t reserved_schema_version = OB_INVALID_VERSION;
  bool is_delay_delete = false;
  recycle_schema_version = OB_INVALID_VERSION;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_delay_delete_schema_version(
                 tenant_id, schema_service_, is_delay_delete, reserved_schema_version))) {
    LOG_WARN("fail to get delay delete schema_version", K(ret), K(tenant_id));
  } else if (is_delay_delete) {
    if (reserved_schema_version <= 0) {
      // When the backup is on, but there is no baseline backup, as long as the archive is complete
      //(__all_tenant_gc_partition_info is empty), it can force drop by occupying DDL thread.
      // it is no problem that open baseline backup tasks up DDL thread and use the newest schema_version of tenant
      recycle_schema_version = INT64_MAX;
    } else {
      recycle_schema_version = reserved_schema_version;
    }
  } else {
    recycle_schema_version = 0;
  }
  return ret;
}

// FIXME:() should batch fetch tables' partition_ids
// get partition ids from __all_tenant_gc_partition_info
int ObForceDropSchemaChecker::get_existed_partition_ids(
    const uint64_t tenant_id, const uint64_t table_id, ObArray<int64_t>& partition_ids)
{
  int ret = OB_SUCCESS;
  if (RESTORE_MODE == task_mode_ || FAILOVER_MODE == task_mode_) {
    // force_drop_schema called by physical recovery or failover, is focus on filter delay dropped schema,
    // the force_drop_schema will not consult __all_tenant_gc_partition_info
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(tenant_id), K(table_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult* result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(&sql_proxy_);
      if (OB_FAIL(sql.append_fmt("select partition_id from %s "
                                 "where tenant_id = %ld and table_id = %ld "
                                 "order by tenant_id, table_id, partition_id",
              OB_ALL_TENANT_GC_PARTITION_INFO_TNAME,
              ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
              ObSchemaUtils::get_extract_schema_id(tenant_id, table_id)))) {
        LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(table_id), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", K(ret));
      } else if (OB_FAIL(result->next())) {
        // iter end
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to iter", K(ret));
        }
      } else {
        do {
          int64_t partition_id = OB_INVALID_ID;
          EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", partition_id, int64_t);
          if (OB_FAIL(partition_ids.push_back(partition_id))) {
            LOG_WARN("fail to set gc partition infos", K(ret), K(tenant_id), K(table_id), K(partition_id));
          }
        } while (OB_SUCC(ret) && OB_SUCC(result->next()));

        if (OB_ITER_END == ret) {
          // rewrite ret
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

// force_drop_schema call by physical recovery, try to purge __all_tenant_gc_partition_info.
// for avoid intrusion into existing drop logical, do not do this in force_drop_schema
int ObForceDropSchemaChecker::recycle_gc_partition_info(const obrpc::ObForceDropSchemaArg& arg)
{
  int ret = OB_SUCCESS;
  if (NORMAL_MODE != task_mode_) {
    // skip
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else if (share::schema::TABLEGROUP_SCHEMA == arg.type_ || share::schema::TABLE_SCHEMA == arg.type_) {
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = arg.exec_tenant_id_;
    ObSqlString sql;
    int64_t total_cnt = arg.partition_ids_.count();
    if (total_cnt <= 0) {
      // recycle by table
      if (OB_FAIL(sql.append_fmt("delete from %s where tenant_id = %lu and table_id = %ld",
              OB_ALL_TENANT_GC_PARTITION_INFO_TNAME,
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, exec_tenant_id),
              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, arg.schema_id_)))) {
        LOG_WARN("fail to generate sql", K(ret), K(exec_tenant_id));
      } else if (OB_FAIL(sql_proxy_.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(exec_tenant_id));
      } else {
        // it is an transaction with force_drop_schema.
        // it's guaranteed reentry here, no need to check affected_rows
      }
    } else {
      // recycle by partition
      int64_t start_idx = 0;
      const int64_t BATCH_RECYCLE_CNT = 100;
      while (OB_SUCC(ret) && start_idx < total_cnt) {
        sql.reset();
        if (OB_FAIL(sql.append_fmt("delete from %s where tenant_id = %lu and table_id = %ld and partition_id in (",
                OB_ALL_TENANT_GC_PARTITION_INFO_TNAME,
                ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, exec_tenant_id),
                ObSchemaUtils::get_extract_schema_id(exec_tenant_id, arg.schema_id_)))) {
          LOG_WARN("fail to generate sql", K(ret), K(exec_tenant_id));
        } else {
          int64_t end_idx = min(start_idx + BATCH_RECYCLE_CNT - 1, total_cnt - 1);
          for (int64_t i = start_idx; OB_SUCC(ret) && i <= end_idx; i++) {
            if (OB_FAIL(sql.append_fmt(
                    "%s %ld%s", i == start_idx ? "" : ",", arg.partition_ids_.at(i), i == end_idx ? ")" : ""))) {
              LOG_WARN("fail to append fmt", K(ret), K(i));
            }
          }
          if (FAILEDx(sql_proxy_.write(exec_tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(ret));
          } else {
            // it is an transaction with force_drop_schema.
            // it's guaranteed reentry here, no need to check affected_rows
            start_idx = end_idx + 1;
          }
        }
      }
    }
  }
  return ret;
}

int ObForceDropSchemaChecker::try_drop_schema(const ObPartitionSchema& partition_schema,
    const int64_t recycle_schema_version, const DropSchemaMode mode, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  bool need_drop = false;
  uint64_t tenant_id = partition_schema.get_tenant_id();
  uint64_t schema_id = partition_schema.get_table_id();
  int64_t drop_schema_version = partition_schema.get_drop_schema_version();
  ObArray<int64_t> dropped_partition_ids;
  ObArray<int64_t> dropped_subpartition_ids;
  obrpc::ObForceDropSchemaArg arg;
  arg.exec_tenant_id_ = tenant_id;
  bool check_partition = (DROP_SCHEMA_AND_PARTITION == mode || DROP_PARTITION_ONLY == mode);
  // check last backup schema version
  if (partition_schema.is_dropped_schema()) {
    // drop table/tablegroup
    need_drop = schema_need_drop(drop_schema_version, recycle_schema_version);
    if (DROP_PARTITION_ONLY == mode && need_drop) {
      // while schema is dropped, no need to check partition, it is an optimization
      need_drop = false;
      check_partition = false;
    }
  }
  if (check_partition && !need_drop) {
    // drop partition
    if (OB_FAIL(get_dropped_partition_ids(
            recycle_schema_version, partition_schema, dropped_partition_ids, dropped_subpartition_ids))) {
      LOG_WARN("fail to fill dropped partition ids", K(ret), K(partition_schema));
    } else {
      need_drop = dropped_partition_ids.count() > 0 || dropped_subpartition_ids.count() > 0;
    }
  }
  // check __all_tenant_gc_partition_info
  if (OB_SUCC(ret) && need_drop) {
    ObArray<int64_t> existed_partition_ids;
    bool has_partition = partition_schema.has_self_partition();
    if (has_partition && OB_FAIL(get_existed_partition_ids(tenant_id, schema_id, existed_partition_ids))) {
      LOG_WARN("fail to get existed partition ids", K(ret), K(tenant_id), K(schema_id));
    } else {
      arg.schema_id_ = schema_id;
      arg.recycle_schema_version_ = recycle_schema_version;
      arg.type_ = is_new_tablegroup_id(schema_id) ? share::schema::TABLEGROUP_SCHEMA : share::schema::TABLE_SCHEMA;
      if (dropped_partition_ids.count() <= 0 && dropped_subpartition_ids.count() <= 0) {
        // drop table/tablegroup
        need_drop = (existed_partition_ids.count() == 0);
      } else {
        // drop partition
        if (OB_FAIL(filter_partition_ids(
                partition_schema, dropped_partition_ids, dropped_subpartition_ids, existed_partition_ids, arg))) {
          LOG_WARN("fail to filter partition ids", K(ret), K(arg));
        } else if (arg.partition_ids_.count() <= 0 && arg.subpartition_ids_.count() <= 0) {
          need_drop = false;
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_drop) {
    if (OB_FAIL(recycle_gc_partition_info(arg))) {
      LOG_WARN("fail to recycle gc partition info", K(ret), K(arg));
    } else if (FAILOVER_MODE == task_mode_) {
      // Failover need clear delayed deleted schema objects. But can not clear by RPC as DDL thread has been occupied.
      if (OB_FAIL(root_service_.force_drop_schema(arg))) {
        LOG_WARN("force drop schema failed", KR(ret), K(arg));
      }
    } else if (OB_FAIL(rpc_proxy_.force_drop_schema(arg))) {
      LOG_WARN("force_drop_schema failed", K(ret), K(arg));
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("force drop schema", K(arg));
      task_cnt++;
      usleep(DROP_SCHEMA_IDLE_TIME);
    }
  }
  LOG_DEBUG("try drop schema", K(ret), K(mode), K(recycle_schema_version), K(partition_schema));
  return ret;
}

// ensure the order from small to big:
// dropped_partition_ids,dropped_subpartition_ids,existed_partition_ids
int ObForceDropSchemaChecker::filter_partition_ids(const ObPartitionSchema& partition_schema,
    const ObArray<int64_t>& dropped_partition_ids, const ObArray<int64_t>& dropped_subpartition_ids,
    const ObArray<int64_t>& existed_partition_ids, obrpc::ObForceDropSchemaArg& arg)
{
  int ret = OB_SUCCESS;
  bool is_subpart = (PARTITION_LEVEL_TWO == partition_schema.get_part_level());

  int64_t j = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < dropped_partition_ids.count(); i++) {
    int64_t dropped_partition_id = dropped_partition_ids.at(i);
    bool finded = false;
    for (; OB_SUCC(ret) && j < existed_partition_ids.count(); j++) {
      int64_t existed_partition_id = existed_partition_ids.at(j);
      if (is_subpart) {
        // for subpartition, the force_drop_schema of fist level partition need wait for
        // its next level partition to be archived
        existed_partition_id = extract_part_idx(existed_partition_id);
      }
      if (existed_partition_id >= dropped_partition_id) {
        finded = (existed_partition_id == dropped_partition_id);
        break;
      }
    }
    if (OB_FAIL(ret) || finded) {
    } else if (OB_FAIL(arg.partition_ids_.push_back(dropped_partition_id))) {
      LOG_WARN("fail to push back partition_id", K(ret), K(dropped_partition_id));
    }
  }

  if (OB_SUCC(ret) && is_subpart) {
    j = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_subpartition_ids.count(); i++) {
      int64_t dropped_subpartition_id = dropped_subpartition_ids.at(i);
      bool finded = false;
      for (; OB_SUCC(ret) && j < existed_partition_ids.count(); j++) {
        int64_t existed_subpartition_id = existed_partition_ids.at(j);
        if (existed_subpartition_id >= dropped_subpartition_id) {
          finded = (existed_subpartition_id == dropped_subpartition_id);
          break;
        }
      }
      if (OB_FAIL(ret) || finded) {
      } else if (OB_FAIL(arg.subpartition_ids_.push_back(dropped_subpartition_id))) {
        LOG_WARN("fail to push back subpartition_id", K(ret), K(dropped_subpartition_id));
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObInspector::ObInspector(ObRootService& rs) : ObAsyncTimerTask(rs.get_inspect_task_queue()), rs_(rs)
{}

int ObInspector::process()
{
  // @todo ObTaskController::get().switch_task(share::ObTaskType::ROOT_SERVICE);
  int ret = OB_SUCCESS;
  ObTableGroupChecker tablegroup_checker(rs_.get_schema_service());
  ObRootInspection system_schema_checker;
  ObTenantChecker tenant_checker(rs_.get_schema_service(), rs_.get_sql_proxy(), rs_.get_common_rpc_proxy());
  ObDropTenantChecker drop_tenant_checker(rs_.get_schema_service(), rs_.get_common_rpc_proxy());
  ObPrimaryClusterInspection primary_cluster_inspection;
  primary_cluster_inspection.init(
      rs_.get_schema_service(), rs_.get_pt_operator(), rs_.get_zone_mgr(), rs_.get_sql_proxy(), rs_.get_server_mgr());
  ret = E(EventTable::EN_STOP_ROOT_INSPECTION) OB_SUCCESS;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablegroup_checker.init())) {
    LOG_WARN("init tablegroup_checker failed", K(ret));
  } else if (OB_FAIL(system_schema_checker.init(rs_.get_schema_service(), rs_.get_zone_mgr(), rs_.get_sql_proxy()))) {
    LOG_WARN("init root inspection failed", K(ret));
  } else {
    ObInspectionTask* inspection_tasks[] = {&tablegroup_checker,
        &system_schema_checker,
        &tenant_checker,
        &drop_tenant_checker,
        &primary_cluster_inspection};
    bool passed = true;
    const char* warning_info = NULL;
    int N = ARRAYSIZEOF(inspection_tasks);
    for (int i = 0; i < N; ++i) {
      passed = true;
      warning_info = NULL;
      int tmp_ret = inspection_tasks[i]->inspect(passed, warning_info);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("inpection task failed", K(tmp_ret), K(i), "task", inspection_tasks[i]->get_task_name());
      } else if (passed) {
        LOG_INFO("inspection task succ", K(i), "task", inspection_tasks[i]->get_task_name());
      } else {
        LOG_ERROR(warning_info);
        ROOTSERVICE_EVENT_ADD(
            "inspector", inspection_tasks[i]->get_task_name(), "info", (warning_info == NULL ? "" : warning_info));
      }
    }
  }
  return ret;
}

ObAsyncTask* ObInspector::deep_copy(char* buf, const int64_t buf_size) const
{
  ObInspector* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObInspector(rs_);
  }
  return task;
}

////////////////////////////////////////////////////////////////
ObPurgeRecyclebinTask::ObPurgeRecyclebinTask(ObRootService& rs)
    : ObAsyncTimerTask(rs.get_inspect_task_queue()), root_service_(rs)
{}

int ObPurgeRecyclebinTask::process()
{
  LOG_INFO("purge recyclebin task begin");
  int ret = OB_SUCCESS;
  const int64_t PURGE_EACH_TIME = 1000;
  int64_t delay = 1 * 60 * 1000 * 1000;
  int64_t expire_time = GCONF.recyclebin_object_expire_time;
  int64_t purge_interval = GCONF._recyclebin_object_purge_frequency;
  if (expire_time > 0 && purge_interval > 0) {
    if (OB_FAIL(root_service_.purge_recyclebin_objects(PURGE_EACH_TIME))) {
      LOG_WARN("fail to purge recyclebin objects", KR(ret));
    }
    delay = purge_interval;
  }
  // the error code is only for outputtion log, the function will return success.
  // the task no need retry, because it will be triggered periodically.
  if (OB_FAIL(root_service_.schedule_recyclebin_task(delay))) {
    LOG_WARN("schedule purge recyclebin task failed", KR(ret), K(delay));
  } else {
    LOG_INFO("submit purge recyclebin task success", K(delay));
  }
  LOG_INFO("purge recyclebin task end", K(delay));
  return OB_SUCCESS;
}

ObAsyncTask* ObPurgeRecyclebinTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObPurgeRecyclebinTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObPurgeRecyclebinTask(root_service_);
  }
  return task;
}

ObForceDropSchemaTask::ObForceDropSchemaTask(ObRootService& rs)
    : ObAsyncTimerTask(rs.get_inspect_task_queue()), root_service_(rs)
{}

int ObForceDropSchemaTask::process()
{
  LOG_INFO("force drop schema task begin");
  int ret = OB_SUCCESS;
  const int64_t FAST_INTERVAL = 10 * 1000 * 1000;  // 10s
  int64_t delay = ObInspector::INSPECT_INTERVAL;
#ifdef ERRSIM
  delay = ObServerConfig::get_instance().schema_drop_gc_delay_time;
#endif
  ObForceDropSchemaChecker drop_schema_checker(root_service_,
      root_service_.get_schema_service(),
      root_service_.get_common_rpc_proxy(),
      root_service_.get_sql_proxy(),
      ObForceDropSchemaChecker::NORMAL_MODE);
  bool passed = true;
  const char* warning_info = NULL;
  if (OB_FAIL(drop_schema_checker.inspect(passed, warning_info))) {
    LOG_WARN("fail to inspect", KR(ret));
  } else if (!passed) {
    // passed = false, indication that there is schema need force dropped
    delay = FAST_INTERVAL;
  }
  // Ignore the error code and generate the next task
  if (OB_FAIL(root_service_.schedule_force_drop_schema_task(delay))) {
    LOG_WARN("schedule force drop schema task failed", KR(ret), K(delay));
  } else {
    LOG_INFO("submit force drop schema task success", KR(ret), K(delay));
  }
  LOG_INFO("force drop schema task end", K(ret), K(delay));
  return OB_SUCCESS;
}

ObAsyncTask* ObForceDropSchemaTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObForceDropSchemaTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObForceDropSchemaTask(root_service_);
  }
  return task;
}

ObRootInspection::ObRootInspection()
    : inited_(false),
      stopped_(false),
      zone_passed_(false),
      sys_param_passed_(false),
      sys_stat_passed_(false),
      sys_table_schema_passed_(false),
      all_checked_(false),
      all_passed_(false),
      can_retry_(false),
      sql_proxy_(NULL),
      rpc_proxy_(NULL),
      schema_service_(NULL),
      zone_mgr_(NULL)
{}

ObRootInspection::~ObRootInspection()
{}

int ObRootInspection::init(ObMultiVersionSchemaService& schema_service, ObZoneManager& zone_mgr,
    ObMySQLProxy& sql_proxy, obrpc::ObCommonRpcProxy* rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    zone_mgr_ = &zone_mgr;
    sql_proxy_ = &sql_proxy;
    stopped_ = false;
    zone_passed_ = false;
    sys_param_passed_ = false;
    sys_stat_passed_ = false;
    sys_table_schema_passed_ = false;
    all_checked_ = false;
    all_passed_ = false;
    can_retry_ = false;
    rpc_proxy_ = rpc_proxy;
    inited_ = true;
  }
  return ret;
}

int ObRootInspection::check_all()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (!schema_service_->is_tenant_full_schema(OB_SYS_TENANT_ID)) {
    ret = OB_EAGAIN;
    LOG_WARN("schema is not ready, try again", K(ret));
  } else {
    can_retry_ = false;
    int tmp = OB_SUCCESS;

    // check __all_zone
    if (OB_SUCCESS != (tmp = check_zone())) {
      LOG_WARN("check_zone failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    zone_passed_ = (OB_SUCCESS == tmp);

    // check sys stat
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_sys_stat())) {
      LOG_WARN("check_sys_stat failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    sys_stat_passed_ = (OB_SUCCESS == tmp);

    // check sys param
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_sys_param())) {
      LOG_WARN("check_sys_param failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    sys_param_passed_ = (OB_SUCCESS == tmp);

    // check sys schema
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_sys_table_schemas())) {
      LOG_WARN("check_sys_table_schemas failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    sys_table_schema_passed_ = (OB_SUCCESS == tmp);

    // upgrade job may still running, in order to avoid the upgrade process error stuck,
    // ignore the 4674 error
    for (int64_t i = 0; i < UPGRADE_JOB_TYPE_COUNT; i++) {
      tmp = OB_SUCCESS;
      ObRsJobType job_type = upgrade_job_type_array[i];
      if (job_type > JOB_TYPE_INVALID && job_type < JOB_TYPE_MAX) {
        if (OB_SUCCESS != (tmp = ObUpgradeUtils::check_upgrade_job_passed(job_type))) {
          LOG_WARN("fail to check upgrade job passed", K(tmp), K(job_type));
          if (OB_RUN_JOB_NOT_SUCCESS != tmp) {
            ret = (OB_SUCCESS == ret) ? tmp : ret;
          } else {
            LOG_WARN("upgrade job may still running, check with __all_virtual_uprade_inspection",
                K(ret),
                K(tmp),
                "job_type",
                ObRsJobTableOperator::get_job_type_str(job_type));
          }
        }
      }
    }

    all_checked_ = true;
    all_passed_ = OB_SUCC(ret);
  }
  return ret;
}

int ObRootInspection::inspect(bool& passed, const char*& warning_info)
{
  int ret = OB_SUCCESS;
  if (!GCONF.in_upgrade_mode()) {
    ret = check_all();
    if (OB_SUCC(ret)) {
      passed = all_passed_;
      warning_info = "system metadata error";
    }
  } else {
    passed = true;
  }
  return ret;
}

int ObRootInspection::check_zone()
{
  int ret = OB_SUCCESS;
  ObSqlString extra_cond;
  HEAP_VAR(ObGlobalInfo, global_zone_info)
  {
    ObArray<ObZoneInfo> zone_infos;
    ObArray<const char*> global_zone_item_names;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(check_cancel())) {
      LOG_WARN("check_cancel failed", K(ret));
    } else if (OB_FAIL(extra_cond.assign_fmt("zone = '%s'", global_zone_info.zone_.ptr()))) {
      LOG_WARN("extra_cond assign_fmt failed", K(ret));
    } else if (OB_FAIL(get_names(global_zone_info.list_, global_zone_item_names))) {
      LOG_WARN("get global zone item names failed", K(ret));
    } else if (OB_FAIL(check_names(OB_SYS_TENANT_ID, OB_ALL_ZONE_TNAME, global_zone_item_names, extra_cond))) {
      LOG_WARN("check global zone item names failed",
          "table_name",
          OB_ALL_ZONE_TNAME,
          K(global_zone_item_names),
          K(extra_cond),
          K(ret));
    } else if (OB_FAIL(zone_mgr_->get_zone(zone_infos))) {
      LOG_WARN("zone manager get_zone failed", K(ret));
    } else {
      ObArray<const char*> zone_item_names;
      FOREACH_CNT_X(zone_info, zone_infos, OB_SUCCESS == ret)
      {
        zone_item_names.reuse();
        extra_cond.reset();
        if (OB_FAIL(check_cancel())) {
          LOG_WARN("check_cancel failed", K(ret));
        } else if (OB_FAIL(extra_cond.assign_fmt("zone = '%s'", zone_info->zone_.ptr()))) {
          LOG_WARN("extra_cond assign_fmt failed", K(ret));
        } else if (OB_FAIL(get_names(zone_info->list_, zone_item_names))) {
          LOG_WARN("get zone item names failed", K(ret));
        } else if (OB_FAIL(check_names(OB_SYS_TENANT_ID, OB_ALL_ZONE_TNAME, zone_item_names, extra_cond))) {
          LOG_WARN("check zone item names failed",
              "table_name",
              OB_ALL_ZONE_TNAME,
              K(zone_item_names),
              "zone_info",
              *zone_info,
              K(extra_cond),
              K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootInspection::check_sys_stat()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else {
    ObArray<const char*> sys_stat_names;
    ObSqlString extra_cond;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCCESS == ret)
    {
      sys_stat_names.reuse();
      extra_cond.reset();
      // ObSysStat should construct every time because items are linked in list in construct
      // function
      ObSysStat sys_stat;
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(*tenant_id);
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", K(ret));
      } else if (!schema_service_->is_tenant_full_schema(*tenant_id)) {
        ret = OB_EAGAIN;
        LOG_WARN("schema is not ready, try again", K(ret), K(*tenant_id));
      } else if (OB_FAIL(sys_stat.set_initial_values(*tenant_id))) {
        LOG_WARN("set initial values failed", K(ret));
      } else if (OB_FAIL(extra_cond.assign_fmt(
                     "tenant_id = %lu", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, *tenant_id)))) {
        LOG_WARN("extra_cond assign_fmt failed", K(ret));
      } else if (OB_FAIL(get_names(sys_stat.item_list_, sys_stat_names))) {
        LOG_WARN("get sys stat names failed", K(ret));
      } else if (OB_FAIL(check_names(*tenant_id, OB_ALL_SYS_STAT_TNAME, sys_stat_names, extra_cond))) {
        LOG_WARN("check all sys stat names failed",
            "tenant_id",
            *tenant_id,
            "table_name",
            OB_ALL_SYS_STAT_TNAME,
            K(sys_stat_names),
            K(ret));
      }
    }
  }
  return ret;
}

int ObRootInspection::check_sys_param()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else {
    ObArray<const char*> sys_param_names;
    ObSqlString extra_cond;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCCESS == ret)
    {
      sys_param_names.reuse();
      extra_cond.reset();
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(*tenant_id);
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", K(ret));
      } else if (!schema_service_->is_tenant_full_schema(*tenant_id)) {
        ret = OB_EAGAIN;
        LOG_WARN("schema is not ready, try again", K(ret), K(*tenant_id));
      } else if (OB_FAIL(extra_cond.assign_fmt(
                     "tenant_id = %lu", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, *tenant_id)))) {
      } else if (OB_FAIL(get_sys_param_names(sys_param_names))) {
        LOG_WARN("get sys param names failed", K(ret));
      } else if (OB_FAIL(
                     check_and_insert_sys_params(*tenant_id, OB_ALL_SYS_VARIABLE_TNAME, sys_param_names, extra_cond))) {
        LOG_WARN("check and insert sys params failed",
            "tenant_id",
            *tenant_id,
            "table_name",
            OB_ALL_SYS_VARIABLE_TNAME,
            K(sys_param_names),
            K(extra_cond),
            K(ret));
      } else if (OB_FAIL(check_names(*tenant_id,
                     OB_ALL_SYS_VARIABLE_TNAME,
                     sys_param_names,
                     extra_cond))) {  // For robustness, read it out again
        LOG_WARN("check all sys params names failed",
            "tenant_id",
            *tenant_id,
            "table_name",
            OB_ALL_SYS_VARIABLE_TNAME,
            K(sys_param_names),
            K(extra_cond),
            K(ret));
      }
    }
  }
  if (OB_SCHEMA_ERROR != ret) {
  } else if (GCONF.in_upgrade_mode()) {
    LOG_WARN("check sys_variable failed", K(ret));
  } else {
    LOG_ERROR("check sys_variable failed", K(ret));
  }
  return ret;
}

template <typename Item>
int ObRootInspection::get_names(const ObDList<Item>& list, ObIArray<const char*>& names)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (list.get_size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("list is empty", K(ret));
  } else {
    const Item* it = list.get_first();
    while (OB_SUCCESS == ret && it != list.get_header()) {
      if (OB_FAIL(names.push_back(it->name_))) {
        LOG_WARN("push_back failed", K(ret));
      } else {
        it = it->get_next();
      }
    }
  }
  return ret;
}

int ObRootInspection::get_sys_param_names(ObIArray<const char*>& names)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t param_count = ObSysVariables::get_amount();
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      if (OB_FAIL(names.push_back(ObSysVariables::get_name(i).ptr()))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRootInspection::check_and_insert_sys_params(
    uint64_t tenant_id, const char* table_name, const ObIArray<const char*>& names, const ObSqlString& extra_cond)
{
  int ret = OB_SUCCESS;
  ObArray<Name> fetch_names;  // Get the data of the internal table
  ObArray<Name> extra_names;  // data inner table more than hard code
  ObArray<Name> miss_names;   // data inner table less than hard code
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (OB_INVALID_ID == tenant_id || NULL == table_name || names.count() <= 0) {
    // extra_cond can be empty, so don't check it here
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid tenant_id or table_name is null or names is empty", K(tenant_id), K(table_name), K(names), K(ret));
  } else if (OB_FAIL(calc_diff_names(tenant_id, table_name, names, extra_cond, fetch_names, extra_names, miss_names))) {
    LOG_WARN("fail to calc diff names", K(ret), K(table_name), K(names), K(extra_cond));
  } else {
    if (miss_names.count() > 0) {
      // don't need to set ret
      LOG_WARN("some sys var exist in hard code, but does not exist in inner table, "
               "they will be inserted into table",
          K(table_name),
          K(miss_names));
    }
    ObSqlString sql;
    ObSysParam sys_param;
    obrpc::ObAddSysVarArg arg;
    arg.exec_tenant_id_ = tenant_id;
    const ObZone global_zone = "";
    for (int64_t i = 0; OB_SUCC(ret) && i < miss_names.count(); ++i) {
      sql.reset();
      sys_param.reset();
      arg.sysvar_.reset();
      arg.if_not_exist_ = true;
      arg.sysvar_.set_tenant_id(tenant_id);
      int64_t var_store_idx = OB_INVALID_INDEX;
      if (OB_ISNULL(rpc_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("common rpc proxy is null");
      } else if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", K(ret));
      } else if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx_by_name(
                     ObString(miss_names.at(i).ptr()), var_store_idx))) {
        LOG_WARN("fail to calc sys var store idx by name", K(ret), K(miss_names.at(i).ptr()));
      } else if (false == ObSysVarFactory::is_valid_sys_var_store_idx(var_store_idx)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("calc sys var store idx success but store_idx is invalid", K(ret), K(var_store_idx));
      } else {
        const ObString& name = ObSysVariables::get_name(var_store_idx);
        const ObObjType& type = ObSysVariables::get_type(var_store_idx);
        const ObString& value = ObSysVariables::get_value(var_store_idx);
        const ObString& min = ObSysVariables::get_min(var_store_idx);
        const ObString& max = ObSysVariables::get_max(var_store_idx);
        const ObString& info = ObSysVariables::get_info(var_store_idx);
        const int64_t flag = ObSysVariables::get_flags(var_store_idx);
        if (OB_FAIL(sys_param.init(
                tenant_id, global_zone, name.ptr(), type, value.ptr(), min.ptr(), max.ptr(), info.ptr(), flag))) {
          LOG_WARN("sys_param init failed",
              K(tenant_id),
              K(name),
              K(type),
              K(value),
              K(min),
              K(max),
              K(info),
              K(flag),
              K(ret));
        } else if (!sys_param.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("sys param is invalid", K(sys_param), K(ret));
        } else if (OB_FAIL(ObSchemaUtils::convert_sys_param_to_sysvar_schema(sys_param, arg.sysvar_))) {
          LOG_WARN("convert sys param to sysvar schema failed", K(ret));
        } else if (OB_FAIL(rpc_proxy_->add_system_variable(arg))) {
          LOG_WARN("add system variable failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootInspection::check_names(
    const uint64_t tenant_id, const char* table_name, const ObIArray<const char*>& names, const ObSqlString& extra_cond)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (NULL == table_name || names.count() <= 0) {
    // extra_cond can be empty, so wo don't check it here
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is null or names is empty", KP(table_name), K(names), K(ret));
  } else {
    ObArray<Name> fetch_names;  // Get the data of the internal table
    ObArray<Name> extra_names;  // data inner table more than hard code
    ObArray<Name> miss_names;   // data inner table less than hard code
    if (OB_FAIL(calc_diff_names(tenant_id, table_name, names, extra_cond, fetch_names, extra_names, miss_names))) {
      LOG_WARN("fail to calc diff names", K(ret), KP(table_name), K(names), K(extra_cond));
    } else {
      if (fetch_names.count() <= 0) {
        // don't need to set ret
        LOG_WARN("maybe tenant or zone has been deleted, ignore it", K(table_name), K(extra_cond));
      } else {
        if (extra_names.count() > 0) {
          // don't need to set ret
          LOG_WARN("some item exist in table, but not hard coded", K(table_name), K(extra_names));
        }
        if (miss_names.count() > 0) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("some item exist in hard code, but not exist in inner table", K(ret), K(table_name), K(miss_names));
        }
      }
    }
  }
  return ret;
}

int ObRootInspection::get_schema_status(
    const uint64_t tenant_id, const char* orig_table_name, ObRefreshSchemaStatus& schema_status)
{
  int ret = OB_SUCCESS;
  schema_status.reset();
  int len = strlen(orig_table_name);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_name", K(ret), K(len), K(orig_table_name));
  } else if (FALSE_IT(schema_status.tenant_id_ = tenant_id)) {
  } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id) {
    ObSchemaGetterGuard schema_guard;
    uint64_t table_id = OB_INVALID_ID;
    bool need_weak_read = false;
    ObString table_name(len, orig_table_name);
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_id(tenant_id,
                   combine_id(tenant_id, OB_SYS_DATABASE_ID),
                   table_name,
                   false, /*is_index*/
                   schema::ObSchemaGetterGuard::ALL_TYPES,
                   table_id))) {
      LOG_WARN("fail to get table_id", K(ret), K(tenant_id), K(table_name));
    } else if (OB_INVALID_ID == table_id) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(tenant_id), K(table_name));
    } else if (OB_FAIL(ObSysTableChecker::is_tenant_table_need_weak_read(table_id, need_weak_read))) {
      LOG_WARN("fail to check if table need weak read", K(ret), K(tenant_id), K(table_name));
    } else if (!need_weak_read) {
      // do nothing
    } else if (OB_ISNULL(GCTX.schema_status_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema status proxy is null", K(ret));
    } else if (OB_FAIL(GCTX.schema_status_proxy_->get_refresh_schema_status(tenant_id, schema_status))) {
      LOG_WARN("fail to get schema status", K(ret), K(tenant_id), K(table_name));
    }
  }
  return ret;
}

int ObRootInspection::calc_diff_names(const uint64_t tenant_id, const char* table_name,
    const ObIArray<const char*>& names, const ObSqlString& extra_cond,
    ObIArray<Name>& fetch_names, /* data reading from inner table*/
    ObIArray<Name>& extra_names, /* data inner table more than hard code*/
    ObIArray<Name>& miss_names /* data inner table less than hard code*/)
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  fetch_names.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (NULL == table_name || names.count() <= 0) {
    // extra_cond can be empty, don't need to check it
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is null or names is empty", KP(table_name), K(names), K(ret));
  } else if (OB_FAIL(get_schema_status(tenant_id, table_name, schema_status))) {
    LOG_WARN("fail to get schema status", K(ret), K(tenant_id), K(table_name));
  } else {
    const uint64_t exec_tenant_id =
        OB_INVALID_TENANT_ID == schema_status.tenant_id_ ? OB_SYS_TENANT_ID : schema_status.tenant_id_;
    bool did_use_weak = schema_status.snapshot_timestamp_ >= 0;
    bool did_use_retry = false;
    int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
    ObSqlString sql;
    ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, did_use_weak, did_use_retry, snapshot_timestamp);
    if (OB_FAIL(sql.append_fmt(
            "SELECT name FROM %s%s%s", table_name, (extra_cond.empty()) ? "" : " WHERE ", extra_cond.ptr()))) {
      LOG_WARN("append_fmt failed", K(table_name), K(extra_cond), K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        ObMySQLResult* result = NULL;
        if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", K(sql), K(ret));
          can_retry_ = true;
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is not expected to be NULL", "result", OB_P(result), K(ret));
        } else {
          // only for filling the out parameter,
          // Ensure that there is no '\ 0' character in the middle of the corresponding string
          int64_t tmp_real_str_len = 0;
          Name name;
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("get next result failed", K(ret));
              }
            } else {
              EXTRACT_STRBUF_FIELD_MYSQL(
                  *result, "name", name.ptr(), static_cast<int64_t>(NAME_BUF_LEN), tmp_real_str_len);
              (void)tmp_real_str_len;  // make compiler happy
              if (OB_FAIL(fetch_names.push_back(name))) {
                LOG_WARN("push_back failed", K(ret));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (fetch_names.count() <= 0) {
        LOG_WARN("maybe tenant or zone has been deleted, ignore it", K(table_name), K(extra_cond));
      } else {
        extra_names.reset();
        miss_names.reset();
        FOREACH_CNT_X(fetch_name, fetch_names, OB_SUCCESS == ret)
        {
          bool found = false;
          FOREACH_CNT_X(name, names, OB_SUCCESS == ret)
          {
            if (Name(*name) == *fetch_name) {
              found = true;
              break;
            }
          }
          if (!found) {
            if (OB_FAIL(extra_names.push_back(*fetch_name))) {
              LOG_WARN("fail to push name into fetch_names", K(ret), K(*fetch_name), K(fetch_names));
            }
          }
        }
        FOREACH_CNT_X(name, names, OB_SUCCESS == ret)
        {
          bool found = false;
          FOREACH_CNT_X(fetch_name, fetch_names, OB_SUCCESS == ret)
          {
            if (Name(*name) == *fetch_name) {
              found = true;
              break;
            }
          }
          if (!found) {
            if (OB_FAIL(miss_names.push_back(Name(*name)))) {
              LOG_WARN("fail to push name into miss_names", K(ret), K(*name), K(miss_names));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObRootInspection::check_sys_table_schemas()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else {
    const schema_create_func all_core_table_schema_creator[] = {
        &share::ObInnerTableSchema::all_core_table_schema, NULL};
    const schema_create_func* creator_ptr_array[] = {all_core_table_schema_creator,
        share::core_table_schema_creators,
        share::sys_table_schema_creators,
        share::virtual_table_schema_creators,
        share::sys_view_schema_creators,
        NULL};

    int back_ret = OB_SUCCESS;
    ObTableSchema table_schema;
    for (const schema_create_func** creator_ptr_ptr = creator_ptr_array; OB_SUCCESS == ret && NULL != *creator_ptr_ptr;
         ++creator_ptr_ptr) {
      for (const schema_create_func* creator_ptr = *creator_ptr_ptr; OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
        table_schema.reset();
        if (OB_FAIL(check_cancel())) {
          LOG_WARN("check_cancel failed", K(ret));
        } else if (OB_FAIL((*creator_ptr)(table_schema))) {
          LOG_WARN("create table schema failed", K(ret));
        } else if (OB_FAIL(check_table_schema(table_schema))) {
          // don't print table_schema, otherwise log will be too much
          LOG_WARN("check_table_schema failed", K(ret));
        }
        back_ret = OB_SUCCESS == back_ret ? ret : back_ret;
        ret = OB_SUCCESS;
      }
    }
    ret = back_ret;
  }
  if (OB_SCHEMA_ERROR != ret) {
  } else if (GCONF.in_upgrade_mode()) {
    LOG_WARN("check sys table schema failed", K(ret));
  } else {
    LOG_ERROR("check sys table schema failed", K(ret));
  }
  return ret;
}

int ObRootInspection::check_table_schema(const ObTableSchema& hard_code_table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table = NULL;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!hard_code_table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", K(hard_code_table), K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(hard_code_table.get_table_id(), table))) {
    LOG_WARN("get_table_schema failed",
        "table_id",
        hard_code_table.get_table_id(),
        "table_name",
        hard_code_table.get_table_name(),
        K(ret));
    // fail may cause by load table schema sql, set retry flag.
    can_retry_ = true;
  } else if (NULL == table) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table should not be null",
        "table_id",
        hard_code_table.get_table_id(),
        "table_name",
        hard_code_table.get_table_name(),
        K(ret));
    can_retry_ = true;
  } else if (OB_FAIL(check_table_schema(hard_code_table, *table))) {
    LOG_WARN("fail to check table schema", KR(ret));
  }
  return ret;
}

int ObRootInspection::check_table_schema(const ObTableSchema& hard_code_table, const ObTableSchema& inner_table)
{
  int ret = OB_SUCCESS;
  if (!hard_code_table.is_valid() || !inner_table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", K(hard_code_table), K(inner_table), K(ret));
  } else if (OB_FAIL(check_table_options(inner_table, hard_code_table))) {
    LOG_WARN("check_table_options failed", "table_id", hard_code_table.get_table_id(), K(ret));
  } else {
    if (hard_code_table.get_column_count() != inner_table.get_column_count()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("column count mismatch",
          "table_id",
          inner_table.get_table_id(),
          "table_name",
          inner_table.get_table_name(),
          "table_column_cnt",
          inner_table.get_column_count(),
          "hard_code_table_column_cnt",
          hard_code_table.get_column_count(),
          K(ret));
    } else {
      int back_ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < hard_code_table.get_column_count(); ++i) {
        const ObColumnSchemaV2* hard_code_column = hard_code_table.get_column_schema_by_idx(i);
        const ObColumnSchemaV2* column = NULL;
        if (NULL == hard_code_column) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("hard_code_column is null", "hard_code_column", OB_P(hard_code_column), K(ret));
        } else if (NULL == (column = inner_table.get_column_schema(hard_code_column->get_column_name()))) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("hard code column not found",
              "table_id",
              hard_code_table.get_table_id(),
              "table_name",
              hard_code_table.get_table_name(),
              "column",
              hard_code_column->get_column_name(),
              K(ret));
        } else {
          const bool ignore_column_id = is_virtual_table(hard_code_table.get_table_id());
          if (OB_FAIL(check_column_schema(
                  hard_code_table.get_table_name(), *column, *hard_code_column, ignore_column_id))) {
            LOG_WARN("column schema mismatch with hard code column schema",
                "table_name",
                inner_table.get_table_name(),
                "column",
                *column,
                "hard_code_column",
                *hard_code_column,
                K(ret));
          }
        }
        back_ret = OB_SUCCESS == back_ret ? ret : back_ret;
        ret = OB_SUCCESS;
      }
      ret = back_ret;
    }
  }
  return ret;
}

int ObRootInspection::check_table_options(const ObTableSchema& table, const ObTableSchema& hard_code_table)
{
  int ret = OB_SUCCESS;
  if (!table.is_valid() || !hard_code_table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table or invalid hard_code_table", K(table), K(hard_code_table), K(ret));
  } else if (table.get_table_id() != hard_code_table.get_table_id()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table id not match",
        "table_id",
        table.get_table_id(),
        "hard_code table_id",
        hard_code_table.get_table_id(),
        K(ret));
  } else if (table.get_table_name_str() != hard_code_table.get_table_name_str()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table name mismatch with hard code table",
        "table_id",
        table.get_table_id(),
        "table_name",
        table.get_table_name(),
        "hard_code_table name",
        hard_code_table.get_table_name(),
        K(ret));
  } else {
    const ObString& table_name = table.get_table_name_str();

    if (table.get_tenant_id() != hard_code_table.get_tenant_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tenant_id mismatch",
          K(table_name),
          "in_memory",
          table.get_tenant_id(),
          "hard_code",
          hard_code_table.get_tenant_id(),
          K(ret));
    } else if (table.get_database_id() != hard_code_table.get_database_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("database_id mismatch",
          K(table_name),
          "in_memory",
          table.get_database_id(),
          "hard_code",
          hard_code_table.get_database_id(),
          K(ret));
    } else if (table.get_tablegroup_id() != hard_code_table.get_tablegroup_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tablegroup_id mismatch",
          K(table_name),
          "in_memory",
          table.get_tablegroup_id(),
          "hard_code",
          hard_code_table.get_tablegroup_id(),
          K(ret));
    } else if (table.get_max_used_column_id() < hard_code_table.get_max_used_column_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("max_used_column_id mismatch",
          K(table_name),
          "in_memory",
          table.get_max_used_column_id(),
          "hard_code",
          hard_code_table.get_max_used_column_id(),
          K(ret));
    } else if (table.get_rowkey_column_num() != hard_code_table.get_rowkey_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("rowkey_column_num mismatch",
          K(table_name),
          "in_memory",
          table.get_rowkey_column_num(),
          "hard_code",
          hard_code_table.get_rowkey_column_num(),
          K(ret));
    } else if (table.get_index_column_num() != hard_code_table.get_index_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index_column_num mismatch",
          K(table_name),
          "in_memory",
          table.get_index_column_num(),
          "hard_code",
          hard_code_table.get_index_column_num(),
          K(ret));
    } else if (table.get_rowkey_split_pos() != hard_code_table.get_rowkey_split_pos()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("rowkey_split_pos mismatch",
          K(table_name),
          "in_memory",
          table.get_rowkey_split_pos(),
          "hard_code",
          hard_code_table.get_rowkey_split_pos(),
          K(ret));
    } else if (table.get_partition_key_column_num() != hard_code_table.get_partition_key_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partition_key_column_num mismatch",
          K(table_name),
          "in_memory",
          table.get_partition_key_column_num(),
          "hard_code",
          hard_code_table.get_partition_key_column_num(),
          K(ret));
    } else if (table.get_subpartition_key_column_num() != hard_code_table.get_subpartition_key_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partition_key_column_num mismatch",
          K(table_name),
          "in_memory",
          table.get_subpartition_key_column_num(),
          "hard_code",
          hard_code_table.get_subpartition_key_column_num(),
          K(ret));
    } else if (table.get_autoinc_column_id() != hard_code_table.get_autoinc_column_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("autoinc_column_id mismatch",
          K(table_name),
          "in_memory",
          table.get_autoinc_column_id(),
          "hard_code",
          hard_code_table.get_autoinc_column_id(),
          K(ret));
    } else if (table.get_auto_increment() != hard_code_table.get_auto_increment()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("auto_increment mismatch",
          K(table_name),
          "in_memory",
          table.get_auto_increment(),
          "hard_code",
          hard_code_table.get_auto_increment(),
          K(ret));
    } else if (table.is_read_only() != hard_code_table.is_read_only()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("read_only mismatch",
          K(table_name),
          "in_memory",
          table.is_read_only(),
          "hard code",
          hard_code_table.is_read_only(),
          K(ret));
    } else if (table.get_load_type() != hard_code_table.get_load_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("load_type mismatch",
          K(table_name),
          "in_memory",
          table.get_load_type(),
          "hard_code",
          hard_code_table.get_load_type(),
          K(ret));
    } else if (table.get_table_type() != hard_code_table.get_table_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("table_type mismatch",
          K(table_name),
          "in_memory",
          table.get_table_type(),
          "hard_code",
          hard_code_table.get_table_type(),
          K(ret));
    } else if (table.get_index_type() != hard_code_table.get_index_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index_type mismatch",
          K(table_name),
          "in_memory",
          table.get_index_type(),
          "hard_code",
          hard_code_table.get_index_type(),
          K(ret));
    } else if (table.get_index_using_type() != hard_code_table.get_index_using_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index_using_type mismatch",
          K(table_name),
          "in_memory",
          table.get_index_using_type(),
          "hard_code",
          hard_code_table.get_index_using_type(),
          K(ret));
    } else if (table.get_def_type() != hard_code_table.get_def_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("def_type mismatch",
          K(table_name),
          "in_memory",
          table.get_def_type(),
          "hard_code",
          hard_code_table.get_def_type(),
          K(ret));
    } else if (table.get_data_table_id() != hard_code_table.get_data_table_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("data_table_id mismatch",
          K(table_name),
          "in_memory",
          table.get_data_table_id(),
          "hard_code",
          hard_code_table.get_data_table_id(),
          K(ret));
    } else if (table.get_tablegroup_name() != hard_code_table.get_tablegroup_name()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tablegroup_name mismatch",
          K(table_name),
          "in_memory",
          table.get_tablegroup_name(),
          "hard_code",
          hard_code_table.get_tablegroup_name(),
          K(ret));
    } else if (table.get_view_schema() != hard_code_table.get_view_schema()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("view_schema mismatch",
          K(table_name),
          "in_memory",
          table.get_view_schema(),
          "hard_code",
          hard_code_table.get_view_schema(),
          K(ret));
    } else if (table.get_part_level() != hard_code_table.get_part_level()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("part_level mismatch",
          K(table_name),
          "in_memory",
          table.get_part_level(),
          "hard_code",
          hard_code_table.get_part_level(),
          K(ret));
    } else if ((table.get_part_option().get_part_func_expr_str() !=
                   hard_code_table.get_part_option().get_part_func_expr_str()) ||
               (table.get_part_option().get_part_func_type() !=
                   hard_code_table.get_part_option().get_part_func_type())) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("part_expr mismatch",
          K(table_name),
          "in_memory",
          table.get_part_option(),
          "hard_code",
          hard_code_table.get_part_option(),
          K(ret));
    } else if ((table.get_sub_part_option().get_part_func_expr_str() !=
                   hard_code_table.get_sub_part_option().get_part_func_expr_str()) ||
               (table.get_sub_part_option().get_part_func_type() !=
                   hard_code_table.get_sub_part_option().get_part_func_type())) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("sub_part_expr mismatch",
          K(table_name),
          "in_memory",
          table.get_sub_part_option(),
          "hard_code",
          hard_code_table.get_sub_part_option(),
          K(ret));
    }

    // options may be different between different ob instance, don't check
    // block_size
    // is_user_bloomfilter
    // progressive_merge_num
    // replica_num
    // index_status
    // name_case_mode
    // charset_type
    // collation_type
    // schema_version
    // comment
    // compress_func_name
    // expire_info
    // zone_list
    // primary_zone
    // create_mem_version
    // part_expr.part_num_
    // sub_part_expr.part_num_
    // store_format
    // row_store_type
    // progressive_merge_round
    // storage_format_version
  }
  return ret;
}

int ObRootInspection::check_column_schema(const ObString& table_name, const ObColumnSchemaV2& column,
    const ObColumnSchemaV2& hard_code_column, const bool ignore_column_id)
{
  int ret = OB_SUCCESS;
  if (table_name.empty() || !column.is_valid() || !hard_code_column.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is empty or invalid column or invalid hard_code_column",
        K(table_name),
        K(column),
        K(hard_code_column),
        K(ret));
  } else {
#define CMP_COLUMN_ATTR(attr)                                   \
  if (OB_SUCC(ret)) {                                           \
    if (column.get_##attr() != hard_code_column.get_##attr()) { \
      ret = OB_SCHEMA_ERROR;                                    \
      LOG_WARN(#attr " mismatch",                               \
          K(table_name),                                        \
          "column_name",                                        \
          column.get_column_name(),                             \
          "in_memory",                                          \
          column.get_##attr(),                                  \
          "hard_code",                                          \
          hard_code_column.get_##attr(),                        \
          K(ret));                                              \
    }                                                           \
  }

#define CMP_COLUMN_IS_ATTR(attr)                              \
  if (OB_SUCC(ret)) {                                         \
    if (column.is_##attr() != hard_code_column.is_##attr()) { \
      ret = OB_SCHEMA_ERROR;                                  \
      LOG_WARN(#attr " mismatch",                             \
          K(table_name),                                      \
          "column_name",                                      \
          column.get_column_name(),                           \
          "in_memory",                                        \
          column.is_##attr(),                                 \
          "hard_code",                                        \
          hard_code_column.is_##attr(),                       \
          K(ret));                                            \
    }                                                         \
  }

    if (!ignore_column_id) {
      uint64_t tid = extract_pure_id(hard_code_column.get_table_id());
      uint64_t cid = hard_code_column.get_column_id();
      if ((OB_ALL_TABLE_HISTORY_TID == tid && cid <= 75) || (OB_ALL_TABLE_TID == tid && cid <= 74) ||
          (OB_ALL_TENANT_TID == tid && cid <= 36) || (OB_ALL_TENANT_HISTORY_TID == tid && cid <= 38)) {
      } else {
        CMP_COLUMN_ATTR(column_id);
      }
    }
    CMP_COLUMN_ATTR(tenant_id);
    CMP_COLUMN_ATTR(table_id);
    // don't need to check schema version
    CMP_COLUMN_ATTR(rowkey_position);
    CMP_COLUMN_ATTR(index_position);
    CMP_COLUMN_ATTR(order_in_rowkey);
    CMP_COLUMN_ATTR(tbl_part_key_pos);
    CMP_COLUMN_ATTR(meta_type);
    CMP_COLUMN_ATTR(accuracy);
    CMP_COLUMN_ATTR(data_length);
    CMP_COLUMN_IS_ATTR(nullable);
    CMP_COLUMN_IS_ATTR(zero_fill);
    CMP_COLUMN_IS_ATTR(autoincrement);
    CMP_COLUMN_IS_ATTR(hidden);
    CMP_COLUMN_IS_ATTR(on_update_current_timestamp);
    CMP_COLUMN_ATTR(charset_type);
    // don't need to check orig default value
    if (ObString("row_store_type") == column.get_column_name() &&
        (ObString("__all_table") == table_name || ObString("__all_table_history") == table_name)) {
      // row_store_type may have two possible default values
    } else {
      CMP_COLUMN_ATTR(cur_default_value);
    }
    CMP_COLUMN_ATTR(comment);

    // special handling __all_table.max_used_constraint_id.is_nullable
    if (ObString("__all_table") == table_name && ObString("max_used_constraint_id") == column.get_column_name()) {
      if (ret == OB_SCHEMA_ERROR) {
        if (column.is_nullable() != hard_code_column.is_nullable()) {
          // Ignore inconsistencies
          ret = OB_SUCCESS;
        }
      }
    }
  }

#undef CMP_COLUMN_IS_ATTR
#undef CMP_COLUMN_INT_ATTR
  return ret;
}

int ObRootInspection::check_cancel()
{
  int ret = OB_SUCCESS;
  if (stopped_) {
    ret = OB_CANCELED;
  }
  return ret;
}

ObUpgradeInspection::ObUpgradeInspection() : inited_(false), schema_service_(NULL), root_inspection_(NULL)
{}

ObUpgradeInspection::~ObUpgradeInspection()
{}

int ObUpgradeInspection::init(ObMultiVersionSchemaService& schema_service, ObRootInspection& root_inspection)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    root_inspection_ = &root_inspection;
    inited_ = true;
  }
  return ret;
}

int ObUpgradeInspection::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard error", K(ret));
  } else if (!start_to_read_) {
    const ObTableSchema* table_schema = NULL;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID);
    const int64_t col_count = output_column_ids_.count();
    ObObj* cells = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("get_table_schema failed", K(table_id), K(ret));
    } else if (NULL == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", KP(table_schema), K(ret));
    } else if (NULL == (cells = static_cast<ObObj*>(allocator_->alloc(col_count * sizeof(ObObj))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc cells failed", K(col_count), K(ret));
    } else {
      new (cells) ObObj[col_count];
      cur_row_.cells_ = cells;
      cur_row_.count_ = col_count;
      ObArray<Column> columns;

#define ADD_ROW(name, info)                                                                     \
  do {                                                                                          \
    columns.reuse();                                                                            \
    if (OB_FAIL(ret)) {                                                                         \
    } else if (OB_FAIL(get_full_row(table_schema, name, info, columns))) {                      \
      LOG_WARN("get_full_row failed", "table_schema", *table_schema, K(name), K(info), K(ret)); \
    } else if (OB_FAIL(project_row(columns, cur_row_))) {                                       \
      LOG_WARN("project_row failed", K(columns), K(ret));                                       \
    } else if (OB_FAIL(scanner_.add_row(cur_row_))) {                                           \
      LOG_WARN("add_row failed", K(cur_row_), K(ret));                                          \
    }                                                                                           \
  } while (false)

#define CHECK_RESULT(checked, value) (checked ? (value ? "succeed" : "failed") : "checking")

      ADD_ROW("zone_check", CHECK_RESULT(root_inspection_->is_all_checked(), root_inspection_->is_zone_passed()));
      ADD_ROW(
          "sys_stat_check", CHECK_RESULT(root_inspection_->is_all_checked(), root_inspection_->is_sys_stat_passed()));
      ADD_ROW(
          "sys_param_check", CHECK_RESULT(root_inspection_->is_all_checked(), root_inspection_->is_sys_param_passed()));
      ADD_ROW("sys_table_schema_check",
          CHECK_RESULT(root_inspection_->is_all_checked(), root_inspection_->is_sys_table_schema_passed()));

      bool upgrade_job_passed = true;
      for (int64_t i = 0; i < UPGRADE_JOB_TYPE_COUNT; i++) {
        int tmp = OB_SUCCESS;
        ObRsJobType job_type = upgrade_job_type_array[i];
        if (job_type > JOB_TYPE_INVALID && job_type < JOB_TYPE_MAX) {
          if (OB_SUCCESS != (tmp = ObUpgradeUtils::check_upgrade_job_passed(job_type))) {
            LOG_WARN("fail to check upgrade job passed", K(tmp), K(job_type));
            upgrade_job_passed = false;
          }
          ADD_ROW(ObRsJobTableOperator::get_job_type_str(job_type),
              CHECK_RESULT(root_inspection_->is_all_checked(), (OB_SUCCESS == tmp)));
        }
      }

      ADD_ROW("all_check",
          CHECK_RESULT(root_inspection_->is_all_checked(), (root_inspection_->is_all_passed() && upgrade_job_passed)));

#undef CHECK_RESULT
#undef ADD_ROW
      if (OB_FAIL(ret)) {
        allocator_->free(cells);
        cells = NULL;
      }
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get_next_row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObUpgradeInspection::get_full_row(
    const share::schema::ObTableSchema* table, const char* name, const char* info, ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table || NULL == name || NULL == info) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(table), KP(name), KP(info), K(ret));
  } else {
    ADD_COLUMN(set_varchar, table, "name", name, columns);
    ADD_COLUMN(set_varchar, table, "info", info, columns);
  }

  return ret;
}
int ObPrimaryClusterInspection::init(share::schema::ObMultiVersionSchemaService& schema_service,
    share::ObPartitionTableOperator& pt_operator, ObZoneManager& zone_mgr, common::ObMySQLProxy& sql_proxy,
    ObServerManager& server_mgr)
{
  int ret = OB_SUCCESS;
  schema_service_ = &schema_service;
  pt_operator_ = &pt_operator;
  zone_mgr_ = &zone_mgr;
  sql_proxy_ = &sql_proxy;
  server_mgr_ = &server_mgr;
  inited_ = true;
  return ret;
}

int ObPrimaryClusterInspection::inspect(bool& passed, const char*& warning_info)
{
  int ret = OB_SUCCESS;
  passed = true;
  bool all_ddl_valid = false;
  bool all_partition_created = false;
  bool has_standby_restore_replica = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (common::PRIMARY_CLUSTER != ObClusterInfoGetter::get_cluster_type_v2()) {
  } else {
    if (OB_FAIL(check_all_ddl_replay(all_ddl_valid))) {
      LOG_WARN("failed to check all ddl valid", K(ret), K(passed));
    } else if (!all_ddl_valid) {
      LOG_ERROR("has not valid ddl, need process");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_all_partition_created(all_partition_created))) {
      LOG_WARN("failed to check all partition created", K(ret));
    } else if (!all_partition_created) {
      LOG_ERROR("has not create partition, may need drop table or tenant");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_has_replica_in_standby_restore(has_standby_restore_replica))) {
      LOG_WARN("failed to check has replica in restore", KR(ret));
    } else if (has_standby_restore_replica) {
      LOG_ERROR("has replica in restore, may need drop replica");
    }

    if (OB_FAIL(ret)) {
    } else {
      if (!all_partition_created || !all_ddl_valid || has_standby_restore_replica) {
        passed = false;
        warning_info = "primary cluster inspection failed";
        ROOTSERVICE_EVENT_ADD("inspector",
            get_task_name(),
            "result",
            passed,
            K(all_ddl_valid),
            K(all_partition_created),
            K(has_standby_restore_replica));

      } else {
        passed = true;
      }
    }
  }
  return ret;
}

int ObPrimaryClusterInspection::check_all_ddl_replay(bool& all_ddl_valid)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_DDL_HELPER_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", KR(ret));
    } else {
      int64_t schema_version = 0;
      int64_t schema_id = 0;
      int64_t ddl_type = 0;
      all_ddl_valid = true;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        all_ddl_valid = false;
        EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", schema_version, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "schema_id", schema_id, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "ddl_type", ddl_type, int64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get cell", K(ret), K(sql));
        } else if (share::schema::OB_DDL_MODIFY_INDEX_STATUS == ddl_type ||
                   share::schema::OB_DDL_MODIFY_GLOBAL_INDEX_STATUS == ddl_type) {
          LOG_ERROR("index not valid", K(ret), K(schema_id), K(schema_version), K(ddl_type));
          ROOTSERVICE_EVENT_ADD("inspector",
              get_task_name(),
              "inspector_task",
              "not valid index",
              "schema_version",
              schema_version,
              "schema_id",
              schema_id,
              "ddl_type",
              ddl_type);
        } else if (share::schema::OB_DDL_FINISH_SPLIT == ddl_type ||
                   share::schema::OB_DDL_FINISH_SPLIT_TABLEGROUP == ddl_type ||
                   share::schema::OB_DDL_SPLIT_PARTITION == ddl_type ||
                   share::schema::OB_DDL_PARTITIONED_TABLE == ddl_type ||
                   share::schema::OB_DDL_PARTITIONED_TABLEGROUP_TABLE == ddl_type ||
                   share::schema::OB_DDL_SPLIT_TABLEGROUP_PARTITION == ddl_type) {
          LOG_ERROR("partition split not finish", K(ret), K(schema_id), K(schema_version), K(ddl_type));
          ROOTSERVICE_EVENT_ADD("inspector",
              get_task_name(),
              "inspector_task",
              "has not finish split",
              "schema_version",
              schema_version,
              "schema_id",
              schema_id,
              "ddl_type",
              ddl_type);
        } else {
          LOG_ERROR("has other unexpected ddl not finish", K(ret), K(schema_id), K(schema_version), K(ddl_type));
          ROOTSERVICE_EVENT_ADD("inspector",
              get_task_name(),
              "inspector_task",
              "has other unexpected ddl not finish",
              "schema_version",
              schema_version,
              "schema_id",
              schema_id,
              "ddl_type",
              ddl_type);
        }
      }
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not be success", K(ret), K(sql));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
      }
    }
  }
  return ret;
}

int ObPrimaryClusterInspection::check_all_partition_created(bool& all_partition_created)
{
  int ret = OB_SUCCESS;
  all_partition_created = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(pt_operator_) || OB_ISNULL(schema_service_) || OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null value", KR(ret), KP(pt_operator_), KP(schema_service_), KP(server_mgr_));
  } else {
    ObPartitionTableIterator partition_iter;
    const bool ignore_checksum_error = false;
    if (OB_FAIL(partition_iter.init(*pt_operator_, *schema_service_, ignore_checksum_error))) {
      LOG_WARN("failed to init meta iter", K(ret));
    } else if (OB_FAIL(partition_iter.get_filters().filter_restore_replica())) {
      // the replica in restore can not be select as leader, and need filter.
      LOG_WARN("failed to set filter", KR(ret));
    } else if (OB_FAIL(partition_iter.get_filters().set_filter_permanent_offline(*server_mgr_))) {
      LOG_WARN("fail to set filter permanent offline", K(ret));
    } else {
      share::ObPartitionInfo part_info;
      while (OB_SUCC(ret) && OB_SUCC(partition_iter.next(part_info))) {
        const int64_t schema_id = part_info.get_table_id();
        const uint64_t tenant_id = part_info.get_tenant_id();
        bool is_restore = false;
        if (!part_info.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition info is invalid", K(ret), K(part_info));
        } else if (OB_FAIL(schema_service_->check_tenant_is_restore(NULL, tenant_id, is_restore))) {
          LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
        } else if (is_restore) {
          // skip
        } else if (0 == part_info.replica_count()) {
          all_partition_created = false;
          // if the replica num of part_info is zero,
          // it is a lack of relica during failover.
          if (is_tablegroup_id(schema_id)) {
            if (OB_SYS_TABLEGROUP_ID == extract_pure_id(schema_id)) {
              // SYS TABLEGROUP can not be binding. Should not be here
              LOG_ERROR("inner binding tablegroup partition not integrity, need drop",
                  K(ret),
                  K(part_info),
                  "tenant_id",
                  part_info.get_tenant_id());
              ROOTSERVICE_EVENT_ADD("inspector",
                  get_task_name(),
                  "inspector_task",
                  "tenant need drop",
                  "need drop tenant",
                  part_info.get_tenant_id(),
                  "partition_info",
                  part_info);
            } else {
              LOG_ERROR("user tablegroup partition not integrity, need drop", K(ret), K(part_info));
              ROOTSERVICE_EVENT_ADD("inspector",
                  get_task_name(),
                  "inspector_task",
                  "tablegroup need drop",
                  "tablegroup_id",
                  schema_id,
                  "partition_info",
                  part_info);
            }
          } else {
            if (is_inner_table(schema_id)) {
              LOG_ERROR("inner table partition not integrity, need drop",
                  K(ret),
                  K(part_info),
                  "teant_id",
                  part_info.get_tenant_id());
              ROOTSERVICE_EVENT_ADD("inspector",
                  get_task_name(),
                  "inspector_task",
                  "tenant need drop",
                  "need drop tenant",
                  part_info.get_tenant_id(),
                  "partition_info",
                  part_info);
            } else {
              LOG_ERROR("user table partition not integrity, need drop", K(ret), K(part_info));
              ROOTSERVICE_EVENT_ADD("inspector",
                  get_task_name(),
                  "inspector_task",
                  "table need drop",
                  "table_id",
                  schema_id,
                  "partition_info",
                  part_info);
            }
          }
        }
      }  // end while
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ret", K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
      }
    }
  }
  return ret;
}
int ObPrimaryClusterInspection::check_has_replica_in_standby_restore(bool& has_standby_restore_replica)
{
  int ret = OB_SUCCESS;
  has_standby_restore_replica = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(pt_operator_) || OB_ISNULL(schema_service_) || OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null value", KR(ret), KP(pt_operator_), KP(schema_service_), KP(server_mgr_));
  } else {
    ObFullPartitionTableIterator partition_iter;
    if (OB_FAIL(partition_iter.init(*pt_operator_, *schema_service_))) {
      LOG_WARN("failed to init meta iter", KR(ret));
    } else if (OB_FAIL(partition_iter.get_filters().set_filter_permanent_offline(*server_mgr_))) {
      LOG_WARN("fail to set filter permanent offline", KR(ret));
    } else {
      share::ObPartitionInfo part_info;
      while (OB_SUCC(ret) && OB_SUCC(partition_iter.next(part_info))) {
        if (OB_UNLIKELY(!part_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition info is invalid", KR(ret), K(part_info));
        } else {
          FOREACH_CNT_X(r, part_info.get_replicas_v2(), OB_SUCC(ret))
          {
            if (OB_ISNULL(r)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to get replica", KR(ret), K(part_info));
            } else if (r->in_standby_restore()) {
              has_standby_restore_replica = true;
              LOG_ERROR("replica in standby restore, need drop", KR(ret), KPC(r), K(part_info));
              ROOTSERVICE_EVENT_ADD(
                  "inspector", get_task_name(), "inspector_task", "replica in standby restore", "replica", *r);
            }
          }
        }
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
