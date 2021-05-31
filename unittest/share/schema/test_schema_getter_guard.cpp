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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#define private public
#include "share/schema/ob_schema_getter_guard.h"
#include "mock_schema_service.h"
#include "share/partition_table/fake_part_property_getter.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_replica_info.h"
#include "share/inner_table/ob_inner_table_schema.h"

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace share::host;

namespace common {

class TestSchemaGettGuard : public ::testing::Test {};

// TODO: move to SchemaTestUtils
#define GEN_TENANT_SCHEMA(tenant_schema, tenant_id, tenant_name, schema_version)    \
  tenant_schema.set_tenant_id(tenant_id);                                           \
  tenant_schema.set_tenant_name(tenant_name);                                       \
  tenant_schema.set_schema_version(schema_version);                                 \
  tenant_schema.set_locality("READOLY@zone1,FULL@zone1,READONLY@zone2,FULL@zone3"); \
  tenant_schema.add_zone("zone");                                                   \
  ObArray<share::ObZoneReplicaNumSet> sets;                                         \
  share::ObZoneReplicaNumSet set;                                                   \
  set.zone_ = ZONE1;                                                                \
  set.replica_num_set_.readonly_replica_num_ = 1;                                   \
  set.replica_num_set_.full_replica_num_ = 1;                                       \
  sets.push_back(set);                                                              \
  set.reset();                                                                      \
  set.zone_ = ZONE2;                                                                \
  set.replica_num_set_.readonly_replica_num_ = 1;                                   \
  sets.push_back(set);                                                              \
  set.reset();                                                                      \
  set.zone_ = ZONE3;                                                                \
  set.replica_num_set_.full_replica_num_ = 1;                                       \
  sets.push_back(set);                                                              \
  tenant_schema.set_zone_replica_num_array(sets);

#define GEN_SYS_VARIABLE_SCHEMA(sys_variable, tenant_id, schema_version) \
  sys_variable.set_tenant_id(tenant_id);                                 \
  sys_variable.set_schema_version(schema_version);                       \
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);

#define GEN_USER_SCHEMA(user_schema, tenant_id, user_id, user_name, schema_version) \
  user_schema.set_tenant_id(tenant_id);                                             \
  user_schema.set_user_id(user_id);                                                 \
  user_schema.set_user_name(user_name);                                             \
  user_schema.set_schema_version(schema_version);

#define GEN_DATABASE_SCHEMA(database_schema, tenant_id, database_id, database_name, schema_version) \
  database_schema.set_tenant_id(tenant_id);                                                         \
  database_schema.set_database_id(database_id);                                                     \
  database_schema.set_database_name(database_name);                                                 \
  database_schema.set_schema_version(schema_version);                                               \
  database_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);

#define GEN_TABLEGROUP_SCHEMA(tablegroup_schema, tenant_id, tablegroup_id, tablegroup_name, schema_version) \
  tablegroup_schema.set_tenant_id(tenant_id);                                                               \
  tablegroup_schema.set_tablegroup_id(tablegroup_id);                                                       \
  tablegroup_schema.set_tablegroup_name(tablegroup_name);                                                   \
  tablegroup_schema.set_schema_version(schema_version);

#define GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, table_id, table_name, table_type, schema_version) \
  table_schema.reset();                                                                                          \
  ObInnerTableSchema::all_tenant_meta_table_schema(table_schema);                                                \
  table_schema.set_tenant_id(tenant_id);                                                                         \
  table_schema.set_database_id(database_id);                                                                     \
  table_schema.set_table_id(table_id);                                                                           \
  table_schema.set_table_name(table_name);                                                                       \
  table_schema.set_table_type(table_type);                                                                       \
  table_schema.set_schema_version(schema_version);                                                               \
  table_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);

#define GEN_OUTLINE_SCHEMA(outline_schema, tenant_id, database_id, outline_id, name, sig, schema_version) \
  outline_schema.reset();                                                                                 \
  outline_schema.set_tenant_id(tenant_id);                                                                \
  outline_schema.set_database_id(database_id);                                                            \
  outline_schema.set_outline_id(outline_id);                                                              \
  outline_schema.set_name(name);                                                                          \
  outline_schema.set_signature(sig);                                                                      \
  outline_schema.set_schema_version(schema_version);

TEST_F(TestSchemaGettGuard, not_use_local_cache_when_iter)
{
  MockSchemaService schema_service;
  ObTenantSchema tenant_schema;
  ObDatabaseSchema database_schema;
  ObTablegroupSchema tablegroup_schema;
  ObTableSchema table_schema;
  ObOutlineInfo outline_schema;
  ObSysVariableSchema sys_variable;

  schema_service.init();
  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  const uint64_t db_id1 = combine_id(1, 1);
  const uint64_t tg_id1 = combine_id(1, 1);
  GEN_DATABASE_SCHEMA(database_schema, 1, db_id1, "database1", 0);
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, 1, tg_id1, "tg1", 0);
  GEN_TABLE_SCHEMA(table_schema, 1, db_id1, combine_id(1, 183), "__all_tenant_meta_table", USER_TABLE, 0);
  table_schema.set_tablegroup_id(tg_id1);
  GEN_OUTLINE_SCHEMA(outline_schema, 1, db_id1, combine_id(1, 1), "outline1", "signa", 0);

  schema_service.add_tenant_schema(tenant_schema, tenant_schema.get_schema_version());
  schema_service.add_sys_variable_schema(sys_variable, sys_variable.get_schema_version());
  schema_service.add_database_schema(database_schema, database_schema.get_schema_version());
  schema_service.add_tablegroup_schema(tablegroup_schema, tablegroup_schema.get_schema_version());
  schema_service.add_table_schema(table_schema, table_schema.get_schema_version());
  schema_service.add_outline_schema(outline_schema, outline_schema.get_schema_version());

  // get table schemas in tenant
  {
    ObSchemaGetterGuard schema_guard;
    schema_service.get_schema_guard(schema_guard);
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ObArray<const ObTableSchema*> tables;
    int64_t old_local_cache_cnt = schema_guard.id_schemas_.count();
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schemas_in_tenant(1, tables));
    ASSERT_EQ(1, tables.count());
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ASSERT_EQ(old_local_cache_cnt, schema_guard.id_schemas_.count());
    ASSERT_EQ(tables.at(0)->get_locality_str().empty(), true);
  }

  // get table schemas in database
  {
    ObSchemaGetterGuard schema_guard;
    schema_service.get_schema_guard(schema_guard);
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ObArray<const ObTableSchema*> tables;
    int64_t old_local_cache_cnt = schema_guard.id_schemas_.count();
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schemas_in_database(1, db_id1, tables));
    ASSERT_EQ(1, tables.count());
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ASSERT_EQ(old_local_cache_cnt, schema_guard.id_schemas_.count());
    ASSERT_EQ(tables.at(0)->get_locality_str().empty(), true);
  }

  // get table schemas in tablegroup
  {
    ObSchemaGetterGuard schema_guard;
    schema_service.get_schema_guard(schema_guard);
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ObArray<const ObTableSchema*> tables;
    int64_t old_local_cache_cnt = schema_guard.id_schemas_.count();
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schemas_in_tablegroup(1, tg_id1, tables));
    ASSERT_EQ(1, tables.count());
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ASSERT_EQ(old_local_cache_cnt, schema_guard.id_schemas_.count());
    ASSERT_EQ(tables.at(0)->get_locality_str().empty(), true);
  }

  // get database schemas in tenant
  {
    ObSchemaGetterGuard schema_guard;
    schema_service.get_schema_guard(schema_guard);
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ObArray<const ObDatabaseSchema*> dbs;
    int64_t old_local_cache_cnt = schema_guard.id_schemas_.count();
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_database_schemas_in_tenant(1, dbs));
    ASSERT_EQ(1, dbs.count());
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ASSERT_EQ(old_local_cache_cnt, schema_guard.id_schemas_.count());
  }

  // get outline schemas in database
  {
    ObSchemaGetterGuard schema_guard;
    schema_service.get_schema_guard(schema_guard);
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ObArray<const ObOutlineInfo*> outlines;
    int64_t old_local_cache_cnt = schema_guard.id_schemas_.count();
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_outline_infos_in_database(1, db_id1, outlines));
    ASSERT_EQ(1, outlines.count());
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ASSERT_EQ(old_local_cache_cnt, schema_guard.id_schemas_.count());
  }

  // test init && reset
  {
    ObSchemaGetterGuard schema_guard;
    schema_service.get_schema_guard(schema_guard);
    ASSERT_TRUE(schema_guard.use_local_cache_);
    schema_guard.use_local_cache_ = false;
    schema_guard.reset();
    ASSERT_TRUE(schema_guard.use_local_cache_);
  }
}

TEST_F(TestSchemaGettGuard, special_table_schema)
{
  MockSchemaService schema_service;
  ObTenantSchema tenant_schema;
  ObDatabaseSchema database_schema;
  ObTablegroupSchema tablegroup_schema;
  ObTableSchema table_schema;
  ObTableSchema table_schema2;
  ObOutlineInfo outline_schema;
  ObSysVariableSchema sys_variable;
  ObClusterInfo cluster_info;
  cluster_info.cluster_type_ = FOLLOWER;
  GCTX.set_cluster_info(cluster_info);

  schema_service.init();
  GEN_TENANT_SCHEMA(tenant_schema, 1, "sys_tenant", 0);
  GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1, 0);
  const uint64_t db_id1 = combine_id(1, 1);
  const uint64_t tg_id1 = combine_id(1, 1);
  GEN_DATABASE_SCHEMA(database_schema, 1, db_id1, "database1", 0);
  GEN_TABLEGROUP_SCHEMA(tablegroup_schema, 1, tg_id1, "tg1", 0);
  GEN_TABLE_SCHEMA(table_schema, 1, db_id1, combine_id(1, 183), "__all_tenant_meta_table", USER_TABLE, 0);
  table_schema.set_tablegroup_id(tg_id1);
  GEN_OUTLINE_SCHEMA(outline_schema, 1, db_id1, combine_id(1, 1), "outline1", "signa", 0);

  schema_service.add_tenant_schema(tenant_schema, tenant_schema.get_schema_version());
  schema_service.add_sys_variable_schema(sys_variable, sys_variable.get_schema_version());
  schema_service.add_database_schema(database_schema, database_schema.get_schema_version());
  schema_service.add_tablegroup_schema(tablegroup_schema, tablegroup_schema.get_schema_version());
  schema_service.add_table_schema(table_schema, table_schema.get_schema_version());
  schema_service.add_outline_schema(outline_schema, outline_schema.get_schema_version());
  table_schema.reset();
  const uint64_t usr_db_id1 = combine_id(1001, 1);
  const uint64_t usr_tg_id1 = combine_id(1001, 1);

  {
    // use tenant;
    ObTenantSchema tenant_schema2;
    GEN_TENANT_SCHEMA(tenant_schema2, 1001, "usr_tenant", 0);
    GEN_SYS_VARIABLE_SCHEMA(sys_variable, 1001, 0);
    GEN_DATABASE_SCHEMA(database_schema, 1001, usr_db_id1, "database1", 0);
    GEN_TABLEGROUP_SCHEMA(tablegroup_schema, 1001, usr_tg_id1, "tg1", 0);
    GEN_TABLE_SCHEMA(table_schema, 1001, usr_db_id1, combine_id(1001, 183), "__all_tenant_meta_table", USER_TABLE, 0);
    GEN_TABLE_SCHEMA(table_schema2, 1001, usr_db_id1, combine_id(1001, 18), "__all_dummy", USER_TABLE, 0);
    schema_service.add_tenant_schema(tenant_schema2, tenant_schema2.get_schema_version());
    schema_service.add_sys_variable_schema(sys_variable, sys_variable.get_schema_version());
    schema_service.add_database_schema(database_schema, database_schema.get_schema_version());
    schema_service.add_tablegroup_schema(tablegroup_schema, tablegroup_schema.get_schema_version());
    schema_service.add_table_schema(table_schema, table_schema.get_schema_version());
    schema_service.add_table_schema(table_schema2, table_schema.get_schema_version());
  }
  // get table schemas in tenant
  {
    ObSchemaGetterGuard schema_guard;
    schema_service.get_schema_guard(schema_guard);
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ObArray<const ObTableSchema*> tables;
    int64_t old_local_cache_cnt = schema_guard.id_schemas_.count();
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schemas_in_tenant(1001, tables));
    ASSERT_EQ(2, tables.count());
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ASSERT_EQ(old_local_cache_cnt, schema_guard.id_schemas_.count());
    // system tenant
    int64_t sys_table_id = combine_id(1, 183);
    const ObTableSchema* table_schema = NULL;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(sys_table_id, table_schema));
    ASSERT_TRUE(NULL != table_schema);
    ASSERT_EQ(true, table_schema->get_locality_str().empty());
    // normal tenant
    int64_t table_id = combine_id(1001, 183);
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(table_id, table_schema));
    ASSERT_TRUE(NULL != table_schema);
    ObString locality("FULL{1}@zone1,FULL{1}@zone2,FULL{1}@zone3");
    ASSERT_EQ(table_schema->get_locality_str(), locality);
    const ObIArray<ObZoneReplicaNumSet>& sets = table_schema->get_zone_replica_num_array();
    ASSERT_EQ(3, sets.count());
    ObReplicaNumSet tmp_set;
    tmp_set.full_replica_num_ = 1;
    for (int64_t i = 0; i < 3; i++) {
      ASSERT_EQ(1, sets.at(i).replica_num_set_.get_specific_replica_num());
    }
    ASSERT_EQ(ZONE1, sets.at(0).zone_);
    ASSERT_EQ(ZONE2, sets.at(1).zone_);
    ASSERT_EQ(ZONE3, sets.at(2).zone_);
  }
  // get simple schema
  {
    ObSchemaGetterGuard schema_guard;
    schema_service.get_schema_guard(schema_guard);
    ObArray<const ObTableSchema*> tables;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schemas(tables));
    ASSERT_EQ(3, tables.count());
    for (int64_t i = 0; i < 2; i++) {
      if (tables.at(i)->get_tenant_id() == 1) {
        ASSERT_EQ(true, tables.at(i)->get_locality_str().empty());
      } else if (183 == extract_pure_id(tables.at(i)->get_table_id())) {
        ASSERT_EQ(1001, tables.at(i)->get_tenant_id());
        ObString locality("FULL{1}@zone1,FULL{1}@zone2,FULL{1}@zone3");
        ASSERT_EQ(tables.at(i)->get_locality_str(), locality);
      } else {
        ASSERT_EQ(true, tables.at(i)->get_locality_str().empty());
      }
    }
  }
  // get table schemas in database
  {
    ObSchemaGetterGuard schema_guard;
    schema_service.get_schema_guard(schema_guard);
    ASSERT_TRUE(schema_guard.use_local_cache_);
    ObArray<const ObTableSchema*> tables;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schemas_in_database(1001, usr_db_id1, tables));
    ASSERT_EQ(2, tables.count());
    for (int64_t i = 0; i < 2; i++) {
      if (183 == extract_pure_id(tables.at(i)->get_table_id())) {
        ObString locality("FULL{1}@zone1,FULL{1}@zone2,FULL{1}@zone3");
        ASSERT_EQ(tables.at(i)->get_locality_str(), locality);
        const ObIArray<ObZoneReplicaNumSet>& sets = tables.at(i)->get_zone_replica_num_array();
        ASSERT_EQ(3, sets.count());
        ObReplicaNumSet set;
        set.full_replica_num_ = 1;
        for (int64_t i = 0; i < 3; i++) {
          ASSERT_EQ(1, sets.at(i).replica_num_set_.get_specific_replica_num());
        }
        ASSERT_EQ(ZONE1, sets.at(0).zone_);
        ASSERT_EQ(ZONE2, sets.at(1).zone_);
        ASSERT_EQ(ZONE3, sets.at(2).zone_);
      } else {
        ASSERT_EQ(true, tables.at(i)->get_locality_str().empty());
      }
    }
  }
}
}  // namespace common
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
