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

#ifndef SCHEMA_TEST_UTILS_H_
#define SCHEMA_TEST_UTILS_H_

#define private public
#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"


namespace oceanbase
{
using namespace common;
namespace common
{
}
namespace share
{
namespace schema
{

#define GEN_TENANT_SCHEMA(tenant_schema, tenant_id, tenant_name, schema_version) \
  tenant_schema.set_tenant_id(tenant_id); \
  tenant_schema.set_tenant_name(tenant_name); \
  tenant_schema.set_schema_version(schema_version);

#define GEN_USER_SCHEMA(user_schema, tenant_id, user_id, user_name, schema_version) \
  user_schema.set_tenant_id(tenant_id); \
  user_schema.set_user_id(user_id); \
  user_schema.set_user_name(user_name); \
  user_schema.set_host(OB_DEFAULT_HOST_NAME); \
  user_schema.set_schema_version(schema_version);

#define GEN_DATABASE_SCHEMA(database_schema, tenant_id, database_id, database_name, schema_version) \
  database_schema.set_tenant_id(tenant_id); \
  database_schema.set_database_id(database_id); \
  database_schema.set_database_name(database_name); \
  database_schema.set_schema_version(schema_version); \
  database_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);

#define GEN_TABLEGROUP_SCHEMA(tablegroup_schema, tenant_id, tablegroup_id, tablegroup_name, schema_version) \
  tablegroup_schema.set_tenant_id(tenant_id); \
  tablegroup_schema.set_tablegroup_id(tablegroup_id); \
  tablegroup_schema.set_tablegroup_name(tablegroup_name); \
  tablegroup_schema.set_schema_version(schema_version);

#define GEN_TABLE_SCHEMA(table_schema, tenant_id, database_id, table_id, table_name, table_type, schema_version) \
  table_schema.reset(); \
  table_schema.set_tenant_id(tenant_id); \
  table_schema.set_database_id(database_id); \
  table_schema.set_table_id(table_id); \
  table_schema.set_table_name(table_name); \
  table_schema.set_table_type(table_type); \
  table_schema.set_schema_version(schema_version); \
  table_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);

#define GEN_OUTLINE_SCHEMA(outline_schema, tenant_id, database_id, outline_id, name, sig, schema_version) \
  outline_schema.reset(); \
  outline_schema.set_tenant_id(tenant_id); \
  outline_schema.set_database_id(database_id); \
  outline_schema.set_outline_id(outline_id); \
  outline_schema.set_name(name); \
  outline_schema.set_signature(sig); \
  outline_schema.set_schema_version(schema_version);

#define GEN_DB_PRIV(db_priv, tenant_id, user_id, database_name, priv_set, schema_version) \
  db_priv.reset(); \
  db_priv.set_tenant_id(tenant_id); \
  db_priv.set_user_id(user_id); \
  db_priv.set_database_name(database_name); \
  db_priv.set_priv_set(priv_set); \
  db_priv.set_schema_version(schema_version);

#define GEN_TABLE_PRIV(table_priv, tenant_id, user_id, database_name, table_name, priv_set, schema_version) \
  table_priv.reset(); \
  table_priv.set_tenant_id(tenant_id); \
  table_priv.set_user_id(user_id); \
  table_priv.set_database_name(database_name); \
  table_priv.set_table_name(table_name); \
  table_priv.set_priv_set(priv_set); \
  table_priv.set_schema_version(schema_version);

class SchemaTestUtils
{
public:
  SchemaTestUtils() {}
  virtual ~SchemaTestUtils() {}

  static bool equal_tenant_schema(
      const ObTenantSchema &a,
      const ObTenantSchema &b)
  {
    return a.get_tenant_id() == b.get_tenant_id() &&
      a.get_tenant_name_str() == b.get_tenant_name_str() &&
      a.get_schema_version() == b.get_schema_version();
  }
  static bool equal_user_schema(
      const ObUserInfo &a,
      const ObUserInfo &b)
  {
    return a.get_tenant_id() == b.get_tenant_id() &&
      a.get_user_id() == b.get_user_id() &&
      a.get_user_name_str() == b.get_user_name_str() &&
      a.get_schema_version() == b.get_schema_version();
  }
  static bool equal_database_schema(
      const ObDatabaseSchema &a,
      const ObDatabaseSchema &b)
  {
    return a.get_tenant_id() == b.get_tenant_id() &&
      a.get_database_id() == b.get_database_id() &&
      a.get_database_name_str() == b.get_database_name_str() &&
      a.get_name_case_mode() == b.get_name_case_mode() &&
      a.get_schema_version() == b.get_schema_version();
  }
  static bool equal_tablegroup_schema(
      const ObTablegroupSchema &a,
      const ObTablegroupSchema &b)
  {
    return a.get_tenant_id() == b.get_tenant_id() &&
      a.get_tablegroup_id() == b.get_tablegroup_id() &&
      a.get_tablegroup_name_str() == b.get_tablegroup_name_str() &&
      a.get_schema_version() == b.get_schema_version();
  }
  static bool equal_table_schema(
      const ObTableSchema &a,
      const ObTableSchema &b)
  {
    return a.get_tenant_id() == b.get_tenant_id() &&
      a.get_database_id() == b.get_database_id() &&
      a.get_table_id() == b.get_table_id() &&
      a.get_table_name_str() == b.get_table_name_str() &&
      a.get_name_case_mode() == b.get_name_case_mode() &&
      a.get_schema_version() == b.get_schema_version();
  }
  static bool equal_outline_schema(
      const ObOutlineInfo &a,
      const ObOutlineInfo &b)
  {
    return a.get_tenant_id() == b.get_tenant_id() &&
      a.get_database_id() == b.get_database_id() &&
      a.get_outline_id() == b.get_outline_id() &&
      a.get_name_str() == b.get_name_str() &&
      a.get_signature_str() == b.get_signature_str() &&
      a.get_schema_version() == b.get_schema_version();
  }
  static bool equal_db_priv(
      const ObDBPriv &a,
      const ObDBPriv &b)
  {
    return a.get_tenant_id() == b.get_tenant_id() &&
      a.get_user_id() == b.get_user_id() &&
      a.get_database_name_str() == b.get_database_name_str() &&
      a.get_priv_set() == b.get_priv_set() &&
      a.get_schema_version() == b.get_schema_version();
  }
  static bool equal_table_priv(
      const ObTablePriv &a,
      const ObTablePriv &b)
  {
    return a.get_tenant_id() == b.get_tenant_id() &&
      a.get_user_id() == b.get_user_id() &&
      a.get_database_name_str() == b.get_database_name_str() &&
      a.get_table_name_str() == b.get_table_name_str() &&
      a.get_priv_set() == b.get_priv_set() &&
      a.get_schema_version() == b.get_schema_version();
  }

private:
};

} // schema
} // share
} // oceanbase

#endif /* SCHEMA_TEST_UTILS_H_ */
