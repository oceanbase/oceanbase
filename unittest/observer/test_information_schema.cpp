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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "../share/schema/ob_schema_test_utils.cpp"
#include "observer/virtual_table/ob_show_database_status.h"
#include "observer/virtual_table/ob_information_partitions_table.h"
#include "observer/virtual_table/ob_information_table_constraints_table.h"
#include "observer/virtual_table/ob_tenant_all_tables.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "../share/schema/mock_schema_service.h"
#include "../share/partition_table/fake_part_property_getter.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server.h"

namespace oceanbase {
using namespace share::schema;
using ::testing::_;
using ::testing::Invoke;
namespace observer {
class TestInformationSchemaService : public ::testing::Test {
  typedef int (*schema_create_func)(share::schema::ObTableSchema& table_schema);

public:
  TestInformationSchemaService();
  virtual ~TestInformationSchemaService()
  {}
  virtual void SetUp();
  virtual void TearDown();
  void add_sys_tenant_info();
  void add_output_column_ids(
      common::ObVirtualTableScannerIterator& table_iter, schema_create_func create_func, ObTableSchema& table);
  void fill_table_schema(
      const uint64_t table_id, const uint64_t database_id, const ObString& table_name, ObTableSchema& table_schema);

  void fill_session();

protected:
  // ObSchemaManager schema_manager_;
  ObArenaAllocator allocator_;
  sql::ObSQLSessionInfo session_info_;
  ObVTableScanParam scan_param_;
};

TestInformationSchemaService::TestInformationSchemaService() : allocator_(ObModIds::TEST), session_info_()
{}

void TestInformationSchemaService::SetUp()
{
  uint32_t session_id = 1;
  uint32_t version = 0;
  uint64_t proxy_sessid = 10;
  OBSERVER.init_schema();
  OBSERVER.init_tz_info_mgr();
  ASSERT_EQ(OB_SUCCESS, session_info_.test_init(version, session_id, proxy_sessid, &allocator_));
  session_info_.set_user(OB_SYS_USER_NAME, OB_SYS_HOST_NAME, OB_SYS_USER_ID);
  session_info_.set_user_priv_set(OB_PRIV_ALL);
  session_info_.set_db_priv_set(OB_PRIV_ALL);
  ASSERT_EQ(OB_SUCCESS, session_info_.init_tenant(OB_SYS_TENANT_NAME, OB_SYS_TENANT_ID));
  session_info_.init_tenant(OB_SYS_TENANT_NAME, OB_SYS_TENANT_ID);
}

void TestInformationSchemaService::TearDown()
{}

void TestInformationSchemaService::add_sys_tenant_info()
{
  ObTenantSchema tenant_info;
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back("zone1"));
  FILL_TENANT_INFO(tenant_info, OB_SYS_TENANT_ID, "sys tenant", 1, zone_list, "zone", false, "this is sys tenant");
  // ASSERT_EQ(OB_SUCCESS, schema_manager_.add_tenant_info(tenant_info));
}

void TestInformationSchemaService::add_output_column_ids(
    common::ObVirtualTableScannerIterator& table_iter, schema_create_func create_func, ObTableSchema& table)
{
  ObArray<uint64_t> column_ids;
  (*create_func)(table);
  for (int64_t i = 0; i < table.get_column_count(); ++i) {
    const ObColumnSchemaV2* column = table.get_column_schema_by_idx(i);
    ASSERT_TRUE(NULL != column);
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(column->get_column_id()));
  }
  ASSERT_EQ(OB_SUCCESS, table_iter.set_output_column_ids(column_ids));
}

void TestInformationSchemaService::fill_table_schema(
    const uint64_t table_id, const uint64_t db_id, const ObString& table_name, ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  share::ObInnerTableSchema::all_server_schema(table_schema);
  table_schema.set_tenant_id(extract_tenant_id(table_id));
  table_schema.set_table_id(table_id);
  table_schema.set_database_id(db_id);
  table_schema.set_table_name(table_name);
  for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
       OB_SUCCESS == ret && iter != table_schema.column_end();
       ++iter) {
    share::schema::ObColumnSchemaV2* column_schema = const_cast<share::schema::ObColumnSchemaV2*>(*iter);
    column_schema->set_table_id(table_id);
    column_schema->set_tenant_id(extract_tenant_id(table_id));
  }
  ASSERT_EQ(ret, OB_SUCCESS);
}

class MockObSchemaGetterGuard : public ObSchemaGetterGuard {
public:
  int add_database_schema(ObDatabaseSchema* database_schema);
  int add_table_schema(ObTableSchema* table_schema);
  MOCK_METHOD2(get_database_schemas_in_tenant,
      int(const uint64_t tenant_id, common::ObIArray<const ObDatabaseSchema*>& database_schemas));
  MOCK_METHOD3(get_table_schemas_in_database,
      int(const uint64_t tenant_id, const uint64_t database_id, ObIArray<const ObTableSchema*>& table_schemas));
  int get_database_schemas_from_tenant(const uint64_t tenant_id, ObIArray<const ObDatabaseSchema*>& database_schemas);
  int get_table_schemas_from_database(
      const uint64_t tenant_id, const uint64_t database_id, ObIArray<const ObTableSchema*>& table_schemas);

private:
  ObSArray<const ObDatabaseSchema*> database_schemas_;
  ObSArray<const ObTableSchema*> table_schemas_;
};

int MockObSchemaGetterGuard::add_database_schema(ObDatabaseSchema* database_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(database_schemas_.push_back(database_schema))) {}
  return ret;
}

int MockObSchemaGetterGuard::add_table_schema(ObTableSchema* table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_schemas_.push_back(table_schema))) {}
  return ret;
}

int MockObSchemaGetterGuard::get_database_schemas_from_tenant(
    const uint64_t tenant_id, ObIArray<const ObDatabaseSchema*>& database_schemas)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < database_schemas_.count(); i++) {
    if (OB_FAIL(database_schemas.push_back(database_schemas_.at(i)))) {
      OB_LOG(WARN, "database_schemas fail");
    }
  }
  return ret;
}

int MockObSchemaGetterGuard::get_table_schemas_from_database(
    const uint64_t tenant_id, const uint64_t database_id, ObIArray<const ObTableSchema*>& table_schemas)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  UNUSED(database_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); i++) {
    if (OB_FAIL(table_schemas.push_back(table_schemas_.at(i)))) {
      OB_LOG(WARN, "");
    }
  }
  return ret;
}
TEST_F(TestInformationSchemaService, show_database_status)
{
  // schema_manager_.reset();

  uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  ObTenantSchema tenant_schema;
  tenant_schema.set_tenant_id(real_tenant_id);
  tenant_schema.set_tenant_name(OB_SYS_TENANT_NAME);
  tenant_schema.set_locality("");
  tenant_schema.add_zone("zone");
  tenant_schema.set_schema_version(1);
  ObSysVariableSchema sys_variable;
  sys_variable.set_tenant_id(real_tenant_id);
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  sys_variable.set_schema_version(1);
  MockSchemaService schema_service;
  schema_service.init();
  schema_service.add_tenant_schema(tenant_schema, tenant_schema.get_schema_version());
  schema_service.add_sys_variable_schema(sys_variable, sys_variable.get_schema_version());
  // add_sys_tenant_info();
  // MockObSchemaGetterGuard schema_guard;
  ObTableSchema table;
  ObSchemaGetterGuard schema_guard;
  schema_service.get_schema_guard(schema_guard, INT64_MAX);
  // ON_CALL(schema_guard, get_database_schemas_in_tenant(_, _))
  //  .WillByDefault(Invoke(&schema_guard, &MockObSchemaGetterGuard::get_database_schemas_from_tenant));
  ObShowDatabaseStatus database_status_iter;
  database_status_iter.set_scan_param(&scan_param_);
  database_status_iter.set_schema_guard(&schema_guard);
  database_status_iter.set_tenant_id(real_tenant_id);
  database_status_iter.set_allocator(&allocator_);
  add_output_column_ids(database_status_iter, share::ObInnerTableSchema::tenant_virtual_database_status_schema, table);
  database_status_iter.set_table_schema(&table);
  ObNewRow* row = NULL;
  database_status_iter.set_reserved_column_cnt(4);
  ASSERT_EQ(OB_SUCCESS, database_status_iter.open());

  ASSERT_EQ(OB_ITER_END, database_status_iter.get_next_row(row));

  ObDatabaseSchema database_schema;
  uint64_t db_id = 50000;
  FILL_DATABASE_SCHEMA(database_schema, real_tenant_id, db_id, "db1", "this is db1");
  // ASSERT_EQ(OB_SUCCESS, schema_guard.add_database_schema(&database_schema));
  schema_service.add_database_schema(database_schema, database_schema.get_schema_version());

  database_status_iter.reset();
  table.reset();
  database_status_iter.set_schema_guard(&schema_guard);
  database_status_iter.set_tenant_id(real_tenant_id);
  database_status_iter.set_allocator(&allocator_);
  add_output_column_ids(database_status_iter, share::ObInnerTableSchema::tenant_virtual_database_status_schema, table);
  database_status_iter.set_table_schema(&table);
  database_status_iter.set_reserved_column_cnt(4);
  ASSERT_EQ(OB_SUCCESS, database_status_iter.open());

  row = NULL;
  ASSERT_EQ(OB_SUCCESS, database_status_iter.get_next_row(row));
  ASSERT_TRUE(row != NULL);
}

TEST_F(TestInformationSchemaService, partitions)
{
  // schema_manager_.reset();
  uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  MockSchemaService schema_service;
  schema_service.init();
  ObTenantSchema tenant_schema;
  tenant_schema.set_tenant_id(real_tenant_id);
  tenant_schema.set_tenant_name(OB_SYS_TENANT_NAME);
  tenant_schema.set_locality("");
  tenant_schema.add_zone("zone");
  tenant_schema.set_schema_version(1);
  schema_service.add_tenant_schema(tenant_schema, tenant_schema.get_schema_version());
  ObSysVariableSchema sys_variable;
  sys_variable.set_tenant_id(real_tenant_id);
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  sys_variable.set_schema_version(1);
  schema_service.add_sys_variable_schema(sys_variable, sys_variable.get_schema_version());
  // add_sys_tenant_info();
  // MockObSchemaGetterGuard schema_guard;
  ObTableSchema table;
  ObSchemaGetterGuard schema_guard;
  schema_service.get_schema_guard(schema_guard, INT64_MAX);
  // ON_CALL(schema_guard, get_database_schemas_in_tenant(_, _))
  //  .WillByDefault(Invoke(&schema_guard, &MockObSchemaGetterGuard::get_database_schemas_from_tenant));
  // ON_CALL(schema_guard, get_table_schemas_in_database(_,_, _))
  //   .WillByDefault(Invoke(&schema_guard, &MockObSchemaGetterGuard::get_table_schemas_from_database));
  ObInfoSchemaPartitionsTable partitions_iter;
  partitions_iter.set_scan_param(&scan_param_);
  partitions_iter.set_schema_guard(&schema_guard);
  partitions_iter.set_tenant_id(real_tenant_id);
  partitions_iter.set_allocator(&allocator_);
  add_output_column_ids(partitions_iter, share::ObInnerTableSchema::partitions_schema, table);
  partitions_iter.set_table_schema(&table);
  share::FakePartPropertyGetter property_getter;
  share::ObPartitionTableOperator partition_table_operator(property_getter);

  // no database
  ObNewRow* row = NULL;
  partitions_iter.set_reserved_column_cnt(25);
  ASSERT_EQ(OB_SUCCESS, partitions_iter.open());
  ASSERT_EQ(OB_ITER_END, partitions_iter.get_next_row(row));

  //
  partitions_iter.reset();
  ObDatabaseSchema database_schema_1;
  ObTableSchema null_table_schema;
  uint64_t db_id = 50000;
  FILL_DATABASE_SCHEMA(database_schema_1, real_tenant_id, db_id, "db_1", "this is db1");
  // ASSERT_EQ(OB_SUCCESS, schema_guard.add_database_schema(&database_schema_1));
  schema_service.add_database_schema(database_schema_1, database_schema_1.get_schema_version());
  // ASSERT_EQ(OB_SUCCESS, schema_manager_.add_new_database_schema(database_schema_1));
  partitions_iter.set_schema_guard(&schema_guard);
  partitions_iter.set_tenant_id(real_tenant_id);
  partitions_iter.set_allocator(&allocator_);
  partitions_iter.set_table_schema(&null_table_schema);
  row = NULL;
  partitions_iter.set_reserved_column_cnt(25);
  ASSERT_EQ(OB_SUCCESS, partitions_iter.open());
  ASSERT_EQ(OB_ITER_END, partitions_iter.get_next_row(row));
  ASSERT_TRUE(row == NULL);

  //
  partitions_iter.reset();
  uint64_t table_id = 50000;
  ObString table_name("table_1");
  ObTableSchema table_schema_1;
  fill_table_schema(
      combine_id(real_tenant_id, table_id), combine_id(real_tenant_id, db_id), table_name, table_schema_1);
  table_schema_1.set_part_level(PARTITION_LEVEL_ZERO);
  // ASSERT_EQ(OB_SUCCESS, schema_guard.add_table_schema(&table_schema_1));
  schema_service.add_table_schema(table_schema_1, table_schema_1.get_schema_version());
  // ASSERT_EQ(OB_SUCCESS, schema_manager_.add_new_table_schema(table_schema_1));

  partitions_iter.set_schema_guard(&schema_guard);
  partitions_iter.set_tenant_id(real_tenant_id);
  partitions_iter.set_allocator(&allocator_);
  partitions_iter.set_table_schema(&table_schema_1);
  row = NULL;
  partitions_iter.set_reserved_column_cnt(25);
  ASSERT_EQ(OB_SUCCESS, partitions_iter.open());
  ASSERT_EQ(OB_SUCCESS, partitions_iter.get_next_row(row));
  ASSERT_TRUE(row != NULL);
}

TEST_F(TestInformationSchemaService, table_constraints)
{
  // schema_manager_.reset();
  ObArenaAllocator allocator(ObModIds::TEST);
  uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  MockSchemaService schema_service;
  schema_service.init();
  ObTenantSchema tenant_schema;
  tenant_schema.set_tenant_id(real_tenant_id);
  tenant_schema.set_tenant_name(OB_SYS_TENANT_NAME);
  tenant_schema.set_schema_version(1);
  tenant_schema.set_locality("");
  tenant_schema.add_zone("zone");
  schema_service.add_tenant_schema(tenant_schema, tenant_schema.get_schema_version());
  ObSysVariableSchema sys_variable;
  sys_variable.set_tenant_id(real_tenant_id);
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  sys_variable.set_schema_version(1);
  schema_service.add_sys_variable_schema(sys_variable, sys_variable.get_schema_version());
  // add_sys_tenant_info();
  // MockObSchemaGetterGuard schema_guard;
  ObTableSchema table;
  ObSchemaGetterGuard schema_guard;
  schema_service.get_schema_guard(schema_guard, INT64_MAX);
  // ON_CALL(schema_guard, get_database_schemas_in_tenant(_, _))
  //  .WillByDefault(Invoke(&schema_guard, &MockObSchemaGetterGuard::get_database_schemas_from_tenant));
  // ON_CALL(schema_guard, get_table_schemas_in_database(_,_, _))
  //   .WillByDefault(Invoke(&schema_guard, &MockObSchemaGetterGuard::get_table_schemas_from_database));

  ObInfoSchemaTableConstraintsTable table_constrints_iter;
  table_constrints_iter.set_scan_param(&scan_param_);
  table_constrints_iter.set_schema_guard(&schema_guard);
  table_constrints_iter.set_tenant_id(real_tenant_id);
  table_constrints_iter.set_allocator(&allocator);
  add_output_column_ids(table_constrints_iter, share::ObInnerTableSchema::table_constraints_schema, table);
  table_constrints_iter.set_table_schema(&table);
  // no database
  ObNewRow* row = NULL;
  table_constrints_iter.set_reserved_column_cnt(6);
  ASSERT_EQ(OB_SUCCESS, table_constrints_iter.open());
  ASSERT_EQ(OB_ITER_END, table_constrints_iter.get_next_row(row));

  //
  table_constrints_iter.reset();
  ObDatabaseSchema database_schema_1;
  ObTableSchema null_table_schema;
  uint64_t db_id = 50000;
  FILL_DATABASE_SCHEMA(database_schema_1, real_tenant_id, db_id, "db_1", "this is db1");
  schema_service.add_database_schema(database_schema_1, database_schema_1.get_schema_version());
  // ASSERT_EQ(OB_SUCCESS, schema_guard.add_database_schema(&database_schema_1));
  // ASSERT_EQ(OB_SUCCESS, schema_manager_.add_new_database_schema(database_schema_1));
  table_constrints_iter.set_schema_guard(&schema_guard);
  table_constrints_iter.set_tenant_id(real_tenant_id);
  table_constrints_iter.set_allocator(&allocator);
  table_constrints_iter.set_table_schema(&null_table_schema);
  row = NULL;
  table_constrints_iter.set_reserved_column_cnt(6);
  ASSERT_EQ(OB_SUCCESS, table_constrints_iter.open());
  ASSERT_EQ(OB_ITER_END, table_constrints_iter.get_next_row(row));
  ASSERT_TRUE(row == NULL);

  //
  table_constrints_iter.reset();
  uint64_t table_id = 50000;
  ObString table_name("table_1");
  // table has at least one constraint primary key
  ObTableSchema table_schema_1;
  fill_table_schema(
      combine_id(real_tenant_id, table_id), combine_id(real_tenant_id, db_id), table_name, table_schema_1);
  // ASSERT_EQ(OB_SUCCESS, schema_guard.add_table_schema(&table_schema_1));
  schema_service.add_table_schema(table_schema_1, table_schema_1.get_schema_version());
  // ASSERT_EQ(OB_SUCCESS, schema_manager_.add_new_table_schema(table_schema_1));
  table_constrints_iter.set_schema_guard(&schema_guard);
  table_constrints_iter.set_tenant_id(real_tenant_id);
  table_constrints_iter.set_allocator(&allocator_);
  table_constrints_iter.set_table_schema(&table_schema_1);
  // add
  row = NULL;
  table_constrints_iter.set_reserved_column_cnt(6);
  ASSERT_EQ(OB_SUCCESS, table_constrints_iter.open());
  ASSERT_EQ(OB_SUCCESS, table_constrints_iter.get_next_row(row));
  ASSERT_TRUE(row != NULL);
}

TEST_F(TestInformationSchemaService, tenant_all_tables)
{
  // schema_manager_.reset();
  //  uint64_t real_tenant_id = OB_SYS_TENANT_ID;
  //  add_sys_tenant_info();
  //
  //  ObTenantAllTables tenant_all_table_iter;
  //  tenant_all_table_iter.set_schema_guard(&schema_manager_);
  //  tenant_all_table_iter.set_tenant_id(real_tenant_id);
  //  tenant_all_table_iter.set_allocator(&allocator_);
  //  tenant_all_table_iter.set_session(&session_info_);
  //  add_output_column_ids(tenant_all_table_iter, share::ObInnerTableSchema::tenant_virtual_all_table_schema);
  //
  //  //no database
  //  ObNewRow *row = NULL;
  //  ASSERT_EQ(OB_ITER_END, tenant_all_table_iter.get_next_row(row));
  //
  //  ObDatabaseSchema database_schema_1;
  //  uint64_t db_id = 50000;
  //  FILL_DATABASE_SCHEMA(database_schema_1, real_tenant_id, db_id, "db_1", "this is db1");
  //  ASSERT_EQ(OB_SUCCESS, schema_manager_.add_new_database_schema(database_schema_1));
  //
  //  //
  //  row = NULL;
  //  ASSERT_EQ(OB_ITER_END, tenant_all_table_iter.get_next_row(row));
  //  ASSERT_TRUE(row == NULL);
  //
  //  uint64_t table_id = 50000;
  //  ObString table_name("table_1");
  //  //table has at least one constraint primary key
  //  ObTableSchema table_schema_1;
  //  fill_table_schema(combine_id(real_tenant_id, table_id),
  //                    combine_id(real_tenant_id, db_id),
  //                    table_name,
  //                    table_schema_1);
  //  ASSERT_EQ(OB_SUCCESS, schema_manager_.add_new_table_schema(table_schema_1));
  //
  //  //add
  //  row = NULL;
  //  ASSERT_EQ(OB_SUCCESS, tenant_all_table_iter.get_next_row(row));
  //  ASSERT_TRUE(row != NULL);
}

}  // namespace observer
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_observer.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
