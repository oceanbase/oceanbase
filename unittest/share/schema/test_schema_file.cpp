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

#define private public
#define protected public
#include <gtest/gtest.h>
#include <stdio.h>
#include "db_initializer.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_manager.h"
#include "share/schema/ob_schema_file.h"
#include "lib/string/ob_sql_string.h"
#include "ob_schema_test_utils.cpp"
#include <iostream>

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;
using namespace std;
class TestSchemaFile : public ::testing::Test
{
   public:
   virtual void SetUp() {}
   virtual void TearDown() {}
   static void SeuUpTestCase() {}
   static void TearDownTestCase() {}

   void make_tenant_schema(ObTenantSchema& ts_schema, const uint64_t ts_id,
       const char* ts_name, const char* comment_ptr)
   {
     ts_schema.set_tenant_id(ts_id);
     if (ts_name != NULL)
     {
       ts_schema.set_tenant_name(ts_name);
     }
     if (comment_ptr != NULL)
     {
       ts_schema.set_comment(comment_ptr);
     }
   }

   void make_database_schema(ObDatabaseSchema& db_schema, const uint64_t db_id,
       const uint64_t db_tenant_id, const char* db_name, const char* comment_ptr)
   {
     db_schema.set_database_id(db_id);
     db_schema.set_tenant_id(db_tenant_id);
     if (db_name != NULL)
     {
       db_schema.set_database_name(db_name);
     }
     if (comment_ptr != NULL)
     {
       db_schema.set_comment(comment_ptr);
     }
   }

   void make_tablegroup_schema(ObTablegroupSchema& tg_schema, const uint64_t tg_id,
       const uint64_t tg_tenant_id, const char* tg_name, const char* comment_ptr)
   {
     tg_schema.set_tablegroup_id(tg_id);
     tg_schema.set_tenant_id(tg_tenant_id);
     if (tg_name != NULL)
     {
       tg_schema.set_tablegroup_name(tg_name);
     }
     if (comment_ptr != NULL)
     {
       tg_schema.set_comment(comment_ptr);
     }
   }
};

TEST_F(TestSchemaFile, schema_ini)
{
  obsys::ObSysConfig c1;
  //use local schema arena
  ObSchemaManager *schema_manager = new ObSchemaManager();
  ObSchemaFile* schema_file =  new ObSchemaFile(*schema_manager);
  bool is_in_test_model = true;
  schema_file->set_test_model(is_in_test_model);
  schema_file->parse_from_file("schema_ini/schema.ini", c1);
  ASSERT_EQ(2, schema_file->table_schema_array_.count());
  ASSERT_EQ(3, schema_file->tablegroup_array_.count());
  ASSERT_EQ(4, schema_file->database_array_.count());

  ObDatabaseSchema db_schema;
  make_database_schema(db_schema, 1,1,"db1","this is db1");
  ObSchemaTestUtils::assert_db_equal(db_schema, schema_file->database_array_[0]);
  db_schema.reset();
  make_database_schema(db_schema, 2,1,"db2","this is db2");
  ObSchemaTestUtils::assert_db_equal(db_schema, schema_file->database_array_[1]);
  db_schema.reset();
  make_database_schema(db_schema, 3,3,"db3","this is db3");
  ObSchemaTestUtils::assert_db_equal(db_schema, schema_file->database_array_[2]);
  db_schema.reset();
  make_database_schema(db_schema, 4,4,"db4",NULL);
  ObSchemaTestUtils::assert_db_equal(db_schema, schema_file->database_array_[3]);

  ObTablegroupSchema tg_schema;
  make_tablegroup_schema(tg_schema,1,1,"tb1", NULL);
  ObSchemaTestUtils::assert_tg_equal(tg_schema, schema_file->tablegroup_array_[0]);
  tg_schema.reset();
  make_tablegroup_schema(tg_schema,2,3,"tb2", "jingqianshidashen");
  ObSchemaTestUtils::assert_tg_equal(tg_schema, schema_file->tablegroup_array_[1]);

  ObTenantSchema tenant;
  make_tenant_schema(tenant,1,"t1", NULL);
  ObSchemaTestUtils::assert_tn_equal(tenant, schema_file->tenant_array_[0]);
  tenant.reset();
  make_tenant_schema(tenant,2,"t2", NULL);
  ObSchemaTestUtils::assert_tn_equal(tenant, schema_file->tenant_array_[1]);
  tenant.reset();
  make_tenant_schema(tenant,3,"t3", NULL);
  ObSchemaTestUtils::assert_tn_equal(tenant, schema_file->tenant_array_[2]);
  tenant.reset();
  make_tenant_schema(tenant,4,"t4", NULL);
  ObSchemaTestUtils::assert_tn_equal(tenant, schema_file->tenant_array_[3]);
  tenant.reset();
  make_tenant_schema(tenant,5,"t5", NULL);
  ObSchemaTestUtils::assert_tn_equal(tenant, schema_file->tenant_array_[4]);
  tenant.reset();

  //for collect info table schema
  ObTableSchema collect_info_schema;
  collect_info_schema.set_tenant_id(1);
  collect_info_schema.set_table_name(ObString::make_string("collect_info"));
  collect_info_schema.set_table_id(1001);
  collect_info_schema.set_tablegroup_id(1);
  collect_info_schema.set_database_id(1);
  collect_info_schema.set_table_type(static_cast<ObTableType>(1));
  collect_info_schema.set_compress_func_name(ObString::make_string("lalala"));
  collect_info_schema.set_block_size(80);
  collect_info_schema.get_part_expr().set_part_num(1);
  collect_info_schema.set_max_used_column_id(39);
  collect_info_schema.set_rowkey_column_num(3);
  collect_info_schema.set_rowkey_split_pos(8);

  ObColumnSchemaV2 column_schema;
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(21);
  column_schema.set_column_name(ObString::make_string("gm_create"));
  column_schema.set_data_type(ObDateTimeType);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(22);
  column_schema.set_column_name(ObString::make_string("gm_modified"));
  column_schema.set_data_type(ObDateTimeType);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(25);
  column_schema.set_column_name(ObString::make_string("title"));
  column_schema.set_data_type(ObVarcharType);
  column_schema.set_data_length(256);
  column_schema.set_join_table_id(1002);
  column_schema.set_join_column_id(16);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(27);
  column_schema.set_column_name(ObString::make_string("owner_id"));
  column_schema.set_data_type(ObIntType);
  column_schema.set_join_table_id(1002);
  column_schema.set_join_column_id(18);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(28);
  column_schema.set_column_name(ObString::make_string("owner_nick"));
  column_schema.set_data_type(ObVarcharType);
  column_schema.set_data_length(32);
  column_schema.set_join_table_id(1002);
  column_schema.set_join_column_id(19);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(29);
  column_schema.set_column_name(ObString::make_string("price"));
  column_schema.set_data_type(ObIntType);
  column_schema.set_join_table_id(1002);
  column_schema.set_join_column_id(20);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(31);
  column_schema.set_column_name(ObString::make_string("proper_xml"));
  column_schema.set_data_type(ObVarcharType);
  column_schema.set_join_table_id(1002);
  column_schema.set_join_column_id(22);
  column_schema.set_data_length(2048);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(33);
  column_schema.set_column_name(ObString::make_string("collector_count"));
  column_schema.set_data_type(ObIntType);
  column_schema.set_join_table_id(1002);
  column_schema.set_join_column_id(24);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(34);
  column_schema.set_column_name(ObString::make_string("collect_count"));
  column_schema.set_data_type(ObIntType);
  column_schema.set_join_table_id(1002);
  column_schema.set_join_column_id(25);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_column_id(36);
  column_schema.set_column_name(ObString::make_string("user_id"));
  column_schema.set_rowkey_position(1);
  column_schema.set_data_type(ObIntType);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_column_id(37);
  column_schema.set_column_name(ObString::make_string("item_type"));
  column_schema.set_rowkey_position(2);
  column_schema.set_data_type(ObIntType);
  collect_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_column_id(38);
  column_schema.set_column_name(ObString::make_string("item_id"));
  column_schema.set_rowkey_position(3);
  column_schema.set_data_type(ObIntType);
  collect_info_schema.add_column(column_schema);

  JoinInfo join_info;
  snprintf(join_info.left_table_name_, common::OB_MAX_TABLE_NAME_LENGTH, "%s", "collect_info");
  join_info.left_table_id_ = 1001;
  snprintf(join_info.left_column_name_, common::OB_MAX_TABLE_NAME_LENGTH, "%s", "item_type");
  join_info.left_column_id_ = 37;

  snprintf(join_info.right_table_name_, common::OB_MAX_TABLE_NAME_LENGTH, "%s", "item_info");
  join_info.right_table_id_ = 1002;
  snprintf(join_info.right_column_name_, common::OB_MAX_TABLE_NAME_LENGTH, "%s", "item_type");
  join_info.right_column_id_ = 27;
  collect_info_schema.add_join_info(join_info);

  JoinInfo join_info1;
  snprintf(join_info1.left_table_name_, common::OB_MAX_TABLE_NAME_LENGTH, "%s", "collect_info");
  join_info1.left_table_id_ = 1001;
  snprintf(join_info1.left_column_name_, common::OB_MAX_TABLE_NAME_LENGTH, "%s", "item_id");
  join_info1.left_column_id_ = 38;

  snprintf(join_info1.right_table_name_, common::OB_MAX_TABLE_NAME_LENGTH, "%s", "item_info");
  join_info1.right_table_id_ = 1002;
  snprintf(join_info1.right_column_name_, common::OB_MAX_TABLE_NAME_LENGTH, "%s", "item_id");
  join_info1.right_column_id_ = 28;

  collect_info_schema.add_join_info(join_info1);

  //for item info table schema
  ObTableSchema item_info_schema;
  item_info_schema.set_tenant_id(1);
  item_info_schema.set_table_name(ObString::make_string("item_info"));
  item_info_schema.table_id_ = 1002;
  item_info_schema.tablegroup_id_ = 1;
  item_info_schema.database_id_ = 1;
  item_info_schema.table_type_ = static_cast<ObTableType>(1);
  item_info_schema.block_size_ = 800;
  item_info_schema.part_expr_.part_num_ = 1;
  item_info_schema.max_used_column_id_ = 28;
  item_info_schema.rowkey_column_num_ = 2;
  item_info_schema.rowkey_split_pos_ = 0;
  //item_info_schema.set_expr----=-

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(16);
  column_schema.set_column_name(ObString::make_string("title"));
  column_schema.set_data_type(ObVarcharType);
  column_schema.set_data_length(256);
  item_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(18);
  column_schema.set_column_name(ObString::make_string("owner_id"));
  column_schema.set_data_type(ObIntType);
  item_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(19);
  column_schema.set_column_name(ObString::make_string("owner_nick"));
  column_schema.set_data_type(ObVarcharType);
  column_schema.set_data_length(32);
  item_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(20);
  column_schema.set_column_name(ObString::make_string("price"));
  column_schema.set_data_type(ObIntType);
  item_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(22);
  column_schema.set_column_name(ObString::make_string("proper_xml"));
  column_schema.set_data_type(ObVarcharType);
  column_schema.set_data_length(2048);
  item_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(24);
  column_schema.set_column_name(ObString::make_string("collector_count"));
  column_schema.set_data_type(ObIntType);
  item_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(25);
  column_schema.set_column_name(ObString::make_string("collect_count"));
  column_schema.set_data_type(ObIntType);
  item_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_column_id(27);
  column_schema.set_column_name(ObString::make_string("item_type"));
  column_schema.set_rowkey_position(1);
  column_schema.set_data_type(ObIntType);
  item_info_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(28);
  column_schema.set_rowkey_position(2);
  column_schema.set_column_name(ObString::make_string("item_id"));
  column_schema.set_data_type(ObIntType);
  item_info_schema.add_column(column_schema);

  ObSchemaTestUtils::expect_table_eq(&collect_info_schema, &schema_file->table_schema_array_.at(0));
  ObSchemaTestUtils::expect_table_eq(&item_info_schema, &schema_file->table_schema_array_.at(1));


  delete(schema_manager);
  delete(schema_file);
};


TEST_F(TestSchemaFile, schema_ini2)
{
  obsys::ObSysConfig c1;
  //use local schema arena
  ObSchemaManager *schema_manager = new ObSchemaManager();
  ObSchemaFile* schema_file =  new ObSchemaFile(*schema_manager);
  bool is_in_test_model = true;
  schema_file->set_test_model(is_in_test_model);

  ASSERT_TRUE(schema_file->parse_from_file("schema_ini/schema.ini2", c1));

  ASSERT_EQ(1, schema_file->table_schema_array_.count());
  ASSERT_EQ(1, schema_file->tablegroup_array_.count());
  ASSERT_EQ(1, schema_file->database_array_.count());

  ObTableSchema table_schema;
  table_schema.set_tenant_id(1);
  table_schema.set_table_name(ObString::make_string("test_table"));
  table_schema.set_table_id(1001);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_type(static_cast<ObTableType>(1));
  table_schema.set_compress_func_name(ObString::make_string("snappy_1.0"));
  table_schema.set_block_size(2097152);
  table_schema.get_part_expr().set_part_num(1);
  table_schema.set_max_used_column_id(100);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_partition_key_column_num(3);

  ObColumnSchemaV2 column_schema;
  column_schema.set_column_id(4);
  column_schema.set_column_name(ObString::make_string("c1"));
  column_schema.set_data_type(ObIntType);
  column_schema.set_rowkey_position(1);
  column_schema.set_partition_key_position(2);
  table_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_column_id(5);
  column_schema.set_column_name(ObString::make_string("c2"));
  column_schema.set_data_type(ObVarcharType);
  column_schema.set_data_length(64);
  column_schema.set_rowkey_position(2);
  column_schema.set_partition_key_position(0);
  table_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(8);
  column_schema.set_column_name(ObString::make_string("c5"));
  column_schema.set_data_type(ObDateTimeType);
  column_schema.set_partition_key_position(1);
  table_schema.add_column(column_schema);

  column_schema.reset();
  column_schema.set_rowkey_position(0);
  column_schema.set_column_id(11);
  column_schema.set_column_name(ObString::make_string("c8"));
  column_schema.set_data_type(ObNumberType);
  column_schema.set_data_length(50);
  column_schema.set_nullable(1);
  column_schema.set_data_precision(50);
  column_schema.set_data_scale(50);
  column_schema.set_partition_key_position(3);
  table_schema.add_column(column_schema);

  ObSchemaTestUtils::expect_table_eq(&table_schema, &schema_file->table_schema_array_.at(0));

  ObTenantSchema tenant;
  make_tenant_schema(tenant,1, "default_tn", NULL);
  ObSchemaTestUtils::assert_tn_equal(tenant, schema_file->tenant_array_[0]);

  ObDatabaseSchema db_schema;
  make_database_schema(db_schema, 1,1,"default_db",NULL);
  ObSchemaTestUtils::assert_db_equal(db_schema, schema_file->database_array_[0]);

  ObTablegroupSchema tg_schema;
  make_tablegroup_schema(tg_schema,1,1,"default_tg", NULL);
  ObSchemaTestUtils::assert_tg_equal(tg_schema, schema_file->tablegroup_array_[0]);
};



TEST_F(TestSchemaFile, write_to_file)
{
  obsys::ObSysConfig c1;
  //use local schema arena
  ObSchemaManager *schema_manager = new ObSchemaManager();
  int ret = OB_SUCCESS;
  ret = schema_manager->init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ObSchemaFile &schema_file = schema_manager->schema_file_;
  schema_file.set_test_model(true);

  ObArray<ObTableSchema> table_array;
  ObTableSchema table_schema;
  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::core_table_schema_creators[i]; ++i) {
    table_schema.reset();
    ret = share::core_table_schema_creators[i](table_schema);
    for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
         iter != table_schema.column_end(); iter++) {
      //TODO oushen confirm order_in_rowkey_
      const_cast<ObColumnSchemaV2*>(*iter)->set_order_in_rowkey(0);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    table_schema.part_expr_.set_part_expr(ObString::make_string("abs(a+b)"),
                                          ObString::make_string("abs(aaaaaaa+b)"));
    table_schema.sub_part_expr_.set_part_expr(ObString::make_string("abs(a+b)"),
                                          ObString::make_string("abs(neg(a-b))"));

    ret = table_array.push_back(table_schema);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  remove("schema_ini/schema_unittest.ini");
  ret = schema_manager->add_new_table_schema_array(table_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_manager->write_to_file("schema_ini/schema_unittest.ini");
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(schema_file.parse_from_file("schema_ini/schema_unittest.ini", c1));

  for (int64_t i = 0; i < table_array.count(); ++i) {
    ObSchemaTestUtils::expect_table_eq(&table_array.at(i), &schema_file.table_schema_array_.at(i));
  }

  if (NULL != schema_manager){
    free(schema_manager);
    schema_manager = NULL;
  }
};


TEST_F(TestSchemaFile, write_to_file2)
{
  obsys::ObSysConfig c1;
  //use local schema arena
  ObSchemaManager *schema_manager = new ObSchemaManager();
  int ret = OB_SUCCESS;
  ret = schema_manager->init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_manager->parse_from_file("schema_ini/schema_manager.ini", c1);
  ASSERT_EQ(true, ret);
  ret = OB_SUCCESS;
  remove("schema_ini/schema_manager2.ini");
  ret = schema_manager->write_to_file("schema_ini/schema_manager2.ini");
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObTableSchema *table_schema_collection = schema_manager->get_table_schema(1001);
  const ObTableSchema *table_schema_item = schema_manager->get_table_schema(1002);
  const ObTableSchema *table_schema_index = schema_manager->get_table_schema(1003);

  obsys::ObSysConfig c2;
  ObSchemaManager *schema_manager2 = new ObSchemaManager();
  ret = schema_manager2->init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_manager2->parse_from_file("schema_ini/schema_manager2.ini", c2);
  ASSERT_EQ(true, ret);

  const ObTableSchema *table_schema_collection2 = schema_manager2->get_table_schema(1001);
  const ObTableSchema *table_schema_item2 = schema_manager2->get_table_schema(1002);
  const ObTableSchema *table_schema_index2 = schema_manager2->get_table_schema(1003);

  ObSchemaTestUtils::expect_table_eq(table_schema_collection, table_schema_collection2);
  ObSchemaTestUtils::expect_table_eq(table_schema_item, table_schema_item2);
  ObSchemaTestUtils::expect_table_eq(table_schema_index, table_schema_index2);

  if (NULL != schema_manager) {
    free(schema_manager);
    schema_manager = NULL;
  }
  if (NULL != schema_manager2) {
    free(schema_manager2);
    schema_manager2 = NULL;
  }
};

TEST_F(TestSchemaFile, write_to_file2)
{
  obsys::ObSysConfig c1;
  //use local schema arena
  ObSchemaManager *schema_manager = new ObSchemaManager();
  int ret = OB_SUCCESS;
  ret = schema_manager->init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_manager->parse_from_file("schema_ini/tianguan.ini", c1);
  ASSERT_EQ(true, ret);
  if (NULL != schema_manager) {
    free(schema_manager);
    schema_manager = NULL;
  }
};

}
}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

