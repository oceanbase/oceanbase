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
#include "db_initializer.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_manager.h"
#include "lib/string/ob_sql_string.h"
#include "ob_schema_test_utils.cpp"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

const int64_t BUFF_SIZE = 10240;

class TestSchemaService : public ::testing::Test
{
   public:
     virtual void SetUp() {}
     virtual void TearDown() {}
     static void SeuUpTestCase() {}
     static void TearDownTestCase() {}
};

TEST_F(TestSchemaService, ColumnSchema_test)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 column_schema;
  ObObj default_value;
  column_schema.set_column_id(4);
  column_schema.set_column_name(ObString::make_string("c1"));
  column_schema.set_rowkey_position(1);
  column_schema.set_order_in_rowkey(ASC);
  column_schema.set_data_type(ObIntType);
  column_schema.set_data_length(100);
  column_schema.set_data_precision(1100);
  column_schema.set_data_scale(88);
  column_schema.set_nullable(false);
  column_schema.set_charset_type(CHARSET_UTF8MB4);
  column_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  default_value.set_int(100);
  column_schema.set_orig_default_value(default_value);
  default_value.set_int(101);
  column_schema.set_cur_default_value(default_value);
  column_schema.set_comment("black gives me black eyes");

  char buff[BUFF_SIZE];
  int64_t pos = 0;
  ret = column_schema.serialize(buff, BUFF_SIZE, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObColumnSchemaV2 column_schema2;
  int64_t pos2 = 0;
  column_schema2.deserialize(buff, BUFF_SIZE, pos2);
  ObSchemaTestUtils::expect_column_eq(&column_schema, &column_schema2);
  ASSERT_EQ(pos, pos2);
  ASSERT_EQ(pos, column_schema.get_serialize_size());
};

TEST_F(TestSchemaService, TableSchema_test)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  table_schema.set_tenant_id(111);
  table_schema.set_table_id(1011);
  table_schema.set_table_name(ObString::make_string("test_table"));
  table_schema.set_table_type(static_cast<ObTableType>(1));
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);
  table_schema.set_rowkey_column_num(20);
  table_schema.set_max_used_column_id(100);
  table_schema.set_rowkey_split_pos(11);
  table_schema.set_compress_func_name(ObString::make_string("snappy_1.0"));
  table_schema.set_expire_info(ObString::make_string("expire: modify_time > 3000s"));
  table_schema.set_is_use_bloomfilter(true);
  table_schema.set_comment("I want to use it search light!");
  table_schema.set_block_size(2097152);
  table_schema.set_charset_type(CHARSET_UTF8MB4);
  table_schema.set_data_table_id(111111);
  table_schema.set_index_status(INDEX_STATUS_UNIQUE_INELIGIBLE);
  table_schema.set_tablegroup_id(1);
  table_schema.set_progressive_merge_num(111110);
  table_schema.set_index_type(INDEX_TYPE_UNIQUE_LOCAL);
  table_schema.set_part_level(PARTITION_LEVEL_TWO);
  table_schema.get_part_expr().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table_schema.get_part_expr().set_part_expr(ObString::make_string("rand() mod 111"));
  table_schema.get_part_expr().set_part_num(100);
  table_schema.get_sub_part_expr().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table_schema.get_sub_part_expr().set_part_expr(ObString::make_string("rand() mod 111"));
  table_schema.get_sub_part_expr().set_part_num(666);
  table_schema.set_create_mem_version(88);

  ObColumnSchemaV2 column_schema;
  column_schema.set_column_id(4);
  column_schema.set_column_name(ObString::make_string("c1"));
  column_schema.set_rowkey_position(1);
  column_schema.set_order_in_rowkey(ASC);
  column_schema.set_data_type(ObIntType);
  column_schema.set_data_length(100);
  column_schema.set_data_precision(1100);
  column_schema.set_data_scale(88);
  column_schema.set_nullable(false);
  column_schema.set_charset_type(CHARSET_UTF8MB4);
  column_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ObObj value;
  value.set_int(100);
  column_schema.set_orig_default_value(value);
  value.set_int(101);
  column_schema.set_cur_default_value(value);
  column_schema.set_comment("black gives me black eyes");

  ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column_schema));

  char buff[BUFF_SIZE];
  int64_t pos = 0;
  ret = table_schema.serialize(buff, BUFF_SIZE, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  //TODO oushen ObPartitionExpr deserialize not supperted now
  //TableSchema table_schema2;
  //int64_t pos2 = 0;
  //table_schema2.deserialize(buff, BUFF_SIZE, pos2);
  //ObSchemaTestUtils::assert_table_equal(table_schema, table_schema2);
  //ASSERT_EQ(pos, pos2);
  //ASSERT_EQ(pos, table_schema.get_serialize_size());
};

TEST_F(TestSchemaService, other_test)
{
  ObTableSchema table_schema;
  table_schema.set_tenant_id(111);
  table_schema.set_table_id(1011);
  table_schema.set_table_name(ObString::make_string("test_table"));
  table_schema.set_table_type(static_cast<ObTableType>(1));
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);
  table_schema.set_rowkey_column_num(20);
  table_schema.set_max_used_column_id(100);
  table_schema.set_rowkey_split_pos(11);
  table_schema.set_compress_func_name(ObString::make_string("snappy_1.0"));
  table_schema.set_expire_info(ObString::make_string("expire: modify_time > 3000s"));
  table_schema.set_is_use_bloomfilter(true);
  table_schema.set_comment("I want to use it search light!");
  table_schema.set_block_size(2097152);
  table_schema.set_charset_type(CHARSET_UTF8MB4);
  table_schema.set_data_table_id(111111);
  table_schema.set_index_status(INDEX_STATUS_UNIQUE_INELIGIBLE);
  table_schema.set_tablegroup_id(1);
  table_schema.set_progressive_merge_num(111110);
  table_schema.set_index_type(INDEX_TYPE_UNIQUE_LOCAL);
  table_schema.set_part_level(PARTITION_LEVEL_TWO);
  table_schema.get_part_expr().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table_schema.get_part_expr().set_part_expr(ObString::make_string("rand() mod 111"));
  table_schema.get_part_expr().set_part_num(100);
  table_schema.get_sub_part_expr().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table_schema.get_sub_part_expr().set_part_expr(ObString::make_string("rand() mod 113431"));
  table_schema.get_sub_part_expr().set_part_num(666);
  table_schema.set_create_mem_version(88);

  ObColumnSchemaV2 column_schema;
  column_schema.set_column_id(4);
  column_schema.set_column_name(ObString::make_string("c1"));
  column_schema.set_rowkey_position(1);
  column_schema.set_order_in_rowkey(ASC);
  column_schema.set_data_type(ObIntType);
  column_schema.set_data_length(100);
  column_schema.set_data_precision(1100);
  column_schema.set_data_scale(88);
  column_schema.set_nullable(false);
  column_schema.set_charset_type(CHARSET_UTF8MB4);
  column_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ObObj obj;
  obj.set_int(100);
  column_schema.set_orig_default_value(obj);
  obj.set_int(101);
  column_schema.set_cur_default_value(obj);
  column_schema.set_comment("black gives me black eyes");

  ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column_schema));
  ObColumnSchemaV2 column_schema2;
  column_schema.reset();
  ObSchemaTestUtils::expect_column_eq(&column_schema, &column_schema2);

  ObTableSchema table_schema3;
  ASSERT_EQ(OB_SUCCESS, table_schema3.assign(table_schema));
  ObSchemaTestUtils::expect_table_eq(&table_schema3, &table_schema);
};

TEST_F(TestSchemaService, test_operation)
{
  const int64_t BUF_SIZE = 10240;
  int64_t pos = 0;
  char *buf = static_cast<char *>(malloc(BUF_SIZE));
  ObSchemaOperation operation;
  operation.reset();
  ASSERT_EQ(-1, operation.schema_version_);
  ASSERT_EQ(0, operation.tenant_id_);
  ASSERT_EQ(0, operation.user_id_);
  ASSERT_EQ(0, operation.database_id_);
  ASSERT_EQ(0, operation.tablegroup_id_);
  ASSERT_EQ(0, operation.table_id_);
  ASSERT_EQ(OB_INVALID_DDL_OP, operation.op_type_);
  ASSERT_FALSE(operation.is_valid());
  ASSERT_STREQ("OB_INVALID_DDL_OP", operation.type_str(operation.op_type_));
  pos = operation.to_string(buf, BUF_SIZE);
  ASSERT_TRUE(0 < pos);
  free(buf);
}

TEST_F(TestSchemaService, test_alter_table)
{
  const int64_t BUF_SIZE = 1024;
  int ret = OB_SUCCESS;
  int64_t buf_len = 0;
  int64_t pos = 0;
  char buf[BUF_SIZE];
  AlterColumnSchema alter_column_schema;
  AlterTableSchema alter_table_schema;
  AlterTableSchema dest_table_schema;
  AlterTableSchema copy_table_schema;

  alter_table_schema.reset();
  alter_column_schema.error_ret_ = OB_SCHEMA_ERROR;
  ret = alter_table_schema.set_origin_table_name("old_table");
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObString("old_table"), alter_table_schema.get_origin_table_name());
  ret = alter_table_schema.set_database_name("database");
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObString("database"), alter_table_schema.get_database_name());
  ret = alter_table_schema.set_origin_database_name("old_database");
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObString("old_database"), alter_table_schema.get_origin_database_name());
  ret = alter_table_schema.add_column(alter_column_schema);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = alter_table_schema.serialize(buf, BUF_SIZE, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  //test serialize
  ASSERT_EQ(pos, alter_table_schema.get_serialize_size());
  buf_len = pos;
  pos = 0;
  ret = dest_table_schema.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(dest_table_schema.get_origin_database_name(),
      alter_table_schema.get_origin_database_name());
  ASSERT_EQ(dest_table_schema.get_origin_table_name(),
      alter_table_schema.get_origin_table_name());
  ASSERT_EQ(dest_table_schema.get_database_name(),
      alter_table_schema.get_database_name());
  ASSERT_TRUE(0 < dest_table_schema.to_string(buf, BUF_SIZE));
  copy_table_schema = alter_table_schema;
  ASSERT_EQ(copy_table_schema.get_origin_database_name(),
      alter_table_schema.get_origin_database_name());
  ASSERT_EQ(copy_table_schema.get_origin_table_name(),
      alter_table_schema.get_origin_table_name());
  ASSERT_EQ(copy_table_schema.get_database_name(),
      alter_table_schema.get_database_name());
}

TEST_F(TestSchemaService, test_get_schema_operation_str)
{
  ObSchemaOperationType ops[] =
  {
    OB_INVALID_DDL_OP,
    OB_DDL_TABLE_OPERATION_BEGIN,
    OB_DDL_DROP_TABLE,
    OB_DDL_ALTER_TABLE,
    OB_DDL_CREATE_TABLE,
    OB_DDL_ADD_COLUMN,
    OB_DDL_DROP_COLUMN,
    OB_DDL_CHANGE_COLUMN,
    OB_DDL_MODIFY_COLUMN,
    OB_DDL_ALTER_COLUMN,
    OB_DDL_MODIFY_META_TABLE_ID,
    OB_DDL_KILL_INDEX,
    OB_DDL_STOP_INDEX_WRITE,
    OB_DDL_MODIFY_INDEX_STATUS,
    OB_DDL_MODIFY_TABLE_SCHEMA_VERSION,
    OB_DDL_MODIFY_TABLE_OPTION,
    OB_DDL_TABLE_RENAME,
    OB_DDL_MODIFY_DATA_TABLE_INDEX,
    OB_DDL_TABLE_OPERATION_END,
    OB_DDL_TENANT_OPERATION_BEGIN,
    OB_DDL_ADD_TENANT,
    OB_DDL_ALTER_TENANT,
    OB_DDL_DEL_TENANT,
    OB_DDL_TENANT_OPERATION_END,
    OB_DDL_DATABASE_OPERATION_BEGIN,
    OB_DDL_ADD_DATABASE,
    OB_DDL_ALTER_DATABASE,
    OB_DDL_DEL_DATABASE,
    OB_DDL_RENAME_DATABASE,
    OB_DDL_DATABASE_OPERATION_END,
    OB_DDL_TABLEGROUP_OPERATION_BEGIN,
    OB_DDL_ADD_TABLEGROUP,
    OB_DDL_DEL_TABLEGROUP,
    OB_DDL_RENAME_TABLEGROUP,
    OB_DDL_TABLEGROUP_OPERATION_END,
    OB_DDL_USER_OPERATION_BEGIN,
    OB_DDL_CREATE_USER,
    OB_DDL_DROP_USER,
    OB_DDL_RENAME_USER,
    OB_DDL_LOCK_USER,
    OB_DDL_SET_PASSWD,
    OB_DDL_GRANT_REVOKE_USER,
    OB_DDL_ALTER_USER_REQUIRE,
    OB_DDL_USER_OPERATION_END,
    OB_DDL_DB_PRIV_OPERATION_BEGIN,
    OB_DDL_GRANT_REVOKE_DB,
    OB_DDL_DEL_DB_PRIV,
    OB_DDL_DB_PRIV_OPERATION_END,
    OB_DDL_TABLE_PRIV_OPERATION_BEGIN,
    OB_DDL_GRANT_REVOKE_TABLE,
    OB_DDL_DEL_TABLE_PRIV,
    OB_DDL_TABLE_PRIV_OPERATION_END,
    OB_DDL_ALTER_ROLE,
    OB_DDL_MAX_OP
  };
  static const char *ddl_op_str_array[] =
  {
    "OB_INVALID_DDL_OP",
    "OB_DDL_TABLE_OPERATION_BEGIN",
    "OB_DDL_DROP_TABLE",
    "OB_DDL_ALTER_TABLE",
    "OB_DDL_CREATE_TABLE",
    "OB_DDL_ADD_COLUMN",
    "OB_DDL_DROP_COLUMN",
    "OB_DDL_CHANGE_COLUMN",
    "OB_DDL_MODIFY_COLUMN",
    "OB_DDL_ALTER_COLUMN",
    "OB_DDL_MODIFY_META_TABLE_ID",
    "OB_DDL_KILL_INDEX",
    "OB_DDL_STOP_INDEX_WRITE",
    "OB_DDL_MODIFY_INDEX_STATUS",
    "OB_DDL_MODIFY_TABLE_SCHEMA_VERSION",
    "OB_DDL_MODIFY_TABLE_OPTION",
    "OB_DDL_TABLE_RENAME",
    "OB_DDL_MODIFY_DATA_TABLE_INDEX",
    "OB_DDL_TABLE_OPERATION_END",
    "OB_DDL_TENANT_OPERATION_BEGIN",
    "OB_DDL_ADD_TENANT",
    "OB_DDL_ALTER_TENANT",
    "OB_DDL_DEL_TENANT",
    "OB_DDL_TENANT_OPERATION_END",
    "OB_DDL_DATABASE_OPERATION_BEGIN",
    "OB_DDL_ADD_DATABASE",
    "OB_DDL_ALTER_DATABASE",
    "OB_DDL_DEL_DATABASE",
    "OB_DDL_RENAME_DATABASE",
    "OB_DDL_DATABASE_OPERATION_END",
    "OB_DDL_TABLEGROUP_OPERATION_BEGIN",
    "OB_DDL_ADD_TABLEGROUP",
    "OB_DDL_DEL_TABLEGROUP",
    "OB_DDL_RENAME_TABLEGROUP",
    "OB_DDL_TABLEGROUP_OPERATION_END",
    "OB_DDL_USER_OPERATION_BEGIN",
    "OB_DDL_CREATE_USER",
    "OB_DDL_DROP_USER",
    "OB_DDL_RENAME_USER",
    "OB_DDL_LOCK_USER",
    "OB_DDL_SET_PASSWD",
    "OB_DDL_GRANT_REVOKE_USER",
    "OB_DDL_ALTER_USER_REQUIRE",
    "OB_DDL_USER_OPERATION_END",
    "OB_DDL_DB_PRIV_OPERATION_BEGIN",
    "OB_DDL_GRANT_REVOKE_DB",
    "OB_DDL_DEL_DB_PRIV",
    "OB_DDL_DB_PRIV_OPERATION_END",
    "OB_DDL_TABLE_PRIV_OPERATION_BEGIN",
    "OB_DDL_GRANT_REVOKE_TABLE",
    "OB_DDL_DEL_TABLE_PRIV",
    "OB_DDL_TABLE_PRIV_OPERATION_END",
    "OB_DDL_ALTER_ROLE",
    "OB_DDL_MAX_OP"
  };

  for (int64_t i = 0; i < ARRAYSIZEOF(ops); ++i) {
    const char *op_str = ObSchemaOperation::type_str(ops[i]);
    ASSERT_STREQ(ddl_op_str_array[i], op_str);
  }
}

}
}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("ERROR");
  OB_LOGGER.set_log_level("ERROR");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
