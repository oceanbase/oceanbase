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

#define private public
#define protected public

#include "share/schema/ob_column_schema.h"
#include "storage/ob_storage_schema.h"
#include "share/ob_encryption_util.h"
#include "storage/test_schema_prepare.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/ob_storage_schema_util.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{
class TestStorageSchema : public ::testing::Test
{
public:
  TestStorageSchema() : allocator_(ObModIds::TEST), tenant_base_(tenant_id) {}
  virtual ~TestStorageSchema() {}
  bool judge_storage_schema_equal(ObStorageSchema &schema1, ObStorageSchema &schema2);
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void generate_heap_table_schema(
      ObIArray<ColumnType> &column_types,
      bool is_out_of_order,
      share::schema::ObTableSchema &table_schema);
  static const int64_t tenant_id = 1;
  common::ObArenaAllocator allocator_;
  ObTenantBase tenant_base_;
};

void TestStorageSchema::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

}
void TestStorageSchema::TearDown()
{
  ObTenantEnv::set_tenant(nullptr);
}

void TestStorageSchema::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}
void TestStorageSchema::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestStorageSchema::generate_heap_table_schema(
    ObIArray<ColumnType> &column_types,
    bool is_out_of_order,
    share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = 1;
  const uint64_t table_id = 7777;

  table_schema.reset();
  ret = table_schema.set_table_name("test_heap_table_storage_schema");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema.set_table_organization_mode(share::schema::ObTableOrganizationMode::TOM_HEAP_ORGANIZED);
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_max_used_column_id(common::OB_APP_MIN_COLUMN_ID + column_types.count());
  table_schema.set_block_size(16 * 1024);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_pctfree(10);
  table_schema.set_schema_version(100);

  share::schema::ObColumnSchemaV2 pk_column;
  pk_column.set_table_id(table_id);
  pk_column.set_column_id(common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_EQ(OB_SUCCESS, pk_column.set_column_name(common::OB_HIDDEN_PK_INCREMENT_COLUMN_NAME));
  pk_column.set_rowkey_position(1);
  pk_column.set_data_type(ObUInt64Type);
  pk_column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  pk_column.set_data_length(10);
  pk_column.set_is_hidden(true);

  if (!is_out_of_order) {
    COMMON_LOG(INFO, "add pk column", K(pk_column));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(pk_column));
  }

  for (int64_t idx = 0; idx < column_types.count(); ++idx) {
    share::schema::ObColumnSchemaV2 column;
    const ColumnType &col_type = column_types.at(idx);
    const int64_t column_id = common::OB_APP_MIN_COLUMN_ID + idx;
    char name[OB_MAX_FILE_NAME_LENGTH];
    memset(name, 0, sizeof(name));
    sprintf(name, "col%020ld", idx);
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_rowkey_position(0);
    column.set_data_type(column_types.at(idx));
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    column.set_is_hidden(false);
    ObSkipIndexColumnAttr skip_index_column_attr;
    skip_index_column_attr.reset();
    if ((idx % 3) == 0) {
      skip_index_column_attr.set_sum();
    } else if ((idx % 3) == 1) {
      skip_index_column_attr.set_min_max();
    }
    column.set_skip_index_attr(skip_index_column_attr.get_packed_value());
    ObObj default_value;
    if (ObIntType == col_type) {
      default_value.set_int(100);
      column.set_orig_default_value(default_value);
    } else if (ObVarcharType == col_type) {
      default_value.set_varchar("default_value");
      default_value.set_collation_type(ObCharset::get_system_collation());
      default_value.set_collation_level(CS_LEVEL_IMPLICIT);
      column.set_orig_default_value(default_value);
    } else if (ObTimeType == col_type) {
      default_value.set_time(10);
      column.set_orig_default_value(default_value);
    } else if (ObDoubleType == col_type) {
      default_value.set_double(60.0);
      column.set_orig_default_value(default_value);
    }
    COMMON_LOG(INFO, "add column", K(idx), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }

  if (is_out_of_order) {
    COMMON_LOG(INFO, "add pk column", K(pk_column));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(pk_column));
  }
  COMMON_LOG(INFO, "Finish generate heap table schema", K(column_types), K(is_out_of_order), K(table_schema));
}

bool TestStorageSchema::judge_storage_schema_equal(ObStorageSchema &schema1, ObStorageSchema &schema2)
{
  bool equal = false;
  equal = schema1.is_use_bloomfilter_ == schema2.is_use_bloomfilter_
      && schema1.table_type_ == schema2.table_type_
      && schema1.table_mode_ == schema2.table_mode_
      && schema1.row_store_type_ == schema2.row_store_type_
      && schema1.schema_version_ == schema2.schema_version_
      && schema1.column_cnt_ == schema2.column_cnt_
      && schema1.tablet_size_ == schema2.tablet_size_
      && schema1.pctfree_ == schema2.pctfree_
      && schema1.block_size_ == schema2.block_size_
      && schema1.master_key_id_ == schema2.master_key_id_
      && schema1.compressor_type_ == schema2.compressor_type_
      && schema1.encryption_ == schema2.encryption_
      && schema1.encrypt_key_ == schema2.encrypt_key_
      && schema1.rowkey_array_.count() == schema2.rowkey_array_.count()
      && schema1.column_array_.count() == schema2.column_array_.count();

  for (int64_t i = 0; equal && i < schema1.rowkey_array_.count(); ++i) {
    equal = schema1.rowkey_array_[i].meta_type_ == schema1.rowkey_array_[i].meta_type_;
  }

  for (int i = 0; equal && i < schema1.column_array_.count(); ++i) {
    equal = schema1.column_array_[i].meta_type_ == schema2.column_array_[i].meta_type_
        && schema1.column_array_[i].is_column_stored_in_sstable_ == schema2.column_array_[i].is_column_stored_in_sstable_;
  }
  if (equal && schema1.version_ >= ObStorageSchema::STORAGE_SCHEMA_VERSION_V3
    && schema2.version_ >= ObStorageSchema::STORAGE_SCHEMA_VERSION_V3) {
    equal = schema1.skip_idx_attr_array_.count() == schema2.skip_idx_attr_array_.count();
    for (int i = 0; equal && i < schema1.skip_idx_attr_array_.count(); ++i) {
      equal = schema1.skip_idx_attr_array_[i].col_idx_ == schema2.skip_idx_attr_array_[i].col_idx_
          && schema1.skip_idx_attr_array_[i].skip_idx_attr_ == schema2.skip_idx_attr_array_[i].skip_idx_attr_;
    }
  }

  return equal;
}

TEST_F(TestStorageSchema, generate_schema)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  COMMON_LOG(INFO, "generate success", K(storage_schema), K(table_schema));

  ObStorageSchema storage_schema2;
  ASSERT_EQ(OB_SUCCESS, storage_schema2.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  COMMON_LOG(INFO, "generate success", K(storage_schema2), K(table_schema));

  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, storage_schema2));
}

TEST_F(TestStorageSchema, serialize_and_deserialize)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));

  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len] = "\0";
  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));

  COMMON_LOG(INFO, "serialize size", K(ser_pos));

  ObStorageSchema des_storage_schema;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));

  COMMON_LOG(INFO, "test", K(storage_schema), K(des_storage_schema));
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
}

TEST_F(TestStorageSchema, serialize_and_deserialize2)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  table_schema.set_compress_func_name("compress_func_1");
  table_schema.add_aux_vp_tid(8989789);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));

  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len];
  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));
  COMMON_LOG(INFO, "serialize size", K(ser_pos));

  ObStorageSchema des_storage_schema;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));

  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
}

TEST_F(TestStorageSchema, serialize_and_deserialize_with_big_schema)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  table_schema.set_compress_func_name("compress_func_1");
  table_schema.add_aux_vp_tid(8989789);

  int64_t column_id = 100;
  share::schema::ObColumnSchemaV2 column;
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));

  for (int i = 0; i < 4000; ++i) {
    ObObjType obj_type = ObIntType;
    column.reset();
    column.set_table_id(table_schema.table_id_);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", column_id);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    column.set_rowkey_position(0);
    COMMON_LOG(INFO, "add column", K(i), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
    ++column_id;
  }
  table_schema.set_max_used_column_id(column_id);

  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));

  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len];
  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));
  COMMON_LOG(INFO, "serialize size", K(ser_pos));

  ObStorageSchema des_storage_schema;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));

  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
}

TEST_F(TestStorageSchema, deep_copy_str)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  table_schema.set_compress_func_name("compress_func_1");
  table_schema.add_aux_vp_tid(8989789);

  const int64_t key_len = share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH;
  char key[key_len + 1];
  MEMSET(key, 1, key_len);
  key[key_len] = '\0'; // to prevent core

  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  int64_t pos = 0;
  char buf[buf_len];

  // test encrypt_key without '\0'
  ObString no_end_key(key_len, key);
  table_schema.set_encrypt_key(no_end_key);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(0, MEMCMP(key, storage_schema.encrypt_key_.ptr(), key_len));

  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));
  COMMON_LOG(INFO, "serialize size", K(ser_pos));

  ObStorageSchema des_storage_schema;
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));

  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
  ASSERT_EQ(0, MEMCMP(key, storage_schema.encrypt_key_.ptr(), key_len));
  ASSERT_EQ(0, MEMCMP(key, des_storage_schema.encrypt_key_.ptr(), key_len));

  // test encrypt_key with '\0' in middle
  key[5] = '\0';
  ObString mid_end_key(key_len, key);
  table_schema.set_encrypt_key(mid_end_key);
  storage_schema.reset();
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(0, MEMCMP(key, storage_schema.encrypt_key_.ptr(), key_len));
  ser_pos = 0;
  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));
  pos = 0;
  des_storage_schema.reset();
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
  ASSERT_EQ(0, MEMCMP(key, storage_schema.encrypt_key_.ptr(), key_len));
  ASSERT_EQ(0, MEMCMP(key, des_storage_schema.encrypt_key_.ptr(), key_len));

}

TEST_F(TestStorageSchema, compat_serialize_and_deserialize)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  storage_schema.storage_schema_version_ = ObStorageSchema::STORAGE_SCHEMA_VERSION;

  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len] = "\0";
  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));

  ASSERT_EQ(ser_pos, storage_schema.get_serialize_size());

  ObStorageSchema des_storage_schema;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));

  COMMON_LOG(INFO, "test", K(storage_schema), K(des_storage_schema));
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
}

TEST_F(TestStorageSchema, test_update_tablet_store_schema)
{
  int ret = OB_SUCCESS;
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema1;
  ObStorageSchema storage_schema2;
  TestSchemaPrepare::prepare_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema1.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, storage_schema2.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  storage_schema2.column_cnt_ += 1;
  storage_schema2.column_info_simplified_ = true;
  storage_schema2.schema_version_ += 100;

  // schema 2 have large store column cnt
  ObStorageSchema *result_storage_schema = NULL;
  ret = ObStorageSchemaUtil::update_tablet_storage_schema(ObTabletID(1), allocator_, storage_schema1, storage_schema2, result_storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(result_storage_schema->schema_version_, storage_schema2.schema_version_);
  ASSERT_EQ(result_storage_schema->store_column_cnt_, storage_schema2.store_column_cnt_);
  ASSERT_EQ(result_storage_schema->is_column_info_simplified(), true);
  ObStorageSchemaUtil::free_storage_schema(allocator_, result_storage_schema);

  // mock schema with virtual column, same column_cnt & store_column_cnt, simplified = false
  storage_schema2.reset();
  ASSERT_EQ(OB_SUCCESS, storage_schema2.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  storage_schema1.store_column_cnt_ -= 1;
  storage_schema2.store_column_cnt_ -= 1;
  ret = ObStorageSchemaUtil::update_tablet_storage_schema(ObTabletID(1), allocator_, storage_schema1, storage_schema2, result_storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(result_storage_schema->schema_version_, storage_schema2.schema_version_);
  ASSERT_EQ(result_storage_schema->store_column_cnt_, storage_schema2.store_column_cnt_);
  ASSERT_EQ(result_storage_schema->is_column_info_simplified(), false);
  ObStorageSchemaUtil::free_storage_schema(allocator_, result_storage_schema);

  // schema_on_tablet and schema1 have same store column cnt, but storage_schema1 have full column info
  ObStorageSchema schema_on_tablet;
  ASSERT_EQ(OB_SUCCESS, schema_on_tablet.init(allocator_, storage_schema1, true/*skip_column_info*/));

  ret = ObStorageSchemaUtil::update_tablet_storage_schema(ObTabletID(1), allocator_, schema_on_tablet, storage_schema1, result_storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema1, *result_storage_schema));
  ASSERT_EQ(result_storage_schema->is_column_info_simplified(), false);
  ObStorageSchemaUtil::free_storage_schema(allocator_, result_storage_schema);
}

TEST_F(TestStorageSchema, test_sort_head_table_column_array_basic)
{
  int ret = OB_SUCCESS;
  share::schema::ObTableSchema table_schema1;
  share::schema::ObTableSchema table_schema2;
  ObSEArray<ColumnType, 4> column_types;
  ASSERT_EQ(OB_SUCCESS, column_types.push_back(ObIntType));
  ASSERT_EQ(OB_SUCCESS, column_types.push_back(ObVarcharType));
  ASSERT_EQ(OB_SUCCESS, column_types.push_back(ObTimeType));
  ASSERT_EQ(OB_SUCCESS, column_types.push_back(ObDoubleType));
  TestStorageSchema::generate_heap_table_schema(column_types, true  /*out_of_order*/, table_schema1);
  TestStorageSchema::generate_heap_table_schema(column_types, false /*out_of_order*/, table_schema2);

  ObStorageSchema storage_schema1;
  ObStorageSchema storage_schema2;
  ASSERT_EQ(OB_SUCCESS, storage_schema1.init(allocator_, table_schema1, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, storage_schema2.init(allocator_, table_schema2, lib::Worker::CompatMode::MYSQL));
  COMMON_LOG(INFO, "Finish init storage schema1", K(storage_schema1));
  COMMON_LOG(INFO, "Finish init storage schema2", K(storage_schema2));

  ASSERT_TRUE(storage_schema1.is_heap_table());
  ASSERT_TRUE(storage_schema2.is_heap_table());
  ASSERT_EQ(storage_schema1.rowkey_array_.count(), storage_schema2.rowkey_array_.count());
  ASSERT_EQ(storage_schema1.column_array_.count(), storage_schema2.column_array_.count());
  ASSERT_EQ(storage_schema1.skip_idx_attr_array_.count(), storage_schema2.skip_idx_attr_array_.count());

  bool is_out_of_order = false;
  ASSERT_EQ(OB_SUCCESS, storage_schema1.check_is_column_array_out_of_order_for_heap_table(is_out_of_order));
  ASSERT_FALSE(is_out_of_order);
  ASSERT_EQ(OB_SUCCESS, storage_schema2.check_is_column_array_out_of_order_for_heap_table(is_out_of_order));
  ASSERT_FALSE(is_out_of_order);

  ASSERT_EQ(storage_schema1.rowkey_array_[0].column_idx_, storage_schema2.rowkey_array_[0].column_idx_);
  for (int64_t idx = 0; idx < storage_schema1.column_array_.count(); idx++) {
    ASSERT_EQ(storage_schema1.column_array_[idx].get_data_type(), storage_schema2.column_array_[idx].get_data_type());
    ASSERT_TRUE(storage_schema1.column_array_[idx].get_orig_default_value() == storage_schema2.column_array_[idx].get_orig_default_value());
  }
  for (int64_t idx = 0; idx < storage_schema1.skip_idx_attr_array_.count(); idx++) {
    ASSERT_EQ(storage_schema1.skip_idx_attr_array_[idx].col_idx_, storage_schema2.skip_idx_attr_array_[idx].col_idx_);
  }
}

} // namespace unittest
} // namespace oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_storage_schema.log*");
  OB_LOGGER.set_file_name("test_storage_schema.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
