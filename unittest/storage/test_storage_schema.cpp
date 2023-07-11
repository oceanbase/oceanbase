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

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{
class TestStorageSchema : public ::testing::Test
{
public:
  TestStorageSchema() : allocator_(ObModIds::TEST) {}
  virtual ~TestStorageSchema() {}

  void prepare_schema(share::schema::ObTableSchema &table_schema);
  bool judge_storage_schema_equal(ObStorageSchema &schema1, ObStorageSchema &schema2);

  static const int64_t TENANT_ID = 1;
  static const int64_t TABLE_ID = 7777;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 3;
  static const int64_t TEST_COLUMN_CNT = 6;

  common::ObArenaAllocator allocator_;
};

void TestStorageSchema::prepare_schema(share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t micro_block_size = 16 * 1024;
  const uint64_t tenant_id = TENANT_ID;
  const uint64_t table_id = TABLE_ID;
  share::schema::ObColumnSchemaV2 column;

  //generate data table schema
  table_schema.reset();
  ret = table_schema.set_table_name("test_merge_multi_version");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  const int64_t column_ids[] = {16,17,20,21,22,23,24,29};
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    ObObjType obj_type = ObIntType;
    const int64_t column_id = column_ids[i];

    if (i == 1) {
      obj_type = ObVarcharType;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    if (i < TEST_ROWKEY_COLUMN_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    COMMON_LOG(INFO, "add column", K(i), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  COMMON_LOG(INFO, "dump stable schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema));
}

bool TestStorageSchema::judge_storage_schema_equal(ObStorageSchema &schema1, ObStorageSchema &schema2)
{
  bool equal = false;
  equal = schema1.version_ == schema2.version_
      && schema1.is_use_bloomfilter_ == schema2.is_use_bloomfilter_
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

  return equal;
}

TEST_F(TestStorageSchema, generate_schema)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  prepare_schema(table_schema);
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
  prepare_schema(table_schema);
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
  prepare_schema(table_schema);
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
  prepare_schema(table_schema);
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
  prepare_schema(table_schema);
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
