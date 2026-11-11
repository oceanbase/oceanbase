/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <set>
#include <vector>
#include <type_traits>

#define private public
#define protected public

#include "src/share/schema/ob_table_schema.h"
#include "storage/test_schema_prepare.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/ob_storage_schema_util.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{
static_assert(std::is_same<decltype(std::declval<ObStorageSchemaHandle &>().get_schema()), const ObStorageSchema &>::value, "schema handle must expose const access only");

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
  static const int64_t tenant_id = 1;
  common::ObArenaAllocator allocator_;
  ObTenantBase tenant_base_;
};

// Arena free() alone cannot detect freeing an interior slice or freeing the same allocation twice.
class ColumnGroupTrackingAllocator : public ObIAllocator
{
public:
  explicit ColumnGroupTrackingAllocator(ObIAllocator &backing) : backing_(backing), fail_next_(false) {}
  ~ColumnGroupTrackingAllocator() override { EXPECT_TRUE(live_.empty()); }
  void *alloc(const int64_t size) override
  {
    void *ptr = fail_next_ ? nullptr : backing_.alloc(size);
    fail_next_ = false;
    if (nullptr != ptr) {
      EXPECT_TRUE(live_.insert(ptr).second);
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      if (live_.erase(ptr) != 1) {
        ADD_FAILURE() << "free of a borrowed slice or an already freed allocation: " << ptr;
      } else {
        backing_.free(ptr);
      }
    }
  }
  ObIAllocator &backing_;
  std::set<void *> live_;
  bool fail_next_;
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

  // check V7 fields: minor_row_store_type_ and skip_index_level_
  if (equal && schema1.storage_schema_version_ >= ObStorageSchema::STORAGE_SCHEMA_VERSION_V7
    && schema2.storage_schema_version_ >= ObStorageSchema::STORAGE_SCHEMA_VERSION_V7) {
    equal = schema1.minor_row_store_type_ == schema2.minor_row_store_type_
        && schema1.skip_index_level_ == schema2.skip_index_level_;
  }

  // check V8 fields: hidden_rowkey_column_group_
  if (equal && schema1.storage_schema_version_ >= ObStorageSchema::STORAGE_SCHEMA_VERSION_V8
    && schema2.storage_schema_version_ >= ObStorageSchema::STORAGE_SCHEMA_VERSION_V8) {
    equal = schema1.hidden_rowkey_column_group_.count() == schema2.hidden_rowkey_column_group_.count();
    for (int i = 0; equal && i < schema1.hidden_rowkey_column_group_.count(); ++i) {
      equal = schema1.hidden_rowkey_column_group_[i].type_ == schema2.hidden_rowkey_column_group_[i].type_
          && schema1.hidden_rowkey_column_group_[i].column_cnt_ == schema2.hidden_rowkey_column_group_[i].column_cnt_
          && schema1.hidden_rowkey_column_group_[i].row_store_type_ == schema2.hidden_rowkey_column_group_[i].row_store_type_;
    }
  }

  // check column_group_array_
  if (equal) {
    equal = schema1.column_group_array_.count() == schema2.column_group_array_.count();
    for (int i = 0; equal && i < schema1.column_group_array_.count(); ++i) {
      equal = schema1.column_group_array_[i].type_ == schema2.column_group_array_[i].type_
          && schema1.column_group_array_[i].column_cnt_ == schema2.column_group_array_[i].column_cnt_;
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
  storage_schema1.progressive_merge_round_ = 3;
  storage_schema2.progressive_merge_round_ = 2;
  storage_schema1.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  storage_schema2.compressor_type_ = ObCompressorType::LZ4_COMPRESSOR;

  // schema 2 have large store column cnt
  ObStorageSchema *result_storage_schema = NULL;
  ObSSTableArray major_sstables;
  ret = ObStorageSchemaUtil::update_tablet_storage_schema(ObTabletID(1), false, allocator_, storage_schema1, storage_schema2, major_sstables, result_storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(result_storage_schema->schema_version_, storage_schema2.schema_version_);
  ASSERT_EQ(result_storage_schema->store_column_cnt_, storage_schema2.store_column_cnt_);
  ASSERT_EQ(result_storage_schema->is_column_info_simplified(), true);
  ASSERT_EQ(result_storage_schema->progressive_merge_round_, storage_schema1.progressive_merge_round_);
  ASSERT_EQ(result_storage_schema->compressor_type_, ObCompressorType::NONE_COMPRESSOR);
  ObStorageSchemaUtil::free_storage_schema(allocator_, result_storage_schema);

  // mock schema with virtual column, same column_cnt & store_column_cnt, simplified = false
  storage_schema2.reset();
  ASSERT_EQ(OB_SUCCESS, storage_schema2.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  storage_schema1.store_column_cnt_ -= 1;
  storage_schema2.store_column_cnt_ -= 1;
  ret = ObStorageSchemaUtil::update_tablet_storage_schema(ObTabletID(1), false, allocator_, storage_schema1, storage_schema2, major_sstables, result_storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(result_storage_schema->schema_version_, storage_schema2.schema_version_);
  ASSERT_EQ(result_storage_schema->store_column_cnt_, storage_schema2.store_column_cnt_);
  ASSERT_EQ(result_storage_schema->is_column_info_simplified(), false);
  ObStorageSchemaUtil::free_storage_schema(allocator_, result_storage_schema);

  // schema_on_tablet and schema1 have same store column cnt, but storage_schema1 have full column info
  ObStorageSchema schema_on_tablet;
  ASSERT_EQ(OB_SUCCESS, schema_on_tablet.init(allocator_, storage_schema1, true/*skip_column_info*/));

  ret = ObStorageSchemaUtil::update_tablet_storage_schema(ObTabletID(1), false, allocator_, schema_on_tablet, storage_schema1, major_sstables, result_storage_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema1, *result_storage_schema));
  ASSERT_EQ(result_storage_schema->is_column_info_simplified(), false);
  ObStorageSchemaUtil::free_storage_schema(allocator_, result_storage_schema);
}

TEST_F(TestStorageSchema, test_clipped_schema_for_tablet_split)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema1;
  TestSchemaPrepare::prepare_schema(table_schema);
  table_schema.set_compress_func_name("compress_func_1");
  table_schema.add_aux_vp_tid(8989789);

  int64_t stored_column_cnt;
  int64_t column_id = 100;
  share::schema::ObColumnSchemaV2 column;
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));

  for (int i = 0; i < 800; ++i) {
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
    if (i % 2 == 0) {
       column.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG); // virtual column.
    }
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
    ++column_id;
  }
  table_schema.set_max_used_column_id(column_id);
  ASSERT_EQ(OB_SUCCESS, table_schema.get_store_column_count(stored_column_cnt));
  ASSERT_EQ(OB_SUCCESS, storage_schema1.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));

  const int64_t buf_len = 1024 * 1024;
  char buf[buf_len] = "\0";
  const ObTabletID src_tablet_id(200001);
  for (int i = TestSchemaPrepare::TEST_ROWKEY_COLUMN_CNT; i <= stored_column_cnt; i++) {
    memset(buf, 0, sizeof(buf));
    int64_t ser_pos = 0;
    int64_t deser_pos = 0;

    ObStorageSchema storage_schema2; // clipped storage schema.
    ObUpdateCSReplicaSchemaParam update_param;
    ASSERT_EQ(OB_SUCCESS, update_param.init(src_tablet_id,
          i + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()/*major_column_cnt*/,
          ObUpdateCSReplicaSchemaParam::UpdateType::TRUNCATE_COLUMN_ARRAY));
    ASSERT_EQ(OB_SUCCESS, storage_schema2.init(allocator_,
            storage_schema1/*old_schema*/,
            false/*skip_column_info*/,
            nullptr/*column_group_schema*/,
            false/*generate_cs_replica_cg_array*/,
            &update_param/*ObUpdateCSReplicaSchemaParam*/));
    ASSERT_EQ(OB_SUCCESS, storage_schema2.serialize(buf, buf_len, ser_pos));
    ASSERT_EQ(ser_pos, storage_schema2.get_serialize_size());

    ObStorageSchema des_storage_schema;
    ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, deser_pos));
    COMMON_LOG(INFO, "test", K(storage_schema2), K(des_storage_schema));
    ASSERT_EQ(true, judge_storage_schema_equal(storage_schema2, des_storage_schema));
  }
}

TEST_F(TestStorageSchema, test_update_column_info_fix_column_cnt)
{
  int ret = OB_SUCCESS;

  // Step 1: Build a base table schema with virtual columns
  // 3 rowkey + 3 stored non-rowkey + 2 virtual = 8 total, 6 stored
  share::schema::ObTableSchema base_table_schema;
  TestSchemaPrepare::prepare_schema(base_table_schema, 3/*rowkey*/, 6/*column*/);

  int64_t next_col_id = common::OB_APP_MIN_COLUMN_ID + 6;
  share::schema::ObColumnSchemaV2 column;
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int i = 0; i < 2; ++i) {
    column.reset();
    column.set_table_id(base_table_schema.get_table_id());
    column.set_column_id(next_col_id);
    sprintf(name, "virtual_col%020ld", next_col_id);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(ObIntType);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    column.set_rowkey_position(0);
    column.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
    ASSERT_EQ(OB_SUCCESS, base_table_schema.add_column(column));
    ++next_col_id;
  }
  base_table_schema.set_max_used_column_id(next_col_id);
  ASSERT_EQ(8, base_table_schema.get_column_count());

  // Step 2: Init storage schema from base table schema
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, base_table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(8, storage_schema.column_cnt_);
  ASSERT_EQ(6, storage_schema.store_column_cnt_);
  ASSERT_EQ(8, storage_schema.column_array_.count());
  ASSERT_FALSE(storage_schema.column_info_simplified_);

  // Step 3: Simulate update_column_cnt_and_schema_version from memtable
  // (a new stored column was added, memtable recorded 7 stored columns)
  storage_schema.store_column_cnt_ = 7;
  storage_schema.column_info_simplified_ = true;

  // Step 4: Build the new table schema with one more stored column (DDL add column)
  // 3 rowkey + 4 stored non-rowkey + 2 virtual = 9 total, 7 stored
  share::schema::ObTableSchema new_table_schema;
  TestSchemaPrepare::prepare_schema(new_table_schema, 3/*rowkey*/, 7/*column*/);

  for (int i = 0; i < 2; ++i) {
    column.reset();
    column.set_table_id(new_table_schema.get_table_id());
    column.set_column_id(next_col_id + i);
    sprintf(name, "virtual_col%020ld", next_col_id + i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(ObIntType);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    column.set_rowkey_position(0);
    column.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
    ASSERT_EQ(OB_SUCCESS, new_table_schema.add_column(column));
  }
  new_table_schema.set_max_used_column_id(next_col_id + 2);
  ASSERT_EQ(9, new_table_schema.get_column_count());

  // Step 5: Call update_column_info — this is the function under test
  ret = storage_schema.update_column_info(new_table_schema, DATA_CURRENT_VERSION);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Step 6: Verify the fix
  ASSERT_FALSE(storage_schema.column_info_simplified_);
  ASSERT_EQ(9, storage_schema.column_cnt_);
  ASSERT_EQ(9, storage_schema.column_array_.count());
  ASSERT_EQ(7, storage_schema.store_column_cnt_);

  // Verify store_column_cnt_ matches actual stored columns in column_array_
  int64_t actual_stored = 0;
  for (int64_t i = 0; i < storage_schema.column_array_.count(); ++i) {
    if (storage_schema.column_array_[i].is_column_stored_in_sstable_) {
      ++actual_stored;
    }
  }
  ASSERT_EQ(storage_schema.store_column_cnt_, actual_stored);

  // Verify skip index attr works correctly (the original crash site)
  int64_t stored_col_cnt_in_sstable = 0;
  ASSERT_EQ(OB_SUCCESS, storage_schema.get_stored_column_count_in_sstable(stored_col_cnt_in_sstable));
  ObArray<share::schema::ObSkipIndexColumnAttr> skip_idx_attrs;
  ASSERT_EQ(OB_SUCCESS, skip_idx_attrs.prepare_allocate(stored_col_cnt_in_sstable));
  ret = storage_schema.get_skip_index_col_attr_by_schema(skip_idx_attrs);
  ASSERT_EQ(OB_SUCCESS, ret);

  COMMON_LOG(INFO, "test_update_column_info_fix_column_cnt passed",
      K(storage_schema.column_cnt_), K(storage_schema.store_column_cnt_),
      K(storage_schema.column_array_.count()), K(actual_stored));
}

TEST_F(TestStorageSchema, test_column_store_with_hidden_rowkey_cg)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);

  // add all cg, each cg, and hidden rowkey cg
  TestSchemaPrepare::add_all_each_and_hidden_rowkey_column_group(allocator_, table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));

  // verify hidden rowkey cg is created
  ASSERT_EQ(1, storage_schema.hidden_rowkey_column_group_.count());
  ASSERT_TRUE(storage_schema.hidden_rowkey_column_group_[0].is_rowkey_column_group());
  ASSERT_TRUE(storage_schema.hidden_rowkey_column_group_[0].is_valid());
  COMMON_LOG(INFO, "column store schema with hidden rowkey cg", K(storage_schema));

  // test serialize and deserialize
  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len] = "\0";
  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));
  ASSERT_EQ(ser_pos, storage_schema.get_serialize_size());

  ObStorageSchema des_storage_schema;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));

  COMMON_LOG(INFO, "deserialize result", K(storage_schema), K(des_storage_schema));
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
  ASSERT_EQ(1, des_storage_schema.hidden_rowkey_column_group_.count());
  ASSERT_TRUE(des_storage_schema.hidden_rowkey_column_group_[0].is_rowkey_column_group());
}

TEST_F(TestStorageSchema, test_storage_cg_schema_iterator)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);

  // add all cg, each cg, and hidden rowkey cg
  TestSchemaPrepare::add_all_each_and_hidden_rowkey_column_group(allocator_, table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));

  // test ObStorageCGSchemaIterator
  ObStorageCGSchemaIterator cg_iter(storage_schema);
  const int64_t normal_cg_cnt = storage_schema.column_group_array_.count();
  const int64_t special_cg_cnt = storage_schema.hidden_rowkey_column_group_.count();
  const int64_t total_cg_cnt = cg_iter.get_column_group_count();

  ASSERT_EQ(normal_cg_cnt + special_cg_cnt, total_cg_cnt);

  // iterate all column groups
  const ObStorageColumnGroupSchema *cg_schema = nullptr;
  int64_t cg_idx = 0;
  int64_t iter_idx = 0;
  int64_t iter_count = 0;
  int64_t last_iter_idx = 0;
  while (OB_SUCCESS == cg_iter.next(cg_schema, cg_idx, iter_idx)) {
    ASSERT_TRUE(nullptr != cg_schema);
    ASSERT_TRUE(cg_schema->is_valid());
    iter_count++;
    last_iter_idx = iter_idx;
    COMMON_LOG(INFO, "iterate cg", K(iter_count), K(cg_idx), K(last_iter_idx));
  }
  ASSERT_EQ(total_cg_cnt, iter_count);
  ASSERT_EQ(normal_cg_cnt, last_iter_idx);

  // test convert_iter_idx_to_column_group_idx for hidden rowkey cg
  cg_iter.reset();
  int64_t converted_cg_idx = 0;
  ASSERT_EQ(OB_SUCCESS, cg_iter.convert_iter_idx_to_column_group_idx(normal_cg_cnt, converted_cg_idx));
  ASSERT_EQ(HIDDEN_ROWKEY_COLUMN_GROUP_IDX, converted_cg_idx);

  // test get_cg_schema_with_column_group_idx for hidden rowkey cg
  cg_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, cg_iter.get_cg_schema_with_column_group_idx(HIDDEN_ROWKEY_COLUMN_GROUP_IDX, cg_schema));
  ASSERT_TRUE(nullptr != cg_schema);
  ASSERT_TRUE(cg_schema->is_rowkey_column_group());
}

TEST_F(TestStorageSchema, test_column_store_serialize_and_deserialize)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);

  // add all cg and each cg (without hidden rowkey cg)
  TestSchemaPrepare::add_all_and_each_column_group(allocator_, table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));

  // verify it's column store
  ASSERT_FALSE(storage_schema.is_row_store());
  ASSERT_TRUE(storage_schema.has_all_column_group_);
  ASSERT_EQ(0, storage_schema.hidden_rowkey_column_group_.count());
  COMMON_LOG(INFO, "column store schema", K(storage_schema));

  // test serialize and deserialize
  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len] = "\0";
  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));
  ASSERT_EQ(ser_pos, storage_schema.get_serialize_size());

  ObStorageSchema des_storage_schema;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));

  COMMON_LOG(INFO, "column store deserialize", K(storage_schema), K(des_storage_schema));
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
  ASSERT_FALSE(des_storage_schema.is_row_store());
  ASSERT_TRUE(des_storage_schema.has_all_column_group_);
}

TEST_F(TestStorageSchema, test_init_from_old_schema_with_cg)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema1;
  TestSchemaPrepare::prepare_schema(table_schema);

  // add all cg, each cg, and hidden rowkey cg
  TestSchemaPrepare::add_all_each_and_hidden_rowkey_column_group(allocator_, table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema1.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));

  // init storage_schema2 from storage_schema1
  ObStorageSchema storage_schema2;
  ASSERT_EQ(OB_SUCCESS, storage_schema2.init(allocator_, storage_schema1, false/*skip_column_info*/));

  COMMON_LOG(INFO, "init from old schema", K(storage_schema1), K(storage_schema2));
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema1, storage_schema2));
  ASSERT_EQ(storage_schema1.hidden_rowkey_column_group_.count(), storage_schema2.hidden_rowkey_column_group_.count());
  ASSERT_EQ(storage_schema1.column_group_array_.count(), storage_schema2.column_group_array_.count());
}

TEST_F(TestStorageSchema, test_generate_cs_replica_cg_array)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL,
      false/*skip_column_info*/, DATA_CURRENT_VERSION, true/*generate_cs_replica_cg_array*/));

  // verify cs replica cg array is generated
  ASSERT_FALSE(storage_schema.is_row_store());
  ASSERT_TRUE(storage_schema.is_cs_replica_compat_);
  COMMON_LOG(INFO, "cs replica cg array", K(storage_schema));

  // test serialize and deserialize
  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len] = "\0";
  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));
  ASSERT_EQ(ser_pos, storage_schema.get_serialize_size());

  ObStorageSchema des_storage_schema;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));

  COMMON_LOG(INFO, "cs replica deserialize", K(storage_schema), K(des_storage_schema));
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
}

TEST_F(TestStorageSchema, test_build_mock_rowkey_cg_schema_for_migration_full)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema row_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, row_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_TRUE(row_schema.is_row_store());
  ASSERT_FALSE(row_schema.is_column_info_simplified());

  ObStorageColumnGroupSchema mock_rowkey;
  ASSERT_EQ(OB_SUCCESS, row_schema.build_rowkey_column_group_schema(allocator_, mock_rowkey));

  ASSERT_TRUE(mock_rowkey.is_inited());
  ASSERT_TRUE(mock_rowkey.is_valid());
  ASSERT_TRUE(mock_rowkey.is_rowkey_column_group());
  ASSERT_EQ(row_schema.get_rowkey_column_num(), mock_rowkey.schema_column_cnt_);
  ASSERT_EQ(row_schema.get_rowkey_column_num()
              + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(),
            mock_rowkey.column_cnt_);
  ASSERT_EQ(mock_rowkey.column_cnt_, mock_rowkey.rowkey_column_cnt_);
  ASSERT_EQ(ObRowStoreType::CS_ENCODING_ROW_STORE, mock_rowkey.row_store_type_);
  ASSERT_TRUE(nullptr != mock_rowkey.column_idxs_);
  for (int64_t i = 0; i < mock_rowkey.column_cnt_; ++i) {
    ASSERT_EQ(static_cast<uint16_t>(i), mock_rowkey.column_idxs_[i]);
  }
}

TEST_F(TestStorageSchema, test_build_mock_rowkey_cg_schema_for_migration_simplified)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema row_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, row_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL,
      true /*skip_column_info*/));
  ASSERT_TRUE(row_schema.is_row_store());
  ASSERT_TRUE(row_schema.is_column_info_simplified());

  // simplified source must still produce a valid rowkey cg (rowkey columns are
  // always carried, regardless of column_info_simplified_).
  ObStorageColumnGroupSchema mock_rowkey;
  ASSERT_EQ(OB_SUCCESS, row_schema.build_rowkey_column_group_schema(allocator_, mock_rowkey));
  ASSERT_TRUE(mock_rowkey.is_inited());
  ASSERT_TRUE(mock_rowkey.is_valid());
  ASSERT_TRUE(mock_rowkey.is_rowkey_column_group());
}

TEST_F(TestStorageSchema, test_generate_single_column_group_schema_full)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema row_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, row_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_FALSE(row_schema.is_column_info_simplified());

  ObStorageColumnGroupSchema mock_single;
  const uint16_t expected_fake_idx = static_cast<uint16_t>(
      row_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt());
  ASSERT_EQ(OB_SUCCESS, row_schema.generate_single_column_group_schema(
      allocator_, mock_single, expected_fake_idx));

  ASSERT_TRUE(mock_single.is_inited());
  ASSERT_TRUE(mock_single.is_valid());
  ASSERT_TRUE(mock_single.is_single_column_group());
  ASSERT_EQ(1, mock_single.column_cnt_);
  ASSERT_EQ(1, mock_single.schema_column_cnt_);
  ASSERT_EQ(0, mock_single.rowkey_column_cnt_);
  ASSERT_EQ(0, mock_single.schema_rowkey_column_cnt_);
  ASSERT_EQ(ObRowStoreType::CS_ENCODING_ROW_STORE, mock_single.row_store_type_);
  ASSERT_TRUE(nullptr != mock_single.column_idxs_);
  // fake_idx = rowkey_col_cnt + extra_rowkey_col_cnt; in full path this is the
  // first non-rowkey column's position. The single-template is shared by all
  // NORMAL_CG sstables on the destination side -- correctness argued in
  // proposal v3 §2.5(b)(c)(d): col_desc / agg_meta / column_checksums all
  // either bypass the destination desc or get overwritten from source.
  ASSERT_EQ(expected_fake_idx, mock_single.column_idxs_[0]);
}

TEST_F(TestStorageSchema, test_generate_single_column_group_schema_simplified)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema row_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  ASSERT_EQ(OB_SUCCESS, row_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL,
      true /*skip_column_info*/));
  ASSERT_TRUE(row_schema.is_column_info_simplified());

  ObStorageColumnGroupSchema mock_single;
  const uint16_t expected_fake_idx = static_cast<uint16_t>(
      row_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt());
  ASSERT_EQ(OB_SUCCESS, row_schema.generate_single_column_group_schema(
      allocator_, mock_single, expected_fake_idx));
  ASSERT_TRUE(mock_single.is_inited());
  ASSERT_TRUE(mock_single.is_valid());
  ASSERT_TRUE(mock_single.is_single_column_group());
  ASSERT_EQ(1, mock_single.column_cnt_);
  // simplified path: fake_idx should be >= source's multi_version_rowkey
  // count, so destination's add_col_desc_from_cg_schema falls into the
  // binary-placeholder branch.
  ASSERT_EQ(expected_fake_idx, mock_single.column_idxs_[0]);
}

TEST_F(TestStorageSchema, test_default_mock_cg_schema_is_not_inited)
{
  // Default-constructed ObStorageColumnGroupSchema has row_store_type_ =
  // MAX_ROW_STORE; this drives ObMigrationTabletParam's serialization gate
  // (skip emitting the cg payload when not inited, only 1 byte for the bool).
  ObStorageColumnGroupSchema cg;
  ASSERT_FALSE(cg.is_inited());
}

TEST_F(TestStorageSchema, test_rowkey_and_each_column_group)
{
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema);

  // add rowkey cg and each cg (without all cg)
  TestSchemaPrepare::add_rowkey_and_each_column_group(allocator_, table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));

  // verify it's column store without all cg
  ASSERT_FALSE(storage_schema.is_row_store());
  ASSERT_FALSE(storage_schema.has_all_column_group_);
  COMMON_LOG(INFO, "rowkey and each cg schema", K(storage_schema));

  // test serialize and deserialize
  const int64_t buf_len = 1024 * 1024;
  int64_t ser_pos = 0;
  char buf[buf_len] = "\0";
  ASSERT_EQ(OB_SUCCESS, storage_schema.serialize(buf, buf_len, ser_pos));
  ASSERT_EQ(ser_pos, storage_schema.get_serialize_size());

  ObStorageSchema des_storage_schema;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_storage_schema.deserialize(allocator_, buf, ser_pos, pos));

  COMMON_LOG(INFO, "rowkey and each cg deserialize", K(storage_schema), K(des_storage_schema));
  ASSERT_EQ(true, judge_storage_schema_equal(storage_schema, des_storage_schema));
}

TEST_F(TestStorageSchema, column_group_index_ownership)
{
  ColumnGroupTrackingAllocator allocator(allocator_);
  ObStorageColumnGroupIdxBuffer indexes;
  ASSERT_EQ(OB_SUCCESS, indexes.init(allocator, 6));
  ObStorageColumnGroupSchema source(share::schema::ROWKEY_COLUMN_GROUP, NONE_COMPRESSOR,
      ObRowStoreType::CS_ENCODING_ROW_STORE, 16384, 1, 3, 1, 3, nullptr);
  ASSERT_EQ(OB_SUCCESS, source.alloc_column_idxs_(allocator, nullptr));
  for (uint16_t i = 0; i < source.column_cnt_; ++i) {
    source.column_idxs_[i] = i;
  }
  ObStorageColumnGroupSchema borrowed;
  ObStorageColumnGroupSchema second_slice;
  ObStorageColumnGroupSchema standalone;
  ObStorageColumnGroupSchema exhausted;
  ASSERT_EQ(OB_SUCCESS, borrowed.deep_copy(source, allocator, &indexes));
  ASSERT_EQ(OB_SUCCESS, second_slice.deep_copy(source, allocator, &indexes));
  ASSERT_EQ(borrowed.column_idxs_ + 3, second_slice.column_idxs_);
  ASSERT_TRUE(borrowed.column_idxs_borrowed_);
  ASSERT_TRUE(second_slice.column_idxs_borrowed_);
  ASSERT_EQ(OB_SUCCESS, standalone.deep_copy(borrowed, allocator));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, exhausted.deep_copy(borrowed, allocator, &indexes));
  ASSERT_EQ(nullptr, exhausted.column_idxs_);
  ASSERT_FALSE(standalone.column_idxs_borrowed_);
  ASSERT_FALSE(exhausted.column_idxs_borrowed_);
  ASSERT_FALSE(standalone.is_schema_changed(borrowed));
  ASSERT_EQ(24, sizeof(ObStorageColumnGroupSchema));
  ASSERT_EQ(3, allocator.live_.size());

  ObStorageColumnGroupSchema failed;
  allocator.fail_next_ = true;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, failed.deep_copy(borrowed, allocator));
  ASSERT_EQ(nullptr, failed.column_idxs_);
  ASSERT_FALSE(failed.column_idxs_borrowed_);
  failed.destroy(allocator);
  ObStorageColumnGroupSchema alias = second_slice;
  alias.destroy(allocator); // Copying a borrowed group must preserve its ownership bit.
  borrowed.destroy(allocator);
  second_slice.destroy(allocator);
  ASSERT_FALSE(borrowed.column_idxs_borrowed_);
  ASSERT_EQ(0, static_cast<uint8_t>(borrowed.version_));
  ASSERT_EQ(3, allocator.live_.size());
  source.destroy(allocator);
  standalone.destroy(allocator);
  exhausted.destroy(allocator);
  ASSERT_EQ(1, allocator.live_.size());
  indexes.reset();
  indexes.reset();
  ASSERT_TRUE(allocator.live_.empty());
}

TEST_F(TestStorageSchema, column_group_borrowed_wire_compatibility)
{
  ColumnGroupTrackingAllocator allocator(allocator_);
  ObStorageColumnGroupIdxBuffer indexes;
  ASSERT_EQ(OB_SUCCESS, indexes.init(allocator, 6));
  ObStorageColumnGroupSchema source(share::schema::ROWKEY_COLUMN_GROUP, NONE_COMPRESSOR,
      ObRowStoreType::CS_ENCODING_ROW_STORE, 16384, 1, 3, 1, 3, nullptr);
  ASSERT_EQ(OB_SUCCESS, source.alloc_column_idxs_(allocator, nullptr));
  for (uint16_t i = 0; i < source.column_cnt_; ++i) {
    source.column_idxs_[i] = i;
  }
  ObStorageColumnGroupSchema borrowed;
  ASSERT_EQ(OB_SUCCESS, borrowed.deep_copy(source, allocator, &indexes));
  ASSERT_TRUE(borrowed.column_idxs_borrowed_);
  ASSERT_EQ(source.get_serialize_size(), borrowed.get_serialize_size());
  std::vector<char> expected(source.get_serialize_size());
  std::vector<char> actual(borrowed.get_serialize_size());
  int64_t expected_pos = 0;
  int64_t actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(expected.data(), expected.size(), expected_pos));
  ASSERT_EQ(OB_SUCCESS, borrowed.serialize(actual.data(), actual.size(), actual_pos));
  ASSERT_EQ(expected.size(), expected_pos);
  ASSERT_EQ(expected_pos, actual_pos);
  ASSERT_EQ(0, MEMCMP(expected.data(), actual.data(), expected_pos));
  ASSERT_EQ(1, static_cast<uint8_t>(actual[0])); // The ownership bit must not enter the wire version.

  ObStorageColumnGroupSchema decoded_owned;
  ObStorageColumnGroupSchema decoded_borrowed;
  actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_owned.deserialize(allocator, actual.data(), actual.size(), actual_pos));
  ASSERT_EQ(expected_pos, actual_pos);
  ASSERT_FALSE(decoded_owned.column_idxs_borrowed_);
  actual_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_borrowed.deserialize(allocator, actual.data(), actual.size(), actual_pos, &indexes));
  ASSERT_EQ(expected_pos, actual_pos);
  ASSERT_TRUE(decoded_borrowed.column_idxs_borrowed_);
  ASSERT_TRUE(decoded_borrowed.is_valid());
  ASSERT_FALSE(source.is_schema_changed(decoded_owned));
  ASSERT_FALSE(source.is_schema_changed(decoded_borrowed));
  source.destroy(allocator);
  borrowed.destroy(allocator);
  decoded_owned.destroy(allocator);
  decoded_borrowed.destroy(allocator);
  indexes.reset();
  ASSERT_TRUE(allocator.live_.empty());
}

TEST_F(TestStorageSchema, column_group_decode_failure_cleanup)
{
  ColumnGroupTrackingAllocator allocator(allocator_);
  ObStorageColumnGroupIdxBuffer indexes;
  ASSERT_EQ(OB_SUCCESS, indexes.init(allocator, 3));
  ObStorageColumnGroupSchema source(share::schema::ROWKEY_COLUMN_GROUP, NONE_COMPRESSOR,
      ObRowStoreType::CS_ENCODING_ROW_STORE, 16384, 1, 3, 1, 3, nullptr);
  ASSERT_EQ(OB_SUCCESS, source.alloc_column_idxs_(allocator, nullptr));
  for (uint16_t i = 0; i < source.column_cnt_; ++i) {
    source.column_idxs_[i] = i;
  }
  source.column_idxs_[2] = 128; // Truncate inside a varint, not at an optional trailing-field boundary.
  std::vector<char> wire(source.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(wire.data(), wire.size(), pos));
  const uint8_t invalid_versions[] = {0, 2, 0x81, 0xff};
  for (const uint8_t version : invalid_versions) {
    ObStorageColumnGroupSchema decoded;
    wire[0] = static_cast<char>(version);
    pos = 0;
    ASSERT_EQ(OB_ERR_UNEXPECTED, decoded.deserialize(allocator, wire.data(), wire.size(), pos, &indexes));
    ASSERT_EQ(nullptr, decoded.column_idxs_);
    ASSERT_FALSE(decoded.column_idxs_borrowed_);
    decoded.destroy(allocator);
    ASSERT_EQ(2, allocator.live_.size());
  }
  wire[0] = 1;
  for (const bool borrow : {false, true}) {
    ObStorageColumnGroupSchema decoded;
    pos = 0;
    // Truncate an index after the destination allocation, exercising both cleanup paths.
    ASSERT_NE(OB_SUCCESS, decoded.deserialize(allocator, wire.data(), wire.size() - 1, pos, borrow ? &indexes : nullptr));
    ASSERT_EQ(nullptr, decoded.column_idxs_);
    ASSERT_FALSE(decoded.column_idxs_borrowed_);
    decoded.destroy(allocator);
    ASSERT_EQ(2, allocator.live_.size());
  }
  source.destroy(allocator);
  indexes.reset();
  ASSERT_TRUE(allocator.live_.empty());
}

TEST_F(TestStorageSchema, column_group_buffer_schema_lifecycle)
{
  ColumnGroupTrackingAllocator allocator(allocator_);
  share::schema::ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  TestSchemaPrepare::add_all_each_and_hidden_rowkey_column_group(allocator_, table_schema);
  ObStorageSchema source;
  ObStorageSchema copied;
  ObStorageSchema decoded;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, copied.init(allocator, source));
  std::vector<char> wire(source.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(wire.data(), wire.size(), pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator, wire.data(), wire.size(), pos));
  ASSERT_EQ(wire.size(), pos);
  for (ObStorageSchema *schema : {&source, &copied, &decoded}) {
    for (int64_t i = 0; i < schema->column_group_array_.count(); ++i) {
      const ObStorageColumnGroupSchema &cg = schema->column_group_array_.at(i);
      ASSERT_TRUE(cg.is_valid());
      ASSERT_EQ(!cg.is_all_column_group(), static_cast<bool>(cg.column_idxs_borrowed_));
    }
    ASSERT_EQ(1, schema->hidden_rowkey_column_group_.count());
    ASSERT_TRUE(schema->hidden_rowkey_column_group_.at(0).column_idxs_borrowed_);
  }
  source.reset();
  ASSERT_TRUE(copied.is_valid());
  ASSERT_TRUE(decoded.is_valid());
  copied.reset();
  decoded.reset();
  ASSERT_TRUE(allocator.live_.empty());
}

TEST_F(TestStorageSchema, column_group_index_buffer_errors)
{
  ColumnGroupTrackingAllocator allocator(allocator_);
  ObStorageColumnGroupIdxBuffer indexes;
  uint16_t *data = nullptr;
  ASSERT_EQ(OB_INVALID_ARGUMENT, indexes.init(allocator, -1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, indexes.init(allocator, INT64_MAX));
  ASSERT_EQ(OB_SUCCESS, indexes.init(allocator, 0));
  ASSERT_EQ(OB_INIT_TWICE, indexes.init(allocator, 0));
  ASSERT_EQ(OB_INIT_TWICE, indexes.init(allocator, 3));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, indexes.alloc(1, data));
  ASSERT_EQ(nullptr, data);
  indexes.reset();
  ASSERT_TRUE(allocator.live_.empty());
  ASSERT_EQ(OB_SUCCESS, indexes.init(allocator, OB_USER_ROW_MAX_COLUMNS_COUNT + 3));
  indexes.reset();
  allocator.fail_next_ = true;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, indexes.init(allocator, 3));
  ASSERT_EQ(OB_SUCCESS, indexes.init(allocator, 3));
  ASSERT_EQ(OB_INIT_TWICE, indexes.init(allocator, 3));
  ASSERT_EQ(OB_INVALID_ARGUMENT, indexes.alloc(0, data));
  ASSERT_EQ(nullptr, data);
  ASSERT_EQ(OB_SUCCESS, indexes.alloc(2, data));
  ASSERT_NE(nullptr, data);
  uint16_t *first = data;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, indexes.alloc(2, data));
  ASSERT_EQ(nullptr, data);
  ASSERT_EQ(OB_SUCCESS, indexes.alloc(1, data));
  ASSERT_EQ(first + 2, data); // Failed allocations must not advance the buffer.
  indexes.reset();
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, indexes.alloc(1, data));
  ASSERT_EQ(nullptr, data);
  ASSERT_TRUE(allocator.live_.empty());
}

TEST_F(TestStorageSchema, schema_handle_owns_validated_schema)
{
  share::schema::ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema);
  TestSchemaPrepare::add_all_and_each_column_group(allocator_, table_schema);
  ObStorageSchema source;
  ObStorageSchema invalid;
  ObStorageSchemaHandle handle;
  ObStorageSchemaHandle copied;
  ASSERT_FALSE(handle.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, handle.init(allocator_, invalid));
  ASSERT_FALSE(handle.is_valid());
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, handle.init(allocator_, source));
  ASSERT_NE(&source, &handle.get_schema());
  ASSERT_NE(&source.column_array_.at(0), &handle.get_schema().column_array_.at(0));
  source.reset();
  ASSERT_TRUE(handle.is_valid());
  ASSERT_TRUE(handle.get_schema().is_valid());
  ASSERT_EQ(OB_INIT_TWICE, handle.init(allocator_, invalid));
  ASSERT_TRUE(handle.is_valid());
  ASSERT_EQ(OB_SUCCESS, handle.assign(allocator_, handle));
  ASSERT_EQ(OB_SUCCESS, handle.assign(allocator_, handle.get_schema()));
  ASSERT_EQ(OB_SUCCESS, copied.init(allocator_, handle));
  ObStorageSchema output;
  ASSERT_EQ(OB_SUCCESS, output.init(allocator_, handle));
  handle.reset();
  ASSERT_FALSE(handle.is_valid());
  ASSERT_TRUE(copied.is_valid());
  ASSERT_TRUE(copied.get_schema().is_valid());
  ASSERT_TRUE(output.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, copied.assign(allocator_, invalid));
  ASSERT_FALSE(copied.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, copied.assign(allocator_, copied));
}

TEST_F(TestStorageSchema, schema_handle_assignment_failure_cleanup)
{
  ColumnGroupTrackingAllocator allocator(allocator_);
  ObStorageSchema source;
  ObStorageSchemaHandle handle;
  ASSERT_EQ(OB_SUCCESS, source.init_placeholder(allocator_));
  ASSERT_EQ(OB_SUCCESS, handle.init(allocator, source));
  allocator.fail_next_ = true;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, handle.assign(allocator, source));
  ASSERT_FALSE(handle.is_valid());
  ASSERT_TRUE(allocator.live_.empty());
  ASSERT_EQ(OB_SUCCESS, handle.assign(allocator, source));
  handle.reset();
  ASSERT_TRUE(allocator.live_.empty());
}

TEST_F(TestStorageSchema, schema_handle_wire_and_decode_state)
{
  ObStorageSchema source;
  ObStorageSchemaHandle handle;
  ASSERT_EQ(OB_SUCCESS, source.init_placeholder(allocator_));
  ASSERT_EQ(OB_SUCCESS, handle.init_placeholder(allocator_));
  const int64_t offset = 7;
  std::vector<char> expected(offset + source.get_serialize_size(), '\x5a');
  std::vector<char> actual(offset + handle.get_serialize_size(), '\x5a');
  int64_t pos = offset;
  ASSERT_EQ(OB_SUCCESS, source.serialize(expected.data(), expected.size(), pos));
  ASSERT_EQ(expected.size(), pos);
  pos = offset;
  ASSERT_EQ(OB_SUCCESS, handle.serialize(actual.data(), actual.size(), pos));
  ASSERT_EQ(actual.size(), pos);
  ASSERT_EQ(expected, actual);

  ObStorageSchemaHandle decoded;
  pos = offset;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator_, expected.data(), expected.size(), pos));
  ASSERT_TRUE(decoded.is_valid());
  ASSERT_EQ(expected.size(), pos);
  pos = offset;
  ASSERT_EQ(OB_INIT_TWICE, decoded.deserialize(allocator_, expected.data(), expected.size(), pos));
  ASSERT_TRUE(decoded.is_valid());
  decoded.reset();
  pos = offset;
  ASSERT_NE(OB_SUCCESS, decoded.deserialize(allocator_, expected.data(), offset + 1, pos));
  ASSERT_FALSE(decoded.is_valid());

  // Decoding and schema validity are distinct for legacy restore/placeholder data.
  source.column_cnt_ = 0;
  expected.resize(offset + source.get_serialize_size());
  pos = offset;
  ASSERT_EQ(OB_SUCCESS, source.serialize(expected.data(), expected.size(), pos));
  pos = offset;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator_, expected.data(), expected.size(), pos));
  ASSERT_FALSE(decoded.is_valid());
  ASSERT_EQ(OB_SUCCESS, decoded.init_placeholder(allocator_));
  ASSERT_TRUE(decoded.is_valid());
}

TEST_F(TestStorageSchema, schema_handle_prepared_serialize_size)
{
  ObStorageSchemaHandle handle;
  ASSERT_EQ(-1, handle.serialize_size_);
  ASSERT_EQ(handle.get_schema().get_serialize_size(), handle.get_serialize_size());
  ASSERT_EQ(-1, handle.serialize_size_); // Const queries must not populate the cache.
  ASSERT_EQ(OB_INVALID_ARGUMENT, handle.prepare_serialize_size());
  ASSERT_EQ(OB_SUCCESS, handle.init_placeholder(allocator_));
  ASSERT_EQ(-1, handle.serialize_size_); // Initialization alone must not walk the schema for its size.
  const int64_t original_size = handle.get_serialize_size();
  ASSERT_EQ(-1, handle.serialize_size_);
  ASSERT_EQ(OB_SUCCESS, handle.prepare_serialize_size());
  ASSERT_EQ(original_size, handle.serialize_size_);
  ASSERT_EQ(OB_SUCCESS, handle.prepare_serialize_size());
  ASSERT_EQ(original_size, handle.get_schema().get_serialize_size());
  ASSERT_EQ(OB_SUCCESS, handle.assign(allocator_, handle));
  ASSERT_EQ(original_size, handle.serialize_size_);
  ASSERT_EQ(OB_INIT_TWICE, handle.init(allocator_, handle));
  ASSERT_EQ(original_size, handle.serialize_size_);

  // Simplifying the column array changes the wire length and must discard the old cache.
  ASSERT_EQ(OB_SUCCESS, handle.update_column_cnt_and_schema_version(2, 2, 128));
  ASSERT_EQ(-1, handle.serialize_size_);
  const int64_t updated_size = handle.get_serialize_size();
  ASSERT_EQ(-1, handle.serialize_size_);
  ASSERT_EQ(OB_SUCCESS, handle.prepare_serialize_size());
  ASSERT_EQ(updated_size, handle.serialize_size_);
  ASSERT_EQ(handle.get_schema().get_serialize_size(), updated_size);
  ASSERT_NE(original_size, updated_size);
  ASSERT_EQ(updated_size, handle.get_serialize_size());
  std::vector<char> wire(updated_size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, handle.serialize(wire.data(), wire.size(), pos));
  ASSERT_EQ(updated_size, pos);

  ObStorageSchemaHandle decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.init_placeholder(allocator_));
  ASSERT_EQ(OB_SUCCESS, decoded.prepare_serialize_size());
  decoded.reset();
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator_, wire.data(), wire.size(), pos));
  ASSERT_EQ(-1, decoded.serialize_size_);
  ASSERT_EQ(decoded.get_schema().get_serialize_size(), decoded.get_serialize_size());
  ASSERT_EQ(-1, decoded.serialize_size_);
  ASSERT_EQ(OB_SUCCESS, decoded.prepare_serialize_size());
  std::vector<char> decoded_wire(decoded.get_serialize_size());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.serialize(decoded_wire.data(), decoded_wire.size(), pos));
  ASSERT_EQ(decoded_wire.size(), pos);
  ASSERT_EQ(OB_SUCCESS, handle.assign(allocator_, decoded));
  ASSERT_EQ(-1, handle.serialize_size_);
  // Schema copying may materialize a column group; prepare the destination's own length.
  ASSERT_EQ(OB_SUCCESS, handle.prepare_serialize_size());
  ASSERT_EQ(handle.get_schema().get_serialize_size(), handle.get_serialize_size());

  ObStorageSchema invalid;
  ASSERT_EQ(OB_INVALID_ARGUMENT, handle.assign(allocator_, invalid));
  ASSERT_EQ(-1, handle.serialize_size_);
  ASSERT_EQ(handle.get_schema().get_serialize_size(), handle.get_serialize_size());
  handle.reset();
  ASSERT_EQ(-1, handle.serialize_size_);
  ASSERT_EQ(handle.get_schema().get_serialize_size(), handle.get_serialize_size());
  pos = 0;
  ASSERT_NE(OB_SUCCESS, handle.deserialize(allocator_, wire.data(), 1, pos));
  ASSERT_EQ(-1, handle.serialize_size_);
  ASSERT_EQ(handle.get_schema().get_serialize_size(), handle.get_serialize_size());
  ASSERT_EQ(OB_SUCCESS, handle.init_placeholder(allocator_));
  ASSERT_EQ(-1, handle.serialize_size_);
  ASSERT_EQ(original_size, handle.get_serialize_size());

  ObStorageSchema source;
  ASSERT_EQ(OB_SUCCESS, source.init_placeholder(allocator_));
  for (int64_t version = ObStorageSchema::STORAGE_SCHEMA_VERSION;
      version <= ObStorageSchema::STORAGE_SCHEMA_VERSION_LATEST; ++version) {
    source.storage_schema_version_ = version;
    std::vector<char> legacy_wire(source.get_serialize_size());
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, source.serialize(legacy_wire.data(), legacy_wire.size(), pos));
    handle.reset();
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, handle.deserialize(allocator_, legacy_wire.data(), legacy_wire.size(), pos)) << version;
    ASSERT_EQ(-1, handle.serialize_size_);
    ASSERT_EQ(OB_SUCCESS, handle.prepare_serialize_size()) << version;
    ASSERT_EQ(handle.get_schema().get_serialize_size(), handle.get_serialize_size()) << version;
    if (version >= ObStorageSchema::STORAGE_SCHEMA_VERSION_V8) {
      // Decoding replaces the unspecified engine (255, two bytes) with engine 0 (one byte).
      ASSERT_EQ(legacy_wire.size() - 1, handle.get_serialize_size());
    }
    std::vector<char> encoded(handle.get_serialize_size());
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, handle.serialize(encoded.data(), encoded.size(), pos)) << version;
    ASSERT_EQ(encoded.size(), pos) << version;
    ObStorageSchemaHandle second_decode;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, second_decode.deserialize(allocator_, encoded.data(), encoded.size(), pos)) << version;
    ASSERT_EQ(encoded.size(), second_decode.get_serialize_size()) << version;
    std::vector<char> reencoded(second_decode.get_serialize_size());
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, second_decode.serialize(reencoded.data(), reencoded.size(), pos)) << version;
    ASSERT_EQ(encoded, reencoded) << version;
  }
}

TEST_F(TestStorageSchema, schema_handle_column_update)
{
  ObStorageSchema schema;
  ObStorageSchemaHandle handle;
  ASSERT_EQ(OB_INVALID_ARGUMENT, handle.update_column_cnt_and_schema_version(2, 2, 10));
  ASSERT_EQ(OB_ERR_UNEXPECTED, schema.update_column_cnt_and_schema_version(2, 2, 10));
  ASSERT_EQ(OB_SUCCESS, schema.init_placeholder(allocator_));
  ASSERT_EQ(OB_SUCCESS, handle.init_placeholder(allocator_));
  ASSERT_EQ(OB_SUCCESS, schema.update_column_cnt_and_schema_version(2, 2, 10));
  ASSERT_EQ(OB_SUCCESS, handle.update_column_cnt_and_schema_version(2, 2, 10));
  ASSERT_EQ(schema.column_cnt_, handle.get_schema().column_cnt_);
  ASSERT_EQ(schema.store_column_cnt_, handle.get_schema().store_column_cnt_);
  ASSERT_EQ(schema.schema_version_, handle.get_schema().schema_version_);
  ASSERT_EQ(schema.is_column_info_simplified(), handle.get_schema().is_column_info_simplified());
  ASSERT_EQ(2, handle.get_schema().column_cnt_);
  ASSERT_EQ(10, handle.get_schema().schema_version_);
  ASSERT_TRUE(handle.get_schema().is_column_info_simplified());
  ASSERT_TRUE(handle.is_valid());
  ASSERT_TRUE(handle.get_schema().is_valid());
  // Inject an invalid field to verify that the update error invalidates both cached results.
  ASSERT_EQ(OB_SUCCESS, handle.prepare_serialize_size());
  ASSERT_EQ(handle.get_schema().get_serialize_size(), handle.get_serialize_size());
  handle.schema_.pctfree_ = -1;
  ASSERT_EQ(OB_ERR_UNEXPECTED, handle.update_column_cnt_and_schema_version(3, 3, 11));
  ASSERT_FALSE(handle.is_valid());
  ASSERT_EQ(-1, handle.serialize_size_);
  ASSERT_EQ(OB_SUCCESS, handle.init_placeholder(allocator_));
  ASSERT_EQ(1, handle.get_schema().column_cnt_);
  ASSERT_TRUE(handle.is_valid());
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
