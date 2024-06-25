/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define private public
#define protected public

#include "lib/allocator/page_arena.h"
#include "storage/ob_i_table.h"
#include "storage/schema_utils.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ob_storage_struct.h"
#include "storage/tablet/ob_table_store_util.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/ob_storage_schema.h"
#include "share/schema/ob_table_schema.h"


namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace share;
using namespace share::schema;

namespace unittest
{

class TestCOSSTable: public ::testing::Test
{
public:
  static void generate_table_key(
    const ObITable::TableType &type,
    const int64_t base_version,
    const int64_t snapshot_version,
    ObITable::TableKey &table_key);

  static int mock_major_sstable(
    const ObITable::TableType &type,
    ObArenaAllocator &allocator,
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t column_group_cnt,
    ObTableHandleV2 &table_handle);

  static int mock_column_store_schema(
    common::ObIAllocator &allocator,
    const bool with_all_cg,
    ObStorageSchema &storage_schema);

public:
  TestCOSSTable();
  ~TestCOSSTable() = default;
public:
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static constexpr int64_t TEST_TENANT_ID = 1;
  static constexpr int64_t TEST_TABLET_ID = 2323233;
  ObTabletID tablet_id_;
  ObArenaAllocator allocator_;
};

TestCOSSTable::TestCOSSTable()
  : tablet_id_(TEST_TABLET_ID),
    allocator_()
{
}

void TestCOSSTable::SetUp()
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
  ret = t3m->init();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestCOSSTable::TearDown()
{
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
}

void TestCOSSTable::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestCOSSTable::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestCOSSTable::generate_table_key(
    const ObITable::TableType &type,
    const int64_t base_version,
    const int64_t snapshot_version,
    ObITable::TableKey &table_key)
{
  table_key.reset();
  table_key.tablet_id_ = TEST_TABLET_ID;
  table_key.table_type_ = type;
  table_key.version_range_.base_version_ = base_version;
  table_key.version_range_.snapshot_version_ = snapshot_version;
}

int TestCOSSTable::mock_major_sstable(
  const ObITable::TableType &type,
  ObArenaAllocator &allocator,
  const int64_t base_version,
  const int64_t snapshot_version,
  const int64_t column_group_cnt,
  ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ObITable::TableKey table_key;
  generate_table_key(type, base_version, snapshot_version, table_key);

  ObTabletID tablet_id;
  tablet_id = TEST_TABLET_ID;
  ObTabletCreateSSTableParam param;
  ObSSTable *sstable = nullptr;

  ObStorageSchema storage_schema;
  if (OB_FAIL(storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to init storage schema", K(ret));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::build_create_sstable_param(storage_schema, tablet_id, snapshot_version, param))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(table_key));
  } else if (FALSE_IT(param.table_key_ = table_key)) {
  } else if (FALSE_IT(param.column_group_cnt_ = column_group_cnt)) {
  } else if (ObITable::TableType::COLUMN_ORIENTED_SSTABLE == type) {
    param.co_base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator, table_handle))) {
      LOG_WARN("failed to create sstable", K(param));
    }
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator, table_handle))) {
    LOG_WARN("failed to create sstable", K(param));
  }

  if (FAILEDx(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  }
  return ret;
}

int TestCOSSTable::mock_column_store_schema(
    common::ObIAllocator &allocator,
    const bool with_all_cg,
    ObStorageSchema &storage_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);

  ObArray<ObColDesc> col_ids;
  ObArray<ObColDesc> rowkey_col_ids;

  int64_t store_column_count;
  if (OB_FAIL(table_schema.get_store_column_ids(col_ids))) {
    LOG_WARN("failed to get store column ids", K(ret));
  } else if (OB_FAIL(table_schema.get_rowkey_column_ids(rowkey_col_ids))) {
    LOG_WARN("failed to get rowkey column ids", K(ret));
  } else if (OB_FAIL(table_schema.get_store_column_count(store_column_count))) {
    LOG_WARN("failed to get store column count", K(ret));
  } else if (store_column_count != col_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected col cnt", K(store_column_count), K(col_ids.count()), K(col_ids));
  } else {
    // build base cg schema
    ObColumnGroupSchema base_cg;
    base_cg.column_group_type_ = with_all_cg
                               ? ObColumnGroupType::ALL_COLUMN_GROUP
                               : ObColumnGroupType::ROWKEY_COLUMN_GROUP;
    base_cg.column_group_id_ = store_column_count;
    base_cg.column_id_cnt_ = with_all_cg
                           ? store_column_count
                           : table_schema.get_rowkey_column_num();

    uint64_t *base_cg_ids = static_cast<uint64_t *>(allocator.alloc(base_cg.column_id_cnt_ * sizeof(uint64_t)));
    const ObArray<ObColDesc> &base_column_ids = with_all_cg
                                              ? col_ids
                                              : rowkey_col_ids;
    for(int64_t i = 0; i < base_cg.column_id_cnt_; ++i) {
      base_cg_ids[i] = base_column_ids.at(i).col_id_;
    }
    base_cg.column_id_arr_ = base_cg_ids;
    base_cg.column_group_name_ = "test_all";
    base_cg.row_store_type_ = FLAT_ROW_STORE;
    table_schema.add_column_group(base_cg);

    //add normal cg
    for(int64_t i = 0; i < store_column_count; i++) {
      ObColumnGroupSchema normal_cg;
      normal_cg.column_group_type_ = ObColumnGroupType::SINGLE_COLUMN_GROUP;
      normal_cg.column_group_id_ = i;
      char c = i + '0';
      normal_cg.column_group_name_ = &c;
      normal_cg.column_id_cnt_ = 1;
      normal_cg.column_id_arr_capacity_ = 1;
      uint64_t column_ids[1] = { col_ids.at(i).col_id_ };
      normal_cg.column_id_arr_ = column_ids;
      normal_cg.row_store_type_ = FLAT_ROW_STORE;
      table_schema.add_column_group(normal_cg);
    }
  }

  if (OB_FAIL(storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to init storage schema", K(ret));
  }

  return ret;
}


TEST_F(TestCOSSTable, co_table_basic_test)
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  // create co sstable
  const int64_t base_version = 0;
  const int64_t snapshot_version = 100;
  ObTableHandleV2 co_handle;
  ret = TestCOSSTable::mock_major_sstable(ObITable::COLUMN_ORIENTED_SSTABLE, allocator_, base_version, snapshot_version, 2, co_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObITable *co_table = co_handle.get_table();
  ASSERT_EQ(true, co_table->is_co_sstable());

  // create cg sstable
  ObTableHandleV2 cg_handle;
  ret = TestCOSSTable::mock_major_sstable(ObITable::NORMAL_COLUMN_GROUP_SSTABLE, allocator_, base_version, snapshot_version, 1, cg_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObITable *cg_table = cg_handle.get_table();
  ASSERT_EQ(true, cg_table->is_cg_sstable());

  // add cg table to co sstable
  ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(co_table);
  ObSEArray<ObITable *, 1> cg_sstables;
  ret = cg_sstables.push_back(cg_table);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = co_sstable->fill_cg_sstables(cg_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, co_sstable->valid_for_cs_reading_);

  // serialize co sstable
  const int64_t buf_len = co_sstable->get_serialize_size();
  char * encode_buf = static_cast<char *>(allocator_.alloc(sizeof(char) * buf_len));
  int64_t pos = 0;
  ret = co_sstable->serialize(encode_buf, buf_len, pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  // deserialize co sstable
  void* decode_buf = allocator_.alloc(sizeof(ObCOSSTableV2));
  ObCOSSTableV2 *decode_table = new (decode_buf) ObCOSSTableV2();
  pos = 0;
  ret = decode_table->deserialize(allocator_, encode_buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(buf_len, pos);
}


TEST_F(TestCOSSTable, get_cg_table_test)
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  // create co sstable
  const int64_t base_version = 0;
  const int64_t snapshot_version = 100;
  ObTableHandleV2 co_handle;
  ret = TestCOSSTable::mock_major_sstable(ObITable::COLUMN_ORIENTED_SSTABLE, allocator_, base_version, snapshot_version, 3, co_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObITable *co_table = co_handle.get_table();
  ASSERT_EQ(true, co_table->is_co_sstable());
  co_table->key_.column_group_idx_ = 1;
  static_cast<ObCOSSTableV2 *>(co_table)->base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;

  // create cg sstables
  ObTableHandleV2 cg_handle1;
  ret = TestCOSSTable::mock_major_sstable(ObITable::NORMAL_COLUMN_GROUP_SSTABLE, allocator_, base_version, snapshot_version, 1, cg_handle1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObITable *cg_table1 = cg_handle1.get_table();
  ASSERT_EQ(true, cg_table1->is_cg_sstable());
  cg_table1->key_.column_group_idx_ = 0;

  ObTableHandleV2 cg_handle2;
  ret = TestCOSSTable::mock_major_sstable(ObITable::NORMAL_COLUMN_GROUP_SSTABLE, allocator_, base_version, snapshot_version, 1, cg_handle2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObITable *cg_table2 = cg_handle2.get_table();
  ASSERT_EQ(true, cg_table2->is_cg_sstable());
  cg_table2->key_.column_group_idx_ = 2;


  // add cg table to co sstable
  ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(co_table);
  ObSEArray<ObITable *, 2> cg_sstables;
  ret = cg_sstables.push_back(cg_table1);
  ret = cg_sstables.push_back(cg_table2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = co_sstable->fill_cg_sstables(cg_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, co_sstable->valid_for_cs_reading_);


  ObSSTableWrapper get_cg_table1;
  ret = co_sstable->get_cg_sstable(1, get_cg_table1);
  EXPECT_EQ(co_sstable->key_.column_group_idx_, get_cg_table1.sstable_->key_.column_group_idx_);

  ObSSTableWrapper get_cg_table2;
  ret = co_sstable->get_cg_sstable(0, get_cg_table2);
  EXPECT_EQ(cg_table1->key_.column_group_idx_, get_cg_table2.sstable_->key_.column_group_idx_);

  ObSSTableWrapper get_cg_table3;
  ret = co_sstable->get_cg_sstable(2, get_cg_table3);
  EXPECT_EQ(cg_table2->key_.column_group_idx_, get_cg_table3.sstable_->key_.column_group_idx_);

  ObSSTableWrapper get_cg_table4;
  ret = co_sstable->get_cg_sstable(3, get_cg_table4);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST_F(TestCOSSTable, without_all_cg_test)
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  ObArenaAllocator allocator;
  ObStorageSchema storage_schema;
  ret = mock_column_store_schema(allocator, false/*with all cg*/, storage_schema);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 co_table_handle;
  int64_t snapshot_version = 100;
  ObTabletID tablet_id;
  tablet_id.id_ = TEST_TABLET_ID;
  ret = ObTabletCreateDeleteHelper::create_empty_sstable(
                                    allocator,
                                    storage_schema,
                                    tablet_id,
                                    snapshot_version,
                                    co_table_handle);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObCOSSTableV2 *co_table = static_cast<ObCOSSTableV2 *>(co_table_handle.get_table());
  ASSERT_TRUE(co_table->is_rowkey_cg_base());
}


TEST_F(TestCOSSTable, empty_co_table_test)
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  ObArenaAllocator allocator;
  ObStorageSchema storage_schema;
  ret = mock_column_store_schema(allocator, true/*with all cg*/, storage_schema);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 co_table_handle;
  int64_t snapshot_version = 100;
  ObTabletID tablet_id;
  tablet_id.id_ = TEST_TABLET_ID;
  ret = ObTabletCreateDeleteHelper::create_empty_sstable(
                                    allocator,
                                    storage_schema,
                                    tablet_id,
                                    snapshot_version,
                                    co_table_handle);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObCOSSTableV2 *co_table = static_cast<ObCOSSTableV2 *>(co_table_handle.get_table());
  EXPECT_EQ(0, co_table->meta_->cg_sstables_.count());
  EXPECT_EQ(true, co_table->is_empty());
  EXPECT_EQ(storage_schema.get_column_group_count(), co_table->get_cs_meta().column_group_cnt_);
}

TEST_F(TestCOSSTable, copy_from_old_sstable_test)
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  // create co sstable
  const int64_t base_version = 0;
  const int64_t snapshot_version = 100;
  ObTableHandleV2 co_handle;
  ret = TestCOSSTable::mock_major_sstable(ObITable::COLUMN_ORIENTED_SSTABLE, allocator_, base_version, snapshot_version, 2, co_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObITable *co_table = co_handle.get_table();
  ASSERT_EQ(true, co_table->is_co_sstable());

  // create cg sstable
  ObTableHandleV2 cg_handle;
  ret = TestCOSSTable::mock_major_sstable(ObITable::NORMAL_COLUMN_GROUP_SSTABLE, allocator_, base_version, snapshot_version, 1, cg_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObITable *cg_table = cg_handle.get_table();
  ASSERT_EQ(true, cg_table->is_cg_sstable());

  // add cg table to co sstable
  ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(co_table);
  ObSEArray<ObITable *, 1> cg_sstables;
  ret = cg_sstables.push_back(cg_table);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = co_sstable->fill_cg_sstables(cg_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, co_sstable->valid_for_cs_reading_);

  // copy old sstable and test
  ObSSTable *copied_sstable = nullptr;
  ObSSTable::copy_from_old_sstable(*co_sstable, allocator_, copied_sstable);
  ASSERT_TRUE(copied_sstable->is_tmp_sstable_);
  ASSERT_EQ(1, copied_sstable->meta_->cg_sstables_.count());
  ASSERT_TRUE(static_cast<ObSSTable *>(copied_sstable->meta_->cg_sstables_.at(0))->is_tmp_sstable_);
}

} //namespace unittest
} //namespace oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_co_sstable.log");
  OB_LOGGER.set_file_name("test_co_sstable.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  CLOG_LOG(INFO, "begin unittest: test_co_sstable");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
