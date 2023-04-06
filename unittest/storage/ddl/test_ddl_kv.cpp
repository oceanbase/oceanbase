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

#define USING_LOG_PREFIX STORAGE

#define ASSERT_OK(x) ASSERT_EQ(OB_SUCCESS, (x))
#include <gtest/gtest.h>

#define private public
#define protected public
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/blocksstable/ob_row_generate.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#undef private

namespace oceanbase
{

using namespace common;
using namespace lib;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace share::schema;

static const uint64_t TEST_TENANT_ID = 1002;
static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
static const int64_t TEST_ROW_CNT = 1000;
static const int64_t SNAPSHOT_VERSION = 2;


class TestBlockMetaTree : public ::testing::Test
{
public:
  TestBlockMetaTree(): tenant_base_(TEST_TENANT_ID) {}
  virtual void SetUp();
  virtual void TearDown();

  void prepare_schema();

protected:
  ObTenantBase tenant_base_;
  ObTableSchema table_schema_;
  ObDataStoreDesc data_desc_;
  ObRowGenerate row_generate_;
  ObArenaAllocator allocator_;
};


void TestBlockMetaTree::SetUp()
{
  TearDown();
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(TEST_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_base_);
  prepare_schema();
  ASSERT_OK(data_desc_.init(table_schema_, ObLSID(1), ObTabletID(1), MAJOR_MERGE));
  ASSERT_OK(row_generate_.init(table_schema_, &allocator_));
}

void TestBlockMetaTree::TearDown()
{
  table_schema_.reset();
  row_generate_.reset();
  data_desc_.reset();
  tenant_base_.destroy();
  ObTenantEnv::set_tenant(nullptr);
}

void TestBlockMetaTree::prepare_schema()
{

  ObColumnSchemaV2 column;
  uint64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_ddl_kv"));
  table_schema_.set_tenant_id(TEST_TENANT_ID);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(ENCODING_ROW_STORE);
  //init column
  const int64_t schema_version = 1;
  const int64_t rowkey_count = TEST_ROWKEY_COLUMN_CNT;
  const int64_t column_count = TEST_COLUMN_CNT;
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_data_length(1);
    if(obj_type == common::ObInt32Type){
      column.set_rowkey_position(1);
      column.set_order_in_rowkey(ObOrderType::ASC);
//    } else if(obj_type == common::ObIntType) { // wenqu test
//      column.set_rowkey_position(2);
//      column.set_order_in_rowkey(ObOrderType::ASC);
    } else if(obj_type == common::ObVarcharType) {
      column.set_rowkey_position(2);
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_order_in_rowkey(ObOrderType::DESC);
    } else {
      column.set_rowkey_position(0);
    }
    share::schema::ObColDesc col;
    col.col_id_ = static_cast<uint64_t>(i + OB_APP_MIN_COLUMN_ID);
    col.col_type_.set_type(obj_type);
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

TEST_F(TestBlockMetaTree, random_keybtree)
{
  ObBlockMetaTree meta_tree;

  LOG_INFO("wenqu debug: check size",
      "sizeof(BtreeNode)", sizeof(keybtree::BtreeNode<blocksstable::ObDatumRowkeyWrapper, const blocksstable::ObDataMacroBlockMeta *>),
      "sizeof(Iterator)", sizeof(keybtree::Iterator<blocksstable::ObDatumRowkeyWrapper, const blocksstable::ObDataMacroBlockMeta *>));

  ObDDLMacroHandle macro_handle;
  macro_handle.block_id_ = MacroBlockId(0, 1, 0);
  ObDataMacroBlockMeta meta;

  FILE *dump_file = fopen("btree.log", "w");

  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_OK(meta_tree.block_tree_.init());
    meta_tree.is_inited_ = true;
    ASSERT_OK(meta_tree.data_desc_.assign(data_desc_));
    for (int64_t j = 0; j < 10000; ++j) {
      void *buf = allocator_.alloc(sizeof(ObDatumRow));
      ASSERT_TRUE(nullptr != buf);
      ObDatumRow *row = new (buf) ObDatumRow();
      ASSERT_OK(row->init(allocator_, TEST_COLUMN_CNT));
      ASSERT_OK(row_generate_.get_next_row(j, *row));

      buf = allocator_.alloc(sizeof(ObDatumRowkey));
      ASSERT_TRUE(nullptr != buf);
      ObDatumRowkey *rowkey = new (buf) ObDatumRowkey(row->storage_datums_, TEST_ROWKEY_COLUMN_CNT);

      if (REACH_COUNT_INTERVAL(1000)) {
        LOG_INFO("wenqu: generate row", K(*rowkey), KP(rowkey->datums_), K(i), K(j));
        meta_tree.block_tree_.print(dump_file);
      }
      ASSERT_EQ(OB_SUCCESS, meta_tree.insert_macro_block(
            macro_handle,
            rowkey,
            &meta)) << "i: " << i << ", j: " << j << "\n";
    }
    meta_tree.destroy();
  }
  fclose(dump_file);
}

TEST_F(TestBlockMetaTree, update_ddl_sstable)
{
  ObTenantMetaMemMgr t3m(TEST_TENANT_ID);
  ASSERT_OK(t3m.init());
  tenant_base_.set(&t3m);
  ObTenantEnv::set_tenant(&tenant_base_);
  ObTabletTableStore old_table_store;
  ObArenaAllocator arena;
  ObStorageSchema storage_schema;
  ASSERT_OK(storage_schema.init(arena, table_schema_, Worker::CompatMode::MYSQL));
  ObArray<ObITable *> ddl_sstables;
  for (int64_t i = 1; i <= 5; ++i) {
    void *buf = arena.alloc(sizeof(ObSSTable));
    ObSSTable *tmp_sstable = new (buf) ObSSTable();
    tmp_sstable->key_.table_type_ = ObITable::DDL_DUMP_SSTABLE;
    tmp_sstable->key_.scn_range_.start_scn_ = SCN::plus(SCN::min_scn(), 10 * i);
    tmp_sstable->key_.scn_range_.end_scn_ = SCN::plus(SCN::min_scn(), 10 * (i + 1));
    ASSERT_OK(ddl_sstables.push_back(tmp_sstable));
  }
  ObSSTable compact_sstable;
  compact_sstable.key_.table_type_ = ObITable::DDL_DUMP_SSTABLE;
  compact_sstable.key_.scn_range_.start_scn_ = SCN::plus(SCN::min_scn(), 10);
  compact_sstable.key_.scn_range_.end_scn_ = SCN::plus(SCN::min_scn(), 60);

  ObUpdateTableStoreParam update_param(1, //snapshot_version,
                                       1, //multi_version_start,
                                       &storage_schema, //storage_schema,
                                       1//rebuild_seq
      );
  update_param.ddl_info_.keep_old_ddl_sstable_ = true;
  update_param.table_handle_.set_table(&compact_sstable, &arena);
  ASSERT_OK(old_table_store.ddl_sstables_.init_and_copy(arena, ddl_sstables));
  ObTabletTableStore new_table_store;
  ObTablet tablet;
  tablet.tablet_meta_.start_scn_ = SCN::plus(SCN::min_scn(), 10);
  compact_sstable.meta_.basic_meta_.ddl_scn_ = tablet.tablet_meta_.start_scn_;
  new_table_store.tablet_ptr_ = &tablet;
  ASSERT_OK(new_table_store.build_ddl_sstables(arena, update_param, old_table_store));
  ASSERT_EQ(new_table_store.ddl_sstables_.count(), 1);
  ASSERT_EQ(new_table_store.ddl_sstables_.get_boundary_table(true)->get_start_scn(), compact_sstable.get_start_scn());
  ASSERT_EQ(new_table_store.ddl_sstables_.get_boundary_table(true)->get_end_scn(), compact_sstable.get_end_scn());
}

}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -rf btree.log");
  system("rm -rf test_ddl_kv.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ddl_kv.log", true);
  return RUN_ALL_TESTS();
}
