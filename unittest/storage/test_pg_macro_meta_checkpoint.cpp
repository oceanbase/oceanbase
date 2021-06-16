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
#include "lib/container/ob_array.h"
#include "lib/stat/ob_diagnose_info.h"
#define private public
#include "share/ob_tenant_mgr.h"
#include "blocksstable/ob_row_generate.h"
#include "blocksstable/ob_data_file_prepare.h"
#include "storage/ob_sstable_test.h"
#include "storage/ob_pg_meta_checkpoint_writer.h"
#include "storage/ob_pg_meta_checkpoint_reader.h"
#include "storage/ob_pg_macro_meta_checkpoint_writer.h"
#include "storage/ob_pg_macro_meta_checkpoint_reader.h"
#undef private

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest {
class TestPGMacroMetaCheckpoint : public ObSSTableTest {
public:
  TestPGMacroMetaCheckpoint();

  virtual ~TestPGMacroMetaCheckpoint() = default;

  void prepare_sstable(const ObITable::TableKey& table_key, const int64_t row_cnt, ObSSTable& sstable);

  void test_pg_macro_meta_checkpoint(const int64_t sstable_cnt, const int64_t row_cnt);

protected:
  int check_macro_meta(ObPGMacroMetaCheckpointReader& reader, const common::ObIArray<MacroBlockId>& block_ids);

protected:
  static const int64_t MACRO_BLOCK_SIZE = 128 * 1024;
  static const int64_t MACRO_BLOCK_CNT = 500;
  common::ObPGKey pg_key_;
};

TestPGMacroMetaCheckpoint::TestPGMacroMetaCheckpoint()
    : ObSSTableTest("test_pg_macro_meta_checkpoint", MACRO_BLOCK_SIZE, MACRO_BLOCK_CNT),
      pg_key_(combine_id(1, 3001), 0, 0)
{}

void TestPGMacroMetaCheckpoint::prepare_sstable(
    const ObITable::TableKey& table_key, const int64_t row_count, ObSSTable& sstable)
{
  int ret = OB_SUCCESS;
  int64_t data_version = 1;
  int64_t write_row_count = 0;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ObDataStoreDesc data_desc;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObCreateSSTableParamWithTable param;
  ObRowGenerate row_generate;

  param.table_key_ = table_key;
  param.schema_ = &table_schema_;
  param.schema_version_ = 10;
  param.checksum_method_ = oceanbase::blocksstable::CCM_VALUE_ONLY;
  param.pg_key_ = pg_key_;
  param.logical_data_version_ = table_key.version_;
  sstable.destroy();
  ret = sstable.init(table_key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, sstable.set_storage_file_handle(get_storage_file_handle()));
  ret = sstable.open(param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObPGKey pg_key(combine_id(1, table_schema_.get_tablegroup_id()), 1, table_schema_.get_partition_cnt());
  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;
  ret = ObFileSystemUtil::get_pg_file_with_guard(pg_key, pg_guard, file);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = data_desc.init(table_schema_,
      data_version,
      NULL,
      1,
      MAJOR_MERGE,
      true,
      true,
      pg_key,
      pg_guard.get_partition_group()->get_storage_file_handle());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.open(data_desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = row_generate.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  write_row_count = 0;
  while (write_row_count < row_count) {
    ret = row_generate.get_next_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = writer.append_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ++write_row_count;
  }

  // close sstable
  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, sstable.append_macro_blocks(writer.get_macro_block_write_ctx()));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestPGMacroMetaCheckpoint::test_pg_macro_meta_checkpoint(const int64_t sstable_cnt, const int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_SSTABLE_CNT = 16;
  const ObAddr self_addr(ObAddr::IPV4, "127.0.0.01", 80);
  ObSSTable sstables[MAX_SSTABLE_CNT];
  ObITable::TableKey table_key;
  ObTablesHandle tables_handle;
  ObPGMetaItemWriter item_writer;
  ObPGMacroMetaCheckpointWriter writer;
  ObPGMacroMetaCheckpointReader reader;
  ObArray<blocksstable::MacroBlockId> macro_block_ids;
  const int64_t table_id = combine_id(1, 3001);
  table_key.table_type_ = ObITable::MAJOR_SSTABLE;
  table_key.pkey_ = ObPartitionKey(table_id, 0, 0);
  table_key.table_id_ = table_id;
  table_key.version_ = ObVersion(1, 0);
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.snapshot_version_ = 20;
  ASSERT_TRUE(sstable_cnt <= MAX_SSTABLE_CNT);
  ObStorageFile* file = nullptr;
  ObSuperBlockMetaEntry meta_entry;
  ObStoreFileSystem& file_system = get_file_system();
  ASSERT_EQ(OB_SUCCESS, file_system.alloc_file(file));
  ASSERT_EQ(OB_SUCCESS, file->init(self_addr, 1, 0, ObStorageFile::FileType::TENANT_DATA));
  ObStorageFileHandle file_handle;
  ObStorageFileWithRef file_with_ref;
  file_with_ref.file_ = file;
  file_handle.set_storage_file_with_ref(file_with_ref);
  ASSERT_EQ(OB_SUCCESS, item_writer.init(file_handle));
  for (int64_t i = 0; i < sstable_cnt; ++i) {
    table_key.trans_version_range_.multi_version_start_ = i * 20;
    table_key.trans_version_range_.base_version_ = i * 20;
    table_key.trans_version_range_.snapshot_version_ = (i + 1) * 20;
    prepare_sstable(table_key, row_cnt, sstables[i]);
    ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstables[i]));
    const ObIArray<MacroBlockId>& sstable_block_ids = sstables[i].get_macro_block_ids();
    for (int64_t i = 0; i < sstable_block_ids.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, macro_block_ids.push_back(sstable_block_ids.at(i)));
    }
  }
  ASSERT_EQ(OB_SUCCESS, writer.init(tables_handle, item_writer));
  ASSERT_EQ(OB_SUCCESS, writer.write_checkpoint());
  ASSERT_EQ(OB_SUCCESS, item_writer.close());
  MacroBlockId entry_block;
  ASSERT_EQ(OB_SUCCESS, item_writer.get_entry_block_index(entry_block));
  ASSERT_TRUE(entry_block.is_valid());
  ObMacroMetaReplayMap replay_map;
  ASSERT_EQ(OB_SUCCESS, replay_map.init());
  ASSERT_EQ(OB_SUCCESS, reader.init(entry_block, file_handle, &replay_map));
  ASSERT_EQ(OB_SUCCESS, check_macro_meta(reader, macro_block_ids));
}

int TestPGMacroMetaCheckpoint::check_macro_meta(
    ObPGMacroMetaCheckpointReader& reader, const ObIArray<MacroBlockId>& block_ids)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaV2 meta;
  ObObj endkey[OB_MAX_ROWKEY_COLUMN_NUMBER];
  int64_t i = 0;
  while (OB_SUCC(ret)) {
    MEMSET(&meta, 0, sizeof(meta));
    meta.endkey_ = endkey;
    ObPGMacroBlockMetaCheckpointEntry entry(meta);
    if (OB_FAIL(reader.read_next_entry(entry))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to read next entry", K(ret));
      } else {
        ret = OB_SUCCESS;
        if (i != block_ids.count()) {
          ret = OB_ERROR;
          STORAGE_LOG(ERROR, "block id count is not equal", K(i), K(block_ids.count()));
        }
        break;
      }
    } else if (entry.macro_block_id_.block_index() != block_ids.at(i).block_index()) {
      ret = OB_ERROR;
      STORAGE_LOG(ERROR, "block id not equal", K(i), K(entry.macro_block_id_), K(block_ids.at(i)));
    } else {
      STORAGE_LOG(INFO, "compare block id success", K(i), K(block_ids.at(i)));
    }
    ++i;
  }
  return ret;
}

TEST_F(TestPGMacroMetaCheckpoint, test_pg_macro_meta_checkpoint)
{
  test_pg_macro_meta_checkpoint(1, 300);
  test_pg_macro_meta_checkpoint(1, 300);
  test_pg_macro_meta_checkpoint(2, 300);
  test_pg_macro_meta_checkpoint(13, 300);
}

TEST_F(TestPGMacroMetaCheckpoint, test_pg_macro_meta_checkpoint_large_than_macro_block)
{
  test_pg_macro_meta_checkpoint(13, 1600);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_pg_macro_meta_checkpoint.log*");
  //  OB_LOGGER.set_file_name("test_pg_macro_meta_checkpoint.log", true, false);
  //  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
