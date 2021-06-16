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

class TestCheckpoint : public TestDataFilePrepare {
public:
  TestCheckpoint();
  virtual ~TestCheckpoint() = default;
  virtual void SetUp();
  virtual void TearDown();

protected:
  void test_pg_meta_checkpoint(const int64_t meta_size);

protected:
  static const int64_t MACRO_BLOCK_SIZE = 128 * 1024;
  common::ObPGKey pg_key_;
};

TestCheckpoint::TestCheckpoint()
    : TestDataFilePrepare("checkpoint_test", MACRO_BLOCK_SIZE), pg_key_(combine_id(1, 1000), 0, 0)
{}

void TestCheckpoint::SetUp()
{
  TestDataFilePrepare::SetUp();
}

void TestCheckpoint::TearDown()
{
  TestDataFilePrepare::TearDown();
}

void TestCheckpoint::test_pg_meta_checkpoint(const int64_t meta_size)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TEST_SIZE = 16 * MACRO_BLOCK_SIZE;
  const ObAddr self_addr(ObAddr::IPV4, "127.0.0.01", 80);
  ObArenaAllocator allocator(ObModIds::TEST);
  char* buf = nullptr;
  buf = static_cast<char*>(allocator.alloc(MAX_TEST_SIZE));
  ASSERT_NE(nullptr, buf);
  for (int64_t i = 0; i < MAX_TEST_SIZE; ++i) {
    buf[i] = i % 128;
  }
  ObPGMetaItem item;
  ObPGMetaItemBuffer read_item;
  ASSERT_TRUE(meta_size <= MAX_TEST_SIZE);
  item.set_serialize_buf(buf, meta_size);
  ObPGMetaItemWriter item_writer;
  ObPGMetaCheckpointWriter writer;
  ObPGMetaCheckpointReader reader;
  blocksstable::ObSuperBlockMetaEntry entry;
  ObStorageFile* file = nullptr;
  ObStoreFileSystem& file_system = get_file_system();
  ASSERT_EQ(OB_SUCCESS, file_system.alloc_file(file));
  ASSERT_EQ(OB_SUCCESS, file->init(self_addr, 1, 0, ObStorageFile::FileType::TENANT_DATA));
  ObStorageFileHandle file_handle;
  ObStorageFileWithRef file_with_ref;
  file_with_ref.file_ = file;
  file_handle.set_storage_file_with_ref(file_with_ref);
  ASSERT_EQ(OB_SUCCESS, item_writer.init(file_handle));
  ASSERT_EQ(OB_SUCCESS, writer.init(item, item_writer));
  ASSERT_EQ(OB_SUCCESS, writer.write_checkpoint());
  ASSERT_EQ(OB_SUCCESS, item_writer.close());
  ASSERT_EQ(OB_SUCCESS, item_writer.get_entry_block_index(entry.macro_block_id_));
  ASSERT_TRUE(entry.macro_block_id_.is_valid());
  STORAGE_LOG(INFO, "entry_block", K(entry));

  ObPartitionMetaRedoModule pg_mgr;
  ASSERT_EQ(OB_SUCCESS, reader.init(entry.macro_block_id_, file_handle, pg_mgr));
  ASSERT_EQ(OB_SUCCESS, reader.read_item(read_item));
  STORAGE_LOG(INFO, "read_item", K(read_item));
  ASSERT_EQ(item.buf_len_, read_item.buf_len_);
  for (int64_t i = 0; i < read_item.buf_len_; ++i) {
    STORAGE_LOG(INFO, "compare item", K(i));
    ASSERT_EQ(item.buf_[i], read_item.buf_[i]);
  }
  int cmp = memcmp(item.buf_, read_item.buf_, item.buf_len_);
  ASSERT_EQ(0, cmp);
}

TEST_F(TestCheckpoint, test_pg_meta_checkpoint)
{
  test_pg_meta_checkpoint(MACRO_BLOCK_SIZE / 2);
  test_pg_meta_checkpoint(MACRO_BLOCK_SIZE / 4 * 3);
  test_pg_meta_checkpoint(MACRO_BLOCK_SIZE - 50);
  test_pg_meta_checkpoint(MACRO_BLOCK_SIZE);
  test_pg_meta_checkpoint(2 * MACRO_BLOCK_SIZE);
  test_pg_meta_checkpoint(3 * MACRO_BLOCK_SIZE);
  test_pg_meta_checkpoint(6 * MACRO_BLOCK_SIZE);
  test_pg_meta_checkpoint(8 * MACRO_BLOCK_SIZE);
  test_pg_meta_checkpoint(12 * MACRO_BLOCK_SIZE);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_pg_meta_checkpoint.log*");
  //  OB_LOGGER.set_file_name("test_pg_meta_checkpoint.log", true, false);
  //  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
