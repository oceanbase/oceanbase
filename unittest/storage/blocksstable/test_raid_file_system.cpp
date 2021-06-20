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
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "share/ob_define.h"
#include "storage/blocksstable/ob_raid_file_system.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/ob_task_define.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace share;

namespace unittest {

static const char* TEST_CASE_NAME = "test_raid_file_system_data";
class TestRaidFileSystem : public ::testing::Test {
public:
  virtual void SetUp()
  {
    for (int64_t i = 0; i < OB_DEFAULT_MACRO_BLOCK_SIZE; ++i) {
      buf_[i] = (char)ObRandom::rand(INT8_MIN, INT8_MAX);
    }
  }
  virtual void TeareDown()
  {}

  TestRaidFileSystem() : buf_(), buf_len_(OB_DEFAULT_MACRO_BLOCK_SIZE)
  {}
  virtual ~TestRaidFileSystem()
  {}
  int write_block(const uint64_t block_id);
  int read_block(const uint64_t block_id);

  char buf_[OB_DEFAULT_MACRO_BLOCK_SIZE];
  int64_t buf_len_;
};

int TestRaidFileSystem::write_block(const uint64_t block_id)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator;
  ObStoreFileWriteInfo write_info;
  common::ObIOHandle io_handle;
  ObStoreFileSystem*& store_file_system = OB_STORE_FILE.store_file_system_;
  ObStoreFileCtx ctx(allocator);

  write_info.block_id_.block_index_ = (uint32_t)block_id;
  write_info.buf_ = buf_;
  write_info.size_ = buf_len_;
  write_info.io_desc_.category_ = USER_IO;
  write_info.ctx_ = &ctx;

  hex_dump(buf_, 128, false /*no char*/, OB_LOG_LEVEL_INFO);
  if (OB_FAIL(store_file_system->async_write(write_info, io_handle))) {
    LOG_WARN("failed to async write", K(ret), K(write_info));
  } else if (OB_FAIL(io_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
    LOG_WARN("Failed to wait io handle", K(ret));
  } else if (OB_FAIL(store_file_system->fsync())) {
    LOG_WARN("Failed to fsync file", K(ret));
  }

  return ret;
}

int TestRaidFileSystem::read_block(const uint64_t block_id)
{
  int ret = OB_SUCCESS;
  ObStoreFileReadInfo read_info;
  ObMacroBlockCtx macro_block_ctx;
  common::ObIOHandle io_handle;
  ObStoreFileSystem*& store_file_system = OB_STORE_FILE.store_file_system_;

  macro_block_ctx.sstable_block_id_.macro_block_id_.block_index_ = (uint32_t)block_id;
  read_info.macro_block_ctx_ = &macro_block_ctx;
  read_info.offset_ = 0;
  read_info.size_ = buf_len_;
  read_info.io_desc_.category_ = USER_IO;

  if (OB_FAIL(store_file_system->async_read(read_info, io_handle))) {
    LOG_WARN("failed to async read", K(ret), K(read_info));
  } else if (OB_FAIL(io_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
    LOG_WARN("Failed to wait io handle", K(ret));
  }
  if (0 != memcmp(io_handle.get_buffer(), buf_, buf_len_)) {
    hex_dump(buf_, 128, false /*print char*/, OB_LOG_LEVEL_INFO);
    hex_dump(io_handle.get_buffer(), 128, false /*print char*/, OB_LOG_LEVEL_INFO);
    LOG_INFO("dump + 512K");
    hex_dump(buf_ + 512 * 1024, 128, false /*print char*/, OB_LOG_LEVEL_INFO);
    hex_dump(io_handle.get_buffer() + 512 * 1024, 128, false /*print char*/, OB_LOG_LEVEL_INFO);
    ret = OB_ERR_SYS;
  }

  return ret;
}

TEST_F(TestRaidFileSystem, normal)
{
  TestDataFilePrepareUtil util;
  const int64_t macro_block_count = 100;
  const int64_t disk_num = 5;
  int64_t block_id = 3;

  ASSERT_EQ(OB_SUCCESS, util.init(TEST_CASE_NAME, OB_DEFAULT_MACRO_BLOCK_SIZE, macro_block_count, disk_num));
  ASSERT_EQ(OB_SUCCESS, util.open());
  ASSERT_EQ(OB_SUCCESS, write_block(block_id));
  ASSERT_EQ(OB_SUCCESS, read_block(block_id));
  // TODO(): add back later
  /*ASSERT_EQ(OB_SUCCESS, util.restart());
  ASSERT_EQ(OB_SUCCESS, read_block(block_id));
  block_id = 4;
  ASSERT_EQ(OB_SUCCESS, write_block(block_id));
  ASSERT_EQ(OB_SUCCESS, read_block(block_id));*/
}

TEST_F(TestRaidFileSystem, drop_one_disk)
{
  TestDataFilePrepareUtil util;
  const int64_t macro_block_size = 2 * 1024 * 1024;
  const int64_t macro_block_count = 100;
  const int64_t disk_num = 5;
  int64_t block_id = 3;

  ASSERT_EQ(OB_SUCCESS, util.init(TEST_CASE_NAME, macro_block_size, macro_block_count, disk_num));
  ASSERT_EQ(OB_SUCCESS, util.open());
  ObStoreFileSystem*& store_file_system = OB_STORE_FILE.store_file_system_;
  const ObServerSuperBlock& super_block = store_file_system->get_server_super_block();
  // TODO(): fix exit
  //  ASSERT_EQ(OB_SUCCESS, OB_STORE_FILE.drop_disk("data", "1"));
  ASSERT_TRUE(super_block.is_valid());
  LOG_INFO("dump super block", K(super_block));
  ASSERT_EQ(OB_SUCCESS, write_block(block_id));
  ASSERT_EQ(OB_SUCCESS, read_block(block_id));
  //  ASSERT_EQ(OB_SUCCESS, util.restart());
  //  ASSERT_EQ(OB_SUCCESS, read_block(block_id));
  //  block_id = 4;
  //  ASSERT_EQ(OB_SUCCESS, write_block(block_id));
  //  ASSERT_EQ(OB_SUCCESS, read_block(block_id));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_raid_file_system.log");
  OB_LOGGER.set_log_level("DEBUG");
  LOG_INFO("begin unittest: test_raid_file system");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
