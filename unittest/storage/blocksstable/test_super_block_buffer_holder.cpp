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
#include "storage/blocksstable/ob_super_block_buffer_holder.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace unittest {
class TestSuperBlockBufferHolder : public ::testing::Test {
public:
  TestSuperBlockBufferHolder(){};
  virtual ~TestSuperBlockBufferHolder(){};
  virtual void SetUp(){};
  virtual void TearDown(){};

  static void format_super_block(ObSuperBlockV1& super_block);

private:
  // disallow copy
  TestSuperBlockBufferHolder(const TestSuperBlockBufferHolder& other);
  TestSuperBlockBufferHolder& operator=(const TestSuperBlockBufferHolder& other);
};

void TestSuperBlockBufferHolder::format_super_block(ObSuperBlockV1& super_block)
{
  const int64_t data_file_size = 256 << 20;
  const int64_t macro_block_size = 2 << 20;

  MEMSET(&super_block, 0, sizeof(ObSuperBlockV1));
  super_block.header_.super_block_size_ = static_cast<int32_t>(super_block.get_serialize_size());
  super_block.header_.version_ = ObMacroBlockCommonHeader::MACRO_BLOCK_COMMON_HEADER_VERSION;
  super_block.header_.magic_ = SUPER_BLOCK_MAGIC;
  super_block.header_.attr_ = 0;

  // first two blocks are superblock.
  super_block.backup_meta_count_ = 2;
  super_block.backup_meta_blocks_[0] = 0;
  super_block.backup_meta_blocks_[1] = 1;

  super_block.create_timestamp_ = ObTimeUtility::current_time();
  super_block.modify_timestamp_ = super_block.create_timestamp_;
  super_block.macro_block_size_ = macro_block_size;
  super_block.total_macro_block_count_ = data_file_size / macro_block_size;
  super_block.reserved_block_count_ = super_block.backup_meta_count_;
  super_block.free_macro_block_count_ = 0;
  super_block.first_macro_block_ = super_block.backup_meta_blocks_[1] + 1;
  super_block.first_free_block_index_ = super_block.first_macro_block_;
  super_block.total_file_size_ = lower_align(data_file_size, macro_block_size);

  super_block.macro_block_meta_entry_block_index_ = -1;
  super_block.partition_meta_entry_block_index_ = -1;
  super_block.table_mgr_meta_entry_block_index_ = -1;
  super_block.partition_meta_log_seq_ = 0;
  super_block.table_mgr_meta_log_seq_ = 0;
  super_block.replay_start_point_.file_id_ = 1;
  super_block.replay_start_point_.log_id_ = 0;
  super_block.replay_start_point_.offset_ = 0;
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
