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
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/backup/ob_ss_backup_utils.h"
#endif

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace backup {

#ifdef OB_BUILD_SHARED_STORAGE

class TestSSBackupCommonHeader : public ::testing::Test {
public:
  TestSSBackupCommonHeader() {}
  virtual ~TestSSBackupCommonHeader() {}
  virtual void SetUp() override {}
  virtual void TearDown() override {}
};

// Test basic wrap then unwrap roundtrip preserves data
TEST_F(TestSSBackupCommonHeader, test_wrap_unwrap_roundtrip)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  MacroBlockId block_id;

  // Prepare original data
  const int64_t data_len = 4096;
  char *original_buf = static_cast<char *>(allocator.alloc(data_len));
  ASSERT_NE(nullptr, original_buf);
  for (int64_t i = 0; i < data_len; ++i) {
    original_buf[i] = static_cast<char>(i % 256);
  }

  // Save a copy for later comparison
  char *expected_buf = static_cast<char *>(allocator.alloc(data_len));
  ASSERT_NE(nullptr, expected_buf);
  MEMCPY(expected_buf, original_buf, data_len);

  ObBufferReader macro_data(original_buf, data_len, data_len);
  ObBufferReader wrapped_data;

  // Wrap
  ret = ObSSBackupUtils::wrap_with_backup_common_header(block_id, macro_data, allocator, wrapped_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Wrapped data should be larger by sizeof(ObBackupCommonHeader)
  const int64_t header_size = sizeof(ObBackupCommonHeader);
  ASSERT_EQ(data_len + header_size, wrapped_data.length());

  // Verify header fields
  const ObBackupCommonHeader *header =
      reinterpret_cast<const ObBackupCommonHeader *>(wrapped_data.data());
  ASSERT_EQ(static_cast<int16_t>(ObBackupCommonHeader::COMMON_HEADER_VERSION), header->header_version_);
  ASSERT_EQ(static_cast<uint16_t>(ObCompressorType::NONE_COMPRESSOR), header->compressor_type_);
  ASSERT_EQ(static_cast<uint16_t>(BACKUP_MACRO_DATA), header->data_type_);
  ASSERT_EQ(data_len, header->data_length_);
  ASSERT_EQ(data_len, header->data_zlength_);
  ASSERT_EQ(0, header->align_length_);

  // Header checksum should be valid
  ret = header->check_header_checksum();
  ASSERT_EQ(OB_SUCCESS, ret);

  // Data checksum should be valid
  ret = header->check_data_checksum(wrapped_data.data() + header_size, header->data_zlength_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Unwrap
  ret = ObSSBackupUtils::unwrap_backup_common_header(block_id, wrapped_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  // After unwrap, data should match the original
  ASSERT_EQ(data_len, wrapped_data.length());
  ASSERT_EQ(0, MEMCMP(expected_buf, wrapped_data.data(), data_len));
}

// Test wrap with various data sizes
TEST_F(TestSSBackupCommonHeader, test_wrap_unwrap_various_sizes)
{
  int ret = OB_SUCCESS;
  MacroBlockId block_id;

  const int64_t sizes[] = {1, 100, 1024, 8192, 2 * 1024 * 1024};
  for (int64_t s = 0; s < ARRAYSIZEOF(sizes); ++s) {
    ObArenaAllocator allocator;
    const int64_t data_len = sizes[s];
    char *buf = static_cast<char *>(allocator.alloc(data_len));
    ASSERT_NE(nullptr, buf);
    MEMSET(buf, static_cast<char>(s + 1), data_len);

    char *expected = static_cast<char *>(allocator.alloc(data_len));
    ASSERT_NE(nullptr, expected);
    MEMCPY(expected, buf, data_len);

    ObBufferReader macro_data(buf, data_len, data_len);
    ObBufferReader wrapped_data;

    ret = ObSSBackupUtils::wrap_with_backup_common_header(block_id, macro_data, allocator, wrapped_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(data_len + static_cast<int64_t>(sizeof(ObBackupCommonHeader)), wrapped_data.length());

    ret = ObSSBackupUtils::unwrap_backup_common_header(block_id, wrapped_data);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(data_len, wrapped_data.length());
    ASSERT_EQ(0, MEMCMP(expected, wrapped_data.data(), data_len));
  }
}

// Test wrap with invalid input
TEST_F(TestSSBackupCommonHeader, test_wrap_invalid_input)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  MacroBlockId block_id;
  ObBufferReader wrapped_data;

  // Null data
  ObBufferReader null_data(nullptr, 0, 0);
  ret = ObSSBackupUtils::wrap_with_backup_common_header(block_id, null_data, allocator, wrapped_data);
  ASSERT_NE(OB_SUCCESS, ret);

  // Zero length data
  char buf[1] = {'A'};
  ObBufferReader zero_data(buf, 0, 0);
  ret = ObSSBackupUtils::wrap_with_backup_common_header(block_id, zero_data, allocator, wrapped_data);
  ASSERT_NE(OB_SUCCESS, ret);
}

// Test unwrap with invalid input
TEST_F(TestSSBackupCommonHeader, test_unwrap_invalid_input)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  MacroBlockId block_id;

  // Null data
  ObBufferReader null_data(nullptr, 0, 0);
  ret = ObSSBackupUtils::unwrap_backup_common_header(block_id, null_data);
  ASSERT_NE(OB_SUCCESS, ret);

  // Data too small to contain header
  char small_buf[4] = {0};
  ObBufferReader small_data(small_buf, 4, 4);
  ret = ObSSBackupUtils::unwrap_backup_common_header(block_id, small_data);
  ASSERT_NE(OB_SUCCESS, ret);
}

// Test unwrap with corrupted header checksum
TEST_F(TestSSBackupCommonHeader, test_unwrap_corrupted_header)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  MacroBlockId block_id;
  const int64_t data_len = 1024;

  char *buf = static_cast<char *>(allocator.alloc(data_len));
  ASSERT_NE(nullptr, buf);
  MEMSET(buf, 'X', data_len);

  ObBufferReader macro_data(buf, data_len, data_len);
  ObBufferReader wrapped_data;

  ret = ObSSBackupUtils::wrap_with_backup_common_header(block_id, macro_data, allocator, wrapped_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Corrupt the header checksum
  char *writable = const_cast<char *>(wrapped_data.data());
  ObBackupCommonHeader *header = reinterpret_cast<ObBackupCommonHeader *>(writable);
  header->header_checksum_ = ~header->header_checksum_;

  ret = ObSSBackupUtils::unwrap_backup_common_header(block_id, wrapped_data);
  ASSERT_EQ(OB_CHECKSUM_ERROR, ret);
}

// Test unwrap with corrupted data checksum
TEST_F(TestSSBackupCommonHeader, test_unwrap_corrupted_data)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  MacroBlockId block_id;
  const int64_t data_len = 1024;

  char *buf = static_cast<char *>(allocator.alloc(data_len));
  ASSERT_NE(nullptr, buf);
  MEMSET(buf, 'Y', data_len);

  ObBufferReader macro_data(buf, data_len, data_len);
  ObBufferReader wrapped_data;

  ret = ObSSBackupUtils::wrap_with_backup_common_header(block_id, macro_data, allocator, wrapped_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Corrupt data portion (after header)
  const int64_t header_size = sizeof(ObBackupCommonHeader);
  char *writable = const_cast<char *>(wrapped_data.data());
  writable[header_size] = ~writable[header_size];

  ret = ObSSBackupUtils::unwrap_backup_common_header(block_id, wrapped_data);
  ASSERT_EQ(OB_CHECKSUM_ERROR, ret);
}

// Test need_backup_common_header stub returns false for default constructed block_id
TEST_F(TestSSBackupCommonHeader, test_need_backup_common_header_stub)
{
  MacroBlockId block_id;
  ASSERT_FALSE(ObSSBackupUtils::need_backup_common_header(block_id));
}

// Test need_backup_common_header returns true for shared tablet sub meta in table
TEST_F(TestSSBackupCommonHeader, test_need_backup_common_header_true)
{
  using namespace blocksstable;
  MacroBlockId block_id;

  // Construct a shared tablet sub meta in table block id
  block_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  block_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META_IN_TABLE));
  block_id.set_second_id(1001);  // tablet_id
  block_id.set_third_id(1);      // data_seq
  block_id.set_fourth_id(12345); // meta_ls_id

  ASSERT_TRUE(ObSSBackupUtils::need_backup_common_header(block_id));
}

#endif // OB_BUILD_SHARED_STORAGE

} // namespace backup
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_backup_common_header.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ss_backup_common_header.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
