/**
 * Copyright (c) 2026 OceanBase
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
#include "observer/table_load/backup/ob_table_load_logical_backup_partition_scanner.h"
#include "observer/table_load/backup/ob_table_load_physical_backup_partition_scanner_v1.h"
#include "observer/table_load/backup/ob_table_load_physical_backup_partition_scanner_v2.h"
#undef protected
#undef private
#include "share/io/ob_io_manager.h"
#include "share/ob_device_manager.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
using namespace share;

template <typename Scanner>
class ObLobLookupScanner : public Scanner
{
public:
  ObLobLookupScanner() : read_block_idx_(-1) {}
  int read_macro_block_data(const int64_t block_idx, const bool is_lob_block,
                           char *&data_buf, int64_t &read_size) override
  {
    EXPECT_TRUE(is_lob_block);
    read_block_idx_ = block_idx;
    // Stop after resolving the real scanner's index, before reading a data block.
    return OB_CANCELED;
  }
  int64_t read_block_idx_;
};

class TestTableLoadBackupPartitionScanner : public testing::Test
{
public:
  static const int64_t LOB_SEQ = 1L << 44;
  static const int64_t TABLE_ID = 1100611139453989;

  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
  }

  static void TearDownTestCase()
  {
    ObIOManager::get_instance().destroy();
  }

  void SetUp() override
  {
    char cwd[OB_MAX_URI_LENGTH];
    ASSERT_NE(nullptr, getcwd(cwd, sizeof(cwd)));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_, sizeof(dir_), "%s/test_backup_lob_XXXXXX", cwd));
    ASSERT_NE(nullptr, mkdtemp(dir_));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri_, sizeof(uri_), "file://%s/", dir_));
    ASSERT_EQ(OB_SUCCESS, storage_info_.set(OB_STORAGE_FILE, ""));
  }

  void TearDown() override
  {
    char path[OB_MAX_URI_LENGTH];
    ASSERT_EQ(OB_SUCCESS, databuff_printf(path, sizeof(path), "%s/macro_block_index_46", dir_));
    unlink(path);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(path, sizeof(path), "%s/part_list", dir_));
    unlink(path);
    EXPECT_EQ(0, rmdir(dir_));
  }

  void write_file(const char *name, const char *buf, const int64_t size)
  {
    char path[OB_MAX_URI_LENGTH];
    ASSERT_EQ(OB_SUCCESS, databuff_printf(path, sizeof(path), "%s/%s", dir_, name));
    FILE *file = fopen(path, "wb");
    ASSERT_NE(nullptr, file);
    const size_t written = fwrite(buf, 1, size, file);
    EXPECT_EQ(0, fclose(file));
    ASSERT_EQ(size, written);
  }

  void write_physical_index(char *buf, const int64_t size)
  {
    ObBackupCommonHeader header;
    header.compressor_type_ = static_cast<uint8_t>(ObCompressorType::NONE_COMPRESSOR);
    header.data_type_ = BACKUP_MACRO_DATA_INDEX;
    header.data_length_ = size - sizeof(header);
    header.data_zlength_ = header.data_length_;
    ASSERT_EQ(OB_SUCCESS, header.set_checksum(buf + sizeof(header), header.data_length_));
    MEMCPY(buf, &header, sizeof(header));
    ASSERT_NO_FATAL_FAILURE(write_file("macro_block_index_46", buf, size));
  }

  template <typename Scanner>
  void check_lookup(Scanner &scanner, const int64_t seq, const uint64_t version, const int64_t expected_idx)
  {
    ObBackupLobIndex index;
    index.logic_macro_id_ = ObBackupLogicMacroBlockId(seq, version);
    char *buf = nullptr;
    int64_t pos = 0;
    scanner.read_block_idx_ = -1;
    EXPECT_EQ(expected_idx < 0 ? OB_HASH_NOT_EXIST : OB_CANCELED, scanner.fill_lob_buf(index, buf, 0, pos));
    EXPECT_EQ(expected_idx, scanner.read_block_idx_);
    EXPECT_EQ(0, pos);
  }

  template <typename Scanner>
  void check_all_lookups(Scanner &scanner)
  {
    ASSERT_EQ(6, scanner.lob_macro_block_index_.count());
    ASSERT_EQ(1, scanner.data_macro_block_index_.count());
    ASSERT_NO_FATAL_FAILURE(check_lookup(scanner, LOB_SEQ, 520, 0));
    ASSERT_NO_FATAL_FAILURE(check_lookup(scanner, LOB_SEQ, 516, 5));
    ASSERT_NO_FATAL_FAILURE(check_lookup(scanner, LOB_SEQ, 518, 2));
    ASSERT_NO_FATAL_FAILURE(check_lookup(scanner, LOB_SEQ, 517, 3));
    ASSERT_NO_FATAL_FAILURE(check_lookup(scanner, LOB_SEQ + 1, 516, 4));
    ASSERT_NO_FATAL_FAILURE(check_lookup(scanner, LOB_SEQ, 519, -1));
    ASSERT_NO_FATAL_FAILURE(check_lookup(scanner, LOB_SEQ + 1, 520, -1));
    ASSERT_NO_FATAL_FAILURE(check_lookup(scanner, LOB_SEQ + 2, 516, -1));
  }

protected:
  const int64_t versions_[7] = {520, 516, 518, 517, 516, 516, 526};
  const int64_t sequences_[7] = {LOB_SEQ, LOB_SEQ, LOB_SEQ, LOB_SEQ, LOB_SEQ + 1, LOB_SEQ, 0};
  char dir_[OB_MAX_URI_LENGTH];
  char uri_[OB_MAX_URI_LENGTH];
  ObBackupStorageInfo storage_info_;
};

TEST_F(TestTableLoadBackupPartitionScanner, physical_v1_lob_versions)
{
  char buf[4096];
  int64_t pos = sizeof(ObBackupCommonHeader);
  ObTableLoadPhysicalBackupPartScannerV1::ObBackupMacroIndex index;
  index.table_id_ = TABLE_ID;
  index.index_table_id_ = TABLE_ID;
  index.backup_set_id_ = 46;
  index.data_length_ = 1;
  for (int64_t i = 0; i < 7; ++i) {
    index.sstable_macro_index_ = i;
    index.data_version_ = versions_[i];
    index.data_seq_ = sequences_[i];
    ASSERT_EQ(OB_SUCCESS, index.serialize(buf, sizeof(buf), pos));
  }
  ASSERT_NO_FATAL_FAILURE(write_physical_index(buf, pos));
  ObLobLookupScanner<ObTableLoadPhysicalBackupPartScannerV1> scanner;
  ObSchemaInfo schema;
  ASSERT_EQ(OB_SUCCESS, scanner.init(ObTableLoadBackupVersion::V_2_X_PHY, storage_info_, schema,
                                    ObString::make_string(uri_), ObString::make_string("46"), TABLE_ID, 1, 0));
  ASSERT_NO_FATAL_FAILURE(check_all_lookups(scanner));
}

TEST_F(TestTableLoadBackupPartitionScanner, physical_v2_lob_versions)
{
  char buf[4096];
  int64_t pos = sizeof(ObBackupCommonHeader);
  ObTableLoadPhysicalBackupPartScannerV2::ObTableKeyInfo table;
  table.table_key_.table_id_ = TABLE_ID;
  table.total_macro_block_count_ = 7;
  ASSERT_EQ(OB_SUCCESS, table.serialize(buf, sizeof(buf), pos));
  ObTableLoadPhysicalBackupPartScannerV2::ObTableMacroIndex index;
  index.backup_set_id_ = 46;
  index.data_length_ = 1;
  for (int64_t i = 0; i < 7; ++i) {
    index.sstable_macro_index_ = i;
    index.data_version_ = versions_[i];
    index.data_seq_ = sequences_[i];
    ASSERT_EQ(OB_SUCCESS, index.serialize(buf, sizeof(buf), pos));
  }
  ASSERT_NO_FATAL_FAILURE(write_physical_index(buf, pos));
  ObLobLookupScanner<ObTableLoadPhysicalBackupPartScannerV2> scanner;
  ObSchemaInfo schema;
  ASSERT_EQ(OB_SUCCESS, scanner.init(ObTableLoadBackupVersion::V_3_X, storage_info_, schema,
                                    ObString::make_string(uri_), ObString::make_string("46"), TABLE_ID, 1, 0));
  ASSERT_NO_FATAL_FAILURE(check_all_lookups(scanner));
}

TEST_F(TestTableLoadBackupPartitionScanner, logical_lob_versions)
{
  char buf[4096];
  int64_t pos = 0;
  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_EQ(OB_SUCCESS, databuff_printf(buf, sizeof(buf), pos, "%ld_%ld\n", versions_[i], sequences_[i]));
  }
  ASSERT_NO_FATAL_FAILURE(write_file("part_list", buf, pos));
  ObLobLookupScanner<ObTableLoadLogicalBackupPartScanner> scanner;
  ObSchemaInfo schema;
  ObArray<int64_t> column_ids;
  ASSERT_EQ(OB_SUCCESS, column_ids.push_back(16));
  ASSERT_EQ(OB_SUCCESS, scanner.init(ObTableLoadBackupVersion::V_2_X_LOG, storage_info_, schema, column_ids,
                                    ObString::make_string(uri_), ObString::make_string(uri_),
                                    ObString::make_string("46"), 1, 0));
  ASSERT_NO_FATAL_FAILURE(check_all_lookups(scanner));
}

TEST_F(TestTableLoadBackupPartitionScanner, logical_invalid_data_version)
{
  const char *invalid_versions[] = {"", "invalid", "516x", "18446744073709551616"};
  const int expected_errors[] = {OB_ERR_UNEXPECTED, OB_ERR_UNEXPECTED, OB_ERR_UNEXPECTED, OB_SIZE_OVERFLOW};
  ObSchemaInfo schema;
  ObArray<int64_t> column_ids;
  ASSERT_EQ(OB_SUCCESS, column_ids.push_back(16));
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_versions); ++i) {
    char buf[128];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(buf, sizeof(buf), pos, "%s_%ld\n", invalid_versions[i], LOB_SEQ));
    ASSERT_NO_FATAL_FAILURE(write_file("part_list", buf, pos));
    ObLobLookupScanner<ObTableLoadLogicalBackupPartScanner> scanner;
    EXPECT_EQ(expected_errors[i], scanner.init(ObTableLoadBackupVersion::V_2_X_LOG, storage_info_, schema, column_ids,
                                              ObString::make_string(uri_), ObString::make_string(uri_),
                                              ObString::make_string("46"), 1, 0));
    EXPECT_EQ(0, scanner.lob_macro_block_index_.count());
  }
}

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_table_load_backup_partition_scanner.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
