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

#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/restore/ob_storage_file.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_backup_io_adapter.h"

#undef private
#undef protected

using namespace oceanbase;
using namespace common;
using namespace share;

namespace oceanbase {
namespace backup {

class ObIDirectoryScanner {
public:
  virtual ~ObIDirectoryScanner() = default;
  virtual int list_directories(const ObString &uri, ObBaseDirEntryOperator &op) = 0;
};

// Mock 扫描器：模拟 read dir 的行为
class MockDirectoryScanner : public ObIDirectoryScanner {
public:
  void set_entries(const ObArray<ObString>& entries) {
    entries_.assign(entries);
  }

  virtual int list_directories(const ObString &uri, ObBaseDirEntryOperator &op) override {
    int ret = OB_SUCCESS;
    UNUSED(uri);

    for (int64_t i = 0; i < entries_.count(); ++i) {
      const ObString& name = entries_[i];
      struct dirent entry;
      memset(&entry, 0, sizeof(entry));

      int len = static_cast<int>(name.length());
      int max_len = static_cast<int>(sizeof(entry.d_name) - 1);
      if (len > max_len) len = max_len;
      strncpy(entry.d_name, name.ptr(), len);
      entry.d_name[len] = '\0';
      entry.d_type = DT_DIR;

      if (OB_FAIL(op.func(&entry))) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
    }
    return ret;
  }

private:
  ObArray<ObString> entries_;
};


class BackupPathTest : public ::testing::Test {
protected:
  MockDirectoryScanner create_configured_scanner(const ObArray<ObString>& entries_to_mock) {
    MockDirectoryScanner scanner;
    scanner.set_entries(entries_to_mock);
    return scanner;
  }

  void SetUp() override {}
  void TearDown() override {}
};

// --- Test Cases for ObDirPrefixLSIDFilter ---
TEST_F(BackupPathTest, Test_ObDirPrefixLSIDFilter_Success) {
  ObArray<ObLSID> ls_ids;
  ObDirPrefixLSIDFilter filter(ls_ids);
  ASSERT_EQ(OB_SUCCESS, filter.init(OB_STR_LS, strlen(OB_STR_LS))); // Initialize filter prefix

  ObArray<ObString> entries;
  entries.push_back(ObString::make_string("logstream_1001"));
  entries.push_back(ObString::make_string("invalid_logstream")); // should be ignored
  entries.push_back(ObString::make_string("logstream_2002"));
  entries.push_back(ObString::make_string("logstream_500"));

  MockDirectoryScanner scanner = create_configured_scanner(entries);

  int ret = scanner.list_directories(ObString::make_string("/fake/path"), filter);
  ASSERT_EQ(ret, OB_SUCCESS); // Expect overall success

  // Verify the extracted logstream IDs
  EXPECT_EQ(ls_ids.count(), 3);  // Only three valid logstream IDs extracted
  EXPECT_EQ(ls_ids.at(0).id(), 1001);
  EXPECT_EQ(ls_ids.at(1).id(), 2002);
  EXPECT_EQ(ls_ids.at(2).id(), 500);
}

TEST_F(BackupPathTest, Test_ObDirPrefixLSIDFilter_InvalidFormat) {
  ObArray<ObLSID> ls_ids;
  ObDirPrefixLSIDFilter filter(ls_ids);
  ASSERT_EQ(OB_SUCCESS, filter.init(OB_STR_LS, strlen(OB_STR_LS)));

  ObArray<ObString> entries;
  entries.push_back(ObString::make_string("logstream_1001"));  // Valid entry
  entries.push_back(ObString::make_string("logstream_abc"));   // Invalid format, should be numbers
  entries.push_back(ObString::make_string("logstream_2002"));  // Valid entry ( not be processed if error aborts early)

  MockDirectoryScanner scanner = create_configured_scanner(entries);

  int ret = scanner.list_directories(ObString::make_string("/fake/path"), filter);
  ASSERT_EQ(ret, OB_ERR_UNEXPECTED); // Expect an unexpected error due to malformed directory name
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_path.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_path.log", true);
  logger.set_log_level("info");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
