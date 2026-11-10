/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <climits>
#include <ctime>
#define private public
#define protected public
#include "sql/audit/ob_audit_logger.h"
#include "lib/file/ob_file.h"
#include "lib/file/file_directory_utils.h"
#include "lib/restore/ob_object_device.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_device_manager.h"
#undef private
#undef protected

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;
using namespace oceanbase::share;

class TestAuditLogArchive : public ::testing::Test
{
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestAuditLogArchive::SetUpTestCase()
{
  const int64_t test_memory = 6 * 1024 * 1024;
  ObTenantBase *tenant_base = new ObTenantBase(OB_SYS_TENANT_ID);
  auto malloc = ObMallocAllocator::get_instance();
  if (NULL == malloc->get_tenant_ctx_allocator(OB_SYS_TENANT_ID, 0)) {
    malloc->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
  }
  tenant_base->init();
  ObTenantEnv::set_tenant(tenant_base);
  ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init(test_memory));
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
  ObTenantIOManager *io_service = NULL;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_base->set(io_service);
  ObTenantEnv::set_tenant(tenant_base);
}

void TestAuditLogArchive::TearDownTestCase()
{
  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
}

TEST_F(TestAuditLogArchive, check_and_get_timestamp)
{
  const struct {
    const char *filename_;
    bool valid_without_suffix_;
    bool valid_with_suffix_;
  } cases[] = {
    {"audit.log.20261027123456001", true, true},
    {"audit.log.20261027123456001.zst", false, true},
    {"audit.log.20261027123456001.enc.1", false, true},
    {"audit.log.20261027123456001.zst.enc.1", false, true},
    {"audit.log", false, false},
    {"other.log.20261027123456001", false, false},
    {"audit.logx20261027123456001", false, false},
    {"audit.log.20261027123456", true, true},
    {"audit.log.20261027123456.zst", false, true},
    {"audit.log.202610271234560010", true, true},
    {"audit.log.20261027", false, false},
    {"audit.log.20261027123456abc", false, false},
    {"audit.log.20261327123456001", false, false},
  };
  struct tm tm_time = {};
  tm_time.tm_year = 2026 - 1900;
  tm_time.tm_mon = 9;
  tm_time.tm_mday = 27;
  tm_time.tm_hour = 12;
  tm_time.tm_min = 34;
  tm_time.tm_sec = 56;
  const int64_t expected_timestamp = mktime(&tm_time) * 1000000;
  for (const auto &test : cases) {
    struct dirent entry = {};
    strcpy(entry.d_name, test.filename_);
    entry.d_type = DT_REG;
    for (const bool ignore_suffix : {false, true}) {
      SCOPED_TRACE(::testing::Message() << test.filename_ << ", ignore_suffix=" << ignore_suffix);
      bool is_valid = true;
      int64_t timestamp = -1;
      EXPECT_EQ(OB_SUCCESS, ObAuditLogCollectOp::check_and_get_timestamp(
          entry, ignore_suffix, is_valid, timestamp));
      const bool expected_valid = ignore_suffix ? test.valid_with_suffix_ : test.valid_without_suffix_;
      EXPECT_EQ(expected_valid, is_valid);
      EXPECT_EQ(expected_valid ? expected_timestamp : 0, timestamp);
    }
  }
}

TEST_F(TestAuditLogArchive, collect_audit_log_files)
{
  const struct {
    const char *filename_;
    unsigned char type_;
    int64_t size_;
  } cases[] = {
    {"audit.log.20261027123456001", DT_REG, 10},
    {"audit.log.20261027123456002", DT_UNKNOWN, 20},
    {"audit.log.20261027123456003.zst", DT_REG, 30},
    {"audit.log.20261027123456004", DT_DIR, 40},
    {"audit.log.20261027123456005", DT_LNK, 50},
    {"audit.log", DT_REG, 60},
  };
  ObAuditLogCollectOp raw_op(false, false);
  ObAuditLogCollectOp archive_op(true, true);
  struct dirent entry = {};
  for (const auto &test : cases) {
    strcpy(entry.d_name, test.filename_);
    entry.d_type = test.type_;
    ASSERT_EQ(OB_SUCCESS, raw_op.func(&entry));
    archive_op.set_size(test.size_);
    ASSERT_EQ(OB_SUCCESS, archive_op.func(&entry));
  }
  ASSERT_EQ(2, raw_op.get_files().count());
  ASSERT_EQ(3, archive_op.get_files().count());
  for (int64_t i = 0; i < archive_op.get_files().count(); ++i) {
    EXPECT_STREQ(cases[i].filename_, archive_op.get_files().at(i).filename_.ptr());
    EXPECT_EQ(cases[i].size_, archive_op.get_files().at(i).size_);
    if (i < raw_op.get_files().count()) {
      EXPECT_STREQ(cases[i].filename_, raw_op.get_files().at(i).filename_.ptr());
      EXPECT_EQ(archive_op.get_files().at(i).timestamp_, raw_op.get_files().at(i).timestamp_);
    }
  }
}

// Isolate filesystem failure tests from both existing test data and observer audit directories.
class AuditArchiveFiles
{
public:
  AuditArchiveFiles() { strcpy(root_, "audit_retry_test.XXXXXX"); }
  ~AuditArchiveFiles()
  {
    logger_.log_file_.close_all();
    FileDirectoryUtils::delete_directory_rec(root_);
  }

  int init()
  {
    int ret = OB_SUCCESS;
    if (nullptr == ::mkdtemp(root_)) {
      ret = OB_IO_ERROR;
    } else if (OB_FAIL(FileDirectoryUtils::create_full_path(directory().ptr()))) {
    } else if (OB_FAIL(logger_.log_file_.set_root_path(root_))) {
    } else if (OB_FAIL(databuff_printf(logger_.log_file_.dir_path_, sizeof(logger_.log_file_.dir_path_), "1004/node"))) {
    } else if (OB_FAIL(logger_.log_file_.open(ObString::make_string(path("audit.log").ptr())))) {
    } else if (OB_FAIL(set_archive_path("/archive"))) {
    } else {
      logger_.log_file_.type_ = OB_STORAGE_OSS;
      logger_.log_cfg_.audit_log_compression_ = ObCompressorType::ZSTD_COMPRESSOR;
    }
    return ret;
  }

  int set_archive_path(const char *subdir = "")
  {
    ObAuditLogPath &dest = logger_.log_cfg_.audit_log_path_;
    return databuff_printf(dest.ptr(), dest.capacity(), "file://%s%s", root_, subdir);
  }

  ObAuditLogPath directory() const
  {
    ObAuditLogPath result;
    EXPECT_EQ(OB_SUCCESS, databuff_printf(result.ptr(), result.capacity(), "%s/1004/node", root_));
    return result;
  }
  ObAuditLogPath log_path(const int64_t index, const char *suffix = "") const
  {
    return path(name(index, suffix).ptr());
  }
  ObAuditLogPath path(const char *filename) const
  {
    ObAuditLogPath result;
    EXPECT_EQ(OB_SUCCESS, databuff_printf(result.ptr(), result.capacity(), "%s/1004/node/%s", root_, filename));
    return result;
  }
  static ObAuditLogPath name(const int64_t index, const char *suffix = "")
  {
    ObAuditLogPath result;
    EXPECT_EQ(OB_SUCCESS, databuff_printf(result.ptr(), result.capacity(),
        "audit.log.20261027000000%03ld%s", index, suffix));
    return result;
  }
  bool exists(const int64_t index) const { return 0 == ::access(log_path(index).ptr(), F_OK); }
  ObAuditLogPath archive(const int64_t index) const
  {
    ObAuditLogPath result;
    EXPECT_EQ(OB_SUCCESS, databuff_printf(result.ptr(), result.capacity(),
        "%s/archive/1004/node/%s", root_, name(index, ".zst").ptr()));
    return result;
  }
  int create(const char *name)
  {
    int ret = OB_SUCCESS;
    const int fd = ::open(path(name).ptr(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) {
      ret = OB_IO_ERROR;
    } else {
      if (static_cast<ssize_t>(strlen(name)) != ::write(fd, name, strlen(name))) {
        ret = OB_IO_ERROR;
      }
      if (0 != ::close(fd)) {
        ret = OB_IO_ERROR;
      }
    }
    return ret;
  }
  int create(const int64_t index) { return create(name(index).ptr()); }

  void write_log()
  {
    alignas(ObAuditLogItem) char buf[sizeof(ObAuditLogItem) + 5];
    ObAuditLogItem *item = new (buf) ObAuditLogItem();
    item->set_buf_size(5);
    MEMCPY(item->get_buf(), "audit", 5);
    item->set_data_len(5);
    logger_.flush_logs_to_file(&item, 1);
    item->~ObAuditLogItem();
  }

  char root_[64];
  ObAuditLogger logger_;
};

TEST_F(TestAuditLogArchive, local_archive_limit_and_filename_filter)
{
  AuditArchiveFiles files;
  ASSERT_EQ(OB_SUCCESS, files.init());
  const char *ignored[] = {"other.log", "audit.log.20261027", "audit.log.20261027000000001.zst",
                           "audit.log.20261027000000001.enc.1", "audit.log.20261327000000001",
                           "audit.log.20261027000000abc", "audit.logx20261027000000001"};
  for (const char *name : ignored) {
    ASSERT_EQ(OB_SUCCESS, files.create(name));
  }
  ASSERT_EQ(0, ::mkdir(files.path("audit.log.20261026000000000").ptr(), 0700));
  ASSERT_EQ(0, ::symlink("audit.log", files.path("audit.log.20261026000000001").ptr()));
  for (int64_t i = 1; i <= 9; ++i) {
    ASSERT_EQ(OB_SUCCESS, files.create(i));
  }
  ASSERT_EQ(OB_SUCCESS, files.logger_.prune_local_log_files());
  for (int64_t i = 1; i <= 9; ++i) {
    EXPECT_TRUE(files.exists(i));
  }
  ASSERT_EQ(OB_SUCCESS, files.create(10));
  ASSERT_EQ(OB_SUCCESS, files.logger_.prune_local_log_files());
  EXPECT_FALSE(files.exists(1));
  for (int64_t i = 11; i <= 35; ++i) {
    ASSERT_EQ(OB_SUCCESS, files.create(i));
  }
  ASSERT_EQ(OB_SUCCESS, files.logger_.prune_local_log_files());
  for (int64_t i = 2; i <= 35; ++i) {
    EXPECT_EQ(i >= 27, files.exists(i)) << i;
  }
  EXPECT_EQ(0, ::access(files.path("audit.log").ptr(), F_OK));
  for (const char *name : ignored) {
    EXPECT_EQ(0, ::access(files.path(name).ptr(), F_OK)) << name;
  }
  EXPECT_EQ(0, ::access(files.path("audit.log.20261026000000000").ptr(), F_OK));
  EXPECT_EQ(0, ::access(files.path("audit.log.20261026000000001").ptr(), F_OK));
}

TEST_F(TestAuditLogArchive, rotation_retries_after_reopen_failure)
{
  AuditArchiveFiles files;
  ASSERT_EQ(OB_SUCCESS, files.init());
  ASSERT_EQ(5, ::write(files.logger_.log_file_.fd_, "audit", 5));
  files.logger_.log_file_.file_size_ = 5;
  ASSERT_EQ(0, ::rename(files.path("audit.log").ptr(), files.log_path(0).ptr()));
  files.logger_.log_cfg_.audit_log_rotate_on_size_ = 5;
  for (int64_t i = 1; i <= 12; ++i) {
    ASSERT_EQ(OB_SUCCESS, files.create(i));
  }
  files.logger_.log_cfg_.audit_log_path_ = "invalid://audit";
  EXPECT_NE(OB_SUCCESS, files.logger_.rotate_and_prune_log(5));
  EXPECT_EQ(5, files.logger_.log_file_.file_size_);
  for (int64_t i = 0; i <= 12; ++i) {
    EXPECT_TRUE(files.exists(i)) << i;
    EXPECT_NE(0, ::access(files.archive(i).ptr(), F_OK)) << i;
  }

  ASSERT_EQ(OB_SUCCESS, files.set_archive_path());
  ASSERT_EQ(OB_SUCCESS, files.logger_.rotate_and_prune_log(5));
  EXPECT_EQ(0, files.logger_.log_file_.file_size_);
  EXPECT_EQ(0, ::access(files.path("audit.log").ptr(), F_OK));
  for (int64_t i = 0; i <= 12; ++i) {
    EXPECT_FALSE(files.exists(i)) << i;
    EXPECT_EQ(0, ::access(files.log_path(i, ".zst").ptr(), F_OK)) << i;
  }
}

TEST_F(TestAuditLogArchive, local_archive_rotation_uploads_all_with_latest_config)
{
  AuditArchiveFiles files;
  ASSERT_EQ(OB_SUCCESS, files.init());
  files.logger_.log_cfg_.audit_log_compression_ = ObCompressorType::NONE_COMPRESSOR;
  for (int64_t i = 1; i <= 12; ++i) {
    ASSERT_EQ(OB_SUCCESS, files.create(i));
  }
  files.logger_.log_cfg_.audit_log_path_ = "invalid://audit";
  files.logger_.log_cfg_.audit_log_rotate_on_size_ = 10;
  files.write_log();
  EXPECT_EQ(5, files.logger_.log_file_.file_size_);
  for (int64_t i = 1; i <= 12; ++i) {
    EXPECT_TRUE(files.exists(i)) << i;
  }
  ASSERT_EQ(OB_SUCCESS, files.set_archive_path());
  files.logger_.log_cfg_.audit_log_compression_ = ObCompressorType::ZSTD_COMPRESSOR;
  files.write_log();
  EXPECT_EQ(0, files.logger_.log_file_.file_size_);
  for (int64_t i = 1; i <= 12; ++i) {
    EXPECT_FALSE(files.exists(i)) << i;
    EXPECT_EQ(0, ::access(files.log_path(i, ".zst").ptr(), F_OK)) << i;
  }
  ObArenaAllocator allocator;
  ObSEArray<ObString, 10> logs;
  ASSERT_EQ(OB_SUCCESS, files.logger_.collect_local_log_files(allocator, logs));
  EXPECT_EQ(0, logs.count());
  EXPECT_EQ(0, ::access(files.path("audit.log").ptr(), F_OK));
}

TEST_F(TestAuditLogArchive, local_archive_upload_failure_still_prunes)
{
  AuditArchiveFiles files;
  ASSERT_EQ(OB_SUCCESS, files.init());
  for (int64_t i = 1; i <= 12; ++i) {
    ASSERT_EQ(OB_SUCCESS, files.create(i));
  }
  files.logger_.log_cfg_.audit_log_path_ = "invalid://audit";
  EXPECT_NE(OB_SUCCESS, files.logger_.archive_local_log_files());
  for (int64_t i = 1; i <= 12; ++i) {
    EXPECT_EQ(i >= 4, files.exists(i)) << i;
  }
}

TEST_F(TestAuditLogArchive, local_archive_leaves_file_storage_retention_unchanged)
{
  AuditArchiveFiles files;
  ASSERT_EQ(OB_SUCCESS, files.init());
  for (int64_t i = 1; i <= 12; ++i) {
    ASSERT_EQ(OB_SUCCESS, files.create(i));
  }
  files.logger_.log_file_.type_ = OB_STORAGE_FILE;
  ASSERT_EQ(OB_SUCCESS, files.set_archive_path());
  files.logger_.log_cfg_.audit_log_rotate_on_size_ = 5;
  files.write_log();
  EXPECT_EQ(0, files.logger_.log_file_.file_size_);
  for (int64_t i = 1; i <= 12; ++i) {
    EXPECT_TRUE(files.exists(i));
  }
}

TEST_F(TestAuditLogArchive, rotation_size_is_independent_of_archive_failure)
{
  AuditArchiveFiles files;
  ASSERT_EQ(OB_SUCCESS, files.init());
  ASSERT_EQ(OB_SUCCESS, files.set_archive_path());
  ASSERT_EQ(OB_SUCCESS, files.create(1));
  // A directory at the archive path forces upload failure after a successful reopen.
  ASSERT_EQ(0, ::mkdir(files.log_path(1, ".zst").ptr(), 0700));
  struct stat source_st = {};
  ASSERT_EQ(0, ::stat(files.log_path(1).ptr(), &source_st));
  files.logger_.log_cfg_.audit_log_rotate_on_size_ = 5;
  ASSERT_EQ(5, ::write(files.logger_.log_file_.fd_, "audit", 5));
  files.logger_.log_file_.file_size_ = 5;
  EXPECT_NE(OB_SUCCESS, files.logger_.rotate_and_prune_log(5));
  EXPECT_EQ(0, files.logger_.log_file_.file_size_);
  struct stat st = {};
  ASSERT_EQ(0, ::fstat(files.logger_.log_file_.fd_, &st));
  EXPECT_EQ(0, st.st_size);
  ObArenaAllocator allocator;
  ObSEArray<ObString, 10> logs;
  ASSERT_EQ(OB_SUCCESS, files.logger_.collect_local_log_files(allocator, logs));
  ASSERT_EQ(1, logs.count());
  EXPECT_STREQ(files.log_path(1).ptr(), logs.at(0).ptr());
  ASSERT_EQ(0, ::stat(logs.at(0).ptr(), &st));
  EXPECT_EQ(source_st.st_size, st.st_size);
}

TEST_F(TestAuditLogArchive, rotation_reopen_failure_skips_archive_and_prune)
{
  AuditArchiveFiles files;
  ASSERT_EQ(OB_SUCCESS, files.init());
  for (int64_t i = 1; i <= 12; ++i) {
    ASSERT_EQ(OB_SUCCESS, files.create(i));
  }
  char invalid_path[sizeof("file://") + 300];
  MEMSET(invalid_path, 'x', sizeof(invalid_path) - 1);
  MEMCPY(invalid_path, "file://", sizeof("file://") - 1);
  invalid_path[sizeof(invalid_path) - 1] = '\0';
  files.logger_.log_cfg_.audit_log_path_ = invalid_path;
  files.logger_.log_cfg_.audit_log_rotate_on_size_ = 5;
  ASSERT_EQ(5, ::write(files.logger_.log_file_.fd_, "audit", 5));
  files.logger_.log_file_.file_size_ = 5;
  EXPECT_NE(OB_SUCCESS, files.logger_.rotate_and_prune_log(5));
  EXPECT_EQ(5, files.logger_.log_file_.file_size_);
  for (int64_t i = 1; i <= 12; ++i) {
    EXPECT_TRUE(files.exists(i)) << i;
    EXPECT_NE(0, ::access(files.archive(i).ptr(), F_OK)) << i;
  }
  struct stat st = {};
  ASSERT_EQ(0, ::fstat(files.logger_.log_file_.fd_, &st));
  EXPECT_EQ(5, st.st_size);
  EXPECT_EQ(1, st.st_nlink);
}

TEST_F(TestAuditLogArchive, reopen_descriptor_failure_keeps_file_size)
{
  AuditArchiveFiles files;
  ASSERT_EQ(OB_SUCCESS, files.init());
  ASSERT_EQ(5, ::write(files.logger_.log_file_.fd_, "audit", 5));
  files.logger_.log_file_.file_size_ = 5;
  ASSERT_EQ(0, ::rename(files.path("audit.log").ptr(), files.log_path(0).ptr()));
  const int active_fd = files.logger_.log_file_.fd_;
  files.logger_.log_file_.fd_ = INT_MAX;
  const int ret = files.logger_.log_file_.reopen();
  files.logger_.log_file_.fd_ = active_fd;
  EXPECT_NE(OB_SUCCESS, ret);
  EXPECT_EQ(5, files.logger_.log_file_.file_size_);
  struct stat st = {};
  ASSERT_EQ(0, ::fstat(active_fd, &st));
  EXPECT_EQ(5, st.st_size);
  EXPECT_TRUE(files.exists(0));
  EXPECT_EQ(0, ::access(files.path("audit.log").ptr(), F_OK));
}

TEST_F(TestAuditLogArchive, archive_writer_type_for_storage_backend)
{
  const struct {
    const char *path_;
    bool is_multipart_;
  } cases[] = {
    {"file:///audit", false},
    {"hdfs://127.0.0.1:9000/audit", false},
    {"oss://audit-test/logs?host=localhost&access_id=test&access_key=test", true},
    {"s3://audit-test/logs?host=localhost&access_id=test&access_key=test&s3_region=test", true},
  };
  // Stop at filename formatting after writer selection, before any storage I/O.
  char time_suffix[ObAuditLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
  MEMSET(time_suffix, '1', sizeof(time_suffix) - 1);
  time_suffix[sizeof(time_suffix) - 1] = '\0';
  for (const auto &test : cases) {
    SCOPED_TRACE(test.path_);
    ObAuditLogger logger;
    ASSERT_EQ(OB_SUCCESS, logger.log_cfg_.audit_log_path_.assign(test.path_));
    ObIODevice *device = nullptr;
    ObIOFd fd;
    bool is_multipart = !test.is_multipart_;
    EXPECT_EQ(OB_SIZE_OVERFLOW, logger.get_archive_write_device(time_suffix,
        ObCompressorType::NONE_COMPRESSOR, device, fd, is_multipart));
    EXPECT_EQ(test.is_multipart_, is_multipart);
    EXPECT_EQ(nullptr, device);
    EXPECT_FALSE(fd.is_valid());
  }
}

class MockAuditArchiveDevice : public ObObjectDevice
{
public:
  MOCK_METHOD1(complete, int(const ObIOFd &fd));
  MOCK_METHOD1(abort, int(const ObIOFd &fd));
  MOCK_METHOD1(close, int(const ObIOFd &fd));
};

TEST_F(TestAuditLogArchive, multipart_complete_precedes_close)
{
  testing::StrictMock<MockAuditArchiveDevice> device;
  const ObIOFd fd(&device, 0, 0);
  testing::InSequence sequence;
  EXPECT_CALL(device, complete(testing::_)).WillOnce(testing::Return(OB_SUCCESS));
  EXPECT_CALL(device, close(testing::_)).WillOnce(testing::Return(OB_SUCCESS));
  EXPECT_EQ(OB_SUCCESS, ObAuditLogger::finish_archive_write(device, fd, true, OB_SUCCESS));
}

TEST_F(TestAuditLogArchive, multipart_write_failure_preserves_original_error)
{
  testing::StrictMock<MockAuditArchiveDevice> device;
  const ObIOFd fd(&device, 0, 0);
  testing::InSequence sequence;
  EXPECT_CALL(device, abort(testing::_)).WillOnce(testing::Return(OB_IO_ERROR));
  EXPECT_CALL(device, close(testing::_)).WillOnce(testing::Return(OB_ERR_UNEXPECTED));
  EXPECT_EQ(OB_TIMEOUT, ObAuditLogger::finish_archive_write(device, fd, true, OB_TIMEOUT));
}

TEST_F(TestAuditLogArchive, multipart_complete_failure_aborts_before_close)
{
  testing::StrictMock<MockAuditArchiveDevice> device;
  const ObIOFd fd(&device, 0, 0);
  testing::InSequence sequence;
  EXPECT_CALL(device, complete(testing::_)).WillOnce(testing::Return(OB_TIMEOUT));
  EXPECT_CALL(device, abort(testing::_)).WillOnce(testing::Return(OB_IO_ERROR));
  EXPECT_CALL(device, close(testing::_)).WillOnce(testing::Return(OB_ERR_UNEXPECTED));
  EXPECT_EQ(OB_TIMEOUT, ObAuditLogger::finish_archive_write(device, fd, true, OB_SUCCESS));
}

TEST_F(TestAuditLogArchive, multipart_close_failure_is_not_success)
{
  testing::StrictMock<MockAuditArchiveDevice> device;
  const ObIOFd fd(&device, 0, 0);
  testing::InSequence sequence;
  EXPECT_CALL(device, complete(testing::_)).WillOnce(testing::Return(OB_SUCCESS));
  EXPECT_CALL(device, close(testing::_)).WillOnce(testing::Return(OB_IO_ERROR));
  EXPECT_EQ(OB_IO_ERROR, ObAuditLogger::finish_archive_write(device, fd, true, OB_SUCCESS));
}

TEST_F(TestAuditLogArchive, multipart_open_failure_does_not_use_invalid_fd)
{
  testing::StrictMock<MockAuditArchiveDevice> device;
  const ObIOFd fd;
  EXPECT_EQ(OB_IO_ERROR, ObAuditLogger::finish_archive_write(device, fd, true, OB_IO_ERROR));
}

TEST_F(TestAuditLogArchive, local_archive_writer_does_not_complete_or_abort)
{
  testing::StrictMock<MockAuditArchiveDevice> device;
  const ObIOFd fd(&device, 0, 0);
  EXPECT_CALL(device, close(testing::_)).WillOnce(testing::Return(OB_SUCCESS));
  EXPECT_EQ(OB_SUCCESS, ObAuditLogger::finish_archive_write(device, fd, false, OB_SUCCESS));
}

} // end namespace sql
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_audit_log_archive.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
