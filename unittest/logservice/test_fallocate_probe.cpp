/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PALF

#include <gtest/gtest.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>

#include "lib/file/file_directory_utils.h"
#include "lib/ob_define.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_io_utils.h"
#include "logservice/ob_server_log_block_mgr.h"
#include "share/config/ob_server_config.h"
#ifdef OB_BUILD_ARBITRATION
#include "logservice/arbserver/palf_lite_define.h"
#endif

// The bundled googletest predates GTEST_SKIP() (googletest >= 1.10): degrade
// the environment guards to an early return. On an unsupported environment
// the test reports PASSED and the printed marker tells which real-filesystem
// coverage did not run.
#define SKIP_REAL_PROBE_TEST(why) \
  do { \
    fprintf(stderr, "[  SKIPPED  ] %s\n", why); \
    return; \
  } while (0)

namespace oceanbase
{
namespace palf
{

namespace
{

struct FallocateCall
{
  FallocateCall(const int fd, const int mode, const int64_t offset, const int64_t len)
    : fd_(fd), mode_(mode), offset_(offset), len_(len)
  {}

  int fd_;
  int mode_;
  int64_t offset_;
  int64_t len_;
};

struct InjectedFailure
{
  InjectedFailure() : call_(0), sys_errno_(0), result_(OB_SUCCESS), all_calls_(false), fold_eisdir_(false) {}

  // map_not_supported mirrors the production errno classification: the open
  // and fallocate ops fold EOPNOTSUPP/ENOTSUP/ENOSYS/EINVAL into
  // OB_NOT_SUPPORTED, the open op additionally folds EISDIR (kernels < 3.11
  // reject O_TMPFILE with EISDIR), while close and unlink only run
  // convert_sys_errno() (e.g. EINVAL stays OB_IO_ERROR there).
  void set(const int call, const int sys_errno, const int result = OB_SUCCESS,
           const bool map_not_supported = true)
  {
    call_ = call;
    sys_errno_ = sys_errno;
    all_calls_ = false;
    init_result_(sys_errno, result, map_not_supported);
  }

  // Fails every call, e.g. an environment property such as "O_TMPFILE unsupported".
  void set_persistent(const int sys_errno, const bool map_not_supported = true)
  {
    sys_errno_ = sys_errno;
    all_calls_ = true;
    init_result_(sys_errno, OB_SUCCESS, map_not_supported);
  }

  int get_result(const int call, const int success_result = OB_SUCCESS) const
  {
    int result = success_result;
    if (all_calls_ || call_ == call) {
      errno = sys_errno_;
      result = result_;
    }
    return result;
  }

  int call_;
  int sys_errno_;
  int result_;
  bool all_calls_;
  // Only the open op folds EISDIR into OB_NOT_SUPPORTED in production; see
  // the comment on set().
  bool fold_eisdir_;

private:
  void init_result_(const int sys_errno, const int result, const bool map_not_supported)
  {
    if (OB_SUCCESS != result) {
      result_ = result;
    } else {
      const int saved_errno = errno;
      errno = sys_errno;
      if (map_not_supported
          && (EOPNOTSUPP == sys_errno || ENOTSUP == sys_errno
              || ENOSYS == sys_errno || EINVAL == sys_errno
              || (fold_eisdir_ && EISDIR == sys_errno))) {
        result_ = OB_NOT_SUPPORTED;
      } else {
        result_ = palf::convert_sys_errno();
      }
      errno = saved_errno;
    }
  }
};

class FakeFallocateProbeOps
{
public:
  FakeFallocateProbeOps()
    : open_call_count_(0),
      fallocate_call_count_(0),
      close_call_count_(0),
      open_compat_call_count_(0),
      unlink_call_count_(0)
  {
    // Production posix_probe_open folds EISDIR like the other "O_TMPFILE
    // unsupported" errnos; fallocate and the compat open do not.
    open_failure_.fold_eisdir_ = true;
  }

  void fail_open_at(const int call, const int sys_errno, const int result = OB_SUCCESS)
  {
    open_failure_.set(call, sys_errno, result);
  }

  // Simulates a file system where O_TMPFILE is unsupported (e.g. NFS).
  void fail_open_always(const int sys_errno)
  {
    open_failure_.set_persistent(sys_errno);
  }

  void fail_fallocate_at(const int call, const int sys_errno, const int result = OB_SUCCESS)
  {
    fallocate_failure_.set(call, sys_errno, result);
  }

  void fail_close_at(const int call, const int sys_errno, const int result = OB_SUCCESS)
  {
    // Production posix_probe_close only maps through convert_sys_errno().
    close_failure_.set(call, sys_errno, result, false /* map_not_supported */);
  }

  void fail_open_compat_at(const int call, const int sys_errno, const int result = OB_SUCCESS)
  {
    open_compat_failure_.set(call, sys_errno, result);
  }

  void fail_unlink_at(const int call, const int sys_errno, const int result = OB_SUCCESS)
  {
    // Production posix_probe_unlink only maps through convert_sys_errno().
    unlink_failure_.set(call, sys_errno, result, false /* map_not_supported */);
  }

  int open_op(const char *dir, int &fd)
  {
    ++open_call_count_;
    open_paths_.push_back(dir);
    fd = 100 + open_call_count_;
    return open_failure_.get_result(open_call_count_, OB_SUCCESS);
  }

  int fallocate_op(const int fd,
                   const int mode,
                   const int64_t offset,
                   const int64_t len)
  {
    ++fallocate_call_count_;
    fallocate_calls_.push_back(FallocateCall(fd, mode, offset, len));
    return fallocate_failure_.get_result(fallocate_call_count_, OB_SUCCESS);
  }

  int close_op(const int /* fd */)
  {
    ++close_call_count_;
    return close_failure_.get_result(close_call_count_, OB_SUCCESS);
  }

  int open_compat_op(const char *path, int &fd)
  {
    ++open_compat_call_count_;
    open_compat_paths_.push_back(path);
    fd = 200 + open_compat_call_count_;
    return open_compat_failure_.get_result(open_compat_call_count_, OB_SUCCESS);
  }

  int unlink_op(const char *path)
  {
    ++unlink_call_count_;
    unlink_paths_.push_back(path);
    return unlink_failure_.get_result(unlink_call_count_, OB_SUCCESS);
  }

  int open_call_count_;
  int fallocate_call_count_;
  int close_call_count_;
  int open_compat_call_count_;
  int unlink_call_count_;
  std::vector<std::string> open_paths_;
  std::vector<FallocateCall> fallocate_calls_;
  std::vector<std::string> open_compat_paths_;
  std::vector<std::string> unlink_paths_;

private:
  InjectedFailure open_failure_;
  InjectedFailure fallocate_failure_;
  InjectedFailure close_failure_;
  InjectedFailure open_compat_failure_;
  InjectedFailure unlink_failure_;
};

class RecordingPosixFallocateProbeOps
{
public:
  int open_op(const char *dir, int &fd)
  {
    open_paths_.push_back(dir);
    return palf::posix_probe_open(dir, fd);
  }

  int fallocate_op(const int fd,
                   const int mode,
                   const int64_t offset,
                   const int64_t len)
  {
    fallocate_calls_.push_back(FallocateCall(fd, mode, offset, len));
    return palf::posix_probe_fallocate(fd, mode, offset, len);
  }

  int close_op(const int fd)
  {
    close_fds_.push_back(fd);
    return palf::posix_probe_close(fd);
  }

  int open_compat_op(const char *path, int &fd)
  {
    open_compat_paths_.push_back(path);
    return palf::posix_probe_open_compat(path, fd);
  }

  int unlink_op(const char *path)
  {
    unlink_paths_.push_back(path);
    return palf::posix_probe_unlink(path);
  }

  std::vector<std::string> open_paths_;
  std::vector<FallocateCall> fallocate_calls_;
  std::vector<int> close_fds_;
  std::vector<std::string> open_compat_paths_;
  std::vector<std::string> unlink_paths_;
};

// NFS-like environment: O_TMPFILE is rejected while all other operations hit
// the real local file system.
class TmpfileUnsupportedProbeOps : public RecordingPosixFallocateProbeOps
{
public:
  int open_op(const char *dir, int &fd)
  {
    open_paths_.push_back(dir);
    fd = -1;
    errno = EOPNOTSUPP;
    return OB_NOT_SUPPORTED;
  }
};

template <typename ProbeOps>
ObFallocateProbeOps make_probe_ops(ProbeOps &ops)
{
  return ObFallocateProbeOps(
      [&ops](const char *dir, int &fd) { return ops.open_op(dir, fd); },
      [&ops](const int fd, const int op_mode, const int64_t offset, const int64_t len) {
        return ops.fallocate_op(fd, op_mode, offset, len);
      },
      [&ops](const int fd) { return ops.close_op(fd); },
      [&ops](const char *path, int &fd) { return ops.open_compat_op(path, fd); },
      [&ops](const char *path) { return ops.unlink_op(path); });
}

// Ops without the compat fallback: O_TMPFILE is the only probe method.
template <typename ProbeOps>
ObFallocateProbeOps make_tmpfile_only_probe_ops(ProbeOps &ops)
{
  return ObFallocateProbeOps(
      [&ops](const char *dir, int &fd) { return ops.open_op(dir, fd); },
      [&ops](const int fd, const int op_mode, const int64_t offset, const int64_t len) {
        return ops.fallocate_op(fd, op_mode, offset, len);
      },
      [&ops](const int fd) { return ops.close_op(fd); });
}

} // end anonymous namespace

// Static mock probe ops pointer for unit test weak symbol overriding
static const ObFallocateProbeOps *g_mock_fallocate_probe_ops = nullptr;

class ObFallocateProbeOpsGuard
{
public:
  explicit ObFallocateProbeOpsGuard(const ObFallocateProbeOps &mock_ops)
    : prev_ops_(g_mock_fallocate_probe_ops)
  {
    g_mock_fallocate_probe_ops = &mock_ops;
  }
  ~ObFallocateProbeOpsGuard()
  {
    g_mock_fallocate_probe_ops = prev_ops_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObFallocateProbeOpsGuard);
  const ObFallocateProbeOps *prev_ops_;
};

// Strong symbol definition overriding weak symbol in log_io_utils.cpp
const ObFallocateProbeOps &get_default_fallocate_probe_ops()
{
  return (nullptr != g_mock_fallocate_probe_ops)
      ? *g_mock_fallocate_probe_ops
      : POSIX_FALLOCATE_PROBE_OPS;
}

namespace
{

template <typename ProbeOps>
int check_clog_capability_with_ops(const char *clog_dir,
                                   ProbeOps &ops)
{
  const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
  ObFallocateProbeOpsGuard guard(probe_ops);
  return palf::check_file_system_fallocate_capability(clog_dir);
}

#ifdef OB_BUILD_ARBITRATION
template <typename ProbeOps>
int check_arbitration_capability_with_ops(const char *arb_dir,
                                          ProbeOps &ops)
{
  const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
  ObFallocateProbeOpsGuard guard(probe_ops);
  return palf::check_arbitration_file_system_fallocate_capability(arb_dir);
}
#endif

} // end anonymous namespace

class TestFallocateProbe : public ::testing::Test
{
public:
  void SetUp() override
  {
    GCONF._enable_fallocate_probe = true;
    char dir_template[] = "./test_fallocate_probe_XXXXXX";
    char *dir = ::mkdtemp(dir_template);
    ASSERT_NE(nullptr, dir);
    temp_dir_ = dir;
    detect_probe_environment_();
  }

  void TearDown() override
  {
    GCONF._enable_fallocate_probe = true;
    if (!temp_dir_.empty()) {
      (void)common::FileDirectoryUtils::delete_directory_rec(temp_dir_.c_str());
    }
  }

protected:
  std::string path(const char *name) const
  {
    return temp_dir_ + "/" + name;
  }

  // Number of entries in /proc/self/fd, or -1 when /proc is unavailable
  // (non-Linux): the fd-leak assertions degrade to a no-op then. The count
  // includes the directory handle opened by the counting itself, so as long
  // as before/after are measured the same way the comparison stays sound.
  static int64_t count_open_fds()
  {
    const char * const proc_fd = "/proc/self/fd";
    DIR *dir = ::opendir(proc_fd);
    if (nullptr == dir) {
      return -1;
    }
    int64_t count = 0;
    struct dirent *entry = nullptr;
    while (nullptr != (entry = ::readdir(dir))) {
      if (0 != std::strcmp(entry->d_name, ".") && 0 != std::strcmp(entry->d_name, "..")) {
        ++count;
      }
    }
    ::closedir(dir);
    return count;
  }

  void create_file(const std::string &file, const char *content)
  {
    const int fd = ::open(file.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(static_cast<ssize_t>(std::strlen(content)),
              ::write(fd, content, std::strlen(content)));
    ASSERT_EQ(0, ::close(fd));
  }

  void expect_file_content(const std::string &file, const char *content)
  {
    char buf[128] = {'\0'};
    const int fd = ::open(file.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    const ssize_t read_size = ::read(fd, buf, sizeof(buf) - 1);
    ASSERT_GE(read_size, 0);
    ASSERT_EQ(0, ::close(fd));
    EXPECT_EQ(std::string(content), std::string(buf, read_size));
  }

  // Real-filesystem cases must degrade to a visible GTEST_SKIP when the build
  // directory lacks the probed capabilities or free space: otherwise a nearly
  // full CI disk fails the count assertions mid-test, and an overlayfs build
  // directory silently exercises the compat path instead of the O_TMPFILE
  // path the test claims to cover.
  bool tmpfile_probe_env_ok() const { return tmpfile_probe_env_ok_ && free_space_ok_; }
  bool compat_probe_env_ok() const { return compat_probe_env_ok_ && free_space_ok_; }

private:
  static const int64_t ENV_PROBE_SIZE = 4 * 1024;

  // Probe the production posix ops once on the temp directory itself. The
  // WARN logs of a failing probe are expected and harmless.
  void detect_probe_environment_()
  {
    int fd = -1;
    tmpfile_probe_env_ok_ = (OB_SUCCESS == posix_probe_open(temp_dir_.c_str(), fd));
    if (tmpfile_probe_env_ok_) {
      const int fallocate_ret = posix_probe_fallocate(fd, 0, 0, ENV_PROBE_SIZE);
      const int close_ret = posix_probe_close(fd);
      tmpfile_probe_env_ok_ = (OB_SUCCESS == fallocate_ret) && (OB_SUCCESS == close_ret);
    }

    char env_probe_path[common::MAX_PATH_SIZE] = {'\0'};
    if (OB_SUCCESS == databuff_printf(env_probe_path, sizeof(env_probe_path),
                                      "%s/.env_probe.tmp", temp_dir_.c_str())) {
      compat_probe_env_ok_ = (OB_SUCCESS == posix_probe_open_compat(env_probe_path, fd));
      if (compat_probe_env_ok_) {
        const int fallocate_ret = posix_probe_fallocate(fd, 0, 0, ENV_PROBE_SIZE);
        const int close_ret = posix_probe_close(fd);
        compat_probe_env_ok_ = (OB_SUCCESS == fallocate_ret) && (OB_SUCCESS == close_ret)
            && (OB_SUCCESS == posix_probe_unlink(env_probe_path));
      }
    }

    struct statvfs vfs;
    // A full probe run holds one 4KB preallocation at a time; keep a generous
    // margin so the space check does not race with concurrent builds.
    free_space_ok_ = (0 == ::statvfs(temp_dir_.c_str(), &vfs))
        && (static_cast<int64_t>(vfs.f_bavail) * static_cast<int64_t>(vfs.f_bsize)
            >= 2 * PALF_PHY_BLOCK_SIZE);
  }

  bool tmpfile_probe_env_ok_ = false;
  bool compat_probe_env_ok_ = false;
  bool free_space_ok_ = false;

protected:
  std::string temp_dir_;
};

TEST_F(TestFallocateProbe, clog_success_and_restart_use_required_modes_and_sizes)
{
  FakeFallocateProbeOps ops;
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));

  ASSERT_EQ(2, ops.open_call_count_);
  ASSERT_EQ(4, ops.fallocate_call_count_);
  ASSERT_EQ(2, ops.close_call_count_);
  ASSERT_EQ(4U, ops.fallocate_calls_.size());

  // Primary O_TMPFILE path succeeds: compat ops must stay silent
  EXPECT_EQ(0, ops.open_compat_call_count_);
  EXPECT_EQ(0, ops.unlink_call_count_);

  const int expected_modes[] = {
    0,
    FALLOC_FL_ZERO_RANGE
  };
  const int64_t expected_lengths[] = {
    palf::LOG_DIO_ALIGN_SIZE,
    palf::LOG_DIO_ALIGN_SIZE
  };
  for (int64_t restart = 0; restart < 2; ++restart) {
    for (int64_t i = 0; i < ARRAYSIZEOF(expected_modes); ++i) {
      const FallocateCall &call = ops.fallocate_calls_.at(restart * 2 + i);
      EXPECT_EQ(expected_modes[i], call.mode_);
      EXPECT_EQ(0, call.offset_);
      EXPECT_EQ(expected_lengths[i], call.len_);
    }
    EXPECT_EQ(ops.fallocate_calls_.at(restart * 2).fd_,
              ops.fallocate_calls_.at(restart * 2 + 1).fd_);
  }

  const char *expected_probe_paths[] = {
    "/fake/clog",
    "/fake/clog"
  };
  ASSERT_EQ(ARRAYSIZEOF(expected_probe_paths),
            static_cast<int64_t>(ops.open_paths_.size()));
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_probe_paths); ++i) {
    EXPECT_EQ(expected_probe_paths[i], ops.open_paths_.at(i));
  }
}

TEST_F(TestFallocateProbe, required_fallocate_failure_policy)
{
  // 1. mode=0 fails with EOPNOTSUPP
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    ASSERT_EQ(1U, ops.fallocate_calls_.size());
    EXPECT_EQ(0, ops.fallocate_calls_.at(0).mode_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 2. extra_mode (FALLOC_FL_ZERO_RANGE) fails with EOPNOTSUPP
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(2, EOPNOTSUPP);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    ASSERT_EQ(2U, ops.fallocate_calls_.size());
    EXPECT_EQ(FALLOC_FL_ZERO_RANGE, ops.fallocate_calls_.at(1).mode_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 3. Space errors (ENOSPC, EDQUOT) on mode=0 -> skips probe, returns OB_SUCCESS
  const int space_errors[] = { ENOSPC, EDQUOT };
  for (int64_t i = 0; i < ARRAYSIZEOF(space_errors); ++i) {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, space_errors[i]);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 4. Space errors on extra_mode -> skips probe, returns OB_SUCCESS
  for (int64_t i = 0; i < ARRAYSIZEOF(space_errors); ++i) {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(2, space_errors[i]);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 5. Hard errors on mode=0 (EINVAL/ENOSYS mapped to OB_NOT_SUPPORTED, EACCES/EIO mapped via convert_sys_errno)
  const int hard_errors[] = { EINVAL, ENOSYS, EACCES, EIO };
  const int expected_hard_errs[] = {
    OB_NOT_SUPPORTED,
    OB_NOT_SUPPORTED,
    OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
    OB_IO_ERROR
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(hard_errors); ++i) {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, hard_errors[i]);
    EXPECT_EQ(expected_hard_errs[i], check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 6. Hard errors on extra_mode (FALLOC_FL_ZERO_RANGE)
  for (int64_t i = 0; i < ARRAYSIZEOF(hard_errors); ++i) {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(2, hard_errors[i]);
    EXPECT_EQ(expected_hard_errs[i], check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.close_call_count_);
  }
}

TEST_F(TestFallocateProbe, restart_rechecks_required_failure_policy)
{
  {
    FakeFallocateProbeOps ops;
    ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    ops.fail_fallocate_at(3, EOPNOTSUPP); // mode=0 in second restart
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    ASSERT_EQ(3U, ops.fallocate_calls_.size());
    EXPECT_EQ(0, ops.fallocate_calls_.back().mode_);
    EXPECT_EQ(palf::LOG_DIO_ALIGN_SIZE, ops.fallocate_calls_.back().len_);
  }
  {
    FakeFallocateProbeOps ops;
    ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    ops.fail_fallocate_at(4, EOPNOTSUPP); // extra_mode in second restart
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    ASSERT_EQ(4U, ops.fallocate_calls_.size());
    EXPECT_EQ(FALLOC_FL_ZERO_RANGE, ops.fallocate_calls_.back().mode_);
    EXPECT_EQ(palf::LOG_DIO_ALIGN_SIZE, ops.fallocate_calls_.back().len_);
  }
  {
    FakeFallocateProbeOps ops;
    ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    ops.fail_fallocate_at(4, EIO);
    EXPECT_EQ(OB_IO_ERROR, check_clog_capability_with_ops("/fake/clog", ops));
    ASSERT_EQ(4U, ops.fallocate_calls_.size());
  }
}

TEST_F(TestFallocateProbe, syscall_failures_cleanup_and_preserve_first_error)
{
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, EACCES);
    EXPECT_EQ(OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
    // A permission error must NOT widen the fallback whitelist: no named-file
    // probe may be created in a directory where O_TMPFILE was denied
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.unlink_call_count_);
  }
  {
    // fd exhaustion is a hard error and must NOT trigger the fallback either
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, EMFILE);
    EXPECT_EQ(OB_TOO_MANY_OPEN_FILES,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.unlink_call_count_);
  }
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, EIO);
    EXPECT_EQ(OB_IO_ERROR,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
  }
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, EIO);
    ops.fail_close_at(1, EACCES);
    EXPECT_EQ(OB_IO_ERROR,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }
  {
    FakeFallocateProbeOps ops;
    ops.fail_close_at(1, EACCES);
    EXPECT_EQ(OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(2, EIO);
    ops.fail_close_at(1, EACCES);
    EXPECT_EQ(OB_IO_ERROR,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }
}

TEST_F(TestFallocateProbe, empty_directory_creates_fresh_probe_and_cleans_up)
{
  if (!tmpfile_probe_env_ok()) {
    SKIP_REAL_PROBE_TEST("environment lacks O_TMPFILE/fallocate support or free space for real probe");
  }
  const std::string clog_dir = path("clog");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  RecordingPosixFallocateProbeOps ops;
  const int64_t fd_count_before = count_open_fds();
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops(clog_dir.c_str(), ops));

  ASSERT_EQ(1U, ops.open_paths_.size());
  EXPECT_EQ(clog_dir, ops.open_paths_.at(0));
  ASSERT_EQ(2U, ops.fallocate_calls_.size());
  EXPECT_EQ(0, ops.fallocate_calls_.at(0).mode_);
  EXPECT_EQ(FALLOC_FL_ZERO_RANGE, ops.fallocate_calls_.at(1).mode_);
  // The anonymous probe file was really closed on the real path
  ASSERT_EQ(1U, ops.close_fds_.size());
  EXPECT_GE(ops.close_fds_.at(0), 0);
  // No fd leaked by the primary O_TMPFILE probe run (the second production
  // call below is also covered by the re-check after it)
  if (fd_count_before >= 0) {
    EXPECT_EQ(fd_count_before, count_open_fds());
  }

  // With O_TMPFILE, probe creates unnamed regular files, no .tmp file left in directory
  errno = 0;
  EXPECT_EQ(-1, ::access((clog_dir + "/.ob_fallocate_probe_clog.tmp").c_str(), F_OK));
  EXPECT_EQ(ENOENT, errno);

  // Real production check_file_system_fallocate_capability on empty directory
  ASSERT_EQ(OB_SUCCESS,
            palf::check_file_system_fallocate_capability(clog_dir.c_str()));
  errno = 0;
  EXPECT_EQ(-1, ::access((clog_dir + "/.ob_fallocate_probe_clog.tmp").c_str(), F_OK));
  EXPECT_EQ(ENOENT, errno);
  if (fd_count_before >= 0) {
    EXPECT_EQ(fd_count_before, count_open_fds());
  }
}

TEST_F(TestFallocateProbe, non_empty_directory_still_executes_fallocate_probe)
{
  if (!tmpfile_probe_env_ok()) {
    SKIP_REAL_PROBE_TEST("environment lacks O_TMPFILE/fallocate support or free space for real probe");
  }
  const std::string clog_dir = path("clog");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  const std::string business_log = clog_dir + "/business_log_1";
  const std::string lost_found = clog_dir + "/lost+found";
  const char log_content[] = "business log content";
  create_file(business_log, log_content);
  ASSERT_EQ(0, ::mkdir(lost_found.c_str(), 0755));

  RecordingPosixFallocateProbeOps ops;
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops(clog_dir.c_str(), ops));

  // Probe must NOT be skipped even though directory is not empty
  EXPECT_EQ(1U, ops.open_paths_.size());
  EXPECT_EQ(clog_dir, ops.open_paths_.at(0));
  EXPECT_EQ(2U, ops.fallocate_calls_.size());

  // Business files remain untouched
  expect_file_content(business_log, log_content);
  EXPECT_EQ(0, ::access(lost_found.c_str(), F_OK));
}

TEST_F(TestFallocateProbe, stale_probe_file_does_not_affect_fresh_tmpfile_probe)
{
  if (!tmpfile_probe_env_ok()) {
    SKIP_REAL_PROBE_TEST("environment lacks O_TMPFILE/fallocate support or free space for real probe");
  }
  const std::string clog_dir = path("clog");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  const std::string stale_clog_probe = clog_dir + "/.ob_fallocate_probe_clog.tmp";
  create_file(stale_clog_probe, "stale clog probe");

  RecordingPosixFallocateProbeOps ops;
  // With O_TMPFILE, existing files in directory do not interfere with the anonymous probe
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops(clog_dir.c_str(), ops));

  ASSERT_EQ(1U, ops.open_paths_.size());
  EXPECT_EQ(clog_dir, ops.open_paths_.at(0));
  ASSERT_EQ(2U, ops.fallocate_calls_.size());

  // Stale probe file content remains intact (to be trimmed by ObServerLogBlockMgr::do_load_)
  expect_file_content(stale_clog_probe, "stale clog probe");
}

TEST_F(TestFallocateProbe, stale_probe_file_with_business_file_executes_probe)
{
  if (!tmpfile_probe_env_ok()) {
    SKIP_REAL_PROBE_TEST("environment lacks O_TMPFILE/fallocate support or free space for real probe");
  }
  const std::string clog_dir = path("clog");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  const std::string stale_clog_probe = clog_dir + "/.ob_fallocate_probe_clog.tmp";
  const std::string business_log = clog_dir + "/business_log";
  create_file(stale_clog_probe, "stale clog probe");
  create_file(business_log, "business log");

  RecordingPosixFallocateProbeOps ops;
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops(clog_dir.c_str(), ops));

  EXPECT_EQ(1U, ops.open_paths_.size());
  EXPECT_EQ(clog_dir, ops.open_paths_.at(0));
  EXPECT_EQ(2U, ops.fallocate_calls_.size());

  // Business file remains intact
  expect_file_content(business_log, "business log");
  expect_file_content(stale_clog_probe, "stale clog probe");
}

TEST_F(TestFallocateProbe, disk_space_not_enough_skips_fallocate_probe)
{
  // 1. fallocate mode=0 fails with ENOSPC -> skips probe, returns OB_SUCCESS
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, ENOSPC);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 2. fallocate mode=0 fails with EDQUOT -> skips probe, returns OB_SUCCESS
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, EDQUOT);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 3. fallocate extra mode fails with ENOSPC -> skips probe, returns OB_SUCCESS
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(2, ENOSPC); // extra mode (FALLOC_FL_ZERO_RANGE)
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 4. open fails with ENOSPC -> skips probe, returns OB_SUCCESS
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, ENOSPC);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
  }

  // 5. open fails with EDQUOT -> skips probe, returns OB_SUCCESS
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, EDQUOT);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
  }

  // 6. On the primary O_TMPFILE path, the ENOSPC skip decision must survive a
  //    later close failure (no compat cleanup exists on this path)
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, ENOSPC);
    ops.fail_close_at(1, EIO);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.unlink_call_count_);
  }

  // 7. Probe succeeded, then close reports ENOSPC (the NFS deferred-error
  //    slot per close(2)) -> the space skip covers the cleanup phase too
  {
    FakeFallocateProbeOps ops;
    ops.fail_close_at(1, ENOSPC);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.unlink_call_count_);
  }

  // 8. Anti-laundering: a non-space probe verdict must survive a later space
  //    error at close; the final space normalization must not swallow it
  //    into a false pass
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    ops.fail_close_at(1, ENOSPC);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.unlink_call_count_);
  }
}

TEST_F(TestFallocateProbe, config_disable_fallocate_probe)
{
  ASSERT_TRUE(GCONF._enable_fallocate_probe);

  // Disable probe via config
  GCONF._enable_fallocate_probe = false;

  FakeFallocateProbeOps ops;
  const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
  ObFallocateProbeOpsGuard guard(probe_ops);

  // 1. ObServerLogBlockMgr init respects _enable_fallocate_probe=false and bypasses probe
  const std::string clog_dir = path("clog_disabled");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));
  logservice::ObServerLogBlockMgr log_block_mgr;
  EXPECT_EQ(OB_SUCCESS, log_block_mgr.init(clog_dir.c_str()));
  EXPECT_EQ(0, ops.open_call_count_);
  EXPECT_EQ(0, ops.fallocate_call_count_);
  EXPECT_EQ(0, ops.close_call_count_);
  log_block_mgr.destroy();

  // Re-enable probe
  GCONF._enable_fallocate_probe = true;
  EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
  EXPECT_EQ(1, ops.open_call_count_);
  EXPECT_EQ(2, ops.fallocate_call_count_);
  EXPECT_EQ(1, ops.close_call_count_);
}

#ifdef OB_BUILD_ARBITRATION
TEST_F(TestFallocateProbe, arbitration_probe_behavior)
{
  // 1. Mock ops test: arbitration only probes mode=0 with size LOG_DIO_ALIGN_SIZE (4KB)
  {
    FakeFallocateProbeOps ops;
    ASSERT_EQ(OB_SUCCESS, check_arbitration_capability_with_ops("/fake/arb", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);

    ASSERT_EQ(1U, ops.open_paths_.size());
    EXPECT_EQ("/fake/arb", ops.open_paths_.at(0));
    EXPECT_EQ(0, ops.fallocate_calls_.at(0).mode_);
    EXPECT_EQ(palf::LOG_DIO_ALIGN_SIZE, ops.fallocate_calls_.at(0).len_);
  }

  // 2. Mock failure: mode=0 returns EOPNOTSUPP -> OB_NOT_SUPPORTED
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_arbitration_capability_with_ops("/fake/arb", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 3. Mock failure: mode=0 hard errors (EINVAL, ENOSYS, EACCES, EIO)
  const int hard_errors[] = { EINVAL, ENOSYS, EACCES, EIO };
  const int expected_hard_errs[] = {
    OB_NOT_SUPPORTED,
    OB_NOT_SUPPORTED,
    OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
    OB_IO_ERROR
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(hard_errors); ++i) {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, hard_errors[i]);
    EXPECT_EQ(expected_hard_errs[i], check_arbitration_capability_with_ops("/fake/arb", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 4. Space errors (ENOSPC, EDQUOT) on fallocate -> skips probe, returns OB_SUCCESS
  const int space_errors[] = { ENOSPC, EDQUOT };
  for (int64_t i = 0; i < ARRAYSIZEOF(space_errors); ++i) {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, space_errors[i]);
    EXPECT_EQ(OB_SUCCESS, check_arbitration_capability_with_ops("/fake/arb", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 5. Space errors (ENOSPC, EDQUOT) on open -> skips probe, returns OB_SUCCESS
  for (int64_t i = 0; i < ARRAYSIZEOF(space_errors); ++i) {
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, space_errors[i]);
    EXPECT_EQ(OB_SUCCESS, check_arbitration_capability_with_ops("/fake/arb", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
  }

  // 6. Syscall failures during arbitration (open, close)
  {
    // Open fails with EACCES
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, EACCES);
    EXPECT_EQ(OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
              check_arbitration_capability_with_ops("/fake/arb", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
  }
  {
    // Close fails with EACCES -> cleanup continues and preserves error
    FakeFallocateProbeOps ops;
    ops.fail_close_at(1, EACCES);
    EXPECT_EQ(OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
              check_arbitration_capability_with_ops("/fake/arb", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }
  {
    // Multiple failures: first error preserved (fallocate EIO, close EACCES)
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, EIO);
    ops.fail_close_at(1, EACCES);
    EXPECT_EQ(OB_IO_ERROR,
              check_arbitration_capability_with_ops("/fake/arb", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 7. Real filesystem test: check_arbitration_file_system_fallocate_capability
  if (tmpfile_probe_env_ok()) {
    ASSERT_EQ(OB_SUCCESS,
              palf::check_arbitration_file_system_fallocate_capability(temp_dir_.c_str()));
    errno = 0;
    EXPECT_EQ(-1,
              ::access(path(".ob_fallocate_probe_arbitration_clog.tmp").c_str(), F_OK));
    EXPECT_EQ(ENOENT, errno);
  } else {
    fprintf(stderr, "[  SKIPPED  ] real tmpfile arb probe: environment lacks O_TMPFILE/fallocate or free space\n");
  }

  // 8. Compat fallback in arbitration mode: probe file is role-scoped and
  //    arbitration probes mode=0 only
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    EXPECT_EQ(OB_SUCCESS, check_arbitration_capability_with_ops("/fake/arb", ops));
    EXPECT_EQ(1, ops.open_compat_call_count_);
    ASSERT_EQ(1U, ops.open_compat_paths_.size());
    EXPECT_EQ("/fake/arb/.ob_fallocate_probe_arbitration_clog.tmp",
              ops.open_compat_paths_.at(0));
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.fallocate_calls_.at(0).mode_);
    EXPECT_EQ(palf::LOG_DIO_ALIGN_SIZE, ops.fallocate_calls_.at(0).len_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }
  {
    // Compat probe detects unsupported fallocate -> OB_NOT_SUPPORTED
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_arbitration_capability_with_ops("/fake/arb", ops));
  }
  {
    // Real compat flow on local filesystem with O_TMPFILE simulated unsupported
    if (!compat_probe_env_ok()) {
      SKIP_REAL_PROBE_TEST("environment lacks named-file fallocate support for real compat probe");
    }
    TmpfileUnsupportedProbeOps ops;
    ASSERT_EQ(OB_SUCCESS, check_arbitration_capability_with_ops(temp_dir_.c_str(), ops));
    ASSERT_EQ(1U, ops.open_compat_paths_.size());
    EXPECT_EQ(temp_dir_ + "/.ob_fallocate_probe_arbitration_clog.tmp",
              ops.open_compat_paths_.at(0));
    errno = 0;
    EXPECT_EQ(-1,
              ::access(path(".ob_fallocate_probe_arbitration_clog.tmp").c_str(), F_OK));
    EXPECT_EQ(ENOENT, errno);
  }
}
#endif

TEST_F(TestFallocateProbe, invalid_arguments_are_rejected)
{
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            palf::check_file_system_fallocate_capability(nullptr));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            palf::check_file_system_fallocate_capability(""));
#ifdef OB_BUILD_ARBITRATION
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            palf::check_arbitration_file_system_fallocate_capability(nullptr));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            palf::check_arbitration_file_system_fallocate_capability(""));
#endif

  // Empty directory is rejected by param validation before any syscall is
  // attempted: the probe layer owns the deterministic empty-path semantics
  // (unlike ObServerLogBlockMgr::init(""), whose outcome depends on the
  // effective uid and which must not be exercised here).
  {
    FakeFallocateProbeOps ops;
    EXPECT_EQ(OB_INVALID_ARGUMENT, check_clog_capability_with_ops("", ops));
    EXPECT_EQ(0, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
#ifdef OB_BUILD_ARBITRATION
    FakeFallocateProbeOps arb_ops;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              check_arbitration_capability_with_ops("", arb_ops));
    EXPECT_EQ(0, arb_ops.open_call_count_);
#endif
  }

  // Invalid ops
  ObFallocateProbeOps::OpenOp empty_open;
  ObFallocateProbeOps::FallocateOp empty_fallocate;
  ObFallocateProbeOps::CloseOp empty_close;
  const ObFallocateProbeOps invalid_ops(empty_open, empty_fallocate, empty_close);
  EXPECT_FALSE(invalid_ops.is_valid());
  EXPECT_FALSE(invalid_ops.is_compat_valid());
  const ObFallocateProbeParam valid_clog_param =
      ObFallocateProbeParam::get_clog_param("/fake/clog");
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            palf::probe_fallocate_capability_with_ops(valid_clog_param, invalid_ops));

  // Ops with compat members: primary and compat validity are independent
  FakeFallocateProbeOps fake_ops;
  const ObFallocateProbeOps full_ops = make_probe_ops(fake_ops);
  EXPECT_TRUE(full_ops.is_valid());
  EXPECT_TRUE(full_ops.is_compat_valid());
  const ObFallocateProbeOps tmpfile_only_ops = make_tmpfile_only_probe_ops(fake_ops);
  EXPECT_TRUE(tmpfile_only_ops.is_valid());
  EXPECT_FALSE(tmpfile_only_ops.is_compat_valid());
}

TEST_F(TestFallocateProbe, fallocate_probe_param_validation_and_to_string)
{
  // Mode name helper
  EXPECT_STREQ("mode=0", ObFallocateProbeParam::get_fallocate_mode_name(0));
  EXPECT_STREQ("FALLOC_FL_ZERO_RANGE",
               ObFallocateProbeParam::get_fallocate_mode_name(FALLOC_FL_ZERO_RANGE));
  EXPECT_EQ(nullptr, ObFallocateProbeParam::get_fallocate_mode_name(9999));

  // Factory methods
  ObFallocateProbeParam clog_param = ObFallocateProbeParam::get_clog_param("/data/clog");
  EXPECT_TRUE(clog_param.is_valid());
  EXPECT_STREQ("clog", clog_param.get_disk_role());
  EXPECT_STREQ("/data/clog", clog_param.get_probe_file_dir());
  EXPECT_EQ(palf::LOG_DIO_ALIGN_SIZE, clog_param.get_probe_size());
  EXPECT_EQ(FALLOC_FL_ZERO_RANGE, clog_param.get_extra_mode());
  EXPECT_STREQ("FALLOC_FL_ZERO_RANGE", clog_param.get_extra_mode_name());

  // Named probe file path used by the compat fallback
  char probe_path[common::MAX_PATH_SIZE] = {'\0'};
  EXPECT_EQ(OB_SUCCESS, clog_param.get_probe_file_path(probe_path, sizeof(probe_path)));
  EXPECT_EQ(std::string("/data/clog/.ob_fallocate_probe_clog.tmp"), probe_path);
  EXPECT_EQ(OB_INVALID_ARGUMENT, clog_param.get_probe_file_path(nullptr, sizeof(probe_path)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, clog_param.get_probe_file_path(probe_path, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, clog_param.get_probe_file_path(probe_path, -1));
  ObFallocateProbeParam invalid_size_param("clog", "/data", 0);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            invalid_size_param.get_probe_file_path(probe_path, sizeof(probe_path)));

  // A positive-but-too-small buffer must fail with OB_SIZE_OVERFLOW instead of
  // silently truncating the probe path, and the truncated output stays
  // NUL-terminated (databuff_printf safety contract). The sentinel fill makes
  // the last-byte check discriminate "wrote the NUL" from "buffer was zeroed".
  char tiny_buf[16];
  ::memset(tiny_buf, 'X', sizeof(tiny_buf));
  const std::string expected_full_path = "/data/clog/.ob_fallocate_probe_clog.tmp";
  EXPECT_EQ(OB_SIZE_OVERFLOW, clog_param.get_probe_file_path(tiny_buf, sizeof(tiny_buf)));
  EXPECT_EQ(0, std::strncmp(tiny_buf, expected_full_path.c_str(), sizeof(tiny_buf) - 1));
  EXPECT_EQ('\0', tiny_buf[sizeof(tiny_buf) - 1]);

#ifdef OB_BUILD_ARBITRATION
  ObFallocateProbeParam arb_param =
      ObFallocateProbeParam::get_arbitration_param("/data/arbitration");
  EXPECT_TRUE(arb_param.is_valid());
  EXPECT_STREQ("arbitration_clog", arb_param.get_disk_role());
  EXPECT_STREQ("/data/arbitration", arb_param.get_probe_file_dir());
  EXPECT_EQ(palf::LOG_DIO_ALIGN_SIZE, arb_param.get_probe_size());
  EXPECT_EQ(0, arb_param.get_extra_mode());
  EXPECT_STREQ("mode=0", arb_param.get_extra_mode_name());
  EXPECT_EQ(OB_SUCCESS, arb_param.get_probe_file_path(probe_path, sizeof(probe_path)));
  EXPECT_EQ(std::string("/data/arbitration/.ob_fallocate_probe_arbitration_clog.tmp"),
            probe_path);
#endif

  // Automatic mode name resolution in constructor without passing mode string
  ObFallocateProbeParam auto_clog("clog", "/data/clog", 4096, FALLOC_FL_ZERO_RANGE);
  EXPECT_TRUE(auto_clog.is_valid());
  EXPECT_STREQ("FALLOC_FL_ZERO_RANGE", auto_clog.get_extra_mode_name());

  // Mode 0 resolves through the same path, without a nullptr special case
  ObFallocateProbeParam base_mode("clog", "/data", 4096, 0);
  EXPECT_TRUE(base_mode.is_valid());
  EXPECT_STREQ("mode=0", base_mode.get_extra_mode_name());

  // Unknown mode fails validation because mode name cannot be resolved
  ObFallocateProbeParam unknown_mode("clog", "/data", 4096, 9999);
  EXPECT_EQ(nullptr, unknown_mode.get_extra_mode_name());
  EXPECT_FALSE(unknown_mode.is_valid());

  // Invalid cases
  ObFallocateProbeParam null_role(nullptr, "/data", 4096);
  EXPECT_FALSE(null_role.is_valid());

  ObFallocateProbeParam empty_role("", "/data", 4096);
  EXPECT_FALSE(empty_role.is_valid());

  ObFallocateProbeParam null_dir("clog", nullptr, 4096);
  EXPECT_FALSE(null_dir.is_valid());

  ObFallocateProbeParam empty_dir("clog", "", 4096);
  EXPECT_FALSE(empty_dir.is_valid());

  ObFallocateProbeParam invalid_size("clog", "/data", 0);
  EXPECT_FALSE(invalid_size.is_valid());

  // to_string
  char str_buf[512] = {'\0'};
  const int64_t len = clog_param.to_string(str_buf, sizeof(str_buf));
  EXPECT_GT(len, 0);
  const std::string param_str(str_buf, len);
  EXPECT_NE(std::string::npos, param_str.find("disk_role"));
  EXPECT_NE(std::string::npos, param_str.find("clog"));
  EXPECT_NE(std::string::npos, param_str.find("directory"));
  EXPECT_NE(std::string::npos, param_str.find("/data/clog"));
  EXPECT_NE(std::string::npos, param_str.find("probe_size"));
  EXPECT_NE(std::string::npos, param_str.find("extra_mode"));
  EXPECT_NE(std::string::npos, param_str.find("extra_mode_name"));
  EXPECT_NE(std::string::npos, param_str.find("FALLOC_FL_ZERO_RANGE"));
}

TEST_F(TestFallocateProbe, posix_probe_ops_handle_errors)
{
  int fd = -1;
  // 1. Invalid arguments
  EXPECT_EQ(OB_INVALID_ARGUMENT, palf::posix_probe_open(nullptr, fd));
  EXPECT_EQ(OB_INVALID_ARGUMENT, palf::posix_probe_fallocate(-1, 0, 0, 1024));
  EXPECT_EQ(OB_INVALID_ARGUMENT, palf::posix_probe_fallocate(10, 0, -1, 1024));
  EXPECT_EQ(OB_INVALID_ARGUMENT, palf::posix_probe_fallocate(10, 0, 0, -1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, palf::posix_probe_close(-1));

  // 2. Open non-existent directory
  const std::string missing_dir = path("no_such_directory");
#ifdef O_TMPFILE
  EXPECT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY,
            palf::posix_probe_open(missing_dir.c_str(), fd));

  // 2.1 Empty (non-null) path fails in the VFS without touching the filesystem
  EXPECT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, palf::posix_probe_open("", fd));
  EXPECT_EQ(-1, fd);
#else
  EXPECT_EQ(OB_NOT_SUPPORTED,
            palf::posix_probe_open(missing_dir.c_str(), fd));
  // Path errors must still be propagated end-to-end by the compat fallback.
  EXPECT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY,
            palf::check_file_system_fallocate_capability(missing_dir.c_str()));
#endif

  // 3. Successful open, fallocate, close (environment-dependent)
  if (tmpfile_probe_env_ok()) {
    EXPECT_EQ(OB_SUCCESS, palf::posix_probe_open(temp_dir_.c_str(), fd));
    EXPECT_GE(fd, 0);
    EXPECT_EQ(OB_SUCCESS, palf::posix_probe_fallocate(fd, 0, 0, 4096));
    EXPECT_EQ(OB_SUCCESS, palf::posix_probe_close(fd));
  } else {
    fprintf(stderr, "[  SKIPPED  ] real tmpfile ops sub-case: environment lacks O_TMPFILE/fallocate or free space\n");
  }
}

TEST_F(TestFallocateProbe, server_log_block_mgr_probe_integration)
{
  if (!tmpfile_probe_env_ok()) {
    SKIP_REAL_PROBE_TEST("environment lacks O_TMPFILE/fallocate support or free space for real probe");
  }
  // Test ObServerLogBlockMgr::init real integration with probe_fallocate_compatibility_
  const std::string clog_dir = path("clog_disk");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  logservice::ObServerLogBlockMgr log_block_mgr;
  ASSERT_EQ(OB_SUCCESS, log_block_mgr.init(clog_dir.c_str()));

  // Verify that inside clog_dir/log_pool, no .tmp probe file was left behind
  const std::string probe_path = clog_dir + "/log_pool/.ob_fallocate_probe_clog.tmp";
  errno = 0;
  EXPECT_EQ(-1, ::access(probe_path.c_str(), F_OK));
  EXPECT_EQ(ENOENT, errno);

  log_block_mgr.destroy();
}

TEST_F(TestFallocateProbe, server_log_block_mgr_stale_probe_cleaned_up_by_load)
{
  if (!tmpfile_probe_env_ok()) {
    SKIP_REAL_PROBE_TEST("environment lacks O_TMPFILE/fallocate support or free space for real probe");
  }
  // First do an init to establish a valid log_pool with its meta file
  const std::string clog_dir = path("clog_disk_stale");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  {
    logservice::ObServerLogBlockMgr log_block_mgr;
    ASSERT_EQ(OB_SUCCESS, log_block_mgr.init(clog_dir.c_str()));
    log_block_mgr.destroy();
  }

  // Now inject stale .tmp files into the initialized log_pool
  const std::string log_pool_dir = clog_dir + "/log_pool";
  const std::string stale_probe_file = log_pool_dir + "/.ob_fallocate_probe_clog.tmp";
  const std::string extra_tmp_file = log_pool_dir + "/stale_block.tmp";
  create_file(stale_probe_file, "stale probe content");
  create_file(extra_tmp_file, "stale extra tmp content");

  // Re-init (restart simulation): do_load_ must clean up these stale .tmp files
  {
    logservice::ObServerLogBlockMgr log_block_mgr;
    ASSERT_EQ(OB_SUCCESS, log_block_mgr.init(clog_dir.c_str()));

    // Both .tmp files must have been cleaned up during init (do_load_ -> scan_log_pool_dir_and_do_trim_)
    errno = 0;
    EXPECT_EQ(-1, ::access(stale_probe_file.c_str(), F_OK));
    EXPECT_EQ(ENOENT, errno);
    errno = 0;
    EXPECT_EQ(-1, ::access(extra_tmp_file.c_str(), F_OK));
    EXPECT_EQ(ENOENT, errno);

    log_block_mgr.destroy();
  }
}

TEST_F(TestFallocateProbe, server_log_block_mgr_probe_failure_stops_init)
{
  const std::string clog_dir = path("clog_disk_fail");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  // 1. Invalid base path fails early.
  // NB: the empty string is deliberately NOT tested here: do_init_ builds
  // "/log_pool" against the filesystem root for it and would really create
  // that directory (plus meta) when the test runs as root.
  {
    logservice::ObServerLogBlockMgr log_block_mgr;
    EXPECT_EQ(OB_INVALID_ARGUMENT, log_block_mgr.init(nullptr));
  }

  // 2. Probe returns OB_NOT_SUPPORTED on mode=0 -> init fails, intercepts before do_load_, calls destroy()
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    EXPECT_EQ(OB_NOT_SUPPORTED, log_block_mgr.init(clog_dir.c_str()));
    // Verify destroy() was invoked: object is uninitialized and not reserved
    EXPECT_FALSE(log_block_mgr.is_reserved());
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.fallocate_calls_.at(0).mode_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 3. Probe returns OB_NOT_SUPPORTED on extra_mode (FALLOC_FL_ZERO_RANGE) -> init fails, calls destroy()
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(2, EOPNOTSUPP);
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    EXPECT_EQ(OB_NOT_SUPPORTED, log_block_mgr.init(clog_dir.c_str()));
    EXPECT_FALSE(log_block_mgr.is_reserved());
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(FALLOC_FL_ZERO_RANGE, ops.fallocate_calls_.at(1).mode_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 4. Probe returns OB_NOT_SUPPORTED on mode=0 when ENOSYS is injected
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, ENOSYS);
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    EXPECT_EQ(OB_NOT_SUPPORTED, log_block_mgr.init(clog_dir.c_str()));
    EXPECT_FALSE(log_block_mgr.is_reserved());
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 5. Probe returns OB_NOT_SUPPORTED on extra_mode when EINVAL is injected
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(2, EINVAL);
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    EXPECT_EQ(OB_NOT_SUPPORTED, log_block_mgr.init(clog_dir.c_str()));
    EXPECT_FALSE(log_block_mgr.is_reserved());
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 6. Probe returns OB_IO_ERROR on fallocate -> init fails and calls destroy()
  {
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, EIO);
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    EXPECT_EQ(OB_IO_ERROR, log_block_mgr.init(clog_dir.c_str()));
    EXPECT_FALSE(log_block_mgr.is_reserved());
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 7. Probe returns OB_FILE_OR_DIRECTORY_PERMISSION_DENIED on open -> init fails and calls destroy()
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, EACCES);
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    EXPECT_EQ(OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
              log_block_mgr.init(clog_dir.c_str()));
    EXPECT_FALSE(log_block_mgr.is_reserved());
    EXPECT_EQ(0, ops.close_call_count_);
  }

  // 8. Normal mock probe succeeds -> init completes successfully
  {
    FakeFallocateProbeOps ops;
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    ASSERT_EQ(OB_SUCCESS, log_block_mgr.init(clog_dir.c_str()));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    log_block_mgr.destroy();
  }

  // 9. When _enable_fallocate_probe = false, probe is skipped even if ops injects failure
  {
    GCONF._enable_fallocate_probe = false;
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    EXPECT_EQ(OB_SUCCESS, log_block_mgr.init(clog_dir.c_str()));
    EXPECT_EQ(0, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
    log_block_mgr.destroy();
    GCONF._enable_fallocate_probe = true;
  }
}

TEST_F(TestFallocateProbe, probe_failure_and_cleanup_matrix)
{
  {
    // open returns EIO -> probe ends without close
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, EIO);
    EXPECT_EQ(OB_IO_ERROR, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
  }
  {
    // fallocate mode=0 fails with ENOTSUP -> OB_NOT_SUPPORTED
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, ENOTSUP);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }
  {
    // fallocate extra_mode fails with ENOTSUP -> OB_NOT_SUPPORTED
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(2, ENOTSUP);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }
  {
    // fallocate mode=0 fails with ENOSYS -> OB_NOT_SUPPORTED
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(1, ENOSYS);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }
  {
    // fallocate extra_mode fails with EINVAL -> OB_NOT_SUPPORTED
    FakeFallocateProbeOps ops;
    ops.fail_fallocate_at(2, EINVAL);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }
}

TEST_F(TestFallocateProbe, compat_fallback_success_when_tmpfile_not_supported)
{
  // Every "O_TMPFILE not supported" errno of open falls back to the
  // named-file probe and succeeds
  const int not_supported_errors[] = { EOPNOTSUPP, ENOSYS, EINVAL, EISDIR };
  for (int64_t i = 0; i < ARRAYSIZEOF(not_supported_errors); ++i) {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(not_supported_errors[i]);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.open_compat_call_count_);
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_); // stale cleanup + probe file cleanup
  }

  // Detailed call sequence and arguments of a single compat probe
  FakeFallocateProbeOps ops;
  ops.fail_open_always(EOPNOTSUPP);
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));

  const std::string probe_file = "/fake/clog/.ob_fallocate_probe_clog.tmp";
  ASSERT_EQ(1U, ops.open_compat_paths_.size());
  EXPECT_EQ(probe_file, ops.open_compat_paths_.at(0));
  ASSERT_EQ(2U, ops.unlink_paths_.size());
  EXPECT_EQ(probe_file, ops.unlink_paths_.at(0));
  EXPECT_EQ(probe_file, ops.unlink_paths_.at(1));

  ASSERT_EQ(2U, ops.fallocate_calls_.size());
  EXPECT_EQ(0, ops.fallocate_calls_.at(0).mode_);
  EXPECT_EQ(FALLOC_FL_ZERO_RANGE, ops.fallocate_calls_.at(1).mode_);
  EXPECT_EQ(palf::LOG_DIO_ALIGN_SIZE, ops.fallocate_calls_.at(0).len_);
  EXPECT_EQ(palf::LOG_DIO_ALIGN_SIZE, ops.fallocate_calls_.at(1).len_);
  EXPECT_EQ(0, ops.fallocate_calls_.at(0).offset_);
  EXPECT_EQ(0, ops.fallocate_calls_.at(1).offset_);
  // Both fallocate probes run against the file opened by open_compat_
  EXPECT_EQ(ops.fallocate_calls_.at(0).fd_, ops.fallocate_calls_.at(1).fd_);
}

TEST_F(TestFallocateProbe, compat_fallback_repeats_across_restarts)
{
  FakeFallocateProbeOps ops;
  ops.fail_open_always(EOPNOTSUPP);
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
  EXPECT_EQ(2, ops.open_call_count_);
  EXPECT_EQ(2, ops.open_compat_call_count_);
  EXPECT_EQ(4, ops.fallocate_call_count_);
  EXPECT_EQ(2, ops.close_call_count_);
  EXPECT_EQ(4, ops.unlink_call_count_);
}

TEST_F(TestFallocateProbe, compat_fallocate_failure_policy_matches_primary)
{
  // 1. mode=0 fails with EOPNOTSUPP -> OB_NOT_SUPPORTED, full cleanup executed
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_compat_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 2. extra_mode (FALLOC_FL_ZERO_RANGE) fails with EOPNOTSUPP
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(2, EOPNOTSUPP);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 3. Space errors on fallocate -> skip probe, OB_SUCCESS
  const int space_errors[] = { ENOSPC, EDQUOT };
  for (int64_t i = 0; i < ARRAYSIZEOF(space_errors); ++i) {
    {
      FakeFallocateProbeOps ops;
      ops.fail_open_always(EOPNOTSUPP);
      ops.fail_fallocate_at(1, space_errors[i]);
      EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
      EXPECT_EQ(1, ops.close_call_count_);
      EXPECT_EQ(2, ops.unlink_call_count_);
    }
    {
      FakeFallocateProbeOps ops;
      ops.fail_open_always(EOPNOTSUPP);
      ops.fail_fallocate_at(2, space_errors[i]);
      EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
      EXPECT_EQ(1, ops.close_call_count_);
    }
  }

  // 4. Hard errors on fallocate keep the primary path error mapping
  const int hard_errors[] = { EINVAL, ENOSYS, EACCES, EIO };
  const int expected_hard_errs[] = {
    OB_NOT_SUPPORTED,
    OB_NOT_SUPPORTED,
    OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
    OB_IO_ERROR
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(hard_errors); ++i) {
    {
      FakeFallocateProbeOps ops;
      ops.fail_open_always(EOPNOTSUPP);
      ops.fail_fallocate_at(1, hard_errors[i]);
      EXPECT_EQ(expected_hard_errs[i], check_clog_capability_with_ops("/fake/clog", ops));
      EXPECT_EQ(1, ops.close_call_count_);
    }
    {
      FakeFallocateProbeOps ops;
      ops.fail_open_always(EOPNOTSUPP);
      ops.fail_fallocate_at(2, hard_errors[i]);
      EXPECT_EQ(expected_hard_errs[i], check_clog_capability_with_ops("/fake/clog", ops));
      EXPECT_EQ(1, ops.close_call_count_);
    }
  }

  // 5. Space error on compat open -> skip probe, OB_SUCCESS, no probe file
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_open_compat_at(1, ENOSPC);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
    EXPECT_EQ(1, ops.unlink_call_count_); // stale cleanup only
  }
}

TEST_F(TestFallocateProbe, compat_probe_file_path_overflow_fails_probe)
{
  // A directory whose probe file path cannot fit into MAX_PATH_SIZE must fail
  // the compat probe with OB_SIZE_OVERFLOW (from databuff_printf) instead of
  // truncating the path and probing somewhere else. Pure mock ops: the long
  // directory never needs to exist.
  const std::string long_dir(1100, 'd');
  ObFallocateProbeParam param(
      "clog", long_dir.c_str(), palf::PALF_PHY_BLOCK_SIZE, FALLOC_FL_ZERO_RANGE);
  ASSERT_TRUE(param.is_valid());

  FakeFallocateProbeOps ops;
  ops.fail_open_always(EOPNOTSUPP);
  const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
  EXPECT_EQ(OB_SIZE_OVERFLOW, palf::probe_fallocate_capability_with_ops(param, probe_ops));
  // The path could not be built: neither stale cleanup nor the compat open ran
  EXPECT_EQ(1, ops.open_call_count_);
  EXPECT_EQ(0, ops.unlink_call_count_);
  EXPECT_EQ(0, ops.open_compat_call_count_);
  EXPECT_EQ(0, ops.fallocate_call_count_);
}

TEST_F(TestFallocateProbe, compat_open_and_stale_cleanup_failures)
{
  // 1. Stale unlink fails with EACCES -> probe aborts before compat open
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_unlink_at(1, EACCES);
    EXPECT_EQ(OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
    EXPECT_EQ(1, ops.unlink_call_count_);
  }

  // 2. Stale unlink misses the file (ENOENT) -> tolerated, compat flow runs
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_unlink_at(1, ENOENT);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_compat_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
    // The tolerated ENOENT must reach the full probe, not just the cleanup
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 3. Compat open fails with EACCES -> no probe, no close, no cleanup unlink
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_open_compat_at(1, EACCES);
    EXPECT_EQ(OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
    EXPECT_EQ(1, ops.unlink_call_count_);
  }

  // 4. Compat open fails with EINVAL (e.g. O_DIRECT rejected) ->
  //    OB_NOT_SUPPORTED, no deeper fallback
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_open_compat_at(1, EINVAL);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(0, ops.fallocate_call_count_);
  }

  // 4.1 Compat open fails with EIO -> hard error, no probe executed
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_open_compat_at(1, EIO);
    EXPECT_EQ(OB_IO_ERROR, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
    EXPECT_EQ(1, ops.unlink_call_count_);
  }

  // 4.2 Compat open fails with EMFILE (fd exhaustion) -> hard error via
  //     convert_sys_errno (OB_TOO_MANY_OPEN_FILES), no probe, no fallback
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_open_compat_at(1, EMFILE);
    EXPECT_EQ(OB_TOO_MANY_OPEN_FILES, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
    EXPECT_EQ(1, ops.unlink_call_count_); // stale cleanup only
  }

  // 4.3 Stale unlink fails with EINVAL: close/unlink ops follow the plain
  //     convert_sys_errno mapping (OB_IO_ERROR), NOT the not-supported
  //     folding used by the open ops
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_unlink_at(1, EINVAL);
    EXPECT_EQ(OB_IO_ERROR, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(1, ops.unlink_call_count_);
  }

  // 4.4 Stale unlink fails with ENOSPC (full disk cannot delete the leftover
  //     probe file) -> skipped like the other space errors (final SUCCESS):
  //     the probe passes unverified and must NOT exclusively open the
  //     undeletable file
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_unlink_at(1, ENOSPC);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.unlink_call_count_);
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.close_call_count_);
  }

  // 5. Primary open hard error (EIO) must NOT trigger the fallback
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, EIO);
    EXPECT_EQ(OB_IO_ERROR, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
    EXPECT_EQ(0, ops.unlink_call_count_);
  }

  // 6. Primary open space error must NOT trigger the fallback either
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_at(1, ENOSPC);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(0, ops.open_compat_call_count_);
    EXPECT_EQ(0, ops.fallocate_call_count_);
  }
}

TEST_F(TestFallocateProbe, compat_cleanup_failures_and_first_error_preserved)
{
  // 1. close fails -> error propagates, cleanup unlink still executed
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_close_at(1, EACCES);
    EXPECT_EQ(OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 2. cleanup unlink fails -> error
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_unlink_at(2, EACCES);
    EXPECT_EQ(OB_FILE_OR_DIRECTORY_PERMISSION_DENIED,
              check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 3. First error preserved: fallocate EIO wins over close EACCES
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(1, EIO);
    ops.fail_close_at(1, EACCES);
    EXPECT_EQ(OB_IO_ERROR, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.close_call_count_);
  }

  // 4. First error preserved: fallocate EOPNOTSUPP wins over unlink EACCES
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    ops.fail_unlink_at(2, EACCES);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 5. ENOSPC skip decision survives a close failure: skipping is a deliberate
  //    pass decision, a later cleanup error must not overturn it
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(1, ENOSPC);
    ops.fail_close_at(1, EIO);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 6. ENOSPC skip decision survives a cleanup-unlink failure
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(1, ENOSPC);
    ops.fail_unlink_at(2, EACCES);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 7. Cleanup unlink misses the file (ENOENT) -> tolerated, probe passed
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_unlink_at(2, ENOENT);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 8. close reports ENOENT -> tolerated by the unified cleanup tolerance rule
  {
    FakeFallocateProbeOps ops;
    ops.fail_close_at(1, ENOENT);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(2, ops.fallocate_call_count_);
  }

  // 9. close fails with EINVAL -> plain convert_sys_errno mapping, hard error
  //    OB_IO_ERROR (NOT the not-supported folding of the open ops)
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_close_at(1, EINVAL);
    EXPECT_EQ(OB_IO_ERROR, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

}

// The disk-space policy at the compat cleanup phase: a space error skips
// the probe (final SUCCESS), but must never launder a pending probe
// verdict into a false pass at the two cleanup merges.
TEST_F(TestFallocateProbe, compat_cleanup_space_error_policy)
{
  // 1. Probe succeeded, cleanup close reports ENOSPC (the NFS deferred-error
  //    slot per close(2)) -> the space skip covers the cleanup phase (stale
  //    unlink already ran as call 1)
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_close_at(1, ENOSPC);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 2. Probe succeeded, cleanup unlink (call 2) reports ENOSPC -> same skip
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_unlink_at(2, ENOSPC);
    EXPECT_EQ(OB_SUCCESS, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(2, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 3. Anti-laundering at the cleanup-close merge: the EOPNOTSUPP probe
  //    verdict must survive a space error at close instead of being
  //    swallowed into a false pass
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    ops.fail_close_at(1, ENOSPC);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }

  // 4. Same guard at the cleanup-unlink merge: the probe verdict must
  //    survive a space error at unlink (call 2) as well
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    ops.fail_unlink_at(2, ENOSPC);
    EXPECT_EQ(OB_NOT_SUPPORTED, check_clog_capability_with_ops("/fake/clog", ops));
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }
}

TEST_F(TestFallocateProbe, compat_fallback_unavailable_without_compat_ops)
{
  FakeFallocateProbeOps ops;
  ops.fail_open_always(EOPNOTSUPP);
  const ObFallocateProbeOps tmpfile_only_ops = make_tmpfile_only_probe_ops(ops);
  ObFallocateProbeOpsGuard guard(tmpfile_only_ops);

  EXPECT_EQ(OB_NOT_SUPPORTED,
            palf::check_file_system_fallocate_capability("/fake/clog"));
  EXPECT_EQ(1, ops.open_call_count_);
  EXPECT_EQ(0, ops.open_compat_call_count_);
  EXPECT_EQ(0, ops.fallocate_call_count_);
  EXPECT_EQ(0, ops.close_call_count_);
  EXPECT_EQ(0, ops.unlink_call_count_);
}

TEST_F(TestFallocateProbe, compat_mode_real_filesystem_lifecycle)
{
  if (!compat_probe_env_ok()) {
    SKIP_REAL_PROBE_TEST("environment lacks named-file fallocate support or free space for real compat probe");
  }
  const std::string clog_dir = path("clog_compat");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  const std::string probe_file = clog_dir + "/.ob_fallocate_probe_clog.tmp";
  const std::string business_log = clog_dir + "/business_log_1";
  const char log_content[] = "business log content";
  // Leftover named probe file from a previous crashed compat probe
  create_file(probe_file, "stale probe content");
  create_file(business_log, log_content);

  TmpfileUnsupportedProbeOps ops;
  const int64_t fd_count_before = count_open_fds();
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops(clog_dir.c_str(), ops));

  ASSERT_EQ(1U, ops.open_paths_.size());
  EXPECT_EQ(clog_dir, ops.open_paths_.at(0));
  ASSERT_EQ(1U, ops.open_compat_paths_.size());
  EXPECT_EQ(probe_file, ops.open_compat_paths_.at(0));
  ASSERT_EQ(2U, ops.unlink_paths_.size());
  EXPECT_EQ(probe_file, ops.unlink_paths_.at(0));
  EXPECT_EQ(probe_file, ops.unlink_paths_.at(1));
  ASSERT_EQ(2U, ops.fallocate_calls_.size());
  EXPECT_EQ(0, ops.fallocate_calls_.at(0).mode_);
  EXPECT_EQ(FALLOC_FL_ZERO_RANGE, ops.fallocate_calls_.at(1).mode_);
  // The opened probe file was really closed on the real path
  ASSERT_EQ(1U, ops.close_fds_.size());
  EXPECT_GE(ops.close_fds_.at(0), 0);
  // No fd leaked by the compat probe run
  if (fd_count_before >= 0) {
    EXPECT_EQ(fd_count_before, count_open_fds());
  }

  // Stale probe file was removed first and the fresh probe file was unlinked
  errno = 0;
  EXPECT_EQ(-1, ::access(probe_file.c_str(), F_OK));
  EXPECT_EQ(ENOENT, errno);
  // Business files remain untouched
  expect_file_content(business_log, log_content);
}

TEST_F(TestFallocateProbe, compat_fallback_fresh_directory_still_probes)
{
  if (!compat_probe_env_ok()) {
    SKIP_REAL_PROBE_TEST("environment lacks named-file fallocate support or free space for real compat probe");
  }
  // Regression pin: on a clean directory the stale-file unlink misses with
  // ENOENT; the tolerated ENOENT must fall through to the exclusive open.
  // An earlier restructuring consumed the branch chain at this point and the
  // compat probe silently became a no-op that always reported success.
  const std::string clog_dir = path("clog_compat_fresh");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  TmpfileUnsupportedProbeOps ops;
  ASSERT_EQ(OB_SUCCESS, check_clog_capability_with_ops(clog_dir.c_str(), ops));

  ASSERT_EQ(1U, ops.open_compat_paths_.size());
  ASSERT_EQ(2U, ops.fallocate_calls_.size());
  EXPECT_EQ(0, ops.fallocate_calls_.at(0).mode_);
  EXPECT_EQ(FALLOC_FL_ZERO_RANGE, ops.fallocate_calls_.at(1).mode_);
  EXPECT_EQ(palf::LOG_DIO_ALIGN_SIZE, ops.fallocate_calls_.at(0).len_);

  const std::string probe_file = clog_dir + "/.ob_fallocate_probe_clog.tmp";
  errno = 0;
  EXPECT_EQ(-1, ::access(probe_file.c_str(), F_OK));
  EXPECT_EQ(ENOENT, errno);
}

TEST_F(TestFallocateProbe, posix_compat_probe_ops_handle_errors)
{
  int fd = -1;
  // 1. Invalid arguments (pure API check: needs no named-file fallocate support
  //    and must still run on environments where the real sub-cases are skipped)
  EXPECT_EQ(OB_INVALID_ARGUMENT, palf::posix_probe_open_compat(nullptr, fd));
  EXPECT_EQ(OB_INVALID_ARGUMENT, palf::posix_probe_unlink(nullptr));

  if (!compat_probe_env_ok()) {
    SKIP_REAL_PROBE_TEST("environment lacks named-file fallocate support for real compat ops");
  }
  // 2. Compat open on non-existent directory
  EXPECT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY,
            palf::posix_probe_open_compat(path("no_such_dir/probe.tmp").c_str(), fd));

  // 3. Exclusive creation: second open of the same path fails with EEXIST
  const std::string probe_file = path("compat_probe.tmp");
  EXPECT_EQ(OB_SUCCESS, palf::posix_probe_open_compat(probe_file.c_str(), fd));
  EXPECT_GE(fd, 0);
  int fd2 = -1;
  EXPECT_EQ(OB_FILE_OR_DIRECTORY_EXIST, palf::posix_probe_open_compat(probe_file.c_str(), fd2));
  EXPECT_EQ(OB_SUCCESS, palf::posix_probe_fallocate(fd, 0, 0, 4096));
  EXPECT_EQ(OB_SUCCESS, palf::posix_probe_close(fd));

  // 4. Unlink: success, then ENOENT reported to the caller
  EXPECT_EQ(OB_SUCCESS, palf::posix_probe_unlink(probe_file.c_str()));
  EXPECT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, palf::posix_probe_unlink(probe_file.c_str()));
}

TEST_F(TestFallocateProbe, server_log_block_mgr_compat_fallback)
{
  const std::string clog_dir = path("clog_disk_compat");
  ASSERT_EQ(0, ::mkdir(clog_dir.c_str(), 0755));

  // 1. NFS-like environment: O_TMPFILE unsupported, named-file probe succeeds
  //    (if-guarded instead of SKIP_REAL_PROBE_TEST so the pure-mock sub-case 2
  //    below still runs on environments without named-file fallocate support)
  if (compat_probe_env_ok()) {
    TmpfileUnsupportedProbeOps ops;
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    ASSERT_EQ(OB_SUCCESS, log_block_mgr.init(clog_dir.c_str()));
    ASSERT_EQ(1U, ops.open_compat_paths_.size());
    EXPECT_EQ(clog_dir + "/log_pool/.ob_fallocate_probe_clog.tmp",
              ops.open_compat_paths_.at(0));
    ASSERT_EQ(2U, ops.unlink_paths_.size());
    // The compat probe really executed both required fallocate modes
    ASSERT_EQ(2U, ops.fallocate_calls_.size());
    EXPECT_EQ(0, ops.fallocate_calls_.at(0).mode_);
    EXPECT_EQ(FALLOC_FL_ZERO_RANGE, ops.fallocate_calls_.at(1).mode_);
    // No probe file left behind in log_pool
    const std::string probe_file = clog_dir + "/log_pool/.ob_fallocate_probe_clog.tmp";
    errno = 0;
    EXPECT_EQ(-1, ::access(probe_file.c_str(), F_OK));
    EXPECT_EQ(ENOENT, errno);
    log_block_mgr.destroy();
  } else {
    fprintf(stderr, "[  SKIPPED  ] real compat init sub-case: environment lacks named-file fallocate support or free space\n");
  }

  // 2. Compat probe detects unsupported fallocate -> init fails
  {
    FakeFallocateProbeOps ops;
    ops.fail_open_always(EOPNOTSUPP);
    ops.fail_fallocate_at(1, EOPNOTSUPP);
    const ObFallocateProbeOps probe_ops = make_probe_ops(ops);
    palf::ObFallocateProbeOpsGuard guard(probe_ops);

    logservice::ObServerLogBlockMgr log_block_mgr;
    EXPECT_EQ(OB_NOT_SUPPORTED, log_block_mgr.init(clog_dir.c_str()));
    EXPECT_FALSE(log_block_mgr.is_reserved());
    EXPECT_EQ(1, ops.open_call_count_);
    EXPECT_EQ(1, ops.open_compat_call_count_);
    EXPECT_EQ(1, ops.fallocate_call_count_);
    EXPECT_EQ(1, ops.close_call_count_);
    EXPECT_EQ(2, ops.unlink_call_count_);
  }
}

} // end namespace palf
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_fallocate_probe.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_fallocate_probe");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
