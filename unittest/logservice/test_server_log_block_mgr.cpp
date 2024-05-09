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
#include "lib/file/file_directory_utils.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_abort.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_utility.h"
#include "logservice/palf/log_block_pool_interface.h"
#include "logservice/palf/log_define.h"
#include "share/ob_errno.h"
#include <thread>
#include <functional>

#define private public
#include "logservice/ob_server_log_block_mgr.h"
#undef private

#include <gtest/gtest.h>
#include <regex>

namespace oceanbase
{
using namespace logservice;
using namespace common;
using namespace palf;
namespace unittest
{
static const int64_t DEFAULT_LS_COUNT = 10;
static int ls_id_array_[DEFAULT_LS_COUNT] = {1,    1001, 1002, 1003, 1004,
                                             1005, 1006, 1007, 1008, 1009};
static const char *log_or_meta_[2] = {"log", "meta"};
class TestServerLogBlockMgr : public ::testing::Test
{
public:
  static void SetUpTestCase();

  static void TearDownTestCase();
  TestServerLogBlockMgr()
  {}
  virtual ~TestServerLogBlockMgr()
  {}

  static int create_ls_in_tenant(const int ls_count, const char *tenant_dir);

  static int remove_ls_in_tenant(const char *tenant_dir);

  int create_new_blocks_at(const int64_t ls_id, const palf::FileDesc &fd,
                           const palf::block_id_t start_block_id, const int64_t block_cnt)
  {
    int ret = OB_SUCCESS;
    char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    for (int i = 0; i < block_cnt && OB_SUCC(ret); i++) {
      char create_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      int64_t pos = 0;
      bool result = false;
      const palf::block_id_t block_id = start_block_id + i;
      databuff_printf(block_path, OB_MAX_FILE_NAME_LENGTH, pos, "%s/%lu/%s/%lu",
                      tenant_string_, ls_id, log_or_meta_[ls_id % 2], block_id);
      if (OB_FAIL(block_id_to_string(block_id, create_block_path, OB_MAX_FILE_NAME_LENGTH))) {
        CLOG_LOG(ERROR, "block_id_to_string failed", K(ret));
      } else if (OB_FAIL(log_block_mgr_.create_block_at(fd, create_block_path,
                                                     ObServerLogBlockMgr::BLOCK_SIZE))) {
        CLOG_LOG(ERROR, "create_block_at failed", K(ret), K(ls_id), K(fd),
                 K(start_block_id), K(block_id));
      } else if (OB_FAIL(FileDirectoryUtils::is_exists(block_path, result))) {
        CLOG_LOG(ERROR, "is_exists failed", K(ret), K(block_path), K(result), K(i),
                 K(start_block_id));
      } else if (false == result) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "file not exist, unexpected error", K(ret), K(block_path),
                 K(start_block_id), K(i));
      } else {
      }
    }
    return ret;
  }

  int delete_blocks_at(const int64_t ls_id, const palf::FileDesc &fd,
                       const palf::block_id_t start_block_id, const int64_t block_cnt)
  {
    int ret = OB_SUCCESS;
    char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    for (int i = 0; i < block_cnt && OB_SUCC(ret); i++) {
      char remove_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      int64_t pos = 0;
      bool result = false;
      const palf::block_id_t block_id = start_block_id + i;
      databuff_printf(block_path, OB_MAX_FILE_NAME_LENGTH, pos, "%s/%lu/%s/%lu",
                      tenant_string_, ls_id, log_or_meta_[ls_id % 2], block_id);
      if (OB_FAIL(block_id_to_string(block_id, remove_block_path, OB_MAX_FILE_NAME_LENGTH))) {
        CLOG_LOG(ERROR, "block_id_to_string failed", K(ret));
      } else if (OB_FAIL(log_block_mgr_.remove_block_at(fd, remove_block_path))) {
        CLOG_LOG(ERROR, "delete_new_block_at failed", K(ret), K(ls_id), K(fd),
                 K(start_block_id), K(i));
      } else if (OB_FAIL(FileDirectoryUtils::is_exists(block_path, result))) {
        CLOG_LOG(ERROR, "is_exists failed", K(ret), K(block_path), K(result), K(i),
                 K(start_block_id));
      } else if (true == result) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "file exist, unexpected error", K(ret), K(block_path),
                 K(start_block_id), K(start_block_id), K(i));
      } else {
      }
    }
    return ret;
  }

  static constexpr int64_t GB = 1024 * 1024 * 1024ul;
  int create_tenant(int key, int64_t size) {
    ObSpinLockGuard guard(this->lock_);
    int ret = OB_SUCCESS;
    if (map_.end() != map_.find(key)) {
      ret = OB_ENTRY_EXIST;
    } else {
      ret = this->log_block_mgr_.create_tenant(size);
      if (OB_SUCC(ret)) {
        map_[key] = size;
      } else {
      }
    }
    return ret;
  };
  int remove_tenant(int key) {
    ObSpinLockGuard guard(this->lock_);
    int ret = OB_SUCCESS;
    if (map_.end() == map_.find(key)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      int64_t size = map_[key];
      map_.erase(key);
      this->log_block_mgr_.remove_tenant(size);
    }
    return ret;
  };
  int update_tenant(int key, int64_t new_size) {
    ObSpinLockGuard guard(this->lock_);
    int ret = OB_SUCCESS;
    if (map_.end() == map_.find(key)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      int64_t old_size = map_[key];
      int64_t tmp_min_log_disk_size_for_all_tenants = this->log_block_mgr_.min_log_disk_size_for_all_tenants_;
      tmp_min_log_disk_size_for_all_tenants -= old_size;
      tmp_min_log_disk_size_for_all_tenants += new_size;
      if (tmp_min_log_disk_size_for_all_tenants > this->log_block_mgr_.get_total_size_guarded_by_lock_()) {
        ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      }
      if (OB_SUCCESS != ret) {
        map_[key] = old_size;
      } else {
        this->log_block_mgr_.min_log_disk_size_for_all_tenants_ = tmp_min_log_disk_size_for_all_tenants;
        map_[key] = new_size;
      }
    }
    return ret;
  };
public:
  virtual void SetUp();
  virtual void TearDown();
  static const char *log_pool_base_path_;
  static const char *log_disk_path_;
  static const char *tenant_string_;
  static std::map<int64_t, int> tenant_ls_fd_map_;
  static std::vector<int> tenant_1_ls_fd_;
  ObServerLogBlockMgr log_block_mgr_;
  ObSpinLock lock_;
  std::map<int, int64_t> map_;
};

const char *TestServerLogBlockMgr::log_pool_base_path_ = "clog_disk/clog";
const char *TestServerLogBlockMgr::tenant_string_ = "clog_disk/clog/tenant_1";
std::map<int64_t, int> TestServerLogBlockMgr::tenant_ls_fd_map_;
std::vector<int> TestServerLogBlockMgr::tenant_1_ls_fd_;

void TestServerLogBlockMgr::SetUpTestCase()
{
  bool result = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(FileDirectoryUtils::create_directory(tenant_string_))) {
    CLOG_LOG(ERROR, "FileDirectoryUtils create_directory failed", K(ret),
             K(tenant_string_));
  } else if (OB_FAIL(create_ls_in_tenant(DEFAULT_LS_COUNT, tenant_string_))) {
    CLOG_LOG(ERROR, "create_ls_in_tenant failed", K(ret));
  } else {
    CLOG_LOG(INFO, "SetUpTestSuite success", K(log_pool_base_path_));
  }
}

void TestServerLogBlockMgr::TearDownTestCase()
{
  remove_ls_in_tenant(tenant_string_);
  system("rm -rf clog_disk");
}

int TestServerLogBlockMgr::create_ls_in_tenant(const int ls_count, const char *tenant_dir)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < ls_count && OB_SUCC(ret); i++) {
    char ls_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    int fd = -1;
    snprintf(ls_path, OB_MAX_FILE_NAME_LENGTH, "%s/%d", tenant_dir, ls_id_array_[i]);
    if (-1 == ::mkdir(ls_path, ObServerLogBlockMgr::CREATE_DIR_MODE)) {
      ret = palf::convert_sys_errno();
      CLOG_LOG(ERROR, "::mkdir failed", K(ret));
    } else if (FALSE_IT(snprintf(ls_path, OB_MAX_FILE_NAME_LENGTH, "%s/%d/%s", tenant_dir,
                                 ls_id_array_[i], log_or_meta_[ls_id_array_[i] % 2]))
               || -1 == ::mkdir(ls_path, ObServerLogBlockMgr::CREATE_DIR_MODE)) {
      ret = palf::convert_sys_errno();
      CLOG_LOG(ERROR, "::mkdir failed", K(ret));
    } else if (-1 == (fd = ::open(ls_path, ObServerLogBlockMgr::OPEN_DIR_FLAG))) {
      ret = palf::convert_sys_errno();
      CLOG_LOG(ERROR, "::open failed", K(ret));
    } else {
      tenant_ls_fd_map_.insert(std::make_pair(ls_id_array_[i], fd));
    }
  }
  return ret;
}

int TestServerLogBlockMgr::remove_ls_in_tenant(const char *tenant_dir)
{
  int ret = OB_SUCCESS;
  for (auto iter = tenant_ls_fd_map_.begin(); iter != tenant_ls_fd_map_.end(); iter++) {
    char ls_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    snprintf(ls_path, OB_MAX_FILE_NAME_LENGTH, "%s/%lu", tenant_dir, iter->first);
    if (-1 == FileDirectoryUtils::delete_directory_rec(ls_path)) {
      ret = palf::convert_sys_errno();
      CLOG_LOG(ERROR, "::rmdir failed", K(ret), K(ls_path));
    } else if (-1 == ::close(iter->second)) {
      ret = palf::convert_sys_errno();
      CLOG_LOG(ERROR, "::close failed", K(ret), K(ls_path));
    } else {
    }
  }
  return ret;
}

void TestServerLogBlockMgr::SetUp()
{
  OB_ASSERT(OB_SUCCESS == log_block_mgr_.init(log_pool_base_path_));
  log_block_mgr_.get_tenants_log_disk_size_func_ = [this](int64_t &out) -> int
  {
    for(auto pair : map_)
    {
      out += pair.second;
      CLOG_LOG(INFO, "current pair", K(pair.first), K(pair.second));
    }
    CLOG_LOG(INFO, "get_tenants_log_disk_size_func_ success", K(out), K(log_block_mgr_));
    return OB_SUCCESS;
  };
}

void TestServerLogBlockMgr::TearDown()
{
  log_block_mgr_.destroy();
}

using namespace palf;
TEST_F(TestServerLogBlockMgr, basic_func)
{
  const int64_t reserved_size = 2 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  // resize
  EXPECT_EQ(0, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.start(aligned_reserved_size));
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(2 * aligned_reserved_size));
  EXPECT_EQ(2 * aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(aligned_reserved_size));
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);

  const int64_t ls_id = 1;
  EXPECT_EQ(OB_SUCCESS, create_new_blocks_at(ls_id, tenant_ls_fd_map_[ls_id], 0, 10));
  int64_t in_use_size_byte, total_size_byte;
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_disk_usage(in_use_size_byte, total_size_byte));
  EXPECT_EQ(10*ObServerLogBlockMgr::BLOCK_SIZE, in_use_size_byte);
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_fd_map_[ls_id], 0, 10));
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_disk_usage(in_use_size_byte, total_size_byte));
  EXPECT_EQ(0, in_use_size_byte);
}

TEST_F(TestServerLogBlockMgr, restart_for_empty_log_disk)
{
  log_block_mgr_.destroy();
  const int64_t reserved_size = 2 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  int64_t in_use_size_byte, total_size_byte;
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.next_total_size_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_disk_usage(in_use_size_byte, total_size_byte));
  EXPECT_EQ(0, in_use_size_byte);
  EXPECT_EQ(0, log_block_mgr_.log_pool_meta_.status_);
}

TEST_F(TestServerLogBlockMgr, allocate_blocks_in_tenant)
{
  const int64_t ls_id = 1001;
  EXPECT_EQ(OB_SUCCESS, create_new_blocks_at(ls_id, tenant_ls_fd_map_[ls_id], 0, 10));
  EXPECT_EQ(OB_SUCCESS,
            create_new_blocks_at(ls_id + 1, tenant_ls_fd_map_[ls_id + 1], 0, 5));
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_fd_map_[ls_id], 0, 3));
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id + 1, tenant_ls_fd_map_[ls_id + 1], 0, 3));
}

TEST_F(TestServerLogBlockMgr, restart_for_non_empty_log_disk)
{
  log_block_mgr_.destroy();
  const int64_t reserved_size = 2 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.next_total_size_);
  EXPECT_EQ(0, log_block_mgr_.log_pool_meta_.status_);
  const int64_t ls_id = 1001;
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_fd_map_[ls_id], 3, 7));
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id + 1, tenant_ls_fd_map_[ls_id + 1], 3, 2));
}

TEST_F(TestServerLogBlockMgr, concurrent_create_delete_resize)
{
  const int64_t reserved_size = 4 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(aligned_reserved_size));
  const int64_t free_block_size = log_block_mgr_.get_free_size_guarded_by_lock_();
  std::vector<std::thread> threads;
  std::atomic<int64_t> total_allocate_block_count(0);
  auto create_func = [&](int64_t ls_id) -> int {
    int64_t block_cnt = 6;
    total_allocate_block_count += block_cnt;
    return create_new_blocks_at(ls_id, tenant_ls_fd_map_[ls_id], 0, block_cnt);
  };
  auto delete_func = [&](int64_t ls_id) -> int {
    int64_t block_cnt = 6;
    create_new_blocks_at(ls_id, tenant_ls_fd_map_[ls_id], 0, block_cnt);
    total_allocate_block_count += 2;
    return delete_blocks_at(ls_id, tenant_ls_fd_map_[ls_id], 0, block_cnt-2);
  };
  std::vector< std::function<int(int64_t) > > funcs;
  funcs.push_back(create_func);
  funcs.push_back(delete_func);

  std::thread resize_thread([&]() -> int
  {
    const int64_t reserved_size = 10 * ObServerLogBlockMgr::GB;
    const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
    return log_block_mgr_.resize_(reserved_size);
  });

  for (int i = 0; i < DEFAULT_LS_COUNT; i++) {
    threads.emplace_back(std::thread(funcs[i%2], ls_id_array_[i]));
  }

  for (int i = 0; i < DEFAULT_LS_COUNT; i++) {
    threads[i].join();
  }
  resize_thread.join();

  const int64_t curr_free_zie = log_block_mgr_.get_free_size_guarded_by_lock_();
  int64_t total_allocate_block_count_int = total_allocate_block_count;
  const int64_t total_block_size = log_block_mgr_.log_pool_meta_.curr_total_size_;
  EXPECT_EQ(total_allocate_block_count_int*ObServerLogBlockMgr::BLOCK_SIZE + curr_free_zie, total_block_size);
  int64_t ls_id = ls_id_array_[0];
  EXPECT_EQ(OB_SUCCESS, create_new_blocks_at(ls_id, tenant_ls_fd_map_[ls_id], 6, 3));
}

TEST_F(TestServerLogBlockMgr, dirty_ls_dir_and_log_pool_file)
{
  system("mkdir clog_disk/clog/tenant_0111");
  system("mkdir clog_disk/clog/tenant_0111/log");
  system("touch clog_disk/clog/tenant_0111/log/0");
  system("touch clog_disk/clog/tenant_0111/log/1");
  system("touch clog_disk/clog/log_pool/0.tmp");
  system("touch clog_disk/clog/tenant_1/1/meta/10000.tmp");
  system("fallocate -l 67108863 clog_disk/clog/tenant_1/1/meta/10000.tmp ");
  system("mkdir clog_disk/clog/log_pool/1.tmp");
  log_block_mgr_.destroy();
  bool result = false;
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists("clog_disk/clog/tenant_1/1/meta/10000.tmp",result));
  EXPECT_EQ(false, result);
  system("rm -rf clog_disk/clog/tenant_0111");
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
}

TEST_F(TestServerLogBlockMgr, resize_failed_and_restar)
{
  const int64_t reserved_size = 15 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  ObServerLogBlockMgr::LogPoolMeta meta = log_block_mgr_.get_log_pool_meta_guarded_by_lock_();
  meta.status_ = ObServerLogBlockMgr::EXPANDING_STATUS;
  meta.next_total_size_ = aligned_reserved_size;
  log_block_mgr_.update_log_pool_meta_guarded_by_lock_(meta);
  // 触发trim
  system("touch clog_disk/clog/log_pool/100050");
  system("touch clog_disk/clog/log_pool/100000");

  log_block_mgr_.destroy();
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));

  EXPECT_EQ(false, log_block_mgr_.log_pool_meta_.resizing());
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
}

// This case will cause disk space not enough, ignore it.
//TEST_F(TestServerLogBlockMgr, expand_when_disk_space_not_enough)
//{
//  const int64_t reserved_size = 100 * 1024  * ObServerLogBlockMgr::GB;
//  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
//  ObServerLogBlockMgr::LogPoolMeta origin_meta = log_block_mgr_.get_log_pool_meta_guarded_by_lock_();
//  EXPECT_EQ(OB_ALLOCATE_DISK_SPACE_FAILED, log_block_mgr_.resize_(aligned_reserved_size));
//  EXPECT_EQ(origin_meta, log_block_mgr_.get_log_pool_meta_guarded_by_lock_());
//}

// TEST_F(TestServerLogBlockMgr, performance)
// {
//   const int64_t reserved_size = 2 * ObServerLogBlockMgr::GB;
//   const int64_t free_size = log_block_mgr_.get_free_size_guarded_by_lock_();
//   const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
//   EXPECT_EQ(OB_NOT_SUPPORTED, log_block_mgr_.resize_(aligned_reserved_size));
//   EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(log_block_mgr_.get_total_size_guarded_by_lock_() - free_size));
//  EXPECT_EQ(OB_ENTRY_NOT_EXIST, create_new_blocks_at(1, tenant_ls_fd_map_[1], 100000, 10));
//
//   system("mkdir clog_disk/tmp.dir");
//   const int64_t reserved_size1 = 1024 * ObServerLogBlockMgr::GB;
//   const int64_t aligned_reserved_size1 = log_block_mgr_.lower_align_(reserved_size1);
//   int64_t start_ts = ObTimeUtility::current_time();
//   int fd = ::open("clog_disk/tmp.dir", ObServerLogBlockMgr::OPEN_DIR_FLAG);
//   EXPECT_EQ(OB_SUCCESS, log_block_mgr_.allocate_blocks_at_tmp_dir_(fd, 0, aligned_reserved_size1/ObServerLogBlockMgr::BLOCK_SIZE));
//   int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
//   ::close(fd);
//   EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::delete_directory_rec("clog_disk/tmp.dir"));
//   CLOG_LOG(INFO, "allocate cost", K(cost_ts));
//   EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(aligned_reserved_size1));
// }

TEST_F(TestServerLogBlockMgr, check_dir_is_empty)
{
  system("mkdir clog_disk/test");
  system("mkdir clog_disk/test/log_pool");
  bool result = false;
  EXPECT_EQ(OB_SUCCESS, ObServerLogBlockMgr::check_clog_directory_is_empty("clog_disk/test", result));
  EXPECT_EQ(true, result);
  system("mkdir clog_disk/test/log_pool1");
  EXPECT_EQ(OB_SUCCESS, ObServerLogBlockMgr::check_clog_directory_is_empty("clog_disk/test", result));
  EXPECT_EQ(false, result);
}
TEST_F(TestServerLogBlockMgr, create_tenant_resize)
{
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(4 * GB));
  EXPECT_EQ(0, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(OB_SUCCESS, create_tenant(OB_MAX_RESERVED_TENANT_ID + 1, 2 * GB));
  EXPECT_EQ(OB_SUCCESS, create_tenant(OB_MAX_RESERVED_TENANT_ID + 2, 2 * GB));
  EXPECT_EQ(OB_MACHINE_RESOURCE_NOT_ENOUGH, create_tenant(OB_MAX_RESERVED_TENANT_ID + 3, 2 * GB));
  EXPECT_EQ(OB_SUCCESS, remove_tenant(OB_MAX_RESERVED_TENANT_ID + 1));
  EXPECT_EQ(2 * GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(OB_SUCCESS, update_tenant(OB_MAX_RESERVED_TENANT_ID + 2, 3 * GB));
  EXPECT_EQ(3 * GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, update_tenant(OB_MAX_RESERVED_TENANT_ID + 1, 3 * GB));
  EXPECT_EQ(OB_MACHINE_RESOURCE_NOT_ENOUGH, update_tenant(OB_MAX_RESERVED_TENANT_ID + 2, 5 * GB));
  EXPECT_EQ(3 * GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(10 * GB));
  EXPECT_EQ(3 * GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(10 * GB, log_block_mgr_.get_total_size_guarded_by_lock_());
  constexpr int64_t MB = 1024*1024;
  std::thread t_create1(
    [&](){
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        create_tenant(i, 32*MB);
      };
    }
  );
  std::thread t_create2(
    [&](){
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        create_tenant(i, 32*MB);
      };
    }
  );
  std::thread t_update1(
    [&](){
      usleep(10);
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        update_tenant(i, 128*MB);
      };
    }
  );
  std::thread t_update2(
    [&](){
      usleep(10);
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        update_tenant(i, 128*MB);
      };
    }
  );
  t_create1.join();
  t_create2.join();
  t_update1.join();
  t_update2.join();
  EXPECT_EQ(10*GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  std::thread t_remove1(
    [&](){
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        remove_tenant(i);
      };
    }
  );
  std::thread t_remove2(
    [&](){
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        remove_tenant(i);
      };
    }
  );
  t_remove1.join();
  t_remove2.join();
  EXPECT_EQ(3*GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
}

TEST_F(TestServerLogBlockMgr, resize_log_loop_and_restart)
{
  ObServerLogBlockMgr::LogPoolMeta meta;
  meta.curr_total_size_ = log_block_mgr_.log_pool_meta_.curr_total_size_;
  int64_t next_total_size = meta.next_total_size_ = meta.curr_total_size_ + 1*1024*1024*1024;
  meta.status_ = ObServerLogBlockMgr::EXPANDING_STATUS;
  log_block_mgr_.update_log_pool_meta_guarded_by_lock_(meta);
  char tmp_dir_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  snprintf(tmp_dir_path, OB_MAX_FILE_NAME_LENGTH, "%s/expanding.tmp", log_block_mgr_.log_pool_path_);
  int tmp_dir_fd = -1;
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.make_resizing_tmp_dir_(tmp_dir_path, tmp_dir_fd));
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.remove_resizing_tmp_dir_(tmp_dir_path, tmp_dir_fd));
  char file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  auto touch_file_in_log_pool = [](const char *file_path) {
    char cmd_touch[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    char cmd_fallocate[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    snprintf(cmd_touch, OB_MAX_FILE_NAME_LENGTH, "touch %s", file_path);
    snprintf(cmd_fallocate, OB_MAX_FILE_NAME_LENGTH, "fallocate -l %lu %s", PALF_PHY_BLOCK_SIZE, file_path);
    system(cmd_touch);
    system(cmd_fallocate);
  };
  for (int i = 0; i < 10; i++) {
    int64_t file_id = i + 10000;
    snprintf(file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%ld", log_block_mgr_.log_pool_path_, file_id);
    touch_file_in_log_pool(file_path);
  }
  log_block_mgr_.destroy();
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(ObServerLogBlockMgr::NORMAL_STATUS, log_block_mgr_.log_pool_meta_.status_);
  EXPECT_EQ(next_total_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
}

class DummyBlockPool : public palf::ILogBlockPool {
public:
  virtual int create_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path,
                              const int64_t block_size)
  {
    if (-1 == ::openat(dir_fd, block_path, palf::LOG_WRITE_FLAG | O_CREAT)) {
      return OB_IO_ERROR;
    }
    return OB_SUCCESS;
  }
  virtual int remove_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path)
  {
    if (-1 == ::unlinkat(dir_fd, block_path, 0)) {
      return OB_IO_ERROR;
    }
    return OB_SUCCESS;
  }
};

TEST_F(TestServerLogBlockMgr, basic_func_test)
{
  const char *test_path = "clog_disk/basic_func_test";
  const char *file_path_obs= "1.tmp";
  const char *file_path = "clog_disk/basic_func_test/1.tmp";
  char cmd_mkdir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char cmd_touch[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char cmd_fallocate[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  snprintf(cmd_mkdir, OB_MAX_FILE_NAME_LENGTH, "mkdir %s", test_path);
  snprintf(cmd_touch, OB_MAX_FILE_NAME_LENGTH, "touch %s", file_path);
  snprintf(cmd_fallocate, OB_MAX_FILE_NAME_LENGTH, "fallocate -l %lu %s", PALF_PHY_BLOCK_SIZE, file_path);
  system(cmd_mkdir);
  system(cmd_touch);
  int dir_fd = ::open(test_path, O_DIRECTORY | O_RDONLY);
  bool result = false;
  EXPECT_EQ(OB_SUCCESS, is_block_used_for_palf(dir_fd, file_path_obs, result));
  EXPECT_EQ(false, result);
  system(cmd_fallocate);
  EXPECT_EQ(OB_SUCCESS, is_block_used_for_palf(dir_fd, file_path_obs, result));
  EXPECT_EQ(true, result);
  DummyBlockPool block_pool;
  EXPECT_EQ(OB_SUCCESS, remove_tmp_file_or_directory_at(test_path, &block_pool));
  EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::is_empty_directory(test_path, result));
  EXPECT_EQ(true, result);
  EXPECT_EQ(OB_SUCCESS, remove_directory_rec("clog_disk", &block_pool));
  EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists("clog_disk", result));
  EXPECT_EQ(false, result);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_server_log_block_mgr.log");
  system("rm -rf clog_disk");
  system("rm -rf clog_disk/clog");
  system("mkdir clog_disk");
  system("mkdir clog_disk/clog");
  OB_LOGGER.set_file_name("test_server_log_block_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_server_log_block_mgr");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
