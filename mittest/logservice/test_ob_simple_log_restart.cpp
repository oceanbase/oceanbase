// owner: anli.hx
// owner group: log

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

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <fstream>
#include <gtest/gtest.h>
#include <signal.h>
#include <stdexcept>
#define private public
#define protected public
#include "env/ob_simple_log_cluster_env.h"
#include "logservice/palf/log_reader_utils.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_io_worker.h"
#include "logservice/palf/log_block_mgr.h"
#undef private
#undef protected
#include "logservice/palf/lsn.h"

const std::string TEST_NAME = "log_restart";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
class TestObSimpleLogClusterRestart : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterRestart() : ObSimpleLogClusterTestEnv()
  {
    int ret = init();
    if (OB_SUCCESS != ret) {
      throw std::runtime_error("TestObSimpleLogClusterLogEngine init failed");
    }
  }
  ~TestObSimpleLogClusterRestart()
  {
    destroy();
  }
  int init()
  {
    return OB_SUCCESS;
  }
  void destroy()
  {}
  int64_t id_;
  PalfHandleImplGuard leader_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
constexpr int64_t timeout_ts_us = 3 * 1000 * 1000;
int64_t log_entry_size = 2 * 1024 * 1024 + 16 * 1024;
bool ObSimpleLogClusterTestBase::need_remote_log_store_ = false;

class GcCrashEnv : public ObSimpleLogClusterTestEnv
{
public:
  void TestBody() override {}
};

// Re-exec each phase so the process that opens the recovered files inherits no PALF memory.
TEST(SharedLogGcCrashDeathTest, pending_writes_recover_after_gc_stall_and_sigkill)
{
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  // A failed child assertion must not unwind the deliberately blocked I/O task.
#define CHECK_CRASH_CHILD(condition) \
  do { \
    if (!(condition)) { \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
      fflush(stderr); \
      _exit(2); \
    } \
  } while (false)
  // Re-executed death-test children share the parent's unique image directory.
  // Keep failed images for diagnosis without colliding with the next invocation.
  bool owns_image_dir = false;
  if (NULL == getenv("OB_TEST_GC_CRASH_IMAGE_DIR")) {
    char image_dir[] = "gc_stall_crash.XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(image_dir));
    ASSERT_EQ(0, setenv("OB_TEST_GC_CRASH_IMAGE_DIR", image_dir, 1));
    owns_image_dir = true;
  }
  DEFER({
    if (owns_image_dir) {
      unsetenv("OB_TEST_GC_CRASH_IMAGE_DIR");
    }
  });
  const std::string image = std::string(getenv("OB_TEST_GC_CRASH_IMAGE_DIR")) + "/image";
  const std::string manifest = image + "/expected";
  const int64_t id = 55001;
  const int64_t timeout_us = 30 * 1000 * 1000L;
  auto checksum_prefix = [](PalfHandleImpl *handle, const LSN &end, uint64_t &checksum,
                            int64_t &count) {
    int ret = OB_SUCCESS;
    checksum = 0;
    count = 0;
    PalfBufferIterator iter;
    if (OB_FAIL(handle->alloc_palf_buffer_iterator(LSN(0), iter))) {
    } else {
      while (OB_SUCCESS == (ret = iter.next())) {
        const char *buf = nullptr;
        int64_t len = 0;
        SCN scn;
        LSN lsn;
        bool raw = false;
        if (OB_FAIL(iter.get_entry(buf, len, scn, lsn, raw))) {
          break;
        } else if (lsn >= end) {
          break;
        } else {
          checksum = ob_crc64(checksum, buf, len);
          ++count;
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    return ret;
  };
  ASSERT_EXIT({
    ObSimpleLogClusterTestBase::member_cnt_ = 3;
    ObSimpleLogClusterTestBase::node_cnt_ = 3;
    // Give both fresh processes the same bounded port range when Farm has not assigned one.
    if (nullptr == getenv("mit_network_start_port")) {
      CHECK_CRASH_CHILD(0 == setenv("mit_network_start_port", "22000", 1));
      CHECK_CRASH_CHILD(0 == setenv("mit_network_port_num", "100", 1));
    }
    CHECK_CRASH_CHILD(OB_SUCCESS == GcCrashEnv::start());
    GcCrashEnv env;
    PalfHandleImplGuard leader;
    int64_t leader_idx = 0;
    CHECK_CRASH_CHILD(OB_SUCCESS == env.create_paxos_group(id, leader_idx, leader));
    CHECK_CRASH_CHILD(OB_SUCCESS == env.submit_log(leader, 8, id, 4096));
    CHECK_CRASH_CHILD(OB_SUCCESS == env.wait_until_has_committed(
        leader, leader.palf_handle_impl_->get_max_lsn()));
    std::vector<PalfHandleImplGuard*> replicas;
    CHECK_CRASH_CHILD(OB_SUCCESS == env.get_cluster_palf_handle_guard(id, replicas));
    for (int64_t i = 0; i < 3; ++i) {
      CHECK_CRASH_CHILD(OB_SUCCESS == env.check_replica_sync(id, &leader, replicas[i], timeout_us));
      PalfEnvImpl *palf_env = replicas[i]->palf_env_impl_;
      palf_env->block_gc_timer_task_.stop();
      palf_env->block_gc_timer_task_.wait();
      ObServerLogBlockMgr *pool = nullptr;
      CHECK_CRASH_CHILD(OB_SUCCESS == env.get_log_pool(i, pool));
      CHECK_CRASH_CHILD(OB_SUCCESS == pool->persist_cross_tenant_reclaim_enabled(true));
    }
    PalfHandleImpl *handle = leader.palf_handle_impl_;
    PalfEnvImpl *palf_env = leader.palf_env_impl_;
    ObTenantEnv::set_tenant(env.get_cluster()[leader_idx]->get_tenant_base());
    const LSN committed = handle->get_end_lsn();
    uint64_t checksum = 0;
    int64_t count = 0;
    CHECK_CRASH_CHILD(OB_SUCCESS == checksum_prefix(handle, committed, checksum, count));
    CHECK_CRASH_CHILD(8 == count);
    // Two healthy followers could otherwise commit the queued entry without the leader's flush.
    env.block_all_net(leader_idx);
    IOTaskCond pending_io(id, handle->log_engine_.palf_epoch_);
    CHECK_CRASH_CHILD(OB_SUCCESS == handle->log_engine_.log_io_worker_->submit_io_task(&pending_io));
    const int64_t deadline = ObTimeUtility::current_time() + timeout_us;
    while (0 == ATOMIC_LOAD(&pending_io.count_) && ObTimeUtility::current_time() < deadline) {
      ob_usleep(1000);
    }
    CHECK_CRASH_CHILD(1 == ATOMIC_LOAD(&pending_io.count_));
    CHECK_CRASH_CHILD(OB_SUCCESS == env.submit_log(leader, 1, id, 4096));
    CHECK_CRASH_CHILD(handle->get_max_lsn() > committed);
    CHECK_CRASH_CHILD(committed == handle->get_end_lsn());

    PalfDiskOptions opts = palf_env->disk_options_wrapper_.get_disk_opts_for_stopping_writing();
    opts.log_disk_throttling_percentage_ = 60;
    opts.log_disk_throttling_maximum_duration_ = 1000 * 1000L;
    CHECK_CRASH_CHILD(OB_SUCCESS == palf_env->disk_options_wrapper_.update_disk_options(opts));
    TenantLogBlockReclaimService &service = palf_env->tenant_reclaim_service_;
    // Seed an outstanding GC round; the process dies with real log writes queued behind I/O.
    service.sync_reclaim_mode(LogReclaimState::ENABLED, true, false);
    service.update_reclaim_snapshot(1, 0);
    CHECK_CRASH_CHILD(OB_SUCCESS == service.apply_reclaim_quota(1));
    const int64_t stall_deadline = ObTimeUtility::current_time() + timeout_us;
    do {
      palf_env->period_calc_disk_usage();
      if (!palf_env->gc_stall_stopped_) { ob_usleep(1000); }
    } while (!palf_env->gc_stall_stopped_ && ObTimeUtility::current_time() < stall_deadline);
    CHECK_CRASH_CHILD(palf_env->gc_stall_stopped_);
    CHECK_CRASH_CHILD(!(palf_env->check_disk_space_enough()));
    PalfAppendOptions append_opts;
    LSN rejected_lsn;
    SCN rejected_scn;
    const char data[] = "write rejected during GC stall";
    CHECK_CRASH_CHILD(OB_LOG_OUTOF_DISK_SPACE == handle->submit_log(
        append_opts, data, sizeof(data), SCN::min_scn(), rejected_lsn, rejected_scn));
    CHECK_CRASH_CHILD(committed == handle->get_end_lsn());

    // Preserve the directories outside the test runner's per-process startup cleanup.
    const int rename_ret = ::rename(TEST_NAME.c_str(), image.c_str());
    if (0 != rename_ret) {
      fprintf(stderr, "rename %s to %s failed: %d (%s)\n",
          TEST_NAME.c_str(), image.c_str(), errno, strerror(errno));
    }
    CHECK_CRASH_CHILD(0 == rename_ret);
    std::ofstream expected(manifest);
    expected << committed.val_ << ' ' << checksum << ' ' << count << '\n';
    for (int64_t i = 0; i < 3; ++i) {
      expected << env.get_cluster()[i]->get_addr().get_port() << '\n';
    }
    expected.close();
    CHECK_CRASH_CHILD(expected.good());
    if (::testing::Test::HasFailure()) { _exit(2); }
    ::kill(::getpid(), SIGKILL);
    _exit(3);
  }, ::testing::KilledBySignal(SIGKILL), "");

  ASSERT_EXIT({
    uint64_t committed_val = 0;
    uint64_t expected_checksum = 0;
    int64_t expected_count = 0;
    int ports[3] = {0};
    std::ifstream expected(manifest);
    expected >> committed_val >> expected_checksum >> expected_count;
    for (int64_t i = 0; i < 3; ++i) { expected >> ports[i]; }
    CHECK_CRASH_CHILD(expected.good());
    expected.close();
    CHECK_CRASH_CHILD(ports[0] > 3);
    // The allocator rounds an environment start to the next multiple of three.
    const std::string start_port = std::to_string(ports[0] - 4);
    CHECK_CRASH_CHILD(0 == setenv("mit_network_start_port", start_port.c_str(), 1));
    CHECK_CRASH_CHILD(0 == setenv("mit_network_port_num", "9", 1));
    for (int64_t i = 0; i < 3; ++i) {
      const std::string dir = "/port_" + std::to_string(ports[i]);
      CHECK_CRASH_CHILD(0 == ::rename((image + dir).c_str(), (TEST_NAME + dir).c_str()));
    }
    ObSimpleLogClusterTestBase::member_cnt_ = 3;
    ObSimpleLogClusterTestBase::node_cnt_ = 3;
    CHECK_CRASH_CHILD(OB_SUCCESS == GcCrashEnv::start());
    GcCrashEnv env;
    PalfHandleImplGuard leader;
    int64_t leader_idx = 0;
    CHECK_CRASH_CHILD(OB_SUCCESS == env.get_leader(id, leader, leader_idx));
    std::vector<PalfHandleImplGuard*> replicas;
    CHECK_CRASH_CHILD(OB_SUCCESS == env.get_cluster_palf_handle_guard(id, replicas));
    for (int64_t i = 0; i < 3; ++i) {
      CHECK_CRASH_CHILD(ports[i] == env.get_cluster()[i]->get_addr().get_port());
      ObServerLogBlockMgr *pool = nullptr;
      CHECK_CRASH_CHILD(OB_SUCCESS == env.get_log_pool(i, pool));
      CHECK_CRASH_CHILD(pool->is_cross_tenant_reclaim_enabled());
      CHECK_CRASH_CHILD(LogReclaimState::DRAINING == pool->reclaim_service_.get_reclaim_state());
      PalfEnvImpl *palf_env = replicas[i]->palf_env_impl_;
      CHECK_CRASH_CHILD(0 == palf_env->tenant_reclaim_service_.pending_blocks_);
      CHECK_CRASH_CHILD(OB_INVALID_TIMESTAMP == palf_env->tenant_reclaim_service_.get_gc_stall_start_ts());
      palf_env->period_calc_disk_usage();
      CHECK_CRASH_CHILD(!(palf_env->gc_stall_stopped_));
      CHECK_CRASH_CHILD(palf_env->check_disk_space_enough());
    }
    // A new append also carries the recovered leader's committed LSN to its followers.
    CHECK_CRASH_CHILD(OB_SUCCESS == env.submit_log(leader, 1, id, 4096));
    CHECK_CRASH_CHILD(OB_SUCCESS == env.wait_until_has_committed(
        leader, leader.palf_handle_impl_->get_max_lsn()));
    for (int64_t i = 0; i < 3; ++i) {
      CHECK_CRASH_CHILD(OB_SUCCESS == env.check_replica_sync(id, &leader, replicas[i], timeout_us));
      PalfHandleImpl *handle = replicas[i]->palf_handle_impl_;
      const LSN committed(committed_val);
      const int64_t deadline = ObTimeUtility::current_time() + timeout_us;
      LSN flushed;
      do {
        handle->sw_.get_max_flushed_end_lsn(flushed);
        if (handle->get_end_lsn() >= committed && flushed >= committed) { break; }
        ob_usleep(1000);
      } while (ObTimeUtility::current_time() < deadline);
      CHECK_CRASH_CHILD(handle->get_end_lsn() >= committed && flushed >= committed);
      uint64_t checksum = 0;
      int64_t count = 0;
      CHECK_CRASH_CHILD(OB_SUCCESS == checksum_prefix(
          replicas[i]->palf_handle_impl_, LSN(committed_val), checksum, count));
      CHECK_CRASH_CHILD(expected_count == count);
      CHECK_CRASH_CHILD(expected_checksum == checksum);
    }
    CHECK_CRASH_CHILD(OB_SUCCESS == env.revert_cluster_palf_handle_guard(replicas));
    leader.reset();
    CHECK_CRASH_CHILD(OB_SUCCESS == GcCrashEnv::close());
    _exit(::testing::Test::HasFailure() ? 2 : 0);
  }, ::testing::ExitedWithCode(0), "");
#undef CHECK_CRASH_CHILD
  EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::delete_directory_rec(image.c_str()));
}

TEST_F(TestObSimpleLogClusterRestart, log_sync_state_is_volatile)
{
  SET_CASE_LOG_FILE(TEST_NAME, "log_sync_state_is_volatile");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  {
    PalfHandleImplGuard leader;
    ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    ASSERT_EQ(OB_SUCCESS, submit_log(leader, 3, id, 4096));
    ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    PalfHandleImpl *handle = leader.palf_handle_impl_;
    ASSERT_EQ(OB_SUCCESS, handle->disable_sync());
    ASSERT_FALSE(handle->is_sync_enabled());
  }
  ASSERT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    IPalfHandleImplGuard guard;
    ASSERT_EQ(OB_SUCCESS, get_cluster()[0]->get_palf_env()->get_palf_handle_impl(id, guard));
    IPalfHandleImpl *handle = guard.get_palf_handle_impl();
    ASSERT_NE(nullptr, handle);
    ASSERT_TRUE(handle->is_sync_enabled());
    ASSERT_EQ(OB_SUCCESS, handle->disable_sync());
    ASSERT_FALSE(handle->is_sync_enabled());
    ASSERT_EQ(OB_SUCCESS, handle->enable_sync());
    ASSERT_TRUE(handle->is_sync_enabled());
  }
}

TEST_F(TestObSimpleLogClusterRestart, read_block_in_flashback)
{
  disable_hot_cache_ = true;
  SET_CASE_LOG_FILE(TEST_NAME, "read_block_in_flashback");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  PalfEnv *palf_env = NULL;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 2 * 32 + 2, id, log_entry_size));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));

  block_id_t min_block_id, max_block_id;
  LogStorage *log_storage = &leader.get_palf_handle_impl()->log_engine_.log_storage_;
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(2, max_block_id);
  SCN scn;
  char block_name_tmp[OB_MAX_FILE_NAME_LENGTH];
  EXPECT_EQ(OB_SUCCESS, construct_absolute_tmp_block_path(log_storage->block_mgr_.log_dir_, max_block_id, OB_MAX_FILE_NAME_LENGTH, block_name_tmp));
  char block_name[OB_MAX_FILE_NAME_LENGTH];
  EXPECT_EQ(OB_SUCCESS, construct_absolute_block_path(log_storage->block_mgr_.log_dir_, max_block_id, OB_MAX_FILE_NAME_LENGTH, block_name));
  ::rename(block_name, block_name_tmp);
  EXPECT_EQ(-1, open(block_name, LOG_READ_FLAG));
  EXPECT_EQ(OB_NEED_RETRY, read_log(leader));
  EXPECT_EQ(OB_NEED_RETRY, log_storage->get_block_min_scn(max_block_id, scn));

  // 测试边界场景，read_log_tail_为文件中间，最后一个文件完全被flashback掉, 此时log_tail_是最后一个文件头
  log_storage->log_tail_ = LSN(2*PALF_BLOCK_SIZE);
  EXPECT_EQ(OB_NEED_RETRY, read_log(leader));
  EXPECT_EQ(OB_NEED_RETRY, log_storage->get_block_min_scn(max_block_id, scn));

  // 测试边界场景，read_log_tail_最后一个文件头，最后一个文件完全被flashback掉
  log_storage->log_tail_ = LSN(2*PALF_BLOCK_SIZE);
  log_storage->readable_log_tail_ = LSN(2*PALF_BLOCK_SIZE);
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, log_storage->get_block_min_scn(max_block_id, scn));

 // 不太好模拟这种场景，考虑引入debug sync
 // // 测试边界场景，readable_log_tail_还没改变前检验是否可读通过，直接读文件时报错文件不存在。
 // log_storage->log_tail_ = LSN(3*PALF_BLOCK_SIZE);
 // log_storage->readable_log_tail_ = LSN(3*PALF_BLOCK_SIZE);
 // // 设置max_block_id_为1是为了构造check_read_out_of_bound返回OB_ERR_OUT_OF_UPPER_BOUND的场景
 // log_storage->block_mgr_.max_block_id_ = 1;
 // // log_storage返回OB_ERR_OUT_OF_UPPER_BOUND, iterator将其转换为OB_ITER_END
 // EXPECT_EQ(OB_ITER_END, read_log(leader));
 // EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, log_storage->get_block_min_scn(max_block_id, scn));
}

TEST_F(TestObSimpleLogClusterRestart, restart_when_first_log_block_is_empty)
{
  SET_CASE_LOG_FILE(TEST_NAME, "restart_when_first_log_block_is_empty");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  // 创建日志流后不写入任何数据
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  // 测试truncate场景
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), leader.palf_handle_impl_->get_max_lsn());
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, log_entry_size));
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.truncate(LSN(0)));
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  LSN rebuild_lsn(2*PALF_BLOCK_SIZE);
  // 测试rebuild场景
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), leader.palf_handle_impl_->get_max_lsn());
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 40, id, log_entry_size));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    PalfBaseInfo base_info;
    base_info.generate_by_default();
    base_info.curr_lsn_ = rebuild_lsn;
    base_info.prev_log_info_.accum_checksum_ = 10000;
    base_info.prev_log_info_.log_id_ = 100;
    base_info.prev_log_info_.lsn_ = rebuild_lsn - 4096;
    base_info.prev_log_info_.log_proposal_id_ = 2;
    base_info.prev_log_info_.scn_ = leader.palf_handle_impl_->get_max_scn();

    leader.palf_handle_impl_->state_mgr_.role_ = FOLLOWER;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->disable_sync());
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->advance_base_info(base_info, true));
    while (leader.palf_handle_impl_->log_engine_.log_storage_.get_end_lsn() != rebuild_lsn) {
      sleep(1);
      PALF_LOG(INFO, "has not finish rebuild", K(leader.palf_handle_impl_->log_engine_.log_storage_));
    }
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  // 测试flashback场景
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(rebuild_lsn, leader.palf_handle_impl_->get_max_lsn());
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, log_entry_size));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    int64_t mode_version;
    switch_append_to_flashback(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->flashback(mode_version, SCN::min_scn(), 10*1000*1000));
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(rebuild_lsn, leader.palf_handle_impl_->get_max_lsn());
    int64_t mode_version;
    switch_flashback_to_append(leader, mode_version);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, log_entry_size));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  }
}

TEST_F(TestObSimpleLogClusterRestart, test_restart)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_restart");
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  char meta_fd[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char log_fd[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ObServerLogBlockMgr *pool = NULL;
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, log_entry_size));
    wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
    LogEngine *log_engine = &leader.palf_handle_impl_->log_engine_;
    char *meta_log_dir = log_engine->log_meta_storage_.block_mgr_.log_dir_;
    char *log_dir = log_engine->log_storage_.block_mgr_.log_dir_;
    EXPECT_EQ(OB_SUCCESS, get_log_pool(leader_idx, pool));
    char *pool_dir = pool->log_pool_path_;
    snprintf(meta_fd, OB_MAX_FILE_NAME_LENGTH, "mv %s/%d %s/%d", meta_log_dir, 0, pool_dir, 10000000);
    snprintf(log_fd, OB_MAX_FILE_NAME_LENGTH, "mv %s/%d %s/%d", log_dir, 0, pool_dir, 100000001);
    system(meta_fd);
  }
  OB_LOGGER.set_log_level("TRACE");
  sleep(1);
  EXPECT_EQ(OB_ERR_UNEXPECTED, restart_paxos_groups());
  system(log_fd);
  PALF_LOG(INFO, "first restart_paxos_groups, after meta dir is empty while log dir is not");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  // 验证切文件过程中宕机重启
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 33, id, log_entry_size));
    wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
    block_id_t min_block_id, max_block_id;
    LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
    LogStorage *meta_storage = &leader.get_palf_handle_impl()->log_engine_.log_meta_storage_;
    EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
    EXPECT_EQ(1, max_block_id);
    // 模拟只switch block，但没有更新manifest, 此时manifest依旧是1, 宕机重启后由于2号文件为空，manifest会被更新为2
    EXPECT_EQ(OB_SUCCESS, log_storage->truncate(LSN(PALF_BLOCK_SIZE)));
    EXPECT_EQ(OB_SUCCESS, log_storage->update_manifest_(1));
    EXPECT_EQ(PALF_BLOCK_SIZE, log_storage->curr_block_writable_size_);
    EXPECT_EQ(1, lsn_2_block(meta_storage->log_block_header_.min_lsn_, PALF_BLOCK_SIZE));
  }
  PALF_LOG(INFO, "second restart_paxos_groups after restart in process of switch block");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    //检查manifest是否为3
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, log_entry_size));
    LogStorage *meta_storage = &leader.get_palf_handle_impl()->log_engine_.log_meta_storage_;
    EXPECT_EQ(2, lsn_2_block(meta_storage->log_block_header_.min_lsn_, PALF_BLOCK_SIZE));
  }
  PALF_LOG(INFO, "third restart_paxos_groups");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  // 验证重启后新建日志流
  {
    PalfHandleImplGuard leader;
    id = ATOMIC_AAF(&palf_id_, 1);
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 66, id, log_entry_size));
    wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
    EXPECT_EQ(OB_ITER_END, read_log(leader));
  }
  // 验证truncate或flashback过程中，修改完manifest后，删除文件前宕机重启（删除1个文件）
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    block_id_t min_block_id, max_block_id;
    // 此时manifest为3
    LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
    LogStorage *meta_storage = &leader.get_palf_handle_impl()->log_engine_.log_meta_storage_;
    EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
    EXPECT_EQ(2, max_block_id);
    EXPECT_EQ(3, lsn_2_block(meta_storage->log_block_header_.min_lsn_, PALF_BLOCK_SIZE));
    // truncate 或 flashback会先更新manifest为2
    EXPECT_EQ(OB_SUCCESS, log_storage->update_manifest_(2));
    EXPECT_EQ(2, lsn_2_block(meta_storage->log_block_header_.min_lsn_, PALF_BLOCK_SIZE));
  }
  PALF_LOG(INFO, "fourth restart_paxos_groups after modify manifest while not delete block");
  // 验证truncate或flashback过程中，修改完manifest后，truncaet/flashback正好将最后一个文件清空
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    block_id_t min_block_id, max_block_id;
    // 此时manifest为2
    LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
    LogStorage *meta_storage = &leader.get_palf_handle_impl()->log_engine_.log_meta_storage_;
    EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
    EXPECT_EQ(2, max_block_id);
    // 尽管manifest为2，但在这种场景下，2号文件是可以删除的
    EXPECT_EQ(2, lsn_2_block(meta_storage->log_block_header_.min_lsn_, PALF_BLOCK_SIZE));
    EXPECT_EQ(OB_SUCCESS, log_storage->truncate(LSN(2*PALF_BLOCK_SIZE)));
    EXPECT_EQ(OB_SUCCESS, log_storage->update_manifest_(2));
  }
  PALF_LOG(INFO, "five restart_paxos_groups after modify manifest and last block is empty");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    block_id_t min_block_id, max_block_id;
    // 重启之后，由于磁盘上最大的文件为2，同时该文件为空，此时会更新manifest为3
    LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
    LogStorage *meta_storage = &leader.get_palf_handle_impl()->log_engine_.log_meta_storage_;
    EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
    EXPECT_EQ(2, max_block_id);
    EXPECT_EQ(3, lsn_2_block(meta_storage->log_block_header_.min_lsn_, PALF_BLOCK_SIZE));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, log_entry_size));
    wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
    EXPECT_EQ(3, lsn_2_block(meta_storage->log_block_header_.min_lsn_, PALF_BLOCK_SIZE));
  }
  PALF_LOG(INFO, "six restart_paxos_groups");
  // 验证base lsn 大于持久化的committed 位点
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
    LogIOWorker *iow = leader.palf_handle_impl_->log_engine_.log_io_worker_;
    int64_t epoch = leader.palf_handle_impl_->log_engine_.palf_epoch_;
    int64_t palf_id = leader.palf_handle_impl_->palf_id_;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 29, id, log_entry_size));
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
    // 预期log_tail接近文件2的尾部
    EXPECT_LE(LSN(3*PALF_BLOCK_SIZE) - log_storage->log_tail_, 5*1024*1024);
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_end_lsn()));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1000));
    wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
    IOTaskConsumeCond cond(palf_id, epoch);
    EXPECT_EQ(OB_SUCCESS, iow->submit_io_task(&cond));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, log_entry_size));
    while (1) {
      if (leader.palf_handle_impl_->sw_.last_submit_end_lsn_ < leader.palf_handle_impl_->get_max_lsn()) {
        usleep(5000);
        leader.palf_handle_impl_->sw_.freeze_mode_ = FEEDBACK_FREEZE_MODE;
        leader.palf_handle_impl_->sw_.feedback_freeze_last_log_();
        PALF_LOG(INFO, "has log in sw", "last_submit_end_lsn", leader.palf_handle_impl_->sw_.last_submit_end_lsn_,
                 "max_lsn", leader.palf_handle_impl_->get_max_lsn());
      } else {
        break;
      }
    }
    cond.cond_.signal();
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
    PALF_LOG(INFO, "after wait_lsn_until_flushed", "end_lsn:", leader.palf_handle_impl_->get_end_lsn(),
             "max_lsn:", leader.palf_handle_impl_->get_end_lsn());
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    EXPECT_GE(leader.palf_handle_impl_->get_max_lsn(), LSN(3*PALF_BLOCK_SIZE));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(LSN(3*PALF_BLOCK_SIZE)));
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  PALF_LOG(INFO, "seven restart_paxos_groups after committed lsn is smaller than base lsn");
  // 验证rebuild过程中持久化palf_base_info后，宕机重启
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    LogStorage *log_storage = &leader.palf_handle_impl_->log_engine_.log_storage_;
    LogIOWorker *iow = leader.palf_handle_impl_->log_engine_.log_io_worker_;
    int64_t epoch = leader.palf_handle_impl_->log_engine_.palf_epoch_;
    int64_t palf_id = leader.palf_handle_impl_->palf_id_;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, log_entry_size));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_end_lsn()));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 1000));
    wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader);
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
    IOTaskConsumeCond cond(palf_id, epoch);
    EXPECT_EQ(OB_SUCCESS, iow->submit_io_task(&cond));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, log_entry_size));
    while (1) {
      if (leader.palf_handle_impl_->sw_.last_submit_end_lsn_ < leader.palf_handle_impl_->get_max_lsn()) {
        usleep(5000);
        leader.palf_handle_impl_->sw_.freeze_mode_ = FEEDBACK_FREEZE_MODE;
        leader.palf_handle_impl_->sw_.feedback_freeze_last_log_();
        PALF_LOG(INFO, "has log in sw", "last_submit_end_lsn", leader.palf_handle_impl_->sw_.last_submit_end_lsn_,
                 "max_lsn", leader.palf_handle_impl_->get_max_lsn());
      } else {
        break;
      }
    }
    cond.cond_.signal();
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
    PALF_LOG(INFO, "after wait_lsn_until_flushed", "end_lsn:", leader.palf_handle_impl_->get_end_lsn(),
             "max_lsn:", leader.palf_handle_impl_->get_end_lsn());
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    EXPECT_GE(leader.palf_handle_impl_->get_max_lsn(), LSN(3*PALF_BLOCK_SIZE));
    PalfBaseInfo base_info;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(
      leader.palf_handle_impl_->get_max_lsn(), base_info));
    LogSnapshotMeta snapshot;
    base_info.prev_log_info_.lsn_ = LSN(10*PALF_BLOCK_SIZE - 10*1024);
    EXPECT_EQ(OB_SUCCESS, snapshot.generate(LSN(10*PALF_BLOCK_SIZE), base_info.prev_log_info_));
    FlushMetaCbCtx meta_ctx;
    meta_ctx.type_ = SNAPSHOT_META;
    meta_ctx.base_lsn_ = snapshot.base_lsn_;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.submit_flush_snapshot_meta_task(meta_ctx, snapshot));
    sleep(2);
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  PALF_LOG(INFO, "seven restart_paxos_groups after committed lsn is smaller than base lsn");
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(LSN(10*PALF_BLOCK_SIZE), leader.palf_handle_impl_->get_max_lsn());
    EXPECT_EQ(LSN(10*PALF_BLOCK_SIZE), leader.palf_handle_impl_->log_engine_.log_storage_.get_end_lsn());
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 1000));
    EXPECT_EQ(OB_SUCCESS, wait_lsn_until_flushed(leader.palf_handle_impl_->get_max_lsn(), leader));
  }
}

TEST_F(TestObSimpleLogClusterRestart, advance_base_lsn_with_restart)
{
  SET_CASE_LOG_FILE(TEST_NAME, "advance_base_lsn_with_restart");
  OB_LOGGER.set_log_level("INFO");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start advance_base_lsn", K(id));
  int64_t leader_idx = 0;
  int64_t log_ts = 1;
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
    sleep(2);
    LSN log_tail =
        leader.palf_handle_impl_->log_engine_.log_meta_storage_.log_tail_;
    int count = (LSN(PALF_META_BLOCK_SIZE) - log_tail)/4096;
    for (int64_t i = 0; i < count; i++) {
      EXPECT_UNTIL_EQ(OB_SUCCESS, leader.palf_handle_impl_->enable_vote());
    }
    while (LSN(PALF_META_BLOCK_SIZE) !=
        leader.palf_handle_impl_->log_engine_.log_meta_storage_.log_tail_)
    {
      sleep(1);
    }
    sleep(1);
    EXPECT_EQ(LSN(PALF_META_BLOCK_SIZE), leader.palf_handle_impl_->log_engine_.log_meta_storage_.log_tail_);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.log_meta_storage_.block_mgr_.switch_next_block(1));
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_LT(LSN(PALF_META_BLOCK_SIZE), leader.palf_handle_impl_->log_engine_.log_meta_storage_.log_tail_);
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(LSN(0)));
    sleep(1);
    EXPECT_LT(LSN(PALF_META_BLOCK_SIZE) + 4096, leader.palf_handle_impl_->log_engine_.log_meta_storage_.log_tail_);
  }
}

TEST_F(TestObSimpleLogClusterRestart, restart_and_clear_tmp_files)
{
  SET_CASE_LOG_FILE(TEST_NAME, "restart_and_clear_tmp_files");
  ObTimeGuard guard("restart_and_clear_tmp_files", 0);
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  std::string logserver_dir;
  {
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    guard.click("create");
    logserver_dir = leader.palf_env_impl_->log_dir_;
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, leader_idx, 1 * 1024 * 1024));
    guard.click("submit_log");
    while (leader.palf_handle_impl_->get_end_lsn()
           < LSN(100 * 1024 * 1024ul)) {
      usleep(100 * 1000);
    }
  }
  const std::string base_dir = logserver_dir;
  const std::string tmp_1_dir = base_dir + "/10000.tmp/log/";
  const std::string mkdir_tmp_1 = "mkdir -p " + tmp_1_dir;
  const std::string dir_2 = base_dir + "/10000000";
  const std::string dir_normal_file = base_dir + "/10000000/log/1";
  const std::string dir_normal_file1 = base_dir + "/10000000/meta/";
  const std::string mkdir_2 = "mkdir -p " + dir_normal_file;
  const std::string mkdir_3 = "mkdir -p " + dir_normal_file1;
  system(mkdir_tmp_1.c_str());
  system(mkdir_2.c_str());
  system(mkdir_3.c_str());
  int ret = OB_SUCCESS;
  guard.click("mkdir");
  EXPECT_EQ(OB_ERR_UNEXPECTED, restart_paxos_groups());
  CLOG_LOG(INFO, "after restart_paxos_groups after exist tmp dir");
  guard.click("restart");
  const std::string rm_dir_2 = "rm -rf " + dir_2;
  system(rm_dir_2.c_str());
  guard.click("rm_dir");
  if (OB_FAIL(restart_paxos_groups())) {
    PALF_LOG(ERROR, "restart_paxos_groups failed", K(ret));
  } else {
    {
      CLOG_LOG(INFO, "after restart_paxos_groups after remove tmp dir");
      guard.click("restart");
      bool result = false;
      EXPECT_EQ(OB_SUCCESS,
                common::FileDirectoryUtils::is_exists(tmp_1_dir.c_str(), result));
      EXPECT_EQ(result, false);
      PalfHandleImplGuard leader1;
      EXPECT_EQ(OB_SUCCESS, get_leader(id, leader1, leader_idx));
      guard.click("get_leader");
      LogStorage *log_storage =
          &leader1.palf_handle_impl_->log_engine_.log_storage_;
      LSN lsn_origin_log_tail = log_storage->get_log_tail_guarded_by_lock_();
      EXPECT_EQ(OB_SUCCESS, submit_log(leader1, 10, leader_idx, 1 * 1024 * 1024));
      while (log_storage->log_tail_ == lsn_origin_log_tail) {
        usleep(1 * 1000);
        PALF_LOG(INFO, "log_tail is same", KPC(log_storage), K(lsn_origin_log_tail));
      }
      guard.click("submit_log");
      EXPECT_EQ(OB_ITER_END, read_log(leader1));
      guard.click("read_log");
      PALF_LOG(INFO, "finish read_log", KPC(log_storage), K(lsn_origin_log_tail), KPC(leader1.palf_handle_impl_));
    }
    // 验证tenant下有临时文件的场景，该临时文件需要归还给log_pool
    {
      PalfHandleImplGuard leader1;
      int64_t leader_idx1 = 0;
      EXPECT_EQ(OB_SUCCESS, get_leader(id, leader1, leader_idx1));
      std::string palf_log_dir = leader1.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.log_dir_;
      ObISimpleLogServer *i_server = get_cluster()[leader_idx1];
      ObSimpleLogServer *server = dynamic_cast<ObSimpleLogServer*>(i_server);
      std::string log_pool = server->log_block_pool_.log_pool_path_;
      const block_id_t min_block_id = server->log_block_pool_.min_block_id_;
      char src[1024] = {'\0'};
      char dest[1024] = {'\0'};
      block_id_to_tmp_string(10000, dest, 1024);
      block_id_to_string(min_block_id, src, 1024);
      std::string src_str = log_pool + "/" + src;
      std::string dest_str = palf_log_dir + "/" + dest;
      std::string mv_system = "mv " + src_str + " " + dest_str;
      system(mv_system.c_str());
      bool result1 = false;
      EXPECT_EQ(OB_SUCCESS,
                common::FileDirectoryUtils::is_exists(dest_str.c_str(), result1));
      EXPECT_EQ(true, result1);
      leader1.reset();
      EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
      EXPECT_EQ(OB_SUCCESS,
                common::FileDirectoryUtils::is_exists(dest_str.c_str(), result1));
      EXPECT_EQ(false, result1);
    }
    EXPECT_EQ(OB_SUCCESS, remove_dir());
    EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  PALF_LOG(INFO, "end test restart", K(id), K(guard));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
