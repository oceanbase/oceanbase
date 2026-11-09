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

#include <gtest/gtest.h>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <signal.h>
#define private public
#include "env/ob_simple_log_cluster_env.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/palf/log_reader_utils.h"
#undef private

const std::string TEST_NAME = "test_rebuild";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{
class TestObSimpleLogClusterRebuild : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterRebuild() : ObSimpleLogClusterTestEnv()
  {}
public:
  class TestRebuildCbImpl : public PalfRebuildCb, public share::ObThreadPool
  {
  public:
    TestRebuildCbImpl():
        test_base_(NULL),
        server_idx_(-1),
        rebuild_palf_id_(-1),
        rebuild_lsn_(),
        allow_rebuild_(false),
        is_inited_(false) {}
    virtual ~TestRebuildCbImpl() { destroy(); }
  public:
    int init(TestObSimpleLogClusterRebuild *ptr, const int64_t server_idx)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(ptr) || server_idx < 0 || server_idx >= ptr->get_member_cnt()) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(ERROR, "invalid_argument", KP(ptr), K(server_idx));
      } else {
        test_base_ = ptr;
        server_idx_ = server_idx;
        is_inited_ = true;
      }
      return ret;
    }

    int start()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
      } else if (OB_FAIL(share::ObThreadPool::start())) {
        PALF_LOG(ERROR, "RebuildCB thread failed to start");
      } else {
        PALF_LOG(INFO, "RebuildCB start success", K(ret));
      }
      return ret;
    }

    void destroy()
    {
      is_inited_ = false;
      stop();
      wait();
      server_idx_ = -1;
      rebuild_palf_id_ = -1;
      rebuild_lsn_.reset();
      allow_rebuild_ = false;
      test_base_ = NULL;
    }

    int on_rebuild(const int64_t id, const LSN &lsn)
    {
      int ret = OB_SUCCESS;
      if (!is_inited_) {
        ret = OB_NOT_INIT;
      } else if (id < 0 || !lsn.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(ERROR, "invalid rebuild argument", K(server_idx_), K(id), K(lsn));
      } else {
        rebuild_palf_id_ = id;
        rebuild_lsn_ = lsn;
        SERVER_LOG(INFO, "on_rebuild success", K(server_idx_), K(id), K(lsn));
      }
      return ret;
    }

    void run1()
    {
      share::ObTenantBase tenant_base(OB_SYS_TENANT_ID);
      tenant_base.init();
      ObTenantEnv::set_tenant(&tenant_base);
      lib::set_thread_name("RebuildCB");
      while (!has_set_stop()) {
        if (true == allow_rebuild_ && rebuild_palf_id_ != -1 && rebuild_lsn_.is_valid()) {
          PalfHandleImplGuard leader;
          PalfHandleImplGuard *rebuild_palf;
          int64_t leader_idx;
          PalfBaseInfo rebuild_base_info;
          std::vector<PalfHandleImplGuard*> palf_list;
          EXPECT_EQ(OB_SUCCESS, test_base_->get_cluster_palf_handle_guard(rebuild_palf_id_, palf_list));
          rebuild_palf = palf_list[server_idx_];
          EXPECT_EQ(OB_SUCCESS, test_base_->get_leader(test_base_->palf_id_, leader, leader_idx));
          EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->disable_sync());
          EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(rebuild_lsn_, rebuild_base_info));
          EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->advance_base_info(rebuild_base_info, true));
          EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->enable_sync());
          rebuild_palf = NULL;
          test_base_->revert_cluster_palf_handle_guard(palf_list);
          rebuild_palf_id_ = -1;
          rebuild_lsn_.reset();
        }
        SERVER_LOG(INFO, "RebuildCB", K(server_idx_));
        usleep(500 * 1000);
      }
    }
  public:
    TestObSimpleLogClusterRebuild *test_base_;
    int64_t server_idx_;
    int64_t rebuild_palf_id_;
    LSN rebuild_lsn_;
    bool allow_rebuild_;
    bool is_inited_;
  };
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
bool ObSimpleLogClusterTestBase::need_remote_log_store_ = false;


class RebuildCrashEnv : public ObSimpleLogClusterTestEnv
{
public:
  RebuildCrashEnv() : ObSimpleLogClusterTestEnv() {}
  void TestBody() override {}
  static void check(const bool condition, const char *stage)
  {
    if (!condition || ::testing::Test::HasFailure()) {
      fprintf(stderr, "rebuild crash phase failed: %s, errno=%d (%s)\n", stage, errno, strerror(errno));
      fflush(stderr);
      // Do not unwind the blocked truncate worker on a failed child assertion.
      _exit(2);
    }
  }
  static void checksum(PalfHandleImpl *handle, const LSN &end, uint64_t &crc, int64_t &count)
  {
    const int64_t deadline = ObTimeUtility::current_time() + TIMEOUT_US;
    LSN flushed;
    do {
      handle->sw_.get_max_flushed_end_lsn(flushed);
      if (handle->get_end_lsn() >= end && flushed >= end) { break; }
      ob_usleep(1000);
    } while (ObTimeUtility::current_time() < deadline);
    check(handle->get_end_lsn() >= end && flushed >= end, "wait for readable healthy prefix");
    PalfBufferIterator iter;
    crc = 0;
    count = 0;
    check(OB_SUCCESS == handle->alloc_palf_buffer_iterator(LSN(0), iter), "open healthy LS iterator");
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter.next())) {
      const char *buf = NULL;
      int64_t len = 0;
      SCN scn;
      LSN lsn;
      bool raw = false;
      check(OB_SUCCESS == iter.get_entry(buf, len, scn, lsn, raw), "read healthy LS payload");
      if (lsn >= end) {
        break;
      }
      ObLogBaseHeader header;
      int64_t pos = 0;
      check(OB_SUCCESS == header.deserialize(buf, len, pos), "decode healthy log type");
      if (PADDING_LOG_BASE_TYPE != header.get_log_type()) {
        check(TRANS_SERVICE_LOG_BASE_TYPE == header.get_log_type(), "healthy application log type");
        crc = ob_crc64(crc, buf, len);
        ++count;
      }
    }
    check(OB_SUCCESS == ret || OB_ITER_END == ret, "finish healthy LS iterator");
  }
  static void sync_committed(PalfHandleImpl *leader, PalfHandleImpl *replica)
  {
    const LSN committed = leader->get_end_lsn();
    int64_t log_id = OB_INVALID_LOG_ID;
    int64_t log_proposal_id = INVALID_PROPOSAL_ID;
    int64_t checksum = 0;
    SCN scn;
    LSN lsn;
    LSN end;
    const int64_t deadline = ObTimeUtility::current_time() + TIMEOUT_US;
    do {
      leader->sw_.get_last_slide_log_info_(log_id, scn, lsn, end, log_proposal_id, checksum);
      if (end < committed) { ob_usleep(1000); }
    } while (end < committed && ObTimeUtility::current_time() < deadline);
    check(end == committed, "read committed healthy prefix metadata");
    // Submitted logs can be ahead of the follower's last received committed-info message.
    while (replica->get_end_lsn() < committed && ObTimeUtility::current_time() < deadline) {
      check(OB_SUCCESS == leader->log_engine_.submit_committed_info_req(
          replica->self_, leader->state_mgr_.get_proposal_id(), log_id, log_proposal_id, committed),
          "publish committed healthy prefix");
      ob_usleep(10 * 1000);
    }
    check(replica->get_end_lsn() >= committed, "healthy prefix committed on replica");
  }
  class BlockFirstReturn : public ILogBlockPool
  {
  public:
    explicit BlockFirstReturn(ILogBlockPool *pool) : pool_(pool), entered_(false) {}
    int create_block(const char *path, const int64_t size) override
    { return pool_->create_block(path, size); }
    int remove_block(const char *path) override
    {
      // No block has reached the underlying pool when the parent phase observes entry.
      entered_.store(true);
      const int64_t deadline = ObTimeUtility::current_time() + 2 * TIMEOUT_US;
      while (ObTimeUtility::current_time() < deadline) { ob_usleep(1000); }
      RebuildCrashEnv::check(false, "first-return barrier timed out before SIGKILL");
      return OB_ERR_UNEXPECTED;
    }
    ILogBlockPool *pool_;
    std::atomic<bool> entered_;
  };
  static void prepare(const std::string &image)
  {
    test_name_ = image + "_live";
    // Use the same port alignment in preparation and recovery outside Farm.
    if (NULL == getenv("mit_network_start_port") || NULL == getenv("mit_network_port_num")) {
      check(0 == setenv("mit_network_start_port", "28000", 1), "set default port start");
      check(0 == setenv("mit_network_port_num", "100", 1), "set default port count");
    }
    check(OB_SUCCESS == start(), "start preparation cluster");
    RebuildCrashEnv env;
    PalfHandleImplGuard leader;
    PalfHandleImplGuard healthy_leader;
    int64_t leader_idx = 0;
    int64_t healthy_leader_idx = 0;
    std::vector<PalfHandleImplGuard*> rebuilding;
    std::vector<PalfHandleImplGuard*> healthy;
    check(OB_SUCCESS == env.create_paxos_group(REBUILD_ID, leader_idx, leader), "create rebuild LS");
    check(OB_SUCCESS == env.create_paxos_group(HEALTHY_ID, healthy_leader_idx, healthy_leader), "create healthy LS");
    check(OB_SUCCESS == env.get_cluster_palf_handle_guard(REBUILD_ID, rebuilding), "open rebuild replicas");
    check(OB_SUCCESS == env.get_cluster_palf_handle_guard(HEALTHY_ID, healthy), "open healthy replicas");
    const int64_t target_idx = (leader_idx + 1) % 3;
    PalfHandleImpl *target = rebuilding[target_idx]->palf_handle_impl_;
    PalfEnvImpl *palf_env = rebuilding[target_idx]->palf_env_impl_;
    for (int64_t i = 0; i < 3; ++i) {
      rebuilding[i]->palf_env_impl_->block_gc_timer_task_.stop();
      rebuilding[i]->palf_env_impl_->block_gc_timer_task_.wait();
      ObServerLogBlockMgr *pool = NULL;
      check(OB_SUCCESS == env.get_log_pool(i, pool), "get shared pool");
      check(OB_SUCCESS == pool->persist_cross_tenant_reclaim_enabled(true), "persist shared-pool mode");
    }
    check(OB_SUCCESS == env.submit_log(leader, 132, REBUILD_ID, 1024 * 1024), "write rebuild prefix");
    check(OB_SUCCESS == env.submit_log(healthy_leader, 80, HEALTHY_ID, 1024 * 1024), "write healthy history");
    for (int64_t i = 0; i < 3; ++i) {
      check(OB_SUCCESS == env.check_replica_sync(REBUILD_ID, &leader, rebuilding[i], TIMEOUT_US), "sync rebuild prefix");
      check(OB_SUCCESS == env.check_replica_sync(HEALTHY_ID, &healthy_leader, healthy[i], TIMEOUT_US), "sync healthy history");
      sync_committed(healthy_leader.palf_handle_impl_, healthy[i]->palf_handle_impl_);
    }
    ObTenantEnv::set_tenant(env.get_cluster()[target_idx]->get_tenant_base());
    check(OB_SUCCESS == target->disable_sync(), "disable target sync");
    const LSN old_tail = target->log_engine_.log_storage_.get_end_lsn();
    const LSN new_base(4 * PALF_BLOCK_SIZE);
    check(old_tail < new_base, "snapshot beyond old tail");
    check(OB_SUCCESS == env.submit_log(leader, 132, REBUILD_ID, 1024 * 1024), "write rebuild suffix");
    check(OB_SUCCESS == env.wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()), "commit suffix");
    PalfBaseInfo base_info;
    check(OB_SUCCESS == leader.palf_handle_impl_->get_base_info(new_base, base_info), "get rebuild snapshot");
    PalfDiskOptions options = palf_env->disk_options_wrapper_.get_disk_opts_for_stopping_writing();
    options.log_disk_throttling_percentage_ = 60;
    options.log_disk_throttling_maximum_duration_ = 1000 * 1000L;
    check(OB_SUCCESS == palf_env->disk_options_wrapper_.update_disk_options(options), "set stall interval");
    TenantLogBlockReclaimService &service = palf_env->tenant_reclaim_service_;
    service.sync_reclaim_mode(LogReclaimState::ENABLED, true, false);
    service.update_reclaim_snapshot(3, 0);
    check(OB_SUCCESS == service.apply_reclaim_quota(3), "apply pending quota");
    const int64_t stall_deadline = ObTimeUtility::current_time() + TIMEOUT_US;
    do {
      palf_env->period_calc_disk_usage();
      if (!palf_env->gc_stall_stopped_) { ob_usleep(1000); }
    } while (!palf_env->gc_stall_stopped_ && ObTimeUtility::current_time() < stall_deadline);
    check(palf_env->gc_stall_stopped_ && !palf_env->check_disk_space_enough(), "observe GC-stall gate");
    BlockFirstReturn pool(target->log_engine_.log_storage_.block_mgr_.log_block_pool_);
    target->log_engine_.log_storage_.block_mgr_.log_block_pool_ = &pool;
    check(OB_SUCCESS == target->advance_base_info(base_info, true), "submit real rebuild");
    const int64_t deadline = ObTimeUtility::current_time() + TIMEOUT_US;
    while ((!pool.entered_.load() || new_base != target->get_base_lsn_used_for_block_gc())
        && ObTimeUtility::current_time() < deadline) { ob_usleep(1000); }
    // The GC base is published by the successful snapshot flush callback.
    check(pool.entered_.load() && new_base == target->get_base_lsn_used_for_block_gc(), "durable snapshot before first return");
    check(old_tail == target->log_engine_.log_storage_.get_end_lsn(), "truncate still blocked");
    check(3 == service.pending_blocks_, "quota still pending at crash");
    save_image(env, healthy[target_idx]->palf_handle_impl_, target_idx, image);
    ::kill(::getpid(), SIGKILL);
    _exit(3);
  }
  static void save_image(RebuildCrashEnv &env, PalfHandleImpl *healthy,
                         const int64_t target_idx, const std::string &image)
  {
    const LSN end = healthy->get_end_lsn();
    uint64_t crc = 0;
    int64_t count = 0;
    int64_t physical = 0;
    int64_t usage = 0;
    checksum(healthy, end, crc, count);
    check(80 == count, "healthy prefix length");
    check(OB_SUCCESS == healthy->get_total_used_disk_space(physical, usage), "healthy usage before crash");
    check(usage > 80 * 1024 * 1024, "healthy usage is charged");
    check(0 == ::rename(test_name_.c_str(), image.c_str()), "preserve unique crash image");
    std::ofstream manifest(image + "/expected");
    manifest << target_idx << ' ' << end.val_ << ' ' << crc << ' ' << count << ' ' << usage << '\n';
    for (int64_t i = 0; i < 3; ++i) { manifest << env.get_cluster()[i]->get_addr().get_port() << '\n'; }
    manifest.close();
    check(manifest.good(), "write recovery manifest");
  }
  static void recover(const std::string &image)
  {
    int64_t target_idx = -1;
    uint64_t end_val = 0;
    uint64_t expected_crc = 0;
    int64_t expected_count = 0;
    int64_t expected_usage = 0;
    int ports[3] = {0};
    std::ifstream manifest(image + "/expected");
    manifest >> target_idx >> end_val >> expected_crc >> expected_count >> expected_usage;
    for (int64_t i = 0; i < 3; ++i) { manifest >> ports[i]; }
    check(manifest.good() && target_idx >= 0 && target_idx < 3 && ports[0] > 3, "read recovery manifest");
    manifest.close();
    const std::string port_start = std::to_string(ports[0] - 4);
    check(0 == setenv("mit_network_start_port", port_start.c_str(), 1), "restore port range");
    check(0 == setenv("mit_network_port_num", "9", 1), "bound port range");
    // Keep child data separate from the ordinary fixture in the parent process.
    test_name_ = image + "_live";
    check(0 == ::rename(image.c_str(), test_name_.c_str()), "restore server files");
    check(OB_SUCCESS == start(), "start fresh recovery process");
    RebuildCrashEnv env;
    PalfHandleImplGuard leader;
    PalfHandleImplGuard healthy_leader;
    int64_t leader_idx = 0;
    int64_t healthy_leader_idx = 0;
    std::vector<PalfHandleImplGuard*> rebuilding;
    std::vector<PalfHandleImplGuard*> healthy;
    check(OB_SUCCESS == env.get_cluster_palf_handle_guard(REBUILD_ID, rebuilding), "open recovered rebuild LS");
    check(OB_SUCCESS == env.get_cluster_palf_handle_guard(HEALTHY_ID, healthy), "open recovered healthy LS");
    PalfHandleImpl *target = rebuilding[target_idx]->palf_handle_impl_;
    check(LSN(4 * PALF_BLOCK_SIZE) == target->get_base_lsn_used_for_block_gc(), "recover durable snapshot base");
    check(OB_SUCCESS == target->enable_sync(), "resume rebuilt replica");
    check(OB_SUCCESS == env.get_leader(REBUILD_ID, leader, leader_idx), "elect rebuild leader");
    check(OB_SUCCESS == env.get_leader(HEALTHY_ID, healthy_leader, healthy_leader_idx), "elect healthy leader");
    for (int64_t i = 0; i < 3; ++i) {
      check(ports[i] == env.get_cluster()[i]->get_addr().get_port(), "recover same replica identity");
      PalfEnvImpl *palf_env = rebuilding[i]->palf_env_impl_;
      check(0 == palf_env->tenant_reclaim_service_.pending_blocks_, "reconstruct pending quota");
      check(OB_INVALID_TIMESTAMP == palf_env->tenant_reclaim_service_.get_gc_stall_start_ts(), "reconstruct stall timer");
      palf_env->period_calc_disk_usage();
      check(!palf_env->gc_stall_stopped_ && palf_env->check_disk_space_enough(), "recover write gate");
    }
    int64_t physical = 0;
    int64_t healthy_usage = 0;
    check(OB_SUCCESS == healthy[target_idx]->palf_handle_impl_->get_total_used_disk_space(physical, healthy_usage), "recovered healthy usage");
    check(expected_usage == healthy_usage, "healthy LS usage survives interrupted rebuild");
    // A committed append propagates the recovered committed LSN before payload iteration.
    check(OB_SUCCESS == env.submit_log(healthy_leader, 1, HEALTHY_ID, 4096), "append to healthy LS");
    check(OB_SUCCESS == env.wait_until_has_committed(healthy_leader, healthy_leader.palf_handle_impl_->get_max_lsn()), "commit healthy append");
    check(OB_SUCCESS == env.submit_log(leader, 1, REBUILD_ID, 4096), "append to rebuilt LS");
    check(OB_SUCCESS == env.wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()), "commit rebuilt append");
    for (int64_t i = 0; i < 3; ++i) {
      check(OB_SUCCESS == env.check_replica_sync(REBUILD_ID, &leader, rebuilding[i], TIMEOUT_US), "rebuild catch-up after crash");
      check(OB_SUCCESS == env.check_replica_sync(HEALTHY_ID, &healthy_leader, healthy[i], TIMEOUT_US), "healthy catch-up after crash");
      sync_committed(healthy_leader.palf_handle_impl_, healthy[i]->palf_handle_impl_);
      uint64_t crc = 0;
      int64_t count = 0;
      checksum(healthy[i]->palf_handle_impl_, LSN(end_val), crc, count);
      check(expected_crc == crc && expected_count == count, "healthy payload survives crash");
    }
    check(OB_SUCCESS == env.revert_cluster_palf_handle_guard(rebuilding), "release rebuild handles");
    check(OB_SUCCESS == env.revert_cluster_palf_handle_guard(healthy), "release healthy handles");
    leader.reset();
    healthy_leader.reset();
    check(OB_SUCCESS == close(), "close recovered cluster");
    _exit(0);
  }
private:
  static const int64_t REBUILD_ID = 56001;
  static const int64_t HEALTHY_ID = 56002;
  static const int64_t TIMEOUT_US = 30 * 1000 * 1000L;
};

TEST(RebuildSnapshotCrashDeathTest, durable_snapshot_before_first_return_recovers)
{
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  bool owns_image = false;
  if (NULL == getenv("OB_TEST_REBUILD_CRASH_IMAGE")) {
    char path[] = "rebuild_snapshot_crash.XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(path));
    ASSERT_EQ(0, setenv("OB_TEST_REBUILD_CRASH_IMAGE", path, 1));
    owns_image = true;
  }
  DEFER({ if (owns_image) { unsetenv("OB_TEST_REBUILD_CRASH_IMAGE"); } });
  const std::string image = std::string(getenv("OB_TEST_REBUILD_CRASH_IMAGE")) + "/image";
  ASSERT_EXIT({ RebuildCrashEnv::prepare(image); }, ::testing::KilledBySignal(SIGKILL), "");
  ASSERT_EXIT({ RebuildCrashEnv::recover(image); }, ::testing::ExitedWithCode(0), "");
  EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::delete_directory_rec(getenv("OB_TEST_REBUILD_CRASH_IMAGE")));
}

TEST_F(TestObSimpleLogClusterRebuild, test_old_leader_rebuild)
{
  SET_CASE_LOG_FILE(TEST_NAME, "old_leader_rebuild");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  constexpr int64_t KB = 1024;
  constexpr int64_t MB = 1024 * 1024;
  PALF_LOG(INFO, "start test old_leader_rebuild", K(id));
  int64_t leader_idx = 0;
  unittest::PalfHandleImplGuard leader;
  unittest::PalfHandleImplGuard *rebuild_server = NULL;
  std::vector<PalfHandleImplGuard*> palf_list;
  int64_t follower_idx1, follower_idx2;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  follower_idx1 = (leader_idx + 1) % 3;
  follower_idx2 = (leader_idx + 2) % 3;
  TestRebuildCbImpl rebuild_cb;
  PalfRebuildCbNode rebuild_node(&rebuild_cb);
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.init(this, leader_idx));
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.start());
  // reigster rebuild cb
  rebuild_server = &leader;
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->register_rebuild_cb(&rebuild_node));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1000, id, 6 * KB));
  sleep(1);
  PALF_LOG(INFO, "begin block net", K(id), K(leader_idx), K(follower_idx1), K(follower_idx2));
  block_net(leader_idx, follower_idx1);
  block_net(leader_idx, follower_idx2);
  submit_log(leader, 100, id, 6 * KB);
  PALF_LOG(INFO, "begin submit logs", K(id), K(leader_idx), K(follower_idx1), K(follower_idx2));
  (void) submit_log(leader, 1000, leader_idx, MB);
  PALF_LOG(INFO, "after sleep 16s, begin get_leader", K(id), K(leader_idx), K(follower_idx1), K(follower_idx2));
  int64_t new_leader_idx = 0;
  unittest::PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  PALF_LOG(INFO, "after get_leader", K(id), K(leader_idx), K(new_leader_idx));
  // submit logs
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 64 * 8, id, MB));

  // update new_leader's disk option, only reserves 4 * 80% log blocks,
  // that means 2 blocks will be recycled
  PALF_LOG(INFO, "begin advance_base_lsn", K(id), K(leader_idx), K(new_leader_idx));
  LSN recycle_lsn(2 * PALF_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->set_base_lsn(recycle_lsn));
  update_disk_options(new_leader_idx, 8);
  // recycle 2 block
  sleep(5);
  block_id_t leader_min_block_id;
  share::SCN min_scn;
  block_id_t min_block_id, max_block_id;
  while (true) {
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_min_block_info_for_gc(leader_min_block_id, min_scn));
    // palf will reserve one more block
    if (1 ==leader_min_block_id) {
      break;
    } else {
      sleep(2);
    }
  }

  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->log_engine_.get_block_id_range(min_block_id, max_block_id));
  PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "runlin trace get_block_id_range", K(min_block_id), K(max_block_id));

  // submit a cond task before unblocking net to stop truncating task
  IOTaskCond cond(id, rebuild_server->palf_env_impl_->last_palf_epoch_);
  LogIOWorker *io_worker = rebuild_server->palf_handle_impl_->log_engine_.log_io_worker_;
  io_worker->submit_io_task(&cond);

  // after unblocking net, old leader will do rebuild
  unblock_net(leader_idx, follower_idx1);
  unblock_net(leader_idx, follower_idx2);
  sleep(5);

  // is truncating, can not rebuild
  if (rebuild_server->palf_handle_impl_->sw_.is_truncating_) {
    PalfBaseInfo rebuild_base_info;
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_base_info(rebuild_cb.rebuild_lsn_, rebuild_base_info));
    EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->disable_sync());
    EXPECT_EQ(OB_EAGAIN, rebuild_server->palf_handle_impl_->advance_base_info(rebuild_base_info, true));
  }
  cond.cond_.signal();
  sleep(5);
  rebuild_cb.allow_rebuild_ = true;

  PalfBaseInfo base_info_in_leader;
  PalfBaseInfo base_info_after_rebuild;
  // check block_id
  block_id_t rebuild_server_min_block_id;
  // get block_id may be executed when all blocks are deleted, so wait for OB_SUCCESS
  while (OB_SUCCESS != rebuild_server->palf_handle_impl_->get_min_block_info_for_gc(rebuild_server_min_block_id, min_scn))
  {
    sleep(1);
  }
  EXPECT_EQ(2, rebuild_server_min_block_id);
  // check base_lsn
  EXPECT_EQ(recycle_lsn, rebuild_server->palf_handle_impl_->log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_);
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_base_info(recycle_lsn, base_info_in_leader));
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->get_base_info(recycle_lsn, base_info_after_rebuild));
  // check prev_log_info
  EXPECT_EQ(base_info_in_leader.prev_log_info_, base_info_after_rebuild.prev_log_info_);

  LSN leader_end_lsn, rebuild_server_end_lsn;
  leader_end_lsn = new_leader.palf_handle_impl_->get_end_lsn();
  rebuild_server_end_lsn = rebuild_server->palf_handle_impl_->get_end_lsn();
  PALF_LOG(INFO, "rebuild result", K(leader_end_lsn), K(rebuild_server_end_lsn));
  // EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  revert_cluster_palf_handle_guard(palf_list);
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->set_base_lsn(LSN(64*6*MB)));
  sleep(1);
  EXPECT_EQ(OB_SUCCESS, update_disk_options(new_leader_idx, 30));
  PALF_LOG(INFO, "end test old_leader_rebuild", K(id));
}

TEST_F(TestObSimpleLogClusterRebuild, test_follower_rebuild)
{
  SET_CASE_LOG_FILE(TEST_NAME, "follower_rebuild");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  constexpr int64_t MB = 1024 * 1024;
  PALF_LOG(INFO, "start test follower_rebuild", K(id));
  int64_t leader_idx = 0;
  unittest::PalfHandleImplGuard leader;
  unittest::PalfHandleImplGuard *rebuild_server = NULL;
  std::vector<PalfHandleImplGuard*> palf_list;
  int64_t follower_idx;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  follower_idx = (leader_idx + 1) % 3;
  TestRebuildCbImpl rebuild_cb;
  rebuild_cb.allow_rebuild_ = true;
  PalfRebuildCbNode rebuild_node(&rebuild_cb);
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.init(this, follower_idx));
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.start());
  // reigster rebuild cb
  rebuild_server = palf_list[follower_idx];
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->register_rebuild_cb(&rebuild_node));
  // the follower is empty
  block_net(leader_idx, follower_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 64, leader_idx, MB));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 64 * 7, leader_idx, MB));
  // recycle one block
  LSN recycle_lsn(2 * PALF_BLOCK_SIZE);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->set_base_lsn(recycle_lsn));
  ASSERT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 8));
  DEFER({ EXPECT_EQ(OB_SUCCESS, update_disk_options(leader_idx, 30)); });
  sleep(1);
  block_id_t leader_min_block_id, follower_min_block_id;
  share::SCN min_scn;
  while (true) {
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_min_block_info_for_gc(leader_min_block_id, min_scn));
    if (1 == leader_min_block_id) {
      break;
    } else {
      sleep(2);
    }
  }
  EXPECT_EQ(recycle_lsn, leader.palf_handle_impl_->log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_);

  // follower_idx will trigger rebuild when fetching log
  unblock_net(leader_idx, follower_idx);
  sleep(10);
  PalfBaseInfo base_info_in_leader;
  PalfBaseInfo base_info_after_rebuild;
  // check block_id
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->get_min_block_info_for_gc(follower_min_block_id, min_scn));
  EXPECT_EQ(2, follower_min_block_id);
  // check base_lsn
  EXPECT_EQ(recycle_lsn, rebuild_server->palf_handle_impl_->log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(recycle_lsn, base_info_in_leader));
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->get_base_info(recycle_lsn, base_info_after_rebuild));
  // check prev_log_info
  EXPECT_EQ(base_info_in_leader.prev_log_info_, base_info_after_rebuild.prev_log_info_);

  LSN leader_end_lsn, rebuild_server_end_lsn;
  leader_end_lsn = leader.palf_handle_impl_->get_end_lsn();
  rebuild_server_end_lsn = rebuild_server->palf_handle_impl_->get_end_lsn();
  PALF_LOG(INFO, "rebuild result", K(leader_end_lsn), K(rebuild_server_end_lsn));
  // EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test follower_rebuild", K(id));
}

TEST_F(TestObSimpleLogClusterRebuild, test_rebuild_completes_stalled_block_gc)
{
  SET_CASE_LOG_FILE(TEST_NAME, "rebuild_completes_stalled_block_gc");
  constexpr int64_t MB = 1024 * 1024;
  constexpr int64_t WAIT_TIMEOUT_US = 30 * 1000 * 1000L;
  const int64_t first_id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t second_id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t ids[] = {first_id, second_id};
  const LSN frozen_base_lsn(2 * PALF_BLOCK_SIZE);
  int64_t first_leader_idx = 0;
  int64_t second_leader_idx = 0;
  PalfHandleImplGuard first_leader;
  PalfHandleImplGuard second_leader;
  PalfHandleImplGuard *leaders[] = {&first_leader, &second_leader};
  std::vector<PalfHandleImplGuard*> first_palf_list;
  std::vector<PalfHandleImplGuard*> second_palf_list;
  std::vector<PalfHandleImplGuard*> *palf_lists[] = {&first_palf_list, &second_palf_list};

  ASSERT_EQ(OB_SUCCESS, create_paxos_group(first_id, first_leader_idx, first_leader));
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(second_id, second_leader_idx, second_leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(first_id, first_palf_list));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(second_id, second_palf_list));
  DEFER({
    revert_cluster_palf_handle_guard(first_palf_list);
    revert_cluster_palf_handle_guard(second_palf_list);
    first_leader.reset();
    second_leader.reset();
    delete_paxos_group(first_id);
    delete_paxos_group(second_id);
  });

  for (int64_t stream_idx = 0; stream_idx < 2; ++stream_idx) {
    ASSERT_EQ(OB_SUCCESS, submit_log(*leaders[stream_idx], 64 * 3, ids[stream_idx], MB));
    ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(
        *leaders[stream_idx], leaders[stream_idx]->palf_handle_impl_->get_max_lsn()));
    for (int64_t server_idx = 0; server_idx < get_member_cnt(); ++server_idx) {
      ASSERT_EQ(OB_SUCCESS, check_replica_sync(
          ids[stream_idx], leaders[stream_idx], palf_lists[stream_idx]->at(server_idx),
          WAIT_TIMEOUT_US));
    }
  }

  for (int64_t stream_idx = 0; stream_idx < 2; ++stream_idx) {
    for (int64_t server_idx = 0; server_idx < get_member_cnt(); ++server_idx) {
      ObTenantEnv::set_tenant(get_cluster()[server_idx]->get_tenant_base());
      ASSERT_EQ(OB_SUCCESS,
          palf_lists[stream_idx]->at(server_idx)->palf_handle_impl_->set_base_lsn(
              frozen_base_lsn));
    }
  }
  bool all_base_lsn_ready = false;
  const int64_t base_lsn_deadline = ObTimeUtility::current_time() + WAIT_TIMEOUT_US;
  while (!all_base_lsn_ready && ObTimeUtility::current_time() < base_lsn_deadline) {
    all_base_lsn_ready = true;
    for (int64_t stream_idx = 0; all_base_lsn_ready && stream_idx < 2; ++stream_idx) {
      for (int64_t server_idx = 0; all_base_lsn_ready && server_idx < get_member_cnt();
           ++server_idx) {
        LSN base_lsn;
        all_base_lsn_ready = OB_SUCCESS == palf_lists[stream_idx]->at(server_idx)
            ->palf_handle_impl_->get_base_lsn(base_lsn) && frozen_base_lsn == base_lsn;
      }
    }
    if (!all_base_lsn_ready) {
      ob_usleep(1000);
    }
  }
  ASSERT_TRUE(all_base_lsn_ready);

  const int64_t rebuild_server_idx = (first_leader_idx + 1) % get_member_cnt();
  PalfHandleImplGuard *rebuild_palf = first_palf_list.at(rebuild_server_idx);
  PalfHandleImplGuard *other_palf = second_palf_list.at(rebuild_server_idx);
  PalfEnvImpl *target_env = rebuild_palf->palf_env_impl_;
  ASSERT_EQ(target_env, other_palf->palf_env_impl_);
  int64_t reclaim_block_count = 0;
  PalfHandleImplGuard *target_palfs[] = {rebuild_palf, other_palf};
  for (auto target_palf : target_palfs) {
    block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
    share::SCN min_block_scn;
    ASSERT_EQ(OB_SUCCESS,
        target_palf->palf_handle_impl_->get_min_block_info_for_gc(min_block_id, min_block_scn));
    const block_id_t base_block_id = lsn_2_block(
        target_palf->palf_handle_impl_->get_base_lsn_used_for_block_gc(), PALF_BLOCK_SIZE);
    ASSERT_EQ(0, min_block_id);
    ASSERT_EQ(2, base_block_id);
    reclaim_block_count += base_block_id - min_block_id - 1;
  }
  ASSERT_EQ(2, reclaim_block_count);

  target_env->block_gc_timer_task_.stop();
  target_env->block_gc_timer_task_.wait();
  DEFER({ EXPECT_EQ(OB_SUCCESS, target_env->block_gc_timer_task_.start()); });
  ObTenantEnv::set_tenant(get_cluster()[rebuild_server_idx]->get_tenant_base());
  TenantLogBlockReclaimService &reclaim_service = target_env->tenant_reclaim_service_;
  reclaim_service.sync_reclaim_mode(
      LogReclaimState::ENABLED, /* server_need_reclaim */ true,
      /* local_reclaim_done */ false);
  reclaim_service.update_reclaim_snapshot(reclaim_block_count, 0);
  ASSERT_EQ(OB_SUCCESS, reclaim_service.apply_reclaim_quota(reclaim_block_count));
  const int64_t now_us = ObTimeUtility::current_monotonic_time();
  ATOMIC_STORE(&reclaim_service.gc_stall_start_ts_, now_us);
  ASSERT_EQ(reclaim_block_count, reclaim_service.pending_blocks_);
  ASSERT_EQ(now_us, reclaim_service.get_gc_stall_start_ts());

  IOTaskCond block_rebuild_task(
      first_id, rebuild_palf->palf_handle_impl_->log_engine_.palf_epoch_);
  LogIOWorker *io_worker = rebuild_palf->palf_handle_impl_->log_engine_.log_io_worker_;
  ASSERT_EQ(OB_SUCCESS, io_worker->submit_io_task(&block_rebuild_task));
  const int64_t block_rebuild_deadline = ObTimeUtility::current_time() + WAIT_TIMEOUT_US;
  while (0 == ATOMIC_LOAD(&block_rebuild_task.count_)
      && ObTimeUtility::current_time() < block_rebuild_deadline) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, ATOMIC_LOAD(&block_rebuild_task.count_));
  {
    PalfHandleImpl::WLockGuard guard(rebuild_palf->palf_handle_impl_->lock_);
    rebuild_palf->palf_handle_impl_->sw_.is_rebuilding_ = true;
    rebuild_palf->palf_handle_impl_->sw_.last_rebuild_lsn_ = frozen_base_lsn;
  }
  TruncatePrefixBlocksCbCtx rebuild_ctx(frozen_base_lsn);
  EXPECT_EQ(OB_SUCCESS,
      rebuild_palf->palf_handle_impl_->log_engine_.submit_truncate_prefix_blocks_task(rebuild_ctx));

  for (auto palf_list : palf_lists) {
    for (auto palf : *palf_list) {
      LSN base_lsn;
      EXPECT_EQ(OB_SUCCESS, palf->palf_handle_impl_->get_base_lsn(base_lsn));
      EXPECT_EQ(frozen_base_lsn, base_lsn);
    }
  }
  EXPECT_EQ(reclaim_block_count, reclaim_service.pending_blocks_);
  block_rebuild_task.cond_.signal();

  bool rebuild_finished = false;
  const int64_t rebuild_deadline = ObTimeUtility::current_time() + WAIT_TIMEOUT_US;
  while (!rebuild_finished && ObTimeUtility::current_time() < rebuild_deadline) {
    {
      PalfHandleImpl::RLockGuard guard(rebuild_palf->palf_handle_impl_->lock_);
      rebuild_finished = !rebuild_palf->palf_handle_impl_->sw_.is_rebuilding_;
    }
    if (!rebuild_finished) {
      ob_usleep(1000);
    }
  }
  ASSERT_TRUE(rebuild_finished);
  ASSERT_EQ(0, reclaim_service.pending_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, reclaim_service.get_gc_stall_start_ts());
  EXPECT_FALSE(reclaim_service.can_recycle());
  EXPECT_EQ(0, reclaim_service.retention_expired_blocks_);
  EXPECT_EQ(0, reclaim_service.retention_unexpired_blocks_);

  block_id_t rebuild_min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t other_min_block_id = LOG_INVALID_BLOCK_ID;
  share::SCN min_block_scn;
  ASSERT_EQ(OB_SUCCESS,
      rebuild_palf->palf_handle_impl_->get_min_block_info_for_gc(
          rebuild_min_block_id, min_block_scn));
  ASSERT_EQ(OB_SUCCESS,
      other_palf->palf_handle_impl_->get_min_block_info_for_gc(
          other_min_block_id, min_block_scn));
  EXPECT_EQ(2, rebuild_min_block_id);
  EXPECT_EQ(0, other_min_block_id);
  for (auto palf_list : palf_lists) {
    for (auto palf : *palf_list) {
      LSN base_lsn;
      EXPECT_EQ(OB_SUCCESS, palf->palf_handle_impl_->get_base_lsn(base_lsn));
      EXPECT_EQ(frozen_base_lsn, base_lsn);
    }
  }

  reclaim_service.sync_reclaim_mode(
      LogReclaimState::ENABLED, /* server_need_reclaim */ false,
      /* local_reclaim_done */ false);
  EXPECT_EQ(0, reclaim_service.pending_blocks_);
  EXPECT_FALSE(reclaim_service.can_recycle());
}

TEST_F(TestObSimpleLogClusterRebuild, rebuild_new_base_beyond_tail_preserves_tenant_usage)
{
  SET_CASE_LOG_FILE(TEST_NAME, "rebuild_new_base_beyond_tail_preserves_tenant_usage");
  constexpr int64_t MB = 1024 * 1024;
  constexpr int64_t TIMEOUT_US = 30 * 1000 * 1000L;
  const int64_t rebuild_id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t healthy_id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  int64_t healthy_leader_idx = 0;
  PalfHandleImplGuard leader;
  PalfHandleImplGuard healthy_leader;
  std::vector<PalfHandleImplGuard*> rebuilding;
  std::vector<PalfHandleImplGuard*> healthy;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(rebuild_id, leader_idx, leader));
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(healthy_id, healthy_leader_idx, healthy_leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(rebuild_id, rebuilding));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(healthy_id, healthy));
  DEFER({
    revert_cluster_palf_handle_guard(rebuilding);
    revert_cluster_palf_handle_guard(healthy);
    leader.reset();
    healthy_leader.reset();
    delete_paxos_group(rebuild_id);
    delete_paxos_group(healthy_id);
  });
  auto wait_until = [&](const std::function<bool()> &ready) {
    const int64_t deadline = ObTimeUtility::current_time() + TIMEOUT_US;
    while (!ready() && ObTimeUtility::current_time() < deadline) {
      ob_usleep(1000);
    }
    return ready();
  };
  const int64_t target_idx = (leader_idx + 1) % get_member_cnt();
  PalfHandleImpl *target = rebuilding[target_idx]->palf_handle_impl_;
  PalfHandleImpl *other = healthy[target_idx]->palf_handle_impl_;
  PalfEnvImpl *env = rebuilding[target_idx]->palf_env_impl_;
  ASSERT_EQ(env, healthy[target_idx]->palf_env_impl_);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 132, rebuild_id, MB));
  ASSERT_EQ(OB_SUCCESS, submit_log(healthy_leader, 80, healthy_id, MB));
  ASSERT_EQ(OB_SUCCESS, check_replica_sync(rebuild_id, &leader, rebuilding[target_idx], TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, check_replica_sync(healthy_id, &healthy_leader, healthy[target_idx], TIMEOUT_US));
  ObTenantEnv::set_tenant(get_cluster()[target_idx]->get_tenant_base());
  ASSERT_EQ(OB_SUCCESS, target->disable_sync());
  const LSN old_tail = target->log_engine_.log_storage_.get_end_lsn();
  const LSN new_base(4 * PALF_BLOCK_SIZE);
  ASSERT_LT(old_tail, new_base);
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 132, rebuild_id, MB));
  ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  PalfBaseInfo base_info;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(new_base, base_info));
  env->block_gc_timer_task_.stop();
  env->block_gc_timer_task_.wait();
  DEFER({ EXPECT_EQ(OB_SUCCESS, env->block_gc_timer_task_.start()); });
  const PalfDiskOptions saved_options = env->disk_options_wrapper_.get_disk_opts_for_stopping_writing();
  PalfDiskOptions options = saved_options;
  options.log_disk_throttling_percentage_ = 60;
  options.log_disk_throttling_maximum_duration_ = 1000 * 1000L;
  ASSERT_EQ(OB_SUCCESS, env->disk_options_wrapper_.update_disk_options(options));
  DEFER({ EXPECT_EQ(OB_SUCCESS, env->disk_options_wrapper_.update_disk_options(saved_options)); });
  TenantLogBlockReclaimService &service = env->tenant_reclaim_service_;
  service.sync_reclaim_mode(LogReclaimState::ENABLED, true, false);
  service.update_reclaim_snapshot(3, 0);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(3));
  ASSERT_TRUE(wait_until([&]() {
    env->period_calc_disk_usage();
    return env->gc_stall_stopped_;
  }));
  ASSERT_FALSE(env->check_disk_space_enough());

  class BlockingPool : public ILogBlockPool
  {
  public:
    explicit BlockingPool(ILogBlockPool *pool) : pool_(pool), entered_(false), release_(false) {}
    int create_block(const char *path, const int64_t size) override
    { return pool_->create_block(path, size); }
    int remove_block(const char *path) override
    {
      entered_.store(true);
      while (!release_.load()) {
        ob_usleep(1000);
      }
      return pool_->remove_block(path);
    }
    ILogBlockPool *pool_;
    std::atomic<bool> entered_;
    std::atomic<bool> release_;
  } pool(target->log_engine_.log_storage_.block_mgr_.log_block_pool_);
  target->log_engine_.log_storage_.block_mgr_.log_block_pool_ = &pool;
  DEFER({
    pool.release_.store(true);
    EXPECT_TRUE(wait_until([&]() {
      PalfHandleImpl::RLockGuard guard(target->lock_);
      return !target->sw_.is_rebuilding_;
    }));
    target->log_engine_.log_storage_.block_mgr_.log_block_pool_ = pool.pool_;
    EXPECT_EQ(OB_SUCCESS, target->enable_sync());
  });
  ASSERT_EQ(OB_SUCCESS, target->advance_base_info(base_info, true));
  ASSERT_TRUE(wait_until([&]() {
    return pool.entered_.load() && new_base == target->get_base_lsn_used_for_block_gc();
  }));
  EXPECT_EQ(old_tail, target->log_engine_.log_storage_.get_end_lsn());
  int64_t physical = 0;
  int64_t rebuild_usage = 0;
  int64_t healthy_usage = 0;
  ASSERT_EQ(OB_SUCCESS, target->get_total_used_disk_space(physical, rebuild_usage));
  EXPECT_EQ(PALF_META_BLOCK_SIZE + MAX_INFO_BLOCK_SIZE, rebuild_usage);
  ASSERT_EQ(OB_SUCCESS, other->get_total_used_disk_space(physical, healthy_usage));
  EXPECT_GT(healthy_usage, 80 * MB);
  int64_t total_usage = 0;
  int64_t unused_id = 0;
  int64_t unused_size = 0;
  ASSERT_EQ(OB_SUCCESS, env->get_disk_usage_(physical, total_usage, unused_id, unused_size));
  EXPECT_EQ(healthy_usage + rebuild_usage, total_usage);
  env->period_calc_disk_usage();
  EXPECT_TRUE(env->gc_stall_stopped_);
  EXPECT_FALSE(env->check_disk_space_enough());
  EXPECT_EQ(3, service.pending_blocks_);

  pool.release_.store(true);
  ASSERT_TRUE(wait_until([&]() {
    PalfHandleImpl::RLockGuard guard(target->lock_);
    return !target->sw_.is_rebuilding_;
  }));
  EXPECT_EQ(new_base, target->log_engine_.log_storage_.get_end_lsn());
  EXPECT_EQ(0, service.pending_blocks_);
  env->period_calc_disk_usage();
  EXPECT_FALSE(env->gc_stall_stopped_);
  EXPECT_TRUE(env->check_disk_space_enough());
  ASSERT_EQ(OB_SUCCESS, target->enable_sync());
  ASSERT_EQ(OB_SUCCESS, check_replica_sync(rebuild_id, &leader, rebuilding[target_idx], TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 1, rebuild_id, MB));
  ASSERT_EQ(OB_SUCCESS, check_replica_sync(rebuild_id, &leader, rebuilding[target_idx], TIMEOUT_US));
}

TEST_F(TestObSimpleLogClusterRebuild, test_leader_cannot_rebuild)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_leader_cannot_rebuild");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start test_leader_cannot_rebuild", K(id));
  int64_t leader_idx = 0;
  unittest::PalfHandleImplGuard leader;
  unittest::PalfHandleImplGuard *rebuild_server = NULL;
  std::vector<PalfHandleImplGuard*> palf_list;
  int64_t follower_idx1, follower_idx2;
  palflite::PalfHandleLiteLeaderChanger leader_changer;
  palf::PalfRoleChangeCbNode rc_cb_node(&leader_changer);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, leader_changer.init(&(leader.palf_handle_impl_->election_)));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->role_change_cb_wrpper_.add_cb_impl(&rc_cb_node));

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

  LSN base_lsn = LSN(0);
  PalfBaseInfo base_info;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(base_lsn, base_info));

  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->disable_sync());
  EXPECT_EQ(OB_STATE_NOT_MATCH, leader.palf_handle_impl_->advance_base_info(base_info, true));

  AccessMode curr_access_mode;
  int64_t mode_version, proposal_id;
  proposal_id = leader.palf_handle_impl_->state_mgr_.get_proposal_id();
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, AccessMode::RAW_WRITE, share::SCN::min_scn()));

  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->disable_vote(false));
  while(FOLLOWER != leader.palf_handle_impl_->state_mgr_.role_)
  {
    leader_changer.change_leader();
  }
  unittest::PalfHandleImplGuard new_leader;
  int64_t new_leader_idx = 0;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  leader.reset();
  new_leader.reset();
  EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  PALF_LOG(INFO, "end test_leader_cannot_rebuild", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
