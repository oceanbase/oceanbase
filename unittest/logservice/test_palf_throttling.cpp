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
#include "common/ob_clock_generator.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/utility/ob_tracepoint.h"

#define private public
#include "logservice/palf/palf_env.h"
#include "logservice/palf/log_io_task.h"
#include "logservice/ob_server_log_block_mgr.h"
#undef private
#include "logservice/palf/log_block_pool_interface.h"
#include "share/config/ob_server_config.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace palf;

namespace unittest
{

class TestLogBlockReclaimCoordinator : public ILogBlockReclaimCoordinator
{
public:
  int try_advance_reclaim(const int64_t) override { return OB_SUCCESS; }
  LogReclaimState get_reclaim_state() const override { return state_; }
  bool need_reclaim() const override { return need_reclaim_; }
  bool is_pool_write_stopped() const override { return write_stopped_; }
  LogReclaimState state_ = LogReclaimState::DISABLED;
  bool need_reclaim_ = false;
  bool write_stopped_ = false;
};

class TestPalfThrottling : public ::testing::Test
{
public:
  TestPalfThrottling();
  virtual ~TestPalfThrottling();
  virtual void SetUp();
  virtual void TearDown();
protected:
  PalfDiskOptions get_gc_disk_options() const
  {
    PalfDiskOptions opts;
    opts.log_disk_usage_limit_size_ = 1L << 40;
    opts.log_disk_utilization_threshold_ = 80;
    opts.log_disk_utilization_limit_threshold_ = 95;
    opts.log_disk_throttling_percentage_ = 60;
    opts.log_disk_throttling_maximum_duration_ = 1 * 1000 * 1000L;
    opts.log_writer_parallelism_ = 1;
    return opts;
  }
  bool g_need_purging_throttling;
  NeedPurgingThrottlingFunc g_need_purging_throttling_func;
};

TestPalfThrottling::TestPalfThrottling() : g_need_purging_throttling(false) {
  g_need_purging_throttling_func = [this](){ return g_need_purging_throttling; };
}

TestPalfThrottling::~TestPalfThrottling()
{
}

void TestPalfThrottling::SetUp()
{
  //ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
  // init MTL
  //ObTenantBase tbase(1001);
  //ObTenantEnv::set_tenant(&tbase);
}

void TestPalfThrottling::TearDown()
{
  PALF_LOG(INFO, "TestPalfThrottling has TearDown");
  //ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
}

class TestLogDiskUsage : public TestPalfThrottling
{
public:
  TestLogDiskUsage() : TestPalfThrottling(), healthy_(), rebuilding_() {}
  virtual ~TestLogDiskUsage() {}
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1012));
  }
  static void TearDownTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, ObMallocAllocator::get_instance()->recycle_tenant_allocator(1012));
  }
protected:
  PalfHandleImpl *add_log_stream(PalfEnvImpl &env, const int64_t palf_id)
  {
    PalfHandleImpl *handle = PalfHandleImplFactory::alloc();
    if (NULL != handle) {
      const int ret = env.palf_handle_impl_map_.insert_and_get(LSKey(palf_id), handle);
      EXPECT_EQ(OB_SUCCESS, ret);
      if (OB_SUCCESS == ret) {
        env.palf_handle_impl_map_.revert(handle);
      } else {
        PalfHandleImplFactory::free(handle);
        handle = NULL;
      }
    }
    return handle;
  }

  void expect_submit_stopped(PalfHandleImpl &handle, PalfEnvImpl &env)
  {
    PalfAppendOptions opts;
    LSN lsn;
    SCN scn;
    handle.is_inited_ = true;
    handle.palf_env_impl_ = &env;
    EXPECT_EQ(OB_LOG_OUTOF_DISK_SPACE,
              handle.submit_log(opts, "log", 3, SCN::min_scn(), lsn, scn));
    handle.is_inited_ = false;
    handle.palf_env_impl_ = NULL;
  }

  void set_log_range(PalfHandleImpl &handle, const LSN &base_lsn,
                     const LSN &end_lsn, const block_id_t first_block_id)
  {
    LogEngine &engine = handle.log_engine_;
    LogStorage &storage = engine.log_storage_;
    LogStorage &meta = engine.log_meta_storage_;
    storage.is_inited_ = true;
    storage.logical_block_size_ = PALF_BLOCK_SIZE;
    storage.log_tail_ = end_lsn;
    storage.block_mgr_.is_inited_ = true;
    storage.block_mgr_.min_block_id_ = first_block_id;
    storage.block_mgr_.max_block_id_ = end_lsn.val_ > first_block_id * PALF_BLOCK_SIZE
        ? lsn_2_block(end_lsn - 1, PALF_BLOCK_SIZE) + 1 : first_block_id;
    meta.is_inited_ = true;
    meta.logical_block_size_ = PALF_META_BLOCK_SIZE;
    meta.block_mgr_.is_inited_ = true;
    meta.block_mgr_.min_block_id_ = 0;
    meta.block_mgr_.max_block_id_ = 1;
    engine.base_lsn_for_block_gc_ = base_lsn;
  }
  PalfHandleImpl healthy_;
  PalfHandleImpl rebuilding_;
};

TEST_F(TestLogDiskUsage, gc_base_snapshot_survives_later_update)
{
  LogEngine &engine = healthy_.log_engine_;
  const IPalfHandleImpl &handle = healthy_;
  const LSN initial_base(PALF_BLOCK_SIZE);
  const LSN updated_base(2 * PALF_BLOCK_SIZE);
  ASSERT_EQ(OB_SUCCESS, engine.update_base_lsn_used_for_gc(initial_base));
  // Binding the result to a const reference must not expose the mutable GC base.
  const LSN &engine_snapshot = engine.get_base_lsn_used_for_block_gc();
  const LSN &handle_snapshot = handle.get_base_lsn_used_for_block_gc();
  ASSERT_EQ(OB_SUCCESS, engine.update_base_lsn_used_for_gc(updated_base));
  EXPECT_EQ(initial_base, engine_snapshot);
  EXPECT_EQ(initial_base, handle_snapshot);
  EXPECT_EQ(updated_base, engine.get_base_lsn_used_for_block_gc());
  EXPECT_EQ(updated_base, handle.get_base_lsn_used_for_block_gc());
}

TEST_F(TestLogDiskUsage, gc_boundary_includes_only_existing_blocks)
{
  const int64_t block_size = PALF_BLOCK_SIZE;
  const int64_t meta_size = PALF_META_BLOCK_SIZE + MAX_INFO_BLOCK_SIZE;
  const int64_t base_lsns[] = {0, 128, block_size, 2 * block_size, 2 * block_size + 128};
  const int64_t expected_sizes[] = {4 * block_size + 128, 4 * block_size + 128,
      4 * block_size + 128, 3 * block_size + 128, 3 * block_size + 128};
  int64_t physical_size = 0;
  int64_t unrecyclable_size = 0;
  set_log_range(healthy_, LSN(0), LSN(4 * block_size + 128), 0);
  for (int64_t i = 0; i < ARRAYSIZEOF(base_lsns); ++i) {
    SCOPED_TRACE(base_lsns[i]);
    ASSERT_EQ(OB_SUCCESS, healthy_.after_flush_snapshot_meta_(LSN(base_lsns[i])));
    ASSERT_EQ(OB_SUCCESS, healthy_.get_total_used_disk_space(physical_size, unrecyclable_size));
    EXPECT_EQ(expected_sizes[i] + meta_size, unrecyclable_size);
  }

  // A migrated LS may start at the base block without its predecessor.
  set_log_range(healthy_, LSN(3 * block_size), LSN(4 * block_size + 128), 3);
  ASSERT_EQ(OB_SUCCESS, healthy_.get_total_used_disk_space(physical_size, unrecyclable_size));
  EXPECT_EQ(block_size + 128 + meta_size, unrecyclable_size);
}

TEST_F(TestLogDiskUsage, empty_log_range_counts_only_meta)
{
  const int64_t meta_size = PALF_META_BLOCK_SIZE + MAX_INFO_BLOCK_SIZE;
  int64_t physical_size = 0;
  int64_t unrecyclable_size = 0;
  set_log_range(healthy_, LSN(0), LSN(0), 0);
  ASSERT_EQ(OB_SUCCESS, healthy_.get_total_used_disk_space(physical_size, unrecyclable_size));
  EXPECT_EQ(meta_size, physical_size);
  EXPECT_EQ(meta_size, unrecyclable_size);

  set_log_range(healthy_, LSN(5 * PALF_BLOCK_SIZE), LSN(5 * PALF_BLOCK_SIZE), 5);
  ASSERT_EQ(OB_SUCCESS, healthy_.get_total_used_disk_space(physical_size, unrecyclable_size));
  EXPECT_EQ(meta_size, physical_size);
  EXPECT_EQ(meta_size, unrecyclable_size);

  // Truncate can publish a nonzero empty tail before the snapshot callback advances the GC base.
  set_log_range(healthy_, LSN(0), LSN(369 * PALF_BLOCK_SIZE), 369);
  ASSERT_EQ(OB_SUCCESS, healthy_.get_total_used_disk_space(physical_size, unrecyclable_size));
  EXPECT_EQ(meta_size, physical_size);
  EXPECT_EQ(meta_size, unrecyclable_size);
}

TEST_F(TestLogDiskUsage, single_block_range_is_not_empty)
{
  const int64_t meta_size = PALF_META_BLOCK_SIZE + MAX_INFO_BLOCK_SIZE;
  set_log_range(healthy_, LSN(0), LSN(369 * PALF_BLOCK_SIZE + 128), 369);
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  ASSERT_EQ(OB_SUCCESS,
            healthy_.log_engine_.log_storage_.get_block_id_range(min_block_id, max_block_id));
  ASSERT_EQ(min_block_id, max_block_id);
  int64_t physical_size = 0;
  int64_t unrecyclable_size = 0;
  ASSERT_EQ(OB_SUCCESS, healthy_.get_total_used_disk_space(physical_size, unrecyclable_size));
  EXPECT_EQ(128 + MAX_INFO_BLOCK_SIZE + meta_size, physical_size);
  EXPECT_EQ(128 + meta_size, unrecyclable_size);
}

TEST_F(TestLogDiskUsage, ddl_threshold_follows_effective_reclaim_mode)
{
  ObTenantBase tenant(1012);
  ObTenantSwitchGuard guard(&tenant);
  PalfEnvImpl env;
  TestLogBlockReclaimCoordinator coordinator;
  bool reached = true;
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("DDLUsageTest"));
  PalfDiskOptions opts = get_gc_disk_options();
  opts.log_disk_usage_limit_size_ = 10 * PALF_BLOCK_SIZE;
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.init(opts));
  ASSERT_EQ(OB_SUCCESS, env.tenant_reclaim_service_.init(true));
  env.is_inited_ = true;
  env.log_block_reclaim_coordinator_ = &coordinator;
  PalfHandleImpl *handle = add_log_stream(env, 1001);
  ASSERT_NE(nullptr, handle);
  set_log_range(*handle, LSN(19 * PALF_BLOCK_SIZE), LSN(20 * PALF_BLOCK_SIZE), 0);
  for (const LogReclaimState state : {LogReclaimState::ENABLED, LogReclaimState::DRAINING,
                                    LogReclaimState::DISABLED}) {
    coordinator.state_ = state;
    env.tenant_reclaim_service_.sync_reclaim_mode(state, false, false);
    ASSERT_EQ(OB_SUCCESS, env.check_log_disk_usage_reached_threshold(80, reached));
    EXPECT_EQ(LogReclaimState::DISABLED == state, reached);
    if (LogReclaimState::DRAINING == state) {
      env.tenant_reclaim_service_.sync_reclaim_mode(state, false, true);
      ASSERT_EQ(OB_SUCCESS, env.check_log_disk_usage_reached_threshold(80, reached));
      EXPECT_TRUE(reached);
    }
  }
  env.tenant_reclaim_service_.sync_reclaim_mode(LogReclaimState::ENABLED, false, false);
  // DDL measures real bytes even when ordinary writes stop for pool or GC pressure.
  coordinator.write_stopped_ = true;
  env.disk_options_wrapper_.gc_stalled_ = true;
  env.disk_options_wrapper_.cur_unrecyclable_log_disk_size_ = opts.log_disk_usage_limit_size_;
  EXPECT_FALSE(env.check_disk_space_enough());
  ASSERT_EQ(OB_SUCCESS, env.check_log_disk_usage_reached_threshold(80, reached));
  EXPECT_FALSE(reached);
}

TEST_F(TestLogDiskUsage, ddl_threshold_stops_at_boundary_and_resumes)
{
  ObTenantBase tenant(1012);
  ObTenantSwitchGuard guard(&tenant);
  PalfEnvImpl env;
  bool reached = false;
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("DDLBoundaryTest"));
  PalfDiskOptions opts = get_gc_disk_options();
  opts.log_disk_usage_limit_size_ = 10 * PALF_BLOCK_SIZE;
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.init(opts));
  ASSERT_EQ(OB_SUCCESS, env.tenant_reclaim_service_.init(true));
  env.is_inited_ = true;
  PalfHandleImpl *handle = add_log_stream(env, 1001);
  ASSERT_NE(nullptr, handle);
  const int64_t meta_size = PALF_META_BLOCK_SIZE + MAX_INFO_BLOCK_SIZE;
  const int64_t boundary = opts.log_disk_usage_limit_size_ / 100 * 80
      + opts.log_disk_usage_limit_size_ % 100 * 80 / 100;
  for (const int64_t offset : {-1, 0, 1, -1}) {
    set_log_range(*handle, LSN(0), LSN(boundary - meta_size + offset), 0);
    ASSERT_EQ(OB_SUCCESS, env.check_log_disk_usage_reached_threshold(80, reached));
    EXPECT_EQ(offset >= 0, reached);
  }
  ASSERT_EQ(OB_SUCCESS, env.check_log_disk_usage_reached_threshold(0, reached));
  EXPECT_TRUE(reached);
  ASSERT_EQ(OB_SUCCESS, env.check_log_disk_usage_reached_threshold(100, reached));
  EXPECT_FALSE(reached);
}

TEST_F(TestLogDiskUsage, ddl_threshold_uses_shrinking_target)
{
  ObTenantBase tenant(1012);
  ObTenantSwitchGuard guard(&tenant);
  PalfEnvImpl env;
  bool reached = false;
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("DDLShrinkTest"));
  PalfDiskOptions opts = get_gc_disk_options();
  opts.log_disk_usage_limit_size_ = 20 * PALF_BLOCK_SIZE;
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.init(opts));
  ASSERT_EQ(OB_SUCCESS, env.tenant_reclaim_service_.init(true));
  env.is_inited_ = true;
  PalfHandleImpl *handle = add_log_stream(env, 1001);
  ASSERT_NE(nullptr, handle);
  set_log_range(*handle, LSN(0), LSN(8 * PALF_BLOCK_SIZE), 0);
  ASSERT_EQ(OB_SUCCESS, env.check_log_disk_usage_reached_threshold(80, reached));
  EXPECT_FALSE(reached);
  opts.log_disk_usage_limit_size_ = 10 * PALF_BLOCK_SIZE;
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.update_disk_options(opts));
  EXPECT_EQ(20 * PALF_BLOCK_SIZE,
            env.disk_options_wrapper_.get_disk_opts_for_stopping_writing().log_disk_usage_limit_size_);
  ASSERT_EQ(OB_SUCCESS, env.check_log_disk_usage_reached_threshold(80, reached));
  EXPECT_TRUE(reached);
}

TEST_F(TestLogDiskUsage, ddl_threshold_rejects_invalid_inputs)
{
  PalfEnvImpl env;
  bool reached = true;
  EXPECT_EQ(OB_NOT_INIT, env.check_log_disk_usage_reached_threshold(80, reached));
  EXPECT_FALSE(reached);
  env.is_inited_ = true;
  for (const int64_t threshold : {-1, 101}) {
    reached = true;
    EXPECT_EQ(OB_INVALID_ARGUMENT, env.check_log_disk_usage_reached_threshold(threshold, reached));
    EXPECT_FALSE(reached);
  }
  for (const int64_t limit : {0, -1}) {
    env.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_ = limit;
    reached = true;
    EXPECT_EQ(OB_ERR_UNEXPECTED, env.check_log_disk_usage_reached_threshold(80, reached));
    EXPECT_FALSE(reached);
  }
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("DDLOverflowTest"));
  env.disk_options_wrapper_.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_ = INT64_MAX;
  reached = true;
  EXPECT_EQ(OB_SIZE_OVERFLOW, env.check_log_disk_usage_reached_threshold(80, reached));
  EXPECT_FALSE(reached);
}

TEST_F(TestPalfThrottling, disk_options_snapshots_survive_update)
{
  PalfDiskOptionsWrapper wrapper;
  PalfDiskOptions opts = get_gc_disk_options();
  ASSERT_EQ(OB_SUCCESS, wrapper.init(opts));
  const PalfDiskOptions &stop_snapshot = wrapper.get_disk_opts_for_stopping_writing();
  const PalfDiskOptions &recycle_snapshot = wrapper.get_disk_opts_for_recycling_blocks();
  const int64_t original_limit = opts.log_disk_usage_limit_size_;
  opts.log_disk_usage_limit_size_ /= 2;
  ASSERT_EQ(OB_SUCCESS, wrapper.update_disk_options(opts));
  EXPECT_EQ(original_limit, recycle_snapshot.log_disk_usage_limit_size_);
  wrapper.change_to_normal(wrapper.sequence_);
  EXPECT_EQ(original_limit, stop_snapshot.log_disk_usage_limit_size_);
  EXPECT_EQ(opts.log_disk_usage_limit_size_,
            wrapper.get_disk_opts_for_stopping_writing().log_disk_usage_limit_size_);
}

TEST_F(TestLogDiskUsage, capacity_stop_survives_empty_or_ahead_gc_base)
{
  ObTenantBase tenant(1012);
  ObTenantSwitchGuard guard(&tenant);
  PalfEnvImpl env;
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("DiskUsageTest"));
  PalfDiskOptions opts = get_gc_disk_options();
  opts.log_disk_usage_limit_size_ = 10 * PALF_BLOCK_SIZE;
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.init(opts));
  ASSERT_EQ(OB_SUCCESS, env.tenant_reclaim_service_.init(true));
  PalfHandleImpl *healthy = add_log_stream(env, 1);
  PalfHandleImpl *rebuilding = add_log_stream(env, 1003);
  ASSERT_NE(nullptr, healthy);
  ASSERT_NE(nullptr, rebuilding);
  set_log_range(*healthy, LSN(0), LSN(0), 0);
  set_log_range(*rebuilding, LSN(0), LSN(369 * PALF_BLOCK_SIZE), 369);
  env.period_calc_disk_usage();
  ASSERT_TRUE(env.check_disk_space_enough());

  // The other LS exceeds the tenant limit even while the rebuilding LS has no redo.
  set_log_range(*healthy, LSN(0), LSN(9 * PALF_BLOCK_SIZE + 128), 0);
  env.period_calc_disk_usage();
  EXPECT_FALSE(env.diskspace_enough_);
  expect_submit_stopped(*healthy, env);

  // An advanced GC base must not subtract from another LS's usage.
  set_log_range(*rebuilding, LSN(10 * PALF_BLOCK_SIZE), LSN(2 * PALF_BLOCK_SIZE), 0);
  env.period_calc_disk_usage();
  EXPECT_FALSE(env.check_disk_space_enough());
  expect_submit_stopped(*healthy, env);

  ASSERT_EQ(OB_SUCCESS, healthy->after_flush_snapshot_meta_(LSN(9 * PALF_BLOCK_SIZE)));
  env.period_calc_disk_usage();
  EXPECT_TRUE(env.check_disk_space_enough());
}

TEST_F(TestLogDiskUsage, meta_only_usage_can_stop_writes)
{
  ObTenantBase tenant(1012);
  ObTenantSwitchGuard guard(&tenant);
  PalfEnvImpl env;
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("MetaUsageTest"));
  PalfDiskOptions opts = get_gc_disk_options();
  opts.log_disk_usage_limit_size_ = 2 * PALF_BLOCK_SIZE;
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.init(opts));
  ASSERT_EQ(OB_SUCCESS, env.tenant_reclaim_service_.init(true));
  PalfHandleImpl *first = add_log_stream(env, 1);
  PalfHandleImpl *second = add_log_stream(env, 1003);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  set_log_range(*first, LSN(0), LSN(0), 0);
  set_log_range(*second, LSN(0), LSN(369 * PALF_BLOCK_SIZE), 369);
  // Both shared and dedicated tenant accounting must include the meta blocks.
  for (const bool shared : {true, false}) {
    env.tenant_reclaim_service_.shared_reclaim_enabled_ = shared;
    env.period_calc_disk_usage();
    EXPECT_FALSE(env.diskspace_enough_);
    expect_submit_stopped(*first, env);
  }
}

TEST_F(TestLogDiskUsage, empty_redo_does_not_bypass_pool_stop)
{
  ObTenantBase tenant(1012);
  ObTenantSwitchGuard guard(&tenant);
  logservice::ObServerLogBlockMgr pool;
  pool.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, pool.reclaim_service_.init(&pool));
  ATOMIC_STORE64(&pool.reclaim_service_.reclaim_state_, LogReclaimState::ENABLED);
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
  pool.total_block_cnt_ = 100;
  pool.min_block_id_ = 0;
  PalfEnvImpl env;
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("PoolStopTest"));
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.init(get_gc_disk_options()));
  ASSERT_EQ(OB_SUCCESS, env.tenant_reclaim_service_.init(true));
  env.log_block_reclaim_coordinator_ = &pool.reclaim_service_;
  PalfHandleImpl *handle = add_log_stream(env, 1003);
  ASSERT_NE(nullptr, handle);
  set_log_range(*handle, LSN(0), LSN(369 * PALF_BLOCK_SIZE), 369);
  for (const int64_t used : {94, 95, 100, 94}) {
    pool.block_cnt_in_use_ = used;
    pool.max_block_id_ = 100 - used;
    pool.reclaim_service_.refresh_pool_write_stopped();
    env.period_calc_disk_usage();
    EXPECT_TRUE(env.diskspace_enough_);
    EXPECT_EQ(used < 95, env.check_disk_space_enough());
    if (used >= 95) {
      expect_submit_stopped(*handle, env);
    }
  }
}

TEST_F(TestLogDiskUsage, rebuild_usage_preserves_other_ls_usage_and_stall_stop)
{
  const int64_t block_size = PALF_BLOCK_SIZE;
  const int64_t meta_size = PALF_META_BLOCK_SIZE + MAX_INFO_BLOCK_SIZE;
  int64_t physical_size = 0;
  int64_t unrecyclable_size = 0;
  set_log_range(healthy_, LSN(0), LSN(4 * block_size), 0);
  set_log_range(rebuilding_, LSN(0), LSN(2 * block_size), 0);
  // The snapshot callback can advance the GC base before truncate updates the storage tail.
  ASSERT_EQ(OB_SUCCESS, rebuilding_.after_flush_snapshot_meta_(LSN(10 * block_size)));
  ASSERT_EQ(OB_SUCCESS, rebuilding_.get_total_used_disk_space(physical_size, unrecyclable_size));
  EXPECT_EQ(meta_size, unrecyclable_size);

  const LSKey healthy_key(1);
  const LSKey rebuilding_key(2);
  PalfEnvImpl::GetTotalUsedDiskSpace total;
  ASSERT_TRUE(total(healthy_key, &healthy_));
  ASSERT_TRUE(total(rebuilding_key, &rebuilding_));
  EXPECT_EQ(4 * block_size + 2 * meta_size, total.total_unrecyclable_disk_space_);

  PalfDiskOptions opts = get_gc_disk_options();
  opts.log_disk_usage_limit_size_ = 4 * block_size;
  PalfDiskOptionsWrapper wrapper;
  bool need_throttling = false;
  bool stopped = false;
  ASSERT_EQ(OB_SUCCESS, wrapper.init(opts));
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      4 * block_size + meta_size, opts.log_disk_throttling_maximum_duration_,
      need_throttling, stopped));
  ASSERT_TRUE(stopped);
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      total.total_unrecyclable_disk_space_, opts.log_disk_throttling_maximum_duration_,
      need_throttling, stopped));
  EXPECT_TRUE(need_throttling);
  EXPECT_TRUE(stopped);
}

TEST_F(TestPalfThrottling, test_gc_stall_timestamp)
{
  TenantLogBlockReclaimService service;
  const int64_t grace_us = TenantLogBlockReclaimService::GC_STALL_GRACE_US;
  service.shared_reclaim_enabled_ = true;
  service.record_gc_progress();
  service.begin_gc_work_(10 * grace_us);
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.get_gc_stall_start_ts());

  const int64_t now_us = ObTimeUtility::current_monotonic_time();
  const int64_t stalled_since_ts = now_us - grace_us;
  service.pending_blocks_ = 3;
  service.begin_gc_work_(stalled_since_ts - grace_us);
  service.begin_gc_work_(now_us);
  EXPECT_EQ(stalled_since_ts, service.get_gc_stall_start_ts());
  service.record_gc_progress();
  EXPECT_EQ(2, service.pending_blocks_);
  EXPECT_EQ(stalled_since_ts, service.get_gc_stall_start_ts());
  // Consecutive successes must keep the first stall while one assigned block remains.
  service.record_gc_progress();
  service.begin_gc_work_(now_us);
  EXPECT_EQ(1, service.pending_blocks_);
  EXPECT_EQ(stalled_since_ts, service.get_gc_stall_start_ts());
  service.record_gc_progress();
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.get_gc_stall_start_ts());

  // A new single-block round has its own grace period and ends on its only success.
  const int64_t next_start = ObTimeUtility::current_monotonic_time();
  service.pending_blocks_ = 1;
  service.begin_gc_work_(next_start);
  EXPECT_EQ(next_start + grace_us, service.get_gc_stall_start_ts());
  service.record_gc_progress();
  service.record_gc_progress();
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.get_gc_stall_start_ts());

  // Healthy progress uses a timestamp sampled while holding the service lock.
  service.pending_blocks_ = 2;
  ATOMIC_STORE(&service.gc_stall_start_ts_, INT64_MAX);
  const int64_t before_progress_ts = ObTimeUtility::current_monotonic_time();
  service.record_gc_progress();
  const int64_t after_progress_ts = ObTimeUtility::current_monotonic_time();
  const int64_t next_stall_start_ts = service.get_gc_stall_start_ts();
  EXPECT_GE(next_stall_start_ts, before_progress_ts + grace_us);
  EXPECT_LE(next_stall_start_ts, after_progress_ts + grace_us);
  service.record_gc_progress();
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.get_gc_stall_start_ts());
  service.pending_blocks_ = 2;
  service.begin_gc_work_(next_start);
  service.reset();
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.get_gc_stall_start_ts());
}

TEST_F(TestPalfThrottling, test_local_reclaim_does_not_track_gc_stall)
{
  ObTenantBase tenant(1007);
  ObTenantSwitchGuard guard(&tenant);
  TenantLogBlockReclaimService service;
  ASSERT_EQ(OB_SUCCESS, service.init(/* shared_reclaim_enabled */ false));
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(1));
  service.begin_gc_work_(ObTimeUtility::current_monotonic_time());
  service.record_gc_progress();
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.get_gc_stall_start_ts());
}

TEST_F(TestPalfThrottling, test_gc_progress_public_interface)
{
  PalfEnv env;
  PalfEnv other_env;
  const int64_t grace_us = TenantLogBlockReclaimService::GC_STALL_GRACE_US;
  const int64_t now_us = ObTimeUtility::current_monotonic_time();
  const int64_t stalled_since_ts = now_us - grace_us;
  env.palf_env_impl_.tenant_reclaim_service_.shared_reclaim_enabled_ = true;
  env.palf_env_impl_.tenant_reclaim_service_.pending_blocks_ = 2;
  other_env.palf_env_impl_.tenant_reclaim_service_.shared_reclaim_enabled_ = true;
  other_env.palf_env_impl_.tenant_reclaim_service_.pending_blocks_ = 1;
  env.palf_env_impl_.tenant_reclaim_service_.begin_gc_work_(stalled_since_ts - grace_us);
  other_env.palf_env_impl_.tenant_reclaim_service_.begin_gc_work_(stalled_since_ts - grace_us);
  env.record_gc_progress();
  EXPECT_EQ(1, env.palf_env_impl_.tenant_reclaim_service_.pending_blocks_);
  EXPECT_EQ(stalled_since_ts,
            env.palf_env_impl_.tenant_reclaim_service_.get_gc_stall_start_ts());
  env.record_gc_progress();
  EXPECT_EQ(OB_INVALID_TIMESTAMP,
            env.palf_env_impl_.tenant_reclaim_service_.get_gc_stall_start_ts());
  EXPECT_EQ(1, other_env.palf_env_impl_.tenant_reclaim_service_.pending_blocks_);
  EXPECT_EQ(stalled_since_ts,
            other_env.palf_env_impl_.tenant_reclaim_service_.get_gc_stall_start_ts());
}

TEST_F(TestPalfThrottling, test_gc_stall_pressure)
{
  PalfDiskOptions opts = get_gc_disk_options();
  opts.log_disk_usage_limit_size_ = 1000000;
  PalfDiskOptionsWrapper wrapper;
  ASSERT_EQ(OB_SUCCESS, wrapper.init(opts));
  TenantLogBlockReclaimService service;
  PalfThrottleOptions pressure;
  bool stopped = false;
  bool need_throttling = false;
  const int64_t since = 10 * TenantLogBlockReclaimService::GC_STALL_GRACE_US;
  const int64_t duration = opts.log_disk_throttling_maximum_duration_;
  service.shared_reclaim_enabled_ = true;
  service.pending_blocks_ = 3;
  service.begin_gc_work_(since - TenantLogBlockReclaimService::GC_STALL_GRACE_US);
  ASSERT_EQ(since, service.get_gc_stall_start_ts());
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      100, OB_INVALID_TIMESTAMP, need_throttling, stopped));
  wrapper.get_throttling_options(pressure);
  EXPECT_FALSE(pressure.gc_stalled_);
  EXPECT_EQ(100, pressure.unrecyclable_disk_space_);
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      100, 0, need_throttling, stopped));
  wrapper.get_throttling_options(pressure);
  EXPECT_TRUE(pressure.gc_stalled_);
  EXPECT_TRUE(pressure.need_throttling());
  EXPECT_FALSE(stopped);
  EXPECT_EQ(600001, pressure.unrecyclable_disk_space_);
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      100, duration / 2, need_throttling, stopped));
  wrapper.get_throttling_options(pressure);
  EXPECT_EQ(775000, pressure.unrecyclable_disk_space_);
  // Use fresh U, including arrivals after the previous sample.
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      800000, duration / 2, need_throttling, stopped));
  wrapper.get_throttling_options(pressure);
  EXPECT_EQ(875000, pressure.unrecyclable_disk_space_);
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      100, duration - 1, need_throttling, stopped));
  wrapper.get_throttling_options(pressure);
  EXPECT_FALSE(stopped);
  EXPECT_EQ(949999, pressure.unrecyclable_disk_space_);
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      100, duration, need_throttling, stopped));
  wrapper.get_throttling_options(pressure);
  EXPECT_TRUE(stopped);
  EXPECT_EQ(950000, pressure.unrecyclable_disk_space_);
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      950000, 0, need_throttling, stopped));
  EXPECT_TRUE(stopped);

  // Successes keep stalled pressure until every assigned block is returned.
  service.record_gc_progress();
  service.record_gc_progress();
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      100, duration + 1, need_throttling, stopped));
  wrapper.get_throttling_options(pressure);
  EXPECT_TRUE(stopped);
  EXPECT_TRUE(pressure.gc_stalled_);
  EXPECT_EQ(1, service.pending_blocks_);
  service.record_gc_progress();
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      950000, OB_INVALID_TIMESTAMP, need_throttling, stopped));
  wrapper.get_throttling_options(pressure);
  EXPECT_FALSE(stopped);
  EXPECT_FALSE(pressure.gc_stalled_);
  EXPECT_EQ(950000, pressure.unrecyclable_disk_space_);
  EXPECT_TRUE(pressure.need_throttling());

  // The interpolation product exceeds int64_t for large disks and long durations.
  wrapper.reset();
  opts.log_disk_usage_limit_size_ = 1L << 50;
  opts.log_disk_throttling_maximum_duration_ = 7200 * 1000 * 1000L;
  ASSERT_EQ(OB_SUCCESS, wrapper.init(opts));
  service.reset();
  service.shared_reclaim_enabled_ = true;
  service.pending_blocks_ = 1;
  service.begin_gc_work_(since - TenantLogBlockReclaimService::GC_STALL_GRACE_US);
  ASSERT_EQ(OB_SUCCESS, wrapper.try_update_throttling_disk_usage(
      0, opts.log_disk_throttling_maximum_duration_ / 2, need_throttling, stopped));
  wrapper.get_throttling_options(pressure);
  const int64_t start_size = opts.log_disk_usage_limit_size_ * 60 / 100 + 1;
  const int64_t stop_size = opts.log_disk_usage_limit_size_ * 95 / 100;
  EXPECT_EQ(start_size + (stop_size - start_size) / 2, pressure.unrecyclable_disk_space_);
  EXPECT_FALSE(stopped);
}

TEST_F(TestPalfThrottling, test_gc_stall_io_options)
{
  PalfEnvImpl env;
  env.is_inited_ = true;
  const PalfDiskOptions opts = get_gc_disk_options();
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.init(opts));
  LogWritingThrottle throttle;
  ASSERT_EQ(OB_SUCCESS, throttle.update_throttling_options(&env));
  const int64_t since = 10 * TenantLogBlockReclaimService::GC_STALL_GRACE_US;
  env.tenant_reclaim_service_.shared_reclaim_enabled_ = true;
  env.tenant_reclaim_service_.pending_blocks_ = 1;
  env.tenant_reclaim_service_.begin_gc_work_(
      since - TenantLogBlockReclaimService::GC_STALL_GRACE_US);
  bool stopped = false;
  bool need_throttling = false;
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.try_update_throttling_disk_usage(
      0, 0, need_throttling, stopped));
  throttle.notify_need_writing_throttling(true);
  throttle.notify_gc_stalled(true);
  // Freeze the periodic refresh gate so only the stalled transition can refresh options.
  throttle.last_update_ts_ = INT64_MAX - 2 * LogWritingThrottle::UPDATE_INTERVAL_US;
  EXPECT_TRUE(throttle.check_need_update_throtting_options_guarded_by_lock_());
  ASSERT_EQ(OB_SUCCESS, throttle.update_throttling_options(&env));
  EXPECT_TRUE(throttle.throttling_options_.gc_stalled_);
  ASSERT_EQ(OB_SUCCESS, throttle.throttling(1, g_need_purging_throttling_func, &env));
  EXPECT_EQ(1, throttle.stat_.total_throttling_interval_);
  EXPECT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(OB_SUCCESS, throttle.after_append_log(128));
  const int64_t base = throttle.throttling_options_.unrecyclable_disk_space_;
  bool freed = false;
  ASSERT_EQ(OB_SUCCESS, throttle.update_throtting_options_guarded_by_lock_(&env, freed));
  EXPECT_EQ(128, throttle.appended_log_size_cur_round_);
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.try_update_throttling_disk_usage(
      base + 128, 0, need_throttling, stopped));
  ASSERT_EQ(OB_SUCCESS, throttle.update_throtting_options_guarded_by_lock_(&env, freed));
  EXPECT_EQ(base + 128, throttle.throttling_options_.unrecyclable_disk_space_);
  EXPECT_EQ(0, throttle.appended_log_size_cur_round_);

  LogIOWorker worker;
  worker.throttle_ = &throttle;
  worker.need_ignoring_throttling_ = false;
  LogIOFlushLogTask log_task(1, 1);
  LogIOFlushMetaTask meta_task(1, 1);
  EXPECT_FALSE(worker.need_reduce_(&log_task));
  EXPECT_TRUE(meta_task.need_purge_throttling());
  // The entry gate does not reject work that has already entered the IO queue.
  env.gc_stall_stopped_ = true;
  EXPECT_FALSE(env.check_disk_space_enough());
  g_need_purging_throttling = true;
  ASSERT_EQ(OB_SUCCESS, throttle.throttling(1, g_need_purging_throttling_func, &env));
  EXPECT_EQ(1, throttle.stat_.total_skipped_task_cnt_);
  g_need_purging_throttling = false;
  ASSERT_EQ(OB_SUCCESS, throttle.throttling(1, g_need_purging_throttling_func, &env));
  EXPECT_EQ(2, throttle.stat_.total_throttling_task_cnt_);

  // Completing the single-block round refreshes IO even when raw pressure stays high.
  env.tenant_reclaim_service_.record_gc_progress();
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.try_update_throttling_disk_usage(
      base + 128, OB_INVALID_TIMESTAMP, need_throttling, stopped));
  throttle.notify_gc_stalled(false);
  throttle.last_update_ts_ = INT64_MAX - 2 * LogWritingThrottle::UPDATE_INTERVAL_US;
  EXPECT_TRUE(throttle.check_need_update_throtting_options_guarded_by_lock_());
  ASSERT_EQ(OB_SUCCESS, throttle.update_throttling_options(&env));
  EXPECT_FALSE(throttle.throttling_options_.gc_stalled_);
  EXPECT_TRUE(throttle.throttling_options_.need_throttling());
  env.tenant_reclaim_service_.reset();
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.try_update_throttling_disk_usage(
      0, OB_INVALID_TIMESTAMP, need_throttling, stopped));
  throttle.notify_need_writing_throttling(false);
  EXPECT_TRUE(worker.need_reduce_(&log_task));
  worker.need_ignoring_throttling_ = true;
  throttle.notify_need_writing_throttling(true);
  EXPECT_TRUE(worker.need_reduce_(&log_task));
}

TEST_F(TestPalfThrottling, test_gc_stall_public_gate)
{
  TestLogBlockReclaimCoordinator coordinator;
  PalfEnvImpl env;
  coordinator.state_ = LogReclaimState::ENABLED;
  coordinator.need_reclaim_ = true;
  env.log_block_reclaim_coordinator_ = &coordinator;
  env.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("GcStallTest"));
  const PalfDiskOptions opts = get_gc_disk_options();
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.init(opts));
  const int64_t now_us = ObTimeUtility::current_monotonic_time();
  env.tenant_reclaim_service_.is_inited_ = true;
  env.tenant_reclaim_service_.shared_reclaim_enabled_ = true;
  env.tenant_reclaim_service_.pending_blocks_ = 3;
  // Keep this test on the stall path; snapshot reconciliation is covered separately.
  env.tenant_reclaim_service_.last_summary_ts_ = INT64_MAX;
  env.tenant_reclaim_service_.begin_gc_work_(
      now_us - TenantLogBlockReclaimService::GC_STALL_GRACE_US
      - opts.log_disk_throttling_maximum_duration_);
  // LogLoop alone must observe expiry while the GC producer makes no further calls.
  env.period_calc_disk_usage();
  EXPECT_TRUE(env.diskspace_enough_);
  EXPECT_TRUE(env.gc_stall_stopped_);
  EXPECT_FALSE(env.check_disk_space_enough());
  // Assigned work remains relevant even when usage is below the ordinary recycle watermark.
  ASSERT_EQ(OB_SUCCESS, env.try_recycle_blocks());
  EXPECT_TRUE(env.gc_stall_stopped_);
  EXPECT_EQ(3, env.tenant_reclaim_service_.pending_blocks_);
  EXPECT_EQ(now_us - opts.log_disk_throttling_maximum_duration_,
            env.tenant_reclaim_service_.get_gc_stall_start_ts());
  env.tenant_reclaim_service_.record_gc_progress();
  env.tenant_reclaim_service_.record_gc_progress();
  env.period_calc_disk_usage();
  EXPECT_EQ(1, env.tenant_reclaim_service_.pending_blocks_);
  EXPECT_TRUE(env.gc_stall_stopped_);
  EXPECT_FALSE(env.check_disk_space_enough());
  // The last physical return ends the round without another try_recycle_blocks call.
  env.tenant_reclaim_service_.record_gc_progress();
  EXPECT_EQ(OB_INVALID_TIMESTAMP,
            env.tenant_reclaim_service_.get_gc_stall_start_ts());
  env.period_calc_disk_usage();
  EXPECT_FALSE(env.disk_options_wrapper_.is_gc_stalled());
  EXPECT_FALSE(env.gc_stall_stopped_);
  EXPECT_TRUE(env.check_disk_space_enough());
  // Clearing the stalled stop must not clear the existing capacity stop.
  env.diskspace_enough_ = false;
  EXPECT_FALSE(env.check_disk_space_enough());
}

TEST_F(TestPalfThrottling, test_gc_stall_log_loop_fast_path)
{
  TestLogBlockReclaimCoordinator coordinator;
  PalfEnvImpl env;
  env.log_block_reclaim_coordinator_ = &coordinator;
  env.is_inited_ = true;
  env.log_io_worker_wrapper_.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.init("GcStallTest"));
  const PalfDiskOptions opts = get_gc_disk_options();
  ASSERT_EQ(OB_SUCCESS, env.disk_options_wrapper_.init(opts));
  env.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(123);
  // Normal LogLoop samples leave the pressure update to GC.
  env.period_calc_disk_usage();
  EXPECT_EQ(123, env.disk_options_wrapper_.cur_unrecyclable_log_disk_size_);
  ASSERT_EQ(OB_SUCCESS, env.try_recycle_blocks());
  EXPECT_EQ(0, env.disk_options_wrapper_.cur_unrecyclable_log_disk_size_);

  const int64_t now_us = ObTimeUtility::current_monotonic_time();
  env.tenant_reclaim_service_.shared_reclaim_enabled_ = true;
  env.tenant_reclaim_service_.pending_blocks_ = 1;
  env.tenant_reclaim_service_.begin_gc_work_(
      now_us - TenantLogBlockReclaimService::GC_STALL_GRACE_US
      - opts.log_disk_throttling_maximum_duration_);
  // A busy publisher or options lock must preserve the last published state.
  {
    ObSpinLockGuard guard(env.throttling_update_lock_);
    env.period_calc_disk_usage();
  }
  EXPECT_FALSE(env.gc_stall_stopped_);
  {
    ObSpinLockGuard guard(env.disk_options_wrapper_.disk_opts_lock_);
    env.update_log_throttling_(0);
  }
  EXPECT_FALSE(env.gc_stall_stopped_);
  EXPECT_EQ(0, env.disk_options_wrapper_.cur_unrecyclable_log_disk_size_);
  {
    ObSpinLockGuard guard(env.tenant_reclaim_service_.lock_);
    env.update_log_throttling_(0);
  }
  EXPECT_TRUE(env.gc_stall_stopped_);
  EXPECT_TRUE(env.log_io_worker_wrapper_.throttle_.need_writing_throttling_notified());
  EXPECT_TRUE(env.log_io_worker_wrapper_.throttle_.gc_stalled_notified_);

  env.tenant_reclaim_service_.record_gc_progress();
  // Recovery must still run after the atomic GC marker becomes invalid.
  {
    ObSpinLockGuard guard(env.throttling_update_lock_);
    env.period_calc_disk_usage();
  }
  EXPECT_TRUE(env.gc_stall_stopped_);
  env.period_calc_disk_usage();
  EXPECT_FALSE(env.gc_stall_stopped_);
  EXPECT_FALSE(env.disk_options_wrapper_.is_gc_stalled());
  EXPECT_FALSE(env.log_io_worker_wrapper_.throttle_.need_writing_throttling_notified());
  EXPECT_FALSE(env.log_io_worker_wrapper_.throttle_.gc_stalled_notified_);
}

TEST_F(TestPalfThrottling, test_gc_delay_injection)
{
  PalfEnvImpl env;
  env.tenant_id_ = 1002;
  const int errsim_values[] = {2, -2};
  EventItem item;
  for (int64_t i = 0; i < 2; ++i) {
    item.error_code_ = errsim_values[i];
    item.occur_ = 1;
    ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_PALF_BLOCK_GC_DELAY", item));
    const int64_t start_ts = ObTimeUtility::current_monotonic_time();
    env.inject_gc_stall_(1, 0);
    EXPECT_GE(ObTimeUtility::current_monotonic_time() - start_ts, 2 * 1000);
  }
}

TEST_F(TestPalfThrottling, test_palf_options)
{
  char buf[64]  = {0};
  memset(buf, 0, 64);
  int64_t checksum = common::ob_crc64(buf, 64);
  PALF_LOG(INFO, "checksum", K(checksum));
  //test PalfDiskOptionsWrapper
  PalfDiskOptionsWrapper wrapper;
  ASSERT_EQ(false, wrapper.need_throttling());
  int64_t total_disk_size = 1024 * 1024 * 1024L;
  int64_t utilization_limit_threshold = 95;
  int64_t throttling_percentage = 60;
  wrapper.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  wrapper.disk_opts_for_stopping_writing_.log_disk_throttling_maximum_duration_ = 7200 * 1000 * 1000L;
  wrapper.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = total_disk_size;
  wrapper.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_ = 80;
  wrapper.disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_ = utilization_limit_threshold;
  wrapper.disk_opts_for_stopping_writing_.log_writer_parallelism_ = 1;
  int64_t unrecyclable_size = 0;
  wrapper.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  ASSERT_EQ(false, wrapper.need_throttling());
  unrecyclable_size = total_disk_size * 70 /100;
  wrapper.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  PALF_LOG(INFO, "test_palf_options trace", K(wrapper));
  ASSERT_EQ(true, wrapper.need_throttling());

  //test PalfThrottleOptions
  PalfThrottleOptions throttling_options;
  ASSERT_EQ(false, throttling_options.is_valid());
  ASSERT_EQ(false, throttling_options.need_throttling());
  ASSERT_EQ(0, throttling_options.get_available_size_after_limit());
  wrapper.get_throttling_options(throttling_options);
  ASSERT_EQ(true, throttling_options.is_valid());
  ASSERT_EQ(throttling_options.total_disk_space_, total_disk_size);
  ASSERT_EQ(throttling_options.stopping_writing_percentage_, utilization_limit_threshold);
  ASSERT_EQ(throttling_options.trigger_percentage_, throttling_percentage);
  ASSERT_EQ(throttling_options.unrecyclable_disk_space_, unrecyclable_size);
  ASSERT_EQ(true, throttling_options.need_throttling());
  ASSERT_EQ(total_disk_size * (utilization_limit_threshold - throttling_percentage)/100, throttling_options.get_available_size_after_limit());
}

TEST_F(TestPalfThrottling, test_log_disk_pressure)
{
  PalfDiskOptionsWrapper wrapper;
  PalfDiskOptions disk_options;
  disk_options.log_disk_usage_limit_size_ = 101;
  disk_options.log_disk_utilization_threshold_ = 80;
  disk_options.log_disk_utilization_limit_threshold_ = 95;
  disk_options.log_disk_throttling_percentage_ = 60;
  disk_options.log_disk_throttling_maximum_duration_ = 7200 * 1000 * 1000L;
  disk_options.log_writer_parallelism_ = 1;
  wrapper.disk_opts_for_stopping_writing_ = disk_options;
  wrapper.disk_opts_for_recycling_blocks_ = disk_options;

  // Writing throttling is already active at 70%, but disk pressure starts only
  // after crossing PALF GC's 80% recycling threshold.
  bool log_disk_under_pressure = false;
  wrapper.set_cur_unrecyclable_log_disk_size(70);
  ASSERT_TRUE(wrapper.need_throttling());
  ASSERT_EQ(OB_SUCCESS,
      wrapper.check_log_disk_under_pressure(log_disk_under_pressure));
  ASSERT_FALSE(log_disk_under_pressure);

  // Match PALF GC's floor division and strict-greater-than boundary.
  wrapper.set_cur_unrecyclable_log_disk_size(80);
  ASSERT_EQ(OB_SUCCESS,
      wrapper.check_log_disk_under_pressure(log_disk_under_pressure));
  ASSERT_FALSE(log_disk_under_pressure);
  wrapper.set_cur_unrecyclable_log_disk_size(81);
  ASSERT_EQ(OB_SUCCESS,
      wrapper.check_log_disk_under_pressure(log_disk_under_pressure));
  ASSERT_TRUE(log_disk_under_pressure);

  // During log-disk shrinking, PALF GC uses the recycling capacity rather than
  // the old stopping-writing capacity.
  wrapper.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = 1000;
  wrapper.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_ = 500;
  wrapper.set_cur_unrecyclable_log_disk_size(401);
  ASSERT_EQ(OB_SUCCESS,
      wrapper.check_log_disk_under_pressure(log_disk_under_pressure));
  ASSERT_TRUE(log_disk_under_pressure);

  // The threshold calculation must not overflow for a large configured quota.
  wrapper.disk_opts_for_recycling_blocks_.log_disk_usage_limit_size_ = INT64_MAX;
  const int64_t gc_trigger_size = INT64_MAX / 100 * 80
      + INT64_MAX % 100 * 80 / 100;
  wrapper.set_cur_unrecyclable_log_disk_size(gc_trigger_size);
  ASSERT_EQ(OB_SUCCESS,
      wrapper.check_log_disk_under_pressure(log_disk_under_pressure));
  ASSERT_FALSE(log_disk_under_pressure);
  wrapper.set_cur_unrecyclable_log_disk_size(gc_trigger_size + 1);
  ASSERT_EQ(OB_SUCCESS,
      wrapper.check_log_disk_under_pressure(log_disk_under_pressure));
  ASSERT_TRUE(log_disk_under_pressure);
}

TEST_F(TestPalfThrottling, test_server_write_stop)
{
  PalfEnvImpl palf_env_impl;
  bool has_recycled = false;
  int64_t oldest_palf_id = INVALID_PALF_ID;
  SCN oldest_scn;
  EXPECT_EQ(OB_ERR_UNEXPECTED, palf_env_impl.try_reclaim_blocks_by_mode_(
      /* local_reclaim_needed */ false, has_recycled, oldest_palf_id, oldest_scn));
  TestLogBlockReclaimCoordinator coordinator;
  palf_env_impl.log_block_reclaim_coordinator_ = &coordinator;
  ATOMIC_STORE(&palf_env_impl.diskspace_enough_, true);
  EXPECT_TRUE(palf_env_impl.check_disk_space_enough());
  coordinator.write_stopped_ = true;
  EXPECT_FALSE(palf_env_impl.check_disk_space_enough());
  coordinator.write_stopped_ = false;
  ATOMIC_STORE(&palf_env_impl.diskspace_enough_, false);
  EXPECT_FALSE(palf_env_impl.check_disk_space_enough());
}

TEST_F(TestPalfThrottling, test_tenant_reclaim_service)
{
  ObTenantBase tenant(1001);
  ObTenantSwitchGuard guard(&tenant);
  TenantLogBlockReclaimService service;
  ASSERT_EQ(OB_SUCCESS, service.init(/* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_INIT_TWICE, service.init(/* shared_reclaim_enabled */ true));
  bool need_update = false;
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.last_summary_ts_);
  ASSERT_EQ(OB_SUCCESS, service.check_need_update_snapshot(need_update));
  EXPECT_TRUE(need_update);
  EXPECT_GT(service.last_summary_ts_, 0);
  // Drive interval checks through the saved timestamp.
  const int64_t future_ts = ObClockGenerator::getClock() + TenantLogBlockReclaimService::SUMMARY_INTERVAL_US;
  service.last_summary_ts_ = future_ts;
  ASSERT_EQ(OB_SUCCESS, service.check_need_update_snapshot(need_update));
  EXPECT_FALSE(need_update);
  EXPECT_EQ(future_ts, service.last_summary_ts_);
  service.last_summary_ts_ = ObClockGenerator::getClock() - TenantLogBlockReclaimService::SUMMARY_INTERVAL_US;
  ASSERT_EQ(OB_SUCCESS, service.check_need_update_snapshot(need_update));
  EXPECT_TRUE(need_update);
  service.update_reclaim_snapshot(2, 1);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(4));
  EXPECT_EQ(3, service.pending_blocks_);
  service.record_gc_progress();
  const int64_t progress_ts = service.last_progress_ts_;
  EXPECT_EQ(2, service.pending_blocks_);
  EXPECT_GT(progress_ts, 0);

  TenantReclaimSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, service.get_reclaim_snapshot(
                            progress_ts, /* server_need_reclaim */ true, snapshot));
  EXPECT_EQ(tenant.id(), snapshot.get_tenant_id());
  EXPECT_EQ(progress_ts, snapshot.get_last_reclaim_progress_ts());
  service.last_summary_ts_ = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, service.check_need_update_snapshot(need_update));
  EXPECT_TRUE(need_update);
  EXPECT_FALSE(service.reclaim_suspended_);
  EXPECT_EQ(2, service.pending_blocks_);
  ASSERT_EQ(OB_SUCCESS, service.get_reclaim_snapshot(
                            progress_ts + TenantLogBlockReclaimService::SUMMARY_INTERVAL_US,
                            /* server_need_reclaim */ true, snapshot));
  EXPECT_EQ(0, snapshot.get_last_reclaim_progress_ts());
  EXPECT_TRUE(snapshot.is_shared_reclaim_enabled());
  EXPECT_EQ(0, snapshot.get_retention_expired_blocks());
  EXPECT_EQ(0, snapshot.get_retention_unexpired_blocks());
  EXPECT_TRUE(service.reclaim_suspended_);
  service.last_summary_ts_ = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, service.check_need_update_snapshot(need_update));
  EXPECT_TRUE(need_update);
  EXPECT_GT(service.last_summary_ts_, 0);

  service.sync_reclaim_mode(LogReclaimState::ENABLED, false, false);
  EXPECT_FALSE(service.reclaim_suspended_);
  EXPECT_EQ(0, service.pending_blocks_);
  service.sync_reclaim_mode(LogReclaimState::DRAINING, false, false);
  EXPECT_TRUE(service.is_shared_reclaim_enabled());
  service.sync_reclaim_mode(LogReclaimState::DRAINING, false, true);
  EXPECT_FALSE(service.is_shared_reclaim_enabled());
  service.reset();
  ASSERT_EQ(OB_NOT_INIT, service.get_reclaim_snapshot(1, /* server_need_reclaim */ false, snapshot));
  ASSERT_EQ(OB_NOT_INIT, service.check_need_update_snapshot(need_update));
  ASSERT_EQ(OB_NOT_INIT, service.apply_reclaim_quota(1));
  ASSERT_EQ(OB_SUCCESS, service.init(/* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, service.get_reclaim_snapshot(1, /* server_need_reclaim */ false, snapshot));
  EXPECT_EQ(tenant.id(), snapshot.get_tenant_id());
}

TEST_F(TestPalfThrottling, test_snapshot_refresh_retires_stale_quota)
{
  ObTenantBase tenant(1008);
  ObTenantSwitchGuard guard(&tenant);
  TenantLogBlockReclaimService service;
  TenantReclaimSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, service.init(/* shared_reclaim_enabled */ true));
  service.update_reclaim_snapshot(2, 0);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(2));
  const int64_t start_ts = service.last_progress_ts_;
  service.begin_gc_work_(start_ts);
  ASSERT_EQ(OB_SUCCESS, service.get_reclaim_snapshot(
                            start_ts + TenantLogBlockReclaimService::SUMMARY_INTERVAL_US,
                            /* server_need_reclaim */ true, snapshot));
  ASSERT_TRUE(service.reclaim_suspended_);

  service.update_reclaim_snapshot(1, 0);
  EXPECT_EQ(1, service.pending_blocks_);
  EXPECT_TRUE(service.reclaim_suspended_);
  service.update_reclaim_snapshot(0, 0);
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_FALSE(service.reclaim_suspended_);
  EXPECT_FALSE(service.can_recycle());
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.get_gc_stall_start_ts());
}

TEST_F(TestPalfThrottling, test_server_checks_tenant_reclaim_progress)
{
  ObTenantBase tenant(1002);
  ObTenantSwitchGuard guard(&tenant);
  PalfEnvImpl palf_env_impl;
  TenantLogBlockReclaimService &service = palf_env_impl.tenant_reclaim_service_;
  TenantReclaimSnapshot snapshot;
  const int64_t interval = TenantLogBlockReclaimService::SUMMARY_INTERVAL_US;
  ASSERT_EQ(OB_NOT_INIT,
            palf_env_impl.get_reclaim_snapshot(1, /* server_need_reclaim */ true, snapshot));
  ASSERT_EQ(OB_SUCCESS, service.init(/* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            palf_env_impl.get_reclaim_snapshot(0, /* server_need_reclaim */ true, snapshot));
  service.update_reclaim_snapshot(3, 0);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(3));
  const int64_t start_ts = service.last_progress_ts_;
  ASSERT_GT(start_ts, 0);
  EXPECT_EQ(3, service.pending_blocks_);
  EXPECT_EQ(start_ts + TenantLogBlockReclaimService::GC_STALL_GRACE_US,
            service.get_gc_stall_start_ts());
  ASSERT_EQ(OB_SUCCESS, palf_env_impl.get_reclaim_snapshot(
                            start_ts + interval - 1, /* server_need_reclaim */ true, snapshot));
  EXPECT_FALSE(service.reclaim_suspended_);
  EXPECT_TRUE(service.can_recycle());
  EXPECT_EQ(start_ts, snapshot.get_last_reclaim_progress_ts());

  // Reaching the timeout alone must not suspend a tenant without server pressure.
  ASSERT_EQ(OB_SUCCESS, palf_env_impl.get_reclaim_snapshot(
                            start_ts + interval, /* server_need_reclaim */ false, snapshot));
  EXPECT_FALSE(service.reclaim_suspended_);
  EXPECT_EQ(3, service.pending_blocks_);
  EXPECT_EQ(3, snapshot.get_retention_expired_blocks());
  EXPECT_EQ(start_ts, snapshot.get_last_reclaim_progress_ts());

  // A successful recycle resets the timeout observed by the server scan.
  service.record_gc_progress();
  const int64_t progress_ts = service.last_progress_ts_;
  EXPECT_EQ(2, service.pending_blocks_);
  ASSERT_EQ(OB_SUCCESS, palf_env_impl.get_reclaim_snapshot(
                            progress_ts, /* server_need_reclaim */ true, snapshot));
  EXPECT_TRUE(service.can_recycle());

  // No physical return or snapshot refresh occurs before the server checks again.
  ASSERT_EQ(OB_SUCCESS, palf_env_impl.get_reclaim_snapshot(
                            progress_ts + interval, /* server_need_reclaim */ true, snapshot));
  EXPECT_TRUE(service.reclaim_suspended_);
  EXPECT_TRUE(service.can_recycle());
  EXPECT_EQ(2, service.pending_blocks_);
  EXPECT_EQ(tenant.id(), snapshot.get_tenant_id());
  EXPECT_EQ(0, snapshot.get_last_reclaim_progress_ts());
  EXPECT_EQ(0, snapshot.get_retention_expired_blocks());
  // New rounds cannot cancel or replace the quota of a suspended tenant.
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(0));
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(20));
  EXPECT_EQ(2, service.pending_blocks_);
  EXPECT_EQ(2, service.pending_blocks_);
  EXPECT_TRUE(service.reclaim_suspended_);

  // The returning GC thread can finish the old round without delaying other tenants.
  service.record_gc_progress();
  EXPECT_EQ(1, service.pending_blocks_);
  EXPECT_EQ(1, service.retention_expired_blocks_);
  EXPECT_EQ(0, service.retention_unexpired_blocks_);
  EXPECT_TRUE(service.can_recycle());
  ASSERT_EQ(OB_SUCCESS, palf_env_impl.get_reclaim_snapshot(
                            progress_ts + interval, /* server_need_reclaim */ true, snapshot));
  EXPECT_EQ(0, snapshot.get_last_reclaim_progress_ts());
  EXPECT_EQ(0, snapshot.get_retention_expired_blocks());
  ASSERT_EQ(OB_SUCCESS, palf_env_impl.get_reclaim_snapshot(
                            progress_ts + 2 * interval, /* server_need_reclaim */ true, snapshot));
  EXPECT_TRUE(service.reclaim_suspended_);
  service.record_gc_progress();
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_FALSE(service.reclaim_suspended_);
  EXPECT_FALSE(service.can_recycle());
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.last_summary_ts_);
  bool need_update = false;
  ASSERT_EQ(OB_SUCCESS, service.check_need_update_snapshot(need_update));
  EXPECT_TRUE(need_update);
  service.update_reclaim_snapshot(1, 2);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(3));
  EXPECT_EQ(3, service.pending_blocks_);
  EXPECT_TRUE(service.can_recycle());
}

TEST_F(TestPalfThrottling, test_reclaim_mode_preserves_and_resets_progress)
{
  ObTenantBase tenant(1003);
  ObTenantSwitchGuard guard(&tenant);
  TenantLogBlockReclaimService service;
  TenantReclaimSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, service.init(/* shared_reclaim_enabled */ false));
  service.update_reclaim_snapshot(2, 3);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(5));
  EXPECT_FALSE(service.can_recycle());
  service.sync_reclaim_mode(LogReclaimState::ENABLED, true, false);
  EXPECT_TRUE(service.is_shared_reclaim_enabled());
  EXPECT_EQ(0, service.retention_expired_blocks_);
  EXPECT_EQ(0, service.retention_unexpired_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.last_summary_ts_);
  service.update_reclaim_snapshot(2, 3);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(4));
  service.record_gc_progress();
  const int64_t progress_ts = service.last_progress_ts_;
  service.sync_reclaim_mode(LogReclaimState::ENABLED, true, false);
  EXPECT_EQ(3, service.pending_blocks_);
  EXPECT_EQ(progress_ts, service.last_progress_ts_);
  EXPECT_EQ(1, service.retention_expired_blocks_);
  EXPECT_EQ(3, service.retention_unexpired_blocks_);
  EXPECT_EQ(3, service.pending_blocks_);
  EXPECT_EQ(progress_ts, service.last_progress_ts_);

  ASSERT_EQ(OB_SUCCESS, service.get_reclaim_snapshot(
                            progress_ts + TenantLogBlockReclaimService::SUMMARY_INTERVAL_US,
                            /* server_need_reclaim */ true, snapshot));
  service.sync_reclaim_mode(LogReclaimState::ENABLED, true, false);
  EXPECT_TRUE(service.reclaim_suspended_);
  service.update_reclaim_snapshot(10, 10);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(10));
  EXPECT_TRUE(service.can_recycle());
  EXPECT_EQ(3, service.pending_blocks_);
  service.sync_reclaim_mode(LogReclaimState::ENABLED, false, false);
  EXPECT_FALSE(service.reclaim_suspended_);
  EXPECT_EQ(0, service.last_progress_ts_);
  EXPECT_EQ(10, service.retention_expired_blocks_);
  EXPECT_EQ(10, service.retention_unexpired_blocks_);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(1));
  service.last_summary_ts_ = 100;
  service.sync_reclaim_mode(LogReclaimState::DRAINING, false, false);
  EXPECT_TRUE(service.is_shared_reclaim_enabled());
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(0, service.last_progress_ts_);
  service.sync_reclaim_mode(LogReclaimState::DRAINING, false, true);
  EXPECT_FALSE(service.is_shared_reclaim_enabled());
  EXPECT_EQ(0, service.retention_expired_blocks_);
  EXPECT_EQ(0, service.retention_unexpired_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.last_summary_ts_);
  service.sync_reclaim_mode(LogReclaimState::ENABLED, true, false);
  service.update_reclaim_snapshot(1, 1);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(2));
  service.sync_reclaim_mode(LogReclaimState::DISABLED, false, true);
  EXPECT_FALSE(service.can_recycle());
  EXPECT_FALSE(service.is_shared_reclaim_enabled());
  EXPECT_EQ(0, service.retention_expired_blocks_);
  EXPECT_EQ(0, service.retention_unexpired_blocks_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.last_summary_ts_);
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(0, service.last_progress_ts_);
}

TEST_F(TestPalfThrottling, test_reclaim_quota_completion)
{
  ObTenantBase tenant(1004);
  ObTenantSwitchGuard guard(&tenant);
  TenantLogBlockReclaimService service;
  ASSERT_EQ(OB_SUCCESS, service.init(/* shared_reclaim_enabled */ true));
  service.update_reclaim_snapshot(1, 1);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(10));
  EXPECT_EQ(2, service.pending_blocks_);
  const int64_t start_ts = service.last_progress_ts_;
  ASSERT_EQ(OB_INVALID_ARGUMENT, service.apply_reclaim_quota(-1));
  EXPECT_EQ(2, service.pending_blocks_);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(1));
  EXPECT_EQ(start_ts, service.last_progress_ts_);
  service.record_gc_progress();
  EXPECT_FALSE(service.can_recycle());
  TenantReclaimSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, service.get_reclaim_snapshot(
                            start_ts + 1, /* server_need_reclaim */ true, snapshot));
  EXPECT_EQ(0, snapshot.get_last_reclaim_progress_ts());
  ASSERT_EQ(OB_SUCCESS, service.get_reclaim_snapshot(
                            start_ts + 2 * TenantLogBlockReclaimService::SUMMARY_INTERVAL_US,
                            /* server_need_reclaim */ true, snapshot));
  EXPECT_FALSE(service.reclaim_suspended_);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(2));
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(0));
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(0, service.last_progress_ts_);
  EXPECT_FALSE(service.can_recycle());
}

TEST_F(TestPalfThrottling, test_retention_locate_failure_keeps_candidate_counts)
{
  ObTenantBase tenant(1005);
  ObTenantSwitchGuard guard(&tenant);
  SCN retention_scn;
  ASSERT_EQ(OB_SUCCESS, retention_scn.convert_from_ts(100));
  PalfEnvImpl::LogGetRecycableFileCandidate candidate(retention_scn);
  candidate.retention_expired_blocks_ = 2;
  candidate.retention_unexpired_blocks_ = 3;
  PalfHandleImpl handle;
  // An uninitialized handle returns a real locate error, not a retention boundary result.
  EXPECT_EQ(OB_NOT_INIT, candidate.collect_reclaim_blocks_(handle, 0, 5));
  EXPECT_EQ(2, candidate.retention_expired_blocks_);
  EXPECT_EQ(3, candidate.retention_unexpired_blocks_);
}

TEST_F(TestPalfThrottling, test_reclaim_candidate_null_handle_returns_error)
{
  PalfEnvImpl::LogGetRecycableFileCandidate candidate;
  EXPECT_FALSE(candidate(LSKey(1), NULL));
  EXPECT_EQ(OB_ERR_UNEXPECTED, candidate.ret_code_);
}

#if __ENABLE_TRACEPOINT__
TEST_F(TestPalfThrottling, test_retention_locate_expected_errors)
{
  ObTenantBase tenant(1006);
  ObTenantSwitchGuard guard(&tenant);
  SCN retention_scn;
  ASSERT_EQ(OB_SUCCESS, retention_scn.convert_from_ts(100));
  PalfHandleImpl handle;
  EventItem item;

  item.error_code_ = OB_ERR_OUT_OF_LOWER_BOUND;
  item.occur_ = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_PALF_RETENTION_LOCATE_ERROR", item));
  PalfEnvImpl::LogGetRecycableFileCandidate lower_bound_candidate(retention_scn);
  EXPECT_EQ(OB_SUCCESS, lower_bound_candidate.collect_reclaim_blocks_(handle, 0, 5));
  EXPECT_EQ(0, lower_bound_candidate.retention_expired_blocks_);
  EXPECT_EQ(4, lower_bound_candidate.retention_unexpired_blocks_);

  item.error_code_ = OB_ENTRY_NOT_EXIST;
  item.occur_ = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_PALF_RETENTION_LOCATE_ERROR", item));
  PalfEnvImpl::LogGetRecycableFileCandidate empty_candidate(retention_scn);
  EXPECT_EQ(OB_SUCCESS, empty_candidate.collect_reclaim_blocks_(handle, 0, 5));
  EXPECT_EQ(0, empty_candidate.retention_expired_blocks_);
  EXPECT_EQ(0, empty_candidate.retention_unexpired_blocks_);
}
#endif

TEST_F(TestPalfThrottling, test_wall_clock_retention_cutoff_passes_frozen_tail)
{
  const int64_t hour_us = 60L * 60L * 1000L * 1000L;
  SCN frozen_tail_scn;
  SCN retention_scn;
  ASSERT_EQ(OB_SUCCESS, frozen_tail_scn.convert_from_ts(3 * hour_us));

  ASSERT_EQ(OB_SUCCESS, PalfEnvImpl::calc_retention_scn_(
                            4 * hour_us, 2 * hour_us, retention_scn));
  EXPECT_EQ(2 * hour_us, retention_scn.convert_to_ts());
  EXPECT_LT(retention_scn.convert_to_ts(), frozen_tail_scn.convert_to_ts());
  ASSERT_EQ(OB_SUCCESS, PalfEnvImpl::calc_retention_scn_(
                            6 * hour_us, 2 * hour_us, retention_scn));
  EXPECT_EQ(4 * hour_us, retention_scn.convert_to_ts());
  EXPECT_GT(retention_scn.convert_to_ts(), frozen_tail_scn.convert_to_ts());
  EXPECT_EQ(3 * hour_us, frozen_tail_scn.convert_to_ts());

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            PalfEnvImpl::calc_retention_scn_(-1, 2 * hour_us, retention_scn));
  EXPECT_FALSE(retention_scn.is_valid());
}

TEST_F(TestPalfThrottling, test_failed_refresh_consumes_cached_snapshot)
{
  ObTenantBase tenant(1006);
  ObTenantSwitchGuard guard(&tenant);
  PalfEnvImpl palf_env_impl;
  TenantLogBlockReclaimService &service = palf_env_impl.tenant_reclaim_service_;
  ASSERT_EQ(OB_SUCCESS, service.init(/* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(1));
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_FALSE(service.can_recycle());
  service.update_reclaim_snapshot(2, 1);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(1));
  service.record_gc_progress();
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(1, service.retention_expired_blocks_);
  EXPECT_EQ(1, service.retention_unexpired_blocks_);

  // No tenant config exists for this uninitialized env, so the refresh fails.
  ASSERT_EQ(OB_INVALID_ARGUMENT, palf_env_impl.try_update_reclaim_snapshot_());
  EXPECT_EQ(1, service.retention_expired_blocks_);
  EXPECT_EQ(1, service.retention_unexpired_blocks_);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(10));
  EXPECT_EQ(2, service.pending_blocks_);
  EXPECT_EQ(2, service.pending_blocks_);
  EXPECT_EQ(1, service.retention_expired_blocks_);
  EXPECT_EQ(1, service.retention_unexpired_blocks_);

  service.record_gc_progress();
  EXPECT_EQ(1, service.pending_blocks_);
  EXPECT_EQ(0, service.retention_expired_blocks_);
  EXPECT_EQ(1, service.retention_unexpired_blocks_);
  service.last_summary_ts_ = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_INVALID_ARGUMENT, palf_env_impl.try_update_reclaim_snapshot_());
  EXPECT_EQ(1, service.pending_blocks_);
  EXPECT_EQ(0, service.retention_expired_blocks_);
  EXPECT_EQ(1, service.retention_unexpired_blocks_);

  service.record_gc_progress();
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(0, service.retention_expired_blocks_);
  EXPECT_EQ(0, service.retention_unexpired_blocks_);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(10));
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_FALSE(service.can_recycle());
  // A returning successful GC must not make an empty snapshot negative.
  service.record_gc_progress();
  EXPECT_EQ(0, service.pending_blocks_);
  EXPECT_EQ(0, service.retention_expired_blocks_);
  EXPECT_EQ(0, service.retention_unexpired_blocks_);

  // A successful collection replaces the consumed cache rather than adding to it.
  service.update_reclaim_snapshot(1, 2);
  service.update_reclaim_snapshot(2, 1);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(10));
  EXPECT_EQ(3, service.pending_blocks_);
  EXPECT_EQ(2, service.retention_expired_blocks_);
  EXPECT_EQ(1, service.retention_unexpired_blocks_);
}

TEST_F(TestPalfThrottling, test_throttling_stat)
{
  LogThrottlingStat stat;
  ASSERT_EQ(false, stat.has_ever_throttled());
  stat.start_throttling();
  ASSERT_EQ(true, stat.has_ever_throttled());
  stat.after_throttling(0, 1024);
  stat.after_throttling(100, 1024);
  stat.after_throttling(200, 1024);
  stat.after_throttling(0, 1024);
  stat.stop_throttling();
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != stat.start_ts_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != stat.stop_ts_);
  ASSERT_EQ(2048, stat.total_skipped_size_);
  ASSERT_EQ(2, stat.total_skipped_task_cnt_);
  ASSERT_EQ(2048, stat.total_throttling_size_);
  ASSERT_EQ(2, stat.total_throttling_task_cnt_);
  ASSERT_EQ(300, stat.total_throttling_interval_);
  ASSERT_EQ(200, stat.max_throttling_interval_);
  stat.start_throttling();
  ASSERT_EQ(0, stat.total_skipped_size_);
  ASSERT_EQ(0, stat.total_skipped_task_cnt_);
  ASSERT_EQ(0, stat.total_throttling_size_);
  ASSERT_EQ(0, stat.total_throttling_task_cnt_);
  ASSERT_EQ(0, stat.total_throttling_interval_);
  ASSERT_EQ(0, stat.max_throttling_interval_);
}

TEST_F(TestPalfThrottling, test_log_write_throttle)
{
  int64_t total_disk_size = 1024 * 1024 * 1024L;
  int64_t utilization_limit_threshold = 95;
  int64_t throttling_percentage = 60;
  PalfEnvImpl palf_env_impl;
  palf_env_impl.is_inited_ = true;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_maximum_duration_ = 7200 * 1000 * 1000L;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = total_disk_size;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_ = 80;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_ = utilization_limit_threshold;
  int64_t unrecyclable_size = 0;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  PalfThrottleOptions throttle_options;
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);

  LogWritingThrottle throttle;
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(false, throttle.need_writing_throttling_notified());
  throttle.notify_need_writing_throttling(true);
  ASSERT_EQ(true, throttle.need_writing_throttling_notified());

  throttle.notify_need_writing_throttling(false);
  ASSERT_EQ(false, throttle.need_writing_throttling_notified());

  ASSERT_EQ(OB_INVALID_ARGUMENT, throttle.after_append_log(-1));

  //test throttling only after notified
  PALF_LOG(INFO, "case 1: test no need throttling while notify_need_writing_throttling is false");
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  PalfThrottleOptions invalid_throttle_options;
  ASSERT_EQ(invalid_throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.last_update_ts_);
  ASSERT_EQ(false, throttle.need_writing_throttling_notified_);

  // test update interval 500ms
  PALF_LOG(INFO, "case 2: test update interval");
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(invalid_throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.last_update_ts_);
  ASSERT_EQ(false, throttle.need_writing_throttling_notified_);

  //test no need throttling after update
  PALF_LOG(INFO, "case 3: test no need throttling while unrecyclable_log_disk_size is no more than trigger_size");
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.notify_need_writing_throttling(true);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(false, throttle.stat_.has_ever_throttled());


  PALF_LOG(INFO, "case 4: test no need throttling while trigger percentage is 100", K(throttle));
  unrecyclable_size = total_disk_size * 70 / 100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(false, throttle.stat_.has_ever_throttled());

  //test need throttling after update
  PALF_LOG(INFO, "case 4: test need throttling", K(throttle));
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  unrecyclable_size = total_disk_size * 70 / 100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  PALF_LOG(INFO, "case 4: YYY test need throttling", K(throttle_options), K(throttle.throttling_options_));
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(true, throttle.stat_.has_ever_throttled());
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);

  //test no need throttling with flush meta task in queue
  //.1 flush log task
  g_need_purging_throttling = true;
  PALF_LOG(INFO, "case 5: test no need throttling while flush meta task ", K(throttle));
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  // meta task need purging throttling
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(1, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(1024, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(2048, throttle.appended_log_size_cur_round_);

//.2 flush meta task
  PALF_LOG(INFO, "case 6: test no need throttling and flush meta task ", K(throttle));
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(2, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(2048, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(3072, throttle.appended_log_size_cur_round_);

  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(3, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(3072, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(4096, throttle.appended_log_size_cur_round_);

  PALF_LOG(INFO, "case 7: need throttling after all flush meta task handled", K(throttle));
  g_need_purging_throttling = false;
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(2048, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(2, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(3, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(3072, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(5120, throttle.appended_log_size_cur_round_);

  //test  notify_need_writing_throttling(false) changed
  PALF_LOG(INFO, "case 8: no need to throttle after notify_need_throttling(false)", K(throttle));
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  throttle.notify_need_writing_throttling(false);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(invalid_throttle_options, throttle.throttling_options_);
  ASSERT_EQ(false, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(2048, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(2, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(3, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(3072, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);
  ASSERT_EQ(0, throttle.decay_factor_);

  //test need write throttling again
  PALF_LOG(INFO, "case 9: need to throttle after notify_need_throttling(true)", K(throttle));
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  throttle.notify_need_writing_throttling(true);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, throttle.stat_.total_throttling_interval_ > 0);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP == throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);

  double old_decay_factor = throttle.decay_factor_;
  //
  PALF_LOG(INFO, "case 10: test recalculate decay_factor", K(throttle));
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 55;
  palf_env_impl.get_throttling_options(throttle_options);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(true, throttle.decay_factor_ != old_decay_factor);
  ASSERT_EQ(2048, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(2, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP == throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(2048, throttle.appended_log_size_cur_round_);

  //test reset appended_log_size_cur_round_
  PALF_LOG(INFO, "case 11: test reset appended_log_size_cur_round_ after unrecyclable_size changes", K(throttle));
  old_decay_factor = throttle.decay_factor_;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  unrecyclable_size = total_disk_size * 65/100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  palf_env_impl.get_throttling_options(throttle_options);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(true, throttle.decay_factor_ == old_decay_factor);
  ASSERT_EQ(3072, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(3, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.stat_.start_ts_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP == throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);
  ASSERT_EQ(OB_SUCCESS, throttle.after_append_log(0));

//test stop write throttling when trigger percentage changed
  PALF_LOG(INFO, "case 12: test stop write throttling when trigger percentage changed", K(throttle));
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 80;
  palf_env_impl.get_throttling_options(throttle_options);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(false, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(invalid_throttle_options, throttle.throttling_options_);
  ASSERT_EQ(0, throttle.decay_factor_);
  ASSERT_EQ(3072, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(3, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.stat_.stop_ts_);
  ASSERT_EQ(0, throttle.appended_log_size_cur_round_);

  PALF_LOG(INFO, "case 12: test stop writing throttling when unrecyclable size fallbacks", K(throttle));
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 60;
  throttle.notify_need_writing_throttling(true);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  palf_env_impl.get_throttling_options(throttle_options);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, throttle.stat_.total_throttling_interval_ > 0);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP == throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);

  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  unrecyclable_size = total_disk_size * 45/100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(false, throttle.need_throttling_with_options_not_guarded_by_lock_());

}

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -rf ./test_palf_throttling.log*");
  OB_LOGGER.set_file_name("test_palf_throttling.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_palf_throttling");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
