/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gmock/gmock.h>

#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/compaction/ob_window_loop.h"
#include "rootserver/freeze/window/ob_window_compaction_helper.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;
using namespace compaction;

namespace unittest
{
class TestWindowLoop : public ::testing::Test
{
public:
  TestWindowLoop();
  virtual ~TestWindowLoop() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static ObTabletCompactionScoreDecisionInfo get_valid_decision_info(const int64_t cg_merge_batch_cnt = 3);
private:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
  ObTenantTabletStatMgr *stat_mgr_;
  ObTimerService *timer_service_;
};

TestWindowLoop::TestWindowLoop()
  : tenant_id_(1),
    tenant_base_(tenant_id_),
    stat_mgr_(nullptr),
    timer_service_(nullptr)
{
}

void TestWindowLoop::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestWindowLoop::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestWindowLoop::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;

  timer_service_ = OB_NEW(ObTimerService, ObModIds::TEST, tenant_id_);
  ASSERT_NE(nullptr, timer_service_);
  ASSERT_EQ(OB_SUCCESS, timer_service_->start());
  tenant_base_.set(timer_service_);
  stat_mgr_ = OB_NEW(ObTenantTabletStatMgr, ObModIds::TEST);
  ret = stat_mgr_->init(tenant_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  tenant_base_.set(stat_mgr_);

  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  ASSERT_EQ(tenant_id_, MTL_ID());
  ASSERT_EQ(stat_mgr_, MTL(ObTenantTabletStatMgr *));
  ASSERT_EQ(timer_service_, MTL(ObTimerService *));
}

void TestWindowLoop::TearDown()
{
  stat_mgr_->destroy();
  if (nullptr != timer_service_) {
    timer_service_->stop();
    timer_service_->wait();
    timer_service_->destroy();
    OB_DELETE(ObTimerService, ObModIds::TEST, timer_service_);
  }
  ObTenantEnv::set_tenant(nullptr);
}

ObTabletCompactionScoreDecisionInfo TestWindowLoop::get_valid_decision_info(const int64_t cg_merge_batch_cnt)
{
  ObTabletCompactionScoreDecisionInfo decision_info;
  decision_info.dynamic_info_.need_recycle_mds_ = false;
  decision_info.dynamic_info_.cg_merge_batch_cnt_ = cg_merge_batch_cnt;
  decision_info.base_inc_row_cnt_ = 1;
  decision_info.tablet_snapshot_version_ = 1;
  decision_info.major_snapshot_version_ = 1;
  return decision_info;
}

TEST_F(TestWindowLoop, test_window_compaction_params)
{
  rootserver::ObWindowParameters params;

  bool need_do_window_compaction = false;
  // params without window durations, not valid
  ASSERT_EQ(OB_NOT_INIT, params.check_need_do_window_compaction(need_do_window_compaction));
  params.tenant_id_ = 1002;
  params.is_inited_ = true;
  params.merge_mode_ = ObGlobalMergeInfo::MERGE_MODE_TENANT;
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_IDLE;
  ASSERT_TRUE(params.is_valid());

  // 1. enable_window_compaction_ is false, do not need do window compaction
  params.enable_window_compaction_ = false;
  ASSERT_TRUE(params.is_window_compaction_stopped());
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);

  // 2. enable_window_compaction_ is true, but window_duration_us_ is 0, do not need do window compaction
  params.enable_window_compaction_ = true;
  params.window_duration_us_ = 0;
  ASSERT_TRUE(params.is_window_compaction_stopped());
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);

  // Both enable_window_compaction_ and window_duration_us_ are set
  params.window_duration_us_ = 5 * 1000 * 1000L; // 5s
  ASSERT_FALSE(params.is_window_compaction_stopped());
  // 3 merge_status_ is idle
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_IDLE;
  ASSERT_FALSE(params.is_window_compaction_forever());
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);
  params.window_duration_us_ = rootserver::ObWindowParameters::FULL_DAY_US;
  ASSERT_TRUE(params.is_window_compaction_forever());
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_TRUE(need_do_window_compaction);

  // 4. merge_status_ is not idle
  params.merge_mode_ = ObGlobalMergeInfo::MERGE_MODE_TENANT;
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_MERGING;
  need_do_window_compaction = false;
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_VERIFYING;
  need_do_window_compaction = false;
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);

  params.merge_mode_ = ObGlobalMergeInfo::MERGE_MODE_WINDOW;
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_VERIFYING; // window compaction
  need_do_window_compaction = false;
  ASSERT_EQ(OB_ERR_UNEXPECTED, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);

  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_MERGING;
  const int64_t current_time_us = ObTimeUtility::current_time_us();
  params.window_start_time_us_ = current_time_us;
  params.window_duration_us_ = 1 * 1000 * 1000L;
  need_do_window_compaction = false;
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_TRUE(need_do_window_compaction);
  sleep(2);
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);
}

TEST_F(TestWindowLoop, test_basic_init)
{
  ObWindowLoop window_loop;
  const int64_t merge_start_time = ObTimeUtility::current_time();
  ASSERT_FALSE(window_loop.is_active());
  ASSERT_FALSE(window_loop.is_inited_);
  ASSERT_EQ(OB_SUCCESS, window_loop.stop_window_compaction());
  ASSERT_FALSE(window_loop.is_active());
  ASSERT_FALSE(window_loop.is_inited_);

  ASSERT_EQ(OB_SUCCESS, window_loop.start_window_compaction(merge_start_time));
  ASSERT_TRUE(window_loop.is_active());
  ASSERT_TRUE(window_loop.is_inited_);
  ASSERT_EQ(OB_SUCCESS, window_loop.start_window_compaction(merge_start_time));
}

TEST_F(TestWindowLoop, test_window_loop_memory_size)
{
  ObWindowLoop window_loop;
  ObWindowCompactionMemoryContext &mem_ctx = window_loop.mem_ctx_;
  ObIAllocator &allocator = mem_ctx.allocator_;
  ObWindowCompactionPriorityQueue &priority_queue = window_loop.score_prio_queue_;
  LOG_INFO("start test_window_loop_memory_size");
  const int64_t merge_start_time = ObTimeUtility::current_time();
  const int64_t tablet_cnts[5] = {100, 1000, 10000, 10 * 10000, 100 * 10000};
  for (int64_t i = 0; i < 5; i++) {
    const int64_t tablet_cnt = tablet_cnts[i];
    LOG_INFO("start test_window_controller_memory_size", K(tablet_cnt));
    ASSERT_EQ(OB_SUCCESS, window_loop.start_window_compaction(merge_start_time));
    for (int64_t i = 0; i < tablet_cnt; i++) {
      ObTabletCompactionScore *score = nullptr;
      ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
      ASSERT_NE(nullptr, score);
      score->key_.ls_id_ = 1001;
      score->key_.tablet_id_ = 200001 + i;
      score->score_ = rand() % 1000000 + 1;
      score->decision_info_ = get_valid_decision_info();
      ASSERT_EQ(OB_SUCCESS, window_loop.score_prio_queue_.push(score));
      if (i % 100 == 0) {
        LOG_INFO("finish push scores", K(tablet_cnt), K(i));
      }
    }
    LOG_INFO("finish push all scores", K(i), K(tablet_cnt), "total", allocator.total(), "used", allocator.used());
    window_loop.stop_window_compaction();
    ASSERT_EQ(0, allocator.total());
    ASSERT_EQ(0, allocator.used()); // check if all scores are destroyed
    LOG_INFO("finish stop window compaction", K(i), K(tablet_cnt), "total", allocator.total(), "used", allocator.used());
  }
  LOG_INFO("finish test_window_loop_memory_size");
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_window_loop.log*");
  OB_LOGGER.set_file_name("test_window_loop.log", true);
  OB_LOGGER.set_log_level("TRACE");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}