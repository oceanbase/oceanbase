/**
 * Copyright (c) 2023 OceanBase
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
#include <gmock/gmock.h>
#include <thread>

#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;
using namespace compaction;

class TestTenantTabletStatMgr : public ::testing::Test
{
public:
  TestTenantTabletStatMgr();
  virtual ~TestTenantTabletStatMgr() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void report(ObTenantTabletStatMgr *mgr, const ObTabletStat &stat);
  void batch_report_stat(int64_t report_num);

private:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
  ObTenantTabletStatMgr *stat_mgr_;
};

TestTenantTabletStatMgr::TestTenantTabletStatMgr()
  : tenant_id_(1),
    tenant_base_(tenant_id_),
    stat_mgr_(nullptr)
{
}

void TestTenantTabletStatMgr::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestTenantTabletStatMgr::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTenantTabletStatMgr::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;

  ObTenantEnv::set_tenant(&tenant_base_);
  stat_mgr_ = OB_NEW(ObTenantTabletStatMgr, ObModIds::TEST);
  ret = stat_mgr_->init(tenant_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  tenant_base_.set(stat_mgr_);

  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  ASSERT_EQ(tenant_id_, MTL_ID());
  ASSERT_EQ(stat_mgr_, MTL(ObTenantTabletStatMgr *));
}

void TestTenantTabletStatMgr::TearDown()
{
  stat_mgr_->destroy();
  ObTenantEnv::set_tenant(nullptr);
}

void TestTenantTabletStatMgr::report(ObTenantTabletStatMgr *mgr, const ObTabletStat &stat)
{
  ASSERT_TRUE(NULL != mgr);
  ASSERT_TRUE(stat.is_valid());
  bool report_succ = false;
  ASSERT_EQ(OB_SUCCESS, mgr->report_stat(stat, report_succ));
}

void TestTenantTabletStatMgr::batch_report_stat(int64_t report_num)
{
  ASSERT_TRUE(NULL != stat_mgr_);
  ASSERT_EQ(true, stat_mgr_->is_inited_);

  std::thread *threads = new std::thread[report_num];
  for (int64_t i = 0; i < report_num; ++i) {
    ObTabletStat curr_stat;
    curr_stat.ls_id_ = 1;
    curr_stat.tablet_id_ = 300001 + i;
    curr_stat.query_cnt_ = 100 * (i + 1);
    curr_stat.scan_physical_row_cnt_ = 10000 + i;

    threads[i] = std::thread(report, stat_mgr_, curr_stat);
  }
  for (int64_t i = 0; i < report_num; ++i) {
    if (threads[i].joinable()) {
      threads[i].join();
    }
  }
  delete []threads;
}

namespace unittest
{
TEST_F(TestTenantTabletStatMgr, basic)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);
  ASSERT_TRUE(NULL != stat_mgr);
}

TEST_F(TestTenantTabletStatMgr, basic_tablet_stat_bucket)
{
  ObTabletStat tablet_stat;
  tablet_stat.ls_id_ = 1;
  tablet_stat.tablet_id_ = 1;
  tablet_stat.query_cnt_ = 100;
  tablet_stat.scan_logical_row_cnt_ = 100;
  tablet_stat.scan_physical_row_cnt_ = 100;

  {
    uint32_t step = 1;
    ObTabletStatBucket<8> bucket(step);
    ObTabletStat retired_stat;
    bool has_retired = false;
    for (int64_t i = 0; i < 8; ++i) {
      ASSERT_EQ(i, bucket.head_idx_);
      ASSERT_EQ(7 + i, bucket.curr_idx_);
      bucket.add(tablet_stat);
      ASSERT_EQ(100, bucket.units_[bucket.get_idx(7 + i)].query_cnt_);
      bucket.refresh(retired_stat, has_retired);
      ASSERT_EQ(true, has_retired);
    }
    bucket.refresh(retired_stat, has_retired);
    ASSERT_EQ(100, retired_stat.scan_logical_row_cnt_);
  }

  {
    uint32_t step = 16;
    ObTabletStatBucket<4> bucket(step);
    ObTabletStat retired_stat;
    bool has_retired = false;
    for (int64_t i = 0; i < 64; ++i) {
      ASSERT_EQ(i / step, bucket.head_idx_);
      ASSERT_EQ(3 + i / step, bucket.curr_idx_);
      bucket.add(tablet_stat);
      bucket.refresh(retired_stat, has_retired);
      if (has_retired) {
        ASSERT_EQ(0, bucket.units_[bucket.get_idx(bucket.curr_idx_)].query_cnt_);
      }
    }

    ASSERT_TRUE(has_retired);
    ASSERT_EQ(1600, retired_stat.scan_logical_row_cnt_);
  }

  {
    uint32_t step = 32;
    ObTabletStatBucket<4> bucket(step);
    ObTabletStat retired_stat;
    bool has_retired = false;
    for (int64_t i = 0; i < 128; ++i) {
      ASSERT_EQ(i / step, bucket.head_idx_);
      ASSERT_EQ(3 + i / step, bucket.curr_idx_);
      bucket.add(tablet_stat);
      bucket.refresh(retired_stat, has_retired);
      if (has_retired) {
        ASSERT_EQ(0, bucket.units_[bucket.get_idx(bucket.curr_idx_)].query_cnt_);
      }
    }
    ASSERT_TRUE(has_retired);
    ASSERT_EQ(3200, retired_stat.scan_logical_row_cnt_);
  }
}

TEST_F(TestTenantTabletStatMgr, basic_tablet_stream)
{
  ObTabletStat tablet_stat;
  tablet_stat.ls_id_ = 1;
  tablet_stat.tablet_id_ = 200123;
  tablet_stat.query_cnt_ = 100;
  tablet_stat.scan_logical_row_cnt_ = 1000000;
  tablet_stat.scan_physical_row_cnt_ = 1000000;

  ObTabletStream stream;
  auto &curr_buckets = stream.curr_buckets_;
  auto &latest_buckets = stream.latest_buckets_;
  auto &past_buckets = stream.past_buckets_;

  for (int64_t i = 0; i < ObTabletStream::CURR_BUCKET_CNT * ObTabletStream::CURR_BUCKET_STEP; ++i) {
    stream.add_stat(tablet_stat);
    stream.refresh();
  }
  ASSERT_EQ(8, latest_buckets.refresh_cnt_);
  // retired from curr_buckets
  ASSERT_EQ(100, latest_buckets.units_[latest_buckets.get_idx(latest_buckets.curr_idx_)].query_cnt_);

  for (int64_t i = 0; i < ObTabletStream::LATEST_BUCKET_CNT * ObTabletStream::LATEST_BUCKET_STEP; ++i) {
    stream.add_stat(tablet_stat);
    stream.refresh();
  }
  ASSERT_EQ(72, latest_buckets.refresh_cnt_);
  ASSERT_EQ(800, latest_buckets.units_[latest_buckets.get_idx(latest_buckets.curr_idx_)].query_cnt_);
  ASSERT_EQ(72, past_buckets.refresh_cnt_);
  ASSERT_EQ(0, past_buckets.units_[past_buckets.get_idx(past_buckets.curr_idx_)].query_cnt_);

  for (int64_t i = 0; i < ObTabletStream::CURR_BUCKET_CNT * ObTabletStream::CURR_BUCKET_STEP; ++i) {
    stream.add_stat(tablet_stat);
    stream.refresh();
  }
  ASSERT_EQ(80, latest_buckets.refresh_cnt_);
  ASSERT_EQ(80, past_buckets.refresh_cnt_);
  ASSERT_EQ(1600, past_buckets.units_[past_buckets.get_idx(past_buckets.curr_idx_)].query_cnt_);
}

TEST_F(TestTenantTabletStatMgr, get_all_tablet_stat)
{
  int ret = OB_SUCCESS;

  ObTabletStat tablet_stat;
  tablet_stat.ls_id_ = 1;
  tablet_stat.tablet_id_ = 1;
  tablet_stat.query_cnt_ = 100;
  tablet_stat.scan_logical_row_cnt_ = 100;
  tablet_stat.scan_physical_row_cnt_ = 100;

  ObTabletStream stream;
  auto &curr_buckets = stream.curr_buckets_;
  auto &latest_buckets = stream.latest_buckets_;
  auto &past_buckets = stream.past_buckets_;
  ObArray<ObTabletStat> tablet_stats;

  int64_t curr_bucket_size = curr_buckets.count();
  for (int64_t i = 0; i < curr_bucket_size; ++i) {
    curr_buckets.units_[i] += tablet_stat;
  }
  ret = stream.get_bucket_tablet_stat<ObTabletStream::CURR_BUCKET_CNT>(curr_buckets, tablet_stats);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(curr_bucket_size == tablet_stats.count());

  int64_t latest_bucket_size = latest_buckets.count();
  for (int64_t i = 0; i < latest_bucket_size; ++i) {
    latest_buckets.units_[i] += tablet_stat;
  }
  ret = stream.get_bucket_tablet_stat(stream.latest_buckets_, tablet_stats);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE((curr_bucket_size + latest_bucket_size) == tablet_stats.count());

  int64_t past_bucket_size = past_buckets.count();
  for (int64_t i = 0; i < past_bucket_size; ++i) {
    past_buckets.units_[i] += tablet_stat;
  }
  ret = stream.get_bucket_tablet_stat(stream.past_buckets_, tablet_stats);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE((curr_bucket_size + latest_bucket_size + past_bucket_size) == tablet_stats.count());
}

TEST_F(TestTenantTabletStatMgr, basic_stream_pool)
{
  int ret = OB_SUCCESS;
  const int64_t max_free_list_num = 500;
  const int64_t up_limit_node_num = 1000;
  ObTabletStreamPool pool;

  ret = pool.init(max_free_list_num, up_limit_node_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t free_num = pool.get_free_num();
  ASSERT_EQ(max_free_list_num, free_num);

  ObTabletStreamNode *fixed_node = nullptr;
  bool is_retired = false;
  ret = pool.alloc(fixed_node, is_retired);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != fixed_node);
  ASSERT_EQ(storage::ObTabletStreamPool::NodeAllocType::FIXED_ALLOC, fixed_node->flag_);
  ASSERT_EQ(max_free_list_num - 1, pool.get_free_num());
  pool.free(fixed_node);
  fixed_node = nullptr;
  ASSERT_EQ(max_free_list_num, pool.get_free_num());

  common::ObSEArray<ObTabletStreamNode *, 500> free_nodes;
  for (int64_t i = 0; i < max_free_list_num; ++i) {
    ObTabletStreamNode *free_node = nullptr;
    ret = pool.alloc(free_node, is_retired);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(NULL != free_node);
    ASSERT_EQ(storage::ObTabletStreamPool::NodeAllocType::FIXED_ALLOC, free_node->flag_);
    ret = free_nodes.push_back(free_node);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(0, pool.get_free_num());

  ObTabletStreamNode *dynamic_node = nullptr;
  ret = pool.alloc(dynamic_node, is_retired);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != dynamic_node);
  ASSERT_EQ(storage::ObTabletStreamPool::NodeAllocType::DYNAMIC_ALLOC, dynamic_node->flag_);
  pool.free(dynamic_node);
  dynamic_node = nullptr;
  ASSERT_EQ(0, pool.get_free_num());

  for (int64_t i = 0; i < free_nodes.count(); ++i) {
    ObTabletStreamNode *node = free_nodes.at(i);
    ASSERT_TRUE(NULL != node);
    pool.free(node);
    ASSERT_EQ(i + 1, pool.get_free_num());
  }
}

TEST_F(TestTenantTabletStatMgr, check_fetch_node)
{
  int ret = OB_SUCCESS;
  const int64_t max_free_list_num = 500;
  const int64_t up_limit_node_num = 500;
  ObTabletStreamPool pool;
  common::ObDList<ObTabletStreamNode> lru_list_;

  ret = pool.init(max_free_list_num, up_limit_node_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t free_num = pool.get_free_num();
  ASSERT_EQ(max_free_list_num, free_num);

  ObTabletStreamNode *first_node = nullptr;
  for (int64_t i = 0; i < max_free_list_num; ++i) {
    ObTabletStreamNode *curr_node = nullptr;
    ret = pool.free_list_.pop(curr_node);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(NULL == curr_node->prev_);
    ASSERT_TRUE(NULL == curr_node->next_);

    ASSERT_EQ(true, lru_list_.add_first(curr_node));
    first_node = (NULL == first_node) ? curr_node : first_node;
  }

  ObTabletStreamNode *last_node = nullptr;
  for (int64_t i = 0; i < max_free_list_num; ++i) {
    last_node = lru_list_.get_last();
    ASSERT_EQ(true, lru_list_.move_to_first(last_node));
    ret = pool.free_list_.push(last_node);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  last_node = lru_list_.get_last();
  ASSERT_TRUE(last_node == first_node);
}

TEST_F(TestTenantTabletStatMgr, basic_tablet_stat_mgr)
{
  int ret = OB_SUCCESS;

  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);
  ASSERT_TRUE(NULL != stat_mgr);

  ObTabletStat tablet_stat;
  tablet_stat.ls_id_ = 1;
  tablet_stat.tablet_id_ = 200123;
  tablet_stat.query_cnt_ = 100;
  tablet_stat.scan_logical_row_cnt_ = 100000;
  tablet_stat.scan_physical_row_cnt_ = 1000000;

  bool report_succ = false;
  ret = stat_mgr_->report_stat(tablet_stat, report_succ);
  ASSERT_EQ(OB_SUCCESS, ret);
  stat_mgr_->process_stats();

  ObTabletStat res;
  share::ObLSID ls_id(1);
  common::ObTabletID tablet_id(200123);
  storage::ObTabletStat unused_tablet_stat;
  share::schema::ObTableModeFlag unused_mode;
  ret = stat_mgr_->get_latest_tablet_stat(ls_id, tablet_id, res, unused_tablet_stat, unused_mode);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, res.query_cnt_);

  ASSERT_EQ(1, stat_mgr_->stream_map_.size());
  ASSERT_EQ(OB_SUCCESS, stat_mgr_->clear_tablet_stat(ls_id, tablet_id));
  const ObTabletStatKey key(ls_id, tablet_id);
  ObTabletStreamNode *stream_node = nullptr;
  ASSERT_TRUE(key.is_valid());
  ASSERT_EQ(OB_SUCCESS, stat_mgr_->stream_map_.get_refactored(key, stream_node));
  ASSERT_TRUE(stream_node->stream_.key_.is_valid());
  ASSERT_FALSE(stream_node->stream_.total_stat_.is_valid());

  tablet_stat.delete_row_cnt_ = 12345;
  for (int64_t i = 0; i < 1000; i++) {
    ret = stat_mgr_->report_stat(tablet_stat, report_succ);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  stat_mgr_->process_stats();
  storage::ObTabletStat total_tablet_stat;
  ret = stat_mgr_->get_latest_tablet_stat(ls_id, tablet_id, res, total_tablet_stat, unused_mode);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100 * 1000, total_tablet_stat.query_cnt_);
  ASSERT_EQ(100000 * 1000, total_tablet_stat.scan_logical_row_cnt_);
  ASSERT_EQ(1000000 * 1000, total_tablet_stat.scan_physical_row_cnt_);
  ASSERT_EQ(12345 * 1000, total_tablet_stat.delete_row_cnt_);
}

TEST_F(TestTenantTabletStatMgr, multi_report_tablet_stat)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);
  ASSERT_TRUE(NULL != stat_mgr);
  ASSERT_TRUE(stat_mgr->is_inited_);

  int64_t report_num = 10;
  batch_report_stat(report_num);
  stat_mgr_->process_stats();

  int64_t report_cnt = 0;
  ObTenantTabletStatMgr::TabletStreamMap::iterator iter = stat_mgr_->stream_map_.begin();
  for ( ; iter != stat_mgr_->stream_map_.end(); ++iter) {
    ++report_cnt;
  }
  ASSERT_TRUE(report_cnt == 10);
}

TEST_F(TestTenantTabletStatMgr, bacth_clear_tablet_stat)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);
  ASSERT_TRUE(NULL != stat_mgr);
  ASSERT_TRUE(stat_mgr->is_inited_);

  int64_t report_num = 100;
  batch_report_stat(report_num);
  stat_mgr_->process_stats();

  ObLSID ls_id(1);
  ObSEArray<ObTabletID, 100> tablet_ids;
  for (int64_t i = 0; i < report_num; i++) {
    ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(300001 + i)));
  }
  ASSERT_EQ(100, stat_mgr_->stream_map_.size());
  ASSERT_EQ(OB_SUCCESS, stat_mgr->batch_clear_tablet_stat(ls_id, tablet_ids));
  ObTenantTabletStatMgr::TabletStreamMap::iterator iter = stat_mgr_->stream_map_.begin();
  for ( ; iter != stat_mgr_->stream_map_.end(); ++iter) {
    ASSERT_TRUE(iter->second->stream_.key_.is_valid());
    ASSERT_FALSE(iter->second->stream_.total_stat_.is_valid());
  }
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -f test_tenant_tablet_stat_mgr.log*");
  OB_LOGGER.set_file_name("test_tenant_tablet_stat_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_max_file_size(256*1024*1024);
  return RUN_ALL_TESTS();
}
