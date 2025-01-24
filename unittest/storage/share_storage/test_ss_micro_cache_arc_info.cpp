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
#include "gtest/gtest.h"
#define private public
#define protected public

#include "storage/shared_storage/micro_cache/ob_ss_arc_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheArcInfo : public ::testing::Test
{
public:
  TestSSMicroCacheArcInfo();
  virtual ~TestSSMicroCacheArcInfo();
  virtual void SetUp();
  virtual void TearDown();

private:
  static const int32_t BLOCK_SIZE = 2 * 1024 * 1024;
};

TestSSMicroCacheArcInfo::TestSSMicroCacheArcInfo()
{}

TestSSMicroCacheArcInfo::~TestSSMicroCacheArcInfo()
{}

void TestSSMicroCacheArcInfo::SetUp()
{}

void TestSSMicroCacheArcInfo::TearDown()
{}

TEST_F(TestSSMicroCacheArcInfo, arc_seg_op_info)
{
  ObSSARCSegOpInfo seg_op;
  ASSERT_EQ(false, seg_op.is_valid());
  ASSERT_EQ(false, seg_op.exist_op());
  seg_op.set_op_info(false, 30);
  ASSERT_EQ(true, seg_op.is_valid());
  ASSERT_EQ(true, seg_op.exist_op());
  ASSERT_EQ(30, seg_op.get_op_cnt());
  ASSERT_EQ(false, seg_op.to_delete());
  seg_op.dec_op_cnt();
  ASSERT_EQ(29, seg_op.get_op_cnt());

  seg_op.update_op_info(20, 100);
  seg_op.inc_obtained_cnt();
  seg_op.inc_obtained_cnt();
  ASSERT_EQ(2, seg_op.obtained_cnt_);
  ASSERT_EQ(true, seg_op.need_obtain_more());
  for (int64_t i = 0; i < 98; ++i) {
    seg_op.inc_obtained_cnt();
  }
  ASSERT_EQ(false, seg_op.need_obtain_more());

  ObSSARCSegOpInfo seg_op_cp = seg_op;
  ASSERT_EQ(false, seg_op_cp.to_delete_);
  ASSERT_EQ(20, seg_op_cp.op_cnt_);
  ASSERT_EQ(100, seg_op_cp.exp_iter_cnt_);
  ASSERT_EQ(100, seg_op_cp.obtained_cnt_);

  seg_op.dec_obtained_cnt();
  ASSERT_EQ(99, seg_op.obtained_cnt_);
  ASSERT_EQ(true, seg_op.is_valid());

  seg_op.op_cnt_ = -1;
  ASSERT_EQ(false, seg_op.is_valid());
}

TEST_F(TestSSMicroCacheArcInfo, arc_iter_seg_info)
{
  ObSSARCIterInfo::ObSSARCIterSegInfo iter_seg_info;
  ASSERT_EQ(false, iter_seg_info.is_valid());
  iter_seg_info.init_iter_seg_info(100, false, 30);
  ASSERT_EQ(true, iter_seg_info.is_valid());
  ASSERT_EQ(100, iter_seg_info.get_seg_cnt());
  ASSERT_EQ(false, iter_seg_info.is_delete_op());
  ASSERT_EQ(30, iter_seg_info.get_op_cnt());
  iter_seg_info.dec_op_cnt();
  ASSERT_EQ(29, iter_seg_info.get_op_cnt());
  iter_seg_info.update_iter_seg_info(15, 75);
  ASSERT_EQ(15, iter_seg_info.get_op_cnt());
  ASSERT_EQ(75, iter_seg_info.op_info_.exp_iter_cnt_);
  ASSERT_EQ(true, iter_seg_info.need_obtain_more_cnt());
  iter_seg_info.inc_obtained_cnt();
  ASSERT_EQ(1, iter_seg_info.op_info_.get_obtained_cnt());
  iter_seg_info.op_info_.obtained_cnt_ += 73;
  ASSERT_EQ(true, iter_seg_info.need_obtain_more_cnt());
  iter_seg_info.inc_obtained_cnt();
  ASSERT_EQ(false, iter_seg_info.need_obtain_more_cnt());
  iter_seg_info.dec_obtained_cnt();
  ASSERT_EQ(74, iter_seg_info.op_info_.get_obtained_cnt());
}

TEST_F(TestSSMicroCacheArcInfo, arc_iter_info)
{
  ObSSARCIterInfo arc_iter_info;
  ASSERT_EQ(false, arc_iter_info.is_inited_);
  ASSERT_EQ(OB_SUCCESS, arc_iter_info.init(1));
  ASSERT_EQ(true, arc_iter_info.is_inited_);

  {
    arc_iter_info.iter_seg_arr_[ARC_T1].init_iter_seg_info(1000, false, 0);
    ASSERT_EQ(false, arc_iter_info.is_delete_op_type(ARC_T1));
    ASSERT_EQ(false, arc_iter_info.need_handle_arc_seg(ARC_T1));
    arc_iter_info.iter_seg_arr_[ARC_T1].init_iter_seg_info(1000, false, 400);
    ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T1));
    arc_iter_info.adjust_arc_iter_seg_info(ARC_T1);
    ASSERT_EQ(1000, arc_iter_info.iter_seg_arr_[ARC_T1].seg_cnt_);
    ASSERT_EQ(1000, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.exp_iter_cnt_);
    ASSERT_EQ(400, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
    arc_iter_info.reuse(ARC_T1);
    ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T1].seg_cnt_);
    ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.exp_iter_cnt_);
    ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
    ASSERT_EQ(false, arc_iter_info.need_handle_arc_seg(ARC_T1));
  }

  {
    arc_iter_info.iter_seg_arr_[ARC_B2].init_iter_seg_info(2000, true, 510);
    ASSERT_EQ(true, arc_iter_info.is_delete_op_type(ARC_B2));
    ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_B2));
    arc_iter_info.adjust_arc_iter_seg_info(ARC_B2);
    ASSERT_EQ(2000, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.exp_iter_cnt_);
    ASSERT_EQ(500, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.op_cnt_);
    arc_iter_info.reuse(ARC_B2);

    arc_iter_info.iter_seg_arr_[ARC_B2].init_iter_seg_info(5000, true, 510);
    ASSERT_EQ(true, arc_iter_info.is_delete_op_type(ARC_B2));
    ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_B2));
    arc_iter_info.adjust_arc_iter_seg_info(ARC_B2);
    ASSERT_EQ(SS_MAX_ARC_FETCH_CNT, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.exp_iter_cnt_);
    ASSERT_EQ(500, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.op_cnt_);
    ASSERT_EQ(OB_SUCCESS, arc_iter_info.finish_handle_cold_micro(ARC_B2));
    ASSERT_EQ(499, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.op_cnt_);
    arc_iter_info.reuse();
    ASSERT_EQ(false, arc_iter_info.need_handle_arc_seg(ARC_B2));
  }
}

TEST_F(TestSSMicroCacheArcInfo, arc_seg_info)
{
  ObSSARCInfo::ObSSARCSegInfo arc_seg_info;
  ASSERT_EQ(true, arc_seg_info.is_empty());
  arc_seg_info.size_ = 100;
  arc_seg_info.cnt_ = 30;
  ASSERT_EQ(3, arc_seg_info.avg_micro_size());
  ASSERT_EQ(18, arc_seg_info.avg_micro_cnt(60));
  int64_t cnt = 0;
  int64_t size = 0;
  arc_seg_info.get_seg_info(size, cnt);
  ASSERT_EQ(cnt, arc_seg_info.cnt_);
  ASSERT_EQ(size, arc_seg_info.size_);
  arc_seg_info.update_seg_info(20, 10);
  ASSERT_EQ(120, arc_seg_info.size());
  ASSERT_EQ(40, arc_seg_info.count());

  ObSSARCInfo::ObSSARCSegInfo arc_seg_info2;
  arc_seg_info2 = arc_seg_info;
  ASSERT_EQ(arc_seg_info.cnt_, arc_seg_info2.cnt_);
  ASSERT_EQ(arc_seg_info.size_, arc_seg_info2.size_);
}

TEST_F(TestSSMicroCacheArcInfo, arc_info)
{
  const int32_t micro_size = 10;

  ObSSARCInfo arc_info;
  ASSERT_EQ(false, arc_info.is_valid());
  arc_info.update_arc_limit(10000);
  ASSERT_EQ(10000, arc_info.limit_);
  ASSERT_EQ(10000, arc_info.work_limit_);
  ASSERT_EQ(5000, arc_info.p_);
  ASSERT_EQ(5500, arc_info.max_p_);
  ASSERT_EQ(2000, arc_info.min_p_);
  ASSERT_EQ(true, arc_info.is_valid());
  arc_info.mem_limit_ = 0;
  ASSERT_EQ(false, arc_info.is_valid());
  arc_info.mem_limit_ = INT64_MAX;
  ASSERT_EQ(true, arc_info.is_valid());
  arc_info.dec_arc_work_limit_for_prewarm();
  ASSERT_EQ(10000, arc_info.limit_);
  ASSERT_EQ(9000, arc_info.work_limit_);
  ASSERT_EQ(4500, arc_info.p_);
  ASSERT_EQ(4950, arc_info.max_p_);
  ASSERT_EQ(1800, arc_info.min_p_);
  arc_info.inc_arc_work_limit_for_prewarm();
  ASSERT_EQ(10000, arc_info.limit_);
  ASSERT_EQ(10000, arc_info.work_limit_);
  ASSERT_EQ(5000, arc_info.p_);
  ASSERT_EQ(5500, arc_info.max_p_);
  ASSERT_EQ(2000, arc_info.min_p_);

  ASSERT_EQ(false, arc_info.trigger_eviction());
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 800;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 800;
  ASSERT_EQ(false, arc_info.trigger_eviction());
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 100;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 100;
  ASSERT_EQ(false, arc_info.trigger_eviction());
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 1000;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 1000;
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 0;
  arc_info.seg_info_arr_[ARC_T2].size_ = 0;
  ASSERT_EQ(false, arc_info.trigger_eviction());
  arc_info.seg_info_arr_[ARC_B1].cnt_ = 100;
  arc_info.seg_info_arr_[ARC_B1].size_ = micro_size * 100;
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 0;
  arc_info.seg_info_arr_[ARC_T1].size_ = 0;
  ASSERT_EQ(false, arc_info.trigger_eviction());
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 600;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 600;
  arc_info.seg_info_arr_[ARC_B2].cnt_ = 500;
  arc_info.seg_info_arr_[ARC_B2].size_ = micro_size * 500;
  ASSERT_EQ(true, arc_info.trigger_eviction());

  ObSSARCIterInfo arc_iter_info;
  ASSERT_EQ(OB_SUCCESS, arc_iter_info.init(1));

  // 1. need to evict T2
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 500;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 500;
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 1000;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 1000;
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T1);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(500, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  ASSERT_EQ(false, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.to_delete_);
  arc_iter_info.reuse(ARC_T2);

  // 2. need to evict T1/T2
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 750;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 750;
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 750;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 750;
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(250, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 500;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 500;
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(250, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  arc_iter_info.reuse();

  // 3. need to evict T1/T2
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 1250;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 1250;
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 750;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 750;
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(750, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T1));
  arc_iter_info.reuse(ARC_T1);
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 750;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 750;
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(250, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T2));
  arc_iter_info.reuse();

  // 4. need to evict T1/T2
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 1300;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 1300;
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 1200;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 1200;
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(800, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T1));
  arc_iter_info.reuse(ARC_T1);
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 800;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 800;
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(700, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  arc_iter_info.reuse();

  // 5. need to delete B1
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 800;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 800;
  arc_info.seg_info_arr_[ARC_B1].cnt_ = 500;
  arc_info.seg_info_arr_[ARC_B1].size_ = micro_size * 500;
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_B1);
  ASSERT_EQ(300, arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.to_delete_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_B1));
  arc_iter_info.reuse();

  // 6. need to delete B2
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 0;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 0;
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 1000;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 1000;
  arc_info.seg_info_arr_[ARC_B2].cnt_ = 600;
  arc_info.seg_info_arr_[ARC_B2].size_ = micro_size * 600;
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  ASSERT_EQ(false, arc_iter_info.need_handle_arc_seg(ARC_T2));
  arc_iter_info.reuse(ARC_T2);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_B2);
  ASSERT_EQ(600, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.to_delete_);
  arc_iter_info.reuse();

  // 7. adjust p_
  arc_info.p_ = 5500;
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 1300;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 1300;
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 1200;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 1200;
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(750, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T1));
  arc_iter_info.reuse(ARC_T1);
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 800;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 800;
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(750, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T2));
  arc_iter_info.reuse(ARC_T2);

  arc_info.p_ = 4000;
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 1300;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 1300;
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 1200;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 1200;
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(900, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T1));
  arc_iter_info.reuse(ARC_T1);
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 800;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 800;
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(600, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T2));
  arc_iter_info.reuse(ARC_T2);

  // 8. test trigger eviction by memory size
  arc_info.p_ = 5000;
  arc_info.seg_info_arr_[ARC_T1].cnt_ = 200;
  arc_info.seg_info_arr_[ARC_T1].size_ = micro_size * 200;
  arc_info.seg_info_arr_[ARC_T2].cnt_ = 200;
  arc_info.seg_info_arr_[ARC_T2].size_ = micro_size * 200;
  arc_info.seg_info_arr_[ARC_B1].cnt_ = 100;
  arc_info.seg_info_arr_[ARC_B1].size_ = micro_size * 100;
  arc_info.seg_info_arr_[ARC_B2].cnt_ = 100;
  arc_info.seg_info_arr_[ARC_B2].size_ = micro_size * 100;
  ASSERT_EQ(false, arc_info.trigger_eviction());
  arc_info.mem_limit_ = 667 * (SS_MICRO_META_POOL_ITEM_SIZE + SS_MICRO_META_MAP_ITEM_SIZE);
  ASSERT_EQ(false, arc_info.trigger_eviction());
  arc_info.mem_limit_ = 500 * (SS_MICRO_META_POOL_ITEM_SIZE + SS_MICRO_META_MAP_ITEM_SIZE);
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_B1);
  ASSERT_EQ(100, arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_B1);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_B2);
  ASSERT_EQ(100, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_B2);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T1);
  arc_info.mem_limit_ = 300 * (SS_MICRO_META_POOL_ITEM_SIZE + SS_MICRO_META_MAP_ITEM_SIZE);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T2);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(130, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T1);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_B1);
  ASSERT_EQ(100, arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_B1);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_B2);
  ASSERT_EQ(100, arc_iter_info.iter_seg_arr_[ARC_B2].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_B2);

  // test copy assignment
  ObSSARCInfo arc_info2;
  arc_info2 = arc_info;
  ASSERT_EQ(arc_info.limit_, arc_info2.limit_);
  ASSERT_EQ(arc_info.max_p_, arc_info2.max_p_);
  ASSERT_EQ(arc_info.min_p_, arc_info2.min_p_);
  ASSERT_EQ(arc_info.p_, arc_info2.p_);
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    ASSERT_EQ(arc_info.seg_info_arr_[i].size_, arc_info2.seg_info_arr_[i].size_);
    ASSERT_EQ(arc_info.seg_info_arr_[i].cnt_, arc_info2.seg_info_arr_[i].cnt_);
  }
}

TEST_F(TestSSMicroCacheArcInfo, arc_state_machine)
{
  const int64_t micro_size = 10;
  const int64_t max_micro_cnt = 1000;
  ObSSARCInfo arc_info;
  arc_info.update_arc_limit(micro_size * max_micro_cnt);
  ASSERT_EQ(arc_info.limit_, micro_size * max_micro_cnt);
  ASSERT_EQ(arc_info.p_, arc_info.limit_ / 2);
  ASSERT_EQ(arc_info.max_p_, arc_info.limit_ * 55 / 100);
  ASSERT_EQ(arc_info.min_p_, arc_info.limit_ * 20 / 100);

  // 1. Add new micro, util T1 is full
  // T1 : 1000
  for (int64_t i = 0; i < max_micro_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(true, false, ObSSARCOpType::SS_ARC_NEW_ADD, micro_size, 1));
  }
  ASSERT_EQ(false, arc_info.trigger_eviction());
  ASSERT_EQ(micro_size * max_micro_cnt, arc_info.seg_info_arr_[ARC_T1].size());
  ASSERT_EQ(max_micro_cnt, arc_info.seg_info_arr_[ARC_T1].count());

  // 2. Continue to add some new micro into T1
  // T1 : 1600
  for (int64_t i = 0; i < SS_MAX_ARC_HANDLE_OP_CNT + 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(true, false, ObSSARCOpType::SS_ARC_NEW_ADD, micro_size, 1));
  }
  ASSERT_EQ(max_micro_cnt + SS_MAX_ARC_HANDLE_OP_CNT + 100, arc_info.seg_info_arr_[ARC_T1].count());
  ASSERT_EQ(true, arc_info.trigger_eviction());

  ObSSARCIterInfo arc_iter_info;
  ASSERT_EQ(OB_SUCCESS, arc_iter_info.init(1));
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(600, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T1));
  arc_iter_info.adjust_arc_iter_seg_info(ARC_T1);
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  ASSERT_EQ(arc_info.seg_info_arr_[ARC_T1].count(), arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.exp_iter_cnt_);
  ASSERT_EQ(false, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.to_delete_);
  arc_iter_info.reuse(ARC_T1);

  // 3. Evict from T1 to B1
  // T1: 1100; B1: 500
  for (int64_t i = 0; i < SS_MAX_ARC_HANDLE_OP_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(true, false, ObSSARCOpType::SS_ARC_TASK_EVICT_OP, micro_size, 1));
  }
  ASSERT_EQ(max_micro_cnt + 100, arc_info.seg_info_arr_[ARC_T1].count());
  ASSERT_EQ((max_micro_cnt + 100) * micro_size, arc_info.seg_info_arr_[ARC_T1].size());
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT, arc_info.seg_info_arr_[ARC_B1].count());
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT * micro_size, arc_info.seg_info_arr_[ARC_B1].size());

  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_B1);
  ASSERT_EQ(500, arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.to_delete_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_B1));
  arc_iter_info.reuse(ARC_B1);

  // 4. Hit T1 again, T1 -> T2
  // T1: 600; B1: 500; T2: 500
  for (int64_t i = 1; i <= SS_MAX_ARC_HANDLE_OP_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(true, false, ObSSARCOpType::SS_ARC_HIT_T1, micro_size, 1));
  }
  ASSERT_EQ(max_micro_cnt - 400, arc_info.seg_info_arr_[ARC_T1].count());
  ASSERT_EQ((max_micro_cnt - 400) * micro_size, arc_info.seg_info_arr_[ARC_T1].size());
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT, arc_info.seg_info_arr_[ARC_T2].count());
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT * micro_size, arc_info.seg_info_arr_[ARC_T2].size());

  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(100, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  ASSERT_EQ(false, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.to_delete_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_T1));
  arc_iter_info.reuse(ARC_T1);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_B1);
  ASSERT_EQ(100, arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.op_cnt_);
  ASSERT_EQ(true, arc_iter_info.iter_seg_arr_[ARC_B1].op_info_.to_delete_);
  ASSERT_EQ(true, arc_iter_info.need_handle_arc_seg(ARC_B1));
  arc_iter_info.reuse(ARC_B1);

  // 5. Add some new micro into T1
  // T1: 700; B1: 500; T2: 500
  for (int64_t i = 1; i <= 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(true, false, ObSSARCOpType::SS_ARC_NEW_ADD, micro_size, 1));
  }
  ASSERT_EQ(max_micro_cnt - 300, arc_info.seg_info_arr_[ARC_T1].count());
  ASSERT_EQ((max_micro_cnt - 300) * micro_size, arc_info.seg_info_arr_[ARC_T1].size());

  // 6. Hit B1, B1 -> T2
  // T1: 700; B1: 300; T2: 700
  ASSERT_EQ(5000, arc_info.p_);
  for (int64_t i = 1; i <= 200; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(true, true, ObSSARCOpType::SS_ARC_HIT_GHOST, micro_size, 1));
  }
  ASSERT_EQ(5500, arc_info.p_);
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT + 200, arc_info.seg_info_arr_[ARC_T2].count());
  ASSERT_EQ((SS_MAX_ARC_HANDLE_OP_CNT + 200) * micro_size, arc_info.seg_info_arr_[ARC_T2].size());
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT - 200, arc_info.seg_info_arr_[ARC_B1].count());
  ASSERT_EQ((SS_MAX_ARC_HANDLE_OP_CNT - 200) * micro_size, arc_info.seg_info_arr_[ARC_B1].size());

  // 7. Continue to hit B1, B1 -> T2
  // T1: 700; B1: 100; T2: 900
  for (int64_t i = 1; i <= 200; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(true, true, ObSSARCOpType::SS_ARC_HIT_GHOST, micro_size, 1));
  }
  ASSERT_EQ(5500, arc_info.p_);
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(150, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T1);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(450, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  arc_iter_info.adjust_arc_iter_seg_info(ARC_T2);
  ASSERT_EQ(false, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.to_delete_);
  ASSERT_EQ(900, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.exp_iter_cnt_);
  arc_iter_info.reuse(ARC_T2);

  // 8. Evict from T2 to B2
  // T1: 700; B1: 100; T2: 400; B2: 500
  for (int64_t i = 1; i <= SS_MAX_ARC_HANDLE_OP_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(false, false, ObSSARCOpType::SS_ARC_TASK_EVICT_OP, micro_size, 1));
  }
  ASSERT_EQ(400, arc_info.seg_info_arr_[ARC_T2].count());
  ASSERT_EQ(400 * micro_size, arc_info.seg_info_arr_[ARC_T2].size());
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT, arc_info.seg_info_arr_[ARC_B2].count());
  ASSERT_EQ(SS_MAX_ARC_HANDLE_OP_CNT * micro_size, arc_info.seg_info_arr_[ARC_B2].size());
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(100, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T1);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T2);

  // 9. Hit B2, B2 -> T2
  // T1 : 700; B1: 100; T2: 600; B2: 300
  for (int64_t i = 1; i <= 200; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(false, true, ObSSARCOpType::SS_ARC_HIT_GHOST, micro_size, 1));
  }
  ASSERT_EQ(3500, arc_info.p_);
  ASSERT_EQ(600, arc_info.seg_info_arr_[ARC_T2].count());
  ASSERT_EQ(600 * micro_size, arc_info.seg_info_arr_[ARC_T2].size());
  ASSERT_EQ(300, arc_info.seg_info_arr_[ARC_B2].count());
  ASSERT_EQ(300 * micro_size, arc_info.seg_info_arr_[ARC_B2].size());
  ASSERT_EQ(true, arc_info.trigger_eviction());

  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(300, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T1);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T2);

  // 10. Add some new micro into T1
  // T1: 1100; B1: 100; T2: 600; B2: 300
  for (int64_t i = 1; i <= 400; ++i) {
    ASSERT_EQ(OB_SUCCESS, arc_info.adjust_seg_info(true, false, ObSSARCOpType::SS_ARC_NEW_ADD, micro_size, 1));
  }
  ASSERT_EQ(1100, arc_info.seg_info_arr_[ARC_T1].count());
  ASSERT_EQ(1100 * micro_size, arc_info.seg_info_arr_[ARC_T1].size());
  ASSERT_EQ(600, arc_info.seg_info_arr_[ARC_T2].count());
  ASSERT_EQ(600 * micro_size, arc_info.seg_info_arr_[ARC_T2].size());
  ASSERT_EQ(true, arc_info.trigger_eviction());
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T1);
  ASSERT_EQ(700, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.op_cnt_);
  arc_iter_info.adjust_arc_iter_seg_info(ARC_T1);
  ASSERT_EQ(1100, arc_iter_info.iter_seg_arr_[ARC_T1].op_info_.exp_iter_cnt_);
  arc_iter_info.reuse(ARC_T1);
  arc_info.calc_arc_iter_info(arc_iter_info, ARC_T2);
  ASSERT_EQ(0, arc_iter_info.iter_seg_arr_[ARC_T2].op_info_.op_cnt_);
  arc_iter_info.reuse(ARC_T2);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_arc_info.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_arc_info.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}