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

#define USING_LOG_PREFIX RS

#include <algorithm>
#include <gmock/gmock.h>
#include <random>

#define private public
#include "rootserver/ob_partition_balance.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{

bool share::schema::ObSimpleTableSchemaV2::is_valid() const { return true; }

namespace rootserver
{
using namespace common;
using namespace share;

static const uint64_t GB = 1024L * 1024L * 1024L;
static const int64_t TEST_DISK_BALANCE_THRESHOLD = 0;
static const int64_t TEST_BALANCER_TOLERANCE_PERCENTAGE = 10;
static int64_t g_ls_cnt = 3;
// optional per-LS ls_group_id for ls_group level disk balance tests; empty means all 0
static ObArray<uint64_t> g_ls_group_ids;
struct TestTransferTask
{
public:
  TestTransferTask() : src_ls_id_(), dest_ls_id_(), part_() {}
  TestTransferTask(const ObLSID &src_ls_id, const ObLSID &dest_ls_id, const ObTransferPartInfo &part_info)
      : src_ls_id_(src_ls_id), dest_ls_id_(dest_ls_id), part_(part_info) {}
  ObLSID src_ls_id_;
  ObLSID dest_ls_id_;
  ObTransferPartInfo part_;
  TO_STRING_KV(K_(src_ls_id), K_(dest_ls_id), K_(part));
};
ObArray<TestTransferTask> g_transfer_task_arr;

struct DiskBalanceMetric
{
  DiskBalanceMetric() : min_size_(0), max_size_(0), diff_ratio_bp_(0), balanced_(true) {}
  int64_t min_size_;
  int64_t max_size_;
  int64_t diff_ratio_bp_;
  bool balanced_;
  TO_STRING_KV(K_(min_size), K_(max_size), K_(diff_ratio_bp), K_(balanced));
};

struct DiskBalanceRatioStat
{
  DiskBalanceRatioStat()
      : avg_bp_(0), p50_bp_(0), p90_bp_(0), p95_bp_(0), p99_bp_(0), max_bp_(0),
        balanced_cnt_(0), total_cnt_(0) {}
  int64_t avg_bp_;
  int64_t p50_bp_;
  int64_t p90_bp_;
  int64_t p95_bp_;
  int64_t p99_bp_;
  int64_t max_bp_;
  int64_t balanced_cnt_;
  int64_t total_cnt_;
  TO_STRING_KV(K_(avg_bp), K_(p50_bp), K_(p90_bp), K_(p95_bp), K_(p99_bp), K_(max_bp),
      K_(balanced_cnt), K_(total_cnt));
};

struct RandomDiskBalanceMetricStat
{
  RandomDiskBalanceMetricStat() : ratio_bps_(), balanced_cnt_(0) {}
  ObArray<int64_t> ratio_bps_;
  int64_t balanced_cnt_;

  int add(const DiskBalanceMetric &metric)
  {
    int ret = ratio_bps_.push_back(metric.diff_ratio_bp_);
    if (OB_SUCC(ret) && metric.balanced_) {
      ++balanced_cnt_;
    }
    return ret;
  }

  int build(DiskBalanceRatioStat &stat) const
  {
    int ret = OB_SUCCESS;
    stat = DiskBalanceRatioStat();
    stat.total_cnt_ = ratio_bps_.count();
    stat.balanced_cnt_ = balanced_cnt_;
    if (ratio_bps_.empty()) {
    } else {
      ObArray<int64_t> sorted_ratio_bps;
      int64_t sum = 0;
      ARRAY_FOREACH(ratio_bps_, idx) {
        if (OB_FAIL(sorted_ratio_bps.push_back(ratio_bps_.at(idx)))) {
          LOG_WARN("push back failed", KR(ret), K(idx));
        } else {
          sum += ratio_bps_.at(idx);
        }
      }
      if (OB_SUCC(ret)) {
        std::sort(sorted_ratio_bps.begin(), sorted_ratio_bps.end());
        stat.avg_bp_ = sum / sorted_ratio_bps.count();
        stat.p50_bp_ = percentile(sorted_ratio_bps, 50);
        stat.p90_bp_ = percentile(sorted_ratio_bps, 90);
        stat.p95_bp_ = percentile(sorted_ratio_bps, 95);
        stat.p99_bp_ = percentile(sorted_ratio_bps, 99);
        stat.max_bp_ = sorted_ratio_bps.at(sorted_ratio_bps.count() - 1);
      }
    }
    return ret;
  }

  static int64_t percentile(const ObArray<int64_t> &sorted_values, const int64_t percent)
  {
    int64_t value = 0;
    if (!sorted_values.empty()) {
      int64_t idx = (sorted_values.count() * percent + 99) / 100 - 1; // nearest-rank
      idx = std::max<int64_t>(0, std::min<int64_t>(idx, sorted_values.count() - 1));
      value = sorted_values.at(idx);
    }
    return value;
  }
};

struct RandomDiskBalanceStat
{
  RandomDiskBalanceMetricStat before_ls_;
  RandomDiskBalanceMetricStat after_ls_;
  RandomDiskBalanceMetricStat before_lsg_;
  RandomDiskBalanceMetricStat after_lsg_;

  int add(
      const DiskBalanceMetric &before_ls,
      const DiskBalanceMetric &after_ls,
      const DiskBalanceMetric &before_lsg,
      const DiskBalanceMetric &after_lsg)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(before_ls_.add(before_ls))) {
    } else if (OB_FAIL(after_ls_.add(after_ls))) {
    } else if (OB_FAIL(before_lsg_.add(before_lsg))) {
    } else if (OB_FAIL(after_lsg_.add(after_lsg))) {
    }
    return ret;
  }
};

int ObPartitionBalance::prepare_ls_()
{
  common::ObZone primary_zone;
  (void)primary_zone.assign("z1");
  int ls_cnt = g_ls_cnt;
  allocator_.reset();
  ls_desc_array_.reset();
  ls_desc_map_.reuse();
  bg_map_.reuse();
  for (int i = 1; i <= ls_cnt; i++) {
    uint64_t ls_group_id = 0;
    if (i - 1 < g_ls_group_ids.count()) {
      ls_group_id = g_ls_group_ids.at(i - 1);
    }
    auto ls_desc = new ObLSDesc(ObLSID(i), ls_group_id, primary_zone);
    ls_desc_array_.push_back(ls_desc);
    ls_desc_map_.set_refactored(ls_desc->ls_id_, ls_desc);
  }
  LOG_INFO("TEST: prepare ls", K(g_ls_cnt), K(ls_desc_array_));
  return OB_SUCCESS;
}

int ObPartitionBalance::add_transfer_task_(
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    ObPartGroupInfo *part_group,
    bool modify_ls_desc)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(part_group->get_part_list(), idx) {
    const ObTransferPartInfo &part_info = part_group->get_part_list().at(idx);
    ret = g_transfer_task_arr.push_back(TestTransferTask(src_ls_id, dest_ls_id, part_info));
  }
  if (OB_FAIL(ret) || !modify_ls_desc) {
  } else if (OB_FAIL(update_ls_desc_(
      src_ls_id,
      -1,
      part_group->get_data_size() * -1,
      part_group->get_weight() * -1))) {
    LOG_WARN("update_ls_desc", KR(ret), K(src_ls_id), KPC(part_group));
  } else if (OB_FAIL(update_ls_desc_(
      dest_ls_id,
      1,
      part_group->get_data_size(),
      part_group->get_weight()))) {
    LOG_WARN("update_ls_desc", KR(ret), K(dest_ls_id), KPC(part_group));
  }
  LOG_INFO("TEST: add transfer task", K(src_ls_id), K(dest_ls_id), KPC(part_group));
  return ret;
}

class TestObPartitionBalance : public ::testing::Test
{
public:
  void SetUp() override
  {
    pb_.tenant_id_ = 1;
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(pb_.tenant_id_));
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(pb_.tenant_id_));
    ASSERT_TRUE(tenant_config.is_valid());
    tenant_config->_partition_balance_disk_threshold = TEST_DISK_BALANCE_THRESHOLD;
    GCONF.balancer_tolerance_percentage = TEST_BALANCER_TOLERANCE_PERCENTAGE;
    pb_.bg_map_.create(128, "UnwightedBG");
    pb_.weighted_bg_map_.create(128, "WeightedBG");
    pb_.ls_desc_map_.create(10, "LsDescMap");
    pb_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    pb_.inited_ = true;
    g_ls_group_ids.reset();
    pb_.prepare_ls_();
    g_transfer_task_arr.reset();
  }
  void TearDown() override { g_ls_group_ids.reset(); }
private:
  ObPartitionBalance pb_;

  int add_pg_to_ls_desc(
      const ObLSID &ls_id,
      const uint64_t data_size,
      const int64_t unweighted_part_cnt = 1,
      const int64_t weighted_part_cnt = 0)
  {
    int ret = OB_SUCCESS;
    UNUSED(weighted_part_cnt);
    ObLSDesc *ls_desc = nullptr;
    for (int64_t i = 0; i < unweighted_part_cnt && OB_SUCC(ret); ++i) {
      int64_t table_id = i + ls_id.id() * 10000;
      int64_t part_object_id = table_id;
      ObTransferPartInfo new_part(table_id, part_object_id);
      ObBalanceGroup bg;
      bg.id_ = ObBalanceGroupID(table_id % 50, table_id % 50);
      bg.name_ = "TESTBG";
      ObSimpleTableSchemaV2 mock_table_schema;
      mock_table_schema.set_database_id(123123123);
      int64_t part_group_uid = part_object_id;
      int64_t tablet_size = data_size/unweighted_part_cnt;
      int64_t balance_weight = 0;
      int64_t part_balance_weight = 0;
      if (OB_FAIL(pb_.add_new_part_to_update_maps_(ls_id, bg, mock_table_schema, part_group_uid, new_part, tablet_size, balance_weight, part_balance_weight))) {
        LOG_WARN("add new partition group to balance group failed", KR(ret), K(ls_id), K(bg), K(new_part));
      } else if (OB_FAIL(pb_.ls_desc_map_.get_refactored(ls_id, ls_desc))) {
      } else {
        ls_desc->add_partgroup(1, tablet_size, 0);
      }
    }
    return ret;
  }

  int add_pg_to_ls_desc(
      const ObLSID &ls_id,
      const ObBalanceGroupID &bg_id,
      const uint64_t part_group_uid,
      const uint64_t data_size)
  {
    int ret = OB_SUCCESS;
    ObLSDesc *ls_desc = nullptr;
    int64_t table_id = part_group_uid;
    ObTransferPartInfo new_part(table_id, table_id);
    ObBalanceGroup bg;
    bg.id_ = bg_id;
    bg.name_ = "TESTBG";
    ObSimpleTableSchemaV2 mock_table_schema;
    mock_table_schema.set_database_id(123123123);
    int64_t balance_weight = 0;
    int64_t part_balance_weight = 0;
    if (OB_FAIL(pb_.add_new_part_to_update_maps_(
        ls_id, bg, mock_table_schema, part_group_uid, new_part,
        data_size, balance_weight, part_balance_weight))) {
      LOG_WARN("add new partition group to balance group failed", KR(ret), K(ls_id), K(bg), K(new_part));
    } else if (OB_FAIL(pb_.ls_desc_map_.get_refactored(ls_id, ls_desc))) {
      LOG_WARN("get ls desc failed", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls desc is null", KR(ret), K(ls_id));
    } else {
      ls_desc->add_partgroup(1, data_size, 0);
    }
    return ret;
  }

  int64_t get_ls_data_size(const uint64_t ls_id) const
  {
    int64_t data_size = -1;
    for (int64_t idx = 0; idx < pb_.ls_desc_array_.count(); ++idx) {
      if (pb_.ls_desc_array_.at(idx)->get_ls_id() == ObLSID(ls_id)) {
        data_size = pb_.ls_desc_array_.at(idx)->get_data_size();
        break;
      }
    }
    return data_size;
  }

  int prepare_random_ls_group_layout(int64_t &lsg_cnt, int64_t &ls_per_lsg)
  {
    int ret = OB_SUCCESS;
    const int64_t MAX_LS_CNT = 50;
    ls_per_lsg = std::rand() % 4 + 2; // [2, 5]
    const int64_t max_lsg_cnt = MAX_LS_CNT / ls_per_lsg;
    const int64_t min_lsg_cnt = std::max<int64_t>(2, max_lsg_cnt / 2);
    lsg_cnt = min_lsg_cnt + std::rand() % (max_lsg_cnt - min_lsg_cnt + 1);
    g_ls_cnt = lsg_cnt * ls_per_lsg;
    g_ls_group_ids.reset();
    for (int64_t lsg_id = 1; OB_SUCC(ret) && lsg_id <= lsg_cnt; ++lsg_id) {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < ls_per_lsg; ++idx) {
        if (OB_FAIL(g_ls_group_ids.push_back(lsg_id))) {
          LOG_WARN("push back ls group id failed", KR(ret), K(lsg_id), K(idx));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = pb_.prepare_ls_();
    }
    return ret;
  }

  int calc_ls_metric(DiskBalanceMetric &metric) const
  {
    int ret = OB_SUCCESS;
    ObArray<int64_t> sizes;
    ARRAY_FOREACH(pb_.ls_desc_array_, idx) {
      if (OB_FAIL(sizes.push_back(pb_.ls_desc_array_.at(idx)->get_data_size()))) {
        LOG_WARN("push back ls size failed", KR(ret), K(idx));
      }
    }
    if (OB_SUCC(ret)) {
      ret = calc_disk_metric(sizes, metric);
    }
    return ret;
  }

  int calc_lsg_metric(DiskBalanceMetric &metric) const
  {
    int ret = OB_SUCCESS;
    ObArray<uint64_t> lsg_ids;
    ObArray<int64_t> lsg_sizes;
    ARRAY_FOREACH(pb_.ls_desc_array_, idx) {
      const ObLSDesc *ls_desc = pb_.ls_desc_array_.at(idx);
      int64_t pos = OB_INVALID_INDEX;
      for (int64_t lsg_idx = 0; lsg_idx < lsg_ids.count(); ++lsg_idx) {
        if (lsg_ids.at(lsg_idx) == ls_desc->get_ls_group_id()) {
          pos = lsg_idx;
          break;
        }
      }
      if (OB_INVALID_INDEX != pos) {
        lsg_sizes.at(pos) += ls_desc->get_data_size();
      } else if (OB_FAIL(lsg_ids.push_back(ls_desc->get_ls_group_id()))) {
        LOG_WARN("push back lsg id failed", KR(ret), K(idx));
      } else if (OB_FAIL(lsg_sizes.push_back(ls_desc->get_data_size()))) {
        LOG_WARN("push back lsg size failed", KR(ret), K(idx));
      }
    }
    if (OB_SUCC(ret)) {
      ret = calc_disk_metric(lsg_sizes, metric);
    }
    return ret;
  }

  static int calc_disk_metric(const ObArray<int64_t> &sizes, DiskBalanceMetric &metric)
  {
    int ret = OB_SUCCESS;
    metric = DiskBalanceMetric();
    if (OB_UNLIKELY(sizes.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty size array", KR(ret));
    } else {
      metric.min_size_ = sizes.at(0);
      metric.max_size_ = sizes.at(0);
      ARRAY_FOREACH(sizes, idx) {
        metric.min_size_ = std::min(metric.min_size_, sizes.at(idx));
        metric.max_size_ = std::max(metric.max_size_, sizes.at(idx));
      }
      if (metric.max_size_ > 0) {
        metric.diff_ratio_bp_ = static_cast<int64_t>(
            static_cast<double>(metric.max_size_ - metric.min_size_) * 10000.0 / metric.max_size_);
        metric.balanced_ = metric.diff_ratio_bp_ <= TEST_BALANCER_TOLERANCE_PERCENTAGE * 100;
      }
    }
    return ret;
  }

};

// case 1: completely balanced
TEST_F(TestObPartitionBalance, BasicDiskBalance) {
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), 1200 * GB, 12));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), 12 * GB, 12));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), 0, 12));
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  LOG_INFO("TEST: transfer task", K(pb_.ls_desc_array_), K(g_transfer_task_arr));
  ObLSDesc *ls_max = pb_.ls_desc_array_.at(g_ls_cnt - 1);
  ObLSDesc *ls_min = pb_.ls_desc_array_.at(0);
  ASSERT_TRUE((ls_max->get_data_size() - ls_min->get_data_size()) < ls_max->get_data_size() * 10 / 100);
  // no task any more
  g_transfer_task_arr.reset();
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  ASSERT_TRUE(g_transfer_task_arr.empty());
}

// case 2: In scenarios where a single partition is too large, it cannot be completely balanced, but no error will be reported
TEST_F(TestObPartitionBalance, OneLargePart) {
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), 1000 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), 10 * GB, 2));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), 10 * GB, 3));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), 0, 3));
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  LOG_INFO("TEST: transfer task", K(pb_.ls_desc_array_), K(g_transfer_task_arr));
  ObLSDesc *ls_max = pb_.ls_desc_array_.at(g_ls_cnt - 1);
  ObLSDesc *ls_min = pb_.ls_desc_array_.at(0);
  ASSERT_FALSE((ls_max->get_data_size() - ls_min->get_data_size()) < ls_max->get_data_size() * 10 / 100);
  // no task any more
  g_transfer_task_arr.reset();
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  ASSERT_TRUE(g_transfer_task_arr.empty());
}

// case 3: random test
TEST_F(TestObPartitionBalance, RandomDiskBalance) {
  int ret = OB_SUCCESS;
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  const int64_t LOOP_CNT = 200;

  // subcase 1: LS 间 data_size 随机差异大，同 bg 内不同 ls 上的分区大小差异也很大，
  // 但同一 LS 内不同 bg 间 pg 个数和 data_size 差异小，且 bg 内 pg 的个数分布均匀。
  RandomDiskBalanceStat subcase1_stat;
  int64_t subcase1_transfer_task_cnt = 0;
  int64_t subcase1_max_transfer_task_cnt = 0;
  for (int64_t i = 0; i < LOOP_CNT; ++i) {
    int64_t transfer_task_cnt = 0;
    int64_t lsg_cnt = 0;
    int64_t ls_per_lsg = 0;
    ASSERT_EQ(OB_SUCCESS, prepare_random_ls_group_layout(lsg_cnt, ls_per_lsg));
    g_transfer_task_arr.reset();
    // This count as baseline, per ls may be 1 more than this count randomly,
    // so that each ls count diff is no more than 1.
    int64_t part_count_per_ls = std::rand() % 2000 + 1;
    for (int64_t j = 1; j <= g_ls_cnt && OB_SUCC(ret); ++j) {
      int64_t part_count = part_count_per_ls + std::rand() % 2;
      const int64_t data_size = (std::rand() % 10000) * GB + (std::rand() % GB) + 1;
      if (OB_FAIL(add_pg_to_ls_desc(ObLSID(j), data_size, part_count))) {
        LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "add_pg_to_ls_desc failed", KR(ret), K(j), K(pb_.ls_desc_array_));
      }
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    DiskBalanceMetric before_ls;
    DiskBalanceMetric before_lsg;
    DiskBalanceMetric after_ls;
    DiskBalanceMetric after_lsg;
    ASSERT_EQ(OB_SUCCESS, calc_ls_metric(before_ls));
    ASSERT_EQ(OB_SUCCESS, calc_lsg_metric(before_lsg));
    if (FAILEDx(pb_.process_balance_partition_disk_())) {
      LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "disk_balance failed", KR(ret), K(pb_.ls_desc_array_));
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    transfer_task_cnt = g_transfer_task_arr.count();
    subcase1_transfer_task_cnt += transfer_task_cnt;
    subcase1_max_transfer_task_cnt = std::max(subcase1_max_transfer_task_cnt, transfer_task_cnt);
    ASSERT_EQ(OB_SUCCESS, calc_ls_metric(after_ls));
    ASSERT_EQ(OB_SUCCESS, calc_lsg_metric(after_lsg));
    ASSERT_EQ(OB_SUCCESS, subcase1_stat.add(before_ls, after_ls, before_lsg, after_lsg));
    // no task any more
    g_transfer_task_arr.reset();
    ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
    ASSERT_TRUE(g_transfer_task_arr.empty());
  }
  DiskBalanceRatioStat subcase1_before_ls;
  DiskBalanceRatioStat subcase1_after_ls;
  DiskBalanceRatioStat subcase1_before_lsg;
  DiskBalanceRatioStat subcase1_after_lsg;
  ASSERT_EQ(OB_SUCCESS, subcase1_stat.before_ls_.build(subcase1_before_ls));
  ASSERT_EQ(OB_SUCCESS, subcase1_stat.after_ls_.build(subcase1_after_ls));
  ASSERT_EQ(OB_SUCCESS, subcase1_stat.before_lsg_.build(subcase1_before_lsg));
  ASSERT_EQ(OB_SUCCESS, subcase1_stat.after_lsg_.build(subcase1_after_lsg));
  LOG_DBA_WARN(OB_PARTITION_ALREADY_BALANCED, "msg", "TEST: finish random subcase1",
      K(subcase1_transfer_task_cnt), K(subcase1_max_transfer_task_cnt),
      K(subcase1_before_ls), K(subcase1_after_ls), K(subcase1_before_lsg), K(subcase1_after_lsg));

  // subcase 2: 保证 bg 间 pg 个数和 data_size 也随机差异大. 但 LS 间差异可能并不很大（大数定律）。
  RandomDiskBalanceStat subcase2_stat;
  int64_t subcase2_transfer_task_cnt = 0;
  int64_t subcase2_max_transfer_task_cnt = 0;
  for (int64_t i = 0; i < LOOP_CNT; ++i) {
    int64_t transfer_task_cnt = 0;
    int64_t lsg_cnt = 0;
    int64_t ls_per_lsg = 0;
    ASSERT_EQ(OB_SUCCESS, prepare_random_ls_group_layout(lsg_cnt, ls_per_lsg));
    g_transfer_task_arr.reset();
    int64_t pg_uid = 500000;
    int64_t ls_idx = 0;
    const int64_t bg_count = std::rand() % 21 + 20; // [20, 40]
    // 每个 BG 内 PG 数不超过两轮 LS，单 BG 内和全局 LS 总 PG 个数差都不超过 1。
    // BG 间基准 PG size 随机差异大，BG 内 PG size 在基准 size 的 10% 范围内小幅波动。
    for (int64_t bg_idx = 1; bg_idx <= bg_count; ++bg_idx) {
      const ObBalanceGroupID bg_id(bg_idx, bg_idx);
      int64_t part_count = std::rand() % (2 * g_ls_cnt) + 1;
      const double rand_unit = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
      const int64_t base_pg_size = static_cast<int64_t>(
          rand_unit * rand_unit * rand_unit * static_cast<double>(100 * GB - 1)) + 1;
      const int64_t max_delta = std::max<int64_t>(1, base_pg_size / 10);
      for (int64_t j = 0; j < part_count; ++j) {
        int64_t data_size = base_pg_size + std::rand() % (2 * max_delta + 1) - max_delta;
        data_size = std::max<int64_t>(1, data_size);
        if (OB_FAIL(add_pg_to_ls_desc(ObLSID((ls_idx % g_ls_cnt) + 1), bg_id, pg_uid, data_size))) {
          LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "add_pg_to_ls_desc failed", KR(ret), K(ls_idx), K(bg_idx), K(pb_.ls_desc_array_));
        }
        ASSERT_EQ(OB_SUCCESS, ret);
        ls_idx += 1;
        pg_uid += 1;
      }
    }
    DiskBalanceMetric before_ls;
    DiskBalanceMetric before_lsg;
    DiskBalanceMetric after_ls;
    DiskBalanceMetric after_lsg;
    ASSERT_EQ(OB_SUCCESS, calc_ls_metric(before_ls));
    ASSERT_EQ(OB_SUCCESS, calc_lsg_metric(before_lsg));
    if (FAILEDx(pb_.process_balance_partition_disk_())) {
      LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "disk_balance failed", KR(ret), K(pb_.ls_desc_array_));
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    transfer_task_cnt = g_transfer_task_arr.count();
    subcase2_transfer_task_cnt += transfer_task_cnt;
    subcase2_max_transfer_task_cnt = std::max(subcase2_max_transfer_task_cnt, transfer_task_cnt);
    ASSERT_EQ(OB_SUCCESS, calc_ls_metric(after_ls));
    ASSERT_EQ(OB_SUCCESS, calc_lsg_metric(after_lsg));
    ASSERT_EQ(OB_SUCCESS, subcase2_stat.add(before_ls, after_ls, before_lsg, after_lsg));
    // no task any more
    g_transfer_task_arr.reset();
    ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
    ASSERT_TRUE(g_transfer_task_arr.empty());
  }
  DiskBalanceRatioStat subcase2_before_ls;
  DiskBalanceRatioStat subcase2_after_ls;
  DiskBalanceRatioStat subcase2_before_lsg;
  DiskBalanceRatioStat subcase2_after_lsg;
  ASSERT_EQ(OB_SUCCESS, subcase2_stat.before_ls_.build(subcase2_before_ls));
  ASSERT_EQ(OB_SUCCESS, subcase2_stat.after_ls_.build(subcase2_after_ls));
  ASSERT_EQ(OB_SUCCESS, subcase2_stat.before_lsg_.build(subcase2_before_lsg));
  ASSERT_EQ(OB_SUCCESS, subcase2_stat.after_lsg_.build(subcase2_after_lsg));
  LOG_DBA_WARN(OB_PARTITION_ALREADY_BALANCED, "msg", "TEST: finish random subcase2",
      K(subcase2_transfer_task_cnt), K(subcase2_max_transfer_task_cnt),
      K(subcase2_before_ls), K(subcase2_after_ls), K(subcase2_before_lsg), K(subcase2_after_lsg));

  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
}

// case 4: When swapping partitions cannot achieve balance, try transferring one PG at a time to balance.
// This situation occurs when the disk usage gap between LSes is mainly caused by differences in the PG count
// of BGs containing very large PGs. (Even after both intra-BG and inter-BG balancing, a difference of 1 PG may remain.)
// For example:
//            bg1           bg2             total
//   ls1     10G+50M   100G, 100G        210G+50M
//   ls2     10G+50M   100G, 100G        210G+50M
//   ls3     10G       100G              110G
//   ls4     10G       100G              110G
// In this scenario, the max LSes are ls1 and ls2, while the min LSes are ls3 and ls4.
// No swapping can balance them, as the size difference between swappable PGs within the same BG is insignificant compared to the gap between max and min LS.
// Therefore, we transfer one 100G PG from each max LS to a min LS, achieving the following:
//            bg1           bg2             total
//   ls1     10G+50M   100G              110G+50M
//   ls2     10G+50M   100G              110G+50M
//   ls3     10G       100G, 100G        210G
//   ls4     10G       100G, 100G        210G
TEST_F(TestObPartitionBalance, DiskBalanceByTransferOnly) {
  g_ls_cnt = 4;
  pb_.prepare_ls_();
  g_transfer_task_arr.reset();
  const ObBalanceGroupID bg1(1, 1);
  const ObBalanceGroupID bg2(2, 2);
  uint64_t part_group_uid = 1;
  const uint64_t small_skew = 50L * 1024L * 1024L;
  // bg1: all LSes have one small PG. The 50M size skew is below the swap size segment.
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 10 * GB + small_skew));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg1, part_group_uid++, 10 * GB + small_skew));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), bg1, part_group_uid++, 10 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(4), bg1, part_group_uid++, 10 * GB));
  // bg2: ls1/ls2 have two 100G PGs, ls3/ls4 have one 100G PG.
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg2, part_group_uid++, 100 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg2, part_group_uid++, 100 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg2, part_group_uid++, 100 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg2, part_group_uid++, 100 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), bg2, part_group_uid++, 100 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(4), bg2, part_group_uid++, 100 * GB));

  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  LOG_INFO("TEST: DiskBalanceByTransferOnly", K(pb_.ls_desc_array_), K(g_transfer_task_arr));
  ASSERT_EQ(2, g_transfer_task_arr.count());
  bool has_ls1_to_ls4 = false;
  bool has_ls2_to_ls3 = false;
  for (int64_t idx = 0; idx < g_transfer_task_arr.count(); ++idx) {
    has_ls1_to_ls4 = has_ls1_to_ls4
        || (ObLSID(1) == g_transfer_task_arr.at(idx).src_ls_id_
            && ObLSID(4) == g_transfer_task_arr.at(idx).dest_ls_id_);
    has_ls2_to_ls3 = has_ls2_to_ls3
        || (ObLSID(2) == g_transfer_task_arr.at(idx).src_ls_id_
            && ObLSID(3) == g_transfer_task_arr.at(idx).dest_ls_id_);
  }
  ASSERT_TRUE(has_ls1_to_ls4);
  ASSERT_TRUE(has_ls2_to_ls3);
  for (int64_t idx = 0; idx < pb_.ls_desc_array_.count(); ++idx) {
    const ObLSID ls_id = pb_.ls_desc_array_.at(idx)->get_ls_id();
    const uint64_t expected_size = (ObLSID(1) == ls_id || ObLSID(2) == ls_id)
        ? 110 * GB + small_skew : 210 * GB;
    ASSERT_EQ(expected_size, pb_.ls_desc_array_.at(idx)->get_data_size())
        << "idx=" << idx << ", ls_id=" << pb_.ls_desc_array_.at(idx)->get_ls_id().id();
  }

  // idempotent: no task on second run
  g_transfer_task_arr.reset();
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  ASSERT_TRUE(g_transfer_task_arr.empty());
}

// case 4.1: verify disk-balance move edge cases.
TEST_F(TestObPartitionBalance, DiskBalanceMoveBoundary) {
  {
    // pg_size == ls_diff/lsg_diff is not movable. After adding one more 1B PG,
    // LS-level still can not move, but LS-group level can move one 1B PG.
    g_ls_cnt = 4;
    ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(1)); // ls1 -> lsg1
    ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(1)); // ls2 -> lsg1
    ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(2)); // ls3 -> lsg2
    ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(2)); // ls4 -> lsg2
    pb_.prepare_ls_();
    g_transfer_task_arr.reset();

    const ObBalanceGroupID bg1(101, 101);
    uint64_t part_group_uid = 500000;
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 1));
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 1));
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg1, part_group_uid++, 1));
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), bg1, part_group_uid++, 1));
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(4), bg1, part_group_uid++, 1));

    ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
    ASSERT_TRUE(g_transfer_task_arr.empty());

    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg1, part_group_uid++, 1));
    ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
    ASSERT_EQ(1, g_transfer_task_arr.count());
    ASSERT_TRUE(ObLSID(1) == g_transfer_task_arr.at(0).src_ls_id_
        || ObLSID(2) == g_transfer_task_arr.at(0).src_ls_id_);
    ASSERT_TRUE(ObLSID(3) == g_transfer_task_arr.at(0).dest_ls_id_
        || ObLSID(4) == g_transfer_task_arr.at(0).dest_ls_id_);

    g_transfer_task_arr.reset();
    ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
    ASSERT_TRUE(g_transfer_task_arr.empty());
  }

  {
    // There is imbalance and src has more PGs in the BG, but every src PG is
    // larger than max_movable_pg_size, so get_closest_pg_to_target returns
    // OB_ENTRY_NOT_EXIST and disk balance should still succeed with no task.
    g_ls_cnt = 2;
    g_ls_group_ids.reset();
    pb_.prepare_ls_();
    g_transfer_task_arr.reset();

    const ObBalanceGroupID bg1(111, 111);
    uint64_t part_group_uid = 500000;
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 100 * GB));
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 110 * GB));
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg1, part_group_uid++, 180 * GB));
    ASSERT_EQ(210 * GB, get_ls_data_size(1));
    ASSERT_EQ(180 * GB, get_ls_data_size(2));

    ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
    ASSERT_TRUE(g_transfer_task_arr.empty());
    ASSERT_EQ(210 * GB, get_ls_data_size(1));
    ASSERT_EQ(180 * GB, get_ls_data_size(2));
  }

  {
    // A desc pair with a successful same-BG swap should not also do move in
    // the same round. The result has exactly the two opposite-direction swap tasks.
    g_ls_cnt = 2;
    g_ls_group_ids.reset();
    pb_.prepare_ls_();
    g_transfer_task_arr.reset();

    const ObBalanceGroupID bg1(121, 121);
    uint64_t part_group_uid = 500000;
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 100 * GB));
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 10 * GB));
    ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg1, part_group_uid++, 20 * GB));

    ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
    ASSERT_EQ(2, g_transfer_task_arr.count());
    bool has_swap_out = false;
    bool has_swap_in = false;
    for (int64_t idx = 0; idx < g_transfer_task_arr.count(); ++idx) {
      has_swap_out = has_swap_out
          || (ObLSID(1) == g_transfer_task_arr.at(idx).src_ls_id_
              && ObLSID(2) == g_transfer_task_arr.at(idx).dest_ls_id_);
      has_swap_in = has_swap_in
          || (ObLSID(2) == g_transfer_task_arr.at(idx).src_ls_id_
              && ObLSID(1) == g_transfer_task_arr.at(idx).dest_ls_id_);
    }
    ASSERT_TRUE(has_swap_out);
    ASSERT_TRUE(has_swap_in);
    ASSERT_EQ(30 * GB, get_ls_data_size(1));
    ASSERT_EQ(100 * GB, get_ls_data_size(2));
  }
}

// case 4.2: LS-level cross-BG swap.
// Same-BG swap can not work because each useful BG has PG only on one side.
// Move can not work because ls1 and ls2 have the same PG count.
TEST_F(TestObPartitionBalance, DiskBalanceCrossBgSwapByLs) {
  g_ls_cnt = 2;
  pb_.prepare_ls_();
  g_transfer_task_arr.reset();

  const ObBalanceGroupID bg1(201, 201);
  const ObBalanceGroupID bg2(202, 202);
  const ObBalanceGroupID bg3(203, 203);
  const ObBalanceGroupID bg4(204, 204);
  uint64_t part_group_uid = 500000;

  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 100 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg2, part_group_uid++, 1 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg3, part_group_uid++, 50 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg4, part_group_uid++, 1 * GB));

  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  LOG_INFO("TEST: DiskBalanceCrossBgSwapByLs", K(pb_.ls_desc_array_), K(g_transfer_task_arr));

  ASSERT_EQ(2, g_transfer_task_arr.count());
  bool has_ls1_to_ls2 = false;
  bool has_ls2_to_ls1 = false;
  for (int64_t idx = 0; idx < g_transfer_task_arr.count(); ++idx) {
    has_ls1_to_ls2 = has_ls1_to_ls2
        || (ObLSID(1) == g_transfer_task_arr.at(idx).src_ls_id_
            && ObLSID(2) == g_transfer_task_arr.at(idx).dest_ls_id_);
    has_ls2_to_ls1 = has_ls2_to_ls1
        || (ObLSID(2) == g_transfer_task_arr.at(idx).src_ls_id_
            && ObLSID(1) == g_transfer_task_arr.at(idx).dest_ls_id_);
  }
  ASSERT_TRUE(has_ls1_to_ls2);
  ASSERT_TRUE(has_ls2_to_ls1);

  g_transfer_task_arr.reset();
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  ASSERT_TRUE(g_transfer_task_arr.empty());
}

// case 4.3: LS-group level cross-BG swap.
// LS-level cross-BG swap can not work because pg_size equals ls_diff.
// LS-group level can still work because pg_size is smaller than ls_group_diff.
TEST_F(TestObPartitionBalance, DiskBalanceCrossBgSwapByLsGroup) {
  g_ls_cnt = 4;
  ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(1)); // ls1 -> lsg1
  ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(1)); // ls2 -> lsg1
  ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(2)); // ls3 -> lsg2
  ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(2)); // ls4 -> lsg2
  pb_.prepare_ls_();
  g_transfer_task_arr.reset();

  const ObBalanceGroupID bg1(211, 211);
  const ObBalanceGroupID bg2(212, 212);
  const ObBalanceGroupID bg3(213, 213);
  const ObBalanceGroupID bg4(214, 214);
  uint64_t part_group_uid = 500000;

  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 100 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(4), bg2, part_group_uid++, 1 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg3, part_group_uid++, 50 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), bg4, part_group_uid++, 20 * GB));

  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  LOG_INFO("TEST: DiskBalanceCrossBgSwapByLsGroup", K(pb_.ls_desc_array_), K(g_transfer_task_arr));

  ASSERT_EQ(2, g_transfer_task_arr.count());
  bool has_lsg1_to_lsg2 = false;
  bool has_lsg2_to_lsg1 = false;
  for (int64_t idx = 0; idx < g_transfer_task_arr.count(); ++idx) {
    has_lsg1_to_lsg2 = has_lsg1_to_lsg2
        || ((ObLSID(1) == g_transfer_task_arr.at(idx).src_ls_id_
              || ObLSID(2) == g_transfer_task_arr.at(idx).src_ls_id_)
            && (ObLSID(3) == g_transfer_task_arr.at(idx).dest_ls_id_
              || ObLSID(4) == g_transfer_task_arr.at(idx).dest_ls_id_));
    has_lsg2_to_lsg1 = has_lsg2_to_lsg1
        || ((ObLSID(3) == g_transfer_task_arr.at(idx).src_ls_id_
              || ObLSID(4) == g_transfer_task_arr.at(idx).src_ls_id_)
            && (ObLSID(1) == g_transfer_task_arr.at(idx).dest_ls_id_
              || ObLSID(2) == g_transfer_task_arr.at(idx).dest_ls_id_));
  }
  ASSERT_TRUE(has_lsg1_to_lsg2);
  ASSERT_TRUE(has_lsg2_to_lsg1);

  g_transfer_task_arr.reset();
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  ASSERT_TRUE(g_transfer_task_arr.empty());
}

// case 5: LS-level disk balance can not continue, while LS-group level disk balance can still move PGs.
// LS-group level disk balance does one swap and one transfer across LS-groups.
// Before balance: lsg1 total = 679G, lsg2 total = 800G.
//            bg1     bg2     bg3     total
//   ls1      200G            140G    340G
//   ls2     *180G*   159G            339G
//   ls3      200G     30G    100G    330G
//   ls4     *200G*  *30G*    240G    470G
// In this scenario, the LS-level max/min pair is ls4/ls3 and no LS-level move can be generated.
// LS-group level disk balance swaps 200G with 180G, then transfers 30G from ls4 to ls1.
// After balance: lsg1 total = 729G, lsg2 total = 750G.
//            bg1     bg2     bg3     total
//   ls1      200G   *30G*    140G    370G
//   ls2     *200G*   159G            359G
//   ls3      200G     30G    100G    330G
//   ls4     *180G*           240G    420G
TEST_F(TestObPartitionBalance, LsGroupSameLsgMaxMin) {
  g_ls_cnt = 4;
  ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(1)); // ls1 -> lsg1
  ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(1)); // ls2 -> lsg1
  ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(2)); // ls3 -> lsg2
  ASSERT_EQ(OB_SUCCESS, g_ls_group_ids.push_back(2)); // ls4 -> lsg2
  pb_.prepare_ls_();
  g_transfer_task_arr.reset();

  const ObBalanceGroupID bg1(11, 11);
  const ObBalanceGroupID bg2(12, 12);
  const ObBalanceGroupID bg3(13, 13);
  uint64_t part_group_uid = 500000;

  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg1, part_group_uid++, 200 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), bg3, part_group_uid++, 140 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg1, part_group_uid++, 180 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), bg2, part_group_uid++, 159 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), bg1, part_group_uid++, 200 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), bg2, part_group_uid++, 30 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), bg3, part_group_uid++, 100 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(4), bg1, part_group_uid++, 200 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(4), bg2, part_group_uid++, 30 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(4), bg3, part_group_uid++, 240 * GB));

  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  LOG_INFO("TEST: LsGroupSameLsgMaxMin", K(pb_.ls_desc_array_), K(g_transfer_task_arr));

  ASSERT_EQ(3, g_transfer_task_arr.count());
  bool has_lsg_swap_out = false;
  bool has_lsg_swap_in = false;
  bool has_lsg_transfer = false;
  for (int64_t idx = 0; idx < g_transfer_task_arr.count(); ++idx) {
    has_lsg_swap_out = has_lsg_swap_out
        || (ObLSID(4) == g_transfer_task_arr.at(idx).src_ls_id_
            && ObLSID(2) == g_transfer_task_arr.at(idx).dest_ls_id_);
    has_lsg_swap_in = has_lsg_swap_in
        || (ObLSID(2) == g_transfer_task_arr.at(idx).src_ls_id_
            && ObLSID(4) == g_transfer_task_arr.at(idx).dest_ls_id_);
    has_lsg_transfer = has_lsg_transfer
        || (ObLSID(4) == g_transfer_task_arr.at(idx).src_ls_id_
            && ObLSID(1) == g_transfer_task_arr.at(idx).dest_ls_id_);
  }
  ASSERT_TRUE(has_lsg_swap_out);
  ASSERT_TRUE(has_lsg_swap_in);
  ASSERT_TRUE(has_lsg_transfer);
  ASSERT_EQ(370 * GB, get_ls_data_size(1));
  ASSERT_EQ(359 * GB, get_ls_data_size(2));
  ASSERT_EQ(330 * GB, get_ls_data_size(3));
  ASSERT_EQ(420 * GB, get_ls_data_size(4));
  ASSERT_EQ(729 * GB, get_ls_data_size(1) + get_ls_data_size(2));
  ASSERT_EQ(750 * GB, get_ls_data_size(3) + get_ls_data_size(4));

  // idempotent: no task on second run
  g_transfer_task_arr.reset();
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  ASSERT_TRUE(g_transfer_task_arr.empty());
}

} // rootservice
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
