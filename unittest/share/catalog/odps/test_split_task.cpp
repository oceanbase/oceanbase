/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE
#include "share/external_table/ob_external_table_utils.h"
#include <random>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <fstream>
#include <cmath>
#include <iomanip>
#include <cstdlib>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
class TestSplitTask : public ::testing::Test
{
public:
  TestSplitTask() = default;
  ~TestSplitTask() = default;
  std::random_device rd;
  std::mt19937 gen;
  int64_t seed;
  // 在每个case开始前，种子一次，记录错误结果
  void SetUp() override {
    seed = rd();
    gen.seed(seed);
    cout << "seed: " << seed << endl;
  }
};

void show(
    common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx,
                      20> &assigned_idx, int64_t sqc_count, int64_t total_file_size) {
  struct SortByFileIdx {
    bool
    operator()(const oceanbase::share::ObExternalTableUtils::FileInfoWithIdx &l,
               const oceanbase::share::ObExternalTableUtils::FileInfoWithIdx &r)
        const {
      return l.file_idx_ < r.file_idx_ || (l.file_idx_ == r.file_idx_ && l.start_permyriad_ < r.start_permyriad_); // 同一个分区编号放一起
    }
  };
  lib::ob_sort(assigned_idx.begin(), assigned_idx.end(), SortByFileIdx());
  vector<int64_t> size_array(sqc_count, 0);
  vector<int64_t> count_array(sqc_count, 0);
  for (int64_t i = 0; i < assigned_idx.count(); i++) {
    if (assigned_idx.at(i).should_split_) {
      if (i == 0 ||
          assigned_idx.at(i).file_idx_ != assigned_idx.at(i - 1).file_idx_) {
        if (i != 0) {
          cout << endl;
        }
        cout << "* assigned file " << assigned_idx.at(i).file_idx_ << " to sqc "
             << assigned_idx.at(i).sqc_idx_ << " with split "
             << (assigned_idx.at(i).should_split_ ? "true" : "false") << endl;
        cout << "- should split file_idx_" << assigned_idx.at(i).file_idx_
             << endl;
      }
      size_array.at(assigned_idx.at(i).sqc_idx_) += assigned_idx.at(i).process_size_;
      count_array.at(assigned_idx.at(i).sqc_idx_) += 1;
      cout << "- file_size_ "    << setw(10) << assigned_idx.at(i).file_info_->file_size_ << " "
           << " file_idx_" << assigned_idx.at(i).file_idx_ << " "
           << " start_permyriad_ " << setw(3) << assigned_idx.at(i).start_permyriad_ << " "
           << " end_permyriad_ "  << setw(3) << assigned_idx.at(i).end_permyriad_ << " "
           << " process_size_ " << setw(10) << assigned_idx.at(i).process_size_ << " "
           << " assign to "     << setw(3) << assigned_idx.at(i).sqc_idx_ << endl;
    } else {
      /*cout << endl;
      cout << "* assigned file " << assigned_idx.at(i).file_idx_ << " to sqc "
           << assigned_idx.at(i).sqc_idx_ << " with split "
           << (assigned_idx.at(i).should_split_ ? "true" : "false") << endl;
      cout << "- should_not_split_" << assigned_idx.at(i).file_idx_ << endl;
      cout << " file_idx_ "     << assigned_idx.at(i).file_idx_ << " "
           << " process_size_ " << assigned_idx.at(i).process_size_
           << " file_size_ "    << assigned_idx.at(i).file_info_->file_size_
           << " assign to "     << assigned_idx.at(i).sqc_idx_ << " " << endl;
      */
      size_array.at(assigned_idx.at(i).sqc_idx_) += assigned_idx.at(i).process_size_;
      count_array.at(assigned_idx.at(i).sqc_idx_) += 1;
    }
  }
  int64_t total_get_size = 0;
  for (int64_t i = 0; i < size_array.size(); i++) {
    total_get_size += size_array.at(i);
    if (i < 10)
      cout << "sqc[" << i << " ] size = " << setw(10) << size_array.at(i) << " count = " << setw(10) << count_array.at(i) << endl;
    else
      cout << "sqc[" << i << "] size = " << setw(10) << size_array.at(i) << " count = " << setw(10) << count_array.at(i) << endl;
  }
  cout << "total_get_size: " << total_get_size << " total_file_size: " << total_file_size << endl;
}

void show_files(ObIArray<ObExternalFileInfo> &files) {
  common::ObSEArray<ObExternalFileInfo, 100> sorted_files;
  sorted_files.assign(files);
  struct SortByFileSize {
    bool operator()(const ObExternalFileInfo &l, const ObExternalFileInfo &r) const {
      return l.file_size_ > r.file_size_;
    }
  };
  lib::ob_sort(sorted_files.begin(), sorted_files.end(), SortByFileSize());

  for (int64_t i = 0; i < sorted_files.count(); i++) {
    cout << sorted_files.at(i).file_size_ << " ";
  }
}

// 计算标准差（标准差是方差的平方根）
double calculate_stddev(const vector<int64_t> &size_array) {
  if (size_array.empty()) return 0.0;

  double sum = 0.0;
  for (int64_t size : size_array) {
    sum += size;
  }
  double mean = sum / size_array.size();

  double variance = 0.0;
  for (int64_t size : size_array) {
    double diff = size - mean;
    variance += diff * diff;
  }
  variance /= size_array.size();

  // 标准差 = 方差的平方根
  return sqrt(variance);
}

// 计算SQC大小数组和标准差
double get_sqc_sizes_and_stddev(
    common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20> &assigned_idx,
    int64_t sqc_count,
    vector<int64_t> &size_array,
    int64_t &max_size) {
  struct SortByFileIdx {
    bool operator()(const oceanbase::share::ObExternalTableUtils::FileInfoWithIdx &l,
                    const oceanbase::share::ObExternalTableUtils::FileInfoWithIdx &r) const {
      return l.file_idx_ < r.file_idx_ || (l.file_idx_ == r.file_idx_ && l.end_permyriad_ < r.end_permyriad_);
    }
  };
  lib::ob_sort(assigned_idx.begin(), assigned_idx.end(), SortByFileIdx());

  size_array.assign(sqc_count, 0);
  for (int64_t i = 0; i < assigned_idx.count(); i++) {
    size_array.at(assigned_idx.at(i).sqc_idx_) += assigned_idx.at(i).process_size_;
  }
  // get max size_
  max_size = 0;
  int64_t sum_size = 0;
  for (int64_t i = 0; i < size_array.size(); i++) {
    if (size_array.at(i) > max_size) {
      max_size = size_array.at(i);
    }
    sum_size += size_array.at(i);
  }
  cout << "sum_size: " << sum_size << endl;

  return calculate_stddev(size_array);
}


class PowerLawDistribution {
public:
  PowerLawDistribution(double alpha, double x_min, std::mt19937 &gen)
      : alpha_(alpha), x_min_(x_min), gen_(gen), uniform_(0.0, 1.0) {
    // 确保 alpha > 1，否则分布没有有限均值
    if (alpha_ <= 1.0) {
      alpha_ = 1.1;  // 默认值
    }
  }

  double operator()() {
    double u = uniform_(gen_);
    // 避免 u 为 0 或 1 的边界情况
    if (u <= 0.0) u = 1e-10;
    if (u >= 1.0) u = 1.0 - 1e-10;
    // 逆变换采样: x = x_min * u^(-1/(alpha-1))
    // 等价于: x = x_min * (1-u)^(-1/(alpha-1))，但使用 u 更稳定
    return x_min_ * std::pow(u, -1.0 / (alpha_ - 1.0));
  }

private:
  double alpha_; // 幂律指数 (必须 > 1)
  double x_min_; // 最小值
  std::mt19937 &gen_;
  std::uniform_real_distribution<double> uniform_;
};

// 有界区间的幂律分布
// 概率密度函数: p(x) ∝ x^n, x ∈ [x0, x1]
// 使用逆变换采样方法，累积分布函数的逆函数为:
// x = [y * (x1^(n+1) - x0^(n+1)) + x0^(n+1)]^(1/(n+1))
// 其中 y 是 [0, 1] 上的均匀随机数
// 注意: n ≠ -1，否则会导致除以0
class BoundedPowerLawDistribution {
public:
  BoundedPowerLawDistribution(double alpha, double x0, double x1, std::mt19937 &gen)
      : alpha_(alpha), x0_(x0), x1_(x1), gen_(gen), uniform_(0.0, 1.0) {
    // 预计算常数项以提高性能
    // alpha < 0 x0 max value
    x0_power_ = std::pow(x0_, alpha_ + 1.0);
    x1_power_ = std::pow(x1_, alpha_ + 1.0);
    diff_power_ = x1_power_ - x0_power_;
    inv_n_plus_1_ = 1.0 / (alpha_ + 1.0);
  }

  double operator()() {
    double y = uniform_(gen_);  // 均匀随机数 [0, 1]
    if (y <= 0.0)
      y = 1e-10;
    if (y >= 1.0)
      y = 1.0 - 1e-10;

    // 逆变换采样: x = [y * (x1^(n+1) - x0^(n+1)) + x0^(n+1)]^(1/(n+1))
    double x = std::pow(diff_power_ * y + x0_power_, inv_n_plus_1_);
    return x;
  }

private:
  double alpha_;              // 幂律指数
  double x0_;             // 最小值
  double x1_;             // 最大值
  std::mt19937 &gen_;
  std::uniform_real_distribution<double> uniform_;
  // 预计算的常数项
  double x0_power_;
  double x1_power_;
  double diff_power_;
  double inv_n_plus_1_;
};

// 场景1: 泊松分布：分区数字是1000，SQC从4到64变化。第一组分割第二组不分割。
TEST_F(TestSplitTask, scenario1_variance_split_1000_partitions_sqc_variation) {
  // 设置环境变量，使 last_k_files_count = 0（不分割）
  setenv("OB_VARIANCE_SPLIT_DISABLE", "1", 1);

  std::poisson_distribution<int> dis(1000000000.0);
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 5; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = dis(gen);
    cout << "file.file_size_: " << file.file_size_ << endl;
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }
  vector<int64_t> sqc_counts = {4, 8, 16, 32, 64};
  for (int64_t sqc_count : sqc_counts) {
    {
      setenv("OB_VARIANCE_SPLIT_DISABLE", "1", 1);

      common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx,
                        20>
          assigned_idx;

      // 调用分配函数（此时 last_k_files_count =
      // 0，所有文件都不会被标记为不分割）
      int ret =
          ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
              files, assigned_idx, sqc_count);
      ASSERT_EQ(ret, OB_SUCCESS);

      // 计算SQC大小和标准差
      vector<int64_t> size_array;
      int64_t max_size = 0;
      double stddev = get_sqc_sizes_and_stddev(assigned_idx, sqc_count,
                                               size_array, max_size);

      cout << "no cutSQC Count: " << sqc_count << " max_size: " << max_size
           << ", StdDev no cut: " << stddev << endl;
      unsetenv("OB_VARIANCE_SPLIT_DISABLE");
    }
    {
      common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx,
                        20>
          assigned_idx;

      int ret =
          ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
              files, assigned_idx, sqc_count);
      ASSERT_EQ(ret, OB_SUCCESS);

      // 计算SQC大小和标准差
      vector<int64_t> size_array;
      int64_t max_size = 0;
      double stddev = get_sqc_sizes_and_stddev(assigned_idx, sqc_count,
                                               size_array, max_size);

      cout << "cut SQC Count: " << sqc_count << " max_size: " << max_size
           << ", StdDev no cut: " << stddev << endl;
    }
  }
}

// 场景2: 差异大：分区数字为1000，SQC从少到多进行变化。第一组分割第二组不分割。
TEST_F(TestSplitTask, scenario2_variance_split_1000_partitions_sqc_variation) {
  // 设置环境变量，使 last_k_files_count = 0（不分割）

  BoundedPowerLawDistribution power_law(-1.5, 10000000000.0, 1000000.0, gen);

  // 固定分区数为1000
  const int64_t partition_count = 1000;
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < partition_count; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = power_law();
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  // SQC从4到64变化
  vector<int64_t> sqc_counts = {4, 8, 16, 32, 64};

  for (int64_t sqc_count : sqc_counts) {
    {
      setenv("OB_VARIANCE_SPLIT_DISABLE", "1", 1);

      common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx,
                        20>
          assigned_idx;

      // 调用分配函数（此时 last_k_files_count = 0，所有文件都不会被标记为不分割）
      int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
          files, assigned_idx, sqc_count);
      ASSERT_EQ(ret, OB_SUCCESS);

      // 计算SQC大小和标准差
      vector<int64_t> size_array;
      int64_t max_size = 0;
      double stddev =
          get_sqc_sizes_and_stddev(assigned_idx, sqc_count, size_array, max_size);

      cout << "no cutSQC Count: " << sqc_count << " max_size: " << max_size << ", StdDev no cut: " << stddev << endl;
      unsetenv("OB_VARIANCE_SPLIT_DISABLE");
    }
    {
      common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx,
                        20>
          assigned_idx;

      int ret =
          ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
              files, assigned_idx, sqc_count);
      ASSERT_EQ(ret, OB_SUCCESS);

      // 计算SQC大小和标准差
      vector<int64_t> size_array;
      int64_t max_size = 0;
      double stddev = get_sqc_sizes_and_stddev(assigned_idx, sqc_count,
                                               size_array, max_size);

      cout << "cut SQC Count: " << sqc_count << " max_size: " << max_size
           << ", StdDev no cut: " << stddev << endl;
    }
  }
}

// 场景3: 差异大：分区数字为8，SQC从少到多进行变化。第一组分割第二组不分割。
TEST_F(TestSplitTask, scenario3_variance_split_4_partitions_sqc_variation) {
  // 设置环境变量，使 last_k_files_count = 0（不分割）

  BoundedPowerLawDistribution power_law(-1.1, 100000000000.0, 1000000.0, gen);

  // 固定分区数为1000
  const int64_t partition_count = 4;
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < partition_count; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = power_law();
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  // SQC从4到64变化
  vector<int64_t> sqc_counts = {2, 4, 8, 16, 32, 64};
  setenv("OB_VARIANCE_SPLIT_DISABLE", "1", 1);

  for (int64_t sqc_count : sqc_counts) {
    common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx,
                      20>
        assigned_idx;

    // 调用分配函数（此时 last_k_files_count = 0，所有文件都不会被标记为不分割）
    int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
        files, assigned_idx, sqc_count);
    ASSERT_EQ(ret, OB_SUCCESS);

    // 计算SQC大小和标准差
    vector<int64_t> size_array;
    int64_t max_size = 0;
    double stddev =
        get_sqc_sizes_and_stddev(assigned_idx, sqc_count, size_array, max_size);

    cout << "SQC Count: " << sqc_count << " max_size: " << max_size
         << ", StdDev no cut: " << stddev << endl;
  }

  // 清除环境变量
  unsetenv("OB_VARIANCE_SPLIT_DISABLE");
  for (int64_t sqc_count : sqc_counts) {
    common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx,
                      20>
        assigned_idx;

    int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
        files, assigned_idx, sqc_count);
    ASSERT_EQ(ret, OB_SUCCESS);

    // 计算SQC大小和标准差
    vector<int64_t> size_array;
    int64_t max_size = 0;
    double stddev =
        get_sqc_sizes_and_stddev(assigned_idx, sqc_count, size_array, max_size);

    cout << "SQC Count: " << sqc_count << " max_size: " << max_size
         << ", StdDev no cut: " << stddev << endl;
  }
}

// 场景4: 差异大：SQC少，分区数字从4到1024变化。第一组分割第二组不分割。
TEST_F(TestSplitTask, scenario4_variance_split_few_sqc_partition_variation) {
  // 设置环境变量，使 last_k_files_count = 0（不分割）

  BoundedPowerLawDistribution power_law(-1.1, 100000000000.0, 1000000.0, gen);

  // SQC数量固定为8（少）
  const int64_t sqc_count = 8;

  // 分区数从4到1024变化（2的幂次）
  vector<int64_t> partition_counts;
  for (int64_t i = 2; i <= 10; i++) {
    partition_counts.push_back(1LL << i); // 4, 8, 16, 32, 64, 128, 256, 512, 1024
  }


  for (int64_t partition_count : partition_counts) {
    common::ObSEArray<ObExternalFileInfo, 100> files;
    int64_t total_file_size = 0;
    for (int64_t i = 0; i < partition_count; i++) {
      ObExternalFileInfo file;
      file.file_id_ = i;
      file.file_size_ = power_law();
      file.row_count_ = 100000000;
      total_file_size += file.file_size_;
      files.push_back(file);
    }
    {
      setenv("OB_VARIANCE_SPLIT_DISABLE", "1", 1);
      common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20> assigned_idx;

      // 调用分配函数（此时 last_k_files_count = 0，所有文件都不会被标记为不分割）
      int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
          files, assigned_idx, sqc_count);
      ASSERT_EQ(ret, OB_SUCCESS);

      // 计算SQC大小和标准差
      vector<int64_t> size_array;
      int64_t max_size = 0;
      double stddev = get_sqc_sizes_and_stddev(assigned_idx, sqc_count, size_array, max_size);



      cout << "no cut partition " << "sqc_count: " << sqc_count << " partition_count: " << partition_count << " max_size: " << max_size << ", StdDev: " << stddev << endl;
      unsetenv("OB_VARIANCE_SPLIT_DISABLE");
    }
    {
      common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx,
                        20>
          assigned_idx;

      // 调用分配函数（此时 last_k_files_count =
      // 0，所有文件都不会被标记为不分割）
      int ret =
          ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
              files, assigned_idx, sqc_count);
      ASSERT_EQ(ret, OB_SUCCESS);

      // 计算SQC大小和标准差
      vector<int64_t> size_array;
      int64_t max_size = 0;
      double stddev = get_sqc_sizes_and_stddev(assigned_idx, sqc_count,
                                               size_array, max_size);

      cout << "cut partition " << "sqc_count: " << sqc_count << " partition_count: " << partition_count
           << " max_size: " << max_size << ", StdDev: " << stddev << endl;

    }
  }

  // 清除环境变量
  // 输出数据到文件用于绘制折线图
}


TEST_F(TestSplitTask, single_file_split_two_machine) {
  std::uniform_int_distribution<int> dis(10000000, 60 * 1024 * 1024 * 10);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;

  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 1; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = dis(gen);
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 2;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
}

TEST_F(TestSplitTask, single_file_split_four_machine) {
  std::uniform_int_distribution<int> dis(10000000, 60 * 1024 * 1024 * 10);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 1; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = dis(gen);
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 4;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
}

TEST_F(TestSplitTask, two_files_split_two_machine) {
  std::uniform_int_distribution<int> dis(10000000, 60 * 1024 * 1024 * 10);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  // 307924947 180461142
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 2; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = dis(gen);
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 2;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);
  show(assigned_idx, sqc_count, total_file_size);
  show_files(files);
}

TEST_F(TestSplitTask, two_files_split_8_machine) {
  std::uniform_int_distribution<int64_t> dis(10000000, 600LL * 1024 * 1024 *
10);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  // 307924947 180461142
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 2; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = dis(gen);
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 8;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);
  show(assigned_idx, sqc_count, total_file_size);
}

TEST_F(TestSplitTask, ten_files_split) {
  std::uniform_int_distribution<int> dis(10000000, 60 * 1024 * 1024 * 10);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 10; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = dis(gen);
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 2;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
}

// 按照用户财富，地区分区的日志分区大小通常符合幂律分布
TEST_F(TestSplitTask, power_law_distribution_thousand_files_split_64_machine) {
  BoundedPowerLawDistribution power_law(-1.5, 10000000000.0, 1000000.0, gen);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 1000; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = power_law();
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;

    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 64;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
  show_files(files);
}

// 按照用户财富，地区分区的日志分区大小通常符合幂律分布
TEST_F(TestSplitTask, power_law_distribution_thousand_files_split_8_machine) {
  BoundedPowerLawDistribution power_law(-1.5, 10000000000.0, 1000000.0, gen);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 1000; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = power_law();
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;

    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 8;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
  show_files(files);
}

// 按照用户财富，地区分区的日志分区大小通常符合幂律分布
TEST_F(TestSplitTask, power_law_distribution_8_files_split_8_machine) {
  BoundedPowerLawDistribution power_law(-1.1, 10000000000.0, 1000000.0, gen);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 8; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = power_law();
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;

    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 8;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
  show_files(files);
}

// 测试正态分布生成文件大小
// 大多数日志文件大小符合正态分布
TEST_F(TestSplitTask, normal_distribution_thousand_file_64_machine) {
  std::normal_distribution<double> dis(1000000000.0, 15.0);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 1000; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = dis(gen);
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 64;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
}

// 指数分布适用于记录事件之间的时间间隔或等待时间
// 按照时间间隔大小分区的文件大小符合指数分布（牵强）
TEST_F(TestSplitTask,
       thousand_files_split_64_machine_using_geometric_distribution) {
  std::geometric_distribution<int> dis(0.00000001);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 1000; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = dis(gen);
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 64;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
  show_files(files);
}

//柏松分布对于独立随机事件在时间或空间中发生的次数的随机过程，特别适用于稀疏事件的计数
// 如果按照日期统计，日志文件大小符合柏松分布，因为每天的日志文件大小是独立的，而且符合柏松分布
TEST_F(TestSplitTask,
       thousand_files_split_64_machine_using_poisson_distribution) {
  std::poisson_distribution<int> dis(1000000000.0);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 1000; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = dis(gen);
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 64;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
  show_files(files);
}

TEST_F(TestSplitTask,
       lineitem) {
  std::poisson_distribution<int> dis(1000000000.0);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 1; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = 1657402685;
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 64;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
  show_files(files);
}

TEST_F(TestSplitTask, x1) {
  std::poisson_distribution<int> dis(1000000000.0);
  // calc_assigned_odps_files_to_sqcs_optimized
  common::ObSEArray<ObExternalFileInfo, 100> files;
  int64_t total_file_size = 0;
  for (int64_t i = 0; i < 8; i++) {
    ObExternalFileInfo file;
    file.file_id_ = i;
    file.file_size_ = 11*1024*1024*(i+7);
    file.row_count_ = 100000000;
    total_file_size += file.file_size_;
    files.push_back(file);
  }

  ObExternalFileInfo file;
  file.file_id_ = 101;
  file.file_size_ = 11 * 1024 * 1024 * 80;
  file.row_count_ = 100000000;
  total_file_size += file.file_size_;
  files.push_back(file);

  common::ObSEArray<oceanbase::share::ObExternalTableUtils::FileInfoWithIdx, 20>
      assigned_idx;

  int64_t sqc_count = 6;
  int ret = ObExternalTableUtils::calc_assigned_odps_files_to_sqcs_optimized(
      files, assigned_idx, sqc_count);
  ASSERT_EQ(ret, OB_SUCCESS);

  show(assigned_idx, sqc_count, total_file_size);
  show_files(files);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}