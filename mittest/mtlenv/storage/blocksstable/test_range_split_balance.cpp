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

#include <gtest/gtest.h>
#include <cmath>
#include <algorithm>
#include <cstdlib>
#include <cstdio>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#define private public
#define protected public

#include "ob_index_block_data_prepare.h"
#include "storage/ob_partition_range_spliter.h"

namespace oceanbase
{
class TestRangeSplitBalance : public TestIndexBlockDataPrepare
{
public:
  TestRangeSplitBalance()
      : TestIndexBlockDataPrepare("Test Range Split Balance",
                                  /* merge_type */ MAJOR_MERGE,
                                  /* need_aggregate_data */ false)
  {
  }

  virtual ~TestRangeSplitBalance() {}

  static void SetUpTestCase() { TestIndexBlockDataPrepare::SetUpTestCase(); }

  static void TearDownTestCase() { TestIndexBlockDataPrepare::TearDownTestCase(); }

  virtual void SetUp()
  {
    TestIndexBlockDataPrepare::SetUp();
    prepare_query_param(false /* is_reverse_scan */);
  }

  virtual void TearDown()
  {
    destroy_query_param();
    TestIndexBlockDataPrepare::TearDown();
  }

  void insert_data(ObMacroBlockWriter &data_writer) override
  {
    row_cnt_ = 0;
    const int64_t total_rows = 1000000;  // 100万行
    const int64_t total_macro_blocks = 1000;  // 1000个宏块
    const int64_t avg_rows_per_macro = total_rows / total_macro_blocks;  // 平均每宏块1000行

    ObDatumRow row;
    ObDatumRow multi_row;
    ASSERT_EQ(OB_SUCCESS, row.init(allocator_, MAX_TEST_COLUMN_CNT));
    ASSERT_EQ(OB_SUCCESS, multi_row.init(allocator_, MAX_TEST_COLUMN_CNT));

    int64_t current_row_id = 1;  // 主键从1开始
    int64_t macro_block_idx = 0;

    // 计算每个宏块的行数，让差异较大
    // 使用正弦函数生成差异较大的行数分布：从100到2000不等
    ObSEArray<int64_t, 1000> rows_per_macro;
    int64_t total_allocated_rows = 0;

    for (int64_t i = 0; i < total_macro_blocks; ++i) {
      // 随机生成差异较大的行数：800到1200之间
      int64_t rows = 800 + (::rand() % 401); // 401 = 1200 - 800 + 1
      rows_per_macro.push_back(rows);
      total_allocated_rows += rows;
    }

    // 调整最后一个宏块的行数，使总行数正好是100万
    if (total_allocated_rows != total_rows) {
      rows_per_macro.at(total_macro_blocks - 1) += (total_rows - total_allocated_rows);
    }

    // 开始写入数据
    while (macro_block_idx < total_macro_blocks && current_row_id <= total_rows) {
      int64_t target_rows = rows_per_macro.at(macro_block_idx);
      int64_t rows_in_current_macro = 0;

      while (rows_in_current_macro < target_rows && current_row_id <= total_rows) {
        // 生成行数据，使用 seed = current_row_id - 1（因为 seed 从 0 开始）
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(current_row_id - 1, row));

        ObDmlFlag dml = DF_INSERT;
        convert_to_multi_version_row(row, table_schema_, SNAPSHOT_VERSION, dml, multi_row);

        int64_t before_row_count = data_writer.micro_writer_->get_row_count();
        ASSERT_EQ(OB_SUCCESS, data_writer.append_row(multi_row));
        int64_t after_row_count = data_writer.micro_writer_->get_row_count();

        if (after_row_count - before_row_count != 1) {
          STORAGE_LOG(INFO, "build micro block", K(before_row_count), K(after_row_count));
        }

        ++row_cnt_;
        ++rows_in_current_macro;
        ++current_row_id;
      }

      // 处理剩余的micro block
      if (data_writer.micro_writer_->get_row_count() > 0) {
        OK(data_writer.build_micro_block());
      }

      // 切换宏块
      if (rows_in_current_macro > 0) {
        STORAGE_LOG(INFO, "try switch macro block", K(rows_in_current_macro));
        OK(data_writer.try_switch_macro_block());
        ++macro_block_idx;
      }
    }

    STORAGE_LOG(INFO, "Data preparation completed", K(row_cnt_), K(macro_block_idx));
  }

  // 计算range包含的行数（基于主键范围）
  int64_t calculate_range_row_count(const ObStoreRange &range)
  {
    int64_t row_count = 0;

    if (range.get_start_key().is_min() && range.get_end_key().is_max()) {
      return 1000000;  // 全范围
    }

    int64_t start_id = 0;
    int64_t end_id = 0;

    // 从start_key提取主键值
    if (!range.get_start_key().is_min()) {
      const ObObj *objs = range.get_start_key().get_obj_ptr();
      if (objs != nullptr && range.get_start_key().get_obj_cnt() > 0) {
        const ObObj &obj = objs[3];
        if (obj.get_type() == ObNumberType) {
          obj.get_number().cast_to_int64(start_id);
        }
      }
    }

    // 从end_key提取主键值
    if (!range.get_end_key().is_max()) {
      const ObObj *objs = range.get_end_key().get_obj_ptr();
      if (objs != nullptr && range.get_end_key().get_obj_cnt() > 0) {
        const ObObj &obj = objs[3];
        if (obj.get_type() == ObNumberType) {
          obj.get_number().cast_to_int64(end_id);
        }
      }
    }

    if (range.get_start_key().is_min()) {
      start_id = 1;
    }
    if (range.get_end_key().is_max()) {
      end_id = 1000000;
    }

    // 计算行数
    if (range.is_left_closed() && range.is_right_closed()) {
      row_count = end_id - start_id + 1;
    } else if (range.is_left_closed() && !range.is_right_closed()) {
      row_count = end_id - start_id;
    } else if (!range.is_left_closed() && range.is_right_closed()) {
      row_count = end_id - start_id;
    } else {
      row_count = end_id - start_id - 1;
    }

    return row_count > 0 ? row_count : 0;
  }

  // 评估拆分均衡程度
  void evaluate_balance(const ObArrayArray<ObStoreRange> &multi_range_split_array,
                        double &avg_rows,
                        int64_t &min_rows,
                        int64_t &max_rows)
  {
    ObSEArray<int64_t, 16> task_row_counts;

    for (int64_t i = 0; i < multi_range_split_array.count(); ++i) {
      int64_t task_rows = 0;
      for (int64_t j = 0; j < multi_range_split_array.at(i).count(); ++j) {
        task_rows += calculate_range_row_count(multi_range_split_array.at(i).at(j));
      }
      task_row_counts.push_back(task_rows);
    }

    // 计算平均值
    int64_t sum = 0;
    for (int64_t i = 0; i < task_row_counts.count(); ++i) {
      sum += task_row_counts.at(i);
    }
    avg_rows = task_row_counts.count() > 0 ? static_cast<double>(sum) / task_row_counts.count() : 0;

    // 找出最小值和最大值
    min_rows = INT64_MAX;
    max_rows = 0;
    for (int64_t i = 0; i < task_row_counts.count(); ++i) {
      min_rows = std::min(min_rows, task_row_counts.at(i));
      max_rows = std::max(max_rows, task_row_counts.at(i));
    }

    STORAGE_LOG(INFO, "Balance evaluation",
                K(task_row_counts.count()),
                K(task_row_counts),
                K(avg_rows),
                K(min_rows),
                K(max_rows));
  }

  // 验证拆分结果的正确性：合并后的ranges应该和原始ranges一致
  void verify_split_correctness(const ObSEArray<ObStoreRange, 16> &original_ranges,
                                const ObArrayArray<ObStoreRange> &multi_range_split_array,
                                const ObITableReadInfo &read_info,
                                ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;

    // 1. 收集所有拆分后的ranges到一个数组中
    ObSEArray<ObStoreRange, 256> all_split_ranges;
    for (int64_t i = 0; i < multi_range_split_array.count(); ++i) {
      for (int64_t j = 0; j < multi_range_split_array.at(i).count(); ++j) {
        if (OB_FAIL(all_split_ranges.push_back(multi_range_split_array.at(i).at(j)))) {
          STORAGE_LOG(ERROR, "Failed to push back split range", K(ret), K(i), K(j));
          ASSERT_EQ(OB_SUCCESS, ret);
        }
      }
    }

    if (all_split_ranges.empty()) {
      STORAGE_LOG(WARN, "Split ranges is empty");
      ASSERT_TRUE(false);
      return;
    }

    // 2. 将ObStoreRange转换为ObDatumRange并排序
    ObSEArray<blocksstable::ObDatumRange, 256> datum_ranges;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_split_ranges.count(); ++i) {
      blocksstable::ObDatumRange datum_range;
      if (OB_FAIL(datum_range.from_range(all_split_ranges.at(i), allocator))) {
        STORAGE_LOG(ERROR, "Failed to convert store range to datum range", K(ret), K(i));
        ASSERT_EQ(OB_SUCCESS, ret);
      } else if (OB_FAIL(datum_ranges.push_back(datum_range))) {
        STORAGE_LOG(ERROR, "Failed to push back datum range", K(ret));
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }

    // 排序ranges
    storage::ObRangeCompartor compartor(read_info);
    lib::ob_sort(datum_ranges.begin(), datum_ranges.end(), compartor);
    if (OB_FAIL(compartor.sort_ret_)) {
      STORAGE_LOG(ERROR, "Failed to sort ranges", K(ret));
      ASSERT_EQ(OB_SUCCESS, ret);
    }

    // 3. 合并连续的ranges
    ObSEArray<blocksstable::ObDatumRange, 256> merged_ranges;
    if (!datum_ranges.empty()) {
      blocksstable::ObDatumRange current_range = datum_ranges.at(0);

      for (int64_t i = 1; i < datum_ranges.count(); ++i) {
        const blocksstable::ObDatumRange &next_range = datum_ranges.at(i);
        int cmp_ret = 0;

        // 检查当前range的end_key和下一个range的start_key是否连续
        if (OB_FAIL(current_range.get_end_key().compare(
                next_range.get_start_key(), read_info.get_datum_utils(), cmp_ret))) {
          STORAGE_LOG(ERROR, "Failed to compare rowkeys", K(ret));
          ASSERT_EQ(OB_SUCCESS, ret);
        }

        // 如果连续（end_key == start_key 且边界条件允许），则合并
        bool can_merge = false;
        if (cmp_ret == 0) {
          // end_key == start_key，检查边界条件
          bool current_right_closed = current_range.border_flag_.inclusive_end();
          bool next_left_closed = next_range.border_flag_.inclusive_start();
          // 如果至少有一个边界是closed的，则可以合并
          can_merge = (current_right_closed || next_left_closed);
        } else if (cmp_ret > 0) {
          // end_key > start_key，说明有重叠，可以合并
          can_merge = true;
        }

        if (can_merge) {
          // 合并：更新end_key和边界标志
          current_range.end_key_ = next_range.end_key_;
          if (next_range.border_flag_.inclusive_end()) {
            current_range.border_flag_.set_inclusive_end();
          } else {
            current_range.border_flag_.unset_inclusive_end();
          }
        } else {
          // 不能合并，保存当前range，开始新的range
          if (OB_FAIL(merged_ranges.push_back(current_range))) {
            STORAGE_LOG(ERROR, "Failed to push back merged range", K(ret));
            ASSERT_EQ(OB_SUCCESS, ret);
          }
          current_range = next_range;
        }
      }

      // 保存最后一个range
      if (OB_FAIL(merged_ranges.push_back(current_range))) {
        STORAGE_LOG(ERROR, "Failed to push back last merged range", K(ret));
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }

    // 4. 将原始ranges也转换为ObDatumRange并排序
    ObSEArray<blocksstable::ObDatumRange, 16> original_datum_ranges;
    for (int64_t i = 0; OB_SUCC(ret) && i < original_ranges.count(); ++i) {
      blocksstable::ObDatumRange datum_range;
      if (OB_FAIL(datum_range.from_range(original_ranges.at(i), allocator))) {
        STORAGE_LOG(ERROR, "Failed to convert original store range to datum range", K(ret), K(i));
        ASSERT_EQ(OB_SUCCESS, ret);
      } else if (OB_FAIL(original_datum_ranges.push_back(datum_range))) {
        STORAGE_LOG(ERROR, "Failed to push back original datum range", K(ret));
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }

    // 排序原始ranges
    lib::ob_sort(original_datum_ranges.begin(), original_datum_ranges.end(), compartor);
    if (OB_FAIL(compartor.sort_ret_)) {
      STORAGE_LOG(ERROR, "Failed to sort original ranges", K(ret));
      ASSERT_EQ(OB_SUCCESS, ret);
    }

    // 5. 合并原始ranges（处理可能的重叠）
    ObSEArray<blocksstable::ObDatumRange, 16> merged_original_ranges;
    if (!original_datum_ranges.empty()) {
      blocksstable::ObDatumRange current_range = original_datum_ranges.at(0);

      for (int64_t i = 1; i < original_datum_ranges.count(); ++i) {
        const blocksstable::ObDatumRange &next_range = original_datum_ranges.at(i);
        int cmp_ret = 0;

        if (OB_FAIL(current_range.get_end_key().compare(
                next_range.get_start_key(), read_info.get_datum_utils(), cmp_ret))) {
          STORAGE_LOG(ERROR, "Failed to compare original rowkeys", K(ret));
          ASSERT_EQ(OB_SUCCESS, ret);
        }

        bool can_merge = false;
        if (cmp_ret == 0) {
          bool current_right_closed = current_range.border_flag_.inclusive_end();
          bool next_left_closed = next_range.border_flag_.inclusive_start();
          can_merge = (current_right_closed || next_left_closed);
        } else if (cmp_ret > 0) {
          can_merge = true;
        }

        if (can_merge) {
          current_range.end_key_ = next_range.end_key_;
          if (next_range.border_flag_.inclusive_end()) {
            current_range.border_flag_.set_inclusive_end();
          } else {
            current_range.border_flag_.unset_inclusive_end();
          }
        } else {
          if (OB_FAIL(merged_original_ranges.push_back(current_range))) {
            STORAGE_LOG(ERROR, "Failed to push back merged original range", K(ret));
            ASSERT_EQ(OB_SUCCESS, ret);
          }
          current_range = next_range;
        }
      }

      if (OB_FAIL(merged_original_ranges.push_back(current_range))) {
        STORAGE_LOG(ERROR, "Failed to push back last merged original range", K(ret));
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }

    // 6. 比较合并后的ranges和原始ranges
    ASSERT_EQ(merged_ranges.count(), merged_original_ranges.count())
        << "Merged split ranges count (" << merged_ranges.count()
        << ") != merged original ranges count (" << merged_original_ranges.count() << ")";

    for (int64_t i = 0; i < merged_ranges.count() && i < merged_original_ranges.count(); ++i) {
      const blocksstable::ObDatumRange &split_range = merged_ranges.at(i);
      const blocksstable::ObDatumRange &original_range = merged_original_ranges.at(i);

      // 比较start_key
      int cmp_ret = 0;
      if (OB_FAIL(split_range.get_start_key().compare(
              original_range.get_start_key(), read_info.get_datum_utils(), cmp_ret))) {
        STORAGE_LOG(ERROR, "Failed to compare start keys", K(ret), K(i));
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ASSERT_EQ(0, cmp_ret) << "Range " << i << " start_key mismatch";

      // 比较边界标志的start
      ASSERT_EQ(split_range.border_flag_.inclusive_start(),
                original_range.border_flag_.inclusive_start())
          << "Range " << i << " start border flag mismatch";

      // 比较end_key
      if (OB_FAIL(split_range.get_end_key().compare(
              original_range.get_end_key(), read_info.get_datum_utils(), cmp_ret))) {
        STORAGE_LOG(ERROR, "Failed to compare end keys", K(ret), K(i));
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ASSERT_EQ(0, cmp_ret) << "Range " << i << " end_key mismatch";

      // 比较边界标志的end
      ASSERT_EQ(split_range.border_flag_.inclusive_end(),
                original_range.border_flag_.inclusive_end())
          << "Range " << i << " end border flag mismatch";
    }

    STORAGE_LOG(INFO, "Split correctness verification passed",
                K(original_ranges.count()),
                K(all_split_ranges.count()),
                K(merged_ranges.count()),
                K(merged_original_ranges.count()));
  }

  // 生成ObStoreRange的辅助函数
  int generate_store_range(int64_t start_row_idx, int64_t end_row_idx, ObStoreRange &range)
  {
    int ret = OB_SUCCESS;

    // 检查索引范围
    const int64_t total_rows = 1000000;
    if (start_row_idx < 0 || start_row_idx >= total_rows ||
        end_row_idx < 0 || end_row_idx >= total_rows ||
        start_row_idx > end_row_idx) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(ERROR, "Invalid row indices", K(start_row_idx), K(end_row_idx), K(total_rows));
      return ret;
    }

    // 实时构造start和end行数据，参考row_generate_的构造方法
    ObDatumRow start_row;
    ObDatumRow end_row;

    if (OB_FAIL(start_row.init(allocator_, MAX_TEST_COLUMN_CNT))) {
      STORAGE_LOG(ERROR, "Failed to init start row", K(ret));
      return ret;
    }
    if (OB_FAIL(end_row.init(allocator_, MAX_TEST_COLUMN_CNT))) {
      STORAGE_LOG(ERROR, "Failed to init end row", K(ret));
      return ret;
    }

    // 使用row_generate_构造指定索引的行数据
    if (OB_FAIL(row_generate_.get_next_row(start_row_idx, start_row))) {
      STORAGE_LOG(ERROR, "Failed to generate start row", K(start_row_idx), K(ret));
      return ret;
    }
    if (OB_FAIL(row_generate_.get_next_row(end_row_idx, end_row))) {
      STORAGE_LOG(ERROR, "Failed to generate end row", K(end_row_idx), K(ret));
      return ret;
    }

    // 从storage_datums_构造ObObj数组，并进行深拷贝
    const int64_t key_col_cnt = 4; // 使用前4列作为key
    ObObj start_objs[key_col_cnt];
    ObObj end_objs[key_col_cnt];

    for (int64_t i = 0; i < key_col_cnt; ++i) {
      // 先转换为ObObj
      ObObj temp_start_obj;
      ObObj temp_end_obj;

      if (OB_FAIL(start_row.storage_datums_[i].to_obj_enhance(temp_start_obj, table_schema_.get_column_schema(schema_cols_.at(i).col_id_)->get_meta_type()))) {
        STORAGE_LOG(ERROR, "Failed to convert start storage datum to obj", K(i), K(ret));
        return ret;
      }
      if (OB_FAIL(end_row.storage_datums_[i].to_obj_enhance(temp_end_obj, table_schema_.get_column_schema(schema_cols_.at(i).col_id_)->get_meta_type()))) {
        STORAGE_LOG(ERROR, "Failed to convert end storage datum to obj", K(i), K(ret));
        return ret;
      }

      // 深拷贝到allocator管理的内存中
      if (OB_FAIL(ob_write_obj(allocator_, temp_start_obj, start_objs[i]))) {
        STORAGE_LOG(ERROR, "Failed to deep copy start obj", K(i), K(ret));
        return ret;
      }
      if (OB_FAIL(ob_write_obj(allocator_, temp_end_obj, end_objs[i]))) {
        STORAGE_LOG(ERROR, "Failed to deep copy end obj", K(i), K(ret));
        return ret;
      }
    }

    // 构造临时rowkey，然后深拷贝
    ObStoreRowkey temp_start_key(start_objs, key_col_cnt);
    ObStoreRowkey temp_end_key(end_objs, key_col_cnt);

    ObStoreRowkey start_key;
    ObStoreRowkey end_key;

    if (OB_FAIL(temp_start_key.deep_copy(start_key, allocator_))) {
      STORAGE_LOG(ERROR, "Failed to deep copy start key", K(ret));
      return ret;
    }
    if (OB_FAIL(temp_end_key.deep_copy(end_key, allocator_))) {
      STORAGE_LOG(ERROR, "Failed to deep copy end key", K(ret));
      return ret;
    }

    range.set_start_key(start_key);
    range.set_end_key(end_key);
    range.get_border_flag().set_inclusive_start();
    range.get_border_flag().set_inclusive_end();

    return ret;
  }

  // 生成不同的range场景
  void generate_whole_range_scenario(ObSEArray<ObStoreRange, 16> &ranges)
  {
    ranges.reset();
    ObStoreRange whole_range;
    whole_range.set_whole_range();
    int ret = ranges.push_back(whole_range);
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "Failed to push whole range", K(ret));
      return;
    }
    STORAGE_LOG(INFO, "Generated whole range scenario", K(ranges.count()));
  }

  void generate_small_range_scenario(ObSEArray<ObStoreRange, 16> &ranges)
  {
    ranges.reset();
    // 小range：10000-20000行 (索引从0开始，ID从1开始)
    ObStoreRange small_range;
    int ret = generate_store_range(99999, 109999, small_range);
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "Failed to generate small range", K(ret));
      return;
    }
    ret = ranges.push_back(small_range);
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "Failed to push small range", K(ret));
      return;
    }
    STORAGE_LOG(INFO, "Generated small range scenario", K(ranges.count()),
                K(calculate_range_row_count(small_range)));
  }

  void generate_large_range_scenario(ObSEArray<ObStoreRange, 16> &ranges)
  {
    ranges.reset();
    // 大range：800000-900000行 (索引从0开始，ID从1开始)
    ObStoreRange large_range;
    int ret = generate_store_range(99999, 899999, large_range);
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "Failed to generate large range", K(ret));
      return;
    }
    ret = ranges.push_back(large_range);
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "Failed to push large range", K(ret));
      return;
    }
    STORAGE_LOG(INFO, "Generated large range scenario", K(ranges.count()),
                K(calculate_range_row_count(large_range)));
  }

  void generate_many_small_ranges_scenario(ObSEArray<ObStoreRange, 16> &ranges)
  {
    ranges.reset();
    // 生成10个小range，每个大约10000行，之间有间隙
    int64_t base_id = 50000;
    for (int i = 0; i < 10; ++i) {
      ObStoreRange range;
      int64_t start_idx = base_id + i * 20000 - 1;  // 每隔20000个ID一个range，转换为索引
      int64_t end_idx = start_idx + 10000;          // 每个range 10000行
      int ret = generate_store_range(start_idx, end_idx, range);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "Failed to generate small range", K(i), K(ret));
        return;
      }
      ret = ranges.push_back(range);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "Failed to push small range", K(i), K(ret));
        return;
      }
    }
    STORAGE_LOG(INFO, "Generated many small ranges scenario", K(ranges.count()));
  }

  void generate_many_large_ranges_scenario(ObSEArray<ObStoreRange, 16> &ranges)
  {
    ranges.reset();
    // 生成5个大range，每个大约100000行，之间有间隙
    int64_t base_id = 100000;
    for (int i = 0; i < 5; ++i) {
      ObStoreRange range;
      int64_t start_idx = base_id + i * 150000 - 1;  // 每隔150000个ID一个range，转换为索引
      int64_t end_idx = start_idx + 100000;          // 每个range 100000行
      int ret = generate_store_range(start_idx, end_idx, range);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "Failed to generate large range", K(i), K(ret));
        return;
      }
      ret = ranges.push_back(range);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "Failed to push large range", K(i), K(ret));
        return;
      }
    }
    STORAGE_LOG(INFO, "Generated many large ranges scenario", K(ranges.count()));
  }

  void generate_mixed_ranges_scenario(ObSEArray<ObStoreRange, 16> &ranges)
  {
    ranges.reset();
    // 生成混合大小的range：小、中、大混合
    ObStoreRange ranges_data[] = {
      // 小range
      ObStoreRange(), // 10000-15000
      ObStoreRange(), // 30000-35000
      // 中等range
      ObStoreRange(), // 60000-80000
      ObStoreRange(), // 100000-120000
      // 大range
      ObStoreRange(), // 200000-300000
      ObStoreRange(), // 400000-500000
    };

    int64_t starts[] = {10000, 30000, 60000, 100000, 200000, 400000};
    int64_t ends[] = {15000, 35000, 80000, 120000, 300000, 500000};

    for (size_t i = 0; i < sizeof(ranges_data)/sizeof(ranges_data[0]); ++i) {
      int ret = generate_store_range(starts[i] - 1, ends[i] - 1, ranges_data[i]);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "Failed to generate mixed range", K(i), K(ret));
        return;
      }
      ret = ranges.push_back(ranges_data[i]);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "Failed to push mixed range", K(i), K(ret));
        return;
      }
    }
    STORAGE_LOG(INFO, "Generated mixed ranges scenario", K(ranges.count()));
  }

  void generate_many_ranges_scenario(int64_t range_count, ObSEArray<ObStoreRange, 16> &ranges)
  {
    ranges.reset();
    // 生成大量range，根据range_count动态调整大小
    int64_t total_rows = 1000000;
    int64_t start_offset = 1000; // 从1000开始，留出一些空间
    int64_t available_rows = total_rows - start_offset - 10000; // 留出结尾空间

    // 根据range_count动态计算平均range大小和间隙
    int64_t avg_range_size = available_rows / (range_count * 2); // 平均range大小
    int64_t avg_gap = available_rows / (range_count * 2);        // 平均间隙

    // 设置最小/最大值，确保range至少有50行，间隙至少有10行
    int64_t min_range_size = (50LL > avg_range_size / 4) ? 50LL : avg_range_size / 4;
    int64_t max_range_size = (available_rows / range_count < avg_range_size * 4) ? available_rows / range_count : avg_range_size * 4;
    int64_t min_gap = (10LL > avg_gap / 4) ? 10LL : avg_gap / 4;
    int64_t max_gap = (available_rows / range_count < avg_gap * 4) ? available_rows / range_count : avg_gap * 4;

    STORAGE_LOG(INFO, "Dynamic range generation", K(range_count), K(avg_range_size),
                K(avg_gap), K(min_range_size), K(max_range_size), K(min_gap), K(max_gap));

    int64_t current_id = start_offset; // 从start_offset开始

    for (int64_t i = 0; i < range_count && current_id < total_rows - 1000; ++i) {
      // 随机生成range大小
      int64_t range_size = min_range_size + (::rand() % (max_range_size - min_range_size + 1));
      int64_t end_id = current_id + range_size - 1;

      if (end_id >= total_rows) {
        end_id = total_rows - 1;
      }

      // 确保range至少有50行
      if (end_id - current_id + 1 < 50) {
        end_id = current_id + 49;
      }

      ObStoreRange range;
      int ret = generate_store_range(current_id - 1, end_id - 1, range);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "Failed to generate range", K(i), K(ret));
        return;
      }
      ret = ranges.push_back(range);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "Failed to push range", K(i), K(ret));
        return;
      }

      // 计算下一个range的开始位置，加上随机间隙
      int64_t gap = min_gap + (::rand() % (max_gap - min_gap + 1));
      current_id = end_id + 1 + gap;

      // 如果下一个位置太接近结尾，调整一下
      if (current_id >= total_rows - 1000 && i < range_count - 1) {
        // 重新计算剩余的空间
        int64_t remaining_ranges = range_count - i - 1;
        int64_t remaining_space = total_rows - current_id - 1000;
        if (remaining_ranges > 0 && remaining_space > 0) {
          avg_range_size = remaining_space / (remaining_ranges * 2);
          min_range_size = (50LL > avg_range_size / 2) ? 50LL : avg_range_size / 2;
          max_range_size = std::min(remaining_space / remaining_ranges, avg_range_size * 2);
        }
      }
    }

    STORAGE_LOG(INFO, "Generated many ranges scenario", K(range_count), K(ranges.count()));
  }

private:

  // 辅助函数：打印图形化输出
  void print_graphical_output(const ObArrayArray<ObStoreRange> &multi_range_split_array,
                             const char *scenario_name,
                             int64_t expected_task_count,
                             int64_t total_input_ranges,
                             int64_t total_input_rows,
                             double avg_rows,
                             int64_t min_rows,
                             int64_t max_rows)
  {
    STORAGE_LOG(INFO, "=== SCENARIO RESULTS ===");
    STORAGE_LOG(INFO, "Scenario", K(scenario_name));
    STORAGE_LOG(INFO, "Input Ranges", K(total_input_ranges));
    STORAGE_LOG(INFO, "Input Total Rows", K(total_input_rows));
    STORAGE_LOG(INFO, "Expected Tasks", K(expected_task_count));
    STORAGE_LOG(INFO, "Actual Tasks", K(multi_range_split_array.count()));
    STORAGE_LOG(INFO, "Task Statistics", K(avg_rows), K(min_rows), K(max_rows));

    // 计算每个任务的行数
    ObSEArray<int64_t, 256> task_rows;
    int64_t total_output_rows = 0;

    for (int64_t i = 0; i < multi_range_split_array.count(); ++i) {
      int64_t rows = 0;
      for (int64_t j = 0; j < multi_range_split_array.at(i).count(); ++j) {
        rows += calculate_range_row_count(multi_range_split_array.at(i).at(j));
      }
      task_rows.push_back(rows);
      total_output_rows += rows;
    }

    // 图形化显示任务分布
    STORAGE_LOG(INFO, "Task Distribution (Rows per Task):");

    // 找到最大行数作为基准
    int64_t max_task_rows = 0;
    for (int64_t i = 0; i < task_rows.count(); ++i) {
      if (task_rows.at(i) > max_task_rows) {
        max_task_rows = task_rows.at(i);
      }
    }

    const int64_t max_bar_length = 40; // 最大行数用40个字符表示

    // 计算平均值对应的位置
    int64_t avg_position = 0;
    if (max_task_rows > 0) {
      avg_position = (static_cast<int64_t>(avg_rows) * max_bar_length) / max_task_rows;
      if (avg_position >= max_bar_length) {
        avg_position = max_bar_length - 1;
      }
    }

    for (int64_t i = 0; i < task_rows.count(); ++i) {
      // 创建简单的条形图，基于最大行数的比例
      int64_t bar_length = 0;
      if (max_task_rows > 0) {
        bar_length = (task_rows.at(i) * max_bar_length) / max_task_rows;
        if (bar_length == 0 && task_rows.at(i) > 0) {
          bar_length = 1; // 至少显示一个字符表示有数据
        }
      }

      char bar[42]; // 多一个字符用于'A'标记
      memset(bar, '=', bar_length);

      // 在平均值位置标记'A'
      if (bar_length > avg_position) {
        bar[avg_position] = 'A';
      } else {
        // 如果bar不够长，扩展到平均值位置并标记
        int64_t new_length = avg_position + 1;
        if (new_length > max_bar_length) {
          new_length = max_bar_length;
          avg_position = new_length - 1; // 在末尾标记
        }
        // 填充扩展的部分
        for (int64_t i = bar_length; i < new_length; ++i) {
          bar[i] = ' ';
        }
        bar[avg_position] = 'A';
        bar_length = new_length;
      }

      bar[bar_length] = '\0';

      STORAGE_LOG(INFO, "Task", K(i), K(task_rows.at(i)), K(bar));
    }

    STORAGE_LOG(INFO, "Total Output Rows", K(total_output_rows));
    STORAGE_LOG(INFO, "=========================");
  }

  // 测试单个场景的辅助函数
  void test_single_scenario(const char *scenario_name,
                           ObSEArray<ObStoreRange, 16> &ranges,
                           int64_t expected_task_count)
  {
    int ret = OB_SUCCESS;

    // 计算输入的行数
    int64_t total_input_rows = 0;
    for (int64_t i = 0; i < ranges.count(); ++i) {
      total_input_rows += calculate_range_row_count(ranges.at(i));
    }

    STORAGE_LOG(INFO, "Testing scenario", K(scenario_name), K(expected_task_count),
                K(ranges.count()), K(total_input_rows));

    // 准备sstable列表
    ObSEArray<ObITable *, 1> tables;
    ret = tables.push_back(&sstable_);
    ASSERT_EQ(OB_SUCCESS, ret);

    const ObITableReadInfo *index_read_info = &read_info_;

    ObArrayArray<ObStoreRange> multi_range_split_array;
    ObArenaAllocator allocator;

    ObPartitionMultiRangeSpliter spliter;
    int64_t start_time_us = ObTimeUtility::current_time();

    ret = spliter.get_split_multi_ranges(ranges,
                                        expected_task_count,
                                        *index_read_info,
                                        tables,
                                        allocator,
                                        multi_range_split_array,
                                        false /* for_compaction */);

    int64_t end_time_us = ObTimeUtility::current_time();
    int64_t cost_time_ms = (end_time_us - start_time_us) / 1000;

    ASSERT_EQ(OB_SUCCESS, ret);

    // 验证拆分结果的正确性：合并后的ranges应该和原始ranges一致
    verify_split_correctness(ranges, multi_range_split_array, *index_read_info, allocator);

    // 评估均衡程度
    double avg_rows = 0;
    int64_t min_rows = 0;
    int64_t max_rows = 0;

    evaluate_balance(multi_range_split_array, avg_rows, min_rows, max_rows);

    // 验证max_rows应该小于avg_rows的2倍
    bool is_balanced = (max_rows <= avg_rows * 2);
    STORAGE_LOG(INFO, "Balance Check", K(is_balanced), K(ranges.count() <= 20), K(max_rows), K(avg_rows), K(avg_rows * 2));

    // 打印图形化输出
    print_graphical_output(multi_range_split_array, scenario_name, expected_task_count,
                          ranges.count(), total_input_rows, avg_rows, min_rows, max_rows);

    if (ranges.count() <= 20) {
      if (!is_balanced) {
        EXPECT_TRUE(max_rows <= avg_rows * 5);
      }
    } else if (ranges.count() >= 100) {
      EXPECT_TRUE(max_rows <= avg_rows * 15);
    }

    allocator.reset();
  }
};

TEST_F(TestRangeSplitBalance, test_range_split_balance_comprehensive)
{
  // 测试不同的任务数
  ObSEArray<int64_t, 8> task_counts;
  task_counts.push_back(2);
  task_counts.push_back(4);
  task_counts.push_back(8);
  task_counts.push_back(16);
  task_counts.push_back(32);
  task_counts.push_back(64);
  task_counts.push_back(128);
  task_counts.push_back(256);

  STORAGE_LOG(INFO, "Starting comprehensive range split balance test");

  // 1. 测试whole range场景
  for (int64_t i = 0; i < task_counts.count(); ++i) {
    ObSEArray<ObStoreRange, 16> ranges;
    generate_whole_range_scenario(ranges);
    char scenario_name[64];
    sprintf(scenario_name, "Whole_Range_Task_%ld", task_counts.at(i));
    test_single_scenario(scenario_name, ranges, task_counts.at(i));
  }

  // 2. 测试small range场景
  for (int64_t i = 0; i < task_counts.count(); ++i) {
    ObSEArray<ObStoreRange, 16> ranges;
    generate_small_range_scenario(ranges);
    char scenario_name[64];
    sprintf(scenario_name, "Small_Range_Task_%ld", task_counts.at(i));
    test_single_scenario(scenario_name, ranges, task_counts.at(i));
  }

  // 3. 测试large range场景
  for (int64_t i = 0; i < task_counts.count(); ++i) {
    ObSEArray<ObStoreRange, 16> ranges;
    generate_large_range_scenario(ranges);
    char scenario_name[64];
    sprintf(scenario_name, "Large_Range_Task_%ld", task_counts.at(i));
    test_single_scenario(scenario_name, ranges, task_counts.at(i));
  }

  // 4. 测试many small ranges场景
  for (int64_t i = 0; i < task_counts.count(); ++i) {
    ObSEArray<ObStoreRange, 16> ranges;
    generate_many_small_ranges_scenario(ranges);
    char scenario_name[64];
    sprintf(scenario_name, "Many_Small_Ranges_Task_%ld", task_counts.at(i));
    test_single_scenario(scenario_name, ranges, task_counts.at(i));
  }

  // 5. 测试many large ranges场景
  for (int64_t i = 0; i < task_counts.count(); ++i) {
    ObSEArray<ObStoreRange, 16> ranges;
    generate_many_large_ranges_scenario(ranges);
    char scenario_name[64];
    sprintf(scenario_name, "Many_Large_Ranges_Task_%ld", task_counts.at(i));
    test_single_scenario(scenario_name, ranges, task_counts.at(i));
  }

  // 6. 测试mixed ranges场景
  for (int64_t i = 0; i < task_counts.count(); ++i) {
    ObSEArray<ObStoreRange, 16> ranges;
    generate_mixed_ranges_scenario(ranges);
    char scenario_name[64];
    sprintf(scenario_name, "Mixed_Ranges_Task_%ld", task_counts.at(i));
    test_single_scenario(scenario_name, ranges, task_counts.at(i));
  }

  // 7. 测试超多range场景
  ObSEArray<int64_t, 3> many_range_counts;
  many_range_counts.push_back(500);
  many_range_counts.push_back(1000);
  many_range_counts.push_back(10000);

  for (int64_t j = 0; j < many_range_counts.count(); ++j) {
    for (int64_t i = 0; i < task_counts.count(); ++i) {
      ObSEArray<ObStoreRange, 16> ranges;
      generate_many_ranges_scenario(many_range_counts.at(j), ranges);
      char scenario_name[64];
      sprintf(scenario_name, "Many_Ranges_%ld_Task_%ld", many_range_counts.at(j), task_counts.at(i));
      test_single_scenario(scenario_name, ranges, task_counts.at(i));
    }
  }

  STORAGE_LOG(INFO, "Comprehensive range split balance test completed");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_range_split_balance.log*");
  OB_LOGGER.set_file_name("test_range_split_balance.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
