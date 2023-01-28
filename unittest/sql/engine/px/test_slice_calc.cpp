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

#define USING_LOG_PREFIX SQL_EXE

#include "gtest/gtest.h"
#include "sql/engine/ob_exec_context.h"
#define private public
#define protected public
#include "sql/executor/ob_slice_calc.h"
#undef private
#undef protected
#include "lib/ob_define.h"
#include "common/row/ob_row.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

class TestPkeyRangeSliceCalc : public ::testing::Test
{
public:
  TestPkeyRangeSliceCalc() : ctx_(allocator_) {}
  virtual ~TestPkeyRangeSliceCalc() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  int build_channel_info(const int64_t ch_count, ObPxPartChInfo &ch_info);
public:
  static const int64_t PARTITION_COUNT = 3;
  static const int64_t RANGE_COUNT = 5;
  ObArenaAllocator allocator_;
  ObExecContext ctx_;
  ObArray<ObExpr *> sort_exprs_;
  ObExpr *fake_calc_part_id_expr_;
  ObArray<ObSortFieldCollation> sort_collations_;
  ObArray<ObSortCmpFunc> sort_cmp_funcs_;
  ObPxTabletRange::RangeCut range_cut_;
  int64_t tmp_int_;
  ObDatum int_datum_;
};

void TestPkeyRangeSliceCalc::SetUp()
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, sort_exprs_.push_back(nullptr));
  ASSERT_EQ(OB_SUCCESS, sort_collations_.push_back(ObSortFieldCollation(
          0/*field_idx*/,
          ObCollationType::CS_TYPE_BINARY,
          true/*is_ascending*/,
          ObCmpNullPos::NULL_LAST)));
  ObSortCmpFunc cmp_func;
  cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
      ObObjType::ObIntType,
      ObObjType::ObIntType,
      ObCmpNullPos::NULL_LAST,
      ObCollationType::CS_TYPE_BINARY,
      SCALE_UNKNOWN_YET,
      false/*is_orace_mode*/,
      false);
  ASSERT_EQ(OB_SUCCESS, sort_cmp_funcs_.push_back(cmp_func));
  int_datum_.int_ = &tmp_int_;
  fake_calc_part_id_expr_ = reinterpret_cast<ObExpr *>(&tmp_int_);

  ObDatum tmp_datum;
  for (int64_t j = 1; OB_SUCC(ret) && j < RANGE_COUNT; ++j) {
    tmp_datum.int_ = (int64_t *)allocator_.alloc(sizeof(int64_t));
    ObPxTabletRange::DatumKey tmp_key;
    ASSERT_EQ(OB_SUCCESS, tmp_key.push_back(tmp_datum));
    tmp_key.at(0).set_int(j * 10);
    ASSERT_EQ(OB_SUCCESS, range_cut_.push_back(tmp_key));
  }

  ObArray<ObPxTabletRange> part_ranges;
  for (int64_t i = 0; OB_SUCC(ret) && i < PARTITION_COUNT; ++i) {
    ObPxTabletRange tmp_part_range;
    tmp_part_range.partition_id_ = i;
    ret = tmp_part_range.range_cut_.assign(range_cut_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = part_ranges.push_back(tmp_part_range);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = ctx_.set_partition_ranges(part_ranges);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("init ctx partition ranges", K(ret), K(ctx_.get_partition_ranges()));
}

void TestPkeyRangeSliceCalc::TearDown()
{

}

int TestPkeyRangeSliceCalc::build_channel_info(const int64_t ch_count, ObPxPartChInfo &ch_info)
{
  int ret = OB_SUCCESS;
  ch_info.part_ch_array_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < PARTITION_COUNT; ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < ch_count; ++j) {
      ObPxPartChMapItem item;
      item.first_ = i;
      item.second_ = ch_count * i + j;
      if (OB_FAIL(ch_info.part_ch_array_.push_back(item))) {
        LOG_WARN("push back channel item failed", K(ret), K(item));
      }
    }
  }
  return ret;
}

TEST_F(TestPkeyRangeSliceCalc, build_part_range_map_one_ch)
{
  int ret = OB_SUCCESS;
  ObPxPartChInfo ch_info;
  const int64_t ch_count = 1;
  ret = build_channel_info(ch_count, ch_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  schema::ObTableSchema fake_table_schema;
  ObSlaveMapPkeyRangeIdxCalc range_slice_calc(
      ctx_, fake_table_schema, fake_calc_part_id_expr_, ObPQDistributeMethod::PARTITION_RANGE, ch_info,
      sort_exprs_, &sort_cmp_funcs_, &sort_collations_, OB_REPARTITION_NO_REPARTITION);
  ret = range_slice_calc.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t task_idx = -1;
  ObPxTabletRange::DatumKey tmp_key;
  ASSERT_EQ(OB_SUCCESS, tmp_key.push_back(int_datum_));
  for (int64_t j = 0; OB_SUCC(ret) && j < PARTITION_COUNT; ++j) {
    const int64_t partition_id = j;
    for (int64_t i = 0; OB_SUCC(ret) && i < 1000; ++i) {
      tmp_key.at(0).set_int(i);
      ret = range_slice_calc.get_task_idx(partition_id, tmp_key, task_idx);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(task_idx, 0 + partition_id * ch_count);
    }
  }
}

TEST_F(TestPkeyRangeSliceCalc, build_part_range_map_less_ch)
{
  int ret = OB_SUCCESS;
  ObPxPartChInfo ch_info;
  const int64_t ch_count = 3;
  ret = build_channel_info(ch_count, ch_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  schema::ObTableSchema fake_table_schema;
  ObSlaveMapPkeyRangeIdxCalc range_slice_calc(
      ctx_, fake_table_schema, fake_calc_part_id_expr_, ObPQDistributeMethod::PARTITION_RANGE, ch_info,
      sort_exprs_, &sort_cmp_funcs_, &sort_collations_, OB_REPARTITION_NO_REPARTITION);
  ret = range_slice_calc.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t task_idx = -1;
  ObPxTabletRange::DatumKey tmp_key;
  ASSERT_EQ(OB_SUCCESS, tmp_key.push_back(int_datum_));
  for (int64_t j = 0; OB_SUCC(ret) && j < PARTITION_COUNT; ++j) {
    const int64_t partition_id = j;
    for (int64_t i = 0; OB_SUCC(ret) && i < 1000; ++i) {
      tmp_key.at(0).set_int(i);
      ret = range_slice_calc.get_task_idx(partition_id, tmp_key, task_idx);
      ASSERT_EQ(OB_SUCCESS, ret);
      const int64_t range_idx = std::lower_bound(range_cut_.begin(), range_cut_.end(), tmp_key, range_slice_calc.sort_cmp_) - range_cut_.begin();
      if (range_idx < 2) {
        ASSERT_EQ(task_idx, 0 + partition_id * ch_count) << "i: " << i << ", range_idx: " << range_idx << std::endl;
      } else if (range_idx < 4) {
        ASSERT_EQ(task_idx, 1 + partition_id * ch_count) << "i: " << i << ", range_idx: " << range_idx << std::endl;
      } else {
        ASSERT_EQ(task_idx, 2 + partition_id * ch_count) << "i: " << i << ", range_idx: " << range_idx << std::endl;
      }
    }
  }
}

TEST_F(TestPkeyRangeSliceCalc, build_part_range_map_more_ch)
{
  int ret = OB_SUCCESS;
  ObPxPartChInfo ch_info;
  const int64_t ch_count = 30;
  ret = build_channel_info(ch_count, ch_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  schema::ObTableSchema fake_table_schema;
  ObSlaveMapPkeyRangeIdxCalc range_slice_calc(
      ctx_, fake_table_schema, fake_calc_part_id_expr_, ObPQDistributeMethod::PARTITION_RANGE, ch_info,
      sort_exprs_, &sort_cmp_funcs_, &sort_collations_, OB_REPARTITION_NO_REPARTITION);
  ret = range_slice_calc.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t task_idx = -1;
  ObPxTabletRange::DatumKey tmp_key;
  ASSERT_EQ(OB_SUCCESS, tmp_key.push_back(int_datum_));
  for (int64_t j = 0; OB_SUCC(ret) && j < PARTITION_COUNT; ++j) {
    const int64_t partition_id = j;
    for (int64_t i = 0; OB_SUCC(ret) && i < 1000; ++i) {
      tmp_key.at(0).set_int(i);
      ret = range_slice_calc.get_task_idx(partition_id, tmp_key, task_idx);
      ASSERT_EQ(OB_SUCCESS, ret);
      const int64_t range_idx = std::lower_bound(range_cut_.begin(), range_cut_.end(), tmp_key, range_slice_calc.sort_cmp_) - range_cut_.begin();
      ASSERT_EQ(task_idx, range_idx + partition_id * ch_count) << "i: " << i << ", range_idx: " << range_idx << std::endl;
    }
  }
}

int main(int argc, char **argv)
{
  system("rm -f test_slice_calc.log*");
  OB_LOGGER.set_file_name("test_slice_calc.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
