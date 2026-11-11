/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <vector>
#include "lib/allocator/page_arena.h"
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_tree_serializer_impl.ipp"
#include "lib/utility/serialization.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

static int run_case(const uint64_t expr_cnt, const uint64_t frame_size,
                    const int64_t expr_mem_size, const bool no_deser_data = false,
                    const int64_t payload_datum_index = -1, const int64_t payload_size = 0)
{
  int ret = OB_SUCCESS;
  ObFrameInfo frame(expr_cnt, 0, frame_size, 0, 0, false);
  ObSEArray<ObFrameInfo, 1> infos;
  if (OB_FAIL(infos.push_back(frame))) {
    return ret;
  }
  std::vector<char> wire(static_cast<size_t>(expr_mem_size + expr_cnt * 16
      + expr_cnt * (payload_size > 0 ? payload_size : 0) + 64), 0);
  int64_t write_pos = 0;
  if (OB_FAIL(serialization::encode(wire.data(), wire.size(), write_pos, expr_mem_size))) {
    return OB_ERR_UNEXPECTED;
  }
  for (int64_t i = 0; i < expr_mem_size; ++i) {
    wire[write_pos++] = static_cast<char>('A' + i);
  }
  const int64_t extra_mem_size = 0;
  if (OB_FAIL(serialization::encode(wire.data(), wire.size(), write_pos, extra_mem_size))) {
    return OB_ERR_UNEXPECTED;
  }
  for (uint64_t i = 0; i < expr_cnt; ++i) {
    const int64_t expr_datum_size = (static_cast<int64_t>(i) >= payload_datum_index)
        ? payload_size : 0;
    if (OB_FAIL(serialization::encode(wire.data(), wire.size(), write_pos, expr_datum_size))) {
      return OB_ERR_UNEXPECTED;
    }
    for (int64_t j = 0; j < expr_datum_size; ++j) {
      wire[write_pos++] = 'D';
    }
  }

  char *frame_storage[1] = {nullptr};
  char **frames = frame_storage;
  int64_t frame_cnt = 1;
  int64_t read_pos = 0;
  ObArenaAllocator allocator;
  ret = ObPxTreeSerializer::deserialize_frame_info<false>(
      wire.data(), write_pos, read_pos, allocator, infos, nullptr, frames, frame_cnt, no_deser_data);
  return ret;
}

class PxFrameInfoOverflowTest : public ::testing::Test {};

TEST_F(PxFrameInfoOverflowTest, reject_expr_mem_size_larger_than_frame)
{
  const uint64_t frame_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  const int ret = run_case(1, frame_size, static_cast<int64_t>(frame_size) + 1);
  EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
}

TEST_F(PxFrameInfoOverflowTest, accept_expr_mem_size_within_frame)
{
  const uint64_t frame_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  const int ret = run_case(1, frame_size, static_cast<int64_t>(frame_size));
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(PxFrameInfoOverflowTest, reject_expr_count_larger_than_frame_capacity)
{
  const uint64_t frame_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  const int ret = run_case(2, frame_size, static_cast<int64_t>(frame_size));
  EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
}

TEST_F(PxFrameInfoOverflowTest, reject_datum_payload_when_no_deser_data)
{
  const uint64_t frame_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  EXPECT_EQ(OB_SIZE_OVERFLOW, run_case(2, frame_size, 0, true, 1, 1));
}

template <bool SERIALIZE_PLAN_PART>
static void check_trimmed_datum_frame_round_trip(const bool use_rich_format)
{
  const int64_t item_size = sizeof(ObDatum) + sizeof(ObEvalInfo)
      + (use_rich_format ? sizeof(VectorHeader) : 0);
  // Partial plans retain expr_cnt_ when an unused datum frame is trimmed to one item.
  ObFrameInfo frame(2, 0, item_size, 0, 0, use_rich_format);
  ObSEArray<ObFrameInfo, 1> infos;
  ASSERT_EQ(OB_SUCCESS, infos.push_back(frame));
  char source_frame[2 * (sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader))] = {};
  char *source_frames[1] = {source_frame};
  char wire[256] = {};
  int64_t write_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::serialize_frame_info<SERIALIZE_PLAN_PART>(
      wire, sizeof(wire), write_pos, infos, source_frames, 1, true));

  ObSEArray<ObFrameInfo, 1> decoded_infos;
  if (!SERIALIZE_PLAN_PART) {
    ASSERT_EQ(OB_SUCCESS, decoded_infos.assign(infos));
  }
  char *frame_storage[1] = {nullptr};
  char **frames = frame_storage;
  int64_t frame_cnt = 1;
  int64_t read_pos = 0;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::deserialize_frame_info<SERIALIZE_PLAN_PART>(
      wire, write_pos, read_pos, allocator, decoded_infos, nullptr, frames, frame_cnt, true));
  EXPECT_EQ(write_pos, read_pos);
  EXPECT_NE(nullptr, frames[0]);
}

TEST_F(PxFrameInfoOverflowTest, accept_trimmed_datum_frame)
{
  check_trimmed_datum_frame_round_trip<false>(false);
}

TEST_F(PxFrameInfoOverflowTest, accept_trimmed_rich_datum_frame)
{
  check_trimmed_datum_frame_round_trip<false>(true);
}

TEST_F(PxFrameInfoOverflowTest, accept_trimmed_datum_frame_with_plan)
{
  check_trimmed_datum_frame_round_trip<true>(false);
}

TEST_F(PxFrameInfoOverflowTest, accept_trimmed_rich_datum_frame_with_plan)
{
  check_trimmed_datum_frame_round_trip<true>(true);
}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
