/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include <gtest/gtest.h>
#include <vector>

#include "lib/allocator/page_arena.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_util.h"
#define private public
#include "share/ob_partition_split_query.h"
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

TEST(SplitFilterSerializeTest, max_sentinel_does_not_read_payload)
{
  ObArenaAllocator allocator;
  ObExecContext exec_ctx(allocator);
  const int64_t expr_cnt = 3;
  const int64_t item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  const int64_t result_buf_size = sizeof(int64_t);
  const int64_t frame_size = expr_cnt * (item_size + result_buf_size);
  char *frame = static_cast<char *>(allocator.alloc(frame_size));
  ASSERT_NE(nullptr, frame);
  MEMSET(frame, 0, frame_size);
  char *frames[] = {frame};
  exec_ctx.set_frames(frames);
  exec_ctx.set_frame_cnt(1);
  ObEvalCtx eval_ctx(exec_ctx);
  ExprFixedArray filter_params(allocator);
  ASSERT_EQ(OB_SUCCESS, filter_params.init(expr_cnt));
  ObExpr exprs[expr_cnt];
  for (int64_t i = 0; i < expr_cnt; ++i) {
    new (frame + i * item_size) ObDatum();
    exprs[i].frame_idx_ = 0;
    exprs[i].datum_off_ = i * item_size;
    exprs[i].eval_info_off_ = i * item_size + sizeof(ObDatum);
    exprs[i].res_buf_off_ = expr_cnt * item_size + i * result_buf_size;
    exprs[i].res_buf_len_ = result_buf_size;
    ASSERT_EQ(OB_SUCCESS, filter_params.push_back(&exprs[i]));
  }

  blocksstable::ObStorageDatum lower_datum;
  lower_datum.set_int(10);
  storage::ObTabletSplitTscInfo split_info;
  ASSERT_EQ(OB_SUCCESS, split_info.start_partkey_.assign(&lower_datum, 1));
  split_info.end_partkey_.set_max_rowkey();
  share::ObPartitionSplitQuery split_query;
  ASSERT_EQ(OB_SUCCESS, split_query.fill_range_filter_param(split_info, eval_ctx, &filter_params));

  ObDatum &upper_datum = *reinterpret_cast<ObDatum *>(frame + 2 * item_size);
  ASSERT_TRUE(upper_datum.is_ext());
  char *payload = static_cast<char *>(allocator.alloc(1));
  ASSERT_NE(nullptr, payload);
  upper_datum.ptr_ = payload;
  EXPECT_EQ(0, upper_datum.len_);

  ObSEArray<ObFrameInfo, 1> frame_infos;
  ObFrameInfo frame_info;
  frame_info.frame_idx_ = 0;
  frame_info.expr_cnt_ = expr_cnt;
  frame_info.frame_size_ = frame_size;
  frame_info.use_rich_format_ = false;
  ASSERT_EQ(OB_SUCCESS, frame_infos.push_back(frame_info));
  const int64_t output_size = ObPxTreeSerializer::get_serialize_frame_info_size<false>(
    frame_infos, frames, 1, false);
  std::vector<char> output(output_size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::serialize_frame_info<false>(
    output.data(), output.size(), pos, frame_infos, frames, 1, false));
  ASSERT_EQ(output_size, pos);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
