/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#include <vector>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "sql/das/iter/ob_das_vec_index_hnsw_scan_iter.h"
#undef protected
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

namespace
{

class TestVecIndexPreBrute : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ObTenantEnv::get_tenant_local()->id_ = 500;
  }

  int prepare_bitmap(ObVecIndexBitmap::FilterType type, ObVecIndexBitmap &bitmap)
  {
    int ret = OB_SUCCESS;
    const int64_t vids[] = {11, 15, 30, 31, 49};
    if (OB_FAIL(bitmap.init(10, 50, ARRAYSIZEOF(vids)))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(vids); ++i) {
        ret = bitmap.add_vid(vids[i]);
      }
    }
    if (OB_SUCC(ret) && type == ObVecIndexBitmap::BYTE_ARRAY) {
      ret = bitmap.upgrade_to_byte_array();
    } else if (OB_SUCC(ret) && type == ObVecIndexBitmap::ROARING_BITMAP) {
      // Do not construct a bitmap wider than NORMAL_BITMAP_MAX_SIZE just to trigger
      // the production upgrade condition. The explicit upgrade exercises the same
      // representation with only a few VIDs.
      ret = bitmap.upgrade_to_roaring_bitmap();
    }
    return ret;
  }

  void check_bitmap_batches(ObVecIndexBitmap::FilterType type)
  {
    SCOPED_TRACE(::testing::Message() << "bitmap type=" << type);
    ObArenaAllocator allocator("VecBitmapUT");
    ObVecIndexBitmap bitmap(&allocator);
    ASSERT_EQ(OB_SUCCESS, prepare_bitmap(type, bitmap));
    ASSERT_EQ(type, bitmap.type_);

    const int64_t expected[] = {11, 15, 30, 31, 49};
    std::vector<int64_t> actual;
    ObVecIndexBitmapIter bitmap_iter;
    ASSERT_EQ(OB_SUCCESS, bitmap_iter.init(&bitmap));
    while (!bitmap_iter.is_end()) {
      int64_t batch[2];
      int64_t batch_cnt = 0;
      ASSERT_EQ(OB_SUCCESS, bitmap_iter.get_vid_batch(batch, ARRAYSIZEOF(batch), batch_cnt));
      ASSERT_GT(batch_cnt, 0);
      for (int64_t i = 0; i < batch_cnt; ++i) {
        ASSERT_LT(actual.size(), ARRAYSIZEOF(expected));
        EXPECT_EQ(expected[actual.size()], batch[i]);
        actual.push_back(batch[i]);
      }
    }

    ASSERT_EQ(ARRAYSIZEOF(expected), actual.size());
    for (int64_t i = 0; i < ARRAYSIZEOF(expected); ++i) {
      EXPECT_EQ(expected[i], actual[i]);
    }
    bitmap_iter.reset();
    bitmap.reset();
  }

  void check_pre_brute_dispatch(ObVecIndexBitmap::FilterType type,
                                ObVectorIndexAlgorithmType algorithm_type)
  {
    ObArenaAllocator allocator("VecPreBruteUT");
    ObVecIndexBitmap bitmap(&allocator);
    ASSERT_EQ(OB_SUCCESS, prepare_bitmap(type, bitmap));
    ASSERT_EQ(type, bitmap.type_);

    ObDASVecIndexHNSWScanIter iter;
    ASSERT_EQ(OB_SUCCESS, CURRENT_CONTEXT->CREATE_CONTEXT(
        iter.mem_context_, lib::ContextParam().set_label("VecPreBruteUT")));
    iter.set_bitmap(&bitmap);
    iter.set_limit(3);

    float query_vector = 0;
    ObVectorQueryConditions query_cond;
    query_cond.query_vector_.assign_ptr(
        reinterpret_cast<char *>(&query_vector), sizeof(query_vector));
    query_cond.ef_search_ = 3;
    iter.query_cond_ = &query_cond;

    ObDASVecIndexHNSWScanCtDef ctdef(allocator);
    ctdef.algorithm_type_ = algorithm_type;
    iter.vec_index_scan_ctdef_ = &ctdef;

    {
      ObPluginVectorIndexAdaptor adaptor(&allocator, iter.mem_context_, MTL_ID());
      bool need_complete_data = true;
      // An empty adaptor makes the brute-force query succeed with no distances,
      // then the result builder returns OB_ITER_END. This return value proves that
      // all three bitmap representations reached the brute-force implementation.
      EXPECT_EQ(OB_ITER_END, iter.process_adaptor_state_pre_filter_brute_force(
          &iter.ada_ctx_, &adaptor, need_complete_data, false));
      EXPECT_FALSE(need_complete_data);
      EXPECT_EQ(nullptr, iter.adaptor_vid_iter_);
    }

    bitmap.reset();
    DESTROY_CONTEXT(iter.mem_context_);
  }
};

TEST_F(TestVecIndexPreBrute, bitmap_batches)
{
  check_bitmap_batches(ObVecIndexBitmap::BYTE_ARRAY);
  check_bitmap_batches(ObVecIndexBitmap::ROARING_BITMAP);
}

TEST_F(TestVecIndexPreBrute, all_bitmap_types_enter_hnsw_pre_brute)
{
  check_pre_brute_dispatch(ObVecIndexBitmap::VIDS, VIAT_HNSW);
  check_pre_brute_dispatch(ObVecIndexBitmap::BYTE_ARRAY, VIAT_HNSW);
  check_pre_brute_dispatch(ObVecIndexBitmap::ROARING_BITMAP, VIAT_HNSW);
}

TEST_F(TestVecIndexPreBrute, all_bitmap_types_enter_hnsw_bq_pre_brute)
{
  check_pre_brute_dispatch(ObVecIndexBitmap::VIDS, VIAT_HNSW_BQ);
  check_pre_brute_dispatch(ObVecIndexBitmap::BYTE_ARRAY, VIAT_HNSW_BQ);
  check_pre_brute_dispatch(ObVecIndexBitmap::ROARING_BITMAP, VIAT_HNSW_BQ);
}

} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
